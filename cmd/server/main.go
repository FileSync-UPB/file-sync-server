package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "filesync-prototype/api/filesync/v1"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const blockSize = 256 * 1024

type revision struct {
	ID, Path, Hash, Blob, Branch string
	Deleted                      bool
	Device                       string
	Created                      time.Time
}
type database struct {
	Devices   map[string]string
	Revisions []revision
}
type staged struct{ file, hash, device string }
type fileSyncServer struct {
	pb.UnimplementedFileSyncServer
	mu       sync.Mutex
	root     string
	mysqldb  *sql.DB
	db       database
	staging  map[string]staged
	watchers map[chan *pb.ChangeEvent]bool
}

func newServer(root string) (*fileSyncServer, error) {
	dbHost := os.Getenv("MYSQL_HOST")
	if dbHost == "" {
		dbHost = "db"
	}
	dbPort := os.Getenv("MYSQL_PORT")
	if dbPort == "" {
		dbPort = "3306"
	}
	dbUser := os.Getenv("MYSQL_USER")
	if dbUser == "" {
		dbUser = "upb_app"
	}
	dbPass := os.Getenv("MYSQL_PASSWORD")
	if dbPass == "" {
		dbPass = "secret_password"
	}
	dbName := os.Getenv("MYSQL_DATABASE")
	if dbName == "" {
		dbName = "fylesync_users"
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		dbUser, dbPass, dbHost, dbPort, dbName)

	var mysqldb *sql.DB
	var err error
	for attempts := 1; attempts <= 10; attempts++ {
		mysqldb, err = sql.Open("mysql", dsn)
		if err == nil {
			if err = mysqldb.Ping(); err == nil {
				log.Printf("Conexión exitosa a MySQL (%s:%s)", dbHost, dbPort)
				break
			}
		}
		log.Printf("Esperando a MySQL (%s:%s)... intento %d/10: %v", dbHost, dbPort, attempts, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("no se pudo conectar a MySQL: %w", err)
	}

	s := &fileSyncServer{root: root, mysqldb: mysqldb, db: database{Devices: map[string]string{}}, staging: map[string]staged{}, watchers: map[chan *pb.ChangeEvent]bool{}}
	if e := os.MkdirAll(filepath.Join(root, "blobs"), 0700); e != nil {
		return nil, e
	}
	b, e := os.ReadFile(filepath.Join(root, "state.json"))
	if e == nil {
		if e = json.Unmarshal(b, &s.db); e != nil {
			return nil, e
		}
	}
	if s.db.Devices == nil {
		s.db.Devices = map[string]string{}
	}
	return s, nil
}
func (s *fileSyncServer) saveLocked() error {
	b, e := json.MarshalIndent(s.db, "", "  ")
	if e != nil {
		return e
	}
	tmp := filepath.Join(s.root, "state.json.tmp")
	if e = os.WriteFile(tmp, b, 0600); e != nil {
		return e
	}
	return os.Rename(tmp, filepath.Join(s.root, "state.json"))
}
func (s *fileSyncServer) auth(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("falta token")
	}
	for _, v := range md.Get("authorization") {
		token := strings.TrimPrefix(v, "Bearer ")
		hash := sha256.Sum256([]byte(token))
		hashStr := hex.EncodeToString(hash[:])
		
		var username string
		err := s.mysqldb.QueryRow("SELECT username FROM auth_tokens WHERE token_hash = ? AND active = 1 AND expires_at > NOW()", hashStr).Scan(&username)
		if err == nil {
			return username, nil
		}
	}
	return "", errors.New("token inválido")
}
func (s *fileSyncServer) checkDevice(id string, username string) error {
	if id == "" || s.db.Devices[id] != username {
		return errors.New("dispositivo no registrado para este usuario")
	}
	return nil
}
func cleanPath(p string) (string, error) {
	p = filepath.ToSlash(filepath.Clean(filepath.FromSlash(p)))
	if p == "." || p == ".." || strings.HasPrefix(p, "../") || filepath.IsAbs(p) || strings.Contains(p, ":") {
		return "", errors.New("ruta no permitida")
	}
	return p, nil
}
func (s *fileSyncServer) head(p string) (revision, bool) {
	for i := len(s.db.Revisions) - 1; i >= 0; i-- {
		r := s.db.Revisions[i]
		if r.Path == p && r.Branch == "main" {
			return r, true
		}
	}
	return revision{}, false
}
func (s *fileSyncServer) RegisterDevice(ctx context.Context, q *pb.RegisterDeviceRequest) (*pb.RegisterDeviceResponse, error) {
	username, e := s.auth(ctx)
	if e != nil {
		return nil, e
	}
	if q.GetDeviceId() == "" {
		return nil, errors.New("device_id requerido")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Devices[q.GetDeviceId()] = username
	if e := s.saveLocked(); e != nil {
		return nil, e
	}
	return &pb.RegisterDeviceResponse{DeviceId: q.GetDeviceId(), UserId: username}, nil
}
func (s *fileSyncServer) GetSyncPlan(ctx context.Context, q *pb.GetSyncPlanRequest) (*pb.GetSyncPlanResponse, error) {
	username, e := s.auth(ctx)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.checkDevice(q.GetDeviceId(), username); e != nil {
		return nil, e
	}
	out := &pb.GetSyncPlanResponse{SyncId: fmt.Sprintf("%s-%d", q.GetDeviceId(), time.Now().UnixNano())}
	for _, entry := range q.GetEntries() {
		p, e := cleanPath(entry.GetPath())
		if e != nil {
			return nil, e
		}
		h, exists := s.head(p)
		switch {
		case entry.GetDeleted() && exists && !h.Deleted:
			out.Changes = append(out.Changes, &pb.PlannedChange{Path: p, Action: pb.SyncAction_DELETE_REMOTE, ServerRevision: h.ID, Reason: "borrado local pendiente"})
		case entry.GetDeleted():
			out.Changes = append(out.Changes, &pb.PlannedChange{Path: p, Action: pb.SyncAction_NO_OP, Reason: "ya borrado"})
		case !exists || h.Deleted || h.Hash != entry.GetSha256():
			out.Changes = append(out.Changes, &pb.PlannedChange{Path: p, Action: pb.SyncAction_UPLOAD, ServerRevision: h.ID, ServerSha256: h.Hash, Reason: "archivo nuevo o modificado localmente"})
		default:
			out.Changes = append(out.Changes, &pb.PlannedChange{Path: p, Action: pb.SyncAction_NO_OP, ServerRevision: h.ID, ServerSha256: h.Hash})
		}
	}
	return out, nil
}
func (s *fileSyncServer) UploadFile(stream grpc.ClientStreamingServer[pb.UploadFrame, pb.UploadFileResponse]) error {
	username, e := s.auth(stream.Context())
	if e != nil {
		return e
	}
	first, e := stream.Recv()
	if e != nil {
		return e
	}
	head := first.GetHeader()
	if head == nil {
		return errors.New("el primer frame debe ser header")
	}
	p, e := cleanPath(head.GetPath())
	if e != nil {
		return e
	}
	s.mu.Lock()
	e = s.checkDevice(head.GetDeviceId(), username)
	s.mu.Unlock()
	if e != nil {
		return e
	}
	if head.GetSize() < 0 {
		return errors.New("tamaño inválido")
	}
	f, e := os.CreateTemp(filepath.Join(s.root, "blobs"), ".upload-*")
	if e != nil {
		return e
	}
	keepStagedFile := false
	defer func() {
		f.Close()
		if !keepStagedFile {
			os.Remove(f.Name())
		}
	}()
	hash := sha256.New()
	var size int64
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return err
		}
		data := frame.GetChunk()
		size += int64(len(data))
		if size > head.GetSize() {
			f.Close()
			return errors.New("se excedió el tamaño declarado")
		}
		if _, e = f.Write(data); e != nil {
			f.Close()
			return e
		}
		hash.Write(data)
	}
	if e = f.Close(); e != nil {
		return e
	}
	sum := hex.EncodeToString(hash.Sum(nil))
	if size != head.GetSize() || sum != head.GetSha256() {
		return errors.New("tamaño o SHA-256 no coincide")
	}
	s.mu.Lock()
	s.staging[head.GetSyncId()+"|"+p] = staged{f.Name(), sum, head.GetDeviceId()}
	s.mu.Unlock()
	keepStagedFile = true
	return stream.SendAndClose(&pb.UploadFileResponse{SyncId: head.GetSyncId(), Path: p, BytesReceived: size, Sha256: sum})
}
func (s *fileSyncServer) DownloadFile(q *pb.DownloadFileRequest, stream grpc.ServerStreamingServer[pb.FileChunk]) error {
	username, e := s.auth(stream.Context())
	if e != nil {
		return e
	}
	p, e := cleanPath(q.GetPath())
	if e != nil {
		return e
	}
	s.mu.Lock()
	if e = s.checkDevice(q.GetDeviceId(), username); e != nil {
		s.mu.Unlock()
		return e
	}
	var r revision
	if q.GetRevision() == "" {
		r, _ = s.head(p)
	} else {
		for i := len(s.db.Revisions) - 1; i >= 0; i-- {
			x := s.db.Revisions[i]
			if x.Path == p && x.ID == q.GetRevision() {
				r = x
				break
			}
		}
	}
	s.mu.Unlock()
	if r.ID == "" || r.Deleted {
		return errors.New("versión no encontrada")
	}
	f, e := os.Open(r.Blob)
	if e != nil {
		return e
	}
	defer f.Close()
	buf := make([]byte, blockSize)
	var off int64
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if e = stream.Send(&pb.FileChunk{Path: p, Revision: r.ID, Offset: off, Data: append([]byte(nil), buf[:n]...), Last: err == io.EOF}); e != nil {
				return e
			}
			off += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if off == 0 {
		return stream.Send(&pb.FileChunk{Path: p, Revision: r.ID, Last: true})
	}
	return nil
}
func (s *fileSyncServer) WatchChanges(q *pb.WatchChangesRequest, stream grpc.ServerStreamingServer[pb.ChangeEvent]) error {
	username, e := s.auth(stream.Context())
	if e != nil {
		return e
	}
	s.mu.Lock()
	if e := s.checkDevice(q.GetDeviceId(), username); e != nil {
		s.mu.Unlock()
		return e
	}
	ch := make(chan *pb.ChangeEvent, 32)
	s.watchers[ch] = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.watchers, ch); s.mu.Unlock() }()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case ev := <-ch:
			if e := stream.Send(ev); e != nil {
				return e
			}
		}
	}
}
func (s *fileSyncServer) CommitSync(ctx context.Context, q *pb.CommitSyncRequest) (*pb.CommitSyncResponse, error) {
	username, e := s.auth(ctx)
	if e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.checkDevice(q.GetDeviceId(), username); e != nil {
		return nil, e
	}
	out := &pb.CommitSyncResponse{}
	for _, op := range q.GetOperations() {
		p, e := cleanPath(op.GetPath())
		if e != nil {
			return nil, e
		}
		var up staged
		var ok bool
		if op.GetKind() == pb.CommitKind_UPSERT {
			up, ok = s.staging[op.GetSyncId()+"|"+p]
			if !ok || up.device != q.GetDeviceId() {
				return nil, fmt.Errorf("carga validada no encontrada: %s", p)
			}
		}
		h, exists := s.head(p)
		conflict := exists && h.ID != op.GetBaseRevision()
		branch := "main"
		if conflict {
			branch = fmt.Sprintf("conflict-%s-%d", q.GetDeviceId(), time.Now().UnixNano())
		}
		r := revision{ID: fmt.Sprintf("r-%d", time.Now().UnixNano()), Path: p, Branch: branch, Device: q.GetDeviceId(), Created: time.Now().UTC(), Deleted: op.GetKind() == pb.CommitKind_DELETE}
		if op.GetKind() == pb.CommitKind_UPSERT {
			r.Hash = up.hash
			r.Blob = filepath.Join(s.root, "blobs", r.ID)
			if e = os.Rename(up.file, r.Blob); e != nil {
				return nil, e
			}
			delete(s.staging, op.GetSyncId()+"|"+p)

			// Espejar archivo al repositorio central del usuario
			userHomePath := filepath.Join(s.root, username, filepath.FromSlash(p))
			if e = os.MkdirAll(filepath.Dir(userHomePath), 0755); e == nil {
				if src, e := os.Open(r.Blob); e == nil {
					if dst, e := os.Create(userHomePath); e == nil {
						io.Copy(dst, src)
						dst.Close()
					}
					src.Close()
				}
			}
		} else if op.GetKind() == pb.CommitKind_DELETE {
			userHomePath := filepath.Join(s.root, username, filepath.FromSlash(p))
			os.Remove(userHomePath)
		}
		s.db.Revisions = append(s.db.Revisions, r)
		if e = s.saveLocked(); e != nil {
			return nil, e
		}
		kind := pb.ChangeKind_FILE_UPDATED
		if r.Deleted {
			kind = pb.ChangeKind_FILE_DELETED
		}
		ev := &pb.ChangeEvent{Kind: kind, Path: p, Revision: r.ID, Sha256: r.Hash, Branch: r.Branch}
		for ch := range s.watchers {
			select {
			case ch <- ev:
			default:
			}
		}
		out.Results = append(out.Results, &pb.CommitResult{Path: p, Revision: r.ID, Branch: r.Branch, Conflict: conflict})
	}
	return out, nil
}

func generateSelfSignedCert(certPath, keyPath string) error {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			Organization: []string{"UPB Fylesync Enterprise"},
			CommonName:   "localhost",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("0.0.0.0")},
		DNSNames:              []string{"localhost", "file-sync-server", "fylesync-file-sync"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(certPath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(keyPath), 0755); err != nil {
		return err
	}

	certOut, err := os.Create(certPath)
	if err != nil {
		return err
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certOut.Close()
		return err
	}
	certOut.Close()

	keyOut, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		keyOut.Close()
		return err
	}
	keyOut.Close()

	log.Printf("Certificado TLS autofirmado generado en %s y %s", certPath, keyPath)
	return nil
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No se encontró el archivo .env; se usarán las variables del sistema")
	}
	root := os.Getenv("FILESYNC_DATA_DIR")
	if root == "" {
		root = "/var/fylesync/home"
	}
	s, e := newServer(root)
	if e != nil {
		log.Fatal(e)
	}
	addr := os.Getenv("FILESYNC_LISTEN")
	if addr == "" {
		addr = ":8443"
	}
	cert, key := os.Getenv("FILESYNC_TLS_CERT"), os.Getenv("FILESYNC_TLS_KEY")
	if cert == "" || key == "" {
		cert = filepath.Join(root, "certs", "server.crt")
		key = filepath.Join(root, "certs", "server.key")
		if _, err := os.Stat(cert); os.IsNotExist(err) {
			if err := generateSelfSignedCert(cert, key); err != nil {
				log.Fatalf("Error generando certificado TLS: %v", err)
			}
		}
	}
	creds, e := credentials.NewServerTLSFromFile(cert, key)
	if e != nil {
		log.Fatalf("Error cargando credenciales TLS: %v", e)
	}
	lis, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal(e)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterFileSyncServer(g, s)
	log.Printf("File Sync gRPC TLS activo en %s (Almacenamiento: %s)", addr, root)
	if e = g.Serve(lis); e != nil {
		log.Fatal(e)
	}
}
