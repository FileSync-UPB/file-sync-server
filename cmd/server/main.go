package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	pb "filesync-prototype/api/filesync/v1"

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
	mu                sync.Mutex
	root, token, user string
	db                database
	staging           map[string]staged
	watchers          map[chan *pb.ChangeEvent]bool
}

func newServer(root, token, user string) (*fileSyncServer, error) {
	if token == "" || user == "" {
		return nil, errors.New("FILESYNC_TOKEN y FILESYNC_USER_ID son obligatorios")
	}
	s := &fileSyncServer{root: root, token: token, user: user, db: database{Devices: map[string]string{}}, staging: map[string]staged{}, watchers: map[chan *pb.ChangeEvent]bool{}}
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
func (s *fileSyncServer) auth(ctx context.Context) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return errors.New("falta token")
	}
	for _, v := range md.Get("authorization") {
		got := strings.TrimPrefix(v, "Bearer ")
		if len(got) == len(s.token) && subtle.ConstantTimeCompare([]byte(got), []byte(s.token)) == 1 {
			return nil
		}
	}
	return errors.New("token inválido")
}
func (s *fileSyncServer) checkDevice(id string) error {
	if id == "" || s.db.Devices[id] != s.user {
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
	if e := s.auth(ctx); e != nil {
		return nil, e
	}
	if q.GetDeviceId() == "" {
		return nil, errors.New("device_id requerido")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.db.Devices[q.GetDeviceId()] = s.user
	if e := s.saveLocked(); e != nil {
		return nil, e
	}
	return &pb.RegisterDeviceResponse{DeviceId: q.GetDeviceId(), UserId: s.user}, nil
}
func (s *fileSyncServer) GetSyncPlan(ctx context.Context, q *pb.GetSyncPlanRequest) (*pb.GetSyncPlanResponse, error) {
	if e := s.auth(ctx); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.checkDevice(q.GetDeviceId()); e != nil {
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
	if e := s.auth(stream.Context()); e != nil {
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
	e = s.checkDevice(head.GetDeviceId())
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
	if e := s.auth(stream.Context()); e != nil {
		return e
	}
	p, e := cleanPath(q.GetPath())
	if e != nil {
		return e
	}
	s.mu.Lock()
	if e = s.checkDevice(q.GetDeviceId()); e != nil {
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
	if e := s.auth(stream.Context()); e != nil {
		return e
	}
	s.mu.Lock()
	if e := s.checkDevice(q.GetDeviceId()); e != nil {
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
	if e := s.auth(ctx); e != nil {
		return nil, e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.checkDevice(q.GetDeviceId()); e != nil {
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
func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No se encontró el archivo .env; se usarán las variables del sistema")
	}
	root := os.Getenv("FILESYNC_DATA_DIR")
	if root == "" {
		root = "./data"
	}
	s, e := newServer(root, os.Getenv("FILESYNC_TOKEN"), os.Getenv("FILESYNC_USER_ID"))
	if e != nil {
		log.Fatal(e)
	}
	addr := os.Getenv("FILESYNC_LISTEN")
	if addr == "" {
		addr = ":8443"
	}
	cert, key := os.Getenv("FILESYNC_TLS_CERT"), os.Getenv("FILESYNC_TLS_KEY")
	if cert == "" || key == "" {
		log.Fatal("defina FILESYNC_TLS_CERT y FILESYNC_TLS_KEY; servidor cerrado sin TLS")
	}
	creds, e := credentials.NewServerTLSFromFile(cert, key)
	if e != nil {
		log.Fatal(e)
	}
	lis, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal(e)
	}
	g := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterFileSyncServer(g, s)
	log.Printf("File Sync gRPC en %s", addr)
	if e = g.Serve(lis); e != nil {
		log.Fatal(e)
	}
}
