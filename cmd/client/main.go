package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "filesync-prototype/api/filesync/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

const uploadChunkSize = 256 * 1024

type clientConfig struct {
	serverAddr string
	token      string
	localDir   string
	deviceID   string
	deviceName string
	platform   string
	interval   time.Duration
	scheduleAt string
	wifiOnly   bool
}

func main() {
	var (
		serverAddr = flag.String("server", "localhost:8443", "Dirección del servidor gRPC (host:puerto)")
		token      = flag.String("token", "", "Token de autenticación Bearer")
		dir        = flag.String("dir", "./sync_folder", "Directorio local a sincronizar")
		deviceID   = flag.String("device", "", "Identificador único del dispositivo")
		deviceName = flag.String("name", "", "Nombre amigable del dispositivo (ej. Laptop-Windows)")
		platform   = flag.String("platform", "Windows", "Plataforma del cliente (Windows, Linux, Android)")
		interval   = flag.Duration("interval", 0, "Intervalo de sincronización periódica (ej. 30s, 5m). 0 = ejecutar una sola vez")
		scheduleAt = flag.String("schedule", "", "Hora programada para sincronización diaria en formato HH:MM (ej. 02:00)")
		wifiOnly   = flag.Bool("wifi-only", false, "Exigir conexión a red Wi-Fi local para sincronizar (política móvil)")
		action     = flag.String("action", "sync", "Acción a ejecutar: sync, register, watch")
	)
	flag.Parse()

	if *token == "" {
		*token = os.Getenv("FILESYNC_TOKEN")
	}
	if *token == "" {
		log.Fatal("Se requiere un token de autenticación (--token o $env:FILESYNC_TOKEN)")
	}

	if *deviceID == "" {
		hostname, err := os.Hostname()
		if err != nil || hostname == "" {
			hostname = "dev-client"
		}
		*deviceID = fmt.Sprintf("%s-%s", strings.ToLower(*platform), hostname)
	}
	if *deviceName == "" {
		*deviceName = fmt.Sprintf("Equipo %s (%s)", *platform, *deviceID)
	}

	cfg := clientConfig{
		serverAddr: *serverAddr,
		token:      *token,
		localDir:   *dir,
		deviceID:   *deviceID,
		deviceName: *deviceName,
		platform:   *platform,
		interval:   *interval,
		scheduleAt: *scheduleAt,
		wifiOnly:   *wifiOnly,
	}

	if err := os.MkdirAll(cfg.localDir, 0755); err != nil {
		log.Fatalf("No se pudo crear/acceder al directorio local %s: %v", cfg.localDir, err)
	}

	client, conn, err := connectGRPC(cfg.serverAddr)
	if err != nil {
		log.Fatalf("Error al conectar con el servidor gRPC %s: %v", cfg.serverAddr, err)
	}
	defer conn.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+cfg.token)

	// registro del dispositivo
	regResp, err := client.RegisterDevice(ctx, &pb.RegisterDeviceRequest{
		DeviceId:   cfg.deviceID,
		DeviceName: cfg.deviceName,
		Platform:   cfg.platform,
	})
	if err != nil {
		log.Fatalf("Fallo al registrar dispositivo: %v", err)
	}
	log.Printf("[DISPOSITIVO] Registrado con éxito: %s (Usuario: %s, Plataforma: %s)", regResp.GetDeviceId(), regResp.GetUserId(), cfg.platform)

	if *action == "register" {
		return
	}

	// Ejecución según modo: Programado, Periódico o Una sola vez
	if cfg.scheduleAt != "" {
		log.Printf("[PROGRAMADOR] Modo programado activo a las %s todos los días", cfg.scheduleAt)
		runScheduled(ctx, client, cfg)
		return
	}

	if cfg.interval > 0 {
		log.Printf("[DEMONIO] Sincronización periódica cada %v", cfg.interval)
		runPeriodic(ctx, client, cfg)
		return
	}

	// Ejecución directa (Sync Once)
	if err := performSync(ctx, client, cfg); err != nil {
		log.Fatalf("Error durante la sincronización: %v", err)
	}
}

// Se encarga de abrir la conexión gRPC con el servidor.
func connectGRPC(serverAddr string) (pb.FileSyncClient, *grpc.ClientConn, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // Permitir certificados autofirmados del servidor local
	}
	creds := credentials.NewTLS(tlsConfig)

	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewFileSyncClient(conn), conn, nil
}

func checkWiFiConnectivity() bool {
	// Comprobar si existe una interfaz de red en el rango local o privado (simulación de Wi-Fi local)
	ifaces, err := net.Interfaces()
	if err != nil {
		return false
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
				if ip4 := ipNet.IP.To4(); ip4 != nil {
					// Redes privadas RFC 1918 (típicas de red Wi-Fi universitaria/empresarial o local)
					if ip4[0] == 10 || (ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31) || (ip4[0] == 192 && ip4[1] == 168) {
						return true
					}
				}
			}
		}
	}
	return false
}

func runScheduled(ctx context.Context, client pb.FileSyncClient, cfg clientConfig) {
	for {
		now := time.Now()
		targetStr := fmt.Sprintf("%04d-%02d-%02d %s:00", now.Year(), now.Month(), now.Day(), cfg.scheduleAt)
		targetTime, err := time.ParseInLocation("2006-01-02 15:04:05", targetStr, time.Local)
		if err != nil {
			log.Fatalf("Formato de hora programada inválido: %v", err)
		}
		if now.After(targetTime) {
			targetTime = targetTime.Add(24 * time.Hour)
		}

		waitDuration := time.Until(targetTime)
		log.Printf("[PROGRAMADOR] Próxima sincronización programada en %v (%s)", waitDuration.Round(time.Second), targetTime.Format("15:04:05"))
		time.Sleep(waitDuration)

		log.Printf("[PROGRAMADOR] Ejecutando sincronización de horario fijado (%s)...", cfg.scheduleAt)
		if err := performSync(ctx, client, cfg); err != nil {
			log.Printf("[ERROR] Sincronización programada falló: %v", err)
		}
	}
}

func runPeriodic(ctx context.Context, client pb.FileSyncClient, cfg clientConfig) {
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()

	// Ejecutar primera vez inmediatamente
	if err := performSync(ctx, client, cfg); err != nil {
		log.Printf("[ERROR] Sincronización inicial falló: %v", err)
	}

	for range ticker.C {
		log.Printf("[INTERVALO] Iniciando ciclo de sincronización automática...")
		if err := performSync(ctx, client, cfg); err != nil {
			log.Printf("[ERROR] Ciclo de sincronización falló: %v", err)
		}
	}
}

/*
realiza la sincronizacion completa
*/
func performSync(ctx context.Context, client pb.FileSyncClient, cfg clientConfig) error {
	log.Printf("[INICIO] Sincronizando directorio local: %s", cfg.localDir)

	if cfg.wifiOnly {
		isWiFi := checkWiFiConnectivity()
		if !isWiFi {
			log.Printf("[POLÍTICA WI-FI] Dispositivo en datos móviles o sin Wi-Fi local UPB. Sincronización en pausa.")
			return nil
		}
		log.Printf("[POLÍTICA WI-FI] Red Wi-Fi local confirmada.")
	}

	// 1. Escanear directorio local
	manifest, err := buildLocalManifest(cfg.localDir)
	if err != nil {
		return fmt.Errorf("error construyendo manifiesto local: %w", err)
	}
	log.Printf("[LOCAL] %d archivos analizados localmente", len(manifest))

	// 2. Solicitar Plan de Sincronización al servidor
	planResp, err := client.GetSyncPlan(ctx, &pb.GetSyncPlanRequest{
		DeviceId: cfg.deviceID,
		Entries:  manifest,
	})
	if err != nil {
		return fmt.Errorf("error obteniendo plan de sincronización: %w", err)
	}

	changes := planResp.GetChanges()
	log.Printf("[PLAN] Recibidas %d instrucciones del servidor (SyncID: %s)", len(changes), planResp.GetSyncId())

	var commitOps []*pb.CommitOperation
	// se recorre cada cambio
	for _, ch := range changes {
		switch ch.GetAction() {
		case pb.SyncAction_UPLOAD:
			log.Printf("  -> [SUBIR] %s (Causa: %s)", ch.GetPath(), ch.GetReason())
			sha, err := uploadLocalFile(ctx, client, cfg, planResp.GetSyncId(), ch.GetPath())
			if err != nil {
				log.Printf("     [ERROR] Falló subida de %s: %v", ch.GetPath(), err)
				continue
			}
			commitOps = append(commitOps, &pb.CommitOperation{
				Kind:         pb.CommitKind_UPSERT,
				Path:         ch.GetPath(),
				SyncId:       planResp.GetSyncId(),
				BaseRevision: ch.GetServerRevision(),
			})
			log.Printf("     [OK] Archivo cargado exitosamente. SHA: %s", sha[:8])

		case pb.SyncAction_DOWNLOAD:
			log.Printf("  <- [DESCARGAR] %s (Causa: %s)", ch.GetPath(), ch.GetReason())
			if err := downloadRemoteFile(ctx, client, cfg, ch.GetPath(), ch.GetServerRevision()); err != nil {
				log.Printf("     [ERROR] Falló descarga de %s: %v", ch.GetPath(), err)
			} else {
				log.Printf("     [OK] Archivo descargado en directorio local.")
			}

		case pb.SyncAction_DELETE_REMOTE:
			log.Printf("  -- [BORRAR REMOTO] %s (Causa: %s)", ch.GetPath(), ch.GetReason())
			commitOps = append(commitOps, &pb.CommitOperation{
				Kind:         pb.CommitKind_DELETE,
				Path:         ch.GetPath(),
				SyncId:       planResp.GetSyncId(),
				BaseRevision: ch.GetServerRevision(),
			})

		case pb.SyncAction_NO_OP:
			// En sincronía
		}
	}

	// 3. Confirmar operaciones (CommitSync)
	if len(commitOps) > 0 {
		commitResp, err := client.CommitSync(ctx, &pb.CommitSyncRequest{
			SyncId:     planResp.GetSyncId(),
			DeviceId:   cfg.deviceID,
			Operations: commitOps,
		})
		if err != nil {
			return fmt.Errorf("error confirmando sincronización: %w", err)
		}
		for _, res := range commitResp.GetResults() {
			if res.GetConflict() {
				log.Printf("[CONFLICTO] %s bifurcado en rama %s", res.GetPath(), res.GetBranch())
			} else {
				log.Printf("[CONFIRMADO] %s rev: %s rama: %s", res.GetPath(), res.GetRevision(), res.GetBranch())
			}
		}
	}

	log.Printf("[FIN] Sincronización completada exitosamente.")
	return nil
}

// recorre el directorio local y crea una lista de entradas
func buildLocalManifest(baseDir string) ([]*pb.ManifestEntry, error) {
	var entries []*pb.ManifestEntry

	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		hash, err := hashFile(path)
		if err != nil {
			return err
		}

		entries = append(entries, &pb.ManifestEntry{
			Path:          rel,
			Sha256:        hash,
			LocalRevision: "1",
			Deleted:       false,
		})
		return nil
	})

	return entries, err
}

// Calcula el hash SHA256 de un archivo.
func hashFile(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Sube un archivo local al servidor usando streaming gRPC.
func uploadLocalFile(ctx context.Context, client pb.FileSyncClient, cfg clientConfig, syncID, relPath string) (string, error) {
	fullPath := filepath.Join(cfg.localDir, filepath.FromSlash(relPath))
	file, err := os.Open(fullPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", err
	}

	sha, err := hashFile(fullPath)
	if err != nil {
		return "", err
	}

	stream, err := client.UploadFile(ctx)
	if err != nil {
		return "", err
	}

	// 1. Enviar encabezado
	headerFrame := &pb.UploadFrame{
		Payload: &pb.UploadFrame_Header{
			Header: &pb.UploadHeader{
				SyncId:   syncID,
				DeviceId: cfg.deviceID,
				Path:     relPath,
				Sha256:   sha,
				Size:     info.Size(),
			},
		},
	}
	if err := stream.Send(headerFrame); err != nil {
		return "", err
	}

	// 2. Enviar fragmentos (chunks)
	buf := make([]byte, uploadChunkSize)
	for {
		n, err := file.Read(buf)
		if n > 0 {
			chunkFrame := &pb.UploadFrame{
				Payload: &pb.UploadFrame_Chunk{
					Chunk: buf[:n],
				},
			}
			if err := stream.Send(chunkFrame); err != nil {
				return "", err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return "", err
	}
	return resp.GetSha256(), nil
}

// Descarga un archivo desde el servidor al directorio local.
func downloadRemoteFile(ctx context.Context, client pb.FileSyncClient, cfg clientConfig, relPath, revision string) error {
	fullPath := filepath.Join(cfg.localDir, filepath.FromSlash(relPath))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return err
	}

	stream, err := client.DownloadFile(ctx, &pb.DownloadFileRequest{
		DeviceId: cfg.deviceID,
		Path:     relPath,
		Revision: revision,
	})
	if err != nil {
		return err
	}

	tmpPath := fullPath + ".download"
	out, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		out.Close()
		os.Remove(tmpPath)
	}()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(chunk.GetData()) > 0 {
			if _, err := out.Write(chunk.GetData()); err != nil {
				return err
			}
		}
		if chunk.GetLast() {
			break
		}
	}

	out.Close()
	return os.Rename(tmpPath, fullPath)
}
