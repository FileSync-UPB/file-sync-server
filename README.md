# File Sync: servidor gRPC en Go

Prototipo del servidor común. Los clientes de escritorio y Android quedan para
una etapa posterior.

## Requisitos

- Go 1.25 o posterior (el `go.mod` actual y `grpc v1.83.2` lo requieren).
- `protoc`, con los plugins `protoc-gen-go` y `protoc-gen-go-grpc`.
- Certificado y clave privada TLS.
- PowerShell.

## Preparación y ejecución

Desde la raíz del proyecto, en PowerShell:

```powershell
.\generate.ps1
go mod tidy
```

Puedes copiar `.env.example` a `.env` y reemplazar los valores de ejemplo por
los reales. El servidor carga ese archivo automáticamente al iniciar; si no
existe, usa las variables de entorno del sistema. No subas `.env` a GitHub.
Como alternativa, configura las variables de entorno antes de iniciar el
servidor:

```powershell
$env:FILESYNC_USER_ID = "usuario"
$env:FILESYNC_TOKEN = "token"
$env:FILESYNC_TLS_CERT = "C:\filesync\certs\server.crt"
$env:FILESYNC_TLS_KEY = "C:\filesync\certs\server.key"
$env:FILESYNC_DATA_DIR = "C:\filesync\data"
```

Después, inicia el servidor:

```powershell
go run .\cmd\server
```

El servidor no inicia sin `FILESYNC_TLS_CERT` y `FILESYNC_TLS_KEY`. El puerto
predeterminado es `:8443`; puede cambiarse con `FILESYNC_LISTEN`.

Las llamadas gRPC deben incluir esta metadata:

```text
authorization: Bearer TOKEN
```

## Flujo gRPC

- `RegisterDevice` asocia un dispositivo al usuario.
- `GetSyncPlan` compara el manifiesto local y devuelve cargas, borrados pendientes
  o `NO_OP`.
- `UploadFile` recibe una cabecera y bloques de 256 KiB, y valida tamaño y
  SHA-256.
- `CommitSync` confirma archivos o tombstones y conserva las ramas de conflicto.
- `DownloadFile` transmite una revisión por bloques.
- `WatchChanges` transmite eventos mientras el cliente está conectado. Al
  reconectar, el cliente debe solicitar un nuevo plan porque todavía no existe
  un cursor durable.

## Decisiones y límites actuales

La sincronización es manual, con flujo inicial de cliente a servidor. Los
conflictos se guardan en ramas alternas y no pisan `main`; los borrados se
conservan como tombstones y se notifican. Los blobs y `state.json` se almacenan
en `FILESYNC_DATA_DIR`.

El diseño actual es monoproceso y no admite múltiples réplicas. Todavía faltan
cuota por usuario, ACL Unix, integración con el directorio institucional,
eventos durables, purga automática de versiones y el backend distribuido final
del CCA. Las rutas relativas se validan.
