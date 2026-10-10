$ErrorActionPreference = 'Stop'
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
$env:Path += ";$env:USERPROFILE\go\bin"
protoc --proto_path=. --go_out=. --go_opt=module=filesync-prototype --go-grpc_out=. --go-grpc_opt=module=filesync-prototype api/filesync/v1/filesync.proto
if ($LASTEXITCODE -ne 0) { throw 'Falló la generación de código protobuf/gRPC' }
