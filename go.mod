module filesync-prototype

go 1.23.0

require (
	github.com/go-sql-driver/mysql v0.0.0-00010101000000-000000000000
	google.golang.org/grpc v1.67.1
	google.golang.org/protobuf v1.36.11
)

replace (
	github.com/go-sql-driver/mysql => github.com/go-sql-driver/mysql v1.7.1
	golang.org/x/net => golang.org/x/net v0.29.0
	golang.org/x/sys => golang.org/x/sys v0.25.0
	golang.org/x/text => golang.org/x/text v0.18.0
	google.golang.org/genproto/googleapis/rpc => google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1
)

require (
	github.com/joho/godotenv v1.5.1
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
)
