module github.com/courier-rpc/courier/integration

go 1.24.0

require (
	github.com/mochi-mqtt/server/v2 v2.7.9
	github.com/simpossible/courier v0.0.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/eclipse/paho.golang v0.23.0 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/redis/go-redis/v9 v9.18.0 // indirect
	github.com/rs/xid v1.4.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace github.com/simpossible/courier => ..
