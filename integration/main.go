// A loopback-only MQTT 5 broker plus two real Courier service instances.
// It is deliberately a separate module: broker dependencies are test-only.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/simpossible/courier/codec"
	"github.com/simpossible/courier/rpc"
	"github.com/simpossible/courier/transport"
	"time"
)

// Emulates the production broker rule: publisher identity comes from its
// authenticated MQTT connection, never from a caller-supplied user property.
type identityHook struct{ mqtt.HookBase }

func (*identityHook) ID() string { return "courier-test-identity" }
func (*identityHook) Provides(b byte) bool {
	return b == mqtt.OnPublish || b == mqtt.OnConnectAuthenticate || b == mqtt.OnACLCheck
}
func (*identityHook) OnConnectAuthenticate(_ *mqtt.Client, _ packets.Packet) bool { return true }
func (*identityHook) OnACLCheck(_ *mqtt.Client, _ string, _ bool) bool            { return true }
func (*identityHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	props := make([]packets.UserProperty, 0, len(pk.Properties.User)+1)
	for _, p := range pk.Properties.User {
		if p.Key != "client_id" {
			props = append(props, p)
		}
	}
	pk.Properties.User = append(props, packets.UserProperty{Key: "client_id", Val: cl.ID})
	return pk, nil
}

func main() {
	ready := flag.String("ready-file", "", "JSON readiness file")
	flag.Parse()
	if *ready == "" {
		log.Fatal("--ready-file is required")
	}
	if err := run(*ready); err != nil {
		log.Fatal(err)
	}
}
func run(ready string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	broker := mqtt.New(&mqtt.Options{Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))})
	if err := broker.AddHook(&identityHook{}, nil); err != nil {
		return err
	}
	listener := listeners.NewTCP(listeners.Config{ID: "local", Address: "127.0.0.1:0"})
	if err := broker.AddListener(listener); err != nil {
		return err
	}
	defer broker.Close()
	if err := broker.Serve(); err != nil {
		return err
	}
	url := "mqtt://" + listener.Address()
	service := fmt.Sprintf("IntegrationEcho-%d", os.Getpid())
	probeID := "fixture-probe"
	probe := rpc.NewClient(rpc.WithClientTransport(transport.NewMQTTTransport(
		transport.WithBrokers(url), transport.WithClientID(probeID))),
		rpc.WithClientID(probeID), rpc.WithTimeout(3*time.Second))
	if err := probe.Connect(); err != nil {
		return err
	}
	defer probe.Close()
	for _, device := range []string{"device-a", "device-b"} {
		tp := transport.NewMQTTTransport(transport.WithBrokers(url), transport.WithClientID("fixture-"+device))
		server := rpc.NewServer(rpc.WithServerTransport(tp), rpc.WithServiceName(service), rpc.WithServerDeviceID(device))
		var mu sync.Mutex
		counts := map[string]int{}
		server.Register(rpc.ServiceInfo{ServiceName: service, Methods: []rpc.MethodInfo{
			{Cmd: 1, Name: "Echo", Handle: func(ctx *rpc.Context, payload []byte) ([]byte, error) {
				mu.Lock()
				counts[ctx.ClientID]++
				mu.Unlock()
				return payload, nil
			}},
			{Cmd: 2, Name: "Device", Handle: func(_ *rpc.Context, _ []byte) ([]byte, error) { return []byte(device), nil }},
			{Cmd: 3, Name: "Error", Handle: func(_ *rpc.Context, _ []byte) ([]byte, error) { return nil, rpc.NewError(70000, "integration error") }},
			{Cmd: 5, Name: "Callback", Handle: func(_ *rpc.Context, payload []byte) ([]byte, error) {
				var request struct {
					Service string
					Device  string
					Payload []byte
					Gzip    bool
				}
				if err := json.Unmarshal(payload, &request); err != nil {
					return nil, err
				}
				if request.Gzip {
					return probe.Call(ctx, request.Service, 1, request.Payload, rpc.WithTargetDevice(request.Device), rpc.WithCallCompression(codec.CompressionGZIP))
				}
				return probe.Call(ctx, request.Service, 1, request.Payload, rpc.WithTargetDevice(request.Device))
			}},
			{Cmd: 4, Name: "Count", Handle: func(ctx *rpc.Context, _ []byte) ([]byte, error) {
				mu.Lock()
				defer mu.Unlock()
				return json.Marshal(counts[ctx.ClientID])
			}},
		}})
		if err := server.Start(); err != nil {
			return err
		}
		defer server.Stop()
	}
	// A real roundtrip is the readiness barrier, including broker subscription ACKs.
	for _, device := range []string{"device-a", "device-b"} {
		result, err := probe.Call(ctx, service, 2, nil, rpc.WithTargetDevice(device), rpc.WithCallCompression(codec.CompressionGZIP))
		if err != nil {
			return fmt.Errorf("readiness for %s: %w", device, err)
		}
		if string(result) != device {
			return fmt.Errorf("readiness reached wrong device: %s", result)
		}
	}
	data, _ := json.Marshal(map[string]string{"url": url, "service": service})
	if err := os.WriteFile(ready+".tmp", data, 0600); err != nil {
		return err
	}
	if err := os.Rename(ready+".tmp", ready); err != nil {
		return err
	}
	log.Printf("READY %s %s", url, service)
	<-ctx.Done()
	return nil
}
