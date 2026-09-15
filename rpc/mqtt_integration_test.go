package rpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpossible/courier/codec"
	"github.com/simpossible/courier/rpc"
	"github.com/simpossible/courier/transport"
)

// Records bytes while delegating every operation to a real MQTT connection.
type wireTransport struct {
	transport.Transport
	mu             sync.Mutex
	sent, received []byte
}

func (w *wireTransport) Publish(topic string, payload []byte) error {
	w.mu.Lock()
	w.sent = bytes.Clone(payload)
	w.mu.Unlock()
	return w.Transport.Publish(topic, payload)
}
func (w *wireTransport) Subscribe(topic string, handler transport.MessageHandler) error {
	return w.Transport.Subscribe(topic, func(topic string, payload []byte, props transport.MessageProperties) {
		w.mu.Lock()
		w.received = bytes.Clone(payload)
		w.mu.Unlock()
		handler(topic, payload, props)
	})
}
func (w *wireTransport) frames() ([]byte, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.sent), bytes.Clone(w.received)
}

func TestMQTTIntegration(t *testing.T) {
	url := os.Getenv("COURIER_MQTT_URL")
	if url == "" {
		t.Skip("run scripts/test-integration.sh --suite go")
	}
	service := os.Getenv("COURIER_INTEGRATION_SERVICE")
	if service == "" {
		t.Fatal("COURIER_INTEGRATION_SERVICE is required")
	}
	id := fmt.Sprintf("go-integration-%d", time.Now().UnixNano())
	wire := &wireTransport{Transport: transport.NewMQTTTransport(transport.WithBrokers(url), transport.WithClientID(id))}
	client := rpc.NewClient(rpc.WithClientTransport(wire), rpc.WithClientID(id), rpc.WithTimeout(time.Second))
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	count := func(device string) int {
		t.Helper()
		data, err := client.Call(ctx, service, 4, nil, rpc.WithTargetDevice(device))
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err = json.Unmarshal(data, &n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	expectedB := 0
	for _, mode := range []string{"legacy", "none", "gzip"} {
		t.Run(mode, func(t *testing.T) {
			opts := []rpc.CallOption{rpc.WithTargetDevice("device-b")}
			version := codec.ProtocolVersion
			algorithm := codec.CompressionNone
			if mode != "legacy" {
				version = 2
				if mode == "gzip" {
					algorithm = codec.CompressionGZIP
				}
				opts = append(opts, rpc.WithCallCompression(algorithm))
			}
			for _, payload := range [][]byte{[]byte(strings.Repeat("设备状态 online;", 4096)), {}} {
				result, err := client.Call(ctx, service, 1, payload, opts...)
				if err != nil || !bytes.Equal(result, payload) {
					t.Fatalf("roundtrip: %v", err)
				}
				expectedB++
				request, response := wire.frames()
				req, err := codec.DecodeRequest(request)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := codec.DecodeResponseWithVersion(response, version)
				if err != nil {
					t.Fatal(err)
				}
				if req.Version != version || req.Compression != algorithm || resp.Compression != algorithm || req.RequestID != resp.RequestID {
					t.Fatal("wire header mismatch")
				}
				if mode == "gzip" && len(payload) > 0 && (len(request) >= len(payload) || len(response) >= len(payload)) {
					t.Fatal("payload was not compressed on wire")
				}
			}
		})
	}
	t.Run("exact-device-and-shared-delivery", func(t *testing.T) {
		for _, device := range []string{"device-a", "device-b"} {
			result, err := client.Call(ctx, service, 2, nil, rpc.WithTargetDevice(device), rpc.WithCallCompression(codec.CompressionGZIP))
			if err != nil || string(result) != device {
				t.Fatalf("wrong device: %s %v", result, err)
			}
		}
		if count("device-a") != 0 || count("device-b") != expectedB {
			t.Fatal("direct call reached other device or duplicated")
		}
		for i := 0; i < 5; i++ {
			if _, err := client.Call(ctx, service, 1, []byte("shared"), rpc.WithCallCompression(codec.CompressionGZIP)); err != nil {
				t.Fatal(err)
			}
		}
		if count("device-a")+count("device-b") != expectedB+5 {
			t.Fatal("shared call was lost or broadcast")
		}
	})
	t.Run("mixed-concurrent-calls", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				opts := []rpc.CallOption{rpc.WithTargetDevice("device-b")}
				if i%2 == 0 {
					opts = append(opts, rpc.WithCallCompression(codec.CompressionGZIP))
				}
				payload := []byte(fmt.Sprintf("parallel-%d", i))
				result, err := client.Call(ctx, service, 1, payload, opts...)
				if err != nil || !bytes.Equal(result, payload) {
					t.Errorf("parallel call: %v", err)
				}
			}(i)
		}
		wg.Wait()
		if count("device-a")+count("device-b") != expectedB+5+12 {
			t.Fatal("concurrent delivery count mismatch")
		}
	})
	t.Run("compressed-error", func(t *testing.T) {
		_, err := client.Call(ctx, service, 3, nil, rpc.WithTargetDevice("device-b"), rpc.WithCallCompression(codec.CompressionGZIP))
		var rpcError *rpc.Error
		if !errors.As(err, &rpcError) || rpcError.Code != 70000 {
			t.Fatalf("expected remote error 70000, got %v", err)
		}
	})
	t.Run("offline-does-not-fallback", func(t *testing.T) {
		before := count("device-a") + count("device-b")
		_, err := client.Call(ctx, service, 1, []byte("offline"), rpc.WithTargetDevice("missing"), rpc.WithCallCompression(codec.CompressionGZIP))
		if !errors.Is(err, rpc.ErrTimeout) {
			t.Fatalf("expected timeout, got %v", err)
		}
		if count("device-a")+count("device-b") != before {
			t.Fatal("offline call fell back to a live device")
		}
	})
}
