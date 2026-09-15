package rpc

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/simpossible/courier/transport"
)

// routingTransport models exact topic delivery and one recipient per shared group.
type routingTransport struct {
	subscriptions map[string]transport.MessageHandler
	peers         *[]*routingTransport
	id            string
	published     []string
	fail          string
}

func (m *routingTransport) Connect() error { return nil }
func (m *routingTransport) Close() error   { return nil }
func (m *routingTransport) Subscribe(topic string, h transport.MessageHandler) error {
	m.subscriptions[topic] = h
	if topic == m.fail {
		return fmt.Errorf("subscribe failed")
	}
	return nil
}
func (m *routingTransport) Unsubscribe(topic string) error {
	delete(m.subscriptions, topic)
	return nil
}
func (m *routingTransport) Publish(topic string, payload []byte) error {
	m.published = append(m.published, topic)
	shared := false
	for _, peer := range *m.peers {
		for sub, h := range peer.subscriptions {
			if sub == topic {
				h(topic, payload, transport.MessageProperties{"client_id": m.id})
			}
			if strings.HasPrefix(sub, "$share/") && strings.SplitN(sub, "/", 3)[2] == topic && !shared {
				shared = true
				h(topic, payload, transport.MessageProperties{"client_id": m.id})
			}
		}
	}
	return nil
}
func newRoutingTransport(peers *[]*routingTransport, id string) *routingTransport {
	m := &routingTransport{subscriptions: map[string]transport.MessageHandler{}, peers: peers, id: id}
	*peers = append(*peers, m)
	return m
}

func TestDirectAndSharedCalls(t *testing.T) {
	var peers []*routingTransport
	hits := map[string]int{}
	for _, id := range []string{"device-a", "device-b"} {
		tp := newRoutingTransport(&peers, id)
		srv := NewServer(WithServerTransport(tp), WithServiceName("Status"), WithServerDeviceID(id))
		srv.Register(ServiceInfo{ServiceName: "Status", Methods: []MethodInfo{{Cmd: 1, Name: "Get", Handle: func(ctx *Context, payload []byte) ([]byte, error) {
			hits[id]++
			if ctx.ClientID != "backend" {
				t.Errorf("caller = %q", ctx.ClientID)
			}
			return []byte(id), nil
		}}}})
		if err := srv.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = srv.Stop()
			if len(tp.subscriptions) != 0 {
				t.Error("subscriptions leaked")
			}
		}()
	}
	tp := newRoutingTransport(&peers, "backend")
	client := NewClient(WithClientTransport(tp), WithClientID("backend"), WithTimeout(20*time.Millisecond), WithRetry(1, time.Millisecond, 1))
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	resp, err := client.Call(context.Background(), "Status", 1, nil, WithTargetDevice("device-b"))
	if err != nil || string(resp) != "device-b" || hits["device-a"] != 0 || hits["device-b"] != 1 {
		t.Fatalf("direct: %q %v hits=%v", resp, err, hits)
	}
	if _, err = client.Call(context.Background(), "Status", 1, nil); err != nil {
		t.Fatal(err)
	}
	if hits["device-a"]+hits["device-b"] != 2 {
		t.Fatalf("shared delivery: %v", hits)
	}
	start := len(tp.published)
	if _, err = client.Call(context.Background(), "Status", 1, nil, WithTargetDevice("offline")); err != ErrTimeout {
		t.Fatalf("offline: %v", err)
	}
	for _, topic := range tp.published[start:] {
		if topic != DirectRequestTopic("Status", "offline") {
			t.Fatalf("retry fell back: %s", topic)
		}
	}
	if hits["device-a"]+hits["device-b"] != 2 {
		t.Fatal("offline request delivered to another device")
	}
	for _, id := range []string{"", "a/b", "+", "#", "a\x00b"} {
		if _, err := client.Call(context.Background(), "Status", 1, nil, WithTargetDevice(id)); err == nil {
			t.Errorf("accepted invalid ID %q", id)
		}
	}
}

func TestDirectSubscriptionRollback(t *testing.T) {
	var peers []*routingTransport
	tp := newRoutingTransport(&peers, "a")
	tp.fail = DirectRequestTopic("Status", "a")
	s := NewServer(WithServerTransport(tp), WithServiceName("Status"), WithServerDeviceID("a"))
	if err := s.Start(); err == nil {
		t.Fatal("expected failure")
	}
	if len(tp.subscriptions) != 0 {
		t.Fatalf("leaked subscriptions: %v", tp.subscriptions)
	}
}

func TestRequestTopicsCompatibility(t *testing.T) {
	for _, shared := range []bool{true, false} {
		s := NewServer(WithServiceName("Status"), WithSharedSubscribe(shared))
		expected := RequestTopic("Status")
		if shared {
			expected = SharedRequestTopic("Status")
		}
		if got := s.requestTopics(); len(got) != 1 || got[0] != expected {
			t.Fatal(got)
		}
		WithServerDeviceID("a")(s)
		if got := s.requestTopics(); len(got) != 2 || got[0] != expected || got[1] != DirectRequestTopic("Status", "a") {
			t.Fatal(got)
		}
	}
}
