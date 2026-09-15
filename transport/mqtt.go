package transport

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session/state"
)

// MQTTTransport implements Transport using the paho v2 MQTT client library.
// It supports MQTT 5.0 with full access to publish properties including user properties.
type MQTTTransport struct {
	brokers              []string
	clientID             string
	username             string
	password             string
	autoReconnect        bool
	connectRetry         bool
	keepAlive            time.Duration
	cleanSession         bool
	defaultQoS           byte
	onConnect            func()
	onConnectionLost     func(err error)

	cm         *autopaho.ConnectionManager // nil before Connect
	cancelFunc context.CancelFunc

	mu            sync.RWMutex
	subs          map[string]MessageHandler
	globalHandler func(topic string, payload []byte, props MessageProperties)
}

// NewMQTTTransport creates a new MQTT transport with the given options.
func NewMQTTTransport(opts ...MQTTTransportOption) *MQTTTransport {
	t := &MQTTTransport{
		autoReconnect: true,
		connectRetry:  true,
		keepAlive:     60 * time.Second,
		cleanSession:  true,
		defaultQoS:    0,
		subs:          make(map[string]MessageHandler),
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

// Connect establishes a connection to the MQTT broker.
func (t *MQTTTransport) Connect() error {
	if len(t.brokers) == 0 {
		return fmt.Errorf("courier/transport: no brokers configured")
	}

	serverUrls := make([]*url.URL, 0, len(t.brokers))
	for _, b := range t.brokers {
		u, err := url.Parse(b)
		if err != nil {
			return fmt.Errorf("courier/transport: invalid broker url %q: %w", b, err)
		}
		serverUrls = append(serverUrls, u)
	}

	clientID := t.clientID
	if clientID == "" {
		clientID = fmt.Sprintf("courier_%d_%04x", time.Now().Unix(), rand.Intn(0xFFFF))
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.cancelFunc = cancel

	cfg := autopaho.ClientConfig{
		ServerUrls:                    serverUrls,
		KeepAlive:                     uint16(t.keepAlive.Seconds()),
		CleanStartOnInitialConnection: t.cleanSession,
		SessionExpiryInterval:         0,
		ReconnectBackoff:              func(attempt int) time.Duration {
			if attempt < 5 {
				return time.Duration(attempt+1) * 2 * time.Second
			}
			return 30 * time.Second
		},
		ConnectTimeout:                10 * time.Second,
		ConnectUsername:               t.username,
		ConnectPassword:               []byte(t.password),
		ClientConfig: paho.ClientConfig{
			ClientID: clientID,
			Session:  state.NewInMemory(),
			OnClientError: func(err error) {
				log.Printf("[courier/transport] client error: %v", err)
			},
			OnServerDisconnect: func(d *paho.Disconnect) {
				log.Printf("[courier/transport] server disconnect: reason=%d", d.ReasonCode)
			},
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connack *paho.Connack) {
			log.Println("[courier/transport] connected, resubscribing...")
			t.mu.Lock()
			t.cm = cm
			t.rebuildGlobalHandler()
			t.mu.Unlock()
			t.resubscribeAll(cm)
			if t.onConnect != nil {
				t.onConnect()
			}
		},
		OnConnectError: func(err error) {
			log.Printf("[courier/transport] connect error: %v", err)
		},
	}

	// Register message handler via OnPublishReceived.
	cfg.OnPublishReceived = []func(paho.PublishReceived) (bool, error){
		func(pr paho.PublishReceived) (bool, error) {
			t.mu.RLock()
			handler := t.globalHandler
			t.mu.RUnlock()

			if handler != nil {
				props := extractPublishProperties(pr.Packet)
				handler(pr.Packet.Topic, pr.Packet.Payload, props)
			}
			return true, nil
		},
	}

	cm, err := autopaho.NewConnection(ctx, cfg)
	if err != nil {
		cancel()
		return fmt.Errorf("courier/transport: connect failed: %w", err)
	}

	// Wait for initial connection.
	if err := cm.AwaitConnection(ctx); err != nil {
		cancel()
		return fmt.Errorf("courier/transport: await connection failed: %w", err)
	}

	return nil
}

// Close disconnects from the MQTT broker.
func (t *MQTTTransport) Close() error {
	if t.cancelFunc != nil {
		t.cancelFunc()
	}
	return nil
}

// Subscribe registers a handler for the given topic.
func (t *MQTTTransport) Subscribe(topic string, handler MessageHandler) error {
	t.mu.Lock()
	t.subs[topic] = handler
	t.rebuildGlobalHandler()
	cm := t.cm
	t.mu.Unlock()

	if cm != nil {
		if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
			Subscriptions: []paho.SubscribeOptions{
				{Topic: topic, QoS: t.defaultQoS},
			},
		}); err != nil {
			return fmt.Errorf("courier/transport: subscribe to %s failed: %w", topic, err)
		}
	}
	return nil
}

// Unsubscribe removes the subscription for the given topic.
func (t *MQTTTransport) Unsubscribe(topic string) error {
	t.mu.Lock()
	delete(t.subs, topic)
	t.rebuildGlobalHandler()
	cm := t.cm
	t.mu.Unlock()

	if cm != nil {
		if _, err := cm.Unsubscribe(context.Background(), &paho.Unsubscribe{
			Topics: []string{topic},
		}); err != nil {
			return fmt.Errorf("courier/transport: unsubscribe from %s failed: %w", topic, err)
		}
	}
	return nil
}

// Publish sends a message to the given topic.
func (t *MQTTTransport) Publish(topic string, payload []byte) error {
	t.mu.RLock()
	cm := t.cm
	t.mu.RUnlock()

	if cm == nil {
		return fmt.Errorf("courier/transport: not connected")
	}

	_, err := cm.Publish(context.Background(), &paho.Publish{
		QoS:     t.defaultQoS,
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		return fmt.Errorf("courier/transport: publish to %s failed: %w", topic, err)
	}
	return nil
}

func (t *MQTTTransport) rebuildGlobalHandler() {
	snapshot := make(map[string]MessageHandler, len(t.subs))
	for k, v := range t.subs {
		snapshot[k] = v
	}
	t.globalHandler = func(topic string, payload []byte, props MessageProperties) {
		if h, ok := snapshot[topic]; ok {
			h(topic, payload, props)
			return
		}
		// Shared subscription: broker delivers on the actual topic (e.g. "mrpc/request/Svc")
		// but the map key is "$share/Group/mrpc/request/Svc". Try stripping the prefix.
		if strings.HasPrefix(topic, "$share/") {
			return
		}
		for subTopic, h := range snapshot {
			if strings.HasPrefix(subTopic, "$share/") {
				// $share/{group}/{actualTopic}
				parts := strings.SplitN(subTopic, "/", 3)
				if len(parts) == 3 && parts[2] == topic {
					h(topic, payload, props)
					return
				}
			}
		}
	}
}

func (t *MQTTTransport) resubscribeAll(cm *autopaho.ConnectionManager) {
	t.mu.RLock()
	topics := make([]string, 0, len(t.subs))
	for topic := range t.subs {
		topics = append(topics, topic)
	}
	t.mu.RUnlock()

	if len(topics) == 0 {
		return
	}

	subs := make([]paho.SubscribeOptions, 0, len(topics))
	for _, topic := range topics {
		subs = append(subs, paho.SubscribeOptions{Topic: topic, QoS: t.defaultQoS})
	}

	if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{Subscriptions: subs}); err != nil {
		log.Printf("[courier/transport] resubscribe failed: %v", err)
	} else {
		log.Printf("[courier/transport] resubscribed to %d topics", len(topics))
	}
}

// extractPublishProperties extracts MQTT 5.0 user properties from a paho Publish packet.
// EMQX injects the publisher's clientID and other metadata here.
func extractPublishProperties(pub *paho.Publish) MessageProperties {
	props := make(MessageProperties)
	if pub.Properties != nil {
		for _, up := range pub.Properties.User {
			props[up.Key] = up.Value
		}
	}
	return props
}
