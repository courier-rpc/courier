package rpc

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/simpossible/courier/codec"
	"github.com/simpossible/courier/transport"
)

type pendingCall struct {
	respChan chan []byte
	timer    *time.Timer
}

// Client sends RPC requests over MQTT and matches responses to pending calls.
type Client struct {
	tp            transport.Transport
	clientID      string
	timeout       time.Duration
	retryCount    int
	retryInterval time.Duration
	retryBackoff  float64
	interceptors  []Interceptor

	mu      sync.RWMutex
	pending map[[16]byte]*pendingCall
}

// NewClient creates a new RPC client with the given options.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		timeout:       10 * time.Second,
		retryCount:    0,
		retryInterval: 1 * time.Second,
		retryBackoff:  1.5,
		pending:       make(map[[16]byte]*pendingCall),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Connect establishes the MQTT connection and subscribes to the response topic.
func (c *Client) Connect() error {
	if c.tp == nil {
		return fmt.Errorf("courier/rpc: client has no transport")
	}
	if c.clientID == "" {
		return fmt.Errorf("courier/rpc: client has no client ID")
	}

	// Connect transport first.
	if err := c.tp.Connect(); err != nil {
		return fmt.Errorf("courier/rpc: transport connect failed: %w", err)
	}

	respTopic := ResponseTopic(c.clientID)
	if err := c.tp.Subscribe(respTopic, c.handleResponse); err != nil {
		return fmt.Errorf("courier/rpc: subscribe to response topic failed: %w", err)
	}

	log.Printf("[courier/rpc] client subscribed to %s", respTopic)
	return nil
}

// Close unsubscribes from the response topic and cleans up pending calls.
func (c *Client) Close() error {
	if c.tp == nil {
		return nil
	}

	respTopic := ResponseTopic(c.clientID)
	_ = c.tp.Unsubscribe(respTopic)

	c.mu.Lock()
	for id, call := range c.pending {
		call.timer.Stop()
		select {
		case call.respChan <- nil:
		default:
		}
		delete(c.pending, id)
	}
	c.mu.Unlock()

	return c.tp.Close()
}

// Call sends an RPC request and waits for the response or timeout.
func (c *Client) Call(ctx context.Context, serviceName string, cmd uint32, payload []byte, opts ...CallOption) ([]byte, error) {
	var options callOptions
	for _, opt := range opts {
		opt(&options)
	}
	reqTopic := RequestTopic(serviceName)
	if options.targetDeviceID != nil {
		if err := validateDeviceID(*options.targetDeviceID); err != nil {
			return nil, err
		}
		reqTopic = DirectRequestTopic(serviceName, *options.targetDeviceID)
	}
	requestID, err := newRequestID()
	if err != nil {
		return nil, fmt.Errorf("courier/rpc: generate request ID: %w", err)
	}

	call := &pendingCall{
		respChan: make(chan []byte, 1),
	}

	c.mu.Lock()
	c.pending[requestID] = call
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if pc, ok := c.pending[requestID]; ok {
			pc.timer.Stop()
			delete(c.pending, requestID)
		}
		c.mu.Unlock()
	}()

	call.timer = time.AfterFunc(c.timeout, func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()

		select {
		case call.respChan <- nil:
		default:
		}
	})

	// ClientID is NOT in the frame — the broker injects it via message properties.
	reqBytes := codec.EncodeRequest(cmd, requestID, nil, payload)

	if pubErr := c.tp.Publish(reqTopic, reqBytes); pubErr != nil {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()
		call.timer.Stop()
		return nil, fmt.Errorf("courier/rpc: publish failed: %w", pubErr)
	}

	// Retry loop.
	interval := c.retryInterval
	for attempt := 0; attempt < c.retryCount; attempt++ {
		select {
		case resp := <-call.respChan:
			return c.handleCallResult(resp)
		case <-ctx.Done():
			return nil, ErrCanceled
		case <-time.After(interval):
			_ = c.tp.Publish(reqTopic, reqBytes)
			interval = time.Duration(float64(interval) * c.retryBackoff)
		}
	}

	select {
	case resp := <-call.respChan:
		return c.handleCallResult(resp)
	case <-ctx.Done():
		return nil, ErrCanceled
	}
}

func (c *Client) handleCallResult(resp []byte) ([]byte, error) {
	if resp == nil {
		return nil, ErrTimeout
	}
	return resp, nil
}

func (c *Client) handleResponse(topic string, payload []byte, props transport.MessageProperties) {
	frame, err := codec.DecodeResponse(payload)
	if err != nil {
		log.Printf("[courier/rpc] failed to decode response: %v", err)
		return
	}

	c.mu.Lock()
	call, ok := c.pending[frame.RequestID]
	if ok {
		delete(c.pending, frame.RequestID)
		call.timer.Stop()
	}
	c.mu.Unlock()

	if !ok {
		return
	}

	var result []byte
	if frame.Code != codec.ResponseCodeOK {
		result = nil
	} else {
		result = frame.Payload
	}

	select {
	case call.respChan <- result:
	default:
		log.Printf("[courier/rpc] response channel full for request %x", frame.RequestID)
	}
}

func newRequestID() ([16]byte, error) {
	var id [16]byte
	_, err := rand.Read(id[:])
	return id, err
}
