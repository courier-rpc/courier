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

type callResult struct {
	payload []byte
	err     error
}

type pendingCall struct {
	respChan chan callResult
	version  uint16
	timer    *time.Timer
}

// Client sends RPC requests over MQTT and matches responses to pending calls.
type Client struct {
	tp            transport.Transport
	clientID      string
	compression   *codec.Compression
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
		case call.respChan <- callResult{err: ErrTimeout}:
		default:
		}
		delete(c.pending, id)
	}
	c.mu.Unlock()

	return c.tp.Close()
}

// Call sends an RPC request and waits for the response or timeout.
func (c *Client) Call(ctx context.Context, serviceName string, cmd uint32, payload []byte, opts ...CallOption) ([]byte, error) {
	options := callOptions{compression: c.compression}
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

	// Encode before registering the call so invalid compression cannot leak pending calls.
	version := codec.ProtocolVersion
	var reqBytes []byte
	if options.compression != nil {
		version = codec.CompressionProtocolVersion
		reqBytes, err = codec.EncodeRequestWithCompression(cmd, requestID, nil, payload, *options.compression)
		if err != nil {
			return nil, err
		}
	} else {
		reqBytes = codec.EncodeRequest(cmd, requestID, nil, payload)
	}
	call := &pendingCall{
		version:  version,
		respChan: make(chan callResult, 1),
	}

	defer func() {
		c.mu.Lock()
		if pc, ok := c.pending[requestID]; ok {
			pc.timer.Stop()
			delete(c.pending, requestID)
		}
		c.mu.Unlock()
	}()

	c.mu.Lock()
	call.timer = time.AfterFunc(c.timeout, func() {
		c.mu.Lock()
		delete(c.pending, requestID)
		c.mu.Unlock()

		select {
		case call.respChan <- callResult{err: ErrTimeout}:
		default:
		}
	})

	c.pending[requestID] = call
	c.mu.Unlock()

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

func (c *Client) handleCallResult(resp callResult) ([]byte, error) {
	return resp.payload, resp.err
}

func (c *Client) handleResponse(topic string, payload []byte, props transport.MessageProperties) {
	// The request ID has the same offset in v1 and v2 responses.
	if len(payload) < 20 {
		return
	}
	var requestID [16]byte
	copy(requestID[:], payload[4:20])
	c.mu.RLock()
	pending := c.pending[requestID]
	c.mu.RUnlock()
	if pending == nil {
		return
	}
	frame, err := codec.DecodeResponseWithVersion(payload, pending.version)
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

	result := callResult{payload: frame.Payload}
	if frame.Code != codec.ResponseCodeOK {
		result = callResult{err: NewError(int32(frame.Code), string(frame.Payload))}
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
