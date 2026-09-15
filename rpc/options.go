package rpc

import (
	"time"

	"github.com/simpossible/courier/transport"
)

// --- Server Options ---

// ServerOption configures an RPC Server.
type ServerOption func(*Server)

// WithServerTransport sets the transport layer for the server.
func WithServerTransport(tp transport.Transport) ServerOption {
	return func(s *Server) {
		s.tp = tp
	}
}

// WithServiceName sets the service name used for MQTT topic routing.
func WithServiceName(name string) ServerOption {
	return func(s *Server) {
		s.serviceName = name
	}
}

// WithSharedSubscribe enables or disables $share shared subscription.
// When true (default), multiple server instances form a load-balanced group.
func WithSharedSubscribe(enabled bool) ServerOption {
	return func(s *Server) {
		s.sharedSubscribe = enabled
	}
}

// WithServerInterceptors adds interceptors to the server's handler chain.
func WithServerInterceptors(is ...Interceptor) ServerOption {
	return func(s *Server) {
		s.interceptors = append(s.interceptors, is...)
	}
}

// --- Client Options ---

// ClientOption configures an RPC Client.
type ClientOption func(*Client)

// WithClientTransport sets the transport layer for the client.
func WithClientTransport(tp transport.Transport) ClientOption {
	return func(c *Client) {
		c.tp = tp
	}
}

// WithClientID sets the client identifier. This must match the MQTT connection ClientID.
// It is used for response topic routing and as the session key for authentication.
func WithClientID(id string) ClientOption {
	return func(c *Client) {
		c.clientID = id
	}
}

// WithTimeout sets the per-request timeout. Default: 10s.
func WithTimeout(d time.Duration) ClientOption {
	return func(c *Client) {
		c.timeout = d
	}
}

// WithRetry configures client-side retry behavior.
// count is the maximum number of retries (0 = no retry).
// interval is the initial delay between retries.
// backoff is the multiplier applied to interval after each retry (e.g. 1.5).
func WithRetry(count int, interval time.Duration, backoff float64) ClientOption {
	return func(c *Client) {
		c.retryCount = count
		c.retryInterval = interval
		c.retryBackoff = backoff
	}
}

// WithClientInterceptors adds interceptors to the client's call chain.
func WithClientInterceptors(is ...Interceptor) ClientOption {
	return func(c *Client) {
		c.interceptors = append(c.interceptors, is...)
	}
}

// WithServerDeviceID adds a direct subscription for this stable device ID.
// Use a unique ID per service instance; omitting it preserves shared-only routing.
func WithServerDeviceID(id string) ServerOption {
	return func(s *Server) { s.deviceID = id }
}

// CallOption configures routing for an individual call.
type CallOption func(*callOptions)
type callOptions struct{ targetDeviceID *string }

// WithTargetDevice directs a call to one device, with no shared-group fallback.
func WithTargetDevice(id string) CallOption {
	return func(o *callOptions) { o.targetDeviceID = &id }
}
