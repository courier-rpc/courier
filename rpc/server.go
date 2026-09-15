package rpc

import (
	"fmt"
	"log"

	"github.com/simpossible/courier/codec"
	"github.com/simpossible/courier/transport"
)

// Server receives RPC requests over MQTT, dispatches them to registered handlers,
// and sends responses back to the requesting client.
type Server struct {
	tp              transport.Transport
	serviceName     string
	deviceID        string
	sharedSubscribe bool
	interceptors    []Interceptor

	dispatcher *dispatcher
}

// NewServer creates a new RPC server with the given options.
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		sharedSubscribe: true,
		dispatcher:      newDispatcher(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Register adds a service's methods to the server's dispatch table.
func (s *Server) Register(info ServiceInfo) {
	chain := ChainInterceptors(s.interceptors...)

	for i, m := range info.Methods {
		wrapped := m.Handle
		if len(s.interceptors) > 0 {
			wrapped = chain(m.Handle)
		}
		info.Methods[i].Handle = wrapped
	}

	s.dispatcher.register(info)
}

// Start connects the transport and subscribes to the request topic.
func (s *Server) Start() error {
	if s.tp == nil {
		return fmt.Errorf("courier/rpc: server has no transport")
	}
	if s.serviceName == "" {
		return fmt.Errorf("courier/rpc: server has no service name")
	}

	if s.deviceID != "" {
		if err := validateDeviceID(s.deviceID); err != nil {
			return err
		}
	}

	// Connect transport first.
	if err := s.tp.Connect(); err != nil {
		return fmt.Errorf("courier/rpc: transport connect failed: %w", err)
	}

	handler := s.makeMessageHandler()
	subscribed := []string{}
	for _, topic := range s.requestTopics() {
		if err := s.tp.Subscribe(topic, handler); err != nil {
			// Also remove the failed subscription: transports may store it before returning an error.
			_ = s.tp.Unsubscribe(topic)
			for _, previous := range subscribed {
				_ = s.tp.Unsubscribe(previous)
			}
			return fmt.Errorf("courier/rpc: subscribe to %s failed: %w", topic, err)
		}
		subscribed = append(subscribed, topic)
		log.Printf("[courier/rpc] server subscribed to %s", topic)
	}
	return nil
}

// Stop unsubscribes and closes the transport.
func (s *Server) Stop() error {
	if s.tp == nil {
		return nil
	}
	for _, topic := range s.requestTopics() {
		_ = s.tp.Unsubscribe(topic)
	}
	return s.tp.Close()
}

func (s *Server) requestTopic() string {
	if s.sharedSubscribe {
		return SharedRequestTopic(s.serviceName)
	}
	return RequestTopic(s.serviceName)
}

func (s *Server) makeMessageHandler() transport.MessageHandler {
	return func(topic string, payload []byte, props transport.MessageProperties) {
		frame, err := codec.DecodeRequest(payload)
		if err != nil {
			log.Printf("[courier/rpc] failed to decode request: %v", err)
			return
		}

		// Extract ClientID from transport properties (injected by EMQX/broker).
		clientID := ""
		if props != nil {
			clientID = props["client_id"]
		}

		ctx := &Context{
			ClientID:  clientID,
			RequestID: frame.RequestID,
		}

		respPayload, dispatchErr := s.dispatcher.dispatch(frame.Cmd, ctx, frame.Payload)

		var respBytes []byte
		var respCode uint32
		if dispatchErr != nil {
			respCode = errorCode(dispatchErr)
			respBytes = []byte(dispatchErr.Error())
		} else {
			respBytes = respPayload
		}

		var respFrame []byte
		if frame.Version == codec.CompressionProtocolVersion {
			respFrame, err = codec.EncodeResponseWithCompression(ctx.RequestID, respCode, respBytes, frame.Compression)
			if err != nil {
				// Return a bounded, uncompressed v2 error rather than timing out on oversized output.
				respFrame, _ = codec.EncodeResponseWithCompression(ctx.RequestID, 2, []byte(err.Error()), codec.CompressionNone)
			}
		} else {
			respFrame = codec.EncodeResponse(ctx.RequestID, respCode, respBytes)
		}
		respTopic := ResponseTopic(ctx.ClientID)

		if pubErr := s.tp.Publish(respTopic, respFrame); pubErr != nil {
			log.Printf("[courier/rpc] failed to publish response to %s: %v", respTopic, pubErr)
		}
	}
}

// errorCode extracts a uint32 code from an error.
func errorCode(err error) uint32 {
	if rpcErr, ok := err.(*Error); ok {
		return uint32(rpcErr.Code)
	}
	return 2
}

func (s *Server) requestTopics() []string {
	topics := []string{s.requestTopic()}
	if s.deviceID != "" {
		topics = append(topics, DirectRequestTopic(s.serviceName, s.deviceID))
	}
	return topics
}
