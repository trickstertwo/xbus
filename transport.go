package xbus

import (
	"context"
	"errors"
	"sync"
)

// Delivery encapsulates a received message with Ack/Nack semantics.
type Delivery interface {
	Message() *Message
	Ack(ctx context.Context) error
	Nack(ctx context.Context, reason error) error
}

// Handler processes a single message. Return error to trigger Nack/Retry.
type Handler func(ctx context.Context, msg *Message) error

// Middleware composes processing concerns around a Handler.
type Middleware func(next Handler) Handler

// Subscription represents an active subscription that can be closed.
type Subscription interface {
	Close() error
}

// Transport is the Strategy interface for message brokers/backends.
type Transport interface {
	// Publish sends messages to a topic/stream.
	Publish(ctx context.Context, topic string, msgs ...*Message) error
	// Subscribe binds a handler to a topic/stream within a consumer group.
	// The transport should drive delivery in background and honor ctx.
	Subscribe(ctx context.Context, topic, group string, handler func(Delivery)) (Subscription, error)
	// Close releases resources.
	Close(ctx context.Context) error
}

// TransportFactory constructs transports from a config blob.
type TransportFactory func(cfg map[string]any) (Transport, error)

var (
	transportRegistryMu sync.RWMutex
	transportRegistry   = map[string]TransportFactory{}
)

// RegisterTransport registers a backend adapter.
func RegisterTransport(name string, factory TransportFactory) error {
	if name == "" {
		return errors.New("transport name must not be empty")
	}
	if factory == nil {
		return errors.New("transport factory must not be nil")
	}
	transportRegistryMu.Lock()
	transportRegistry[name] = factory
	transportRegistryMu.Unlock()
	return nil
}

// NewTransport constructs a transport by name with config.
func NewTransport(name string, cfg map[string]any) (Transport, error) {
	transportRegistryMu.RLock()
	f, ok := transportRegistry[name]
	transportRegistryMu.RUnlock()
	if !ok {
		return nil, ErrUnknownTransport{name: name}
	}
	return f(cfg)
}
