package xbus

import (
	"context"
)

// Handler processes a single message. Return error to trigger Nack/Retry.
type Handler func(ctx context.Context, msg *Message) error

// Middleware composes processing concerns around a Handler.
type Middleware func(next Handler) Handler

// Subscription represents an active subscription that can be closed.
type Subscription interface {
	Close() error
}

// Delivery encapsulates a received message with Ack/Nack semantics.
type Delivery interface {
	Message() *Message
	Ack(ctx context.Context) error
	Nack(ctx context.Context, reason error) error
}

// Transport is the Strategy interface for message brokers/backends.
type Transport interface {
	Publish(ctx context.Context, topic string, msgs ...*Message) error
	Subscribe(ctx context.Context, topic, group string, handler func(Delivery)) (Subscription, error)
	Close(ctx context.Context) error
}

// Codec is the Strategy for encoding/decoding payloads on the wire.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// Observer receives bus lifecycle events. Implementations should be non-blocking.
type Observer interface {
	OnEvent(e Event)
}

// HealthChecker provides health status for production monitoring.
type HealthChecker interface {
	Health(ctx context.Context) HealthStatus
}

// API represents the complete xbus surface for extensibility.
type API interface {
	Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error
	PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error
	Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error)
	Close(ctx context.Context) error
	GetMetrics() Metrics
	Health(ctx context.Context) HealthStatus
	AddObserver(obs Observer)
	RemoveObserver(obs Observer)
}

var _ API = (*Bus)(nil)
