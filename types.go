package xbus

import (
	"sync"
	"time"
)

// Message is the envelope traveling the bus. The Payload is encoded via Codec.
type Message struct {
	ID         string            // Unique message identifier (transport may assign if empty)
	Name       string            // Logical event name for routing/metrics
	Payload    []byte            // Encoded bytes of the event
	Metadata   map[string]string // Headers/tracing/tenancy/etc
	ProducedAt time.Time         // Production timestamp (from injected clock)
}

// PublishEvent describes a single event in a batch publish call.
type PublishEvent struct {
	Name    string
	Payload any
	Meta    map[string]string
}

// EventType enumerates internal lifecycle events for Observer pattern.
type EventType string

const (
	PublishStart EventType = "publish_start"
	PublishDone  EventType = "publish_done"
	ConsumeStart EventType = "consume_start"
	ConsumeDone  EventType = "consume_done"
	Ack          EventType = "ack"
	Nack         EventType = "nack"
	Error        EventType = "error"
)

// Event carries telemetry for observers.
type Event struct {
	Type      EventType
	Topic     string
	Group     string
	MessageID string
	EventName string
	Duration  time.Duration
	Err       error

	// Internal: attached for async dispatch
	observers []Observer
}

// PoolStats returns telemetry about the observer pool.
type PoolStats struct {
	Dropped      uint64 // Events dropped due to full buffer
	Processed    uint64 // Events successfully processed
	ActiveEvents int    // Current queue depth
	Workers      int    // Number of dispatch goroutines
	BufferSize   int    // Channel capacity
}

// Metrics defines observable telemetry for the bus.
type Metrics struct {
	Published           uint64
	Consumed            uint64
	Acked               uint64
	Nacked              uint64
	Errors              uint64
	EventsDropped       uint64
	AvgProcessingTimeMs float64
}

// HealthStatus indicates bus health for Kubernetes probes.
type HealthStatus struct {
	Status    string // "healthy", "degraded", "unhealthy"
	Metrics   Metrics
	Timestamp time.Time
	Message   string
}

// eventPool reduces allocations for event objects (optional optimization)
var eventPool = sync.Pool{
	New: func() interface{} { return &Event{} },
}
