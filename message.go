package xbus

import (
	"time"
)

// Message is the envelope traveling the bus. The Payload is encoded via Codec.
type Message struct {
	// ID is a unique message identifier (transport may assign if empty).
	ID string
	// Name is the logical event name, useful for routing/metrics.
	Name string
	// Payload is the encoded bytes of the event.
	Payload []byte
	// Metadata is a bag for headers/tracing/tenancy/etc.
	Metadata map[string]string
	// ProducedAt is the production timestamp (from injected clock).
	ProducedAt time.Time
}
