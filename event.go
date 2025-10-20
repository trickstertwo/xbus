package xbus

import (
	"sync"
	"time"
)

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

// eventPool reduces allocations for event objects (optional optimization)
var eventPool = sync.Pool{
	New: func() interface{} { return &Event{} },
}
