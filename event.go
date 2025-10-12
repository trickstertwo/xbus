package xbus

import "time"

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
}
