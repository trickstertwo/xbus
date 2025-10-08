package xbus

import "time"

// BusEventType enumerates internal lifecycle events for Observer pattern.
type BusEventType string

const (
	EventPublishStart BusEventType = "publish_start"
	EventPublishDone  BusEventType = "publish_done"
	EventConsumeStart BusEventType = "consume_start"
	EventConsumeDone  BusEventType = "consume_done"
	EventAck          BusEventType = "ack"
	EventNack         BusEventType = "nack"
	EventError        BusEventType = "error"
)

// BusEvent carries telemetry for observers.
type BusEvent struct {
	Type      BusEventType
	Topic     string
	Group     string
	MessageID string
	EventName string
	Duration  time.Duration
	Err       error
}

// Observer receives bus lifecycle events. Implementations should be non-blocking.
type Observer interface {
	OnBusEvent(e BusEvent)
}

// ObserverFunc is an Adapter that lets a plain function satisfy Observer.
type ObserverFunc func(e BusEvent)

func (f ObserverFunc) OnBusEvent(e BusEvent) { f(e) }
