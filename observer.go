package xbus

import (
	"time"

	"github.com/trickstertwo/xlog"
)

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

// LoggingObserver is an Adapter that emits BusEvents via xlog.
type LoggingObserver struct {
	Logger *xlog.Logger
}

func (o LoggingObserver) OnBusEvent(e BusEvent) {
	if o.Logger == nil {
		return
	}
	ev := o.Logger.With(
		xlog.Str("type", string(e.Type)),
		xlog.Str("topic", e.Topic),
		xlog.Str("group", e.Group),
		xlog.Str("message_id", e.MessageID),
		xlog.Str("event_name", e.EventName),
	)
	switch e.Type {
	case EventError, EventNack:
		ev.Warn().Err(e.Err).Msg("xbus event")
	default:
		if e.Duration > 0 {
			ev = ev.With(xlog.Dur("duration", e.Duration))
		}
		ev.Debug().Msg("xbus event")
	}
}
