package xbus

import (
	"github.com/trickstertwo/xlog"
)

// Observer receives bus lifecycle events. Implementations should be non-blocking.
type Observer interface {
	OnEvent(e Event)
}

// ObserverFunc is an Adapter that lets a plain function satisfy Observer.
type ObserverFunc func(e Event)

func (f ObserverFunc) OnEvent(e Event) { f(e) }

// LoggingObserver is an Adapter that emits BusEvents via xlog.
type LoggingObserver struct {
	Logger *xlog.Logger
}

func (o LoggingObserver) OnEvent(e Event) {
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
	case Error, Nack:
		ev.Warn().Err(e.Err).Msg("xbus event")
	default:
		if e.Duration > 0 {
			ev = ev.With(xlog.Dur("duration", e.Duration))
		}
		ev.Debug().Msg("xbus event")
	}
}
