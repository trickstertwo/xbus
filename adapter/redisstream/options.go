package redisstream

import (
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Option configures the xbus.Bus construction when calling Use.
type Option func(*xbus.BusBuilder)

// WithLogger injects a custom xlog logger.
func WithLogger(l *xlog.Logger) Option {
	return func(b *xbus.BusBuilder) { b.WithLogger(l) }
}

// WithClock injects a custom xclock clock.
func WithClock(c xclock.Clock) Option {
	return func(b *xbus.BusBuilder) { b.WithClock(c) }
}

// WithCodec selects a codec by name (default: json).
func WithCodec(name string) Option {
	return func(b *xbus.BusBuilder) { b.WithCodec(name) }
}

// WithMiddleware adds processing middlewares.
func WithMiddleware(mw ...xbus.Middleware) Option {
	return func(b *xbus.BusBuilder) { b.WithMiddleware(mw...) }
}

// WithAckTimeout sets acks/nacks timeout.
func WithAckTimeout(d time.Duration) Option {
	return func(b *xbus.BusBuilder) { b.WithAckTimeout(d) }
}

// WithObserver attaches observers for lifecycle events.
func WithObserver(obs ...xbus.Observer) Option {
	return func(b *xbus.BusBuilder) { b.WithObserver(obs...) }
}
