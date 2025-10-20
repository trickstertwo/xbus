package redisstream

import (
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Option configures xbus.Bus when using Use().
type Option func(*xbus.BusBuilder)

// WithLogger injects a custom logger.
func WithLogger(l *xlog.Logger) Option {
	return func(b *xbus.BusBuilder) { b.WithLogger(l) }
}

// WithClock injects a custom clock.
func WithClock(c xclock.Clock) Option {
	return func(b *xbus.BusBuilder) { b.WithClock(c) }
}

// WithCodec selects a codec by name (default: "json").
func WithCodec(name string) Option {
	return func(b *xbus.BusBuilder) { b.WithCodec(name) }
}

// WithMiddleware adds processing middlewares.
func WithMiddleware(mw ...xbus.Middleware) Option {
	return func(b *xbus.BusBuilder) { b.WithMiddleware(mw...) }
}

// WithAckTimeout sets ack/nack timeout.
func WithAckTimeout(d time.Duration) Option {
	return func(b *xbus.BusBuilder) { b.WithAckTimeout(d) }
}

// WithObserver attaches lifecycle event observers.
func WithObserver(obs ...xbus.Observer) Option {
	return func(b *xbus.BusBuilder) { b.WithObserver(obs...) }
}

// WithObserverPool configures the async observer pool.
func WithObserverPool(workers, bufferSize int) Option {
	return func(b *xbus.BusBuilder) { b.WithObserverPool(workers, bufferSize) }
}
