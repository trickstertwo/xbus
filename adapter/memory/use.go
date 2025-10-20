package memory

import (
	"fmt"
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Use builds a Bus with the in-memory transport and sets it as the default.
// Mirrors redisstream.Use and xlog "Use" pattern: explicit construction with global install.
//
// Example:
//
//	bus := memory.Use(memory.Config{
//	    BufferSize:  4096,
//	    Concurrency: 8,
//	    AssignIDs:   true,
//	},
//	    memory.WithLogger(logger),
//	    memory.WithObserver(observer),
//	)
//
// The returned bus is installed as the process-wide default.
func Use(cfg Config, opts ...Option) *xbus.Bus {
	bb := xbus.NewBusBuilder().
		WithTransport(TransportName, cfg.toMap())

	for _, o := range opts {
		if o != nil {
			o(bb)
		}
	}

	bus, err := bb.Build()
	if err != nil {
		panic(fmt.Errorf("memory.Use: %w", err))
	}

	// Install as process-wide default
	xbus.SetDefault(bus)
	return bus
}

// toMap converts Config to the generic map expected by the transport factory.
func (c Config) toMap() map[string]any {
	return map[string]any{
		"buffer_size":      c.BufferSize,
		"concurrency":      c.Concurrency,
		"redelivery_delay": c.RedeliveryDelay,
		"assign_ids":       c.AssignIDs,
	}
}

// Option configures the xbus.Bus when calling Use.
type Option func(*xbus.BusBuilder)

// WithLogger injects a custom xlog logger.
func WithLogger(l *xlog.Logger) Option {
	return func(b *xbus.BusBuilder) { b.WithLogger(l) }
}

// WithClock injects a custom xclock clock.
func WithClock(c xclock.Clock) Option {
	return func(b *xbus.BusBuilder) { b.WithClock(c) }
}

// WithCodec selects a codec by name (default: "json").
func WithCodec(name string) Option {
	return func(b *xbus.BusBuilder) { b.WithCodec(name) }
}

// WithMiddleware adds processing middlewares (retry, timeout, etc).
func WithMiddleware(mw ...xbus.Middleware) Option {
	return func(b *xbus.BusBuilder) { b.WithMiddleware(mw...) }
}

// WithAckTimeout sets acks/nacks timeout (default: 5s).
func WithAckTimeout(d time.Duration) Option {
	return func(b *xbus.BusBuilder) { b.WithAckTimeout(d) }
}

// WithObserver attaches observers for lifecycle events.
func WithObserver(obs ...xbus.Observer) Option {
	return func(b *xbus.BusBuilder) { b.WithObserver(obs...) }
}

// WithObserverPool configures async observer pool for non-blocking notifications.
func WithObserverPool(workers, bufferSize int) Option {
	return func(b *xbus.BusBuilder) { b.WithObserverPool(workers, bufferSize) }
}
