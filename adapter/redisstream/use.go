package redisstream

import (
	"fmt"

	"github.com/trickstertwo/xbus"
)

// Adapter: Redis Streams Transport (Strategy + Adapter patterns)

const TransportName = "redis-streams"

func init() {
	if err := xbus.RegisterTransport(TransportName, func(cfg map[string]any) (xbus.Transport, error) {
		return NewTransport(ConfigFromMap(cfg))
	}); err != nil {
		panic(fmt.Errorf("xbus: failed to register transport %q: %w", TransportName, err))
	}
}

// Use builds and sets the default xbus.Bus using Redis Streams and returns it,
// mirroring xlog/zerolog.Use for clear, explicit initialization.
//
// It fails fast by panicking if construction fails (production-friendly when
// transport must be available at startup).
func Use(cfg Config, opts ...Option) *xbus.Bus {
	bus, err := xbus.Default(func(b *xbus.BusBuilder) {
		// Prefer typed config; internally go through the factory with a map to avoid extra coupling.
		b.WithTransport(TransportName, cfg.toMap())
		for _, o := range opts {
			if o != nil {
				o(b)
			}
		}
	})
	if err != nil {
		panic(fmt.Errorf("redisstream.Use: %w", err))
	}
	return bus
}
