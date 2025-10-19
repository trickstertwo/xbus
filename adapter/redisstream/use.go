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

// Use builds a Bus with Redis Streams and sets it as the default Bus, then returns it.
// Mirrors xlog/xclock "Use" behavior: explicit construction and global install.
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
		panic(fmt.Errorf("redisstream.Use: %w", err))
	}

	// Install as process-wide default (replaces any existing default).
	xbus.SetDefault(bus)
	return bus
}
