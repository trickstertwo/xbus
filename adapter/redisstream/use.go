package redisstream

import (
	"fmt"

	"github.com/trickstertwo/xbus"
)

const TransportName = "redis-streams"

func init() {
	if err := xbus.RegisterTransport(TransportName, func(cfg map[string]any) (xbus.Transport, error) {
		return NewTransport(ConfigFromMap(cfg))
	}); err != nil {
		panic(fmt.Errorf("xbus: failed to register transport %q: %w", TransportName, err))
	}
}

// Use builds a Bus with Redis Streams and sets it as the process-wide default.
//
// Example:
//
//	bus := redisstream.Use(redisstream.Config{
//	    Addr:        "localhost:6379",
//	    Group:       "payments",
//	    Concurrency: 16,
//	},
//	    redisstream.WithLogger(logger),
//	    redisstream.WithMiddleware(xbus.TimeoutMiddleware(15*time.Second)),
//	)
func Use(cfg Config, opts ...Option) *xbus.Bus {
	bb := xbus.NewBusBuilder().WithTransport(TransportName, cfg.toMap())

	for _, o := range opts {
		if o != nil {
			o(bb)
		}
	}

	bus, err := bb.Build()
	if err != nil {
		panic(fmt.Errorf("redisstream.Use: %w", err))
	}

	xbus.SetDefault(bus)
	return bus
}
