package xbus

import (
	"context"
	"fmt"
	"time"

	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// BusBuilder constructs Bus instances following the Builder pattern.
type BusBuilder struct {
	transportName      string
	transportCfg       map[string]any
	transportInst      Transport
	codecName          string
	codecInst          Codec
	middlewares        []Middleware
	observers          []Observer
	logger             *xlog.Logger
	clock              xclock.Clock
	ackTimeout         time.Duration
	observerWorkers    int
	observerBufferSize int
}

// NewBusBuilder returns a new builder with production-safe defaults.
func NewBusBuilder() *BusBuilder {
	return &BusBuilder{
		codecName:          "json",
		ackTimeout:         5 * time.Second,
		observerWorkers:    4,
		observerBufferSize: 1000,
	}
}

func (bb *BusBuilder) WithTransport(name string, cfg map[string]any) *BusBuilder {
	bb.transportName = name
	bb.transportCfg = cfg
	return bb
}

func (bb *BusBuilder) WithTransportInstance(t Transport) *BusBuilder {
	if t == nil {
		panic("xbus: transport instance must not be nil")
	}
	bb.transportInst = t
	return bb
}

func (bb *BusBuilder) WithCodec(name string) *BusBuilder {
	if name == "" {
		panic("xbus: codec name must not be empty")
	}
	bb.codecName = name
	return bb
}

func (bb *BusBuilder) WithCodecInstance(c Codec) *BusBuilder {
	if c == nil {
		panic("xbus: codec instance must not be nil")
	}
	bb.codecInst = c
	return bb
}

func (bb *BusBuilder) WithMiddleware(mw ...Middleware) *BusBuilder {
	for _, m := range mw {
		if m == nil {
			panic("xbus: middleware must not be nil")
		}
	}
	bb.middlewares = append(bb.middlewares, mw...)
	return bb
}

func (bb *BusBuilder) WithObserver(obs ...Observer) *BusBuilder {
	for _, o := range obs {
		if o == nil {
			panic("xbus: observer must not be nil")
		}
	}
	bb.observers = append(bb.observers, obs...)
	return bb
}

func (bb *BusBuilder) WithLogger(l *xlog.Logger) *BusBuilder {
	if l == nil {
		panic("xbus: logger must not be nil")
	}
	bb.logger = l
	return bb
}

func (bb *BusBuilder) WithClock(c xclock.Clock) *BusBuilder {
	if c == nil {
		panic("xbus: clock must not be nil")
	}
	bb.clock = c
	return bb
}

func (bb *BusBuilder) WithAckTimeout(d time.Duration) *BusBuilder {
	if d <= 0 {
		panic("xbus: ack timeout must be positive")
	}
	bb.ackTimeout = d
	return bb
}

func (bb *BusBuilder) WithObserverPool(workers, bufferSize int) *BusBuilder {
	if workers < 1 {
		panic("xbus: observer workers must be >= 1")
	}
	if bufferSize < 1 {
		panic("xbus: observer buffer size must be >= 1")
	}
	bb.observerWorkers = workers
	bb.observerBufferSize = bufferSize
	return bb
}

// Build constructs and validates the Bus instance.
func (bb *BusBuilder) Build() (*Bus, error) {
	var tr Transport
	switch {
	case bb.transportInst != nil:
		tr = bb.transportInst
	case bb.transportName != "":
		t, err := NewTransport(bb.transportName, bb.transportCfg)
		if err != nil {
			return nil, fmt.Errorf("xbus: failed to create transport %q: %w", bb.transportName, err)
		}
		tr = t
	default:
		return nil, ErrNoTransportConfigured
	}

	var cd Codec
	if bb.codecInst != nil {
		cd = bb.codecInst
	} else {
		c, err := NewCodec(bb.codecName)
		if err != nil {
			return nil, fmt.Errorf("xbus: failed to create codec %q: %w", bb.codecName, err)
		}
		cd = c
	}

	var clk xclock.Clock
	if bb.clock != nil {
		clk = bb.clock
	} else {
		clk = xclock.Default()
	}

	var lg *xlog.Logger
	if bb.logger != nil {
		lg = bb.logger
	} else {
		lg = xlog.Default()
	}

	baseCtx := context.Background()
	baseCtx = injectCodec(baseCtx, cd)
	baseCtx = injectLogger(baseCtx, lg)
	baseCtx = injectClock(baseCtx, clk)

	pool := NewObserverPool(context.Background(), bb.observerWorkers, bb.observerBufferSize)

	b := &Bus{
		transport:    tr,
		codec:        cd,
		clock:        clk,
		logger:       lg,
		middlewares:  bb.middlewares,
		ackTimeout:   bb.ackTimeout,
		observerPool: pool,
		baseCtx:      baseCtx,
		metrics:      &busMetrics{},
	}

	for _, o := range bb.observers {
		b.AddObserver(o)
	}

	lg.Info().
		Str("transport", bb.transportName).
		Str("codec", bb.codecName).
		Int("middlewares", len(bb.middlewares)).
		Int("observers", len(bb.observers)).
		Msg("xbus: bus initialized")

	return b, nil
}
