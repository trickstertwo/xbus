package xbus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Bus is the central Facade handling publish/subscribe against a Transport Strategy.
type Bus struct {
	transport   Transport
	codec       Codec
	clock       xclock.Clock
	logger      *xlog.Logger
	middlewares []Middleware
	ackTimeout  time.Duration

	// Async observer dispatch pool (non-blocking)
	observerPool *ObserverPool

	// Cached observers list (read-heavy path, updated rarely)
	observersMu sync.RWMutex
	observers   []Observer

	// Pre-injected context (eliminates per-delivery context allocations)
	baseCtx context.Context

	// Metrics tracking
	metrics *busMetrics
}

// busMetrics tracks operational telemetry (lock-free with atomics)
type busMetrics struct {
	publishCount atomic.Uint64
	consumeCount atomic.Uint64
	ackCount     atomic.Uint64
	nackCount    atomic.Uint64
	errorCount   atomic.Uint64
}

// PublishEvent describes a single event in a batch publish call.
type PublishEvent struct {
	Name    string
	Payload any
	Meta    map[string]string
}

// Codec returns the configured codec (Strategy).
func (b *Bus) Codec() Codec { return b.codec }

// Publish encodes and sends a payload to a topic as an event name.
// Optimized: minimal allocations, async event notification.
func (b *Bus) Publish(ctx context.Context, topic string, eventName string, payload any, meta map[string]string) error {
	b.metrics.publishCount.Add(1)

	data, err := b.codec.Marshal(payload)
	if err != nil {
		return err
	}

	msg := &Message{
		ID:         "", // transport may assign
		Name:       eventName,
		Payload:    data,
		Metadata:   meta,
		ProducedAt: b.clock.Now(),
	}

	start := b.clock.Now()
	b.notifyAsync(Event{Type: PublishStart, Topic: topic, EventName: eventName})

	err = b.transport.Publish(ctx, topic, msg)

	b.notifyAsync(Event{
		Type:      PublishDone,
		Topic:     topic,
		EventName: eventName,
		Duration:  b.clock.Since(start),
		Err:       err,
	})
	return err
}

// PublishBatch encodes and sends multiple events to a topic atomically.
// Optimized: single batch event notification (not per-message).
func (b *Bus) PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	if len(events) == 0 {
		return nil
	}

	b.metrics.publishCount.Add(uint64(len(events)))

	msgs := make([]*Message, 0, len(events))
	for i := range events {
		data, err := b.codec.Marshal(events[i].Payload)
		if err != nil {
			return err
		}
		msgs = append(msgs, &Message{
			Name:       events[i].Name,
			Payload:    data,
			Metadata:   events[i].Meta,
			ProducedAt: b.clock.Now(),
		})
	}

	// Single batch start event (not per-message)
	b.notifyAsync(Event{
		Type:      PublishStart,
		Topic:     topic,
		EventName: "batch",
	})

	start := b.clock.Now()
	err := b.transport.Publish(ctx, topic, msgs...)

	// Single batch done event with aggregated metrics
	b.notifyAsync(Event{
		Type:      PublishDone,
		Topic:     topic,
		EventName: "batch",
		Duration:  b.clock.Since(start),
		Err:       err,
	})

	return err
}

// Subscribe registers a handler under a consumer group for a topic.
// Optimized: pre-injected context, async event notification.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	// Always enable panic recovery first for dependability
	base := RecoveryMiddleware()(handler)
	wh := Chain(base, b.middlewares...)

	return b.transport.Subscribe(ctx, topic, group, func(d Delivery) {
		b.metrics.consumeCount.Add(1)

		msg := d.Message()

		// Use pre-injected base context (avoids 3x context allocation per delivery)
		hctx := b.baseCtx

		b.notifyAsync(Event{
			Type:      ConsumeStart,
			Topic:     topic,
			Group:     group,
			MessageID: msg.ID,
			EventName: msg.Name,
		})

		start := b.clock.Now()
		err := wh(hctx, msg)

		if err == nil {
			b.metrics.ackCount.Add(1)
			b.ackWithTimeout(hctx, d, true, nil)
			b.notifyAsync(Event{
				Type:      ConsumeDone,
				Topic:     topic,
				Group:     group,
				MessageID: msg.ID,
				EventName: msg.Name,
				Duration:  b.clock.Since(start),
			})
			b.notifyAsync(Event{
				Type:      Ack,
				Topic:     topic,
				Group:     group,
				MessageID: msg.ID,
				EventName: msg.Name,
			})
			return
		}

		b.metrics.nackCount.Add(1)
		b.ackWithTimeout(hctx, d, false, err)
		b.notifyAsync(Event{
			Type:      ConsumeDone,
			Topic:     topic,
			Group:     group,
			MessageID: msg.ID,
			EventName: msg.Name,
			Duration:  b.clock.Since(start),
			Err:       err,
		})
		b.notifyAsync(Event{
			Type:      Nack,
			Topic:     topic,
			Group:     group,
			MessageID: msg.ID,
			EventName: msg.Name,
			Err:       err,
		})
	})
}

func (b *Bus) ackWithTimeout(ctx context.Context, d Delivery, ack bool, reason error) {
	actx := ctx
	cancel := func() {}
	if b.ackTimeout > 0 {
		actx, cancel = context.WithTimeout(ctx, b.ackTimeout)
	}
	defer cancel()

	if ack {
		if err := d.Ack(actx); err != nil {
			b.metrics.errorCount.Add(1)
			b.notifyAsync(Event{Type: Error, Err: err})
			if lg, ok := LoggerFromContext(ctx); ok && lg != nil {
				lg.Warn().Err(err).Msg("xbus ack failed")
			}
		}
		return
	}

	if err := d.Nack(actx, reason); err != nil {
		b.metrics.errorCount.Add(1)
		b.notifyAsync(Event{Type: Error, Err: err})
		if lg, ok := LoggerFromContext(ctx); ok && lg != nil {
			lg.Warn().Err(err).Msg("xbus nack failed")
		}
	}
}

// Close releases underlying resources.
func (b *Bus) Close(ctx context.Context) error {
	// Gracefully shutdown observer pool
	if b.observerPool != nil {
		_ = b.observerPool.Close(5 * time.Second)
	}
	return b.transport.Close(ctx)
}

// AddObserver registers an observer for bus events.
func (b *Bus) AddObserver(obs Observer) {
	if obs == nil {
		return
	}
	b.observersMu.Lock()
	b.observers = append(b.observers, obs)
	b.observersMu.Unlock()
}

// notifyAsync dispatches events asynchronously (non-blocking).
// Drops events if pool buffer is full to prevent blocking the publish path.
func (b *Bus) notifyAsync(e Event) {
	if b.observerPool == nil {
		return // No observer pool configured
	}

	b.observersMu.RLock()
	observers := make([]Observer, len(b.observers))
	copy(observers, b.observers)
	b.observersMu.RUnlock()

	if len(observers) > 0 {
		b.observerPool.Notify(e, observers)
	}
}

// BusBuilder constructs Bus instances (Builder pattern).
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

// NewBusBuilder returns a new builder with sensible defaults.
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

// WithTransportInstance accepts a ready Transport instance.
func (bb *BusBuilder) WithTransportInstance(t Transport) *BusBuilder {
	bb.transportInst = t
	return bb
}

func (bb *BusBuilder) WithCodec(name string) *BusBuilder {
	bb.codecName = name
	return bb
}

// WithCodecInstance accepts a ready Codec instance.
func (bb *BusBuilder) WithCodecInstance(c Codec) *BusBuilder {
	bb.codecInst = c
	return bb
}

func (bb *BusBuilder) WithMiddleware(mw ...Middleware) *BusBuilder {
	if len(mw) == 0 {
		return bb
	}
	bb.middlewares = append(bb.middlewares, mw...)
	return bb
}

func (bb *BusBuilder) WithObserver(obs ...Observer) *BusBuilder {
	if len(obs) == 0 {
		return bb
	}
	for _, o := range obs {
		if o != nil {
			bb.observers = append(bb.observers, o)
		}
	}
	return bb
}

func (bb *BusBuilder) WithLogger(l *xlog.Logger) *BusBuilder {
	bb.logger = l
	return bb
}

func (bb *BusBuilder) WithClock(c xclock.Clock) *BusBuilder {
	bb.clock = c
	return bb
}

func (bb *BusBuilder) WithAckTimeout(d time.Duration) *BusBuilder {
	if d > 0 {
		bb.ackTimeout = d
	}
	return bb
}

// WithObserverPool configures async observer pool workers and buffer size.
func (bb *BusBuilder) WithObserverPool(workers, bufferSize int) *BusBuilder {
	if workers > 0 {
		bb.observerWorkers = workers
	}
	if bufferSize > 0 {
		bb.observerBufferSize = bufferSize
	}
	return bb
}

func (bb *BusBuilder) Build() (*Bus, error) {
	var tr Transport
	var err error

	switch {
	case bb.transportInst != nil:
		tr = bb.transportInst
	case bb.transportName != "":
		tr, err = NewTransport(bb.transportName, bb.transportCfg)
		if err != nil {
			return nil, err
		}
	default:
		return nil, ErrNoTransportConfigured
	}

	var cd Codec
	if bb.codecInst != nil {
		cd = bb.codecInst
	} else {
		cd, err = NewCodec(bb.codecName)
		if err != nil {
			return nil, err
		}
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

	// Pre-inject codec, logger, clock once to eliminate per-delivery context allocation
	baseCtx := context.Background()
	baseCtx = injectCodec(baseCtx, cd)
	baseCtx = injectLogger(baseCtx, lg)
	baseCtx = injectClock(baseCtx, clk)

	// Create async observer pool for non-blocking event dispatch
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

	// Attach configured observers
	for _, o := range bb.observers {
		b.AddObserver(o)
	}

	return b, nil
}

var (
	defaultBus   *Bus
	defaultBusMu sync.Mutex
)

// Default returns the process-wide singleton Bus.
func Default() *Bus {
	defaultBusMu.Lock()
	defer defaultBusMu.Unlock()

	if defaultBus != nil {
		return defaultBus
	}
	b := NewBusBuilder()
	bus, err := b.Build()
	if err != nil {
		panic(err)
	}
	defaultBus = bus
	return defaultBus
}

// SetDefault replaces the process-wide default Bus.
func SetDefault(b *Bus) {
	if b == nil {
		panic("xbus: SetDefault called with nil Bus")
	}
	defaultBusMu.Lock()
	defaultBus = b
	defaultBusMu.Unlock()
}

// Publish is the Facade that uses the default bus.
func Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error {
	return Default().Publish(ctx, topic, eventName, payload, meta)
}

// PublishBatch is the Facade that uses the default bus for batch publishing.
func PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	return Default().PublishBatch(ctx, topic, events...)
}

// Subscribe is the Facade that uses the default bus.
func Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	return Default().Subscribe(ctx, topic, group, handler)
}
