package xbus

import (
	"context"
	"sync"
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

	ackTimeout time.Duration

	observersMu sync.RWMutex
	observers   []Observer
}

// Codec returns the configured codec (Strategy).
func (b *Bus) Codec() Codec { return b.codec }

// Publish encodes and sends a payload to a topic as an event name.
func (b *Bus) Publish(ctx context.Context, topic string, eventName string, payload any, meta map[string]string) error {
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
	b.notify(Event{Type: PublishStart, Topic: topic, EventName: eventName})
	err = b.transport.Publish(ctx, topic, msg)
	b.notify(Event{Type: PublishDone, Topic: topic, EventName: eventName, Duration: b.clock.Since(start), Err: err})
	return err
}

// Subscribe registers a handler under a consumer group for a topic.
// The handler will be wrapped with configured middlewares and protected by recovery.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	// Always enable panic recovery first for dependability.
	base := RecoveryMiddleware()(handler)
	wh := Chain(base, b.middlewares...)

	return b.transport.Subscribe(ctx, topic, group, func(d Delivery) {
		msg := d.Message()
		b.notify(Event{Type: ConsumeStart, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name})
		start := b.clock.Now()

		// inject active codec/logger/clock for downstream decoding and observability
		hctx := ctx
		hctx = injectCodec(hctx, b.codec)
		hctx = injectLogger(hctx, b.logger)
		hctx = injectClock(hctx, b.clock)

		err := wh(hctx, msg)
		if err == nil {
			b.ackWithTimeout(hctx, d, true, nil)
			b.notify(Event{
				Type:      ConsumeDone,
				Topic:     topic,
				Group:     group,
				MessageID: msg.ID,
				EventName: msg.Name,
				Duration:  b.clock.Since(start),
			})
			b.notify(Event{Type: Ack, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name})
			return
		}

		b.ackWithTimeout(hctx, d, false, err)
		b.notify(Event{
			Type:      ConsumeDone,
			Topic:     topic,
			Group:     group,
			MessageID: msg.ID,
			EventName: msg.Name,
			Duration:  b.clock.Since(start),
			Err:       err,
		})
		b.notify(Event{Type: Nack, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name, Err: err})
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
			b.notify(Event{Type: Error, Err: err})
			if lg, ok := LoggerFromContext(ctx); ok && lg != nil {
				lg.Warn().Err(err).Msg("xbus ack failed")
			}
		}
		return
	}
	if err := d.Nack(actx, reason); err != nil {
		b.notify(Event{Type: Error, Err: err})
		if lg, ok := LoggerFromContext(ctx); ok && lg != nil {
			lg.Warn().Err(err).Msg("xbus nack failed")
		}
	}
}

// Close releases underlying resources.
func (b *Bus) Close(ctx context.Context) error {
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

func (b *Bus) notify(e Event) {
	b.observersMu.RLock()
	obs := make([]Observer, len(b.observers))
	copy(obs, b.observers)
	b.observersMu.RUnlock()
	for _, o := range obs {
		o.OnEvent(e)
	}
}

// BusBuilder constructs Bus instances (Builder pattern).
type BusBuilder struct {
	transportName string
	transportCfg  map[string]any
	transportInst Transport

	codecName string
	codecInst Codec

	middlewares []Middleware
	observers   []Observer
	logger      *xlog.Logger
	clock       xclock.Clock
	ackTimeout  time.Duration
}

// NewBusBuilder returns a new builder with sensible defaults.
func NewBusBuilder() *BusBuilder {
	return &BusBuilder{
		codecName:  "json",
		ackTimeout: 5 * time.Second, // safe default for production acknowledgments
	}
}

func (bb *BusBuilder) WithTransport(name string, cfg map[string]any) *BusBuilder {
	bb.transportName = name
	bb.transportCfg = cfg
	return bb
}

// WithTransportInstance accepts a ready Transport instance (e.g., from adapter Use()).
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
		// Default to xlog new logger; Adapter pattern to platform logging.
		lg = xlog.Default()
	}
	b := &Bus{
		transport:   tr,
		codec:       cd,
		clock:       clk,
		logger:      lg,
		middlewares: bb.middlewares,
		ackTimeout:  bb.ackTimeout,
	}

	// Attach logging observer first for dependable telemetry unless already supplied externally.
	hasLoggingObserver := false
	for _, o := range bb.observers {
		if _, ok := o.(LoggingObserver); ok {
			hasLoggingObserver = true
			break
		}
	}
	if !hasLoggingObserver && lg != nil {
		b.AddObserver(LoggingObserver{Logger: lg})
	}

	// Attach any configured observers.
	for _, o := range bb.observers {
		b.AddObserver(o)
	}

	return b, nil
}

var (
	defaultBus   *Bus
	defaultBusMu sync.Mutex
)

// New constructs a Bus via Builder and returns a close func for convenience.
func New(init func(b *BusBuilder)) (*Bus, func() error, error) {
	b := NewBusBuilder()
	if init != nil {
		init(b)
	}
	bus, err := b.Build()
	if err != nil {
		return nil, nil, err
	}
	closeFn := func() error { return bus.Close(context.Background()) }
	return bus, closeFn, nil
}

// Default returns the process-wide singleton Bus. If it isn't initialized yet,
// it initializes it using the optional init function (Builder + Factory).
func Default(init func(b *BusBuilder)) (*Bus, error) {
	defaultBusMu.Lock()
	defer defaultBusMu.Unlock()

	if defaultBus != nil {
		return defaultBus, nil
	}
	b := NewBusBuilder()
	if init != nil {
		init(b)
	}
	bus, err := b.Build()
	if err != nil {
		return nil, err
	}
	defaultBus = bus
	return defaultBus, nil
}

// Publish is the Facade that uses the default bus.
func Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error {
	b, err := Default(nil)
	if err != nil {
		return err
	}
	return b.Publish(ctx, topic, eventName, payload, meta)
}

// PublishBatch is the Facade that uses the default bus for batch publishing.
func PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	b, err := Default(nil)
	if err != nil {
		return err
	}
	return b.PublishBatch(ctx, topic, events...)
}

// Subscribe is the Facade that uses the default bus.
func Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	b, err := Default(nil)
	if err != nil {
		return nil, err
	}
	return b.Subscribe(ctx, topic, group, handler)
}
