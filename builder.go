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
	b.notify(BusEvent{Type: EventPublishStart, Topic: topic, EventName: eventName})
	err = b.transport.Publish(ctx, topic, msg)
	b.notify(BusEvent{Type: EventPublishDone, Topic: topic, EventName: eventName, Duration: b.clock.Since(start), Err: err})
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
		b.notify(BusEvent{Type: EventConsumeStart, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name})
		start := b.clock.Now()

		err := wh(ctx, msg)
		if err == nil {
			b.ackWithTimeout(ctx, d, true, nil)
			b.notify(BusEvent{
				Type:      EventConsumeDone,
				Topic:     topic,
				Group:     group,
				MessageID: msg.ID,
				EventName: msg.Name,
				Duration:  b.clock.Since(start),
			})
			b.notify(BusEvent{Type: EventAck, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name})
			return
		}

		b.ackWithTimeout(ctx, d, false, err)
		b.notify(BusEvent{
			Type:      EventConsumeDone,
			Topic:     topic,
			Group:     group,
			MessageID: msg.ID,
			EventName: msg.Name,
			Duration:  b.clock.Since(start),
			Err:       err,
		})
		b.notify(BusEvent{Type: EventNack, Topic: topic, Group: group, MessageID: msg.ID, EventName: msg.Name, Err: err})
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
		_ = d.Ack(actx)
		return
	}
	_ = d.Nack(actx, reason)
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

func (b *Bus) notify(e BusEvent) {
	b.observersMu.RLock()
	obs := make([]Observer, len(b.observers))
	copy(obs, b.observers)
	b.observersMu.RUnlock()
	for _, o := range obs {
		o.OnBusEvent(e)
	}
}

// BusBuilder constructs Bus instances (Builder pattern).
type BusBuilder struct {
	transportName string
	transportCfg  map[string]any
	codecName     string
	middlewares   []Middleware
	observers     []Observer
	logger        *xlog.Logger
	clock         xclock.Clock
	ackTimeout    time.Duration
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

func (bb *BusBuilder) WithCodec(name string) *BusBuilder {
	bb.codecName = name
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
	tr, err := NewTransport(bb.transportName, bb.transportCfg)
	if err != nil {
		return nil, err
	}
	cd, err := NewCodec(bb.codecName)
	if err != nil {
		return nil, err
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

	// Attach any configured observers.
	for _, o := range bb.observers {
		b.AddObserver(o)
	}

	return b, nil
}

// Facade: package-level singleton access for teams needing quick start.

var (
	defaultBusOnce sync.Once
	defaultBus     *Bus
	defaultBusErr  error
)

// InitDefaultBus initializes the global bus once (Singleton).
func InitDefaultBus(init func(b *BusBuilder)) error {
	defaultBusOnce.Do(func() {
		b := NewBusBuilder()
		if init != nil {
			init(b)
		}
		defaultBus, defaultBusErr = b.Build()
	})
	return defaultBusErr
}

// Default returns the global bus instance or nil if not initialized.
func Default() *Bus { return defaultBus }

// Publish is the Facade that uses the default bus.
func Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error {
	if defaultBus == nil {
		return ErrDefaultBusNotInitialized
	}
	return defaultBus.Publish(ctx, topic, eventName, payload, meta)
}

// Subscribe is the Facade that uses the default bus.
func Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	if defaultBus == nil {
		return nil, ErrDefaultBusNotInitialized
	}
	return defaultBus.Subscribe(ctx, topic, group, handler)
}
