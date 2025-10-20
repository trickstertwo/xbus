package xbus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

var _ API = (*Bus)(nil)
var _ HealthChecker = (*Bus)(nil)

// Bus is the central Facade handling publish/subscribe against a Transport.
type Bus struct {
	transport    Transport
	codec        Codec
	clock        xclock.Clock
	logger       *xlog.Logger
	middlewares  []Middleware
	ackTimeout   time.Duration
	observerPool *ObserverPool
	observersMu  sync.RWMutex
	observers    []Observer
	baseCtx      context.Context
	metrics      *busMetrics
	closed       atomic.Bool
	closeOnce    sync.Once
}

// busMetrics uses lock-free atomics for production-grade telemetry.
type busMetrics struct {
	publishCount atomic.Uint64
	consumeCount atomic.Uint64
	ackCount     atomic.Uint64
	nackCount    atomic.Uint64
	errorCount   atomic.Uint64
	processingNs atomic.Int64
}

// Codec returns the configured codec (Strategy).
func (b *Bus) Codec() Codec { return b.codec }

// Publish encodes and sends a payload to a topic as an event name.
// OPTIMIZATION: Inline validation, fail-fast on closed bus, minimal allocations.
func (b *Bus) Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error {
	// CRITICAL: Check if bus is closed FIRST (before any work)
	if b.closed.Load() {
		return ErrBusClosed
	}

	// CRITICAL: Validate inputs before expensive encoding
	if topic == "" {
		return ErrInvalidTopic
	}
	if eventName == "" {
		return ErrInvalidEventName
	}

	b.metrics.publishCount.Add(1)

	// Encode payload
	data, err := b.codec.Marshal(payload)
	if err != nil {
		b.metrics.errorCount.Add(1)
		return err
	}

	// OPTIMIZATION: Reuse Message from stack (small struct)
	msg := Message{
		ID:         "",
		Name:       eventName,
		Payload:    data,
		Metadata:   meta,
		ProducedAt: b.clock.Now(),
	}

	start := b.clock.Now()
	b.notifyAsync(Event{Type: PublishStart, Topic: topic, EventName: eventName})

	err = b.transport.Publish(ctx, topic, &msg)

	duration := b.clock.Since(start)
	b.recordProcessingTime(duration.Nanoseconds())

	b.notifyAsync(Event{
		Type:      PublishDone,
		Topic:     topic,
		EventName: eventName,
		Duration:  duration,
		Err:       err,
	})

	if err != nil {
		b.metrics.errorCount.Add(1)
	}

	return err
}

// PublishBatch sends multiple events atomically with optimized event notification.
// OPTIMIZATION: Pre-allocate message slice, single batch event (not per-message).
func (b *Bus) PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	if b.closed.Load() {
		return ErrBusClosed
	}

	if len(events) == 0 {
		return nil
	}

	if topic == "" {
		return ErrInvalidTopic
	}

	// CRITICAL: Validate all events BEFORE any encoding (fail-fast)
	for _, evt := range events {
		if evt.Name == "" {
			return ErrInvalidEventName
		}
		if evt.Payload == nil {
			return ErrInvalidPayload
		}
	}

	b.metrics.publishCount.Add(uint64(len(events)))

	// OPTIMIZATION: Pre-allocate exact capacity (no reallocation)
	msgs := make([]*Message, len(events))
	for i := range events {
		data, err := b.codec.Marshal(events[i].Payload)
		if err != nil {
			b.metrics.errorCount.Add(1)
			return err
		}
		msgs[i] = &Message{
			Name:       events[i].Name,
			Payload:    data,
			Metadata:   events[i].Meta,
			ProducedAt: b.clock.Now(),
		}
	}

	// OPTIMIZATION: Single batch event notification (not per-message)
	b.notifyAsync(Event{
		Type:      PublishStart,
		Topic:     topic,
		EventName: "batch",
	})

	start := b.clock.Now()
	err := b.transport.Publish(ctx, topic, msgs...)

	duration := b.clock.Since(start)
	b.recordProcessingTime(duration.Nanoseconds())

	b.notifyAsync(Event{
		Type:      PublishDone,
		Topic:     topic,
		EventName: "batch",
		Duration:  duration,
		Err:       err,
	})

	if err != nil {
		b.metrics.errorCount.Add(1)
	}

	return err
}

// Subscribe registers a handler under a consumer group for a topic.
// OPTIMIZATION: Pre-composed middleware chain, panic-safe wrapper.
func (b *Bus) Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	if b.closed.Load() {
		return nil, ErrBusClosed
	}

	if topic == "" || group == "" || handler == nil {
		return nil, ErrInvalidSubscription
	}

	// CRITICAL: Always enable panic recovery first for dependability
	base := RecoveryMiddleware()(handler)
	wh := Chain(base, b.middlewares...)

	return b.transport.Subscribe(ctx, topic, group, func(d Delivery) {
		// CRITICAL: Panic-safe wrapper for entire lifecycle
		func() {
			defer func() {
				if r := recover(); r != nil {
					b.logger.Warn().Msg("xbus: handler panic (recovered)")
					b.metrics.errorCount.Add(1)
					_ = d.Nack(context.Background(), ErrHandlerPanic)
				}
			}()

			b.metrics.consumeCount.Add(1)
			msg := d.Message()

			// OPTIMIZATION: Use pre-injected context (no per-delivery allocation)
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

			duration := b.clock.Since(start)
			b.recordProcessingTime(duration.Nanoseconds())

			if err == nil {
				b.metrics.ackCount.Add(1)
				b.ackWithTimeout(hctx, d, true, nil)
				b.notifyAsync(Event{
					Type:      ConsumeDone,
					Topic:     topic,
					Group:     group,
					MessageID: msg.ID,
					EventName: msg.Name,
					Duration:  duration,
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
				Duration:  duration,
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
		}()
	})
}

// ackWithTimeout handles ack/nack with configurable timeout.
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
			b.logger.Warn().Err(err).Msg("xbus: ack failed")
		}
		return
	}

	if err := d.Nack(actx, reason); err != nil {
		b.metrics.errorCount.Add(1)
		b.notifyAsync(Event{Type: Error, Err: err})
		b.logger.Warn().Err(err).Msg("xbus: nack failed")
	}
}

// GetMetrics returns current bus metrics.
func (b *Bus) GetMetrics() Metrics {
	return Metrics{
		Published:           b.metrics.publishCount.Load(),
		Consumed:            b.metrics.consumeCount.Load(),
		Acked:               b.metrics.ackCount.Load(),
		Nacked:              b.metrics.nackCount.Load(),
		Errors:              b.metrics.errorCount.Load(),
		EventsDropped:       b.observerPool.Stats().Dropped,
		AvgProcessingTimeMs: float64(b.metrics.processingNs.Load()) / 1e6,
	}
}

// Health checks bus health for Kubernetes probes.
// Implements HealthChecker interface.
func (b *Bus) Health(ctx context.Context) HealthStatus {
	if b.closed.Load() {
		return HealthStatus{
			Status:    "unhealthy",
			Timestamp: time.Now(),
			Message:   "bus is closed",
		}
	}

	metrics := b.GetMetrics()
	status := "healthy"

	// Degraded if error rate > 5%
	if metrics.Errors > 0 && metrics.Published > 0 {
		errorRate := float64(metrics.Errors) / float64(metrics.Published)
		if errorRate > 0.05 {
			status = "degraded"
		}
	}

	return HealthStatus{
		Status:    status,
		Metrics:   metrics,
		Timestamp: time.Now(),
	}
}

// Close gracefully shuts down the bus.
// CRITICAL: Idempotent via sync.Once, cleanup ordering, error handling.
func (b *Bus) Close(ctx context.Context) error {
	var closeErr error

	b.closeOnce.Do(func() {
		b.closed.Store(true)

		// 1. Stop accepting new work
		// 2. Drain observer pool
		if b.observerPool != nil {
			if err := b.observerPool.Close(5 * time.Second); err != nil {
				b.logger.Warn().Err(err).Msg("xbus: observer pool shutdown timeout")
				closeErr = err
			}
		}

		// 3. Close transport
		if err := b.transport.Close(ctx); err != nil {
			b.logger.Error().Err(err).Msg("xbus: transport close failed")
			closeErr = err
		}
	})

	return closeErr
}

// AddObserver registers an observer (thread-safe).
func (b *Bus) AddObserver(obs Observer) {
	if obs == nil {
		return
	}
	b.observersMu.Lock()
	b.observers = append(b.observers, obs)
	b.observersMu.Unlock()
}

// RemoveObserver removes an observer.
func (b *Bus) RemoveObserver(obs Observer) {
	if obs == nil {
		return
	}
	b.observersMu.Lock()
	defer b.observersMu.Unlock()

	for i, o := range b.observers {
		if o == obs {
			b.observers = append(b.observers[:i], b.observers[i+1:]...)
			break
		}
	}
}

// notifyAsync dispatches events asynchronously (non-blocking).
// CRITICAL: Fails fast on closed bus, avoids observer copy if no observers.
func (b *Bus) notifyAsync(e Event) {
	if b.observerPool == nil || b.closed.Load() {
		return
	}

	b.observersMu.RLock()
	observerCount := len(b.observers)
	if observerCount == 0 {
		b.observersMu.RUnlock()
		return
	}

	// OPTIMIZATION: Avoid slice copy if only one observer
	if observerCount == 1 {
		obs := b.observers[0]
		b.observersMu.RUnlock()
		b.observerPool.Notify(e, []Observer{obs})
		return
	}

	// Copy observers for safety
	observers := make([]Observer, observerCount)
	copy(observers, b.observers)
	b.observersMu.RUnlock()

	b.observerPool.Notify(e, observers)
}

// recordProcessingTime records processing time using exponential moving average.
// OPTIMIZATION: More accurate than simple average.
func (b *Bus) recordProcessingTime(ns int64) {
	const alpha = 0.2 // 20% weight to new sample
	current := b.metrics.processingNs.Load()
	if current == 0 {
		b.metrics.processingNs.Store(ns)
		return
	}
	// EMA: new = (alpha * sample) + (1-alpha) * old
	newAvg := int64(float64(ns)*alpha + float64(current)*(1-alpha))
	b.metrics.processingNs.Store(newAvg)
}
