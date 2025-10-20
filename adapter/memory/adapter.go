package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/trickstertwo/xbus"
)

const TransportName = "memory"

func init() {
	if err := xbus.RegisterTransport(TransportName, func(cfg map[string]any) (xbus.Transport, error) {
		return NewTransport(ConfigFromMap(cfg)), nil
	}); err != nil {
		panic(fmt.Errorf("xbus/memory: failed to register transport: %w", err))
	}
}

// Config controls memory transport behavior.
type Config struct {
	// BufferSize is the per-group queue size (default: 1024).
	BufferSize int
	// Concurrency is the default number of worker goroutines per subscription (default: 1).
	Concurrency int
	// RedeliveryDelay is the delay before re-enqueuing a message on Nack (default: 0 = immediate).
	RedeliveryDelay time.Duration
	// AssignIDs instructs the transport to assign IDs for messages with empty ID (default: true).
	AssignIDs bool
}

func ConfigFromMap(cfg map[string]any) Config {
	getInt := func(k string, d int) int {
		switch v := cfg[k].(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		default:
			return d
		}
	}

	getBool := func(k string, d bool) bool {
		if v, ok := cfg[k].(bool); ok {
			return v
		}
		return d
	}

	getDur := func(k string, d time.Duration) time.Duration {
		switch v := cfg[k].(type) {
		case time.Duration:
			return v
		case string:
			if p, err := time.ParseDuration(v); err == nil {
				return p
			}
		case float64:
			return time.Duration(v)
		}
		return d
	}

	return Config{
		BufferSize:      maxInt(1, getInt("buffer_size", 1024)),
		Concurrency:     maxInt(1, getInt("concurrency", 1)),
		RedeliveryDelay: getDur("redelivery_delay", 0),
		AssignIDs:       getBool("assign_ids", true),
	}
}

// Transport implements xbus.Transport using in-memory channels (dev/testing).
// Not suitable for production but excellent for local development and benchmarking.
type Transport struct {
	cfg Config

	mu     sync.RWMutex
	topics map[string]*topic

	closed atomic.Bool

	// Metrics for observability
	metrics *transportMetrics
}

type transportMetrics struct {
	published     atomic.Uint64
	consumed      atomic.Uint64
	acked         atomic.Uint64
	nacked        atomic.Uint64
	redelivered   atomic.Uint64
	publishErrors atomic.Uint64
}

var _ xbus.Transport = (*Transport)(nil)

// NewTransport creates a new in-memory transport.
func NewTransport(cfg Config) *Transport {
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 1024
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}

	return &Transport{
		cfg:     cfg,
		topics:  make(map[string]*topic),
		metrics: &transportMetrics{},
	}
}

// Publish fans out messages to all consumer groups for the topic.
func (t *Transport) Publish(ctx context.Context, topic string, msgs ...*xbus.Message) error {
	if t.closed.Load() {
		return errors.New("memory transport is closed")
	}
	if len(msgs) == 0 {
		return nil
	}

	t.mu.RLock()
	top, ok := t.topics[topic]
	t.mu.RUnlock()

	if !ok {
		// No topic registered => no subscribers => drop (in-memory dev semantics)
		return nil
	}

	for _, m := range msgs {
		if m == nil {
			continue
		}

		// Assign ID if configured and not provided
		if t.cfg.AssignIDs && m.ID == "" {
			m.ID = nextID()
		}

		// Fan-out to all consumer groups (one-of-N delivery per group)
		top.mu.RLock()
		for _, g := range top.groups {
			select {
			case <-ctx.Done():
				top.mu.RUnlock()
				return ctx.Err()
			case g.queue <- &deliveryTask{
				topic:     topic,
				group:     g,
				msg:       m,
				tr:        t,
				createdAt: time.Now(),
			}:
				// Message queued
			default:
				// Queue full: try blocking send to preserve ordering
				select {
				case g.queue <- &deliveryTask{
					topic:     topic,
					group:     g,
					msg:       m,
					tr:        t,
					createdAt: time.Now(),
				}:
					// Message queued after blocking
				case <-ctx.Done():
					top.mu.RUnlock()
					return ctx.Err()
				}
			}
		}
		top.mu.RUnlock()

		t.metrics.published.Add(1)
	}

	return nil
}

// Subscribe registers a handler for a topic/group with configurable concurrency.
func (t *Transport) Subscribe(ctx context.Context, topic, group string, handler func(xbus.Delivery)) (xbus.Subscription, error) {
	if t.closed.Load() {
		return nil, errors.New("memory transport is closed")
	}

	top := t.ensureTopic(topic)
	g := top.ensureGroup(group, t.cfg.BufferSize)

	innerCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	// Spawn configured number of worker goroutines
	workers := t.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.worker(innerCtx, g, handler)
		}()
	}

	return &subscription{
		close: func() error {
			cancel()
			wg.Wait()
			// Keep group and queue alive for other subscribers
			return nil
		},
	}, nil
}

// worker processes messages from the group queue.
func (t *Transport) worker(ctx context.Context, g *group, handler func(xbus.Delivery)) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-g.queue:
			if task == nil {
				continue // Shouldn't happen; we never close the queue
			}

			d := &memDelivery{
				task:    task,
				acked:   false,
				ackOnce: sync.Once{},
				tr:      task.tr,
			}

			t.metrics.consumed.Add(1)
			handler(d)
		}
	}
}

// Close gracefully shuts down the transport.
func (t *Transport) Close(_ context.Context) error {
	if t.closed.Swap(true) {
		return nil // Already closed
	}

	t.mu.Lock()
	t.topics = make(map[string]*topic)
	t.mu.Unlock()

	return nil
}

// Stats returns transport telemetry.
type Stats struct {
	Published     uint64
	Consumed      uint64
	Acked         uint64
	Nacked        uint64
	Redelivered   uint64
	PublishErrors uint64
}

// Stats returns current transport metrics.
func (t *Transport) Stats() Stats {
	return Stats{
		Published:     t.metrics.published.Load(),
		Consumed:      t.metrics.consumed.Load(),
		Acked:         t.metrics.acked.Load(),
		Nacked:        t.metrics.nacked.Load(),
		Redelivered:   t.metrics.redelivered.Load(),
		PublishErrors: t.metrics.publishErrors.Load(),
	}
}

// Internal types

type subscription struct {
	close func() error
}

func (s *subscription) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}

type topic struct {
	mu     sync.RWMutex
	groups map[string]*group
}

type group struct {
	name  string
	queue chan *deliveryTask
}

type deliveryTask struct {
	tr        *Transport
	topic     string
	group     *group
	msg       *xbus.Message
	createdAt time.Time
}

type memDelivery struct {
	task    *deliveryTask
	acked   bool
	ackOnce sync.Once
	tr      *Transport
}

func (d *memDelivery) Message() *xbus.Message {
	return d.task.msg
}

// Ack marks the message as processed.
func (d *memDelivery) Ack(_ context.Context) error {
	d.ackOnce.Do(func() {
		d.acked = true
		d.tr.metrics.acked.Add(1)
	})
	return nil
}

// Nack negative-acknowledges the message for redelivery.
func (d *memDelivery) Nack(ctx context.Context, _ error) error {
	d.ackOnce.Do(func() {
		d.tr.metrics.nacked.Add(1)
		d.tr.metrics.redelivered.Add(1)

		// Re-enqueue for redelivery after configured delay
		delay := d.tr.cfg.RedeliveryDelay
		if delay <= 0 {
			// Immediate requeue
			select {
			case d.task.group.queue <- d.task:
			case <-ctx.Done():
			}
			return
		}

		// Delayed requeue
		timer := time.NewTimer(delay)
		go func() {
			defer timer.Stop()
			select {
			case <-timer.C:
				// Best-effort requeue after delay
				select {
				case d.task.group.queue <- d.task:
				case <-ctx.Done():
				}
			case <-ctx.Done():
			}
		}()
	})
	return nil
}

// Helper functions

func (t *Transport) ensureTopic(name string) *topic {
	t.mu.Lock()
	defer t.mu.Unlock()

	if tp, ok := t.topics[name]; ok {
		return tp
	}

	tp := &topic{
		groups: make(map[string]*group),
	}
	t.topics[name] = tp
	return tp
}

func (tp *topic) ensureGroup(name string, bufferSize int) *group {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if g, ok := tp.groups[name]; ok {
		return g
	}

	g := &group{
		name:  name,
		queue: make(chan *deliveryTask, bufferSize),
	}
	tp.groups[name] = g
	return g
}

// Simple monotonic ID generator (not distributed; dev/testing only).
var idSeq uint64

func nextID() string {
	n := atomic.AddUint64(&idSeq, 1)
	return fmt.Sprintf("mem-%d", n)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
