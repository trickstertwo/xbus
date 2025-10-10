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
		t := NewTransport(ConfigFromMap(cfg))
		return t, nil
	}); err != nil {
		panic(fmt.Errorf("xbus/memory: failed to register transport: %w", err))
	}
}

// Config controls memory transport behavior.
type Config struct {
	// BufferSize is the per-group queue size.
	BufferSize int
	// Concurrency is the default number of worker goroutines per subscription.
	Concurrency int
	// RedeliveryDelay is the delay before re-enqueuing a message on Nack. 0 means immediate requeue.
	RedeliveryDelay time.Duration
	// AssignIDs instructs the transport to assign IDs for messages that have empty ID.
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
		BufferSize:      getInt("buffer_size", 1024),
		Concurrency:     getInt("concurrency", 1),
		RedeliveryDelay: getDur("redelivery_delay", 0),
		AssignIDs:       getBool("assign_ids", true),
	}
}

type Transport struct {
	cfg Config

	mu     sync.RWMutex
	topics map[string]*topic

	closed atomic.Bool
}

var _ xbus.Transport = (*Transport)(nil)

func NewTransport(cfg Config) *Transport {
	if cfg.BufferSize < 1 {
		cfg.BufferSize = 1024
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	return &Transport{
		cfg:    cfg,
		topics: make(map[string]*topic),
	}
}

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
		// No topic => no subscribers => drop (dev semantics).
		return nil
	}

	for _, m := range msgs {
		if m == nil {
			continue
		}
		if t.cfg.AssignIDs && m.ID == "" {
			m.ID = nextID()
		}
		// Fan-out to consumer groups where each group does one-of-N delivery.
		top.mu.RLock()
		for _, g := range top.groups {
			// Copy pointer for safety if caller reuses structure; we reuse the same message for all groups.
			select {
			case g.queue <- &deliveryTask{
				topic: topic,
				group: g,
				msg:   m,
				tr:    t,
			}:
			case <-ctx.Done():
				top.mu.RUnlock()
				return ctx.Err()
			default:
				// If queue is full, block respecting ctx to preserve ordering while not deadlocking indefinitely.
				select {
				case g.queue <- &deliveryTask{topic: topic, group: g, msg: m, tr: t}:
				case <-ctx.Done():
					top.mu.RUnlock()
					return ctx.Err()
				}
			}
		}
		top.mu.RUnlock()
	}
	return nil
}

func (t *Transport) Subscribe(ctx context.Context, topic, group string, handler func(xbus.Delivery)) (xbus.Subscription, error) {
	if t.closed.Load() {
		return nil, errors.New("memory transport is closed")
	}

	top := t.ensureTopic(topic)
	g := top.ensureGroup(group, t.cfg.BufferSize)

	// subscription context
	innerCtx, cancel := context.WithCancel(ctx)

	// use configured concurrency; allow override via ctx value if needed in future
	workers := t.cfg.Concurrency
	wg := &sync.WaitGroup{}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-innerCtx.Done():
					return
				case task := <-g.queue:
					if task == nil {
						// Shouldn't happen; we never close the queue.
						continue
					}
					d := &memDelivery{
						task:    task,
						acked:   false,
						ackOnce: sync.Once{},
					}
					handler(d)
				}
			}
		}()
	}

	return &subscription{
		close: func() error {
			cancel()
			wg.Wait()
			// Keep group and queue alive for other subscribers.
			return nil
		},
	}, nil
}

func (t *Transport) Close(_ context.Context) error {
	t.closed.Store(true)
	// Do not close queues: active subscriptions may still be draining via their own contexts.
	// Best-effort cleanup: drop topic references so further Publishes fail early due to closed flag.
	t.mu.Lock()
	t.topics = make(map[string]*topic)
	t.mu.Unlock()
	return nil
}

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
	tr    *Transport
	topic string
	group *group
	msg   *xbus.Message
}

type memDelivery struct {
	task    *deliveryTask
	acked   bool
	ackOnce sync.Once
}

func (d *memDelivery) Message() *xbus.Message { return d.task.msg }

func (d *memDelivery) Ack(_ context.Context) error {
	d.ackOnce.Do(func() {
		d.acked = true
	})
	return nil
}

func (d *memDelivery) Nack(ctx context.Context, _ error) error {
	d.ackOnce.Do(func() {
		// Re-enqueue for redelivery after configured delay.
		delay := d.task.tr.cfg.RedeliveryDelay
		if delay <= 0 {
			select {
			case d.task.group.queue <- d.task:
			case <-ctx.Done():
			}
			return
		}
		timer := time.NewTimer(delay)
		go func() {
			defer timer.Stop()
			select {
			case <-timer.C:
				// Best effort requeue; if queue is full, block unless ctx canceled.
				select {
				case d.task.group.queue <- d.task:
				case <-ctx.Done():
				}
			case <-ctx.Done():
				return
			}
		}()
	})
	return nil
}

func (t *Transport) ensureTopic(name string) *topic {
	t.mu.Lock()
	defer t.mu.Unlock()
	if tp, ok := t.topics[name]; ok {
		return tp
	}
	tp := &topic{groups: make(map[string]*group)}
	t.topics[name] = tp
	return tp
}

func (tp *topic) ensureGroup(name string, buffer int) *group {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if g, ok := tp.groups[name]; ok {
		return g
	}
	g := &group{
		name:  name,
		queue: make(chan *deliveryTask, buffer),
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
