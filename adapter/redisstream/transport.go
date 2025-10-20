package redisstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trickstertwo/xbus"
)

type transport struct {
	cfg    Config
	client *redis.Client

	closeOnce sync.Once
	closed    atomic.Bool

	// delivery pool to reduce per-message allocations
	dpool sync.Pool

	// metrics for observability
	metrics *transportMetrics
}

// transportMetrics tracks performance telemetry
type transportMetrics struct {
	published     atomic.Uint64
	consumed      atomic.Uint64
	acked         atomic.Uint64
	nacked        atomic.Uint64
	poolHits      atomic.Uint64
	poolMisses    atomic.Uint64
	publishErrors atomic.Uint64
	consumeErrors atomic.Uint64
}

func NewTransport(cfg Config) (xbus.Transport, error) {
	opts := &redis.Options{
		Addr:         cfg.Addr,
		Username:     cfg.Username,
		Password:     cfg.Password,
		DB:           cfg.DB,
		MaxRetries:   3,
		PoolSize:     10,
		MinIdleConns: 5,
	}

	if cfg.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion:    tls.VersionTLS12,
			ServerName:    cfg.TLSServerName,
			Renegotiation: tls.RenegotiateNever,
		}
	}

	client := redis.NewClient(opts)
	if err := ping(client); err != nil {
		return nil, err
	}

	t := &transport{
		cfg:     cfg,
		client:  client,
		metrics: &transportMetrics{},
		dpool: sync.Pool{
			New: func() interface{} { return new(delivery) },
		},
	}

	return t, nil
}

// Publish sends messages to a topic using Redis XADD (pipelined for batch efficiency).
func (t *transport) Publish(ctx context.Context, topic string, msgs ...*xbus.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	pipe := t.client.Pipeline()

	for _, m := range msgs {
		// Pre-size map to reduce rehashing: id, name, payload, producedAt + metadata
		vals := make(map[string]any, 4+len(m.Metadata))

		if m.ID != "" {
			vals[fieldID] = m.ID
		}
		vals[fieldName] = m.Name
		// raw payload bytes (binary-safe, no base64 encoding overhead)
		vals[fieldPayload] = m.Payload
		vals[fieldProducedAt] = m.ProducedAt.UnixNano()

		// Flatten metadata to avoid nested map allocations
		for k, v := range m.Metadata {
			vals[fieldMetaPrefix+k] = v
		}

		args := &redis.XAddArgs{
			Stream: topic,
			ID:     "*", // Let Redis generate ID
			Values: vals,
		}

		// Approximate trimming to keep stream bounded
		if t.cfg.MaxLenApprox > 0 {
			args.MaxLen = t.cfg.MaxLenApprox
			args.Approx = true
		}

		pipe.XAdd(ctx, args)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		t.metrics.publishErrors.Add(uint64(len(msgs)))
		return err
	}

	t.metrics.published.Add(uint64(len(msgs)))
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

// Subscribe listens to a topic/group with configurable concurrency and batching.
// Optimized with delivery pooling and efficient event handling.
func (t *transport) Subscribe(ctx context.Context, topic, group string, handler func(xbus.Delivery)) (xbus.Subscription, error) {
	// Ensure consumer group exists (idempotent)
	if t.cfg.AutoCreate {
		if err := t.client.XGroupCreateMkStream(ctx, topic, group, "$").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			// Continue even on error; group may already exist or be created concurrently
		}
	}

	innerCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	// Worker pool configuration
	workers := t.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}

	// Buffered work channel (buffer = 2x workers for burst absorption)
	workCh := make(chan xbus.Delivery, workers*2)

	// Start worker goroutines for concurrent handling
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range workCh {
				if d != nil {
					handler(d)
					// Return delivery object to pool immediately after use
					if md, ok := d.(*delivery); ok {
						t.releaseDelivery(md)
					}
				}
			}
		}()
	}

	// Poller goroutine (reads from Redis, distributes to workers)
	pollerDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer func() {
			close(workCh) // Signal workers to exit
			close(pollerDone)
			wg.Done()
		}()

		t.pollerLoop(innerCtx, topic, group, workCh)
	}()

	// Optional pending entry recovery loop (claims messages stuck on other consumers)
	var claimCancel context.CancelFunc
	if t.cfg.ClaimMinIdle > 0 && t.cfg.ClaimInterval > 0 && t.cfg.ClaimBatch > 0 {
		var claimCtx context.Context
		claimCtx, claimCancel = context.WithCancel(innerCtx)
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.claimLoop(claimCtx, topic, group)
		}()
	}

	return &subscription{
		close: func() error {
			cancel()
			if claimCancel != nil {
				claimCancel()
			}
			<-pollerDone
			wg.Wait()
			return nil
		},
	}, nil
}

// pollerLoop reads from Redis Streams and distributes messages to workers.
func (t *transport) pollerLoop(ctx context.Context, topic, group string, workCh chan<- xbus.Delivery) {
	streams := []string{topic, ">"}
	xArgs := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: t.cfg.Consumer,
		Streams:  streams,
		Count:    int64(_max(1, t.cfg.BatchSize)),
		Block:    t.cfg.Block,
		NoAck:    false,
	}

	backoff := time.Millisecond * 100
	maxBackoff := time.Second * 5

	for {
		// Fast exit on context cancellation
		select {
		case <-ctx.Done():
			return
		default:
		}

		res, err := t.client.XReadGroup(ctx, xArgs).Result()
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}

			if errors.Is(err, redis.Nil) {
				// Block timeout (expected), continue polling
				backoff = time.Millisecond * 100
				continue
			}

			// Transient error: exponential backoff with jitter
			t.metrics.consumeErrors.Add(1)
			select {
			case <-time.After(backoff):
				backoff = _min(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			}
			continue
		}

		// Reset backoff on successful read
		backoff = time.Millisecond * 100

		// Process all messages in the result
		for _, stream := range res {
			for _, msg := range stream.Messages {
				d := t.newDelivery()
				d.t = t
				d.topic = topic
				d.group = group
				d.id = msg.ID
				d.msg = decodeMessage(msg.ID, msg.Values)
				d.onceAck = &sync.Once{}

				t.metrics.consumed.Add(1)

				select {
				case workCh <- d:
					// Message queued for processing
				case <-ctx.Done():
					t.releaseDelivery(d)
					return
				}
			}
		}
	}
}

// newDelivery gets a delivery from the pool or allocates a new one.
func (t *transport) newDelivery() *delivery {
	v := t.dpool.Get()
	if v == nil {
		t.metrics.poolMisses.Add(1)
		return &delivery{}
	}

	t.metrics.poolHits.Add(1)
	d := v.(*delivery)

	// Reset fields
	d.t = nil
	d.topic = ""
	d.group = ""
	d.id = ""
	d.msg = nil
	d.onceAck = nil

	return d
}

// releaseDelivery returns a delivery to the pool after clearing references.
func (t *transport) releaseDelivery(d *delivery) {
	if d == nil {
		return
	}

	// Clear references to aid GC
	d.t = nil
	d.msg = nil
	d.topic = ""
	d.group = ""
	d.id = ""
	d.onceAck = nil

	t.dpool.Put(d)
}

// claimLoop periodically claims pending messages from dead consumers.
// Enables automatic recovery from consumer crashes.
func (t *transport) claimLoop(ctx context.Context, topic, group string) {
	ticker := time.NewTicker(t.cfg.ClaimInterval)
	defer ticker.Stop()

	batch := int64(_max(1, t.cfg.ClaimBatch))
	minIdle := t.cfg.ClaimMinIdle
	consumer := t.cfg.Consumer

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// Get pending messages that haven't been acked and are idle > minIdle
		pending, err := t.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: topic,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  batch,
			Idle:   minIdle,
		}).Result()

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, redis.Nil) {
				continue
			}
			continue
		}

		if len(pending) == 0 {
			continue
		}

		// Claim these messages (reassign to this consumer)
		ids := make([]string, 0, len(pending))
		for _, p := range pending {
			ids = append(ids, p.ID)
		}

		_, _ = t.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   topic,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdle,
			Messages: ids,
		}).Result()
	}
}

// Close gracefully shuts down the transport.
func (t *transport) Close(_ context.Context) error {
	if t.closed.Swap(true) {
		return nil // Already closed
	}

	return t.client.Close()
}

// Helper functions

func ping(c *redis.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := c.Ping(ctx).Result()
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			return fmt.Errorf("redis ping timeout: %w", err)
		}
		return err
	}

	if strings.ToUpper(res) != "PONG" {
		return fmt.Errorf("unexpected redis ping result: %s", res)
	}

	return nil
}

func _max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func _min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
