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

// transport implements xbus.Transport using Redis Streams.
type transport struct {
	cfg    Config
	client *redis.Client

	closeOnce sync.Once
	closed    atomic.Bool

	// delivery pool reduces per-message allocations
	dpool sync.Pool

	// metrics for observability
	metrics *metrics
}

// metrics tracks Redis Streams performance.
type metrics struct {
	published atomic.Uint64
	consumed  atomic.Uint64
	acked     atomic.Uint64
	nacked    atomic.Uint64
	errors    atomic.Uint64
}

// NewTransport creates a Redis Streams transport with validated config.
func NewTransport(cfg Config) (xbus.Transport, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

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
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &transport{
		cfg:     cfg,
		client:  client,
		metrics: &metrics{},
		dpool: sync.Pool{
			New: func() interface{} { return &delivery{onceAck: &sync.Once{}} },
		},
	}, nil
}

// Publish sends messages to a topic using pipelined XADD (batch efficient).
func (t *transport) Publish(ctx context.Context, topic string, msgs ...*xbus.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	pipe := t.client.Pipeline()

	for _, m := range msgs {
		vals := map[string]any{
			fieldName:       m.Name,
			fieldPayload:    m.Payload,
			fieldProducedAt: m.ProducedAt.UnixNano(),
		}

		if m.ID != "" {
			vals[fieldID] = m.ID
		}

		// Flatten metadata
		for k, v := range m.Metadata {
			vals[fieldMetaPrefix+k] = v
		}

		args := &redis.XAddArgs{
			Stream: topic,
			ID:     "*",
			Values: vals,
		}

		if t.cfg.MaxLenApprox > 0 {
			args.MaxLen = t.cfg.MaxLenApprox
			args.Approx = true
		}

		pipe.XAdd(ctx, args)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		t.metrics.errors.Add(uint64(len(msgs)))
		return fmt.Errorf("publish failed: %w", err)
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

// Subscribe listens to a topic with concurrent message handling.
func (t *transport) Subscribe(ctx context.Context, topic, group string, handler func(xbus.Delivery)) (xbus.Subscription, error) {
	// Ensure consumer group exists (idempotent)
	if t.cfg.AutoCreate {
		_ = t.client.XGroupCreateMkStream(ctx, topic, group, "$").Err()
	}

	innerCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	workers := t.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}

	// Work channel with burst absorption buffer
	workCh := make(chan xbus.Delivery, workers*2)

	// Worker goroutines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range workCh {
				if d != nil {
					handler(d)
					if md, ok := d.(*delivery); ok {
						t.releaseDelivery(md)
					}
				}
			}
		}()
	}

	// Poller goroutine (reads from Redis)
	pollerDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer func() {
			close(workCh)
			close(pollerDone)
			wg.Done()
		}()
		t.pollerLoop(innerCtx, topic, group, workCh)
	}()

	// Optional pending entry recovery (crash recovery)
	var claimCancel context.CancelFunc
	if t.cfg.ClaimMinIdle > 0 && t.cfg.ClaimInterval > 0 {
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

// pollerLoop reads from Redis Streams and distributes to workers.
func (t *transport) pollerLoop(ctx context.Context, topic, group string, workCh chan<- xbus.Delivery) {
	xArgs := &redis.XReadGroupArgs{
		Group:    group,
		Consumer: t.cfg.Consumer,
		Streams:  []string{topic, ">"},
		Count:    int64(t.cfg.BatchSize),
		Block:    t.cfg.Block,
		NoAck:    false,
	}

	backoff := 100 * time.Millisecond
	const maxBackoff = 5 * time.Second

	for {
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
				backoff = 100 * time.Millisecond
				continue
			}

			t.metrics.errors.Add(1)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-ctx.Done():
				return
			}
			continue
		}

		backoff = 100 * time.Millisecond

		for _, stream := range res {
			for _, msg := range stream.Messages {
				d := t.newDelivery()
				d.t = t
				d.topic = topic
				d.group = group
				d.id = msg.ID
				d.msg = decodeMessage(msg.ID, msg.Values)

				t.metrics.consumed.Add(1)

				select {
				case workCh <- d:
				case <-ctx.Done():
					t.releaseDelivery(d)
					return
				}
			}
		}
	}
}

// claimLoop periodically claims pending messages from dead consumers.
func (t *transport) claimLoop(ctx context.Context, topic, group string) {
	ticker := time.NewTicker(t.cfg.ClaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pending, err := t.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: topic,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  int64(t.cfg.ClaimBatch),
			Idle:   t.cfg.ClaimMinIdle,
		}).Result()

		if err != nil || len(pending) == 0 {
			continue
		}

		ids := make([]string, len(pending))
		for i, p := range pending {
			ids[i] = p.ID
		}

		_, _ = t.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   topic,
			Group:    group,
			Consumer: t.cfg.Consumer,
			MinIdle:  t.cfg.ClaimMinIdle,
			Messages: ids,
		}).Result()
	}
}

// Close gracefully shuts down the transport.
func (t *transport) Close(_ context.Context) error {
	if t.closed.Swap(true) {
		return nil
	}
	return t.client.Close()
}

// Delivery pool management

func (t *transport) newDelivery() *delivery {
	if v := t.dpool.Get(); v != nil {
		return v.(*delivery)
	}
	return &delivery{onceAck: &sync.Once{}}
}

func (t *transport) releaseDelivery(d *delivery) {
	if d == nil {
		return
	}
	d.t = nil
	d.msg = nil
	d.topic = ""
	d.group = ""
	d.id = ""
	t.dpool.Put(d)
}

// Helpers

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
		return fmt.Errorf("unexpected redis response: %s", res)
	}

	return nil
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
