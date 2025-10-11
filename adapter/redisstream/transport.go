package redisstream

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trickstertwo/xbus"
)

type transport struct {
	cfg    Config
	client *redis.Client

	closeOnce sync.Once
	closed    chan struct{}

	// delivery pool to reduce per-message allocations
	dpool sync.Pool
}

func NewTransport(cfg Config) (xbus.Transport, error) {
	opts := &redis.Options{
		Addr:     cfg.Addr,
		Username: cfg.Username,
		Password: cfg.Password,
		DB:       cfg.DB,
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
		cfg:    cfg,
		client: client,
		closed: make(chan struct{}),
		dpool: sync.Pool{
			New: func() any { return new(delivery) },
		},
	}
	return t, nil
}

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
		// raw payload bytes (binary-safe, no base64)
		vals[fieldPayload] = m.Payload
		vals[fieldProducedAt] = m.ProducedAt.UnixNano()

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
	return err
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

func (t *transport) Subscribe(ctx context.Context, topic, group string, handler func(xbus.Delivery)) (xbus.Subscription, error) {
	// Ensure group exists if requested.
	if t.cfg.AutoCreate {
		// "$" starts from new messages; ignore BUSYGROUP errors.
		if err := t.client.XGroupCreateMkStream(ctx, topic, group, "$").Err(); err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			// Continue even on error; group may already exist or be created concurrently.
		}
	}

	innerCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	// Work channel and workers
	workers := t.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}
	workCh := make(chan xbus.Delivery, workers*2)

	// workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range workCh {
				handler(d)
				// Return delivery object to pool when our concrete type
				if md, ok := d.(*delivery); ok {
					t.releaseDelivery(md)
				}
			}
		}()
	}

	// poller
	pollerDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer func() {
			close(workCh) // signal workers to exit
			close(pollerDone)
			wg.Done()
		}()

		streams := []string{topic, ">"}
		xArgs := &redis.XReadGroupArgs{
			Group:    group,
			Consumer: t.cfg.Consumer,
			Streams:  streams,
			Count:    int64(m(1, t.cfg.BatchSize)),
			Block:    t.cfg.Block,
			NoAck:    false,
		}

		for {
			// Exit ASAP on context cancellation.
			select {
			case <-innerCtx.Done():
				return
			default:
			}

			res, err := t.client.XReadGroup(innerCtx, xArgs).Result()
			if err != nil {
				if errors.Is(err, context.Canceled) || innerCtx.Err() != nil {
					return
				}
				if errors.Is(err, redis.Nil) {
					// Block timeout, just loop again.
				} else {
					// transient errors: small backoff
					select {
					case <-time.After(200 * time.Millisecond):
					case <-innerCtx.Done():
						return
					}
				}
				continue
			}

			for i := range res {
				str := res[i]
				for j := range str.Messages {
					x := str.Messages[j]
					d := t.newDelivery()
					d.t = t
					d.topic = topic
					d.group = group
					d.id = x.ID
					d.msg = decodeMessage(x.ID, x.Values)
					d.onceAck = &sync.Once{}

					select {
					case workCh <- d:
					case <-innerCtx.Done():
						return
					}
				}
			}
		}
	}()

	// optional pending-claim loop
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

	// subscription close
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

func (t *transport) newDelivery() *delivery {
	d := t.dpool.Get().(*delivery)
	// zero the struct fields we set
	d.t = nil
	d.topic = ""
	d.group = ""
	d.id = ""
	d.msg = nil
	if d.onceAck == nil {
		d.onceAck = &sync.Once{}
	} else {
		// reset Once by replacing with a new one
		d.onceAck = &sync.Once{}
	}
	return d
}

func (t *transport) releaseDelivery(d *delivery) {
	// clear references to avoid leaks
	d.t = nil
	d.msg = nil
	t.dpool.Put(d)
}

func (t *transport) Close(_ context.Context) error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closed)
		err = t.client.Close()
	})
	return err
}

func (t *transport) claimLoop(ctx context.Context, topic, group string) {
	ticker := time.NewTicker(t.cfg.ClaimInterval)
	defer ticker.Stop()

	batch := int64(m(1, t.cfg.ClaimBatch))
	minIdle := t.cfg.ClaimMinIdle
	consumer := t.cfg.Consumer

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		sum, err := t.client.XPendingExt(ctx, &redis.XPendingExtArgs{
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
		if len(sum) == 0 {
			continue
		}

		ids := make([]string, 0, len(sum))
		for i := range sum {
			ids = append(ids, sum[i].ID)
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

func m(a, b int) int {
	if a > b {
		return a
	}
	return b
}
