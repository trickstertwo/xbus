package redisstream

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/trickstertwo/xbus"
)

// Adapter: Redis Streams Transport (Strategy + Adapter patterns)

const TransportName = "redis-streams"

func init() {
	// Fail fast if registration cannot be completed.
	if err := xbus.RegisterTransport(TransportName, func(cfg map[string]any) (xbus.Transport, error) {
		return NewTransport(ConfigFromMap(cfg))
	}); err != nil {
		panic(fmt.Errorf("xbus: failed to register transport %q: %w", TransportName, err))
	}
}

// Config for Redis Streams transport.
type Config struct {
	// Client options
	Addr          string
	Username      string
	Password      string
	DB            int
	TLS           bool
	TLSServerName string // optional SNI if TLS is enabled

	// Consumer options
	Group       string
	Consumer    string
	Concurrency int           // number of concurrent handler workers
	BatchSize   int           // XREADGROUP COUNT
	Block       time.Duration // XREADGROUP BLOCK
	AutoCreate  bool          // auto-create consumer group if missing

	// Acknowledgment & stream trimming
	AutoDeleteOnAck bool   // whether to XDEL after XACK
	DeadLetter      string // optional dead-letter stream
	MaxLenApprox    int64  // XADD MAXLEN ~ approx; 0 disables trimming

	// Pending entries recovery (optional)
	ClaimMinIdle  time.Duration // only claim messages idle for at least this duration; 0 disables claim loop
	ClaimBatch    int           // number of PEL entries to claim per round
	ClaimInterval time.Duration // interval for claim loop
}

// ConfigFromMap safely converts cfg into Config with defaults.
func ConfigFromMap(cfg map[string]any) Config {
	getString := func(k, d string) string {
		if v, ok := cfg[k].(string); ok && v != "" {
			return v
		}
		return d
	}
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
		}
		return d
	}
	getInt64 := func(k string, d int64) int64 {
		switch v := cfg[k].(type) {
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		}
		return d
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
			// Allow raw nanoseconds if provided numerically
			return time.Duration(v)
		}
		return d
	}

	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "xbus"
	}

	return Config{
		Addr:          getString("addr", "127.0.0.1:6379"),
		Username:      getString("username", ""),
		Password:      getString("password", ""),
		DB:            getInt("db", 0),
		TLS:           getBool("tls", false),
		TLSServerName: getString("tls_server_name", ""), // optional

		Group:       getString("group", "xbus"),
		Consumer:    getString("consumer", fmt.Sprintf("xbus-%s-%d", hostname, os.Getpid())),
		Concurrency: getInt("concurrency", 8),
		BatchSize:   getInt("batch_size", 128),
		Block:       getDur("block", 5*time.Second),
		AutoCreate:  getBool("auto_create", true),

		AutoDeleteOnAck: getBool("auto_delete_on_ack", false),
		DeadLetter:      getString("dead_letter", ""),
		MaxLenApprox:    getInt64("max_len_approx", 0),

		ClaimMinIdle:  getDur("claim_min_idle", 0),
		ClaimBatch:    getInt("claim_batch", 128),
		ClaimInterval: getDur("claim_interval", 15*time.Second),
	}
}

type transport struct {
	cfg    Config
	client *redis.Client

	closeOnce sync.Once
	closed    chan struct{}
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
			ServerName:    cfg.TLSServerName, // empty is fine; Redis often doesn't use SNI; set when needed
			Renegotiation: tls.RenegotiateNever,
		}
	}
	// Eager dial to fail fast on misconfiguration
	client := redis.NewClient(opts)
	if err := ping(client); err != nil {
		return nil, err
	}
	return &transport{
		cfg:    cfg,
		client: client,
		closed: make(chan struct{}),
	}, nil
}

func (t *transport) Publish(ctx context.Context, topic string, msgs ...*xbus.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	pipe := t.client.Pipeline()
	for _, m := range msgs {
		args := &redis.XAddArgs{
			Stream: topic,
			ID:     "*", // let Redis assign
			Values: encodeMessage(m),
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

func encodeMessage(m *xbus.Message) map[string]any {
	values := map[string]any{
		"id":         m.ID,
		"name":       m.Name,
		"payload_b":  base64.StdEncoding.EncodeToString(m.Payload),
		"producedAt": m.ProducedAt.UnixNano(),
	}
	if len(m.Metadata) > 0 {
		for k, v := range m.Metadata {
			values["meta:"+k] = v
		}
	}
	return values
}

func decodeMessage(id string, vals map[string]any) *xbus.Message {
	msg := &xbus.Message{
		ID:       id,
		Name:     asString(vals["name"]),
		Metadata: map[string]string{},
	}
	if pb64 := asString(vals["payload_b"]); pb64 != "" {
		if b, err := base64.StdEncoding.DecodeString(pb64); err == nil {
			msg.Payload = b
		}
	}
	if pa := vals["producedAt"]; pa != nil {
		if ns, ok := toInt64(pa); ok && ns > 0 {
			msg.ProducedAt = time.Unix(0, ns)
		}
	}
	for k, v := range vals {
		if strings.HasPrefix(k, "meta:") {
			msg.Metadata[strings.TrimPrefix(k, "meta:")] = asString(v)
		}
	}
	return msg
}

func asString(v any) string {
	switch s := v.(type) {
	case string:
		return s
	case []byte:
		return string(s)
	default:
		return fmt.Sprintf("%v", s)
	}
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case string:
		if n == "" {
			return 0, false
		}
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, true
		}
		// Some RESP decoders may send float-ish
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int64(f), true
		}
	case []byte:
		return toInt64(string(n))
	}
	return 0, false
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
	// Ensure group exists if autovivify.
	if t.cfg.AutoCreate {
		// "$" starts from new messages; "0" from beginning. We use "$" for new message consumption.
		_ = t.client.XGroupCreateMkStream(ctx, topic, group, "$").Err()
	}

	innerCtx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}

	// Work channel and workers
	workers := t.cfg.Concurrency
	if workers < 1 {
		workers = 1
	}
	workCh := make(chan *delivery, workers*2)

	// workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range workCh {
				handler(d)
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
		for {
			// Exit ASAP on context cancellation.
			select {
			case <-innerCtx.Done():
				return
			default:
			}

			streams := []string{topic, ">"}
			res, err := t.client.XReadGroup(innerCtx, &redis.XReadGroupArgs{
				Group:    group,
				Consumer: t.cfg.Consumer,
				Streams:  streams,
				Count:    int64(m(1, t.cfg.BatchSize)),
				Block:    t.cfg.Block,
				NoAck:    false,
			}).Result()

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

			for _, s := range res {
				for _, x := range s.Messages {
					d := &delivery{
						t:       t,
						topic:   topic,
						group:   group,
						id:      x.ID,
						msg:     decodeMessage(x.ID, x.Values),
						onceAck: &sync.Once{},
					}
					select {
					case workCh <- d:
					case <-innerCtx.Done():
						return
					}
				}
			}
		}
	}()

	// optional claim loop for stuck pending entries
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
			// stop claim loop if any
			if claimCancel != nil {
				claimCancel()
			}
			// wait for poller to finish and close workCh, then workers drain and exit
			<-pollerDone
			wg.Wait()
			return nil
		},
	}, nil
}

type delivery struct {
	t     *transport
	topic string
	group string
	id    string
	msg   *xbus.Message

	onceAck *sync.Once
}

func (d *delivery) Message() *xbus.Message { return d.msg }

func (d *delivery) Ack(ctx context.Context) error {
	var err error
	d.onceAck.Do(func() {
		err = d.t.client.XAck(ctx, d.topic, d.group, d.id).Err()
		if err == nil && d.t.cfg.AutoDeleteOnAck {
			_ = d.t.client.XDel(ctx, d.topic, d.id).Err()
		}
	})
	return err
}

func (d *delivery) Nack(ctx context.Context, reason error) error {
	// Redis Streams doesn't have explicit NACK. Leaving message pending causes redelivery.
	// Optionally forward to dead-letter and ack original to prevent poison-pill loops.
	if dl := d.t.cfg.DeadLetter; dl != "" {
		values := map[string]any{
			"orig_topic": d.topic,
			"orig_id":    d.id,
			"error":      fmt.Sprintf("%v", reason),
			"name":       d.msg.Name,
			"payload_b":  base64.StdEncoding.EncodeToString(d.msg.Payload),
		}
		for k, v := range d.msg.Metadata {
			values["meta:"+k] = v
		}
		_ = d.t.client.XAdd(ctx, &redis.XAddArgs{
			Stream: dl,
			ID:     "*",
			Values: values,
		}).Err()
		// acknowledge original to avoid infinite retry on poison message
		return d.Ack(ctx)
	}
	// No-op (leave pending) to allow subsequent delivery via pending processing policies.
	return nil
}

func (t *transport) Close(ctx context.Context) error {
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

		// fetch pending IDs (older than minIdle); we use XPENDING summary+range
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
		for _, p := range sum {
			ids = append(ids, p.ID)
		}
		// XCLAIM to this consumer
		claimed, err := t.client.XClaimJustID(ctx, &redis.XClaimArgs{
			Stream:   topic,
			Group:    group,
			Consumer: consumer,
			MinIdle:  minIdle,
			Messages: ids,
		}).Result()
		if err != nil || len(claimed) == 0 {
			continue
		}
		// Claimed messages will be delivered by XREADGROUP to this consumer in subsequent polls.
	}
}

func ping(c *redis.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := c.Ping(ctx).Result()
	if err != nil {
		// Provide clearer error on DNS/TLS issues
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
