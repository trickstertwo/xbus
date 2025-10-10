package redisstream

import (
	"context"
	"crypto/tls"
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
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Adapter: Redis Streams Transport (Strategy + Adapter patterns)

const TransportName = "redis-streams"

func init() {
	if err := xbus.RegisterTransport(TransportName, func(cfg map[string]any) (xbus.Transport, error) {
		return NewTransport(ConfigFromMap(cfg))
	}); err != nil {
		panic(fmt.Errorf("xbus: failed to register transport %q: %w", TransportName, err))
	}
}

// Field constants (avoid typos/allocs)
const (
	fieldID         = "id"
	fieldName       = "name"
	fieldPayload    = "payload"    // raw []byte to reduce allocs (no base64)
	fieldProducedAt = "producedAt" // int64 ns
	fieldMetaPrefix = "meta:"
)

// Config for Redis Streams transport.
type Config struct {
	// Client options
	Addr          string
	Username      string
	Password      string
	DB            int
	TLS           bool
	TLSServerName string

	// Consumer options
	Group       string
	Consumer    string
	Concurrency int
	BatchSize   int
	Block       time.Duration
	AutoCreate  bool

	// Acknowledgment & stream trimming
	AutoDeleteOnAck bool
	DeadLetter      string
	MaxLenApprox    int64

	// Pending entries recovery (optional)
	ClaimMinIdle  time.Duration
	ClaimBatch    int
	ClaimInterval time.Duration
}

// toMap converts typed Config into the generic map expected by the transport factory.
func (c Config) toMap() map[string]any {
	m := map[string]any{
		"addr":               c.Addr,
		"username":           c.Username,
		"password":           c.Password,
		"db":                 c.DB,
		"tls":                c.TLS,
		"tls_server_name":    c.TLSServerName,
		"group":              c.Group,
		"consumer":           c.Consumer,
		"concurrency":        c.Concurrency,
		"batch_size":         c.BatchSize,
		"block":              c.Block,
		"auto_create":        c.AutoCreate,
		"auto_delete_on_ack": c.AutoDeleteOnAck,
		"dead_letter":        c.DeadLetter,
		"max_len_approx":     c.MaxLenApprox,
		"claim_min_idle":     c.ClaimMinIdle,
		"claim_batch":        c.ClaimBatch,
		"claim_interval":     c.ClaimInterval,
	}
	return m
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
		TLSServerName: getString("tls_server_name", ""),

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

// Option configures the xbus.Bus construction when calling Use.
type Option func(*xbus.BusBuilder)

// WithLogger injects a custom xlog logger.
func WithLogger(l *xlog.Logger) Option {
	return func(b *xbus.BusBuilder) { b.WithLogger(l) }
}

// WithClock injects a custom xclock clock.
func WithClock(c xclock.Clock) Option {
	return func(b *xbus.BusBuilder) { b.WithClock(c) }
}

// WithCodec selects a codec by name (default: json).
func WithCodec(name string) Option {
	return func(b *xbus.BusBuilder) { b.WithCodec(name) }
}

// WithMiddleware adds processing middlewares.
func WithMiddleware(mw ...xbus.Middleware) Option {
	return func(b *xbus.BusBuilder) { b.WithMiddleware(mw...) }
}

// WithObserver attaches observers for lifecycle events.
func WithObserver(obs ...xbus.Observer) Option {
	return func(b *xbus.BusBuilder) { b.WithObserver(obs...) }
}

// WithAckTimeout sets acks/nacks timeout.
func WithAckTimeout(d time.Duration) Option {
	return func(b *xbus.BusBuilder) { b.WithAckTimeout(d) }
}

// Use builds and sets the default xbus.Bus using Redis Streams and returns it,
// mirroring xlog/zerolog.Use for clear, explicit initialization.
//
// It fails fast by panicking if construction fails (production-friendly when
// transport must be available at startup).
func Use(cfg Config, opts ...Option) *xbus.Bus {
	bus, err := xbus.Default(func(b *xbus.BusBuilder) {
		// Prefer typed config; internally go through the factory with a map to avoid extra coupling.
		b.WithTransport(TransportName, cfg.toMap())
		for _, o := range opts {
			if o != nil {
				o(b)
			}
		}
	})
	if err != nil {
		panic(fmt.Errorf("redisstream.Use: %w", err))
	}
	return bus
}

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

func decodeMessage(id string, vals map[string]any) *xbus.Message {
	msg := &xbus.Message{
		ID:       id,
		Metadata: nil, // lazily allocate when we find meta entries
	}
	if v, ok := vals[fieldName]; ok {
		msg.Name = asString(v)
	}
	if v, ok := vals[fieldPayload]; ok {
		switch p := v.(type) {
		case []byte:
			msg.Payload = p
		case string:
			msg.Payload = []byte(p)
		}
	}
	if pa := vals[fieldProducedAt]; pa != nil {
		if ns, ok := toInt64(pa); ok && ns > 0 {
			msg.ProducedAt = time.Unix(0, ns)
		}
	}
	for k, v := range vals {
		if strings.HasPrefix(k, fieldMetaPrefix) {
			if msg.Metadata == nil {
				// Pre-size capacity heuristically (a handful of items)
				msg.Metadata = make(map[string]string, 4)
			}
			msg.Metadata[strings.TrimPrefix(k, fieldMetaPrefix)] = asString(v)
		}
	}
	if msg.Metadata == nil {
		msg.Metadata = map[string]string{}
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
	// Redis Streams doesn't have explicit NACK. Dead-letter optionally, then ack original to avoid poison loops.
	if dl := d.t.cfg.DeadLetter; dl != "" {
		values := make(map[string]any, 4+len(d.msg.Metadata))
		values["orig_topic"] = d.topic
		values["orig_id"] = d.id
		values["error"] = fmt.Sprintf("%v", reason)
		values[fieldName] = d.msg.Name
		values[fieldPayload] = d.msg.Payload
		for k, v := range d.msg.Metadata {
			values[fieldMetaPrefix+k] = v
		}
		_ = d.t.client.XAdd(ctx, &redis.XAddArgs{
			Stream: dl,
			ID:     "*",
			Values: values,
		}).Err()
		// acknowledge original to avoid infinite retry on poison message
		return d.Ack(ctx)
	}
	// Leave pending to allow redelivery by consumer-group policies.
	return nil
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
