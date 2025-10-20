package redisstream

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/trickstertwo/xbus"
)

// delivery implements xbus.Delivery for Redis Streams.
type delivery struct {
	t     *transport
	topic string
	group string
	id    string
	msg   *xbus.Message

	// Ensures Ack/Nack happens exactly once
	onceAck *sync.Once
}

func (d *delivery) Message() *xbus.Message {
	return d.msg
}

// Ack acknowledges a message, marking it as processed.
func (d *delivery) Ack(ctx context.Context) error {
	var err error
	d.onceAck.Do(func() {
		err = d.t.client.XAck(ctx, d.topic, d.group, d.id).Err()
		if err == nil {
			d.t.metrics.acked.Add(1)
			// Optionally delete from stream after ack (saves memory)
			if d.t.cfg.AutoDeleteOnAck {
				_ = d.t.client.XDel(ctx, d.topic, d.id).Err()
			}
		}
	})
	return err
}

// Nack negative-acknowledges a message (redelivery or dead-letter).
// Redis Streams has no explicit NACK; instead we:
// 1. Optionally write to dead-letter queue on error
// 2. Acknowledge the original to prevent poison loops
func (d *delivery) Nack(ctx context.Context, reason error) error {
	if dl := d.t.cfg.DeadLetter; dl != "" {
		// Write to dead-letter queue with metadata
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

		d.t.metrics.nacked.Add(1)
		// Acknowledge original to avoid infinite retry loops
		return d.Ack(ctx)
	}

	// No dead-letter: leave pending to allow Redis consumer group redelivery
	d.t.metrics.nacked.Add(1)
	return nil
}

// decodeMessage reconstructs an xbus.Message from Redis stream entry values.
func decodeMessage(id string, vals map[string]any) *xbus.Message {
	msg := &xbus.Message{
		ID:       id,
		Metadata: nil, // Lazy allocate when needed
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

	// Extract metadata fields
	for k, v := range vals {
		if strings.HasPrefix(k, fieldMetaPrefix) {
			if msg.Metadata == nil {
				// Pre-size to common case (3-4 metadata items)
				msg.Metadata = make(map[string]string, 4)
			}
			msg.Metadata[strings.TrimPrefix(k, fieldMetaPrefix)] = asString(v)
		}
	}

	// Ensure non-nil metadata map
	if msg.Metadata == nil {
		msg.Metadata = make(map[string]string)
	}

	return msg
}

// Helper functions for type conversion

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
		// Try integer parsing first (faster)
		if i, err := strconv.ParseInt(n, 10, 64); err == nil {
			return i, true
		}
		// Fall back to float parsing for scientific notation
		if f, err := strconv.ParseFloat(n, 64); err == nil {
			return int64(f), true
		}
	case []byte:
		return toInt64(string(n))
	}
	return 0, false
}
