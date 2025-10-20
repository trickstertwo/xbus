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

const (
	fieldID         = "id"
	fieldName       = "name"
	fieldPayload    = "payload"
	fieldProducedAt = "producedAt"
	fieldMetaPrefix = "meta:"
)

// delivery implements xbus.Delivery for Redis Streams.
type delivery struct {
	t     *transport
	topic string
	group string
	id    string
	msg   *xbus.Message

	// sync.Once ensures exactly-once semantics for Ack/Nack
	onceAck *sync.Once
}

func (d *delivery) Message() *xbus.Message {
	return d.msg
}

// Ack acknowledges a message, marking it processed.
func (d *delivery) Ack(ctx context.Context) error {
	var err error
	d.onceAck.Do(func() {
		err = d.t.client.XAck(ctx, d.topic, d.group, d.id).Err()
		if err == nil {
			d.t.metrics.acked.Add(1)
			// Optionally delete from stream (memory optimization)
			if d.t.cfg.AutoDeleteOnAck {
				_ = d.t.client.XDel(ctx, d.topic, d.id).Err()
			}
		}
	})
	return err
}

// Nack negative-acknowledges a message (redelivery or dead-letter).
func (d *delivery) Nack(ctx context.Context, reason error) error {
	if dl := d.t.cfg.DeadLetter; dl != "" {
		// Write to dead-letter queue
		values := map[string]any{
			"orig_topic": d.topic,
			"orig_id":    d.id,
			"error":      fmt.Sprintf("%v", reason),
			fieldName:    d.msg.Name,
			fieldPayload: d.msg.Payload,
		}

		// Flatten metadata
		for k, v := range d.msg.Metadata {
			values[fieldMetaPrefix+k] = v
		}

		_ = d.t.client.XAdd(ctx, &redis.XAddArgs{
			Stream: dl,
			ID:     "*",
			Values: values,
		}).Err()

		d.t.metrics.nacked.Add(1)
		return d.Ack(ctx) // Ack original to prevent infinite loops
	}

	// No dead-letter: leave pending for Redis consumer group redelivery
	d.t.metrics.nacked.Add(1)
	return nil
}

// decodeMessage reconstructs xbus.Message from Redis stream entry.
func decodeMessage(id string, vals map[string]any) *xbus.Message {
	msg := &xbus.Message{
		ID:       id,
		Metadata: make(map[string]string),
	}

	if v, ok := vals[fieldName].(string); ok {
		msg.Name = v
	}

	if v, ok := vals[fieldPayload]; ok {
		switch p := v.(type) {
		case []byte:
			msg.Payload = p
		case string:
			msg.Payload = []byte(p)
		}
	}

	if v := vals[fieldProducedAt]; v != nil {
		if ns, ok := toInt64(v); ok && ns > 0 {
			msg.ProducedAt = time.Unix(0, ns)
		}
	}

	// Extract metadata (lazy to avoid allocation for messages without metadata)
	metaCount := 0
	for k := range vals {
		if strings.HasPrefix(k, fieldMetaPrefix) {
			metaCount++
		}
	}

	if metaCount > 0 {
		msg.Metadata = make(map[string]string, metaCount)
		for k, v := range vals {
			if strings.HasPrefix(k, fieldMetaPrefix) {
				msg.Metadata[strings.TrimPrefix(k, fieldMetaPrefix)] = toString(v)
			}
		}
	}

	return msg
}

// Helper functions for type conversion

func toString(v any) string {
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
		i, err := strconv.ParseInt(n, 10, 64)
		if err == nil {
			return i, true
		}
		f, err := strconv.ParseFloat(n, 64)
		if err == nil {
			return int64(f), true
		}
	case []byte:
		return toInt64(string(n))
	}
	return 0, false
}
