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
