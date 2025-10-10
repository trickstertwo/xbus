package xbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Codec is the Strategy for encoding/decoding payloads on the wire.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// JSONCodec is the default JSON implementation.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (JSONCodec) Name() string                    { return "json" }

// CodecFactory constructs codecs via Factory pattern.
type CodecFactory func() Codec

var (
	codecRegistryMu sync.RWMutex
	codecRegistry   = map[string]CodecFactory{
		"json": func() Codec { return JSONCodec{} },
	}
)

// RegisterCodec registers a codec factory by name.
func RegisterCodec(name string, factory CodecFactory) error {
	if name == "" {
		return errors.New("codec name must not be empty")
	}
	if factory == nil {
		return errors.New("codec factory must not be nil")
	}
	codecRegistryMu.Lock()
	codecRegistry[name] = factory
	codecRegistryMu.Unlock()
	return nil
}

// NewCodec constructs a codec by name or returns an error.
func NewCodec(name string) (Codec, error) {
	codecRegistryMu.RLock()
	f, ok := codecRegistry[name]
	codecRegistryMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("codec %q not registered", name)
	}
	return f(), nil
}

// unexported key type to avoid collisions
type ctxKey string

const codecCtxKey ctxKey = "xbus:codec"

// injectCodec attaches the active Codec into context for downstream handlers.
func injectCodec(ctx context.Context, c Codec) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, codecCtxKey, c)
}

// CodecFromContext retrieves a Codec previously injected into the context.
func CodecFromContext(ctx context.Context) (Codec, bool) {
	if v := ctx.Value(codecCtxKey); v != nil {
		if c, ok := v.(Codec); ok && c != nil {
			return c, true
		}
	}
	return nil, false
}

// Decode is a helper to unmarshal a message payload into a typed value using the provided codec.
func DecodeCodec[T any](c Codec, msg *Message) (T, error) {
	var v T
	if err := c.Unmarshal(msg.Payload, &v); err != nil {
		return v, err
	}
	return v, nil
}

// Decode unmarshals msg.Payload into T using a Codec found in ctx.
// Falls back to the default "json" codec if none was injected.
func Decode[T any](ctx context.Context, msg *Message) (T, error) {
	if c, ok := CodecFromContext(ctx); ok && c != nil {
		return DecodeCodec[T](c, msg)
	}
	// fallback to default json codec
	c, _ := NewCodec("json")
	return DecodeCodec[T](c, msg)
}
