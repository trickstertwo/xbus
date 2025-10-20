package xbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// TransportFactory constructs transports from a config blob.
type TransportFactory func(cfg map[string]any) (Transport, error)

// CodecFactory constructs codecs via Factory pattern.
type CodecFactory func() Codec

var (
	transportRegistryMu sync.RWMutex
	transportRegistry   = map[string]TransportFactory{}

	codecRegistryMu sync.RWMutex
	codecRegistry   = map[string]CodecFactory{
		"json": func() Codec { return JSONCodec{} },
	}
)

// RegisterTransport registers a backend adapter.
func RegisterTransport(name string, factory TransportFactory) error {
	if name == "" {
		return errors.New("transport name must not be empty")
	}
	if factory == nil {
		return errors.New("transport factory must not be nil")
	}
	transportRegistryMu.Lock()
	transportRegistry[name] = factory
	transportRegistryMu.Unlock()
	return nil
}

// NewTransport constructs a transport by name with config.
func NewTransport(name string, cfg map[string]any) (Transport, error) {
	transportRegistryMu.RLock()
	f, ok := transportRegistry[name]
	transportRegistryMu.RUnlock()
	if !ok {
		return nil, ErrUnknownTransport{name: name}
	}
	return f(cfg)
}

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

// JSONCodec is the default JSON implementation.
type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func (JSONCodec) Name() string {
	return "json"
}

// Decode unmarshals msg.Payload into T using a Codec found in ctx.
// Falls back to the default "json" codec if none was injected.
func Decode[T any](ctx context.Context, msg *Message) (T, error) {
	var v T
	c, ok := CodecFromContext(ctx)
	if !ok || c == nil {
		c = JSONCodec{}
	}
	if err := c.Unmarshal(msg.Payload, &v); err != nil {
		return v, err
	}
	return v, nil
}

// DecodeCodec is a helper to unmarshal a message payload into a typed value using the provided codec.
func DecodeCodec[T any](c Codec, msg *Message) (T, error) {
	var v T
	if err := c.Unmarshal(msg.Payload, &v); err != nil {
		return v, err
	}
	return v, nil
}
