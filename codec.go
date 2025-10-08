package xbus

import (
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
