package xbus

import (
	"context"
	"fmt"
	"sync"
)

var (
	defaultBus   *Bus
	defaultBusMu sync.Mutex
)

// Default returns the process-wide singleton Bus.
func Default() *Bus {
	defaultBusMu.Lock()
	defer defaultBusMu.Unlock()

	if defaultBus != nil {
		return defaultBus
	}

	b := NewBusBuilder()
	bus, err := b.Build()
	if err != nil {
		panic(fmt.Sprintf("xbus: failed to initialize default bus: %v", err))
	}
	defaultBus = bus
	return defaultBus
}

// SetDefault replaces the process-wide default Bus.
func SetDefault(b *Bus) {
	if b == nil {
		panic("xbus: SetDefault called with nil Bus")
	}
	defaultBusMu.Lock()
	defaultBus = b
	defaultBusMu.Unlock()
}

// Publish is the Facade using the default bus.
func Publish(ctx context.Context, topic, eventName string, payload any, meta map[string]string) error {
	return Default().Publish(ctx, topic, eventName, payload, meta)
}

// PublishBatch is the Facade using the default bus for batch publishing.
func PublishBatch(ctx context.Context, topic string, events ...PublishEvent) error {
	return Default().PublishBatch(ctx, topic, events...)
}

// Subscribe is the Facade using the default bus.
func Subscribe(ctx context.Context, topic, group string, handler Handler) (Subscription, error) {
	return Default().Subscribe(ctx, topic, group, handler)
}
