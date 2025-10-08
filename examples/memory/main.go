package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xbus/adapter/memory"
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
	_ "github.com/trickstertwo/xlog/adapter/zerolog"
)

type OrderCreated struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown on Ctrl+C
	go func() {
		sigC := make(chan os.Signal, 1)
		signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
		<-sigC
		cancel()
	}()

	logger := xlog.New().With(xlog.FStr("app", "xbus-memory-example"))
	clock := xclock.Default()

	// Build the bus with the in-memory adapter and middleware.
	builder := xbus.NewBusBuilder().
		WithLogger(logger).
		WithClock(clock).
		WithTransport(memory.TransportName, map[string]any{
			"buffer_size":      1024,
			"concurrency":      2,       // workers per subscription
			"redelivery_delay": "500ms", // delay before requeue on Nack
			"assign_ids":       true,    // assign message IDs if empty
		}).
		WithMiddleware(
			// Time-bounded processing for dependability.
			xbus.TimeoutMiddleware(10*time.Second),
			// Retry with exponential backoff.
			xbus.RetryMiddleware(xbus.RetryConfig{
				MaxAttempts: 5,
				Backoff: func(attempt int) time.Duration {
					// 50ms, 100ms, 200ms, 400ms, 800ms
					return time.Duration(50*int64(1<<uint(attempt-1))) * time.Millisecond
				},
			}),
			// Simple structured logging middleware.
			//LoggingMiddleware(logger),
		).
		WithAckTimeout(2 * time.Second)

	bus, err := builder.Build()
	if err != nil {
		logger.Error().Err(err).Msg("failed to build bus")
		os.Exit(1)
	}
	defer func() { _ = bus.Close(context.Background()) }()

	// Observer for instrumentation
	bus.AddObserver(&logObserver{l: logger, c: clock})

	// Start a processor (consumer) on "orders" topic, group "processors".
	var wg sync.WaitGroup
	wg.Add(1)
	sub, err := bus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		var evt OrderCreated
		if err := json.Unmarshal(msg.Payload, &evt); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		// Simulate processing
		logger.Info().
			Str("event", msg.Name).
			Str("id", msg.ID).
			Str("order_id", evt.OrderID).
			Str("user_id", evt.UserID).
			Float64("amount_usd", evt.AmountUSD).
			Msg("order created")
		return nil
	})
	if err != nil {
		logger.Error().Err(err).Msg("failed to subscribe")
		os.Exit(1)
	}
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = sub.Close()
	}()

	// Emit a few events as an emitter/producer.
	go func() {
		for i := 0; i < 10000; i++ {
			evt := OrderCreated{
				OrderID:   fmt.Sprintf("ord-%03d", i+1),
				UserID:    fmt.Sprintf("u-%02d", (i%3)+1),
				AmountUSD: float64(10+i) * 1.25,
			}
			if err := bus.Publish(ctx, "orders", "OrderCreated", evt, map[string]string{
				"source": "memory-example",
				"iter":   fmt.Sprint(i + 1),
			}); err != nil {
				logger.Warn().Err(err).Msg("publish failed")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	logger.Info().Msg("memory example running; press Ctrl+C to exit")
	wg.Wait()
	logger.Info().Msg("shutdown complete")
}

// LoggingMiddleware is a simple example of a pluggable middleware for handlers.
func LoggingMiddleware(l *xlog.Logger) xbus.Middleware {
	return func(next xbus.Handler) xbus.Handler {
		return func(ctx context.Context, msg *xbus.Message) error {
			start := time.Now()
			l.Debug().
				Str("name", msg.Name).
				Str("id", msg.ID).
				Msg("handler start")

			err := next(ctx, msg)

			l.Debug().
				Str("name", msg.Name).
				Str("id", msg.ID).
				Dur("dur", time.Since(start)).
				Err(err).
				Msg("handler done")
			return err
		}
	}
}

// logObserver demonstrates the Observer pattern for bus lifecycle events.
type logObserver struct {
	l *xlog.Logger
	c xclock.Clock
}

func (o *logObserver) OnBusEvent(e xbus.BusEvent) {
	switch e.Type {
	case xbus.EventPublishStart:
		o.l.Debug().
			Str("topic", e.Topic).
			Str("event", e.EventName).
			Msg("publish start")
	case xbus.EventPublishDone:
		if e.Err != nil {
			o.l.Warn().
				Str("topic", e.Topic).
				Str("event", e.EventName).
				Dur("dur", e.Duration).
				Err(e.Err).
				Msg("publish failed")
		} else {
			o.l.Info().
				Str("topic", e.Topic).
				Str("event", e.EventName).
				Dur("dur", e.Duration).
				Msg("publish ok")
		}
	case xbus.EventConsumeStart:
		o.l.Debug().
			Str("topic", e.Topic).
			Str("group", e.Group).
			Str("id", e.MessageID).
			Str("event", e.EventName).
			Msg("consume start")
	case xbus.EventConsumeDone:
		if e.Err != nil {
			o.l.Warn().
				Str("topic", e.Topic).
				Str("group", e.Group).
				Str("id", e.MessageID).
				Str("event", e.EventName).
				Dur("dur", e.Duration).
				Err(e.Err).
				Msg("consume failed")
		} else {
			o.l.Info().
				Str("topic", e.Topic).
				Str("group", e.Group).
				Str("id", e.MessageID).
				Str("event", e.EventName).
				Dur("dur", e.Duration).
				Msg("consume ok")
		}
	case xbus.EventAck:
		o.l.Debug().
			Str("topic", e.Topic).
			Str("group", e.Group).
			Str("id", e.MessageID).
			Msg("ack")
	case xbus.EventNack:
		o.l.Warn().
			Str("topic", e.Topic).
			Str("group", e.Group).
			Str("id", e.MessageID).
			Err(e.Err).
			Msg("nack")
	case xbus.EventError:
		o.l.Error().
			Err(e.Err).
			Msg("bus error")
	}
}
