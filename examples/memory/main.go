package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/trickstertwo/xlog"
	"github.com/trickstertwo/xlog/adapter/zerolog"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xbus/adapter/memory"
)

type OrderCreated struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Ctrl+C -> cancel
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		cancel()
	}()

	// Single explicit call, no envs, no blank-imports. Clear and predictable.
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelInfo,
		Console:           false,
		ConsoleTimeFormat: time.RFC3339Nano,
		Caller:            true,
		CallerSkip:        5,
		// Writer:          os.Stdout,
	}).With(xlog.Str("app", "xbus-memory-default"))

	// One-liner: initialize or get the default bus (Singleton + Builder).
	if _, err := xbus.Default(func(b *xbus.BusBuilder) {
		b.WithLogger(logger).
			WithTransport(memory.TransportName, map[string]any{
				"buffer_size":      4096,
				"concurrency":      4,
				"redelivery_delay": "0s",
				"assign_ids":       true,
			}).
			WithMiddleware(
				xbus.TimeoutMiddleware(5*time.Second),
				xbus.RetryMiddleware(xbus.RetryConfig{
					MaxAttempts: 3,
					Backoff: func(attempt int) time.Duration {
						return time.Duration(50*1<<uint(attempt-1)) * time.Millisecond
					},
				}),
			).
			WithAckTimeout(2 * time.Second)
	}); err != nil {
		logger.Fatal().Err(err).Msg("initialize default bus")
	}

	// Consumer using Facade
	sub, err := xbus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		var evt OrderCreated
		if err := json.Unmarshal(msg.Payload, &evt); err != nil {
			return err
		}
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
		logger.Fatal().Err(err).Msg("subscribe failed")
	}
	defer func() { _ = sub.Close() }()

	// Producer using Facade
	go func() {
		for i := 0; i < 100000; i++ {
			evt := OrderCreated{
				OrderID:   fmt.Sprintf("ord-%04d", i+1),
				UserID:    fmt.Sprintf("u-%02d", (i%5)+1),
				AmountUSD: 10 + float64(i)*0.5,
			}
			_ = xbus.Publish(ctx, "orders", "OrderCreated", evt, map[string]string{
				"source": "memory-default",
				"iter":   fmt.Sprint(i + 1),
			})
			time.Sleep(2 * time.Millisecond)
		}
	}()

	<-ctx.Done()

	// Close default explicitly (get it and close)
	if b, err := xbus.Default(nil); err == nil && b != nil {
		_ = b.Close(context.Background())
	}
}
