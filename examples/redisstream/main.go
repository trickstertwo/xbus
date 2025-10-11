package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xbus/adapter/redisstream"
	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
	"github.com/trickstertwo/xlog/adapter/zerolog"
)

type OrderCreated struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	// Explicit init, no blank imports.
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelInfo,
		Console:           true,
		ConsoleTimeFormat: time.RFC3339,
		Caller:            true,
		CallerSkip:        5,
	}).With(xlog.Str("app", "xbus-redis-example"))

	// Build and set the default bus via adapter.Use pattern.
	_ = redisstream.Use(
		redisstream.Config{
			Addr:            env("REDIS_ADDR", "51.255.76.232:63739"),
			Password:        env("REDIS_PASSWORD", "4a1o42U_4zpyUu"),
			DB:              1,
			Group:           env("REDIS_GROUP", "xbus-example"),
			Consumer:        env("REDIS_CONSUMER", "worker-1"),
			Concurrency:     8,
			BatchSize:       256,
			Block:           5 * time.Second,
			AutoCreate:      true,
			AutoDeleteOnAck: false,
			DeadLetter:      env("REDIS_DLQ", "xbus-example-dlq"),
		},
		redisstream.WithLogger(logger),
		redisstream.WithAckTimeout(5*time.Second),
		redisstream.WithCodec("json"),
		redisstream.WithClock(xclock.Default()),
		redisstream.WithMiddleware(
			xbus.TimeoutMiddleware(15*time.Second),
			xbus.RetryMiddleware(xbus.RetryConfig{
				MaxAttempts: 5,
				Backoff: func(attempt int) time.Duration {
					// 50ms, 100ms, 200ms, 400ms, 800ms
					return 50 * time.Millisecond << (attempt - 1)
				},
				Jitter:  25 * time.Millisecond,
				RetryIf: func(err error) bool { return true },
			}),
		),
	)

	// Consumer
	sub, err := xbus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		evt, err := xbus.Decode[OrderCreated](ctx, msg)
		if err != nil {
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
		logger.Fatal().Err(err).Msg("failed to subscribe")
	}
	defer func() { _ = sub.Close() }()

	// Producer
	go func() {
		for i := 0; i < 1_000_000; i++ {
			evt := OrderCreated{
				OrderID:   fmt.Sprintf("ord-%06d", i+1),
				UserID:    fmt.Sprintf("u-%02d", (i%5)+1),
				AmountUSD: 10 + float64(i)*0.75,
			}
			if err := xbus.Publish(ctx, "orders", "OrderCreated", evt, map[string]string{
				"source": "redis-example",
				"iter":   fmt.Sprint(i + 1),
			}); err != nil {
				logger.Warn().Err(err).Msg("publish failed")
				time.Sleep(250 * time.Millisecond)
			}
		}
	}()

	logger.Info().Msg("redis example running; press Ctrl+C to exit")
	<-ctx.Done()

	// Close default bus on shutdown.
	if b, err := xbus.Default(nil); err == nil && b != nil {
		_ = b.Close(context.Background())
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
