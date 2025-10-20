package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
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

	// Graceful shutdown on signal
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		fmt.Println("\n[shutdown] received signal")
		cancel()
	}()

	// Explicit init, no blank imports
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelInfo,
		Console:           true,
		ConsoleTimeFormat: time.RFC3339,
		Caller:            true,
		CallerSkip:        5,
	}).With(xlog.Str("app", "xbus-redis-example"))

	// Build and set the default bus via adapter.Use pattern
	bus := redisstream.Use(
		redisstream.Config{
			Addr:            env("REDIS_ADDR", "51.255.76.232:63739"),
			Password:        env("REDIS_PASSWORD", "4a1o42U_4zpyUu"),
			DB:              2,
			Group:           env("REDIS_GROUP", "xbus-example"),
			Consumer:        env("REDIS_CONSUMER", "worker-1"),
			Concurrency:     8,
			BatchSize:       256,
			Block:           5 * time.Second,
			AutoCreate:      true,
			AutoDeleteOnAck: false,
			DeadLetter:      env("REDIS_DLQ", "xbus-example-dlq"),
			ClaimMinIdle:    30 * time.Second,
			ClaimBatch:      128,
			ClaimInterval:   15 * time.Second,
		},
		redisstream.WithLogger(logger),
		redisstream.WithAckTimeout(5*time.Second),
		redisstream.WithCodec("json"),
		redisstream.WithClock(xclock.Default()),
		redisstream.WithObserverPool(2, 1000),
		redisstream.WithObserver(
			xbus.LoggingObserver{Logger: logger},
		),
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

	// ⚠️  IMPORTANT: Defer close the bus to release resources
	// This gracefully shuts down:
	// - Observer pool (waits up to 5s for pending events)
	// - Redis connection pool
	// - All subscription goroutines
	defer func() {
		logger.Info().Msg("[shutdown] closing bus and releasing resources")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := bus.Close(ctx); err != nil {
			logger.Error().Err(err).Msg("[shutdown] error closing bus")
		}
		logger.Info().Msg("[shutdown] bus closed successfully")
	}()

	// Track metrics
	var (
		published int64
		consumed  int64
		errors    int64
	)

	// Consumer subscription
	sub, err := bus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		evt, err := xbus.Decode[OrderCreated](ctx, msg)
		if err != nil {
			atomic.AddInt64(&errors, 1)
			logger.Warn().Err(err).Msg("failed to decode order event")
			return err
		}

		atomic.AddInt64(&consumed, 1)

		logger.Debug().
			Str("event", msg.Name).
			Str("id", msg.ID).
			Str("order_id", evt.OrderID).
			Str("user_id", evt.UserID).
			Float64("amount_usd", evt.AmountUSD).
			Msg("order processed")

		return nil
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to subscribe")
	}
	defer func() {
		_ = sub.Close()
	}()

	// Publisher goroutine
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; i < 10_000; i++ {
			select {
			case <-ctx.Done():
				logger.Info().Int("published_total", int(atomic.LoadInt64(&published))).Msg("[publisher] stopping")
				return
			case <-ticker.C:
				evt := OrderCreated{
					OrderID:   fmt.Sprintf("ord-%08d", i+1),
					UserID:    fmt.Sprintf("u-%04d", (i%1000)+1),
					AmountUSD: 10 + float64(i)*0.75,
				}

				if err := bus.Publish(ctx, "orders", "OrderCreated", evt, map[string]string{
					"source": "redis-example",
					"index":  fmt.Sprintf("%d", i),
				}); err != nil {
					atomic.AddInt64(&errors, 1)
					logger.Warn().Err(err).Msg("publish failed")
				} else {
					atomic.AddInt64(&published, 1)
				}
			}
		}
	}()

	// Metrics reporter
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	start := time.Now()

	logger.Info().Msg("[startup] redis example running; press Ctrl+C to exit")

	for {
		select {
		case <-ctx.Done():
			// Calculate final metrics
			elapsed := time.Since(start)
			pubCount := atomic.LoadInt64(&published)
			conCount := atomic.LoadInt64(&consumed)
			errCount := atomic.LoadInt64(&errors)

			logger.Info().
				Int64("published", pubCount).
				Int64("consumed", conCount).
				Int64("errors", errCount).
				Float64("elapsed_sec", elapsed.Seconds()).
				Float64("publish_rate_msg_sec", float64(pubCount)/elapsed.Seconds()).
				Float64("consume_rate_msg_sec", float64(conCount)/elapsed.Seconds()).
				Msg("[metrics] final")

			return

		case <-ticker.C:
			elapsed := time.Since(start)
			pubCount := atomic.LoadInt64(&published)
			conCount := atomic.LoadInt64(&consumed)
			errCount := atomic.LoadInt64(&errors)

			logger.Info().
				Int64("published", pubCount).
				Int64("consumed", conCount).
				Int64("errors", errCount).
				Float64("elapsed_sec", elapsed.Seconds()).
				Float64("publish_rate_msg_sec", float64(pubCount)/elapsed.Seconds()).
				Float64("consume_rate_msg_sec", float64(conCount)/elapsed.Seconds()).
				Msg("[metrics] update")
		}
	}
}

// env returns the value of an environment variable or a default
func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
