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
	"github.com/trickstertwo/xlog"
	"github.com/trickstertwo/xlog/adapter/zerolog"
)

// OrderCreated is a sample domain event.
type OrderCreated struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

func main() {
	// Setup context with graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		fmt.Println("\n[shutdown] received signal")
		cancel()
	}()

	// Initialize logger
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelInfo,
		Console:           true,
		ConsoleTimeFormat: time.RFC3339,
		Caller:            true,
		CallerSkip:        5,
	}).With(xlog.Str("app", "xbus-quickstart"))

	// Initialize bus with Redis Streams
	bus := redisstream.Use(
		redisstream.Config{
			Addr:        "51.255.76.232:63739",
			Password:    "4a1o42U_4zpyUu",
			Group:       "xbus-quickstart",
			Concurrency: 4,
			BatchSize:   32,
			Block:       5 * time.Second,
			AutoCreate:  true,
			DeadLetter:  "orders-dlq",
		},
		redisstream.WithLogger(logger),
		redisstream.WithMiddleware(
			xbus.TimeoutMiddleware(10*time.Second),
			xbus.RetryMiddleware(xbus.RetryConfig{
				MaxAttempts: 3,
				Backoff: func(attempt int) time.Duration {
					return 50 * time.Millisecond << (attempt - 1)
				},
			}),
		),
		redisstream.WithObserver(xbus.LoggingObserver{Logger: logger}),
	)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = bus.Close(ctx)
	}()

	// Metrics
	var published, consumed, errors int64

	// Subscribe to "orders" topic
	sub, err := bus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		evt, err := xbus.Decode[OrderCreated](ctx, msg)
		if err != nil {
			atomic.AddInt64(&errors, 1)
			logger.Warn().Err(err).Msg("decode failed")
			return err
		}

		atomic.AddInt64(&consumed, 1)
		logger.Info().
			Str("order_id", evt.OrderID).
			Str("user_id", evt.UserID).
			Float64("amount", evt.AmountUSD).
			Msg("order processed")

		return nil
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("subscribe failed")
	}
	defer sub.Close()

	// Publish sample orders
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for i := 1; i <= 100; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				evt := OrderCreated{
					OrderID:   fmt.Sprintf("ord-%05d", i),
					UserID:    fmt.Sprintf("user-%03d", i%50),
					AmountUSD: 10 + float64(i)*1.5,
				}

				if err := bus.Publish(ctx, "orders", "OrderCreated", evt, nil); err != nil {
					atomic.AddInt64(&errors, 1)
					logger.Warn().Err(err).Msg("publish failed")
				} else {
					atomic.AddInt64(&published, 1)
				}
			}
		}
	}()

	// Print metrics every 2 seconds
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	logger.Info().Msg("quickstart running (press Ctrl+C to exit)")

	for {
		select {
		case <-ctx.Done():
			logger.Info().
				Int64("published", atomic.LoadInt64(&published)).
				Int64("consumed", atomic.LoadInt64(&consumed)).
				Int64("errors", atomic.LoadInt64(&errors)).
				Msg("shutdown complete")
			return

		case <-ticker.C:
			logger.Info().
				Int64("published", atomic.LoadInt64(&published)).
				Int64("consumed", atomic.LoadInt64(&consumed)).
				Int64("errors", atomic.LoadInt64(&errors)).
				Msg("metrics")
		}
	}
}
