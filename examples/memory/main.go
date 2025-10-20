package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/trickstertwo/xlog"
	"github.com/trickstertwo/xlog/adapter/zerolog"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xbus/adapter/memory"
)

// OrderCreated represents a domain event.
type OrderCreated struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	AmountUSD float64 `json:"amount_usd"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup graceful shutdown on Ctrl+C
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch
		fmt.Println("\n[shutdown] received signal")
		cancel()
	}()

	// Setup logger
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelInfo,
		Console:           true,
		ConsoleTimeFormat: time.RFC3339Nano,
		Caller:            true,
		CallerSkip:        5,
	}).With(xlog.Str("app", "xbus-memory-example"))

	// Initialize bus with memory transport using modern Use pattern
	bus := memory.Use(
		memory.Config{
			BufferSize:      4096,
			Concurrency:     4,
			RedeliveryDelay: 0,
			AssignIDs:       true,
		},
		memory.WithLogger(logger),
		memory.WithObserverPool(2, 1000),
		memory.WithObserver(
			xbus.LoggingObserver{Logger: logger},
		),
	)
	defer func() {
		_ = bus.Close(ctx)
	}()

	logger.Info().Msg("xbus initialized with memory transport")

	// Track metrics
	var (
		published int64
		consumed  int64
		errors    int64
	)

	// Start consumer
	sub, err := bus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		var evt OrderCreated
		if err := json.Unmarshal(msg.Payload, &evt); err != nil {
			atomic.AddInt64(&errors, 1)
			logger.Warn().Err(err).Msg("failed to unmarshal order event")
			return err
		}

		atomic.AddInt64(&consumed, 1)

		// Simulate variable processing time
		if evt.AmountUSD > 5000 {
			time.Sleep(5 * time.Millisecond)
		}

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
		logger.Fatal().Err(err).Msg("subscribe failed")
	}
	defer func() { _ = sub.Close() }()

	// Start publisher
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; i < 100; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for j := 0; j < 10; j++ {
					idx := i*10 + j
					evt := OrderCreated{
						OrderID:   fmt.Sprintf("ord-%06d", idx),
						UserID:    fmt.Sprintf("u-%03d", idx%100),
						AmountUSD: 50 + float64(idx%1000)*0.5,
					}

					if err := bus.Publish(ctx, "orders", "OrderCreated", evt, map[string]string{
						"source": "memory-example",
						"index":  fmt.Sprintf("%d", idx),
					}); err != nil {
						atomic.AddInt64(&errors, 1)
						logger.Warn().Err(err).Msg("publish failed")
					} else {
						atomic.AddInt64(&published, 1)
					}
				}
			}
		}
	}()

	// Print metrics every second
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			elapsed := time.Since(start)
			pubCount := atomic.LoadInt64(&published)
			conCount := atomic.LoadInt64(&consumed)
			errCount := atomic.LoadInt64(&errors)

			logger.Info().
				Int64("published", pubCount).
				Int64("consumed", conCount).
				Int64("errors", errCount).
				Float64("elapsed_sec", elapsed.Seconds()).
				Float64("publish_rate", float64(pubCount)/elapsed.Seconds()).
				Float64("consume_rate", float64(conCount)/elapsed.Seconds()).
				Msg("final metrics")

			return

		case <-ticker.C:
			pubCount := atomic.LoadInt64(&published)
			conCount := atomic.LoadInt64(&consumed)
			errCount := atomic.LoadInt64(&errors)
			elapsed := time.Since(start)

			logger.Info().
				Int64("published", pubCount).
				Int64("consumed", conCount).
				Int64("errors", errCount).
				Float64("elapsed_sec", elapsed.Seconds()).
				Float64("publish_rate", float64(pubCount)/elapsed.Seconds()).
				Float64("consume_rate", float64(conCount)/elapsed.Seconds()).
				Msg("metrics update")
		}
	}
}
