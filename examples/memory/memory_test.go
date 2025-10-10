package main

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/trickstertwo/xbus"
	"github.com/trickstertwo/xbus/adapter/memory"
	"github.com/trickstertwo/xlog"
	"github.com/trickstertwo/xlog/adapter/zerolog"
)

type benchEvt struct {
	ID int    `json:"id"`
	P  string `json:"p"`
}

// BenchmarkMemoryAdapter_ThroughputLatency measures end-to-end throughput and latency
// for the in-memory transport by publishing b.N messages and waiting for consumption.
func BenchmarkMemoryAdapter_ThroughputLatency(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Single explicit call, no envs, no blank-imports. Clear and predictable.
	logger := zerolog.Use(zerolog.Config{
		MinLevel:          xlog.LevelDebug,
		Console:           false,
		ConsoleTimeFormat: time.RFC3339Nano,
		Caller:            true,
		CallerSkip:        5,
		// Writer:          os.Stdout,
	}).With(xlog.Str("app", "xbus-memory-default"))

	workers := runtime.GOMAXPROCS(0) * 2
	if workers < 1 {
		workers = 1
	}

	builder := xbus.NewBusBuilder().
		WithLogger(logger).
		WithTransport(memory.TransportName, map[string]any{
			"buffer_size":      1 << 15, // 32k
			"concurrency":      workers, // workers per subscription
			"redelivery_delay": "0s",
			"assign_ids":       true,
		})
	bus, err := builder.Build()
	if err != nil {
		b.Fatalf("build bus: %v", err)
	}
	defer func() { _ = bus.Close(context.Background()) }()

	var (
		received  int64
		sumLatNs  int64
		minLatNs  = int64(^uint64(0) >> 1) // math.MaxInt64 without importing math
		maxLatNs  int64
		onceClose sync.Once
		done      = make(chan struct{})
	)

	// Consumer
	sub, err := bus.Subscribe(ctx, "orders", "processors", func(ctx context.Context, msg *xbus.Message) error {
		now := time.Now()
		lat := now.Sub(msg.ProducedAt).Nanoseconds()

		atomic.AddInt64(&sumLatNs, lat)
		// minLat
		for {
			old := atomic.LoadInt64(&minLatNs)
			if lat >= old {
				break
			}
			if atomic.CompareAndSwapInt64(&minLatNs, old, lat) {
				break
			}
		}
		// maxLat
		for {
			old := atomic.LoadInt64(&maxLatNs)
			if lat <= old {
				break
			}
			if atomic.CompareAndSwapInt64(&maxLatNs, old, lat) {
				break
			}
		}

		if atomic.AddInt64(&received, 1) == int64(b.N) {
			onceClose.Do(func() { close(done) })
		}
		return nil
	})
	if err != nil {
		b.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = sub.Close() }()

	// Warm-up a few messages to spin up workers.
	for i := 0; i < 100; i++ {
		_ = bus.Publish(ctx, "orders", "BenchEvt", benchEvt{ID: i, P: "warmup"}, nil)
	}

	// Run benchmark: publish b.N messages, measure total wall time.
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		if err := bus.Publish(ctx, "orders", "BenchEvt", benchEvt{ID: i, P: "payload"}, nil); err != nil {
			b.Fatalf("publish failed: %v", err)
		}
	}

	// Wait for all messages to be consumed or timeout
	select {
	case <-done:
	case <-ctx.Done():
		b.Fatalf("timeout waiting for consumption: got %d/%d", atomic.LoadInt64(&received), b.N)
	}

	total := time.Since(start)
	b.StopTimer()

	// Metrics
	rcvd := atomic.LoadInt64(&received)
	if rcvd == 0 {
		b.Fatalf("no messages consumed")
	}
	sum := atomic.LoadInt64(&sumLatNs)
	minLat := atomic.LoadInt64(&minLatNs)
	maxLat := atomic.LoadInt64(&maxLatNs)
	avg := float64(sum) / float64(rcvd)

	throughput := float64(rcvd) / total.Seconds() // msgs/sec

	// Report custom metrics; b.NsPerOp is not meaningful for async pub/sub.
	b.ReportMetric(throughput, "msgs/s")
	b.ReportMetric(avg, "avg-lat-ns")
	b.ReportMetric(float64(minLat), "minLat-lat-ns")
	b.ReportMetric(float64(maxLat), "maxLat-lat-ns")
}
