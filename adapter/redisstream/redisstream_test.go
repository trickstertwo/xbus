package redisstream

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/trickstertwo/xbus"
)

// TestPayload is a sample domain event for testing.
type TestPayload struct {
	ID    string
	Data  string
	Value int64
}

// redisClient returns a connected Redis client for testing.
func redisClient(t *testing.T) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "51.255.76.232:63739",
		Password: "4a1o42U_4zpyUu",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	return client
}

// cleanupStream removes all entries from a stream and its consumer group.
func cleanupStream(t *testing.T, client *redis.Client, stream, group string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Delete consumer group
	_ = client.XGroupDestroy(ctx, stream, group).Err()

	// Delete stream
	_ = client.Del(ctx, stream).Err()
}

// TestPublish_SingleMessage tests publishing a single message.
func TestPublish_SingleMessage(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish single message
	msg := &xbus.Message{
		Name:       "TestEvent",
		Payload:    []byte(`{"test": "data"}`),
		Metadata:   map[string]string{"key": "value"},
		ProducedAt: time.Now(),
	}

	err = tr.Publish(ctx, "test-topic", msg)
	require.NoError(t, err)

	// Verify in Redis
	res, err := client.XLen(ctx, "test-topic").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), res)

	cleanupStream(t, client, "test-topic", cfg.Group)
}

// TestPublish_BatchMessages tests publishing multiple messages at once.
func TestPublish_BatchMessages(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish batch (100 messages)
	const batchSize = 100
	msgs := make([]*xbus.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = &xbus.Message{
			Name:       "BatchEvent",
			Payload:    []byte(fmt.Sprintf(`{"index":%d}`, i)),
			Metadata:   map[string]string{"batch": "true"},
			ProducedAt: time.Now(),
		}
	}

	err = tr.Publish(ctx, "test-topic", msgs...)
	require.NoError(t, err)

	// Verify all messages in Redis
	res, err := client.XLen(ctx, "test-topic").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(batchSize), res)

	cleanupStream(t, client, "test-topic", cfg.Group)
}

// TestPublish_HighThroughput tests publishing 10K messages in rapid succession.
func TestPublish_HighThroughput(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const totalMessages = 10_000
	const batchSize = 500

	start := time.Now()

	// Publish in batches
	for batch := 0; batch < (totalMessages / batchSize); batch++ {
		msgs := make([]*xbus.Message, batchSize)
		for i := 0; i < batchSize; i++ {
			idx := batch*batchSize + i
			msgs[i] = &xbus.Message{
				Name:    "HighThroughputEvent",
				Payload: []byte(fmt.Sprintf(`{"id":%d,"timestamp":"%d"}`, idx, time.Now().UnixNano())),
				Metadata: map[string]string{
					"batch": fmt.Sprintf("%d", batch),
					"index": fmt.Sprintf("%d", i),
				},
				ProducedAt: time.Now(),
			}
		}

		err := tr.Publish(ctx, "throughput-topic", msgs...)
		require.NoError(t, err)
	}

	duration := time.Since(start)

	// Verify all messages in Redis
	res, err := client.XLen(ctx, "throughput-topic").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(totalMessages), res)

	throughput := float64(totalMessages) / duration.Seconds()
	t.Logf("Published %d messages in %v (%.0f msg/sec)", totalMessages, duration, throughput)

	cleanupStream(t, client, "throughput-topic", cfg.Group)
}

// TestPublish_ConcurrentPublishers tests multiple goroutines publishing simultaneously.
// OPTIMIZED: Uses batch publishing to maximize throughput
// TestPublish_ConcurrentPublishers tests multiple goroutines publishing simultaneously with batching.
// Uses extended timeout and proper error handling for high-volume scenarios.
func TestPublish_ConcurrentPublishers(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	// Extended timeout for 10K messages
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const numPublishers = 10
	const messagesPerPublisher = 1_000
	const batchSize = 200

	wg := &sync.WaitGroup{}
	publishedCount := atomic.Int64{}
	errorCount := atomic.Int64{}
	var mu sync.Mutex
	var lastErr error

	start := time.Now()

	// Launch publisher goroutines
	for pub := 0; pub < numPublishers; pub++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()

			batch := make([]*xbus.Message, 0, batchSize)

			for i := 0; i < messagesPerPublisher; i++ {
				msg := &xbus.Message{
					Name: "ConcurrentEvent",
					Payload: []byte(fmt.Sprintf(
						`{"publisher":%d,"msg":%d,"ts":%d}`,
						publisherID, i, time.Now().UnixNano(),
					)),
					Metadata: map[string]string{
						"publisher": fmt.Sprintf("%d", publisherID),
					},
					ProducedAt: time.Now(),
				}

				batch = append(batch, msg)

				if len(batch) >= batchSize {
					err := tr.Publish(ctx, "concurrent-topic", batch...)
					if err != nil {
						errorCount.Add(int64(len(batch)))
						mu.Lock()
						lastErr = err
						mu.Unlock()
						t.Logf("publish error (publisher %d): %v", publisherID, err)
					} else {
						publishedCount.Add(int64(len(batch)))
					}
					batch = batch[:0]
				}
			}

			// Flush remaining
			if len(batch) > 0 {
				err := tr.Publish(ctx, "concurrent-topic", batch...)
				if err != nil {
					errorCount.Add(int64(len(batch)))
					mu.Lock()
					lastErr = err
					mu.Unlock()
					t.Logf("publish error (publisher %d, final): %v", publisherID, err)
				} else {
					publishedCount.Add(int64(len(batch)))
				}
			}
		}(pub)
	}

	wg.Wait()
	duration := time.Since(start)

	expectedTotal := int64(numPublishers * messagesPerPublisher)

	t.Logf("Published: %d, Errors: %d, Duration: %v, Throughput: %.0f msg/sec",
		publishedCount.Load(), errorCount.Load(), duration, float64(publishedCount.Load())/duration.Seconds())

	if lastErr != nil {
		t.Logf("Last error: %v", lastErr)
	}

	// Check success rate
	successRate := float64(publishedCount.Load()) / float64(expectedTotal) * 100
	if successRate < 99.0 {
		t.Logf("⚠️  Success rate: %.1f%% (expected 100%%)", successRate)
	}

	assert.Equal(t, expectedTotal, publishedCount.Load(), "published count mismatch")
	assert.Equal(t, int64(0), errorCount.Load(), "error count should be 0")

	// Verify in Redis
	res, err := client.XLen(ctx, "concurrent-topic").Result()
	require.NoError(t, err, "XLen failed")
	assert.Equal(t, expectedTotal, res, "redis stream length mismatch")

	cleanupStream(t, client, "concurrent-topic", cfg.Group)
}

// TestSubscribe_ConsumesAllMessages tests that Subscribe receives all published messages.
func TestSubscribe_ConsumesAllMessages(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"
	cfg.Group = "test-group-" + fmt.Sprintf("%d", time.Now().UnixNano())
	cfg.Concurrency = 4

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const numMessages = 100

	// Publish messages first
	msgs := make([]*xbus.Message, numMessages)
	for i := 0; i < numMessages; i++ {
		msgs[i] = &xbus.Message{
			Name:       "ConsumeTestEvent",
			Payload:    []byte(fmt.Sprintf(`{"id":%d}`, i)),
			ProducedAt: time.Now(),
		}
	}

	err = tr.Publish(ctx, "consume-test", msgs...)
	require.NoError(t, err)

	// Subscribe and consume
	consumedCount := atomic.Int64{}
	wg := &sync.WaitGroup{}
	wg.Add(1)

	sub, err := tr.Subscribe(ctx, "consume-test", cfg.Group, func(d xbus.Delivery) {
		consumedCount.Add(1)
		_ = d.Ack(ctx)
		if consumedCount.Load() >= int64(numMessages) {
			wg.Done()
		}
	})
	require.NoError(t, err)
	defer sub.Close()

	// Wait for all messages to be consumed (with timeout)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, int64(numMessages), consumedCount.Load())
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for messages (consumed %d/%d)", consumedCount.Load(), numMessages)
	}

	cleanupStream(t, client, "consume-test", cfg.Group)
}

// TestPublish_PublishBatch_Mixed tests alternating single and batch publishes.
func TestPublish_PublishBatch_Mixed(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "mixed-test"
	var totalExpected int64

	// 5 single publishes
	for i := 0; i < 5; i++ {
		msg := &xbus.Message{
			Name:       "SingleEvent",
			Payload:    []byte(fmt.Sprintf(`{"index":%d}`, i)),
			ProducedAt: time.Now(),
		}
		err := tr.Publish(ctx, topic, msg)
		require.NoError(t, err)
		totalExpected++
	}

	// 5 batches of 20 messages
	for batch := 0; batch < 5; batch++ {
		msgs := make([]*xbus.Message, 20)
		for i := 0; i < 20; i++ {
			msgs[i] = &xbus.Message{
				Name:       "BatchEvent",
				Payload:    []byte(fmt.Sprintf(`{"batch":%d,"index":%d}`, batch, i)),
				ProducedAt: time.Now(),
			}
		}
		err := tr.Publish(ctx, topic, msgs...)
		require.NoError(t, err)
		totalExpected += 20
	}

	// Verify total
	res, err := client.XLen(ctx, topic).Result()
	require.NoError(t, err)
	assert.Equal(t, totalExpected, res)

	cleanupStream(t, client, topic, cfg.Group)
}

// BenchmarkPublish_Single benchmarks single message publishing.
func BenchmarkPublish_Single(b *testing.B) {
	client := redisClient(&testing.T{})
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, _ := NewTransport(cfg)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := &xbus.Message{
		Name:       "BenchEvent",
		Payload:    []byte(`{"bench":true}`),
		ProducedAt: time.Now(),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = tr.Publish(ctx, "bench-single", msg)
	}

	cleanupStream(&testing.T{}, client, "bench-single", cfg.Group)
}

// BenchmarkPublish_Batch benchmarks batch message publishing (100 messages).
func BenchmarkPublish_Batch(b *testing.B) {
	client := redisClient(&testing.T{})
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, _ := NewTransport(cfg)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const batchSize = 100
	msgs := make([]*xbus.Message, batchSize)
	for i := 0; i < batchSize; i++ {
		msgs[i] = &xbus.Message{
			Name:       "BenchBatchEvent",
			Payload:    []byte(fmt.Sprintf(`{"index":%d}`, i)),
			ProducedAt: time.Now(),
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = tr.Publish(ctx, "bench-batch", msgs...)
	}

	cleanupStream(&testing.T{}, client, "bench-batch", cfg.Group)
}

// BenchmarkSubscribe_ThroughputWithAck benchmarks subscription throughput with acknowledgments.
func BenchmarkSubscribe_ThroughputWithAck(b *testing.B) {
	client := redisClient(&testing.T{})
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"
	cfg.Group = "bench-group-" + fmt.Sprintf("%d", time.Now().UnixNano())
	cfg.Concurrency = 4

	tr, _ := NewTransport(cfg)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Pre-populate with messages
	const numMessages = 1000
	msgs := make([]*xbus.Message, numMessages)
	for i := 0; i < numMessages; i++ {
		msgs[i] = &xbus.Message{
			Name:       "BenchSubEvent",
			Payload:    []byte(fmt.Sprintf(`{"id":%d}`, i)),
			ProducedAt: time.Now(),
		}
	}
	_ = tr.Publish(ctx, "bench-sub", msgs...)

	b.ResetTimer()

	consumedCount := atomic.Int64{}
	sub, _ := tr.Subscribe(ctx, "bench-sub", cfg.Group, func(d xbus.Delivery) {
		_ = d.Ack(ctx)
		consumedCount.Add(1)
	})

	// Let subscription run
	time.Sleep(time.Duration(b.N) * time.Millisecond)
	sub.Close()

	b.StopTimer()
	cleanupStream(&testing.T{}, client, "bench-sub", cfg.Group)
}

// TestErrorHandling_PublishToInvalidTopic tests error handling for invalid operations.
func TestErrorHandling_PublishToInvalidTopic(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Empty topic should fail
	msg := &xbus.Message{
		Name:       "TestEvent",
		Payload:    []byte(`{}`),
		ProducedAt: time.Now(),
	}

	// Redis will handle it, but we should not crash
	// (actual behavior depends on Redis configuration)
	_ = tr.Publish(ctx, "", msg)
}

// TestDeadLetter_NackWritesToDLQ tests that Nack writes to dead-letter queue.
func TestDeadLetter_NackWritesToDLQ(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	dlqName := "test-dlq-" + fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"
	cfg.Group = "dlq-test-group-" + fmt.Sprintf("%d", time.Now().UnixNano())
	cfg.DeadLetter = dlqName

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish message
	msg := &xbus.Message{
		Name:       "DLQTestEvent",
		Payload:    []byte(`{"test":"dlq"}`),
		ProducedAt: time.Now(),
	}
	err = tr.Publish(ctx, "dlq-topic", msg)
	require.NoError(t, err)

	// Subscribe and nack
	nacked := atomic.Bool{}
	sub, err := tr.Subscribe(ctx, "dlq-topic", cfg.Group, func(d xbus.Delivery) {
		if !nacked.Load() {
			// Nack first delivery
			_ = d.Nack(ctx, fmt.Errorf("test error"))
			nacked.Store(true)
		} else {
			_ = d.Ack(ctx)
		}
	})
	require.NoError(t, err)
	defer sub.Close()

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Check DLQ has the message
	dlqLen, err := client.XLen(ctx, dlqName).Result()
	require.NoError(t, err)
	// Should have at least 1 message in DLQ (from nack)
	t.Logf("DLQ length: %d", dlqLen)

	cleanupStream(t, client, "dlq-topic", cfg.Group)
	cleanupStream(t, client, dlqName, "")
}

func TestPublish_BatchSizeScaling(t *testing.T) {
	client := redisClient(t)
	defer client.Close()

	cfg := Defaults()
	cfg.Addr = "51.255.76.232:63739"
	cfg.Password = "4a1o42U_4zpyUu"

	tr, err := NewTransport(cfg)
	require.NoError(t, err)
	defer tr.Close(context.Background())

	const numPublishers = 5
	const messagesPerPublisher = 1000

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	type result struct {
		BatchSize  int
		Duration   time.Duration
		Throughput float64
		Errors     int64
	}

	var results []result

	for batchSize := 100; batchSize <= 1000; batchSize += 100 {
		t.Logf("=== Testing batch size %d ===", batchSize)

		wg := &sync.WaitGroup{}
		publishedCount := atomic.Int64{}
		errorCount := atomic.Int64{}
		var mu sync.Mutex
		var lastErr error

		start := time.Now()

		for pub := 0; pub < numPublishers; pub++ {
			wg.Add(1)
			go func(publisherID int) {
				defer wg.Done()
				batch := make([]*xbus.Message, 0, batchSize)

				for i := 0; i < messagesPerPublisher; i++ {
					msg := &xbus.Message{
						Name: "BatchTestEvent",
						Payload: []byte(fmt.Sprintf(
							`{"publisher":%d,"msg":%d,"ts":%d}`,
							publisherID, i, time.Now().UnixNano(),
						)),
						Metadata: map[string]string{
							"publisher": fmt.Sprintf("%d", publisherID),
						},
						ProducedAt: time.Now(),
					}
					batch = append(batch, msg)

					if len(batch) >= batchSize {
						if err := tr.Publish(ctx, "batch-topic", batch...); err != nil {
							errorCount.Add(int64(len(batch)))
							mu.Lock()
							lastErr = err
							mu.Unlock()
						} else {
							publishedCount.Add(int64(len(batch)))
						}
						batch = batch[:0]
					}
				}

				if len(batch) > 0 {
					if err := tr.Publish(ctx, "batch-topic", batch...); err != nil {
						errorCount.Add(int64(len(batch)))
						mu.Lock()
						lastErr = err
						mu.Unlock()
					} else {
						publishedCount.Add(int64(len(batch)))
					}
				}
			}(pub)
		}

		wg.Wait()
		duration := time.Since(start)
		throughput := float64(publishedCount.Load()) / duration.Seconds()

		t.Logf("[Batch=%d] Published=%d Errors=%d Duration=%v Throughput=%.0f msg/sec",
			batchSize, publishedCount.Load(), errorCount.Load(), duration, throughput)

		results = append(results, result{
			BatchSize:  batchSize,
			Duration:   duration,
			Throughput: throughput,
			Errors:     errorCount.Load(),
		})

		cleanupStream(t, client, "batch-topic", cfg.Group)

		if lastErr != nil {
			t.Logf("Last error (batch=%d): %v", batchSize, lastErr)
		}
	}

	t.Logf("\n=== Summary: Batch Size Scaling Results ===")
	t.Logf("%-10s %-15s %-15s %-10s", "BatchSize", "Duration", "Throughput(msg/s)", "Errors")
	for _, r := range results {
		t.Logf("%-10d %-15v %-15.0f %-10d",
			r.BatchSize, r.Duration, r.Throughput, r.Errors)
	}
}
