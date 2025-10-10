package redisstream

// Package redisstream provides a Redis Streams adapter for xbus.
//
// Transport name: "redis-streams"
//
// Minimal config keys (factory map):
// - addr: "host:port" (default "127.0.0.1:6379")
// - group: consumer group name (default "xbus")
// - consumer: consumer name (default "xbus-...")
// - concurrency: number of workers (default 8)
// - batch_size: XREADGROUP COUNT (default 128)
// - block: XREADGROUP BLOCK duration (default 5s)
// - auto_create: create group/stream if missing (default true)
// - auto_delete_on_ack: XDEL after XACK (default false)
// - dead_letter: stream name to write failed messages (optional)
//
// Modern usage (explicit, no blank imports):
//
//  logger := zerolog.Use(zerolog.Config{ ... })
//  bus := redisstream.Use(redisstream.Config{
//      Addr:            "localhost:6379",
//      Group:           "payments",
//      Consumer:        "service-a",
//      Concurrency:     16,
//      BatchSize:       256,
//      Block:           5 * time.Second,
//      AutoCreate:      true,
//      AutoDeleteOnAck: false,
//      DeadLetter:      "payments-dlq",
//  },
//      redisstream.WithLogger(logger),
//      redisstream.WithAckTimeout(5*time.Second),
//      redisstream.WithMiddleware(
//          xbus.TimeoutMiddleware(15*time.Second),
//          xbus.RetryMiddleware(xbus.RetryConfig{
//              MaxAttempts: 5,
//              Backoff: func(i int) time.Duration { return 50*time.Millisecond << (i-1) },
//          }),
//      ),
//  )
//  _ = bus // default singleton is configured; xbus.Publish/Subscribe are ready.
//
// Legacy builder usage (still supported via factory):
//
//  bus, _ := xbus.NewBusBuilder().
//      WithTransport(redisstream.TransportName, map[string]any{
//          "addr":         "localhost:6379",
//          "group":        "payments",
//          "consumer":     "service-a",
//          "concurrency":  16,
//          "batch_size":   256,
//          "block":        "5s",
//          "auto_create":  true,
//          "dead_letter":  "payments-dlq",
//      }).
//      Build()
