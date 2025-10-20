package redisstream

// Package redisstream provides a Redis Streams adapter for xbus.
//
// Transport name: "redis-streams"
//
// Quick Start (recommended):
//
//	logger := zerolog.Use(zerolog.Config{...})
//	bus := redisstream.Use(redisstream.Config{
//	    Addr:            "localhost:6379",
//	    Group:           "payments",
//	    Concurrency:     16,
//	    BatchSize:       256,
//	    AutoCreate:      true,
//	    DeadLetter:      "payments-dlq",
//	},
//	    redisstream.WithLogger(logger),
//	    redisstream.WithMiddleware(
//	        xbus.TimeoutMiddleware(15*time.Second),
//	        xbus.RetryMiddleware(xbus.RetryConfig{MaxAttempts: 5}),
//	    ),
//	)
//
// Configuration:
//
// - Addr:            Redis address (default: 127.0.0.1:6379)
// - Group:           Consumer group name (default: "xbus")
// - Consumer:        Consumer name (default: "xbus-{hostname}-{pid}")
// - Concurrency:     Worker goroutines per subscription (default: 8)
// - BatchSize:       XREADGROUP COUNT (default: 128)
// - Block:           XREADGROUP BLOCK duration (default: 5s)
// - AutoCreate:      Auto-create group/stream (default: true)
// - DeadLetter:      Dead-letter stream name (optional)
// - MaxLenApprox:    Stream max length for trimming (optional)
// - ClaimMinIdle:    Idle threshold for crash recovery (optional)
//
// Features:
//
// - Automatic consumer group management
// - Pending entry recovery (crash resilience)
// - Dead-letter queue support
// - Batch message handling with pipelined XADD
// - TLS support
// - Production-grade metrics
