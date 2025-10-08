package redisstream

// Package redisstream provides a Redis Streams adapter for xbus.
//
// Transport name: "redis-streams"
//
// Minimal config keys:
// - addr: "host:port" (default "127.0.0.1:6379")
// - group: consumer group name (default "xbus")
// - consumer: consumer name (default "xbus-1")
// - concurrency: number of workers (default 8)
// - batch_size: XREADGROUP COUNT (default 128)
// - block: XREADGROUP BLOCK duration (default 5s)
// - auto_create: create group/stream if missing (default true)
// - auto_delete_on_ack: XDEL after XACK (default false)
// - dead_letter: stream name to write failed messages (optional)
//
// Example builder usage:
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
