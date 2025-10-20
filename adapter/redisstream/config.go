package redisstream

import (
	"fmt"
	"os"
	"time"
)

// Config for Redis Streams transport with production-grade settings.
type Config struct {
	// Redis client options
	Addr          string // Redis address (default: 127.0.0.1:6379)
	Username      string // Auth username (optional)
	Password      string // Auth password (optional)
	DB            int    // Database number (default: 0)
	TLS           bool   // Enable TLS
	TLSServerName string // TLS server name for verification

	// Consumer group options
	Group       string        // Consumer group name (default: "xbus")
	Consumer    string        // Consumer name (default: "xbus-{hostname}-{pid}")
	Concurrency int           // Worker goroutines per subscription (default: 8)
	BatchSize   int           // XREADGROUP COUNT (default: 128)
	Block       time.Duration // XREADGROUP BLOCK duration (default: 5s)
	AutoCreate  bool          // Auto-create group/stream (default: true)

	// Acknowledgment and stream management
	AutoDeleteOnAck bool   // XDEL after XACK (default: false)
	DeadLetter      string // Dead-letter queue stream name (optional)
	MaxLenApprox    int64  // Stream _max length trimming (default: 0, no limit)

	// Pending entry recovery (optional, for crash recovery)
	ClaimMinIdle  time.Duration // Idle time threshold for pending claim (default: 0, disabled)
	ClaimBatch    int           // Max entries to claim per interval (default: 128)
	ClaimInterval time.Duration // Claim interval (default: 15s)
}

// toMap converts typed Config into the generic map for transport factory.
func (c Config) toMap() map[string]any {
	return map[string]any{
		"addr":               c.Addr,
		"username":           c.Username,
		"password":           c.Password,
		"db":                 c.DB,
		"tls":                c.TLS,
		"tls_server_name":    c.TLSServerName,
		"group":              c.Group,
		"consumer":           c.Consumer,
		"concurrency":        c.Concurrency,
		"batch_size":         c.BatchSize,
		"block":              c.Block,
		"auto_create":        c.AutoCreate,
		"auto_delete_on_ack": c.AutoDeleteOnAck,
		"dead_letter":        c.DeadLetter,
		"max_len_approx":     c.MaxLenApprox,
		"claim_min_idle":     c.ClaimMinIdle,
		"claim_batch":        c.ClaimBatch,
		"claim_interval":     c.ClaimInterval,
	}
}

// ConfigFromMap safely converts a generic map into Config with production defaults.
func ConfigFromMap(cfg map[string]any) Config {
	getString := func(k, d string) string {
		if v, ok := cfg[k].(string); ok && v != "" {
			return v
		}
		return d
	}

	getInt := func(k string, d int) int {
		switch v := cfg[k].(type) {
		case int:
			return v
		case int32:
			return int(v)
		case int64:
			return int(v)
		case float64:
			return int(v)
		}
		return d
	}

	getInt64 := func(k string, d int64) int64 {
		switch v := cfg[k].(type) {
		case int:
			return int64(v)
		case int32:
			return int64(v)
		case int64:
			return v
		case float64:
			return int64(v)
		}
		return d
	}

	getBool := func(k string, d bool) bool {
		if v, ok := cfg[k].(bool); ok {
			return v
		}
		return d
	}

	getDur := func(k string, d time.Duration) time.Duration {
		switch v := cfg[k].(type) {
		case time.Duration:
			return v
		case string:
			if p, err := time.ParseDuration(v); err == nil {
				return p
			}
		case float64:
			return time.Duration(v)
		}
		return d
	}

	// Generate default consumer name from hostname and PID
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "xbus"
	}

	return Config{
		Addr:          getString("addr", "127.0.0.1:6379"),
		Username:      getString("username", ""),
		Password:      getString("password", ""),
		DB:            getInt("db", 0),
		TLS:           getBool("tls", false),
		TLSServerName: getString("tls_server_name", ""),

		// Consumer group defaults (production-friendly)
		Group:       getString("group", "xbus"),
		Consumer:    getString("consumer", fmt.Sprintf("xbus-%s-%d", hostname, os.Getpid())),
		Concurrency: _max(1, getInt("concurrency", 8)),
		BatchSize:   _max(1, getInt("batch_size", 128)),
		Block:       getDur("block", 5*time.Second),
		AutoCreate:  getBool("auto_create", true),

		// Stream management
		AutoDeleteOnAck: getBool("auto_delete_on_ack", false),
		DeadLetter:      getString("dead_letter", ""),
		MaxLenApprox:    getInt64("max_len_approx", 0),

		// Pending entry recovery (disabled by default, enable for resilience)
		ClaimMinIdle:  getDur("claim_min_idle", 0),
		ClaimBatch:    _max(1, getInt("claim_batch", 128)),
		ClaimInterval: getDur("claim_interval", 15*time.Second),
	}
}
