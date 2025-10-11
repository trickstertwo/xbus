package redisstream

import (
	"fmt"
	"os"
	"time"
)

// Config for Redis Streams transport.
type Config struct {
	// Client options
	Addr          string
	Username      string
	Password      string
	DB            int
	TLS           bool
	TLSServerName string

	// Consumer options
	Group       string
	Consumer    string
	Concurrency int
	BatchSize   int
	Block       time.Duration
	AutoCreate  bool

	// Acknowledgment & stream trimming
	AutoDeleteOnAck bool
	DeadLetter      string
	MaxLenApprox    int64

	// Pending entries recovery (optional)
	ClaimMinIdle  time.Duration
	ClaimBatch    int
	ClaimInterval time.Duration
}

// toMap converts typed Config into the generic map expected by the transport factory.
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

// ConfigFromMap safely converts cfg into Config with defaults.
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

		Group:       getString("group", "xbus"),
		Consumer:    getString("consumer", fmt.Sprintf("xbus-%s-%d", hostname, os.Getpid())),
		Concurrency: getInt("concurrency", 8),
		BatchSize:   getInt("batch_size", 128),
		Block:       getDur("block", 5*time.Second),
		AutoCreate:  getBool("auto_create", true),

		AutoDeleteOnAck: getBool("auto_delete_on_ack", false),
		DeadLetter:      getString("dead_letter", ""),
		MaxLenApprox:    getInt64("max_len_approx", 0),

		ClaimMinIdle:  getDur("claim_min_idle", 0),
		ClaimBatch:    getInt("claim_batch", 128),
		ClaimInterval: getDur("claim_interval", 15*time.Second),
	}
}
