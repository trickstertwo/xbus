package redisstream

import (
	"fmt"
	"os"
	"time"
)

// Config for Redis Streams transport with production-grade settings.
type Config struct {
	// Connection
	Addr          string
	Username      string
	Password      string
	DB            int
	TLS           bool
	TLSServerName string

	// Consumer group
	Group       string
	Consumer    string
	Concurrency int
	BatchSize   int
	Block       time.Duration
	AutoCreate  bool

	// Stream management
	AutoDeleteOnAck bool
	DeadLetter      string

	// Stream trimming (retention policy)
	MaxLen       int64         // Hard limit on stream length (XADD MAXLEN)
	MaxLenApprox int64         // Approximate limit (faster, use ~)
	MinIdleTime  time.Duration // Remove entries older than this duration
	TrimInterval time.Duration // How often to trim (if not trimming on every XADD)

	// Pending entry recovery (automatic crash recovery)
	ClaimMinIdle  time.Duration
	ClaimBatch    int
	ClaimInterval time.Duration
}

// Defaults returns a Config with production-safe defaults.
func Defaults() Config {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "xbus"
	}

	return Config{
		Addr:            "127.0.0.1:6379",
		DB:              0,
		TLS:             false,
		Group:           "xbus",
		Consumer:        fmt.Sprintf("xbus-%s-%d", hostname, os.Getpid()),
		Concurrency:     8,
		BatchSize:       128,
		Block:           5 * time.Second,
		AutoCreate:      true,
		AutoDeleteOnAck: false,

		// Default: Keep last 1M entries (reasonable for most use cases)
		MaxLenApprox: 1_000_000,
		TrimInterval: 0, // Trim on every XADD (safest, minimal overhead with APPROX)

		ClaimBatch:    128,
		ClaimInterval: 15 * time.Second,
	}
}

// Validate checks Config for production readiness.
func (c Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("config: addr required")
	}
	if c.Group == "" {
		return fmt.Errorf("config: group required")
	}
	if c.Consumer == "" {
		return fmt.Errorf("config: consumer required")
	}
	if c.Concurrency < 1 {
		return fmt.Errorf("config: concurrency must be >= 1, got %d", c.Concurrency)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("config: batch_size must be >= 1, got %d", c.BatchSize)
	}
	if c.Block <= 0 {
		return fmt.Errorf("config: block must be > 0, got %v", c.Block)
	}
	if c.MaxLen > 0 && c.MaxLenApprox > 0 {
		return fmt.Errorf("config: cannot set both max_len (hard) and max_len_approx (soft)")
	}
	if c.TrimInterval < 0 {
		return fmt.Errorf("config: trim_interval must be >= 0")
	}
	if c.ClaimMinIdle > 0 && c.ClaimInterval <= 0 {
		return fmt.Errorf("config: claim_interval must be > 0 if claim_min_idle is set")
	}
	return nil
}

// toMap converts Config to generic map for transport factory.
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
		"max_len":            c.MaxLen,
		"max_len_approx":     c.MaxLenApprox,
		"min_idle_time":      c.MinIdleTime,
		"trim_interval":      c.TrimInterval,
		"claim_min_idle":     c.ClaimMinIdle,
		"claim_batch":        c.ClaimBatch,
		"claim_interval":     c.ClaimInterval,
	}
}

// ConfigFromMap safely converts generic map to Config with defaults.
func ConfigFromMap(m map[string]any) Config {
	c := Defaults()

	if v, ok := m["addr"].(string); ok && v != "" {
		c.Addr = v
	}
	if v, ok := m["username"].(string); ok {
		c.Username = v
	}
	if v, ok := m["password"].(string); ok {
		c.Password = v
	}
	if v, ok := m["db"].(int); ok {
		c.DB = v
	}
	if v, ok := m["tls"].(bool); ok {
		c.TLS = v
	}
	if v, ok := m["tls_server_name"].(string); ok {
		c.TLSServerName = v
	}
	if v, ok := m["group"].(string); ok && v != "" {
		c.Group = v
	}
	if v, ok := m["consumer"].(string); ok && v != "" {
		c.Consumer = v
	}
	if v, ok := m["concurrency"].(int); ok && v > 0 {
		c.Concurrency = v
	}
	if v, ok := m["batch_size"].(int); ok && v > 0 {
		c.BatchSize = v
	}
	if v, ok := m["block"].(time.Duration); ok && v > 0 {
		c.Block = v
	}
	if v, ok := m["auto_create"].(bool); ok {
		c.AutoCreate = v
	}
	if v, ok := m["auto_delete_on_ack"].(bool); ok {
		c.AutoDeleteOnAck = v
	}
	if v, ok := m["dead_letter"].(string); ok {
		c.DeadLetter = v
	}
	if v, ok := m["max_len"].(int64); ok && v > 0 {
		c.MaxLen = v
	}
	if v, ok := m["max_len_approx"].(int64); ok && v > 0 {
		c.MaxLenApprox = v
	}
	if v, ok := m["min_idle_time"].(time.Duration); ok {
		c.MinIdleTime = v
	}
	if v, ok := m["trim_interval"].(time.Duration); ok {
		c.TrimInterval = v
	}
	if v, ok := m["claim_min_idle"].(time.Duration); ok {
		c.ClaimMinIdle = v
	}
	if v, ok := m["claim_batch"].(int); ok && v > 0 {
		c.ClaimBatch = v
	}
	if v, ok := m["claim_interval"].(time.Duration); ok && v > 0 {
		c.ClaimInterval = v
	}

	return c
}
