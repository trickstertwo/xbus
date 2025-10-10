package xbus

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

// RetryConfig controls retry behavior for processing middleware.
type RetryConfig struct {
	// MaxAttempts is the total number of attempts including the first execution.
	MaxAttempts int
	// Backoff computes the base wait before the next attempt (e.g., exponential backoff).
	Backoff func(attempt int) time.Duration
	// RetryIf, when provided, returns true if the error should be retried.
	// If nil, all errors are retried (bounded by MaxAttempts).
	RetryIf func(err error) bool
	// Jitter adds up to [0, Jitter] random delay to the base backoff to avoid thundering herds.
	Jitter time.Duration
}

// RetryMiddleware provides bounded, selective retries around a handler.
func RetryMiddleware(cfg RetryConfig) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			var lastErr error
			attempts := cfg.MaxAttempts
			if attempts < 1 {
				attempts = 1
			}
			shouldRetry := cfg.RetryIf
			if shouldRetry == nil {
				shouldRetry = func(error) bool { return true }
			}
			for i := 1; i <= attempts; i++ {
				lastErr = next(ctx, msg)
				if lastErr == nil {
					return nil
				}
				// Stop if context is canceled or deadline exceeded.
				if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return lastErr
				}
				// If we won't retry, return immediately.
				if i == attempts || !shouldRetry(lastErr) {
					return lastErr
				}
				// Sleep between attempts with optional jitter.
				if cfg.Backoff != nil {
					wait := cfg.Backoff(i)
					if cfg.Jitter > 0 {
						j := time.Duration(rand.Int63n(int64(cfg.Jitter)))
						wait += j
					}
					select {
					case <-ctx.Done():
						return lastErr
					case <-time.After(wait):
					}
				}
			}
			return lastErr
		}
	}
}

// TimeoutMiddleware enforces a maximum processing time for a handler.
// When exceeded, it returns context.DeadlineExceeded and allows transport Nack/handling.
func TimeoutMiddleware(d time.Duration) Middleware {
	if d <= 0 {
		// No-op if duration invalid.
		return func(next Handler) Handler { return next }
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			tctx, cancel := context.WithTimeout(ctx, d)
			defer cancel()

			errCh := make(chan error, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						errCh <- fmt.Errorf("panic recovered: %v", r)
					}
				}()
				errCh <- next(tctx, msg)
			}()

			select {
			case <-tctx.Done():
				return tctx.Err()
			case err := <-errCh:
				return err
			}
		}
	}
}

// RecoveryMiddleware prevents panics from crashing consumers and converts them into errors.
func RecoveryMiddleware() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic recovered: %v", r)
				}
			}()
			return next(ctx, msg)
		}
	}
}

// Chain composes middlewares around a handler in order.
func Chain(h Handler, mws ...Middleware) Handler {
	if len(mws) == 0 {
		return h
	}
	wrapped := h
	// Apply in reverse so that first middleware wraps last.
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		wrapped = mws[i](wrapped)
	}
	return wrapped
}
