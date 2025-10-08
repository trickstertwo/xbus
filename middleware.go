package xbus

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RetryConfig controls retry behavior for processing middleware.
type RetryConfig struct {
	MaxAttempts int
	Backoff     func(attempt int) time.Duration // e.g., exponential backoff
}

// RetryMiddleware provides bounded retries around a handler.
func RetryMiddleware(cfg RetryConfig) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, msg *Message) error {
			var lastErr error
			attempts := cfg.MaxAttempts
			if attempts < 1 {
				attempts = 1
			}
			for i := 1; i <= attempts; i++ {
				lastErr = next(ctx, msg)
				if lastErr == nil {
					return nil
				}
				// If context is canceled, break early.
				if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return lastErr
				}
				// Sleep between attempts.
				if cfg.Backoff != nil {
					select {
					case <-ctx.Done():
						return lastErr
					case <-time.After(cfg.Backoff(i)):
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
					// Prevent goroutine leak if caller returns early on timeout.
					recover()
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
