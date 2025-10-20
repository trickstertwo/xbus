package xbus

import (
	"context"

	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// ctxKey is the base for all context keys in xbus (prevents collisions).
type ctxKey string

const (
	codecCtxKey  ctxKey = "xbus:codec"
	loggerCtxKey ctxKey = "xbus:logger"
	clockCtxKey  ctxKey = "xbus:clock"
)

// injectCodec attaches the active Codec into context for downstream handlers.
func injectCodec(ctx context.Context, c Codec) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, codecCtxKey, c)
}

// CodecFromContext retrieves a Codec previously injected into the context.
func CodecFromContext(ctx context.Context) (Codec, bool) {
	if v := ctx.Value(codecCtxKey); v != nil {
		if c, ok := v.(Codec); ok && c != nil {
			return c, true
		}
	}
	return nil, false
}

func injectLogger(ctx context.Context, l *xlog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerCtxKey, l)
}

func LoggerFromContext(ctx context.Context) (*xlog.Logger, bool) {
	if v := ctx.Value(loggerCtxKey); v != nil {
		if l, ok := v.(*xlog.Logger); ok && l != nil {
			return l, true
		}
	}
	return nil, false
}

func injectClock(ctx context.Context, c xclock.Clock) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, clockCtxKey, c)
}

func ClockFromContext(ctx context.Context) (xclock.Clock, bool) {
	if v := ctx.Value(clockCtxKey); v != nil {
		if c, ok := v.(xclock.Clock); ok && c != nil {
			return c, true
		}
	}
	return nil, false
}

// InjectAll is a convenience helper to inject all standard dependencies.
func InjectAll(ctx context.Context, codec Codec, logger *xlog.Logger, clock xclock.Clock) context.Context {
	ctx = injectCodec(ctx, codec)
	ctx = injectLogger(ctx, logger)
	ctx = injectClock(ctx, clock)
	return ctx
}
