package xbus

import (
	"context"

	"github.com/trickstertwo/xclock"
	"github.com/trickstertwo/xlog"
)

// Reuse ctxKey from codec.go within the same package.
const (
	loggerCtxKey ctxKey = "xbus:logger"
	clockCtxKey  ctxKey = "xbus:clock"
)

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
