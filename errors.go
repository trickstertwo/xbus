package xbus

import (
	"errors"
	"fmt"
)

// ErrUnknownTransport indicates a transport is not registered.
type ErrUnknownTransport struct {
	name string
}

func (e ErrUnknownTransport) Error() string {
	return fmt.Sprintf("unknown transport: %s", e.name)
}

// Standard errors for common scenarios
var (
	// Configuration errors
	ErrNoTransportConfigured = errors.New("xbus: no transport configured")
	ErrInvalidTopic          = errors.New("xbus: topic must not be empty")
	ErrInvalidEventName      = errors.New("xbus: event name must not be empty")
	ErrInvalidPayload        = errors.New("xbus: payload must not be nil")
	ErrInvalidSubscription   = errors.New("xbus: invalid subscription parameters")

	// State errors
	ErrBusClosed                   = errors.New("xbus: bus is closed")
	ErrObserverPoolShutdownTimeout = errors.New("xbus: observer pool shutdown timeout")

	// Handler errors
	ErrHandlerPanic   = errors.New("xbus: handler panic")
	ErrHandlerTimeout = errors.New("xbus: handler timeout")
)
