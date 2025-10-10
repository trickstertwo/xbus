package xbus

import (
	"errors"
	"fmt"
)

type ErrUnknownTransport struct{ name string }

func (e ErrUnknownTransport) Error() string { return fmt.Sprintf("unknown transport: %s", e.name) }

var (
	ErrNoTransportConfigured = errors.New("xbus: no transport configured")
)
