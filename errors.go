package xbus

import "fmt"

type ErrUnknownTransport struct{ name string }

func (e ErrUnknownTransport) Error() string { return fmt.Sprintf("unknown transport: %s", e.name) }

var ErrDefaultBusNotInitialized = fmt.Errorf("xbus default bus not initialized")
