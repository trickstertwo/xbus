package xbus

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ObserverPool manages asynchronous event dispatching to observers.
// Prevents slow observers from blocking the critical publish/subscribe path.
// Non-blocking design: drops events if buffer full to avoid backpressure.
type ObserverPool struct {
	eventCh   chan *Event
	workers   int
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	closed    atomic.Bool
	dropped   atomic.Uint64
	processed atomic.Uint64
}

// NewObserverPool creates a pool for async observer notification.
// workers: number of concurrent observer dispatch goroutines (4-16 for typical use)
// bufferSize: capacity of event channel (1000-5000 for burst resilience)
func NewObserverPool(ctx context.Context, workers, bufferSize int) *ObserverPool {
	if workers < 1 {
		workers = 4
	}
	if bufferSize < 1 {
		bufferSize = 1000
	}

	poolCtx, cancel := context.WithCancel(ctx)
	op := &ObserverPool{
		eventCh: make(chan *Event, bufferSize),
		workers: workers,
		ctx:     poolCtx,
		cancel:  cancel,
	}

	// Start worker goroutines for async dispatch
	for i := 0; i < workers; i++ {
		op.wg.Add(1)
		go op.worker()
	}

	return op
}

// Notify sends an event for asynchronous observer dispatch.
// Non-blocking: returns immediately, drops event if buffer is full.
// observers parameter is captured at send time to ensure they're not modified during dispatch.
func (op *ObserverPool) Notify(e Event, observers []Observer) {
	if len(observers) == 0 {
		return // Fast-path: no observers to notify
	}

	// Copy observers into event to avoid race conditions
	e.observers = make([]Observer, len(observers))
	copy(e.observers, observers)

	select {
	case op.eventCh <- &e:
		// Event queued successfully
	default:
		// Buffer full: drop event to prevent blocking publish path
		// This is intentional for high-throughput scenarios
		op.dropped.Add(1)
	}
}

// worker processes events from the channel and dispatches to observers.
func (op *ObserverPool) worker() {
	defer op.wg.Done()
	for {
		select {
		case <-op.ctx.Done():
			// Drain remaining events before exiting
			for {
				select {
				case e := <-op.eventCh:
					if e != nil {
						op.dispatchEvent(e)
					}
				default:
					return
				}
			}
		case e := <-op.eventCh:
			if e != nil {
				op.dispatchEvent(e)
				op.processed.Add(1)
			}
		}
	}
}

// dispatchEvent calls all observers for a single event.
// Tolerates observer panics to prevent pool corruption.
func (op *ObserverPool) dispatchEvent(e *Event) {
	if len(e.observers) == 0 {
		return
	}
	for _, obs := range e.observers {
		if obs != nil {
			// Recover from observer panic to prevent pool worker crash
			func() {
				defer func() {
					if r := recover(); r != nil {
						// Silent recovery; observer panic shouldn't crash pool
						// In production, log this with the error details
					}
				}()
				obs.OnEvent(*e)
			}()
		}
	}
}

// Close gracefully shuts down the observer pool.
// Waits up to timeout for workers to finish processing queued events.
func (op *ObserverPool) Close(timeout time.Duration) error {
	if op.closed.Swap(true) {
		return nil // Already closed
	}

	op.cancel()

	done := make(chan struct{})
	go func() {
		op.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return ErrObserverPoolShutdownTimeout
	}
}

// Stats returns telemetry about the observer pool.
type PoolStats struct {
	Dropped   uint64
	Processed uint64
}

// Stats returns current pool statistics.
func (op *ObserverPool) Stats() PoolStats {
	return PoolStats{
		Dropped:   op.dropped.Load(),
		Processed: op.processed.Load(),
	}
}
