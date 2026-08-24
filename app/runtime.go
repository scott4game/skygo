// Package app coordinates ordered startup, readiness, signal handling, and
// reverse-order shutdown for long-running services.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Component is one independently startable runtime resource.
type Component interface {
	Start(context.Context) error
	Stop(context.Context) error
}

// ComponentFuncs adapts functions into a Component.
type ComponentFuncs struct {
	StartFunc func(context.Context) error
	StopFunc  func(context.Context) error
}

func (c ComponentFuncs) Start(ctx context.Context) error {
	if c.StartFunc == nil {
		return nil
	}
	return c.StartFunc(ctx)
}

func (c ComponentFuncs) Stop(ctx context.Context) error {
	if c.StopFunc == nil {
		return nil
	}
	return c.StopFunc(ctx)
}

type entry struct {
	name      string
	component Component
}

// Runtime owns a fixed ordered component list.
type Runtime struct {
	mu      sync.Mutex
	entries []entry
	started int
	ready   bool
	stopped bool
}

// Add appends a component. Components cannot be added after Start begins.
func (r *Runtime) Add(name string, component Component) error {
	if name == "" || component == nil {
		return fmt.Errorf("app: invalid component")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started != 0 || r.ready || r.stopped {
		return fmt.Errorf("app: runtime already started")
	}
	for _, current := range r.entries {
		if current.name == name {
			return fmt.Errorf("app: duplicate component %q", name)
		}
	}
	r.entries = append(r.entries, entry{name: name, component: component})
	return nil
}

// Start starts components in registration order. A failure rolls back the
// already-started prefix in reverse order.
func (r *Runtime) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.started != 0 || r.ready || r.stopped {
		r.mu.Unlock()
		return fmt.Errorf("app: runtime cannot start in current state")
	}
	entries := append([]entry(nil), r.entries...)
	r.mu.Unlock()

	for i, current := range entries {
		if err := current.component.Start(ctx); err != nil {
			r.mu.Lock()
			r.started = i
			r.mu.Unlock()
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			rollbackErr := r.stopStarted(rollbackCtx)
			cancel()
			return errors.Join(fmt.Errorf("app: start %s: %w", current.name, err), rollbackErr)
		}
		r.mu.Lock()
		r.started = i + 1
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.ready = true
	r.mu.Unlock()
	return nil
}

// Ready reports whether every component started successfully and Stop has not begun.
func (r *Runtime) Ready() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ready && !r.stopped
}

// Stop stops started components in reverse registration order.
func (r *Runtime) Stop(ctx context.Context) error {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return nil
	}
	r.stopped = true
	r.ready = false
	r.mu.Unlock()
	return r.stopStarted(ctx)
}

func (r *Runtime) stopStarted(ctx context.Context) error {
	r.mu.Lock()
	count := r.started
	entries := append([]entry(nil), r.entries[:count]...)
	r.started = 0
	r.mu.Unlock()
	var result error
	for i := len(entries) - 1; i >= 0; i-- {
		if err := entries[i].component.Stop(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("app: stop %s: %w", entries[i].name, err))
		}
	}
	return result
}

// Run starts the runtime, waits for context cancellation or an interrupt, and
// applies a bounded graceful shutdown.
func (r *Runtime) Run(ctx context.Context, shutdownTimeout time.Duration) error {
	if shutdownTimeout <= 0 {
		shutdownTimeout = 15 * time.Second
	}
	signalCtx, cancelSignals := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancelSignals()
	if err := r.Start(signalCtx); err != nil {
		return err
	}
	<-signalCtx.Done()
	stopCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return r.Stop(stopCtx)
}
