package actor

import (
	"context"
	"errors"
	"sync"
)

var ErrMaintenance = errors.New("actor: maintenance admission closed")

type drainPermitKey struct{}
type drainPermit struct {
	system *System
	active bool // guarded by system.drain.mu
}
type DrainState struct {
	AdmissionClosed bool
	InFlight        uint64
	Failures        uint64
}
type drainTracker struct {
	mu       sync.Mutex
	closed   bool
	pending  uint64
	failures uint64
	durableFailures uint64
	baseline uint64
	zero     chan struct{}
}

func newDrainTracker() *drainTracker {
	zero := make(chan struct{})
	close(zero)
	return &drainTracker{zero: zero}
}
func (s *System) trackAdmission(ctx context.Context, env *serviceEnvelope) error {
	d := s.drain
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		act := activationFromContext(ctx)
		internal := act != nil && act.runtime.service.system == s && !act.maintenanceFinished
		if ctx == nil {
			ctx = context.Background()
		}
		permit, _ := ctx.Value(drainPermitKey{}).(*drainPermit)
		permitted := permit != nil && permit.system == s && permit.active
		if !internal && !permitted {
			return ErrMaintenance
		}
	}
	if d.pending == 0 {
		d.zero = make(chan struct{})
	}
	d.pending++
	env.drainTracked = true
	return nil
}

// WorkReservation keeps an in-process asynchronous handoff visible to Drain.
// Finish must be called only after delivery or a durable handoff has completed.
type WorkReservation struct {
	permit *drainPermit
	once   sync.Once
}

// ReserveWork is for trusted local queues. Once drained, new background work
// is rejected; in-flight work may transfer its outstanding obligation.
func (s *System) ReserveWork() (*WorkReservation, error) {
	if s.drain == nil {
		return &WorkReservation{}, nil
	}
	d := s.drain
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed && d.pending == 0 {
		return nil, ErrMaintenance
	}
	if d.pending == 0 {
		d.zero = make(chan struct{})
	}
	d.pending++
	return &WorkReservation{permit: &drainPermit{system: s, active: true}}, nil
}

// Context starts a fresh local activation rather than retaining the originating
// actor's ownership or call stack. The capability expires when Finish returns.
func (w *WorkReservation) Context(base context.Context) context.Context {
	if base == nil {
		base = context.Background()
	}
	if w == nil || w.permit == nil {
		return base
	}
	base = context.WithValue(base, actorContextKey{}, actorContextState{})
	return context.WithValue(base, drainPermitKey{}, w.permit)
}
func (w *WorkReservation) Finish(err error) {
	if w == nil || w.permit == nil {
		return
	}
	w.once.Do(func() {
		d := w.permit.system.drain
		d.mu.Lock()
		defer d.mu.Unlock()
		w.permit.active = false
		if err != nil {
			d.durableFailures++
		}
		d.pending--
		if d.pending == 0 {
			close(d.zero)
		}
	})
}
func (s *System) finishTracked(env *serviceEnvelope, act *serviceActivation, failed bool) {
	d := s.drain
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if act != nil {
		act.maintenanceFinished = true
	}
	if !env.drainTracked {
		return
	}
	env.drainTracked = false
	if failed {
		d.failures++
	}
	d.pending--
	if d.pending == 0 {
		close(d.zero)
	}
}

// Drain closes new root admission but permits calls originating in already
// admitted local activations to finish their work. It does not cancel handlers
// or discard queued activations. Producers must be paused before this call.
func (s *System) Drain(ctx context.Context) (DrainState, error) {
	if s.drain == nil {
		return DrainState{}, errors.New("actor: maintenance tracking was not enabled")
	}
	d := s.drain
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		d.baseline = d.failures
	}
	zero := d.zero
	d.mu.Unlock()
	select {
	case <-ctx.Done():
		return s.DrainState(), ctx.Err()
	case <-zero:
	}
	state := s.DrainState()
	if state.Failures != 0 {
		return state, errors.New("actor: admitted work failed during drain")
	}
	return state, nil
}
func (s *System) DrainState() DrainState {
	if s.drain == nil {
		return DrainState{}
	}
	d := s.drain
	d.mu.Lock()
	defer d.mu.Unlock()
	return DrainState{d.closed, d.pending, d.failures - d.baseline + d.durableFailures}
}

// MaintenanceWork permits an explicitly trusted local persistence operation
// after admission closes. This capability cannot cross a serialized RPC boundary.
func (s *System) MaintenanceWork(ctx context.Context, fn func(context.Context) error) error {
	if s.drain == nil {
		return fn(ctx)
	}
	d := s.drain
	d.mu.Lock()
	permit := &drainPermit{system: s, active: true}
	if d.pending == 0 {
		d.zero = make(chan struct{})
	}
	d.pending++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		permit.active = false
		d.pending--
		if d.pending == 0 {
			close(d.zero)
		}
	}()
	return fn(context.WithValue(ctx, drainPermitKey{}, permit))
}
func (s *System) ResumeAdmission() {
	if s.drain == nil {
		return
	}
	d := s.drain
	d.mu.Lock()
	d.closed = false
	d.baseline = d.failures
	d.mu.Unlock()
}
