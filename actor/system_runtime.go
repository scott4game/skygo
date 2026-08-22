package actor

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type activationContextKey struct{}

type turnMark struct {
	activation *serviceActivation
	version    uint64
}

// TurnMark records the interleaving version of the current actor activation.
// Its fields are intentionally opaque outside this package.
type TurnMark = turnMark

func MarkTurn(ctx context.Context) TurnMark {
	act := activationFromContext(ctx)
	if act == nil {
		return TurnMark{}
	}
	return TurnMark{activation: act, version: act.interleaveVersion.Load()}
}

func InterleavedSince(ctx context.Context, mark TurnMark) bool {
	act := activationFromContext(ctx)
	return act != nil && mark.activation == act && act.interleaveVersion.Load() != mark.version
}

func activationFromContext(ctx context.Context) *serviceActivation {
	if ctx == nil {
		return nil
	}
	act, _ := ctx.Value(activationContextKey{}).(*serviceActivation)
	return act
}

// NoYield marks a pure local critical section. A mailbox Call attempted from fn
// fails before the target request is sent.
func NoYield(ctx context.Context, fn func(context.Context) error) error {
	if fn == nil {
		return nil
	}
	act := activationFromContext(ctx)
	if act == nil {
		return fn(ctx)
	}
	act.noYield.Add(1)
	defer act.noYield.Add(-1)
	return fn(ctx)
}

// Await cooperatively releases the current service while wait is blocked. It
// executes wait directly outside an actor activation and inside a NoInterleave
// service.
func Await[R any](ctx context.Context, label string, wait func(context.Context) (R, error)) (R, error) {
	var zero R
	if wait == nil {
		return zero, fmt.Errorf("%w: nil await function", ErrInvalidArgs)
	}
	act := activationFromContext(ctx)
	if act == nil {
		return wait(ctx)
	}
	return awaitActivationTyped(ctx, act, label, wait)
}

func awaitActivation(
	ctx context.Context,
	act *serviceActivation,
	label string,
	wait func(context.Context) (any, error),
) (any, error) {
	return awaitActivationTyped(ctx, act, label, wait)
}

func awaitActivationTyped[R any](
	ctx context.Context,
	act *serviceActivation,
	label string,
	wait func(context.Context) (R, error),
) (R, error) {
	var zero R
	if act.noYield.Load() > 0 {
		return zero, ErrYieldForbidden
	}
	if act.runtime != nil && act.runtime.service != nil && act.runtime.service.opts.NoInterleave {
		return wait(ctx)
	}
	accepted := make(chan error, 1)
	if err := act.runtime.emit(runtimeEvent{
		kind:       eventYield,
		activation: act,
		label:      label,
		accepted:   accepted,
	}); err != nil {
		return zero, err
	}
	if err := <-accepted; err != nil {
		return zero, err
	}

	result, waitErr := wait(ctx)
	if err := act.runtime.emit(runtimeEvent{kind: eventResume, activation: act}); err != nil {
		return zero, err
	}
	select {
	case <-act.grant:
		return result, waitErr
	case <-act.runtime.done:
		return zero, ErrServiceStopping
	}
}

type runtimeEventKind uint8

const (
	eventEnqueue runtimeEventKind = iota
	eventYield
	eventResume
	eventDone
)

type runtimeEvent struct {
	kind       runtimeEventKind
	envelope   *serviceEnvelope
	activation *serviceActivation
	label      string
	accepted   chan error
}

type callResult struct {
	message Message
	err     error
}

type serviceEnvelope struct {
	ctx             context.Context
	session         uint64
	protocol        string
	handler         handlerEntry
	request         Message
	expectsResponse bool
	slotHeld        bool
}

type serviceActivation struct {
	runtime           *serviceRuntime
	envelope          *serviceEnvelope
	grant             chan struct{}
	started           bool
	yieldAtSegment    uint64
	interleaveVersion atomic.Uint64
	noYield           atomic.Int32
	label             string
	startedAt         time.Time
}

type serviceRuntime struct {
	service   *Service
	events    chan runtimeEvent
	slots     chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func newServiceRuntime(svc *Service) *serviceRuntime {
	return &serviceRuntime{
		service: svc,
		events:  make(chan runtimeEvent),
		slots:   make(chan struct{}, svc.opts.MailboxSize),
		done:    make(chan struct{}),
	}
}

func (rt *serviceRuntime) emit(ev runtimeEvent) error {
	select {
	case rt.events <- ev:
		return nil
	case <-rt.done:
		return ErrServiceStopping
	}
}

func (rt *serviceRuntime) run() {
	defer rt.closeOnce.Do(func() { close(rt.done) })
	var (
		current     *serviceActivation
		ready       []*serviceActivation
		suspended   = make(map[*serviceActivation]struct{})
		segmentSeq  uint64
		stopping    bool
		serviceDone = rt.service.ctx.Done()
	)

	finishPending := func(act *serviceActivation, err error) {
		if act == nil || act.envelope == nil {
			return
		}
		if act.envelope.slotHeld {
			<-rt.slots
			act.envelope.slotHeld = false
		}
		if act.envelope.expectsResponse {
			rt.service.system.completeSession(act.envelope.session, callResult{err: err})
		}
	}

	schedule := func() {
		if current != nil || len(ready) == 0 {
			return
		}
		act := ready[0]
		copy(ready, ready[1:])
		ready[len(ready)-1] = nil
		ready = ready[:len(ready)-1]
		if stopping && !act.started {
			finishPending(act, ErrServiceStopping)
			return
		}
		if act.envelope.slotHeld {
			<-rt.slots
			act.envelope.slotHeld = false
		}
		if act.started && segmentSeq > act.yieldAtSegment {
			act.interleaveVersion.Add(1)
		}
		segmentSeq++
		current = act
		if !act.started {
			act.started = true
			act.startedAt = time.Now()
			go act.execute()
		}
		act.grant <- struct{}{}
	}

	for {
		if stopping && current == nil && len(ready) == 0 && len(suspended) == 0 {
			return
		}
		schedule()
		select {
		case <-serviceDone:
			stopping = true
			serviceDone = nil
			for _, act := range ready {
				if !act.started {
					finishPending(act, ErrServiceStopping)
				}
			}
			kept := ready[:0]
			for _, act := range ready {
				if act.started {
					kept = append(kept, act)
				}
			}
			ready = kept
		case ev := <-rt.events:
			switch ev.kind {
			case eventEnqueue:
				if stopping {
					finishPending(&serviceActivation{envelope: ev.envelope}, ErrServiceStopping)
					continue
				}
				ready = append(ready, &serviceActivation{
					runtime:  rt,
					envelope: ev.envelope,
					grant:    make(chan struct{}, 1),
				})
			case eventYield:
				if rt.service.opts.NoInterleave {
					ev.accepted <- ErrYieldForbidden
					continue
				}
				if current != ev.activation {
					ev.accepted <- fmt.Errorf("actor: activation is not running")
					continue
				}
				if len(suspended) >= rt.service.opts.MaxSuspended {
					ev.accepted <- ErrSuspendLimit
					continue
				}
				ev.activation.label = ev.label
				ev.activation.yieldAtSegment = segmentSeq
				suspended[ev.activation] = struct{}{}
				current = nil
				ev.accepted <- nil
			case eventResume:
				if _, ok := suspended[ev.activation]; !ok {
					continue
				}
				delete(suspended, ev.activation)
				ready = append(ready, ev.activation)
			case eventDone:
				if current == ev.activation {
					current = nil
				}
				delete(suspended, ev.activation)
			}
		}
	}
}

func (act *serviceActivation) execute() {
	<-act.grant
	env := act.envelope
	baseCtx := env.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	// A handler owns its service state, so it must run to completion even when the
	// caller stops waiting. Drop the caller's cancellation and deadline while keeping
	// its values (trace IDs); only Service.Stop below may cancel the handler.
	ctx, cancel := context.WithCancel(context.WithoutCancel(baseCtx))
	stopServiceCancel := context.AfterFunc(act.runtime.service.ctx, cancel)
	// A caller may itself be an actor, so WithoutCancel can retain its activation
	// value. Always replace it with the activation that owns this handler. The
	// await path enforces NoInterleave without erasing actor identity.
	ctx = context.WithValue(ctx, activationContextKey{}, act)

	var result callResult
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result.err = fmt.Errorf("actor: handler panic: %v\n%s", recovered, debug.Stack())
			}
		}()
		args, err := env.handler.opts.Codec.UnpackRequest(env.request)
		if err != nil {
			result.err = fmt.Errorf("%w: %v", ErrCodec, err)
			return
		}
		value, err := env.handler.fn(ctx, args)
		if err != nil {
			result.err = err
			return
		}
		if env.expectsResponse {
			result.message, err = env.handler.opts.Codec.PackResponse(value)
			if err != nil {
				result.err = fmt.Errorf("%w: %v", ErrCodec, err)
			}
		}
	}()
	stopServiceCancel()
	cancel()
	if env.expectsResponse {
		act.runtime.service.system.completeSession(env.session, result)
	} else if result.err != nil {
		system := act.runtime.service.system
		system.asyncFailed.Add(1)
		if system.onAsyncErr != nil {
			system.onAsyncErr(AsyncError{
				Service:  act.runtime.service.name,
				Protocol: env.protocol,
				Err:      result.err,
			})
		}
	}
	_ = act.runtime.emit(runtimeEvent{kind: eventDone, activation: act})
}

func (svc *Service) admit(ctx context.Context, env *serviceEnvelope) error {
	timer := time.NewTimer(svc.opts.AdmissionTimeout)
	defer timer.Stop()
	select {
	case svc.runtime.slots <- struct{}{}:
		env.slotHeld = true
	case <-timer.C:
		return ErrMailboxTimeout
	case <-ctx.Done():
		return ctx.Err()
	case <-svc.ctx.Done():
		return ErrServiceStopping
	}
	if err := svc.runtime.emit(runtimeEvent{kind: eventEnqueue, envelope: env}); err != nil {
		if env.slotHeld {
			<-svc.runtime.slots
			env.slotHeld = false
		}
		return err
	}
	return nil
}

func (svc *Service) tryAdmit(env *serviceEnvelope) error {
	select {
	case svc.runtime.slots <- struct{}{}:
		env.slotHeld = true
	default:
		return ErrMailboxFull
	}
	if err := svc.runtime.emit(runtimeEvent{kind: eventEnqueue, envelope: env}); err != nil {
		if env.slotHeld {
			<-svc.runtime.slots
			env.slotHeld = false
		}
		return err
	}
	return nil
}

func (svc *Service) callMailbox(ctx context.Context, protocol string, h handlerEntry, args []any) (any, error) {
	request, err := h.opts.Codec.PackRequest(args)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	response := make(chan callResult, 1)
	session := svc.system.nextSessionID()
	svc.system.registerSession(session, response)
	env := &serviceEnvelope{
		ctx:             ctx,
		session:         session,
		protocol:        protocol,
		handler:         h,
		request:         request,
		expectsResponse: true,
	}
	if err := svc.admit(ctx, env); err != nil {
		svc.system.cancelSession(session)
		return nil, err
	}
	timer := time.NewTimer(svc.opts.CallTimeout)
	defer timer.Stop()
	select {
	case result := <-response:
		return unpackCallResult(h, result)
	case <-timer.C:
		if svc.system.cancelSession(session) {
			svc.system.timedOut.Add(1)
			return nil, fmt.Errorf("%w: service=%s protocol=%s session=%d", ErrCallTimeout, svc.name, protocol, session)
		}
		return unpackCallResult(h, <-response)
	case <-ctx.Done():
		if svc.system.cancelSession(session) {
			return nil, ctx.Err()
		}
		return unpackCallResult(h, <-response)
	case <-svc.ctx.Done():
		if svc.system.cancelSession(session) {
			return nil, ErrServiceStopping
		}
		return unpackCallResult(h, <-response)
	}
}

func unpackCallResult(h handlerEntry, result callResult) (any, error) {
	if result.err != nil {
		return nil, result.err
	}
	value, err := h.opts.Codec.UnpackResponse(result.message)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	return value, nil
}

func (svc *Service) sendMailbox(ctx context.Context, protocol string, h handlerEntry, args []any) error {
	request, err := h.opts.Codec.PackRequest(args)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCodec, err)
	}
	return svc.admit(ctx, &serviceEnvelope{
		ctx:      ctx,
		protocol: protocol,
		handler:  h,
		request:  request,
	})
}

func (svc *Service) trySendMailbox(ctx context.Context, protocol string, h handlerEntry, args []any) error {
	request, err := h.opts.Codec.PackRequest(args)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCodec, err)
	}
	return svc.tryAdmit(&serviceEnvelope{
		ctx:      ctx,
		protocol: protocol,
		handler:  h,
		request:  request,
	})
}
