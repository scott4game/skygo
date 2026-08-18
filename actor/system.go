package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrServiceNotFound = errors.New("actor: service not found")
	// ErrActorNotFound remains only for source-compatible legacy adapters.
	ErrActorNotFound        = errors.New("actor: not found")
	ErrServiceNotReady      = errors.New("actor: service not ready")
	ErrServiceStopping      = errors.New("actor: service stopping")
	ErrStaleRef             = errors.New("actor: stale service ref")
	ErrProtocolNotFound     = errors.New("actor: protocol not found")
	ErrProtocolExists       = errors.New("actor: protocol already registered")
	ErrProtocolTypeMismatch = errors.New("actor: protocol descriptor type mismatch")
	ErrInvalidArgs          = errors.New("actor: invalid protocol arguments")
	ErrCodec                = errors.New("actor: codec failure")
	ErrMailboxTimeout       = errors.New("actor: mailbox admission timeout")
	ErrMailboxFull          = errors.New("actor: mailbox full")
	ErrCallTimeout          = errors.New("actor: call completion timeout")
	ErrSuspendLimit         = errors.New("actor: suspended activation limit reached")
	ErrYieldForbidden       = errors.New("actor: yield forbidden in critical section")
)

// Address is a process-local service handle, analogous to a Skynet service handle.
type Address uint64

// Ref identifies one concrete generation of a service. A Ref becomes stale when
// that service stops, even if another service later reuses the same name.
type Ref struct {
	Address    Address
	Generation uint64
	system     *System
}

func (r Ref) valid() bool { return r.system != nil && r.Address != 0 && r.Generation != 0 }

// IsZero reports whether r does not identify a service.
func (r Ref) IsZero() bool { return !r.valid() }

// ProtocolHandler is the dynamically-dispatched function shape used by a
// service protocol. It deliberately differs from Handler, which is the legacy
// typed Actor task shape kept until the final migration removes that API.
type ProtocolHandler func(context.Context, []any) (any, error)

// Handler is retained as a source-compatibility function shape for packages
// that test command adapters. It has no runtime submission API; Service/Ref is
// the only actor scheduler.
type Handler[S any] func(context.Context, S) error

// HandlerOptions configures a dynamically registered protocol handler.
type HandlerOptions struct {
	Codec Codec
}

// ServiceOptions configures mailbox capacity, timeouts, warning thresholds,
// and interleaving behavior for one service.
type ServiceOptions struct {
	MailboxSize      int
	MaxSuspended     int
	AdmissionTimeout time.Duration
	CallTimeout      time.Duration
	ActiveWarn       time.Duration
	SuspendedWarn    time.Duration
	// NoInterleave keeps each activation atomic end to end: a Call made from a
	// handler blocks this service instead of cooperatively yielding to the next
	// message. Use it for services whose handlers do read-modify-write across a
	// cross-service call and would break if another message ran in between.
	//
	// The cost is head-of-line blocking, and re-entrant calls back into the same
	// service deadlock rather than yielding — route those through Send instead.
	NoInterleave bool
}

func (o ServiceOptions) withDefaults() ServiceOptions {
	if o.MailboxSize <= 0 {
		o.MailboxSize = 64
	}
	if o.MaxSuspended <= 0 {
		o.MaxSuspended = 1024
	}
	if o.AdmissionTimeout <= 0 {
		o.AdmissionTimeout = time.Second
	}
	if o.CallTimeout <= 0 {
		o.CallTimeout = 5 * time.Second
	}
	if o.ActiveWarn <= 0 {
		o.ActiveWarn = 100 * time.Millisecond
	}
	if o.SuspendedWarn <= 0 {
		o.SuspendedWarn = 3 * time.Second
	}
	return o
}

// SystemOptions configures process-wide actor runtime callbacks.
type SystemOptions struct {
	// UnknownResponse is called after a response loses the race with timeout or
	// cancellation and its session has already been removed.
	UnknownResponse func(session uint64)
	// AsyncError observes errors and recovered panics from fire-and-forget
	// Notification handlers after Send has successfully admitted the message.
	AsyncError func(AsyncError)
	// Observer receives completed synchronous call events. Observer panics are
	// recovered and never change call results.
	Observer Observer
}

// SystemStats is a snapshot of runtime timeout and asynchronous failure counters.
type SystemStats struct {
	TimedOutCalls    uint64
	UnknownResponses uint64
	AsyncFailures    uint64
}

// AsyncError describes a failed fire-and-forget handler invocation.
type AsyncError struct {
	Service  string
	Protocol string
	Err      error
}

type pendingCall struct {
	response chan callResult
}

type serviceState uint32

const (
	serviceReserved serviceState = iota
	serviceRunning
	serviceStopping
	serviceStopped
)

type handlerEntry struct {
	opts       HandlerOptions
	fn         ProtocolHandler
	descriptor *methodToken
}

// System owns process-local service names, addresses, generations, and sessions.
type System struct {
	mu             sync.RWMutex
	byName         map[string]*Service
	byAddress      map[Address]*Service
	nextAddress    atomic.Uint64
	nextGeneration atomic.Uint64
	nextSession    atomic.Uint64
	pending        sync.Map
	timedOut       atomic.Uint64
	unknown        atomic.Uint64
	asyncFailed    atomic.Uint64
	onUnknown      func(uint64)
	onAsyncErr     func(AsyncError)
	observer       Observer
	stopping       atomic.Bool
}

// NewSystem creates an empty process-local actor system.
func NewSystem(opts SystemOptions) *System {
	return &System{
		byName:     make(map[string]*Service),
		byAddress:  make(map[Address]*Service),
		onUnknown:  opts.UnknownResponse,
		onAsyncErr: opts.AsyncError,
		observer:   opts.Observer,
	}
}

// Service is one addressable actor service and its protocol dispatcher.
type Service struct {
	system       *System
	name         string
	ref          Ref
	opts         ServiceOptions
	mu           sync.RWMutex
	handlers     map[string]handlerEntry
	state        atomic.Uint32
	ctx          context.Context
	cancel       context.CancelFunc
	runtime      *serviceRuntime
	finalizeOnce sync.Once
	finalized    chan struct{}
}

// Reserve allocates an address and reserves name without publishing the service.
func (s *System) Reserve(name string, opts ServiceOptions) (*Service, Ref, error) {
	if s == nil || s.stopping.Load() {
		return nil, Ref{}, ErrServiceStopping
	}
	if name == "" {
		return nil, Ref{}, fmt.Errorf("%w: empty service name", ErrInvalidArgs)
	}
	opts = opts.withDefaults()
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byName[name]; exists {
		return nil, Ref{}, fmt.Errorf("actor: duplicate service name %q", name)
	}
	address := Address(s.nextAddress.Add(1))
	generation := s.nextGeneration.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	ref := Ref{Address: address, Generation: generation, system: s}
	svc := &Service{
		system:    s,
		name:      name,
		ref:       ref,
		opts:      opts,
		handlers:  make(map[string]handlerEntry),
		ctx:       ctx,
		cancel:    cancel,
		finalized: make(chan struct{}),
	}
	svc.state.Store(uint32(serviceReserved))
	s.byName[name] = svc
	s.byAddress[address] = svc
	return svc, ref, nil
}

// Resolve returns the running service registered under name.
func (s *System) Resolve(name string) (Ref, error) {
	if s == nil {
		return Ref{}, ErrServiceNotFound
	}
	s.mu.RLock()
	svc := s.byName[name]
	s.mu.RUnlock()
	if svc == nil {
		return Ref{}, fmt.Errorf("%w: %s", ErrServiceNotFound, name)
	}
	if serviceState(svc.state.Load()) != serviceRunning {
		return Ref{}, fmt.Errorf("%w: %s", ErrServiceNotReady, name)
	}
	return svc.ref, nil
}

func (s *System) service(ref Ref) (*Service, error) {
	if s == nil || !ref.valid() || ref.system != s {
		return nil, ErrStaleRef
	}
	s.mu.RLock()
	svc := s.byAddress[ref.Address]
	s.mu.RUnlock()
	if svc == nil || svc.ref.Generation != ref.Generation {
		return nil, ErrStaleRef
	}
	switch serviceState(svc.state.Load()) {
	case serviceRunning:
		return svc, nil
	case serviceReserved:
		return nil, ErrServiceNotReady
	default:
		return nil, ErrServiceStopping
	}
}

func (s *System) nextSessionID() uint64 { return s.nextSession.Add(1) }

func (s *System) registerSession(session uint64, response chan callResult) {
	s.pending.Store(session, pendingCall{response: response})
}

func (s *System) cancelSession(session uint64) bool {
	_, loaded := s.pending.LoadAndDelete(session)
	return loaded
}

func (s *System) completeSession(session uint64, result callResult) bool {
	value, loaded := s.pending.LoadAndDelete(session)
	if !loaded {
		s.unknown.Add(1)
		if s.onUnknown != nil {
			s.onUnknown(session)
		}
		return false
	}
	pending := value.(pendingCall)
	pending.response <- result
	return true
}

// Stats returns a snapshot of system counters.
func (s *System) Stats() SystemStats {
	if s == nil {
		return SystemStats{}
	}
	return SystemStats{
		TimedOutCalls:    s.timedOut.Load(),
		UnknownResponses: s.unknown.Load(),
		AsyncFailures:    s.asyncFailed.Load(),
	}
}

func (svc *Service) Ref() Ref { return svc.ref }

func (svc *Service) Name() string {
	if svc == nil {
		return ""
	}
	return svc.name
}

func (svc *Service) Handle(protocol string, opts HandlerOptions, fn ProtocolHandler) error {
	return svc.handle(protocol, opts, fn, nil)
}

func (svc *Service) handle(protocol string, opts HandlerOptions, fn ProtocolHandler, descriptor *methodToken) error {
	if svc == nil || protocol == "" || fn == nil || opts.Codec == nil {
		return fmt.Errorf("%w: invalid handler registration", ErrInvalidArgs)
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if serviceState(svc.state.Load()) != serviceReserved {
		return fmt.Errorf("actor: handlers are immutable after service start")
	}
	if _, exists := svc.handlers[protocol]; exists {
		return fmt.Errorf("%w: %s", ErrProtocolExists, protocol)
	}
	svc.handlers[protocol] = handlerEntry{opts: opts, fn: fn, descriptor: descriptor}
	return nil
}

func (svc *Service) handler(protocol string) (handlerEntry, error) {
	svc.mu.RLock()
	h, ok := svc.handlers[protocol]
	svc.mu.RUnlock()
	if !ok {
		return handlerEntry{}, fmt.Errorf("%w: service=%s protocol=%s", ErrProtocolNotFound, svc.name, protocol)
	}
	return h, nil
}

// HasProtocol reports whether a handler has been registered for protocol.
func (svc *Service) HasProtocol(protocol string) bool {
	if svc == nil || protocol == "" {
		return false
	}
	svc.mu.RLock()
	_, ok := svc.handlers[protocol]
	svc.mu.RUnlock()
	return ok
}

// HasProtocol reports whether ref currently names a service with protocol registered.
// It also works while a service is reserved, allowing bootstrap validation before Start.
func (s *System) HasProtocol(ref Ref, protocol string) bool {
	if s == nil || !ref.valid() || ref.system != s {
		return false
	}
	s.mu.RLock()
	svc := s.byAddress[ref.Address]
	s.mu.RUnlock()
	return svc != nil && svc.ref.Generation == ref.Generation && svc.HasProtocol(protocol)
}

func (svc *Service) Start(_ context.Context) error {
	if svc == nil {
		return ErrServiceNotFound
	}
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if serviceState(svc.state.Load()) != serviceReserved {
		return fmt.Errorf("actor: service %s cannot start from state %d", svc.name, svc.state.Load())
	}
	svc.runtime = newServiceRuntime(svc)
	// Publish running only after runtime is fully initialized. Callers may hold a
	// Ref obtained from Reserve and race directly with Start.
	svc.state.Store(uint32(serviceRunning))
	go svc.runtime.run()
	return nil
}

func (svc *Service) Stop(ctx context.Context) error {
	if svc == nil {
		return nil
	}
	svc.mu.Lock()
	state := serviceState(svc.state.Load())
	switch state {
	case serviceStopped:
		svc.mu.Unlock()
		return nil
	case serviceReserved:
		svc.cancel()
		svc.mu.Unlock()
		svc.beginFinalize(nil)
		return svc.waitFinalized(ctx)
	case serviceRunning:
		svc.state.Store(uint32(serviceStopping))
		svc.cancel()
	}
	runtime := svc.runtime
	svc.mu.Unlock()
	var runtimeDone <-chan struct{}
	if runtime != nil {
		runtimeDone = runtime.done
	}
	svc.beginFinalize(runtimeDone)
	return svc.waitFinalized(ctx)
}

func (svc *Service) beginFinalize(runtimeDone <-chan struct{}) {
	svc.finalizeOnce.Do(func() {
		go func() {
			if runtimeDone != nil {
				<-runtimeDone
			}
			svc.unpublish()
			svc.state.Store(uint32(serviceStopped))
			close(svc.finalized)
		}()
	})
}

func (svc *Service) waitFinalized(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-svc.finalized:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (svc *Service) unpublish() {
	if svc == nil || svc.system == nil {
		return
	}
	svc.system.mu.Lock()
	if svc.system.byName[svc.name] == svc {
		delete(svc.system.byName, svc.name)
	}
	if svc.system.byAddress[svc.ref.Address] == svc {
		delete(svc.system.byAddress, svc.ref.Address)
	}
	svc.system.mu.Unlock()
}

// Stop stops every service and waits until ctx expires or shutdown completes.
func (s *System) Stop(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.stopping.Store(true)
	s.mu.RLock()
	services := make([]*Service, 0, len(s.byAddress))
	for _, svc := range s.byAddress {
		services = append(services, svc)
	}
	s.mu.RUnlock()
	for _, svc := range services {
		if err := svc.Stop(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Call invokes protocol on ref. A handler calling another actor cooperatively
// suspends its current activation while it waits for the response.
func Call(ctx context.Context, ref Ref, protocol string, args ...any) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgs)
	}
	if !ref.valid() {
		return nil, ErrStaleRef
	}
	svc, err := ref.system.service(ref)
	if err != nil {
		return nil, err
	}
	caller := "<external>"
	if act := activationFromContext(ctx); act != nil && act.runtime != nil && act.runtime.service != nil {
		caller = act.runtime.service.name
	}
	started := time.Now()
	observe := func(err error) {
		ref.system.observeCall(CallEvent{
			Caller: caller, Callee: svc.name, Protocol: protocol,
			Duration: time.Since(started), Err: err,
		})
	}
	h, err := svc.handler(protocol)
	if err != nil {
		observe(err)
		return nil, err
	}
	wait := func(waitCtx context.Context) (any, error) {
		return svc.callMailbox(waitCtx, protocol, h, args)
	}
	if act := activationFromContext(ctx); act != nil {
		value, callErr := awaitActivation(ctx, act, svc.name+"."+protocol, wait)
		observe(callErr)
		return value, callErr
	}
	value, callErr := wait(ctx)
	observe(callErr)
	return value, callErr
}

func Send(ctx context.Context, ref Ref, protocol string, args ...any) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidArgs)
	}
	if !ref.valid() {
		return ErrStaleRef
	}
	svc, err := ref.system.service(ref)
	if err != nil {
		return err
	}
	h, err := svc.handler(protocol)
	if err != nil {
		return err
	}
	return svc.sendMailbox(ctx, protocol, h, args)
}

// TrySend admits a mailbox notification only when capacity is immediately
// available. Once admitted, delivery follows the same ownership and scheduling
// rules as Send.
func TrySend(ctx context.Context, ref Ref, protocol string, args ...any) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidArgs)
	}
	if !ref.valid() {
		return ErrStaleRef
	}
	svc, err := ref.system.service(ref)
	if err != nil {
		return err
	}
	h, err := svc.handler(protocol)
	if err != nil {
		return err
	}
	return svc.trySendMailbox(ctx, protocol, h, args)
}
