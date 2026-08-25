package actor

import (
	"context"
	"fmt"
	"time"
)

// RemoteTarget identifies one concrete generation of a service on a node.
type RemoteTarget struct {
	Node        string
	Incarnation string
	Service     string
	Address     Address
	Generation  uint64
}

// RemoteTransport is implemented by cluster runtimes. It deliberately accepts
// encoded bytes so actor does not depend on a network implementation.
type RemoteTransport interface {
	ResolveTarget(context.Context, string, string) (RemoteTarget, error)
	Call(context.Context, RemoteTarget, string, string, []byte, []CallFrame) ([]byte, error)
	Send(context.Context, RemoteTarget, string, string, []byte, []CallFrame, bool) error
}

// RemoteResult is the eventual result of an admitted remote call.
type RemoteResult struct {
	Payload []byte
	Err     error
}

// AttachRemoteTransport sets the process node identity and its sole remote
// transport. It must be called before application services begin accepting work.
func (s *System) AttachRemoteTransport(node string, transport RemoteTransport) error {
	if s == nil || node == "" || transport == nil {
		return fmt.Errorf("%w: invalid remote transport", ErrInvalidArgs)
	}
	s.remoteMu.Lock()
	defer s.remoteMu.Unlock()
	if s.remote != nil {
		return fmt.Errorf("actor: remote transport already attached")
	}
	s.node = node
	s.remote = transport
	return nil
}

func (s *System) nodeID() string {
	if s == nil {
		return ""
	}
	s.remoteMu.RLock()
	defer s.remoteMu.RUnlock()
	return s.node
}

func (s *System) remoteTransport() RemoteTransport {
	s.remoteMu.RLock()
	defer s.remoteMu.RUnlock()
	return s.remote
}

// ResolveRemote resolves a named service to a generation-safe remote Ref.
func (s *System) ResolveRemote(ctx context.Context, node, service string) (Ref, error) {
	transport := s.remoteTransport()
	if transport == nil {
		return Ref{}, ErrRemoteUnavailable
	}
	target, err := transport.ResolveTarget(ctx, node, service)
	if err != nil {
		return Ref{}, err
	}
	if target.Node == "" || target.Address == 0 || target.Generation == 0 {
		return Ref{}, fmt.Errorf("%w: invalid target for %s/%s", ErrStaleRef, node, service)
	}
	return Ref{Address: target.Address, Generation: target.Generation, system: s, remote: &target}, nil
}

func callRemote(ctx context.Context, ref Ref, protocol string, codec WireCodec, args []any) (any, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidArgs)
	}
	transport := ref.system.remoteTransport()
	if transport == nil || ref.remote == nil {
		return nil, ErrRemoteUnavailable
	}
	payload, err := codec.MarshalRequest(args)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	act := activationFromContext(ctx)
	caller := "<external>"
	if act != nil && act.runtime != nil && act.runtime.service != nil {
		caller = act.runtime.service.name
	}
	started := time.Now()
	wait := func(waitCtx context.Context) (any, error) {
		response, callErr := transport.Call(waitCtx, *ref.remote, protocol, codec.Fingerprint(), payload, CallPath(waitCtx))
		if callErr != nil {
			return nil, callErr
		}
		value, decodeErr := codec.UnmarshalResponse(response)
		if decodeErr != nil {
			return nil, fmt.Errorf("%w: %v", ErrCodec, decodeErr)
		}
		return value, nil
	}
	var value any
	if act != nil {
		value, err = awaitActivation(ctx, act, ref.remote.Node+"/"+ref.remote.Service+"."+protocol, wait)
	} else {
		value, err = wait(ctx)
	}
	ref.system.observeCall(CallEvent{Caller: caller, Callee: ref.remote.Node + "/" + ref.remote.Service, Protocol: protocol, Duration: time.Since(started), Err: err})
	return value, err
}

func sendRemote(ctx context.Context, ref Ref, protocol string, codec WireCodec, args []any, nonBlocking bool) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidArgs)
	}
	transport := ref.system.remoteTransport()
	if transport == nil || ref.remote == nil {
		return ErrRemoteUnavailable
	}
	payload, err := codec.MarshalRequest(args)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCodec, err)
	}
	return transport.Send(ctx, *ref.remote, protocol, codec.Fingerprint(), payload, CallPath(ctx), nonBlocking)
}

// DispatchRemoteAsync validates and admits one encoded remote request before it
// returns. A call completes on the returned future; a notification returns a
// nil future after successful mailbox admission.
func (s *System) DispatchRemoteAsync(ctx context.Context, target RemoteTarget, protocol, fingerprint string, payload []byte, notify bool) (<-chan RemoteResult, error) {
	if target.Node != "" && s.nodeID() != "" && target.Node != s.nodeID() {
		return nil, ErrStaleRef
	}
	ref := Ref{Address: target.Address, Generation: target.Generation, system: s}
	svc, err := s.service(ref)
	if err != nil {
		return nil, err
	}
	h, err := svc.handler(protocol)
	if err != nil {
		return nil, err
	}
	wire, ok := h.opts.Codec.(WireCodec)
	if !ok || wire.Fingerprint() != fingerprint {
		return nil, fmt.Errorf("%w: service=%s protocol=%s", ErrProtocolTypeMismatch, svc.name, protocol)
	}
	args, err := wire.UnmarshalRequest(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	if notify {
		return nil, Send(ctx, ref, protocol, args...)
	}
	node := svc.system.nodeID()
	if svc.opts.NoInterleave && callPathContains(ctx, node, svc.name) {
		return nil, fmt.Errorf("%w: %s", ErrCallCycle, formatCallCycle(ctx, node, svc.name, protocol))
	}
	result := make(chan RemoteResult, 1)
	response := make(chan callResult, 1)
	session := svc.system.nextSessionID()
	svc.system.registerSession(session, response)
	request, err := h.opts.Codec.PackRequest(args)
	if err != nil {
		svc.system.cancelSession(session)
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	if err := svc.admit(ctx, &serviceEnvelope{
		ctx: ctx, session: session, protocol: protocol, handler: h,
		request: request, expectsResponse: true,
	}); err != nil {
		svc.system.cancelSession(session)
		return nil, err
	}
	started := time.Now()
	go func() {
		var callResult callResult
		timer := time.NewTimer(svc.opts.CallTimeout)
		defer timer.Stop()
		select {
		case callResult = <-response:
		case <-timer.C:
			if svc.system.cancelSession(session) {
				svc.system.timedOut.Add(1)
				callResult.err = fmt.Errorf("%w: service=%s protocol=%s session=%d", ErrCallTimeout, svc.name, protocol, session)
			} else {
				callResult = <-response
			}
		case <-ctx.Done():
			if svc.system.cancelSession(session) {
				callResult.err = ctx.Err()
			} else {
				callResult = <-response
			}
		case <-svc.ctx.Done():
			if svc.system.cancelSession(session) {
				callResult.err = ErrServiceStopping
			} else {
				callResult = <-response
			}
		}
		if callResult.err == nil {
			value, unpackErr := h.opts.Codec.UnpackResponse(callResult.message)
			if unpackErr != nil {
				callResult.err = fmt.Errorf("%w: %v", ErrCodec, unpackErr)
			} else {
				encoded, marshalErr := wire.MarshalResponse(value)
				if marshalErr != nil {
					callResult.err = fmt.Errorf("%w: %v", ErrCodec, marshalErr)
				} else {
					result <- RemoteResult{Payload: encoded}
					s.observeCall(CallEvent{Caller: "<external>", Callee: svc.name, Protocol: protocol, Duration: time.Since(started)})
					close(result)
					return
				}
			}
		}
		s.observeCall(CallEvent{Caller: "<external>", Callee: svc.name, Protocol: protocol, Duration: time.Since(started), Err: callResult.err})
		result <- RemoteResult{Err: callResult.err}
		close(result)
	}()
	return result, nil
}

// DispatchRemote admits one encoded remote request into a local service and
// waits for calls to complete.
func (s *System) DispatchRemote(ctx context.Context, target RemoteTarget, protocol, fingerprint string, payload []byte, notify bool) ([]byte, error) {
	future, err := s.DispatchRemoteAsync(ctx, target, protocol, fingerprint, payload, notify)
	if err != nil || notify {
		return nil, err
	}
	result := <-future
	return result.Payload, result.Err
}
