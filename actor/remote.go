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

// DispatchRemote admits one encoded remote request into a local service.
func (s *System) DispatchRemote(ctx context.Context, target RemoteTarget, protocol, fingerprint string, payload []byte, notify bool) ([]byte, error) {
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
	value, err := Call(ctx, ref, protocol, args...)
	if err != nil {
		return nil, err
	}
	response, err := wire.MarshalResponse(value)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCodec, err)
	}
	return response, nil
}
