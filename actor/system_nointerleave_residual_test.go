package actor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// This file covers the NoInterleave activation contract in two parts.
//
// Part A — regression guards (GREEN). A NoInterleave handler carries its own
// activation (system_runtime.go execute()), so call observation keeps the real
// service identity and NoYield still enforces its critical section, while the
// await path refuses to yield the mailbox. A synchronous self-call is rejected
// with ErrCallCycle before it is ever enqueued.
//
// Part B — regression guards for multi-hop cycles, complete diagnostic paths,
// and the distinction between internal invariants and user NoYield violations.

type recordingObserver struct {
	mu     sync.Mutex
	events []CallEvent
}

func (o *recordingObserver) OnCall(event CallEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, event)
}

func (o *recordingObserver) callerOf(callee, protocol string) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, event := range o.events {
		if event.Callee == callee && event.Protocol == protocol {
			return event.Caller, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// Part A — regression guards
// ---------------------------------------------------------------------------

// Call derives CallEvent.Caller from activationFromContext. Erasing the
// activation to enforce NoInterleave would attribute every outbound call to
// "<external>", losing exactly the callgraph edges that reveal the re-entrant
// deadlock ServiceOptions.NoInterleave warns about.
func TestNoInterleaveOutboundCallKeepsCallerIdentity(t *testing.T) {
	observer := &recordingObserver{}
	system := NewSystem(SystemOptions{Observer: observer})
	defer stopTestSystem(t, system)

	caller, callerRef := newTestService(t, system, "caller")
	callee, calleeRef, err := system.Reserve("no-interleave", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	leaf, leafRef := newTestService(t, system, "leaf")

	mustHandle(t, leaf, "ping", func(context.Context, []any) (any, error) { return "ok", nil })
	mustHandle(t, callee, "via-leaf", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, leafRef, "ping")
	})
	mustHandle(t, caller, "entry", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, calleeRef, "via-leaf")
	})
	startTestService(t, caller)
	startTestService(t, callee)
	startTestService(t, leaf)

	if _, err := callWithTimeout(t, context.Background(), callerRef, "entry"); err != nil {
		t.Fatalf("entry call: %v", err)
	}

	got, ok := observer.callerOf("leaf", "ping")
	if !ok {
		t.Fatal("no CallEvent recorded for leaf.ping")
	}
	if got != "no-interleave" {
		t.Fatalf("CallEvent.Caller for leaf.ping = %q, want %q", got, "no-interleave")
	}
}

// NoYield promises that "a mailbox Call attempted from fn fails before the
// target request is sent" (see TestNoYieldRejectsCallBeforeDispatch). The
// promise must hold inside a NoInterleave service too: awaitActivationTyped
// checks noYield before it short-circuits the yield for NoInterleave.
func TestNoInterleaveNoYieldStillRejectsCall(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	critical, criticalRef, err := system.Reserve("no-interleave", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	target, targetRef := newTestService(t, system, "target")

	var mu sync.Mutex
	dispatched := false
	mustHandle(t, target, "ping", func(context.Context, []any) (any, error) {
		mu.Lock()
		dispatched = true
		mu.Unlock()
		return "ok", nil
	})
	mustHandle(t, critical, "guarded", func(ctx context.Context, _ []any) (any, error) {
		return nil, NoYield(ctx, func(ctx context.Context) error {
			_, err := Call(ctx, targetRef, "ping")
			return err
		})
	})
	startTestService(t, critical)
	startTestService(t, target)

	_, err = callWithTimeout(t, context.Background(), criticalRef, "guarded")
	if !errors.Is(err, ErrYieldForbidden) {
		t.Errorf("NoYield-wrapped Call inside NoInterleave = %v, want ErrYieldForbidden", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if dispatched {
		t.Error("target handler ran; NoYield must reject the Call before dispatch")
	}
}

// A NoInterleave handler owns its service until it returns, so a synchronous
// call back into that same service can never be granted a turn. Call rejects it
// with ErrCallCycle before enqueueing rather than waiting out CallTimeout.
func TestNoInterleaveSelfCallFailsFastInsteadOfTimingOut(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	const callTimeout = 400 * time.Millisecond
	service, ref, err := system.Reserve("no-interleave", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  callTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}

	var selfRef Ref
	mustHandle(t, service, "leaf", func(context.Context, []any) (any, error) { return "ok", nil })
	mustHandle(t, service, "reenter", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, selfRef, "leaf")
	})
	startTestService(t, service)
	selfRef = ref

	started := time.Now()
	_, err = callWithTimeout(t, context.Background(), ref, "reenter")
	elapsed := time.Since(started)

	if err == nil {
		t.Fatal("re-entrant NoInterleave call succeeded, want a cycle error")
	}
	if !errors.Is(err, ErrCallCycle) {
		t.Errorf("re-entrant NoInterleave call failed with %v, want ErrCallCycle", err)
	}
	if errors.Is(err, ErrCallTimeout) {
		t.Errorf("re-entrant NoInterleave call failed with %v after %v; "+
			"want a wait-cycle error raised before the call timeout", err, elapsed)
	}
	if elapsed >= callTimeout {
		t.Errorf("re-entrant NoInterleave call took %v (>= CallTimeout %v); "+
			"the cycle is detectable at Call time and must not wait for the timeout",
			elapsed, callTimeout)
	}
}

// ---------------------------------------------------------------------------
// Part B — regression coverage for resolved call-cycle gaps
// ---------------------------------------------------------------------------

// Cross-service cycles carry a call path and fail before either service burns
// its CallTimeout.
func TestNoInterleaveCrossServiceCycleFailsFast(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	const callTimeout = 400 * time.Millisecond
	options := ServiceOptions{NoInterleave: true, CallTimeout: callTimeout}
	alpha, alphaRef, err := system.Reserve("cycle-alpha", options)
	if err != nil {
		t.Fatal(err)
	}
	beta, betaRef, err := system.Reserve("cycle-beta", options)
	if err != nil {
		t.Fatal(err)
	}

	mustHandle(t, alpha, "entry", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, betaRef, "back")
	})
	mustHandle(t, alpha, "leaf", func(context.Context, []any) (any, error) { return "ok", nil })
	mustHandle(t, beta, "back", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, alphaRef, "leaf")
	})
	startTestService(t, alpha)
	startTestService(t, beta)

	started := time.Now()
	_, err = callWithTimeout(t, context.Background(), alphaRef, "entry")
	elapsed := time.Since(started)

	if !errors.Is(err, ErrCallCycle) {
		t.Errorf("cycle alpha→beta→alpha failed with %v after %v, want ErrCallCycle",
			err, elapsed)
	}
	if elapsed >= callTimeout {
		t.Errorf("cycle alpha→beta→alpha took %v (>= CallTimeout %v); the cycle is "+
			"knowable at Call time and must not wait for the timeout", elapsed, callTimeout)
	}
}

// An interleaving service in the middle must preserve the path so a call back
// into the NoInterleave head is rejected as a cycle.
func TestNoInterleaveCycleThroughInterleavingServiceFailsFast(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	const callTimeout = 400 * time.Millisecond
	head, headRef, err := system.Reserve("cycle-head", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  callTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	middle, middleRef, err := system.Reserve("cycle-middle", ServiceOptions{
		CallTimeout: callTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}

	mustHandle(t, head, "entry", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, middleRef, "back")
	})
	mustHandle(t, head, "leaf", func(context.Context, []any) (any, error) { return "ok", nil })
	mustHandle(t, middle, "back", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, headRef, "leaf")
	})
	startTestService(t, head)
	startTestService(t, middle)

	started := time.Now()
	_, err = callWithTimeout(t, context.Background(), headRef, "entry")
	elapsed := time.Since(started)

	if !errors.Is(err, ErrCallCycle) {
		t.Errorf("cycle head→middle→head failed with %v after %v, want ErrCallCycle",
			err, elapsed)
	}
	if elapsed >= callTimeout {
		t.Errorf("cycle head→middle→head took %v (>= CallTimeout %v); the cycle is "+
			"knowable at Call time and must not wait for the timeout", elapsed, callTimeout)
	}
}

// Cycle errors name the full service chain that closes the loop.
func TestCallCycleErrorNamesTheFullChain(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	const callTimeout = 400 * time.Millisecond
	options := ServiceOptions{NoInterleave: true, CallTimeout: callTimeout}
	alpha, alphaRef, err := system.Reserve("chain-alpha", options)
	if err != nil {
		t.Fatal(err)
	}
	beta, betaRef, err := system.Reserve("chain-beta", options)
	if err != nil {
		t.Fatal(err)
	}

	mustHandle(t, alpha, "entry", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, betaRef, "back")
	})
	mustHandle(t, alpha, "leaf", func(context.Context, []any) (any, error) { return "ok", nil })
	mustHandle(t, beta, "back", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, alphaRef, "leaf")
	})
	startTestService(t, alpha)
	startTestService(t, beta)

	_, err = callWithTimeout(t, context.Background(), alphaRef, "entry")
	if err == nil {
		t.Fatal("cycle chain-alpha→chain-beta→chain-alpha succeeded, want a cycle error")
	}
	for _, name := range []string{"chain-alpha", "chain-beta"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("cycle error %q does not name %q; the message must carry the "+
				"whole chain so the loop is diagnosable without re-deriving it", err, name)
		}
	}
}

// A yield event addressed to a NoInterleave service is an internal invariant
// failure, distinct from a user-facing NoYield violation.
func TestNoInterleaveYieldEventReportsInvariantNotYieldForbidden(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	service, ref, err := system.Reserve("no-interleave", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	mustHandle(t, service, "emit-yield", func(ctx context.Context, _ []any) (any, error) {
		act := activationFromContext(ctx)
		if act == nil {
			return nil, errors.New("handler ran without an activation")
		}
		accepted := make(chan error, 1)
		if err := act.runtime.emit(runtimeEvent{
			kind:       eventYield,
			activation: act,
			label:      "direct-yield",
			accepted:   accepted,
		}); err != nil {
			return nil, err
		}
		return nil, <-accepted
	})
	startTestService(t, service)

	_, err = callWithTimeout(t, context.Background(), ref, "emit-yield")
	if err == nil {
		t.Fatal("yield event on a NoInterleave runtime was accepted, want a rejection")
	}
	if errors.Is(err, ErrYieldForbidden) {
		t.Errorf("yield event on a NoInterleave runtime reported %v; want a distinct "+
			"internal-invariant error, not the user critical-section error", err)
	}
}
