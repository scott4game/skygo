package actor

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Coverage map for system_runtime.go relative to system_test.go:
//
// Already covered indirectly:
//   - eventYield / eventResume / awaitActivationTyped (self-call, A↔B cycle)
//   - MarkTurn / InterleavedSince / interleaveVersion
//   - NoYield → ErrYieldForbidden
//   - callMailbox session timeout + late unknown response
//
// Gaps filled here:
//   - goroutine / slot / suspend-limit resource leaks
//   - timeout-based deadlock probes (self-call, cycle, Stop while suspended)
//   - Await, MaxSuspended, ErrMailboxTimeout, panic recover, Stop enqueue, ctx cancel

const (
	runtimeDeadlockTimeout = 2 * time.Second
	runtimeLeakTolerance   = 10
)

func mustHandle(t testing.TB, service *Service, protocol string, fn ProtocolHandler) {
	t.Helper()
	if err := service.Handle(protocol, HandlerOptions{Codec: immutableCodec}, fn); err != nil {
		t.Fatal(err)
	}
}

func callWithTimeout(t *testing.T, ctx context.Context, ref Ref, protocol string, args ...any) (any, error) {
	t.Helper()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		value any
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := Call(callCtx, ref, protocol, args...)
		done <- outcome{value: value, err: err}
	}()
	select {
	case got := <-done:
		return got.value, got.err
	case <-time.After(runtimeDeadlockTimeout):
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
		t.Fatalf("deadlock suspected: Call %d.%s did not return within %s", ref.Address, protocol, runtimeDeadlockTimeout)
		return nil, nil
	}
}

func waitGoroutineDelta(t *testing.T, baseline int, tolerance int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after-baseline <= tolerance {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("possible goroutine leak: baseline=%d after=%d delta=%d tolerance=%d",
		baseline, after, after-baseline, tolerance)
}

func TestRuntimeNoGoroutineLeakAfterYieldResumeStop(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 15; i++ {
		system := NewSystem(SystemOptions{})
		a, aRef := newTestService(t, system, "a")
		b, bRef := newTestService(t, system, "b")
		mustHandle(t, b, "echo", func(context.Context, []any) (any, error) { return "ok", nil })
		mustHandle(t, a, "via-b", func(ctx context.Context, _ []any) (any, error) {
			return Call(ctx, bRef, "echo")
		})
		startTestService(t, a)
		startTestService(t, b)

		got, err := Call(context.Background(), aRef, "via-b")
		if err != nil || got != "ok" {
			t.Fatalf("via-b = (%v, %v), want (ok, nil)", got, err)
		}
		stopTestSystem(t, system)
	}

	time.Sleep(50 * time.Millisecond)
	waitGoroutineDelta(t, baseline, runtimeLeakTolerance)
}

func TestRuntimeMailboxSlotsReleasedAfterStopWhileBusy(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	release := make(chan struct{})
	entered := make(chan struct{})
	service, ref, err := system.Reserve("busy", ServiceOptions{
		MailboxSize:      2,
		AdmissionTimeout: 50 * time.Millisecond,
		CallTimeout:      time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "block", func(ctx context.Context, _ []any) (any, error) {
		close(entered)
		select {
		case <-release:
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	mustHandle(t, service, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	startTestService(t, service)

	type outcome struct{ err error }
	firstDone := make(chan outcome, 1)
	go func() {
		_, err := Call(context.Background(), ref, "block")
		firstDone <- outcome{err: err}
	}()
	select {
	case <-entered:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("busy handler did not start")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatalf("Stop busy service: %v", err)
	}
	select {
	case got := <-firstDone:
		if got.err == nil {
			t.Fatal("blocked Call succeeded after Stop; want error")
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: blocked Call did not unblock on Stop")
	}
	close(release)

	replacement, newRef, err := system.Reserve("busy", ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, replacement, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	startTestService(t, replacement)

	got, err := callWithTimeout(t, context.Background(), newRef, "ping")
	if err != nil || got != "pong" {
		t.Fatalf("replacement ping = (%v, %v), want (pong, nil)", got, err)
	}
}

func TestRuntimeSuspendLimitDoesNotLeakActivation(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	system := NewSystem(SystemOptions{})
	a, aRef, err := system.Reserve("a", ServiceOptions{
		MaxSuspended: 1,
		CallTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, bRef, err := system.Reserve("b", ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	var active atomic.Int32
	block := make(chan struct{})
	mustHandle(t, b, "wait", func(ctx context.Context, _ []any) (any, error) {
		active.Add(1)
		select {
		case <-block:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	mustHandle(t, a, "hold", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "wait")
	})
	startTestService(t, a)
	startTestService(t, b)

	type outcome struct{ err error }
	first := make(chan outcome, 1)
	second := make(chan outcome, 1)
	go func() {
		_, err := Call(context.Background(), aRef, "hold")
		first <- outcome{err: err}
	}()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for active.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if active.Load() < 1 {
		t.Fatal("first hold did not suspend into B")
	}

	go func() {
		_, err := Call(context.Background(), aRef, "hold")
		second <- outcome{err: err}
	}()
	select {
	case got := <-second:
		if !errors.Is(got.err, ErrSuspendLimit) {
			t.Fatalf("second hold error = %v, want ErrSuspendLimit", got.err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: second hold did not return ErrSuspendLimit")
	}

	close(block)
	select {
	case got := <-first:
		if got.err != nil {
			t.Fatalf("first hold error = %v, want nil", got.err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: first hold did not resume")
	}

	stopTestSystem(t, system)
	time.Sleep(50 * time.Millisecond)
	waitGoroutineDelta(t, baseline, runtimeLeakTolerance)
}

func TestRuntimeSelfCallDoesNotDeadlock(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref := newTestService(t, system, "self")
	mustHandle(t, service, "inner", func(context.Context, []any) (any, error) { return 7, nil })
	mustHandle(t, service, "outer", func(ctx context.Context, _ []any) (any, error) {
		value, err := Call(ctx, ref, "inner")
		if err != nil {
			return nil, err
		}
		return value.(int) * 3, nil
	})
	startTestService(t, service)

	got, err := callWithTimeout(t, context.Background(), ref, "outer")
	if err != nil || got != 21 {
		t.Fatalf("outer = (%v, %v), want (21, nil)", got, err)
	}
}

func TestRuntimeCrossServiceCycleDoesNotDeadlock(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")
	mustHandle(t, a, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	mustHandle(t, a, "start", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "bounce")
	})
	mustHandle(t, b, "bounce", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, aRef, "ping")
	})
	startTestService(t, a)
	startTestService(t, b)

	got, err := callWithTimeout(t, context.Background(), aRef, "start")
	if err != nil || got != "pong" {
		t.Fatalf("cycle = (%v, %v), want (pong, nil)", got, err)
	}
}

func TestRuntimeStopUnblocksSuspendedCall(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	a, aRef, err := system.Reserve("a", ServiceOptions{CallTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	b, bRef, err := system.Reserve("b", ServiceOptions{CallTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	bEntered := make(chan struct{})
	mustHandle(t, b, "wait", func(ctx context.Context, _ []any) (any, error) {
		close(bEntered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	mustHandle(t, a, "outer", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "wait")
	})
	startTestService(t, a)
	startTestService(t, b)

	type outcome struct{ err error }
	done := make(chan outcome, 1)
	go func() {
		_, err := Call(context.Background(), aRef, "outer")
		done <- outcome{err: err}
	}()
	select {
	case <-bEntered:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("B.wait was not entered")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := a.Stop(stopCtx); err != nil {
		t.Fatalf("Stop A: %v", err)
	}
	if err := b.Stop(stopCtx); err != nil {
		t.Fatalf("Stop B: %v", err)
	}

	select {
	case got := <-done:
		if got.err == nil {
			t.Fatal("suspended Call returned nil error after Stop")
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: suspended Call did not unblock after Stop")
	}
}

func TestRuntimeNoYieldThenOuterCallStillProgresses(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")
	mustHandle(t, b, "target", func(context.Context, []any) (any, error) { return "hit", nil })
	mustHandle(t, a, "critical", func(ctx context.Context, _ []any) (any, error) {
		return nil, NoYield(ctx, func(ctx context.Context) error {
			_, err := Call(ctx, bRef, "target")
			return err
		})
	})
	mustHandle(t, a, "ok", func(context.Context, []any) (any, error) { return "ready", nil })
	startTestService(t, a)
	startTestService(t, b)

	if _, err := callWithTimeout(t, context.Background(), aRef, "critical"); !errors.Is(err, ErrYieldForbidden) {
		t.Fatalf("critical error = %v, want ErrYieldForbidden", err)
	}
	got, err := callWithTimeout(t, context.Background(), aRef, "ok")
	if err != nil || got != "ready" {
		t.Fatalf("follow-up Call = (%v, %v), want (ready, nil)", got, err)
	}
}

func TestAwaitYieldsOutsideCallPath(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref := newTestService(t, system, "await")
	gate := make(chan struct{})
	release := make(chan struct{})
	interleaved := make(chan struct{})

	mustHandle(t, service, "wait", func(ctx context.Context, _ []any) (any, error) {
		mark := MarkTurn(ctx)
		close(gate)
		_, err := Await(ctx, "external-wait", func(context.Context) (struct{}, error) {
			<-release
			return struct{}{}, nil
		})
		if err != nil {
			return nil, err
		}
		return InterleavedSince(ctx, mark), nil
	})
	mustHandle(t, service, "poke", func(context.Context, []any) (any, error) {
		close(interleaved)
		return nil, nil
	})
	startTestService(t, service)

	type outcome struct {
		value any
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		value, err := Call(context.Background(), ref, "wait")
		done <- outcome{value: value, err: err}
	}()
	select {
	case <-gate:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("wait handler did not yield")
	}
	if _, err := callWithTimeout(t, context.Background(), ref, "poke"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-interleaved:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("poke did not run during Await")
	}
	close(release)

	select {
	case got := <-done:
		if got.err != nil || got.value != true {
			t.Fatalf("Await interleave = (%v, %v), want (true, nil)", got.value, got.err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: Await did not resume")
	}
}

func TestAwaitNilFunc(t *testing.T) {
	_, err := Await[any](context.Background(), "nil", nil)
	if !errors.Is(err, ErrInvalidArgs) {
		t.Fatalf("Await nil error = %v, want ErrInvalidArgs", err)
	}
}

func TestMaxSuspendedReturnsErrSuspendLimit(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	a, aRef, err := system.Reserve("a", ServiceOptions{
		MaxSuspended: 1,
		CallTimeout:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, bRef, err := system.Reserve("b", ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var waiting atomic.Int32
	mustHandle(t, b, "wait", func(ctx context.Context, _ []any) (any, error) {
		waiting.Add(1)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	mustHandle(t, a, "hold", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "wait")
	})
	startTestService(t, a)
	startTestService(t, b)

	firstDone := make(chan error, 1)
	go func() {
		_, err := Call(context.Background(), aRef, "hold")
		firstDone <- err
	}()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for waiting.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if waiting.Load() < 1 {
		t.Fatal("first activation did not suspend")
	}

	_, err = callWithTimeout(t, context.Background(), aRef, "hold")
	if !errors.Is(err, ErrSuspendLimit) {
		t.Fatalf("second hold error = %v, want ErrSuspendLimit", err)
	}
	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first hold error = %v", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("first hold did not finish")
	}
}

func TestMailboxAdmissionTimeout(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	release := make(chan struct{})
	var started atomic.Int32
	service, ref, err := system.Reserve("admit", ServiceOptions{
		MailboxSize:      1,
		AdmissionTimeout: 30 * time.Millisecond,
		CallTimeout:      time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "slow", func(ctx context.Context, _ []any) (any, error) {
		started.Add(1)
		select {
		case <-release:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	startTestService(t, service)

	firstDone := make(chan error, 1)
	go func() {
		_, err := Call(context.Background(), ref, "slow")
		firstDone <- err
	}()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for started.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if started.Load() < 1 {
		t.Fatal("slow handler did not start")
	}

	// Fill the single admission slot with a queued Call while slow is current.
	queued := make(chan error, 1)
	go func() {
		_, err := Call(context.Background(), ref, "slow")
		queued <- err
	}()
	time.Sleep(20 * time.Millisecond)

	_, err = Call(context.Background(), ref, "slow")
	if !errors.Is(err, ErrMailboxTimeout) {
		t.Fatalf("third Call error = %v, want ErrMailboxTimeout", err)
	}

	close(release)
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("first Call error = %v", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("first Call did not finish")
	}
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("queued Call error = %v", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("queued Call did not finish")
	}
}

func TestHandlerPanicRecoveredOnMailboxCall(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref := newTestService(t, system, "panic")
	mustHandle(t, service, "boom", func(context.Context, []any) (any, error) {
		panic("kaboom")
	})
	mustHandle(t, service, "ok", func(context.Context, []any) (any, error) { return "fine", nil })
	startTestService(t, service)

	_, err := Call(context.Background(), ref, "boom")
	if err == nil || !strings.Contains(err.Error(), "handler panic") {
		t.Fatalf("boom error = %v, want handler panic", err)
	}
	got, err := callWithTimeout(t, context.Background(), ref, "ok")
	if err != nil || got != "fine" {
		t.Fatalf("recovery Call = (%v, %v), want (fine, nil)", got, err)
	}
}

func TestStopRejectsNewEnqueue(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref := newTestService(t, system, "stop")
	mustHandle(t, service, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	startTestService(t, service)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}

	_, err := Call(context.Background(), ref, "ping")
	if !errors.Is(err, ErrStaleRef) && !errors.Is(err, ErrServiceStopping) {
		t.Fatalf("Call after Stop error = %v, want ErrStaleRef or ErrServiceStopping", err)
	}
}

func TestContextCancelDuringCall(t *testing.T) {
	unknown := make(chan uint64, 1)
	system := NewSystem(SystemOptions{UnknownResponse: func(session uint64) { unknown <- session }})
	defer stopTestSystem(t, system)

	release := make(chan struct{})
	var started atomic.Int32
	service, ref, err := system.Reserve("cancel", ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "wait", func(ctx context.Context, _ []any) (any, error) {
		started.Add(1)
		select {
		case <-release:
			return "late", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	startTestService(t, service)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Call(ctx, ref, "wait")
		done <- err
	}()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for started.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if started.Load() < 1 {
		t.Fatal("wait handler did not start")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Call error = %v, want context.Canceled", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: Call did not observe ctx cancel")
	}

	close(release)
	select {
	case <-unknown:
		if stats := system.Stats(); stats.UnknownResponses != 1 {
			t.Fatalf("stats after late response = %#v", stats)
		}
	case <-time.After(time.Second):
		t.Fatal("late response after cancel was not observed as unknown")
	}
}

func TestContextCancelDuringCallDoesNotWedgeService(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	var mu sync.Mutex
	started := 0
	release := make(chan struct{})
	service, ref, err := system.Reserve("wedge", ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "block", func(ctx context.Context, _ []any) (any, error) {
		mu.Lock()
		started++
		n := started
		mu.Unlock()
		if n == 1 {
			select {
			case <-release:
				return "first", nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return "second", nil
	})
	startTestService(t, service)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := Call(ctx, ref, "block")
		done <- err
	}()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for {
		mu.Lock()
		n := started
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("first block did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Call error = %v, want context.Canceled", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("deadlock suspected: canceled Call hung")
	}
	close(release)

	got, err := callWithTimeout(t, context.Background(), ref, "block")
	if err != nil || got != "second" {
		t.Fatalf("service wedged after cancel: got (%v, %v), want (second, nil)", got, err)
	}
}

// A handler owns its service state. Abandoning the response must never abandon
// the mutation half-done, so caller cancellation and caller call-timeout are
// both decoupled from the handler context. Only stopping the service cancels it.

type callerCtxKey struct{}

// awaitStarted spins until started reaches at least one, mirroring the polling
// used by the other cancellation tests.
func awaitStarted(t *testing.T, started *atomic.Int32) {
	t.Helper()
	deadline := time.Now().Add(runtimeDeadlockTimeout)
	for started.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if started.Load() < 1 {
		t.Fatal("handler did not start")
	}
}

func TestCallerCancelDoesNotCancelHandler(t *testing.T) {
	unknown := make(chan uint64, 1)
	system := NewSystem(SystemOptions{UnknownResponse: func(session uint64) { unknown <- session }})
	defer stopTestSystem(t, system)

	release := make(chan struct{})
	var started atomic.Int32
	var sawCancel atomic.Bool
	var completed atomic.Bool

	service, ref, err := system.Reserve("detach-cancel", ServiceOptions{CallTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "wait", func(ctx context.Context, _ []any) (any, error) {
		started.Add(1)
		<-release
		sawCancel.Store(ctx.Err() != nil)
		completed.Store(true)
		return "late", nil
	})
	startTestService(t, service)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, callErr := Call(ctx, ref, "wait")
		done <- callErr
	}()
	awaitStarted(t, &started)
	cancel()

	select {
	case callErr := <-done:
		if !errors.Is(callErr, context.Canceled) {
			t.Fatalf("Call error = %v, want context.Canceled", callErr)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("Call did not observe caller cancel")
	}

	// The handler must still be running: caller cancel abandons the response only.
	if completed.Load() {
		t.Fatal("handler completed before release: caller cancel propagated into the handler")
	}
	close(release)

	select {
	case <-unknown:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("late response after cancel was not observed as unknown")
	}
	if sawCancel.Load() {
		t.Fatal("handler context was cancelled by caller cancel, want handler context still live")
	}
	if !completed.Load() {
		t.Fatal("handler did not run to completion")
	}
}

func TestCallTimeoutDoesNotCancelHandler(t *testing.T) {
	unknown := make(chan uint64, 1)
	system := NewSystem(SystemOptions{UnknownResponse: func(session uint64) { unknown <- session }})
	defer stopTestSystem(t, system)

	release := make(chan struct{})
	var started atomic.Int32
	var sawCancel atomic.Bool
	var completed atomic.Bool

	service, ref, err := system.Reserve("detach-timeout", ServiceOptions{CallTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, service, "wait", func(ctx context.Context, _ []any) (any, error) {
		started.Add(1)
		<-release
		sawCancel.Store(ctx.Err() != nil)
		completed.Store(true)
		return "late", nil
	})
	startTestService(t, service)

	done := make(chan error, 1)
	go func() {
		_, callErr := Call(context.Background(), ref, "wait")
		done <- callErr
	}()
	awaitStarted(t, &started)

	select {
	case callErr := <-done:
		if !errors.Is(callErr, ErrCallTimeout) {
			t.Fatalf("Call error = %v, want ErrCallTimeout", callErr)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("Call did not time out")
	}

	if completed.Load() {
		t.Fatal("handler completed before release: call timeout propagated into the handler")
	}
	close(release)

	select {
	case <-unknown:
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("late response after call timeout was not observed as unknown")
	}
	if sawCancel.Load() {
		t.Fatal("handler context was cancelled by call timeout, want handler context still live")
	}
}

func TestServiceStopCancelsHandlerContext(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	service, ref := newTestService(t, system, "stop-cancels")
	var started atomic.Int32
	handlerCtxErr := make(chan error, 1)
	mustHandle(t, service, "block", func(ctx context.Context, _ []any) (any, error) {
		started.Add(1)
		<-ctx.Done()
		handlerCtxErr <- ctx.Err()
		return nil, ctx.Err()
	})
	startTestService(t, service)

	go func() { _, _ = Call(context.Background(), ref, "block") }()
	awaitStarted(t, &started)

	stopCtx, cancel := context.WithTimeout(context.Background(), runtimeDeadlockTimeout)
	defer cancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatalf("Stop error = %v", err)
	}

	select {
	case err := <-handlerCtxErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(runtimeDeadlockTimeout):
		t.Fatal("service stop did not cancel the handler context")
	}
}

// interleaveProbe builds service "a" (under opts) with an "outer" protocol that
// calls into a second service and blocks there, plus a "probe" protocol that only
// records that it ran. It reports whether probe overtook outer while outer was
// parked in the cross-service call.
func interleaveProbe(t *testing.T, opts ServiceOptions) (probeRanDuringCall bool, order []string) {
	t.Helper()
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	var mu sync.Mutex
	record := func(label string) {
		mu.Lock()
		order = append(order, label)
		mu.Unlock()
	}

	a, aRef, err := system.Reserve("a", opts)
	if err != nil {
		t.Fatal(err)
	}
	b, bRef := newTestService(t, system, "b")

	release := make(chan struct{})
	entered := make(chan struct{})
	probed := make(chan struct{})

	mustHandle(t, b, "slow", func(context.Context, []any) (any, error) {
		close(entered)
		<-release
		return "slow-done", nil
	})
	mustHandle(t, a, "outer", func(ctx context.Context, _ []any) (any, error) {
		value, callErr := Call(ctx, bRef, "slow")
		record("outer")
		return value, callErr
	})
	mustHandle(t, a, "probe", func(context.Context, []any) (any, error) {
		record("probe")
		close(probed)
		return "probe-done", nil
	})
	startTestService(t, a)
	startTestService(t, b)

	outerDone := make(chan error, 1)
	go func() {
		_, callErr := Call(context.Background(), aRef, "outer")
		outerDone <- callErr
	}()
	<-entered

	probeDone := make(chan error, 1)
	go func() {
		_, callErr := Call(context.Background(), aRef, "probe")
		probeDone <- callErr
	}()

	// Give probe a real chance to be scheduled while outer is parked in the call.
	select {
	case <-probed:
		probeRanDuringCall = true
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	if callErr := <-outerDone; callErr != nil {
		t.Fatalf("outer error = %v", callErr)
	}
	if callErr := <-probeDone; callErr != nil {
		t.Fatalf("probe error = %v", callErr)
	}
	mu.Lock()
	defer mu.Unlock()
	return probeRanDuringCall, append([]string(nil), order...)
}

func TestDefaultServiceInterleavesDuringCrossServiceCall(t *testing.T) {
	ran, order := interleaveProbe(t, ServiceOptions{})
	if !ran {
		t.Fatal("probe did not run during the cross-service call, want cooperative yield by default")
	}
	if len(order) != 2 || order[0] != "probe" || order[1] != "outer" {
		t.Fatalf("order = %v, want [probe outer]", order)
	}
}

func TestNoInterleaveKeepsHandlerAtomicAcrossCrossServiceCall(t *testing.T) {
	ran, order := interleaveProbe(t, ServiceOptions{NoInterleave: true})
	if ran {
		t.Fatal("probe ran during the cross-service call, want the handler to stay atomic")
	}
	if len(order) != 2 || order[0] != "outer" || order[1] != "probe" {
		t.Fatalf("order = %v, want [outer probe]", order)
	}
}

func TestNoInterleaveReportsNoInterleaving(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	a, aRef, err := system.Reserve("marker", ServiceOptions{NoInterleave: true})
	if err != nil {
		t.Fatal(err)
	}
	b, bRef := newTestService(t, system, "peer")
	mustHandle(t, b, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	mustHandle(t, a, "outer", func(ctx context.Context, _ []any) (any, error) {
		mark := MarkTurn(ctx)
		if _, callErr := Call(ctx, bRef, "ping"); callErr != nil {
			return nil, callErr
		}
		return InterleavedSince(ctx, mark), nil
	})
	startTestService(t, a)
	startTestService(t, b)

	got, err := callWithTimeout(t, context.Background(), aRef, "outer")
	if err != nil {
		t.Fatal(err)
	}
	if got != false {
		t.Fatalf("InterleavedSince = %v, want false under NoInterleave", got)
	}
}

func TestHandlerContextKeepsCallerValues(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)

	service, ref := newTestService(t, system, "ctx-values")
	mustHandle(t, service, "read", func(ctx context.Context, _ []any) (any, error) {
		value, _ := ctx.Value(callerCtxKey{}).(string)
		return value, nil
	})
	startTestService(t, service)

	ctx := context.WithValue(context.Background(), callerCtxKey{}, "trace-42")
	got, err := callWithTimeout(t, ctx, ref, "read")
	if err != nil {
		t.Fatal(err)
	}
	if got != "trace-42" {
		t.Fatalf("handler ctx value = %v, want trace-42 (caller values must survive)", got)
	}
}
