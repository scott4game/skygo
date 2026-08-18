package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func cloneArgs(args []any) ([]any, error) {
	cloned := make([]any, len(args))
	copy(cloned, args)
	return cloned, nil
}

var immutableCodec = CloneCodec(cloneArgs, func(value any) (any, error) { return value, nil })

func newTestService(t testing.TB, system *System, name string) (*Service, Ref) {
	t.Helper()
	service, ref, err := system.Reserve(name, ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatalf("Reserve(%q): %v", name, err)
	}
	return service, ref
}

func startTestService(t testing.TB, service *Service) {
	t.Helper()
	if err := service.Start(context.Background()); err != nil {
		t.Fatalf("Start(%q): %v", service.Name(), err)
	}
}

func stopTestSystem(t testing.TB, system *System) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := system.Stop(ctx); err != nil {
		t.Errorf("Stop system: %v", err)
	}
}

func TestSystemServiceLifecycleAndStaleRef(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, firstRef := newTestService(t, system, "world")

	if _, err := system.Resolve("world"); !errors.Is(err, ErrServiceNotReady) {
		t.Fatalf("Resolve reserved service error = %v, want ErrServiceNotReady", err)
	}
	if _, _, err := system.Reserve("world", ServiceOptions{}); err == nil {
		t.Fatal("duplicate Reserve succeeded")
	}
	if err := service.Handle("ping", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) { return "pong", nil }); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)

	resolved, err := system.Resolve("world")
	if err != nil {
		t.Fatal(err)
	}
	if resolved != firstRef {
		t.Fatalf("Resolve ref = %#v, want %#v", resolved, firstRef)
	}
	if got, err := Call(context.Background(), firstRef, "ping"); err != nil || got != "pong" {
		t.Fatalf("Call ping = (%v, %v), want (pong, nil)", got, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := Call(context.Background(), firstRef, "ping"); !errors.Is(err, ErrStaleRef) {
		t.Fatalf("old ref Call error = %v, want ErrStaleRef", err)
	}

	replacement, secondRef := newTestService(t, system, "world")
	defer stopTestSystem(t, system)
	if secondRef.Generation != firstRef.Generation+1 {
		t.Fatalf("replacement generation = %d, want %d", secondRef.Generation, firstRef.Generation+1)
	}
	if secondRef.Address == firstRef.Address {
		t.Fatal("replacement reused an address")
	}
	startTestService(t, replacement)
}

func TestProtocolRegistrationRules(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	mailbox, mailboxRef := newTestService(t, system, "mailbox")

	if err := mailbox.Handle("ping", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) { return "pong", nil }); err != nil {
		t.Fatal(err)
	}
	if err := mailbox.Handle("ping", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) { return nil, nil }); !errors.Is(err, ErrProtocolExists) {
		t.Fatalf("duplicate Handle error = %v, want ErrProtocolExists", err)
	}
	startTestService(t, mailbox)

	if _, err := Call(context.Background(), mailboxRef, "missing"); !errors.Is(err, ErrProtocolNotFound) {
		t.Fatalf("unknown protocol error = %v, want ErrProtocolNotFound", err)
	}
	if err := mailbox.Handle("late", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) { return nil, nil }); err == nil {
		t.Fatal("Handle after Start succeeded")
	}
}

func TestMailboxSelfCallAndServiceCycleComplete(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")

	mustHandle := func(service *Service, protocol string, fn ProtocolHandler) {
		t.Helper()
		if err := service.Handle(protocol, HandlerOptions{Codec: immutableCodec}, fn); err != nil {
			t.Fatal(err)
		}
	}
	mustHandle(a, "inner", func(context.Context, []any) (any, error) { return 41, nil })
	mustHandle(a, "self", func(ctx context.Context, _ []any) (any, error) {
		value, err := Call(ctx, aRef, "inner")
		if err != nil {
			return nil, err
		}
		return value.(int) + 1, nil
	})
	mustHandle(a, "cycle-entry", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "back-to-a")
	})
	mustHandle(b, "back-to-a", func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, aRef, "inner")
	})
	startTestService(t, a)
	startTestService(t, b)

	for protocol, want := range map[string]int{"self": 42, "cycle-entry": 41} {
		got, err := Call(context.Background(), aRef, protocol)
		if err != nil || got != want {
			t.Fatalf("Call %s = (%v, %v), want (%d, nil)", protocol, got, err, want)
		}
	}
}

func TestMailboxCallYieldsAndDetectsInterleaving(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")
	bEntered := make(chan struct{})
	releaseB := make(chan struct{})

	if err := b.Handle("wait", HandlerOptions{Codec: immutableCodec},
		func(ctx context.Context, _ []any) (any, error) {
			close(bEntered)
			select {
			case <-releaseB:
				return nil, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}); err != nil {
		t.Fatal(err)
	}
	if err := a.Handle("outer", HandlerOptions{Codec: immutableCodec},
		func(ctx context.Context, _ []any) (any, error) {
			mark := MarkTurn(ctx)
			if _, err := Call(ctx, bRef, "wait"); err != nil {
				return nil, err
			}
			return InterleavedSince(ctx, mark), nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := a.Handle("interleave", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	startTestService(t, a)
	startTestService(t, b)

	type outcome struct {
		value any
		err   error
	}
	outerDone := make(chan outcome, 1)
	go func() {
		value, err := Call(context.Background(), aRef, "outer")
		outerDone <- outcome{value: value, err: err}
	}()
	select {
	case <-bEntered:
	case <-time.After(time.Second):
		t.Fatal("B was not called")
	}
	if _, err := Call(context.Background(), aRef, "interleave"); err != nil {
		t.Fatal(err)
	}
	close(releaseB)
	select {
	case got := <-outerDone:
		if got.err != nil || got.value != true {
			t.Fatalf("outer result = (%v, %v), want (true, nil)", got.value, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("outer call did not resume")
	}
}

func TestNoYieldRejectsCallBeforeDispatch(t *testing.T) {
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")
	var mu sync.Mutex
	dispatched := false

	if err := b.Handle("target", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) {
			mu.Lock()
			dispatched = true
			mu.Unlock()
			return nil, nil
		}); err != nil {
		t.Fatal(err)
	}
	if err := a.Handle("critical", HandlerOptions{Codec: immutableCodec},
		func(ctx context.Context, _ []any) (any, error) {
			err := NoYield(ctx, func(ctx context.Context) error {
				_, err := Call(ctx, bRef, "target")
				return err
			})
			return nil, err
		}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, a)
	startTestService(t, b)

	if _, err := Call(context.Background(), aRef, "critical"); !errors.Is(err, ErrYieldForbidden) {
		t.Fatalf("critical Call error = %v, want ErrYieldForbidden", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if dispatched {
		t.Fatal("target handler ran despite NoYield rejection")
	}
}

type mutableBox struct{ value int }

func TestCodecDetachesMutableRequestAndResponse(t *testing.T) {
	boxCodec := CloneCodec(
		func(args []any) ([]any, error) {
			if len(args) != 1 {
				return nil, fmt.Errorf("want one argument")
			}
			box, ok := args[0].(*mutableBox)
			if !ok {
				return nil, fmt.Errorf("want *mutableBox, got %T", args[0])
			}
			return []any{&mutableBox{value: box.value}}, nil
		},
		func(value any) (any, error) {
			box, ok := value.(*mutableBox)
			if !ok {
				return nil, fmt.Errorf("want *mutableBox, got %T", value)
			}
			return &mutableBox{value: box.value}, nil
		},
	)
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref := newTestService(t, system, "clone")
	var handlerResponse *mutableBox
	if err := service.Handle("mutate", HandlerOptions{Codec: boxCodec},
		func(_ context.Context, args []any) (any, error) {
			request := args[0].(*mutableBox)
			request.value++
			handlerResponse = request
			return request, nil
		}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)

	original := &mutableBox{value: 10}
	value, err := Call(context.Background(), ref, "mutate", original)
	if err != nil {
		t.Fatal(err)
	}
	response := value.(*mutableBox)
	if original.value != 10 {
		t.Fatalf("caller request was mutated to %d", original.value)
	}
	if response.value != 11 {
		t.Fatalf("response value = %d, want 11", response.value)
	}
	if response == original || response == handlerResponse || handlerResponse == original {
		t.Fatal("codec did not establish distinct ownership boundaries")
	}
}

func TestCallTimeoutWinsSessionOnceAndLateResponseIsObserved(t *testing.T) {
	unknown := make(chan uint64, 1)
	system := NewSystem(SystemOptions{UnknownResponse: func(session uint64) { unknown <- session }})
	defer stopTestSystem(t, system)
	service, ref, err := system.Reserve("slow", ServiceOptions{CallTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	if err := service.Handle("wait", HandlerOptions{Codec: immutableCodec},
		func(context.Context, []any) (any, error) {
			<-release
			return "late", nil
		}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)

	if _, err := Call(context.Background(), ref, "wait"); !errors.Is(err, ErrCallTimeout) {
		t.Fatalf("Call error = %v, want ErrCallTimeout", err)
	}
	if stats := system.Stats(); stats.TimedOutCalls != 1 || stats.UnknownResponses != 0 {
		t.Fatalf("stats after timeout = %#v", stats)
	}
	close(release)
	select {
	case session := <-unknown:
		if session == 0 {
			t.Fatal("unknown response had zero session")
		}
	case <-time.After(time.Second):
		t.Fatal("late response was not observed")
	}
	if stats := system.Stats(); stats.TimedOutCalls != 1 || stats.UnknownResponses != 1 {
		t.Fatalf("final stats = %#v", stats)
	}
}
