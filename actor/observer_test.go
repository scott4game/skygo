package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type observerFunc func(CallEvent)

func (f observerFunc) OnCall(event CallEvent) { f(event) }

func TestObserverReceivesExternalNestedAndErrorCalls(t *testing.T) {
	var mu sync.Mutex
	var events []CallEvent
	system := NewSystem(SystemOptions{Observer: observerFunc(func(event CallEvent) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	})})
	defer stopTestSystem(t, system)
	a, aRef := newTestService(t, system, "a")
	b, bRef := newTestService(t, system, "b")
	if err := b.Handle("fail", HandlerOptions{Codec: immutableCodec}, func(context.Context, []any) (any, error) {
		return nil, errors.New("boom")
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.Handle("nested", HandlerOptions{Codec: immutableCodec}, func(ctx context.Context, _ []any) (any, error) {
		return Call(ctx, bRef, "fail")
	}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, a)
	startTestService(t, b)
	if _, err := Call(context.Background(), aRef, "nested"); err == nil {
		t.Fatal("Call succeeded, want handler error")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("events = %#v, want 2", events)
	}
	if events[0].Caller != "a" || events[0].Callee != "b" || events[0].Protocol != "fail" || events[0].Err == nil {
		t.Fatalf("nested event = %#v", events[0])
	}
	if events[1].Caller != "<external>" || events[1].Callee != "a" || events[1].Protocol != "nested" || events[1].Err == nil {
		t.Fatalf("external event = %#v", events[1])
	}
	if events[0].Duration < 0 || events[1].Duration < events[0].Duration {
		t.Fatalf("invalid durations: %#v", events)
	}
}

func TestObserverPanicDoesNotChangeCallResult(t *testing.T) {
	system := NewSystem(SystemOptions{Observer: observerFunc(func(CallEvent) { panic("observer") })})
	service, ref := newTestService(t, system, "echo")
	if err := service.Handle("ping", HandlerOptions{Codec: immutableCodec}, func(context.Context, []any) (any, error) {
		return "pong", nil
	}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)
	defer stopTestSystem(t, system)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := Call(ctx, ref, "ping")
	if err != nil || value != "pong" {
		t.Fatalf("Call = (%v, %v), want (pong, nil)", value, err)
	}
}

func TestObserverReceivesCallTimeout(t *testing.T) {
	events := make(chan CallEvent, 1)
	system := NewSystem(SystemOptions{Observer: observerFunc(func(event CallEvent) {
		events <- event
	})})
	service, ref, err := system.Reserve("slow", ServiceOptions{CallTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	if err := service.Handle("wait", HandlerOptions{Codec: immutableCodec}, func(context.Context, []any) (any, error) {
		<-release
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)
	if _, err := Call(context.Background(), ref, "wait"); !errors.Is(err, ErrCallTimeout) {
		t.Fatalf("Call error = %v, want ErrCallTimeout", err)
	}
	select {
	case event := <-events:
		if event.Callee != "slow" || event.Protocol != "wait" || !errors.Is(event.Err, ErrCallTimeout) {
			t.Fatalf("event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for observer event")
	}
	close(release)
	defer stopTestSystem(t, system)
}
