package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTypedMethodRejectsSameNameDifferentDescriptor(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("typed", ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	canonical := NewMethod[int, int]("value")
	if err := Register(service, canonical, func(_ context.Context, value int) (int, error) { return value, nil }); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer service.Stop(context.Background())

	forged := NewMethod[int, int]("value")
	if _, err := forged.Call(context.Background(), ref, 1); !errors.Is(err, ErrProtocolTypeMismatch) {
		t.Fatalf("forged descriptor error = %v, want ErrProtocolTypeMismatch", err)
	}
}

func TestRegisterRejectsMutableTypeWithoutClone(t *testing.T) {
	type mutableRequest struct{ Values []int }
	system := NewSystem(SystemOptions{})
	service, _, err := system.Reserve("typed", ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	method := NewMethod[mutableRequest, struct{}]("mutable")
	if err := Register(service, method, func(context.Context, mutableRequest) (struct{}, error) { return struct{}{}, nil }); !errors.Is(err, ErrCodec) {
		t.Fatalf("registration error = %v, want ErrCodec", err)
	}
}

func TestNotificationReportsAsyncErrorAndContinues(t *testing.T) {
	asyncErrors := make(chan AsyncError, 1)
	system := NewSystem(SystemOptions{AsyncError: func(failure AsyncError) { asyncErrors <- failure }})
	service, ref, err := system.Reserve("notify", ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	notification := NewNotification[int]("event")
	if err := RegisterNotification(service, notification, func(_ context.Context, value int) error {
		if value == 1 {
			panic("boom")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer service.Stop(context.Background())

	if err := notification.Send(context.Background(), ref, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case failure := <-asyncErrors:
		if failure.Service != "notify" || failure.Protocol != "event" || failure.Err == nil {
			t.Fatalf("unexpected async failure: %+v", failure)
		}
	case <-time.After(time.Second):
		t.Fatal("notification panic was not reported")
	}
	if err := notification.Send(context.Background(), ref, 2); err != nil {
		t.Fatalf("notification did not continue after panic: %v", err)
	}
	if got := system.Stats().AsyncFailures; got != 1 {
		t.Fatalf("async failures = %d, want 1", got)
	}
}
