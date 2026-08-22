package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoInterleaveSelfCallFailsBeforeDispatch(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("nointerleave-self", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	nestedRan := make(chan struct{}, 1)
	if err := service.Handle("self", HandlerOptions{Codec: immutableCodec}, func(ctx context.Context, args []any) (any, error) {
		nested, _ := Arg[bool](args, 0)
		if nested {
			nestedRan <- struct{}{}
			return "nested", nil
		}
		_, err := Call(ctx, ref, "self", true)
		return nil, err
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Handle("barrier", HandlerOptions{Codec: immutableCodec}, func(context.Context, []any) (any, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)
	defer stopTestSystem(t, system)

	if _, err := Call(context.Background(), ref, "self", false); !errors.Is(err, ErrCallCycle) {
		t.Fatalf("self Call error = %v, want ErrCallCycle", err)
	}
	if _, err := Call(context.Background(), ref, "barrier"); err != nil {
		t.Fatalf("barrier Call error = %v", err)
	}
	select {
	case <-nestedRan:
		t.Fatal("nested self call ran; cycle must be rejected before dispatch")
	default:
	}
}
