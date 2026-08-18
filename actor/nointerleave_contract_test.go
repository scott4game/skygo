package actor

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNoInterleaveSelfCallTimesOut(t *testing.T) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("nointerleave-self", ServiceOptions{
		NoInterleave: true,
		CallTimeout:  30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	nestedRan := make(chan struct{})
	if err := service.Handle("self", HandlerOptions{Codec: immutableCodec}, func(ctx context.Context, args []any) (any, error) {
		nested, _ := Arg[bool](args, 0)
		if nested {
			close(nestedRan)
			return "nested", nil
		}
		_, err := Call(ctx, ref, "self", true)
		return nil, err
	}); err != nil {
		t.Fatal(err)
	}
	startTestService(t, service)
	defer stopTestSystem(t, system)

	if _, err := Call(context.Background(), ref, "self", false); !errors.Is(err, ErrCallTimeout) {
		t.Fatalf("self Call error = %v, want ErrCallTimeout", err)
	}
	select {
	case <-nestedRan:
	case <-time.After(time.Second):
		t.Fatal("queued nested call did not run after the outer timeout")
	}
}
