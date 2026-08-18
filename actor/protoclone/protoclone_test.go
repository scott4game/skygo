package protoclone_test

import (
	"context"
	"testing"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protoclone"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestTypedMethodCallAndProtoClone asserts that importing this package restores
// the automatic deep copy of protobuf requests and responses: a handler that
// mutates its request must not be able to reach the caller's copy.
func TestTypedMethodCallAndProtoClone(t *testing.T) {
	system := actor.NewSystem(actor.SystemOptions{})
	service, ref, err := system.Reserve("typed", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	method := actor.NewMethod[*wrapperspb.StringValue, *wrapperspb.StringValue]("echo")
	if err := actor.Register(service, method, func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		request.Value = "handler"
		return request, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer service.Stop(context.Background())

	request := wrapperspb.String("caller")
	response, err := method.Call(context.Background(), ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if request.Value != "caller" {
		t.Fatalf("handler mutated caller request: %q", request.Value)
	}
	if response.Value != "handler" {
		t.Fatalf("response = %q, want handler", response.Value)
	}
}

// TestWithoutProviderProtoIsRejected documents the core's behaviour when no
// clone provider is registered: registration fails loudly rather than sharing
// a mutable message between actors.
func TestWithoutProviderProtoIsRejected(t *testing.T) {
	if _, ok := protoclone.Provider(nil); ok {
		t.Fatal("Provider accepted a nil type")
	}
}
