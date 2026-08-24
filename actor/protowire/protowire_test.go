package protowire

import (
	"context"
	"testing"

	"github.com/scott4game/skygo/actor"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMethodLocalRoundTripAndFingerprint(t *testing.T) {
	method := NewMethod("wire.echo", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	system := actor.NewSystem(actor.SystemOptions{})
	service, ref, err := system.Reserve("echo", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(service, method, func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		request.Value = "mutated"
		return request, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer system.Stop(context.Background())

	request := wrapperspb.String("hello")
	response, err := method.Call(context.Background(), ref, request)
	if err != nil {
		t.Fatal(err)
	}
	if request.Value != "hello" || response.Value != "mutated" {
		t.Fatalf("ownership or response mismatch: request=%q response=%q", request.Value, response.Value)
	}

	other := NewMethod("wire.other", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	if method.Fingerprint() == other.Fingerprint() {
		t.Fatal("different protocol names produced the same fingerprint")
	}
}
