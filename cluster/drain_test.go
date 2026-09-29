package cluster

import (
	"context"
	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protowire"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sync/atomic"
	"testing"
	"time"
)

func TestOutgoingBarrierPrecedesReceiverDrain(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	a, b := actor.NewSystem(actor.SystemOptions{Drainable: true}), actor.NewSystem(actor.SystemOptions{Drainable: true})
	nodeA := startTestNode(t, "a", addressA, registry, a, testSecret)
	startTestNode(t, "b", addressB, registry, b, testSecret)
	notification := protowire.NewNotification("drain-notice", func() *wrapperspb.Int64Value { return &wrapperspb.Int64Value{} })
	service, _, err := b.Reserve("drain-target", actor.ServiceOptions{MailboxSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var seen atomic.Int32
	if err := actor.RegisterNotification(service, notification, func(_ context.Context, message *wrapperspb.Int64Value) error {
		if message.Value == 0 {
			<-release
		}
		seen.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "drain-target")
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 200; i++ {
		if err := notification.Send(context.Background(), ref, wrapperspb.Int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := nodeA.FlushOutgoing(ctx); err != nil {
		close(release)
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := b.Drain(ctx); done <- err }()
	if b.DrainState().InFlight == 0 {
		close(release)
		t.Fatal("barrier acknowledged before mailbox admission")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if seen.Load() != 200 {
		t.Fatalf("dropped notifications: %d", seen.Load())
	}
}
func TestAdmissionCompletionTracksOutOfOrderShards(t *testing.T) {
	c := &inboundConn{}
	a, b := c.beginAdmission(), c.beginAdmission()
	c.finishAdmission(b)
	if c.admissionDone != 0 {
		t.Fatal("out-of-order acknowledgement skipped predecessor")
	}
	c.finishAdmission(a)
	if c.admissionDone != 2 {
		t.Fatal("contiguous barrier not advanced")
	}
}
