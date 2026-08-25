package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protowire"
	"github.com/scott4game/skygo/cluster/internal/wire"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestInboundGlobalQueueBound(t *testing.T) {
	d := &inboundDispatcher{
		shards: []chan *inboundJob{make(chan *inboundJob, 4)},
		slots:  make(chan struct{}, 1),
	}
	job := func(id uint64) *inboundJob {
		return &inboundJob{envelope: &wire.Envelope{RequestId: id, Target: &wire.Target{Address: 1}}}
	}
	if !d.submit(job(1)) {
		t.Fatal("first admission rejected")
	}
	if d.submit(job(2)) {
		t.Fatal("global queue admitted beyond configured capacity")
	}
	if got := d.queued.Load(); got != 1 {
		t.Fatalf("queued=%d, want 1", got)
	}
}

func TestInboundCallLimitReturnsBackpressure(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNodeConfig(t, Config{NodeID: "b", Listen: addressB, Registry: registry, Secret: testSecret, MaxInboundCalls: 1}, systemB)

	method := protowire.NewMethod("limited", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	service, _, err := systemB.Reserve("limited-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := actor.Register(service, method, func(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
		close(entered)
		<-release
		return &emptypb.Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "limited-service")
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, callErr := method.Call(context.Background(), ref, &emptypb.Empty{}); first <- callErr }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first call did not enter")
	}
	if _, err := method.Call(context.Background(), ref, &emptypb.Empty{}); !errors.Is(err, actor.ErrTransportBackpressure) {
		t.Fatalf("second call error=%v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledInboundJobBeforeAdmissionIsDiscarded(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	conn := &inboundConn{conn: left}
	job := &inboundJob{conn: conn, envelope: &wire.Envelope{Kind: wire.Kind_KIND_CALL, RequestId: 42}}
	conn.calls.Store(uint64(42), job)
	job.canceled.Store(true)
	(&inboundDispatcher{}).dispatch(job)
	if _, exists := conn.calls.Load(uint64(42)); exists {
		t.Fatal("canceled job remained tracked")
	}
}

func TestDuplicateCancelIsHarmless(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	nodeB := startTestNode(t, "b", addressB, registry, systemB, testSecret)
	service, _, err := systemB.Reserve("cancel-target", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.Resolve(context.Background(), "b", "cancel-target"); err != nil {
		t.Fatal(err)
	}
	peer, err := nodeA.peerFor(context.Background(), "b", 0)
	if err != nil {
		t.Fatal(err)
	}
	nodeA.sendCancel(peer, 999)
	nodeA.sendCancel(peer, 999)
	deadline := time.Now().Add(time.Second)
	for nodeB.Stats().Counters.CancelsReceived < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := nodeB.Stats().Counters.CancelsReceived; got != 2 {
		t.Fatalf("cancels received=%d, want 2", got)
	}
}

func TestControlQueueRemainsAvailableWhenSendQueueIsFull(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	p := &peer{conn: left, send: make(chan outbound, 1), control: make(chan outbound, 1), done: make(chan struct{})}
	p.send <- outbound{envelope: &wire.Envelope{Kind: wire.Kind_KIND_SEND}}
	p.enqueueControl(&wire.Envelope{Kind: wire.Kind_KIND_PING})
	if len(p.send) != 1 || len(p.control) != 1 || p.isClosed() {
		t.Fatalf("send=%d control=%d closed=%v", len(p.send), len(p.control), p.isClosed())
	}
}

func TestStopFailsPendingCallWithoutReplay(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)
	method := protowire.NewMethod("stop-pending", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	service, _, err := systemB.Reserve("stop-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := actor.Register(service, method, func(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
		close(entered)
		<-release
		return &emptypb.Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "stop-service")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { _, callErr := method.Call(context.Background(), ref, &emptypb.Empty{}); result <- callErr }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("call did not enter")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- nodeA.Stop(stopCtx) }()
	if err := <-result; !errors.Is(err, actor.ErrRemoteUnavailable) {
		t.Fatalf("pending call error=%v", err)
	}
	close(release)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	for _, peer := range nodeA.Stats().Peers {
		if peer.PendingCalls != 0 || peer.QueueDepth != 0 {
			t.Fatalf("peer not drained: %+v", peer)
		}
	}
}
