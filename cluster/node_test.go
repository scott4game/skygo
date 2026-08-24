package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protowire"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var testSecret = []byte("0123456789abcdef0123456789abcdef")

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func startTestNode(t *testing.T, nodeID, address string, registry Registry, system *actor.System, secret []byte) *Node {
	t.Helper()
	node, err := New(Config{NodeID: nodeID, Listen: address, Registry: registry, Secret: secret, CallTimeout: time.Second}, system)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = node.Stop(ctx)
		_ = system.Stop(ctx)
	})
	return node
}

func TestRemoteMethodNotificationAndSchemaMismatch(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)

	echo := protowire.NewMethod("echo", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	notify := protowire.NewNotification("notice", func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	service, _, err := systemB.Reserve("echo-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(service, echo, func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("reply:" + request.Value), nil
	}); err != nil {
		t.Fatal(err)
	}
	notices := make(chan string, 1)
	if err := actor.RegisterNotification(service, notify, func(_ context.Context, request *wrapperspb.StringValue) error { notices <- request.Value; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	ref, err := nodeA.Resolve(context.Background(), "b", "echo-service")
	if err != nil {
		t.Fatal(err)
	}
	response, err := echo.Call(context.Background(), ref, wrapperspb.String("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if response.Value != "reply:hello" {
		t.Fatalf("response=%q", response.Value)
	}
	if err := notify.Send(context.Background(), ref, wrapperspb.String("seen")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-notices:
		if got != "seen" {
			t.Fatalf("notice=%q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("notification timeout")
	}

	wrong := protowire.NewMethod("echo", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	if _, err := wrong.Call(context.Background(), ref, &emptypb.Empty{}); !errors.Is(err, actor.ErrProtocolTypeMismatch) {
		t.Fatalf("schema mismatch error=%v", err)
	}
}

func TestRemoteNoInterleaveCycleFailsFast(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	nodeB := startTestNode(t, "b", addressB, registry, systemB, testSecret)

	entry := protowire.NewMethod("entry", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	back := protowire.NewMethod("back", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	leaf := protowire.NewMethod("leaf", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	alpha, alphaLocal, err := systemA.Reserve("alpha", actor.ServiceOptions{NoInterleave: true, CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	beta, _, err := systemB.Reserve("beta", actor.ServiceOptions{CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var betaRef, alphaRemote actor.Ref
	if err := actor.Register(alpha, entry, func(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
		return back.Call(ctx, betaRef, &emptypb.Empty{})
	}); err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(alpha, leaf, func(context.Context, *emptypb.Empty) (*emptypb.Empty, error) { return &emptypb.Empty{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(beta, back, func(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
		return leaf.Call(ctx, alphaRemote, &emptypb.Empty{})
	}); err != nil {
		t.Fatal(err)
	}
	if err := alpha.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := beta.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	betaRef, err = nodeA.Resolve(context.Background(), "b", "beta")
	if err != nil {
		t.Fatal(err)
	}
	alphaRemote, err = nodeB.Resolve(context.Background(), "a", "alpha")
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	_, err = entry.Call(context.Background(), alphaLocal, &emptypb.Empty{})
	if !errors.Is(err, actor.ErrCallCycle) {
		t.Fatalf("cycle error=%v", err)
	}
	if time.Since(started) >= time.Second {
		t.Fatalf("cycle waited for timeout: %v", time.Since(started))
	}
}

func TestHandshakeRejectsWrongSecret(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, []byte("aaaaaaaaaaaaaaaa"))
	_ = startTestNode(t, "b", addressB, registry, systemB, []byte("bbbbbbbbbbbbbbbb"))
	if _, err := nodeA.Resolve(context.Background(), "b", "missing"); !errors.Is(err, actor.ErrRemoteUnavailable) {
		t.Fatalf("wrong-secret error=%v", err)
	}
}

func TestReloadFailsPendingCallWhenNodeRemoved(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)

	blocking := protowire.NewMethod("blocking", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	service, _, err := systemB.Reserve("blocking-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := actor.Register(service, blocking, func(context.Context, *emptypb.Empty) (*emptypb.Empty, error) {
		close(entered)
		<-release
		return &emptypb.Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "blocking-service")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, callErr := blocking.Call(context.Background(), ref, &emptypb.Empty{})
		result <- callErr
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("remote call did not enter handler")
	}

	registry.Replace(Endpoint{NodeID: "a", Address: addressA})
	if err := nodeA.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, actor.ErrRemoteUnavailable) {
			t.Fatalf("pending call error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending call remained blocked after reload")
	}
	close(release)
}

func TestReloadRejectsSnapshotWithoutLocalNodeAtomically(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	system := actor.NewSystem(actor.SystemOptions{})
	node := startTestNode(t, "a", addressA, registry, system, testSecret)
	revision := node.Revision()
	registry.Replace(Endpoint{NodeID: "b", Address: "127.0.0.1:1"})
	if err := node.Reload(context.Background()); err == nil {
		t.Fatal("Reload() succeeded without local node")
	}
	if node.Revision() != revision {
		t.Fatalf("revision changed after rejected reload: %s", node.Revision())
	}
	endpoint, err := node.endpoint("b")
	if err != nil || endpoint != addressB {
		t.Fatalf("active endpoint changed after rejected reload: endpoint=%q err=%v", endpoint, err)
	}
}
