package cluster

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
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
	return startTestNodeConfig(t, Config{NodeID: nodeID, Listen: address, Registry: registry, Secret: secret, CallTimeout: time.Second}, system)
}

func startTestNodeConfig(t *testing.T, cfg Config, system *actor.System) *Node {
	t.Helper()
	node, err := New(cfg, system)
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

func TestRemoteAdmissionPreservesNotificationOrder(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNodeConfig(t, Config{NodeID: "a", Listen: addressA, Registry: registry, Secret: testSecret, ConnectionsPerPeer: 4}, systemA)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)
	notice := protowire.NewNotification("ordered", func() *wrapperspb.Int64Value { return &wrapperspb.Int64Value{} })
	service, _, err := systemB.Reserve("ordered-service", actor.ServiceOptions{MailboxSize: 1024})
	if err != nil {
		t.Fatal(err)
	}
	values := make(chan int64, 512)
	if err := actor.RegisterNotification(service, notice, func(_ context.Context, value *wrapperspb.Int64Value) error { values <- value.Value; return nil }); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "ordered-service")
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < 500; i++ {
		if err := notice.Send(context.Background(), ref, wrapperspb.Int64(i)); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	for want := int64(0); want < 500; want++ {
		select {
		case got := <-values:
			if got != want {
				t.Fatalf("notification order got=%d want=%d", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("notification %d timed out", want)
		}
	}
}

func TestRemoteAsyncAdmissionAllowsInterleaving(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)
	block := protowire.NewMethod("block", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	probe := protowire.NewMethod("probe", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	service, _, err := systemB.Reserve("interleave-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := actor.Register(service, block, func(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
		_, waitErr := actor.Await(ctx, "remote-block", func(context.Context) (struct{}, error) { close(entered); <-release; return struct{}{}, nil })
		return &emptypb.Empty{}, waitErr
	}); err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(service, probe, func(context.Context, *emptypb.Empty) (*emptypb.Empty, error) { return &emptypb.Empty{}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "interleave-service")
	if err != nil {
		t.Fatal(err)
	}
	blocked := make(chan error, 1)
	go func() { _, callErr := block.Call(context.Background(), ref, &emptypb.Empty{}); blocked <- callErr }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking call did not enter")
	}
	probeDone := make(chan error, 1)
	go func() { _, callErr := probe.Call(context.Background(), ref, &emptypb.Empty{}); probeDone <- callErr }()
	select {
	case err := <-probeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second call was blocked by transport admission")
	}
	close(release)
	if err := <-blocked; err != nil {
		t.Fatal(err)
	}
}

func TestRemoteCancelAbandonsResponseWithoutCancelingHandler(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	nodeB := startTestNode(t, "b", addressB, registry, systemB, testSecret)
	method := protowire.NewMethod("cancel-wait", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *emptypb.Empty { return &emptypb.Empty{} })
	service, _, err := systemB.Reserve("cancel-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entered, release, completed := make(chan struct{}), make(chan struct{}), make(chan bool, 1)
	if err := actor.Register(service, method, func(ctx context.Context, _ *emptypb.Empty) (*emptypb.Empty, error) {
		close(entered)
		<-release
		completed <- ctx.Err() != nil
		return &emptypb.Empty{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ref, err := nodeA.Resolve(context.Background(), "b", "cancel-service")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, callErr := method.Call(ctx, ref, &emptypb.Empty{}); result <- callErr }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("call error=%v", err)
	}
	close(release)
	select {
	case canceled := <-completed:
		if canceled {
			t.Fatal("remote handler context was canceled")
		}
	case <-time.After(time.Second):
		t.Fatal("handler did not complete")
	}
	deadline := time.Now().Add(time.Second)
	for nodeB.Stats().Counters.CancelsReceived == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if nodeA.Stats().Counters.CancelsSent == 0 || nodeB.Stats().Counters.CancelsReceived == 0 {
		t.Fatalf("cancel counters a=%+v b=%+v", nodeA.Stats().Counters, nodeB.Stats().Counters)
	}
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

func TestRemoteCallRejectsRefFromPreviousIncarnation(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA := actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNodeConfig(t, Config{
		NodeID: "a", Listen: addressA, Registry: registry, Secret: testSecret,
		CallTimeout: time.Second, ConnectBackoffMin: 5 * time.Millisecond, ConnectBackoffMax: 5 * time.Millisecond,
	}, systemA)

	identity := protowire.NewMethod("identity", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	systemB1 := actor.NewSystem(actor.SystemOptions{})
	nodeB1 := startTestNode(t, "b", addressB, registry, systemB1, testSecret)
	oldService, oldLocalRef, err := systemB1.Reserve("old-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(oldService, identity, func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("old"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := oldService.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	oldRef, err := nodeA.Resolve(context.Background(), "b", "old-service")
	if err != nil {
		t.Fatal(err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	if err := nodeB1.Stop(stopCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := systemB1.Stop(stopCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		peers := nodeA.Stats().Peers
		if len(peers) == 1 && peers[0].State != PeerReady {
			break
		}
		time.Sleep(time.Millisecond)
	}

	systemB2 := actor.NewSystem(actor.SystemOptions{})
	_ = startTestNode(t, "b", addressB, registry, systemB2, testSecret)
	newService, newLocalRef, err := systemB2.Reserve("new-service", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if oldLocalRef.Address != newLocalRef.Address || oldLocalRef.Generation != newLocalRef.Generation {
		t.Fatalf("restarted system did not reuse identity: old=%+v new=%+v", oldLocalRef, newLocalRef)
	}
	if err := actor.Register(newService, identity, func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("new"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := newService.Start(context.Background()); err != nil {
		t.Fatal(err)
	}

	deadline = time.Now().Add(time.Second)
	for {
		_, err = identity.Call(context.Background(), oldRef, &emptypb.Empty{})
		if !errors.Is(err, actor.ErrRemoteUnavailable) || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !errors.Is(err, actor.ErrStaleRef) {
		t.Fatalf("old incarnation Call error=%v, want ErrStaleRef", err)
	}

	freshRef, err := nodeA.Resolve(context.Background(), "b", "new-service")
	if err != nil {
		t.Fatal(err)
	}
	response, err := identity.Call(context.Background(), freshRef, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Value != "new" {
		t.Fatalf("fresh response=%q, want new", response.Value)
	}
}

func TestValidateInboundTargetRejectsWrongIdentity(t *testing.T) {
	node := &Node{cfg: Config{NodeID: "b"}, incarnation: "current"}
	for _, target := range []actor.RemoteTarget{
		{Node: "other", Incarnation: "current"},
		{Node: "b", Incarnation: "previous"},
	} {
		if err := node.validateInboundTarget(target); !errors.Is(err, actor.ErrStaleRef) {
			t.Fatalf("validateInboundTarget(%+v)=%v, want ErrStaleRef", target, err)
		}
	}
	if err := node.validateInboundTarget(actor.RemoteTarget{Node: "b", Incarnation: "current"}); err != nil {
		t.Fatalf("current target rejected: %v", err)
	}
}

func TestConcurrentResolveSharesOnePeerConnection(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	nodeB := startTestNode(t, "b", addressB, registry, systemB, testSecret)
	service, _, err := systemB.Reserve("shared", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errorsSeen := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, resolveErr := nodeA.Resolve(context.Background(), "b", "shared")
			errorsSeen <- resolveErr
		}()
	}
	wg.Wait()
	close(errorsSeen)
	for resolveErr := range errorsSeen {
		if resolveErr != nil {
			t.Fatal(resolveErr)
		}
	}
	if peers := nodeA.Stats().Peers; len(peers) != 1 || peers[0].State != PeerReady {
		t.Fatalf("peers=%+v", peers)
	}
	if got := nodeB.Stats().InboundConnections; got != 1 {
		t.Fatalf("inbound connections=%d", got)
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
	for _, identity := range []string{"a/alpha", "b/beta"} {
		if !strings.Contains(err.Error(), identity) {
			t.Fatalf("cycle error %q does not include %q", err, identity)
		}
	}
	if time.Since(started) >= time.Second {
		t.Fatalf("cycle waited for timeout: %v", time.Since(started))
	}
}

func TestRemoteNoInterleaveAllowsSameServiceNameOnDifferentNodes(t *testing.T) {
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	registry := NewStaticRegistry(Endpoint{NodeID: "a", Address: addressA}, Endpoint{NodeID: "b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA := startTestNode(t, "a", addressA, registry, systemA, testSecret)
	_ = startTestNode(t, "b", addressB, registry, systemB, testSecret)

	entry := protowire.NewMethod("entry", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	leaf := protowire.NewMethod("leaf", func() *emptypb.Empty { return &emptypb.Empty{} }, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} })
	local, localRef, err := systemA.Reserve("shared", actor.ServiceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	remote, _, err := systemB.Reserve("shared", actor.ServiceOptions{NoInterleave: true})
	if err != nil {
		t.Fatal(err)
	}
	var remoteRef actor.Ref
	if err := actor.Register(local, entry, func(ctx context.Context, _ *emptypb.Empty) (*wrapperspb.StringValue, error) {
		return leaf.Call(ctx, remoteRef, &emptypb.Empty{})
	}); err != nil {
		t.Fatal(err)
	}
	if err := actor.Register(remote, leaf, func(context.Context, *emptypb.Empty) (*wrapperspb.StringValue, error) {
		return wrapperspb.String("ok"), nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := local.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := remote.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	remoteRef, err = nodeA.Resolve(context.Background(), "b", "shared")
	if err != nil {
		t.Fatal(err)
	}

	response, err := entry.Call(context.Background(), localRef, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("same-name cross-node call: %v", err)
	}
	if response.Value != "ok" {
		t.Fatalf("response=%q, want ok", response.Value)
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
	started := time.Now()
	if _, err := nodeA.Resolve(context.Background(), "b", "missing"); !errors.Is(err, actor.ErrRemoteUnavailable) {
		t.Fatalf("backoff error=%v", err)
	}
	if time.Since(started) > 50*time.Millisecond {
		t.Fatalf("backoff did not fail fast: %v", time.Since(started))
	}
	stats := nodeA.Stats()
	if len(stats.Peers) != 1 || stats.Peers[0].State != PeerBackoff || stats.Peers[0].HandshakeFailures == 0 {
		t.Fatalf("unexpected peer stats: %+v", stats.Peers)
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
