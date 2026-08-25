package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protowire"
	"github.com/scott4game/skygo/skylog"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func BenchmarkRemoteCall(b *testing.B) {
	benchmarkRemote(b, false)
}

func BenchmarkRemoteSend(b *testing.B) {
	benchmarkRemote(b, true)
}

func benchmarkRemote(b *testing.B, send bool) {
	for _, connections := range []int{1, 4} {
		for _, size := range []int{64, 1024, 64 * 1024} {
			b.Run(fmt.Sprintf("connections=%d/payload=%d", connections, size), func(b *testing.B) {
				node, systems, refs, method, notification, delivered := setupBenchmarkCluster(b, connections)
				_ = node
				payload := wrapperspb.Bytes(bytes.Repeat([]byte{0x5a}, size))
				var next atomic.Uint64
				var sent atomic.Uint64
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				if send {
					for operation := 0; operation < b.N; operation++ {
						ref := refs[operation%len(refs)]
						var err error
						for {
							sent.Add(1)
							err = notification.Send(context.Background(), ref, payload)
							if !errors.Is(err, actor.ErrTransportBackpressure) {
								break
							}
							sent.Add(^uint64(0))
							runtime.Gosched()
						}
						if err != nil {
							sent.Add(^uint64(0))
							b.Fatal(err)
						}
						for sent.Load()-delivered.Load() > 1024 {
							runtime.Gosched()
						}
					}
				} else {
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							ref := refs[next.Add(1)%uint64(len(refs))]
							_, err := method.Call(context.Background(), ref, payload)
							if err != nil {
								b.Errorf("remote operation: %v", err)
								return
							}
						}
					})
				}
				b.StopTimer()
				drainDeadline := time.Now().Add(5 * time.Second)
				for send && delivered.Load() < sent.Load() && time.Now().Before(drainDeadline) {
					runtime.Gosched()
				}
				if send && delivered.Load() != sent.Load() {
					b.Fatalf("notification drain timed out: sent=%d delivered=%d", sent.Load(), delivered.Load())
				}
				_ = systems
			})
		}
	}
}

func setupBenchmarkCluster(b *testing.B, connections int) (*Node, []*actor.System, []actor.Ref, actor.Method[*wrapperspb.BytesValue, *wrapperspb.BytesValue], actor.Notification[*wrapperspb.BytesValue], *atomic.Uint64) {
	b.Helper()
	skylog.SetDefault(skylog.FuncLogger{})
	b.Cleanup(func() { skylog.SetDefault(nil) })
	addressA, addressB := reserveBenchmarkAddress(b), reserveBenchmarkAddress(b)
	registry := NewStaticRegistry(Endpoint{NodeID: "bench-a", Address: addressA}, Endpoint{NodeID: "bench-b", Address: addressB})
	systemA, systemB := actor.NewSystem(actor.SystemOptions{}), actor.NewSystem(actor.SystemOptions{})
	nodeA, err := New(Config{NodeID: "bench-a", Listen: addressA, Registry: registry, Secret: testSecret, ConnectionsPerPeer: connections, SendQueue: 65536}, systemA)
	if err != nil {
		b.Fatal(err)
	}
	nodeB, err := New(Config{NodeID: "bench-b", Listen: addressB, Registry: registry, Secret: testSecret, ConnectionsPerPeer: connections, SendQueue: 65536, InboundQueue: 65536}, systemB)
	if err != nil {
		b.Fatal(err)
	}
	if err := nodeA.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := nodeB.Start(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = nodeA.Stop(ctx)
		_ = nodeB.Stop(ctx)
		_ = systemA.Stop(ctx)
		_ = systemB.Stop(ctx)
	})

	method := protowire.NewMethod("bench.echo", func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} }, func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} })
	notification := protowire.NewNotification("bench.notify", func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} })
	var delivered atomic.Uint64
	refs := make([]actor.Ref, 4)
	for index := range refs {
		name := fmt.Sprintf("bench-service-%d", index)
		service, _, err := systemB.Reserve(name, actor.ServiceOptions{MailboxSize: 65536})
		if err != nil {
			b.Fatal(err)
		}
		if err := actor.Register(service, method, func(_ context.Context, value *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
			return value, nil
		}); err != nil {
			b.Fatal(err)
		}
		if err := actor.RegisterNotification(service, notification, func(context.Context, *wrapperspb.BytesValue) error { delivered.Add(1); return nil }); err != nil {
			b.Fatal(err)
		}
		if err := service.Start(context.Background()); err != nil {
			b.Fatal(err)
		}
		refs[index], err = nodeA.Resolve(context.Background(), "bench-b", name)
		if err != nil {
			b.Fatal(err)
		}
	}
	return nodeA, []*actor.System{systemA, systemB}, refs, method, notification, &delivered
}

func reserveBenchmarkAddress(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
