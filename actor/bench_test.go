package actor

import (
	"context"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func benchmarkSystem(b *testing.B, noInterleave bool) (*System, Ref, Ref) {
	b.Helper()
	system := NewSystem(SystemOptions{})
	leaf, leafRef, err := system.Reserve("bench-leaf", ServiceOptions{MailboxSize: 4096, CallTimeout: 10 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	mustHandle(b, leaf, "echo", func(context.Context, []any) (any, error) { return nil, nil })
	root, rootRef, err := system.Reserve("bench-root", ServiceOptions{MailboxSize: 4096, CallTimeout: 10 * time.Second, NoInterleave: noInterleave})
	if err != nil {
		b.Fatal(err)
	}
	mustHandle(b, root, "nested", func(ctx context.Context, _ []any) (any, error) { return Call(ctx, leafRef, "echo") })
	startTestService(b, leaf)
	startTestService(b, root)
	b.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := system.Stop(ctx); err != nil {
			b.Errorf("Stop: %v", err)
		}
	})
	return system, leafRef, rootRef
}

func BenchmarkCallSameService(b *testing.B) {
	_, ref, _ := benchmarkSystem(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Call(context.Background(), ref, "echo"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCallContended(b *testing.B) {
	_, ref, _ := benchmarkSystem(b, false)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := Call(context.Background(), ref, "echo"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkCallParallelServices(b *testing.B) {
	serviceCount := runtime.GOMAXPROCS(0) * 2
	if serviceCount < 2 {
		serviceCount = 2
	}
	if serviceCount > 32 {
		serviceCount = 32
	}
	system := NewSystem(SystemOptions{})
	refs := make([]Ref, serviceCount)
	for i := range refs {
		service, ref, err := system.Reserve("bench-parallel-"+strconv.Itoa(i), ServiceOptions{
			MailboxSize: 4096,
			CallTimeout: 10 * time.Second,
		})
		if err != nil {
			b.Fatal(err)
		}
		mustHandle(b, service, "echo", func(context.Context, []any) (any, error) { return nil, nil })
		startTestService(b, service)
		refs[i] = ref
	}
	b.Cleanup(func() { stopTestSystem(b, system) })
	var route atomic.Uint64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localRoute := route.Add(1) - 1
		for pb.Next() {
			ref := refs[localRoute%uint64(len(refs))]
			localRoute++
			if _, err := Call(context.Background(), ref, "echo"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSend(b *testing.B) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("bench-send", ServiceOptions{MailboxSize: 4096, AdmissionTimeout: 10 * time.Second})
	if err != nil {
		b.Fatal(err)
	}
	var handled atomic.Uint64
	mustHandle(b, service, "note", func(context.Context, []any) (any, error) { handled.Add(1); return nil, nil })
	startTestService(b, service)
	b.Cleanup(func() { stopTestSystem(b, system) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := Send(context.Background(), ref, "note"); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	for handled.Load() != uint64(b.N) {
		runtime.Gosched()
	}
}

func BenchmarkTrySend(b *testing.B) {
	system := NewSystem(SystemOptions{})
	service, ref, err := system.Reserve("bench-try-send", ServiceOptions{MailboxSize: 4096})
	if err != nil {
		b.Fatal(err)
	}
	var handled, attempts atomic.Uint64
	mustHandle(b, service, "note", func(context.Context, []any) (any, error) { handled.Add(1); return nil, nil })
	startTestService(b, service)
	b.Cleanup(func() { stopTestSystem(b, system) })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for {
			attempts.Add(1)
			if err := TrySend(context.Background(), ref, "note"); err == nil {
				break
			} else if err != ErrMailboxFull {
				b.Fatal(err)
			}
			runtime.Gosched()
		}
	}
	b.StopTimer()
	for handled.Load() != uint64(b.N) {
		runtime.Gosched()
	}
	b.ReportMetric(float64(attempts.Load())/float64(b.N), "attempts/op")
}

func BenchmarkNestedCallYield(b *testing.B) {
	_, _, ref := benchmarkSystem(b, false)
	benchmarkNestedCall(b, ref)
}

func BenchmarkCallNoInterleave(b *testing.B) {
	_, _, ref := benchmarkSystem(b, true)
	benchmarkNestedCall(b, ref)
}

func benchmarkNestedCall(b *testing.B, ref Ref) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Call(context.Background(), ref, "nested"); err != nil {
			b.Fatal(err)
		}
	}
}
