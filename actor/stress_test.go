//go:build stress

package actor

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/internal/stresstest"
)

func TestStressRandomCallGraph(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	seed := stressSeed(t)
	scale := stresstest.Scale(t)
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	const serviceCount = 8
	refs := make([]Ref, serviceCount)
	services := make([]*Service, serviceCount)
	var notifications atomic.Uint64
	for i := range services {
		service, ref, err := system.Reserve(fmt.Sprintf("random-%d", i), ServiceOptions{
			MailboxSize:      8 + i*3,
			AdmissionTimeout: 250 * time.Millisecond,
			CallTimeout:      10 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		services[i], refs[i] = service, ref
	}
	for index, service := range services {
		index := index
		mustHandle(t, service, "walk", func(ctx context.Context, args []any) (any, error) {
			depth, err := Arg[int](args, 0)
			if err != nil || depth <= 0 {
				return index, err
			}
			route, err := Arg[uint64](args, 1)
			if err != nil {
				return nil, err
			}
			next := (index + int(route%serviceCount) + depth) % serviceCount
			return Call(ctx, refs[next], "walk", depth-1, route*6364136223846793005+1)
		})
		mustHandle(t, service, "note", func(context.Context, []any) (any, error) {
			notifications.Add(1)
			return nil, nil
		})
		startTestService(t, service)
	}

	const workers = 64
	iterations := 40 * scale
	var accepted atomic.Uint64
	stresstest.Guard(t, "random cooperative call graph", 2*time.Minute, func(ctx context.Context) error {
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			worker := worker
			wg.Add(1)
			go func() {
				defer wg.Done()
				rng := rand.New(rand.NewPCG(seed, uint64(worker+1)))
				for iteration := 0; iteration < iterations; iteration++ {
					ref := refs[rng.IntN(serviceCount)]
					switch rng.IntN(3) {
					case 0:
						callCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
						_, err := Call(callCtx, ref, "walk", 1+rng.IntN(4), rng.Uint64())
						cancel()
						if err != nil && !errors.Is(err, ErrMailboxTimeout) && !errors.Is(err, context.Canceled) {
							errs <- stressError("Call", worker, iteration, err)
							return
						}
					case 1:
						if err := Send(ctx, ref, "note"); err == nil {
							accepted.Add(1)
						} else if !errors.Is(err, ErrMailboxTimeout) && !errors.Is(err, context.Canceled) {
							errs <- stressError("Send", worker, iteration, err)
							return
						}
					case 2:
						if err := TrySend(ctx, ref, "note"); err == nil {
							accepted.Add(1)
						} else if !errors.Is(err, ErrMailboxFull) && !errors.Is(err, context.Canceled) {
							errs <- stressError("TrySend", worker, iteration, err)
							return
						}
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			return fmt.Errorf("seed=%d scale=%d stats=%+v: %w", seed, scale, system.Stats(), err)
		}
		if !stresstest.Poll(10*time.Second, 5*time.Millisecond, func() bool {
			return notifications.Load() == accepted.Load()
		}) {
			got, want := notifications.Load(), accepted.Load()
			return fmt.Errorf("accepted notifications: handled=%d accepted=%d", got, want)
		}
		return nil
	})
}

func TestStressNoInterleaveDAG(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	scale := stresstest.Scale(t)
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	const count = 4
	refs := make([]Ref, count)
	services := make([]*Service, count)
	for i := range services {
		service, ref, err := system.Reserve(fmt.Sprintf("dag-%d", i), ServiceOptions{
			NoInterleave:     true,
			MailboxSize:      128,
			AdmissionTimeout: time.Second,
			CallTimeout:      2 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		services[i], refs[i] = service, ref
	}
	for i, service := range services {
		i := i
		mustHandle(t, service, "chain", func(ctx context.Context, _ []any) (any, error) {
			if i == 0 {
				return 1, nil
			}
			return Call(ctx, refs[i-1], "chain")
		})
		startTestService(t, service)
	}
	stresstest.Guard(t, "NoInterleave DAG", time.Minute, func(ctx context.Context) error {
		var wg sync.WaitGroup
		errs := make(chan error, 32)
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 20*scale; j++ {
					if _, err := Call(ctx, refs[count-1], "chain"); err != nil {
						errs <- err
						return
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			return err
		}
		return nil
	})
}

func TestStressNoInterleaveCycleTimesOut(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	const count = 8
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			system := NewSystem(SystemOptions{})
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = system.Stop(ctx)
			}()
			service, ref, err := system.Reserve(fmt.Sprintf("cycle-%d", i), ServiceOptions{NoInterleave: true, CallTimeout: 30 * time.Millisecond})
			if err != nil {
				errs <- err
				return
			}
			if err := service.Handle("self", HandlerOptions{Codec: immutableCodec}, func(ctx context.Context, args []any) (any, error) {
				nested, _ := Arg[bool](args, 0)
				if nested {
					return nil, nil
				}
				return Call(ctx, ref, "self", true)
			}); err != nil {
				errs <- err
				return
			}
			if err := service.Start(context.Background()); err != nil {
				errs <- err
				return
			}
			if _, err := Call(context.Background(), ref, "self", false); !errors.Is(err, ErrCallTimeout) {
				errs <- fmt.Errorf("cycle %d error=%v, want ErrCallTimeout", i, err)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	stresstest.Wait(t, "NoInterleave cycle workers", 5*time.Second, done)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestStressConcurrentLifecycle(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	var current atomic.Pointer[Ref]
	var successfulCalls atomic.Uint64
	stresstest.Guard(t, "concurrent lifecycle", 30*time.Second, func(ctx context.Context) error {
		workerCtx, cancelWorkers := context.WithCancel(ctx)
		defer cancelWorkers()
		errs := make(chan error, 16)
		var workers sync.WaitGroup
		for i := 0; i < 16; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-workerCtx.Done():
						return
					default:
					}
					ptr := current.Load()
					if ptr == nil {
						runtime.Gosched()
						continue
					}
					callCtx, cancel := context.WithTimeout(workerCtx, 100*time.Millisecond)
					value, err := Call(callCtx, *ptr, "ping")
					cancel()
					if err == nil && value != "pong" {
						errs <- fmt.Errorf("ping value=%v", value)
						return
					}
					if err == nil {
						successfulCalls.Add(1)
					}
					if err != nil && !errors.Is(err, ErrStaleRef) && !errors.Is(err, ErrServiceStopping) && !errors.Is(err, ErrServiceNotReady) && !errors.Is(err, ErrMailboxTimeout) && !errors.Is(err, ErrCallTimeout) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errs <- err
						return
					}
				}
			}()
		}
		for round := 0; round < 50; round++ {
			service, ref, err := system.Reserve("rotating", ServiceOptions{MailboxSize: 8, CallTimeout: 200 * time.Millisecond})
			if err != nil {
				return err
			}
			if err := service.Handle("ping", HandlerOptions{Codec: immutableCodec}, func(context.Context, []any) (any, error) { return "pong", nil }); err != nil {
				return err
			}
			if err := service.Start(ctx); err != nil {
				return err
			}
			refCopy := ref
			current.Store(&refCopy)
			if _, err := system.Resolve("rotating"); err != nil {
				return err
			}
			before := successfulCalls.Load()
			_ = stresstest.Poll(50*time.Millisecond, 5*time.Millisecond, func() bool {
				return successfulCalls.Load() > before
			})
			stopCtx, stopCancel := context.WithTimeout(ctx, time.Second)
			err = service.Stop(stopCtx)
			stopCancel()
			if err != nil {
				return err
			}
			if _, err := system.Resolve("rotating"); !errors.Is(err, ErrServiceNotFound) {
				return fmt.Errorf("Resolve after Stop error=%v, want ErrServiceNotFound", err)
			}
		}
		cancelWorkers()
		workers.Wait()
		close(errs)
		for err := range errs {
			return err
		}
		return nil
	})
}

func TestStressMailboxBackpressure(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	scale := stresstest.Scale(t)
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	service, ref, err := system.Reserve("backpressure", ServiceOptions{MailboxSize: 2, AdmissionTimeout: 30 * time.Millisecond, CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	mustHandle(t, service, "block", func(context.Context, []any) (any, error) {
		close(entered)
		<-release
		return nil, nil
	})
	var notes atomic.Uint64
	mustHandle(t, service, "note", func(context.Context, []any) (any, error) { notes.Add(1); return nil, nil })
	mustHandle(t, service, "ping", func(context.Context, []any) (any, error) { return "pong", nil })
	startTestService(t, service)
	blockDone := make(chan error, 1)
	go func() { _, err := Call(context.Background(), ref, "block"); blockDone <- err }()
	stresstest.Wait(t, "blocking handler entry", time.Second, entered)
	var accepted, full atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < 128*scale; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch err := TrySend(context.Background(), ref, "note"); {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrMailboxFull):
				full.Add(1)
			default:
				t.Errorf("TrySend error = %v", err)
			}
		}()
	}
	wg.Wait()
	if full.Load() == 0 || accepted.Load() == 0 {
		t.Fatalf("accepted=%d full=%d, want both paths", accepted.Load(), full.Load())
	}
	close(release)
	if err := stresstest.Wait(t, "blocked call completion", time.Second, blockDone); err != nil {
		t.Fatal(err)
	}
	stresstest.Eventually(t, "accepted backpressure notifications", 3*time.Second, 5*time.Millisecond, func() bool {
		return notes.Load() == accepted.Load()
	})
	if value, err := Call(context.Background(), ref, "ping"); err != nil || value != "pong" {
		t.Fatalf("ping after pressure = (%v, %v)", value, err)
	}
}

func TestStressSuspendLimit(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	system := NewSystem(SystemOptions{})
	defer stopTestSystem(t, system)
	b, bRef := newTestService(t, system, "suspend-target")
	release := make(chan struct{})
	mustHandle(t, b, "hold", func(context.Context, []any) (any, error) { <-release; return nil, nil })
	a, aRef, err := system.Reserve("suspend-source", ServiceOptions{MailboxSize: 64, MaxSuspended: 4, CallTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	mustHandle(t, a, "via", func(ctx context.Context, _ []any) (any, error) { return Call(ctx, bRef, "hold") })
	startTestService(t, a)
	startTestService(t, b)
	results := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() { _, err := Call(context.Background(), aRef, "via"); results <- err }()
	}
	completed := 0
	limited := false
	for !limited {
		err := stresstest.Wait(t, "suspend-limit result", time.Second, results)
		completed++
		if errors.Is(err, ErrSuspendLimit) {
			limited = true
		} else if err != nil {
			t.Fatalf("Call error before release = %v", err)
		}
	}
	close(release)
	for ; completed < 16; completed++ {
		if err := stresstest.Wait(t, "released suspended call", time.Second, results); err != nil && !errors.Is(err, ErrSuspendLimit) {
			t.Fatalf("Call error = %v", err)
		}
	}
}
