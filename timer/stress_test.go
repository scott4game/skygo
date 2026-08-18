//go:build stress

package timer

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/internal/stresstest"
)

func TestStressTimingWheelOrderedConcurrentOperations(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	const taskCount = 1000
	counts := make([]atomic.Uint32, taskCount)
	var callbacks sync.WaitGroup
	for id := 0; id < taskCount; id++ {
		if id%3 != 0 {
			callbacks.Add(1)
		}
	}
	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		if id < taskCount {
			counts[id].Add(1)
			callbacks.Done()
		}
	}, TimingWheelOptions{})
	now := time.Now()
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := worker; id < taskCount; id += 16 {
				if err := tw.Add(uint64(id), now.Add(1500*time.Millisecond)); err != nil {
					t.Errorf("Add(%d): %v", id, err)
				}
			}
		}()
	}
	addDone := make(chan struct{})
	go func() { wg.Wait(); close(addDone) }()
	stresstest.Wait(t, "concurrent timing-wheel adds", 10*time.Second, addDone)
	for worker := 0; worker < 16; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := worker; id < taskCount; id += 16 {
				if id%3 == 0 {
					_ = tw.Remove(uint64(id))
				} else {
					_ = tw.Reschedule(uint64(id), now.Add(1500*time.Millisecond))
				}
			}
		}()
	}
	mutationDone := make(chan struct{})
	go func() { wg.Wait(); close(mutationDone) }()
	stresstest.Wait(t, "ordered timing-wheel mutation", 10*time.Second, mutationDone)
	tw.tick()
	done := make(chan struct{})
	go func() { callbacks.Wait(); close(done) }()
	stresstest.Wait(t, "ordered timing-wheel callbacks", 3*time.Second, done)
	for id := 0; id < taskCount; id++ {
		want := uint32(1)
		if id%3 == 0 {
			want = 0
		}
		if got := counts[id].Load(); got != want {
			t.Fatalf("task %d callbacks=%d want=%d", id, got, want)
		}
	}
}

func TestStressTimingWheelConcurrentMutationRemainsUsable(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	var callbacks atomic.Uint64
	sentinel := make(chan struct{})
	var sentinelOnce sync.Once
	tw := NewTimingWheel(func(ctx context.Context, id uint64) {
		select {
		case <-ctx.Done():
		default:
		}
		callbacks.Add(1)
		if id == 10_000 {
			sentinelOnce.Do(func() { close(sentinel) })
		}
	}, TimingWheelOptions{CallbackTimeout: 50 * time.Millisecond, DedupWindow: 20 * time.Millisecond})
	const workers, iterations = 32, 1000
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(0x71a3, uint64(worker+1)))
			for i := 0; i < iterations; i++ {
				id := uint64(rng.IntN(64) + 1)
				switch rng.IntN(3) {
				case 0:
					_ = tw.Add(id, time.Now().Add(time.Duration(rng.IntN(1500))*time.Millisecond))
				case 1:
					_ = tw.Remove(id)
				case 2:
					_ = tw.Reschedule(id, time.Now().Add(time.Duration(rng.IntN(1500))*time.Millisecond))
				}
				if i%20 == 0 {
					tw.tick()
				}
			}
		}()
	}
	mutationDone := make(chan struct{})
	go func() { wg.Wait(); close(mutationDone) }()
	stresstest.Wait(t, "racing timing-wheel mutation", 30*time.Second, mutationDone)

	before := callbacks.Load()
	if err := tw.Add(10_000, time.Now().Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	stresstest.Wait(t, "timing-wheel sentinel callback", time.Second, sentinel)
	if callbacks.Load() == before {
		t.Fatal("sentinel did not increment callback count")
	}
	tw.cleanLastFired(time.Now().Add(time.Hour))
	tw.dedupMu.Lock()
	remaining := len(tw.lastFired)
	tw.dedupMu.Unlock()
	if remaining != 0 {
		t.Fatalf("dedup history retained %d expired IDs", remaining)
	}
	tw.Start()
	tw.Stop()
	tw.Stop()
}

func TestStressTimingWheelDedupWindow(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	fired := make(chan uint64, 3)
	tw := NewTimingWheel(func(_ context.Context, id uint64) { fired <- id }, TimingWheelOptions{DedupWindow: 20 * time.Millisecond})
	for i := 0; i < 2; i++ {
		if err := tw.Add(7, time.Now().Add(-time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	if id := stresstest.Wait(t, "first deduplicated callback", time.Second, fired); id != 7 {
		t.Fatal(id)
	}
	select {
	case id := <-fired:
		t.Fatalf("duplicate callback id=%d", id)
	case <-time.After(30 * time.Millisecond):
	}
	tw.cleanLastFired(time.Now().Add(time.Second))
	if err := tw.Add(7, time.Now().Add(-time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	stresstest.Wait(t, "callback outside dedup window", time.Second, fired)
}
