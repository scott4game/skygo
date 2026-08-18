package actor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/skylog"
)

func TestDispatchRetrierFirstSuccessNoRetry(t *testing.T) {
	r := NewDispatchRetrier([]RetryConfig{{10 * time.Millisecond, 3}}, nil)
	r.Start()
	defer r.Stop()

	var calls atomic.Int32
	done := make(chan struct{})
	r.Enqueue("test", func() error {
		calls.Add(1)
		close(done)
		return nil
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("task not executed")
	}
	time.Sleep(50 * time.Millisecond) // ensure no extra calls
	if n := calls.Load(); n != 1 {
		t.Fatalf("expected 1 call, got %d", n)
	}
}

func TestDispatchRetrierLevelPromotion(t *testing.T) {
	// level0: 10ms × 3 tries, level1: 20ms × 2 tries
	r := NewDispatchRetrier([]RetryConfig{
		{10 * time.Millisecond, 3},
		{20 * time.Millisecond, 2},
	}, []int{64, 64})
	r.Start()
	defer r.Stop()

	var calls atomic.Int32
	succeedAfter := int32(4) // fail first 4 calls, succeed on 5th (promoted to level1)
	done := make(chan struct{})
	var once sync.Once

	r.Enqueue("promote-test", func() error {
		n := calls.Add(1)
		if n < succeedAfter {
			return errors.New("not yet")
		}
		once.Do(func() { close(done) })
		return nil
	})

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("task never succeeded, calls=%d", calls.Load())
	}
}

func TestDispatchRetrierAllLevelsExhaustedDropsTask(t *testing.T) {
	var mu sync.Mutex
	var logBuf strings.Builder
	capture := func(_ context.Context, format string, args ...any) {
		mu.Lock()
		fmt.Fprintf(&logBuf, format+"\n", args...)
		mu.Unlock()
	}
	skylog.SetDefault(skylog.FuncLogger{WarnFunc: capture, ErrorFunc: capture})
	t.Cleanup(func() { skylog.SetDefault(nil) })

	// Only 1 level, 2 tries, always fails.
	r := NewDispatchRetrier([]RetryConfig{{10 * time.Millisecond, 2}}, []int{64})
	r.Start()
	defer r.Stop()

	done := make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	r.Enqueue("exhaust-test", func() error {
		n := calls.Add(1)
		if n >= 2 {
			once.Do(func() { close(done) })
		}
		return errors.New("always fail")
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("retries not exhausted in time, calls=%d", calls.Load())
	}

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	out := logBuf.String()
	mu.Unlock()
	if !strings.Contains(out, "all retries exhausted") {
		t.Fatalf("expected 'all retries exhausted' in log; got:\n%s", out)
	}
}

func TestDispatchRetrierProcessBatchSnapshotLength(t *testing.T) {
	// processBatch should not process newly enqueued items from the same tick
	// — i.e., it snapshots len() at the start rather than draining indefinitely.
	r := NewDispatchRetrier([]RetryConfig{{20 * time.Millisecond, 5}}, []int{256})
	r.Start()
	defer r.Stop()

	const preload = 5
	var calls atomic.Int32

	// Pre-fill the queue before the first tick fires.
	for range preload {
		r.Enqueue("snap-test", func() error {
			calls.Add(1)
			return errors.New("always fail") // re-enqueues itself
		})
	}

	// Wait for at least one full tick cycle.
	time.Sleep(60 * time.Millisecond)

	got := int(calls.Load())
	// Each of the 5 tasks was attempted exactly once in the first tick.
	// The snapshot guarantees they won't loop infinitely in one tick.
	if got < preload {
		t.Fatalf("expected at least %d calls in first tick, got %d", preload, got)
	}
	// The snapshot also ensures we don't process re-enqueued items in the same tick.
	// Allow some slack for timing but it should not be dramatically more.
	if got > preload*3 {
		t.Fatalf("expected at most %d calls (snapshot guard), got %d", preload*3, got)
	}
}
