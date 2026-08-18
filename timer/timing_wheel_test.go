package timer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestTimingWheel_Basic(t *testing.T) {
	var mu sync.Mutex
	var completed []uint64

	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		mu.Lock()
		completed = append(completed, id)
		mu.Unlock()
	}, TimingWheelOptions{})

	now := time.Now()
	tw.Add(1, now.Add(2*time.Second))
	tw.Add(2, now.Add(3*time.Second))
	tw.Add(3, now.Add(5*time.Second))

	tw.Start()
	defer tw.Stop()

	time.Sleep(6 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(completed) != 3 {
		t.Errorf("expected 3 tasks, got %d: %v", len(completed), completed)
	}
}

func TestTimingWheel_AddOutOfRange(t *testing.T) {
	tw := NewTimingWheel(nil, TimingWheelOptions{})
	err := tw.Add(1, time.Now().Add(time.Hour+time.Second))
	if !errors.Is(err, ErrTimerOutOfRange) {
		t.Fatalf("Add() err = %v, want %v", err, ErrTimerOutOfRange)
	}
}

func TestTimingWheel_Remove(t *testing.T) {
	var mu sync.Mutex
	var completed []uint64

	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		mu.Lock()
		completed = append(completed, id)
		mu.Unlock()
	}, TimingWheelOptions{})

	now := time.Now()
	tw.Add(1, now.Add(2*time.Second))
	tw.Add(2, now.Add(3*time.Second))
	tw.Remove(2)

	tw.Start()
	defer tw.Stop()

	time.Sleep(4 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(completed) != 1 || completed[0] != uint64(1) {
		t.Errorf("expected [1], got %v", completed)
	}
}

func TestTimingWheel_RescheduleEarlier(t *testing.T) {
	ch := make(chan uint64, 2)
	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		ch <- id
	}, TimingWheelOptions{})

	now := time.Now()
	if err := tw.Add(1, now.Add(5*time.Second)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := tw.Reschedule(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Reschedule() error = %v", err)
	}

	tw.tick()
	waitForTimerID(t, ch, 1)

	for i := 0; i < 5; i++ {
		tw.tick()
	}
	assertNoTimerID(t, ch)
}

func TestTimingWheel_DedupWindow(t *testing.T) {
	ch := make(chan uint64, 3)
	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		ch <- id
	}, TimingWheelOptions{})
	tw.SetDedupWindow(time.Second)

	now := time.Now()
	if err := tw.Add(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	tw.tick()
	waitForTimerID(t, ch, 1)

	if err := tw.Add(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Add() duplicate error = %v", err)
	}
	tw.tick()
	assertNoTimerID(t, ch)

	tw.cleanLastFired(now.Add(2 * time.Second))
	if err := tw.Add(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Add() after window error = %v", err)
	}
	tw.tick()
	waitForTimerID(t, ch, 1)
}

func TestTimingWheel_RescheduleClearsDedupWindow(t *testing.T) {
	ch := make(chan uint64, 2)
	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		ch <- id
	}, TimingWheelOptions{})
	tw.SetDedupWindow(time.Minute)

	now := time.Now()
	if err := tw.Add(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	tw.tick()
	waitForTimerID(t, ch, 1)

	if err := tw.Reschedule(1, now.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("Reschedule() error = %v", err)
	}
	tw.tick()
	waitForTimerID(t, ch, 1)
}

func TestTimingWheel_SubSecondFiresNextTick(t *testing.T) {
	ch := make(chan uint64, 1)
	tw := NewTimingWheel(func(_ context.Context, id uint64) {
		ch <- id
	}, TimingWheelOptions{})

	// Regression: delay in (0, 1s) used to land in the current slot and never fire
	// until the wheel wrapped (about one hour).
	if err := tw.Add(7, time.Now().Add(200*time.Millisecond)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	assertNoTimerID(t, ch)
	tw.tick()
	waitForTimerID(t, ch, 7)
}

func TestTimingWheelCallbackContextExpires(t *testing.T) {
	done := make(chan error, 1)
	tw := NewTimingWheel(func(ctx context.Context, _ uint64) {
		<-ctx.Done()
		done <- ctx.Err()
	}, TimingWheelOptions{CallbackTimeout: 10 * time.Millisecond})
	if err := tw.Add(1, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("callback context error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for callback context")
	}
}

func TestTimingWheelStartAndStopAreIdempotent(t *testing.T) {
	tw := NewTimingWheel(nil, TimingWheelOptions{})
	tw.Start()
	tw.Start()
	tw.Stop()
	tw.Stop()
}

func TestSlotsAhead(t *testing.T) {
	if got := slotsAhead(0); got != 0 {
		t.Fatalf("slotsAhead(0)=%d, want 0", got)
	}
	if got := slotsAhead(200 * time.Millisecond); got != 1 {
		t.Fatalf("slotsAhead(200ms)=%d, want 1", got)
	}
	if got := slotsAhead(time.Second); got != 1 {
		t.Fatalf("slotsAhead(1s)=%d, want 1", got)
	}
	if got := slotsAhead(1500 * time.Millisecond); got != 1 {
		t.Fatalf("slotsAhead(1.5s)=%d, want 1", got)
	}
	if got := slotsAhead(2 * time.Second); got != 2 {
		t.Fatalf("slotsAhead(2s)=%d, want 2", got)
	}
}
