package timer

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDelayQueue_Basic(t *testing.T) {
	var mu sync.Mutex
	var completed []uint64

	dq := NewDelayQueue(func(_ context.Context, id uint64) {
		mu.Lock()
		completed = append(completed, id)
		mu.Unlock()
	})

	now := time.Now()
	dq.Add(1, now.Add(1*time.Second))
	dq.Add(2, now.Add(2*time.Second))
	dq.Add(3, now.Add(3*time.Second))

	dq.Start()
	defer dq.Stop()

	time.Sleep(4 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(completed) != 3 {
		t.Errorf("expected 3 tasks, got %d", len(completed))
	}
}

func TestDelayQueue_Remove(t *testing.T) {
	var mu sync.Mutex
	var completed []uint64

	dq := NewDelayQueue(func(_ context.Context, id uint64) {
		mu.Lock()
		completed = append(completed, id)
		mu.Unlock()
	})

	now := time.Now()
	dq.Add(1, now.Add(1*time.Second))
	dq.Add(2, now.Add(2*time.Second))
	dq.Remove(2)

	dq.Start()
	defer dq.Stop()

	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	if len(completed) != 1 || completed[0] != 1 {
		t.Errorf("expected [1], got %v", completed)
	}
}

func TestDelayQueue_RescheduleEarlier(t *testing.T) {
	ch := make(chan uint64, 2)
	dq := NewDelayQueue(func(_ context.Context, id uint64) {
		ch <- id
	})

	now := time.Now()
	if err := dq.Add(1, now.Add(5*time.Second)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	if err := dq.Reschedule(1, now.Add(time.Second)); err != nil {
		t.Fatalf("Reschedule() error = %v", err)
	}

	dq.checkExpired(now.Add(1100 * time.Millisecond))
	waitForTimerID(t, ch, 1)

	dq.checkExpired(now.Add(6 * time.Second))
	assertNoTimerID(t, ch)
}

func TestDelayQueue_DedupWindow(t *testing.T) {
	ch := make(chan uint64, 3)
	dq := NewDelayQueue(func(_ context.Context, id uint64) {
		ch <- id
	}, WithDedupWindow(time.Second))

	now := time.Now()
	if err := dq.Add(1, now.Add(time.Second)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	dq.checkExpired(now.Add(1100 * time.Millisecond))
	waitForTimerID(t, ch, 1)

	if err := dq.Add(1, now.Add(1200*time.Millisecond)); err != nil {
		t.Fatalf("Add() duplicate error = %v", err)
	}
	dq.checkExpired(now.Add(1300 * time.Millisecond))
	assertNoTimerID(t, ch)

	if err := dq.Add(1, now.Add(2200*time.Millisecond)); err != nil {
		t.Fatalf("Add() after window error = %v", err)
	}
	dq.checkExpired(now.Add(2300 * time.Millisecond))
	waitForTimerID(t, ch, 1)
}

func TestDelayQueue_RescheduleClearsDedupWindow(t *testing.T) {
	ch := make(chan uint64, 2)
	dq := NewDelayQueue(func(_ context.Context, id uint64) {
		ch <- id
	}, WithDedupWindow(time.Minute))

	now := time.Now()
	if err := dq.Add(1, now.Add(time.Second)); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	dq.checkExpired(now.Add(1100 * time.Millisecond))
	waitForTimerID(t, ch, 1)

	if err := dq.Reschedule(1, now.Add(1200*time.Millisecond)); err != nil {
		t.Fatalf("Reschedule() error = %v", err)
	}
	dq.checkExpired(now.Add(1300 * time.Millisecond))
	waitForTimerID(t, ch, 1)
}

func TestDelayQueueStartAndStopAreIdempotent(t *testing.T) {
	dq := NewDelayQueue(nil)
	dq.Start()
	dq.Start()
	dq.Stop()
	dq.Stop()
}

func waitForTimerID(t *testing.T, ch <-chan uint64, want uint64) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("callback id = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for callback id %d", want)
	}
}

func assertNoTimerID(t *testing.T, ch <-chan uint64) {
	t.Helper()
	select {
	case got := <-ch:
		t.Fatalf("unexpected callback id %d", got)
	case <-time.After(100 * time.Millisecond):
	}
}
