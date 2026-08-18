//go:build stress

// Package stresstest provides shared watchdog and leak-check primitives for
// build-tagged stress tests.
package stresstest

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// DefaultLeakTolerance allows for bounded runtime and testing goroutine noise.
const DefaultLeakTolerance = 10

// Guard runs fn with a cancellable context. On timeout it captures all stacks
// before cancellation, preserving the stalled state for the failure report.
func Guard(t testing.TB, name string, timeout time.Duration, fn func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-timer.C:
		stacks := allStacks()
		cancel()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
		}
		t.Fatalf("%s stalled after %s\n%s", name, timeout, stacks)
	}
}

// Wait receives one value or fails with a full goroutine dump.
func Wait[T any](t testing.TB, name string, timeout time.Duration, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(timeout):
		stacks := allStacks()
		t.Fatalf("%s stalled after %s\n%s", name, timeout, stacks)
		var zero T
		return zero
	}
}

// Eventually polls predicate at interval and fails with a goroutine dump when
// it does not become true before timeout.
func Eventually(t testing.TB, name string, timeout, interval time.Duration, predicate func() bool) {
	t.Helper()
	if predicate() {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if predicate() {
				return
			}
		case <-timer.C:
			t.Fatalf("%s did not become true within %s\n%s", name, timeout, allStacks())
		}
	}
}

// Poll reports whether predicate becomes true before timeout.
func Poll(timeout, interval time.Duration, predicate func() bool) bool {
	if predicate() {
		return true
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			if predicate() {
				return true
			}
		case <-timer.C:
			return false
		}
	}
}

// LeakCheck captures a goroutine baseline and returns a deferred verifier.
func LeakCheck(t testing.TB, tolerance int) func() {
	t.Helper()
	if tolerance < 0 {
		tolerance = 0
	}
	runtime.GC()
	baseline := runtime.NumGoroutine()
	return func() {
		deadline := time.Now().Add(3 * time.Second)
		for {
			runtime.GC()
			after := runtime.NumGoroutine()
			if after-baseline <= tolerance {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("possible goroutine leak: baseline=%d after=%d delta=%d tolerance=%d\n%s",
					baseline, after, after-baseline, tolerance, allStacks())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// Scale returns SKYGO_STRESS_SCALE, defaulting to one for local runs.
func Scale(t testing.TB) int {
	t.Helper()
	value := os.Getenv("SKYGO_STRESS_SCALE")
	if value == "" {
		t.Log("SKYGO_STRESS_SCALE=1")
		return 1
	}
	scale, err := strconv.Atoi(value)
	if err != nil || scale < 1 || scale > 1000 {
		t.Fatalf("invalid SKYGO_STRESS_SCALE %q: want integer in [1,1000]", value)
	}
	t.Logf("SKYGO_STRESS_SCALE=%d", scale)
	return scale
}

func allStacks() []byte {
	buf := make([]byte, 4<<20)
	n := runtime.Stack(buf, true)
	return buf[:n]
}
