package actor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainFinishesAcceptedWorkAndInternalCalls(t *testing.T) {
	system := NewSystem(SystemOptions{Drainable: true})
	defer stopTestSystem(t, system)
	worker, ref := newTestService(t, system, "worker")
	other, otherRef := newTestService(t, system, "other")
	started := make(chan struct{})
	release := make(chan struct{})
	var count atomic.Int32
	mustHandle(t, other, "inc", func(context.Context, []any) (any, error) { count.Add(1); return nil, nil })
	mustHandle(t, worker, "work", func(ctx context.Context, _ []any) (any, error) {
		close(started)
		<-release
		return Call(ctx, otherRef, "inc")
	})
	startTestService(t, other)
	startTestService(t, worker)
	call := make(chan error, 1)
	go func() { _, err := Call(context.Background(), ref, "work"); call <- err }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := system.Drain(ctx); done <- err }()
	deadline := time.Now().Add(time.Second)
	for !system.DrainState().AdmissionClosed && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := Call(ctx, otherRef, "inc"); !errors.Is(err, ErrMaintenance) {
		t.Fatalf("new root admitted: %v", err)
	}
	close(release)
	if err := <-call; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if count.Load() != 1 {
		t.Fatal(count.Load())
	}
	system.ResumeAdmission()
	if _, err := Call(ctx, otherRef, "inc"); err != nil {
		t.Fatal(err)
	}
}
func TestDrainRejectsExpiredActivationContext(t *testing.T) {
	system := NewSystem(SystemOptions{Drainable: true})
	defer stopTestSystem(t, system)
	worker, ref := newTestService(t, system, "worker")
	var captured context.Context
	mustHandle(t, worker, "capture", func(ctx context.Context, _ []any) (any, error) { captured = ctx; return nil, nil })
	startTestService(t, worker)
	if _, err := Call(context.Background(), ref, "capture"); err != nil {
		t.Fatal(err)
	}
	if _, err := system.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := Call(captured, ref, "capture"); err == nil {
		t.Fatal("expired activation admitted", err)
	}
	if err := system.MaintenanceWork(context.Background(), func(ctx context.Context) error { _, err := Call(ctx, ref, "capture"); return err }); err != nil {
		t.Fatal(err)
	}
}
func TestDrainDoesNotPretendTimeoutIsSuccess(t *testing.T) {
	system := NewSystem(SystemOptions{Drainable: true})
	defer stopTestSystem(t, system)
	worker, ref := newTestService(t, system, "worker")
	started := make(chan struct{})
	release := make(chan struct{})
	mustHandle(t, worker, "wait", func(context.Context, []any) (any, error) { close(started); <-release; return nil, nil })
	startTestService(t, worker)
	done := make(chan struct{})
	go func() { Call(context.Background(), ref, "wait"); close(done) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	state, err := system.Drain(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || state.InFlight != 1 {
		t.Fatal(state, err)
	}
	close(release)
	<-done
	if _, err := system.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncReservationKeepsDrainOpenUntilHandoff(t *testing.T) {
	system := NewSystem(SystemOptions{Drainable: true})
	defer stopTestSystem(t, system)
	worker, ref := newTestService(t, system, "reserved")
	mustHandle(t, worker, "work", func(context.Context, []any) (any, error) { return nil, nil })
	startTestService(t, worker)
	reservation, err := system.ReserveWork()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := system.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	permit := reservation.Context(context.Background())
	if _, err := Call(permit, ref, "work"); err != nil {
		t.Fatal(err)
	}
	reservation.Finish(nil)
	reservation.Finish(nil)
	if _, err := system.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := Call(permit, ref, "work"); !errors.Is(err, ErrMaintenance) {
		t.Fatal("expired permit", err)
	}
	if _, err := system.ReserveWork(); !errors.Is(err, ErrMaintenance) {
		t.Fatal(err)
	}
}

func TestMaintenancePermitCannotEscapeCallback(t *testing.T) {
	system := NewSystem(SystemOptions{Drainable: true})
	defer stopTestSystem(t, system)
	worker, ref := newTestService(t, system, "permit")
	mustHandle(t, worker, "work", func(context.Context, []any) (any, error) { return nil, nil })
	startTestService(t, worker)
	system.Drain(context.Background())
	var captured context.Context
	if err := system.MaintenanceWork(context.Background(), func(ctx context.Context) error { captured = ctx; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := Call(captured, ref, "work"); !errors.Is(err, ErrMaintenance) {
		t.Fatal("escaped permit", err)
	}
}
