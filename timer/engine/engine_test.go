package engine

import (
	"context"
	"sync"
	"testing"
	"time"
)

type testTask struct {
	TaskID  int64
	TimerID uint64
	DueMs   int64
	UID     uint32
}

type testQueue struct {
	mu        sync.Mutex
	added     []testTask
	claimed   []uint64
	completed []uint64
	claim     bool
}

func (q *testQueue) Add(_ context.Context, task testTask) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.added = append(q.added, task)
	return nil
}

func (*testQueue) Remove(context.Context, uint32, int64) error { return nil }

func (*testQueue) LoadAllInWindow(context.Context, time.Duration) ([]testTask, error) {
	return nil, nil
}

func (q *testQueue) ClaimForProcessing(_ context.Context, _ uint32, timerID uint64, _ string, _ time.Duration) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.claimed = append(q.claimed, timerID)
	return q.claim, nil
}

func (q *testQueue) CompleteProcessing(_ context.Context, _ uint32, timerID uint64, _ string) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.completed = append(q.completed, timerID)
	return true, nil
}

func (*testQueue) RequeueExpiredProcessing(context.Context, time.Time) (int, error) {
	return 0, nil
}

func testAccessor() Accessor[testTask] {
	return Accessor[testTask]{
		TaskID:    func(task testTask) int64 { return task.TaskID },
		TimerID:   func(task testTask) uint64 { return task.TimerID },
		DueTimeMs: func(task testTask) int64 { return task.DueMs },
		UID:       func(task testTask) uint32 { return task.UID },
	}
}

func TestEngineClaimProcessCompletesAfterHandler(t *testing.T) {
	queue := &testQueue{claim: true}
	handled := make(chan testTask, 1)
	engine := New(queue, ModeClaimProcess, "test-owner", time.Second, time.Minute, nil, testAccessor(),
		func(_ context.Context, task testTask) { handled <- task })
	task := testTask{TaskID: 11, TimerID: 22, DueMs: time.Now().UnixMilli(), UID: 33}
	engine.doFire(context.Background(), task.TimerID, task, task.UID)

	select {
	case got := <-handled:
		if got != task {
			t.Fatalf("handler task = %#v, want %#v", got, task)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for handler")
	}
	queue.mu.Lock()
	defer queue.mu.Unlock()
	if len(queue.claimed) != 1 || queue.claimed[0] != task.TimerID {
		t.Fatalf("claimed = %v", queue.claimed)
	}
	if len(queue.completed) != 1 || queue.completed[0] != task.TimerID {
		t.Fatalf("completed = %v", queue.completed)
	}
}

func TestEngineFireSubmitDefersHandlerToOwner(t *testing.T) {
	handled := make(chan testTask, 1)
	engine := New[testTask](nil, ModeClaimProcess, "", 0, time.Minute, nil, testAccessor(),
		func(_ context.Context, task testTask) { handled <- task })
	task := testTask{TaskID: 44, TimerID: 55, DueMs: time.Now().UnixMilli(), UID: 66}
	if err := engine.Add(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	submitted := make(chan testTask, 1)
	engine.SetFireSubmit(func(_ context.Context, timerID uint64, got testTask, uid uint32) error {
		if timerID != task.TimerID || uid != task.UID {
			t.Errorf("submitted identity = (%d, %d), want (%d, %d)", timerID, uid, task.TimerID, task.UID)
		}
		submitted <- got
		return nil
	})
	engine.onDelayTimerFire(context.Background(), task.TimerID)

	select {
	case got := <-submitted:
		if got != task {
			t.Fatalf("submitted task = %#v, want %#v", got, task)
		}
		engine.HandleSubmittedFire(context.Background(), task.TimerID, got, task.UID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for submission")
	}
	select {
	case got := <-handled:
		if got != task {
			t.Fatalf("handled task = %#v, want %#v", got, task)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for submitted handler")
	}
}
