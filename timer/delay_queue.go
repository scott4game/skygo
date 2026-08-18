package timer

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Task is an item scheduled for future execution.
type Task struct {
	ID         uint64
	ExpireTime time.Time
	Callback   func(uint64)
}

// DelayQueue schedules tasks with a min-heap and an ID-to-index map.
type DelayQueue struct {
	h           *indexedTaskHeap
	mu          sync.Mutex
	callback    func(context.Context, uint64)
	stopCh      chan struct{}
	dedupWindow time.Duration
	lastFired   map[uint64]time.Time
	startOnce   sync.Once
	stopOnce    sync.Once
}

type DelayQueueOption func(*DelayQueue)

func WithDedupWindow(d time.Duration) DelayQueueOption {
	return func(dq *DelayQueue) {
		dq.SetDedupWindow(d)
	}
}

// NewDelayQueue creates a delay queue.
func NewDelayQueue(callback func(context.Context, uint64), opts ...DelayQueueOption) *DelayQueue {
	dq := &DelayQueue{
		h:         newIndexedTaskHeap(),
		callback:  callback,
		stopCh:    make(chan struct{}),
		lastFired: make(map[uint64]time.Time),
	}
	for _, opt := range opts {
		opt(dq)
	}
	return dq
}

func (dq *DelayQueue) SetDedupWindow(d time.Duration) {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	dq.dedupWindow = d
	if d <= 0 {
		dq.lastFired = nil
		return
	}
	dq.lastFired = make(map[uint64]time.Time)
}

// Add inserts a task or updates an existing task in O(log n).
func (dq *DelayQueue) Add(id uint64, expireTime time.Time) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	if idx, ok := dq.h.index[id]; ok {
		dq.h.tasks[idx].ExpireTime = expireTime
		heap.Fix(dq.h, idx)
		return nil
	}
	heap.Push(dq.h, &Task{ID: id, ExpireTime: expireTime})
	return nil
}

// Remove removes a task in O(log n).
func (dq *DelayQueue) Remove(id uint64) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	dq.clearLastFiredLocked(id)
	idx, ok := dq.h.index[id]
	if !ok {
		return nil
	}
	heap.Remove(dq.h, idx)
	return nil
}

// Reschedule atomically updates a task, inserting it when it does not exist.
func (dq *DelayQueue) Reschedule(id uint64, newExpireTime time.Time) error {
	dq.mu.Lock()
	defer dq.mu.Unlock()
	dq.clearLastFiredLocked(id)
	idx, ok := dq.h.index[id]
	if !ok {
		heap.Push(dq.h, &Task{ID: id, ExpireTime: newExpireTime})
		return nil
	}
	dq.h.tasks[idx].ExpireTime = newExpireTime
	heap.Fix(dq.h, idx)
	return nil
}

// Start begins processing. Repeated calls are safe.
func (dq *DelayQueue) Start() {
	dq.startOnce.Do(func() { go dq.run() })
}

// Stop ends processing. Repeated calls are safe.
func (dq *DelayQueue) Stop() {
	dq.stopOnce.Do(func() { close(dq.stopCh) })
}

func (dq *DelayQueue) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-dq.stopCh:
			return
		case now := <-ticker.C:
			dq.checkExpired(now)
		}
	}
}

func (dq *DelayQueue) checkExpired(now time.Time) {
	dq.mu.Lock()
	defer dq.mu.Unlock()

	ctx := context.Background()

	for dq.h.Len() > 0 {
		task := dq.h.tasks[0]
		if task.ExpireTime.After(now) {
			break
		}
		heap.Pop(dq.h)
		if dq.isDedupedLocked(task.ID, now) {
			continue
		}
		if dq.callback != nil {
			go dq.callback(ctx, task.ID)
		}
	}
	dq.cleanLastFiredLocked(now)
}

func (dq *DelayQueue) clearLastFiredLocked(id uint64) {
	if dq.lastFired != nil {
		delete(dq.lastFired, id)
	}
}

func (dq *DelayQueue) isDedupedLocked(id uint64, now time.Time) bool {
	if dq.dedupWindow <= 0 {
		return false
	}
	if dq.lastFired == nil {
		dq.lastFired = make(map[uint64]time.Time)
	}
	if last, ok := dq.lastFired[id]; ok && now.Sub(last) < dq.dedupWindow {
		return true
	}
	dq.lastFired[id] = now
	return false
}

func (dq *DelayQueue) cleanLastFiredLocked(now time.Time) {
	if dq.dedupWindow <= 0 || dq.lastFired == nil {
		return
	}
	for id, firedAt := range dq.lastFired {
		if now.Sub(firedAt) >= dq.dedupWindow {
			delete(dq.lastFired, id)
		}
	}
}

// indexedTaskHeap maintains an ID-to-index map alongside the heap.
type indexedTaskHeap struct {
	tasks []*Task
	index map[uint64]int
}

func newIndexedTaskHeap() *indexedTaskHeap {
	h := &indexedTaskHeap{
		index: make(map[uint64]int),
	}
	heap.Init(h)
	return h
}

func (h *indexedTaskHeap) Len() int { return len(h.tasks) }

func (h *indexedTaskHeap) Less(i, j int) bool {
	return h.tasks[i].ExpireTime.Before(h.tasks[j].ExpireTime)
}

// Swap exchanges two elements and updates their indexes.
func (h *indexedTaskHeap) Swap(i, j int) {
	h.tasks[i], h.tasks[j] = h.tasks[j], h.tasks[i]
	h.index[h.tasks[i].ID] = i
	h.index[h.tasks[j].ID] = j
}

// Push appends an element and records its index.
func (h *indexedTaskHeap) Push(x interface{}) {
	task := x.(*Task)
	h.index[task.ID] = len(h.tasks)
	h.tasks = append(h.tasks, task)
}

// Pop removes the final heap element and its index entry.
func (h *indexedTaskHeap) Pop() interface{} {
	old := h.tasks
	n := len(old)
	task := old[n-1]
	old[n-1] = nil
	h.tasks = old[:n-1]
	delete(h.index, task.ID)
	return task
}
