package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/scott4game/skygo/skylog"
	"github.com/scott4game/skygo/timer"
)

// Queue is the persistence contract required by Engine.
type Queue[T any] interface {
	Add(ctx context.Context, rec T) error
	Remove(ctx context.Context, uid uint32, taskID int64) error
	LoadAllInWindow(ctx context.Context, window time.Duration) ([]T, error)
	ClaimForProcessing(ctx context.Context, uid uint32, timerID uint64, owner string, lease time.Duration) (bool, error)
	CompleteProcessing(ctx context.Context, uid uint32, timerID uint64, owner string) (bool, error)
	RequeueExpiredProcessing(ctx context.Context, now time.Time) (int, error)
}

// Accessor extracts timer-relevant fields from a task record without reflection.
type Accessor[T any] struct {
	TaskID    func(T) int64
	TimerID   func(T) uint64
	DueTimeMs func(T) int64
	UID       func(T) uint32
}

// Mode controls how the engine manages the Redis lifecycle when a timer fires.
type Mode int

const (
	// ModeDirectFire: fire → Remove(Redis) → onFire.
	// Simple, no crash recovery around the callback.
	// ModeDirectFire Mode = iota

	// ModeClaimProcess: fire → ClaimForProcessing → onFire → CompleteProcessing.
	// If the server crashes between Claim and Complete, the task stays in
	// Processing state and is requeued on the next promotionLoop tick.
	ModeClaimProcess Mode = iota
)

// FireHandler is called after the Engine has managed the Redis lifecycle.
// Its only job is to notify the owner that the task is due.
// It must NOT perform any Redis operations.
type FireHandler[T any] func(ctx context.Context, task T)

const (
	defaultHotWindow       = 30 * time.Minute
	defaultProcessingLease = 10 * time.Second
	defaultRestoreTimeout  = 5 * time.Second
)

// Engine manages in-memory timer state (DelayQueue, bidirectional maps),
// the Redis lifecycle (Claim/Complete or direct Remove), and the promotionLoop.
// Each business module provides only a Queue, Accessor, and FireHandler.
type Engine[T any] struct {
	mu              sync.Mutex
	hotWindow       time.Duration
	idGen           *timer.IDGenerator
	inMem           map[uint64]T     // timerID → task snapshot (hot-zone only)
	byTask          map[int64]uint64 // taskID → timerID (all tasks, for EnsureTimerID)
	queue           Queue[T]
	accessor        Accessor[T]
	mode            Mode
	owner           string
	processingLease time.Duration
	handler         FireHandler[T]
	delayQ          *timer.DelayQueue
	stopCh          chan struct{}
	stopOnce        sync.Once
	fireSubmit      func(context.Context, uint64, T, uint32) error
}

// New creates an Engine. queue may be nil (offline/test mode).
func New[T any](
	queue Queue[T],
	mode Mode,
	owner string,
	processingLease time.Duration,
	hotWindow time.Duration,
	idGen *timer.IDGenerator,
	accessor Accessor[T],
	onFire FireHandler[T],
) *Engine[T] {
	if hotWindow <= 0 {
		hotWindow = defaultHotWindow
	}
	if processingLease <= 0 {
		processingLease = defaultProcessingLease
	}
	if idGen == nil {
		idGen = timer.NewIDGenerator(0)
	}
	e := &Engine[T]{
		hotWindow:       hotWindow,
		idGen:           idGen,
		inMem:           make(map[uint64]T),
		byTask:          make(map[int64]uint64),
		queue:           queue,
		accessor:        accessor,
		mode:            mode,
		owner:           owner,
		processingLease: processingLease,
		handler:         onFire,
		stopCh:          make(chan struct{}),
	}
	e.delayQ = timer.NewDelayQueue(e.onDelayTimerFire)
	return e
}

func (e *Engine[T]) Start() {
	e.delayQ.Start()
	if e.queue != nil {
		go e.promotionLoop()
	}
}

func (e *Engine[T]) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.delayQ.Stop()
	})
}

// SetFireSubmit routes a concrete timer record to an owner mailbox without
// transferring a Go closure across the actor boundary.
func (e *Engine[T]) SetFireSubmit(submit func(context.Context, uint64, T, uint32) error) {
	e.fireSubmit = submit
}

// HandleSubmittedFire completes a record previously emitted through
// SetFireSubmit. Owners call it from their mailbox handler.
func (e *Engine[T]) HandleSubmittedFire(ctx context.Context, timerID uint64, task T, uid uint32) {
	e.doFire(ctx, timerID, task, uid)
}

func (e *Engine[T]) SetDedupWindow(d time.Duration) {
	e.delayQ.SetDedupWindow(d)
}

// RestoreHotZone loads tasks from Redis and registers them in the in-memory
// DelayQueue. Called at startup and indirectly via promotionLoop.
func (e *Engine[T]) RestoreHotZone(ctx context.Context) error {
	if e.queue == nil {
		return nil
	}
	records, err := e.queue.LoadAllInWindow(ctx, e.hotWindow)
	if err != nil {
		return fmt.Errorf("engine: load hot zone: %w", err)
	}
	if n := e.promoteRecords(ctx, records); n > 0 {
		skylog.Infof(ctx, "Engine.RestoreHotZone: promoted %d tasks", n)
	}
	return nil
}

// EnsureTimerID returns the existing timerID for taskID if already tracked,
// otherwise allocates a new one. Idempotent across restarts.
func (e *Engine[T]) EnsureTimerID(taskID int64) (uint64, error) {
	e.mu.Lock()
	if timerID := e.byTask[taskID]; timerID != 0 {
		e.mu.Unlock()
		return timerID, nil
	}
	e.mu.Unlock()
	return e.idGen.Next()
}

// Add writes the task to Redis and registers it in-memory if within hotWindow.
// The caller must set task's TimerID (via EnsureTimerID) before calling Add.
func (e *Engine[T]) Add(ctx context.Context, task T) error {
	timerID := e.accessor.TimerID(task)
	taskID := e.accessor.TaskID(task)
	dueMs := e.accessor.DueTimeMs(task)

	if e.queue != nil {
		if err := e.queue.Add(ctx, task); err != nil {
			return err
		}
	}

	if e.inHotWindow(dueMs) {
		e.mu.Lock()
		if _, exists := e.inMem[timerID]; exists {
			skylog.Warnf(ctx, "timer engine: Add overwrite timerID=%d taskID=%d uid=%d", timerID, taskID, e.accessor.UID(task))
		}
		e.inMem[timerID] = task
		e.byTask[taskID] = timerID
		e.mu.Unlock()
		_ = e.delayQ.Add(timerID, time.UnixMilli(dueMs))
	} else {
		e.mu.Lock()
		e.byTask[taskID] = timerID
		e.mu.Unlock()
	}
	return nil
}

// Cancel removes the task from in-memory maps, the DelayQueue, and Redis.
func (e *Engine[T]) Cancel(ctx context.Context, uid uint32, taskID int64) error {
	timerID := e.forgetTask(taskID)
	if timerID != 0 {
		_ = e.delayQ.Remove(timerID)
	}
	if e.queue == nil {
		return nil
	}
	return e.queue.Remove(ctx, uid, taskID)
}

// Forget removes the task from in-memory maps only.
// Use when persistence removal is managed externally.
func (e *Engine[T]) Forget(taskID int64) {
	e.forgetTask(taskID)
}

// Reschedule updates the task's due time in Redis and re-registers the in-memory timer.
func (e *Engine[T]) Reschedule(ctx context.Context, task T) error {
	timerID := e.accessor.TimerID(task)
	taskID := e.accessor.TaskID(task)
	dueMs := e.accessor.DueTimeMs(task)

	if e.queue != nil {
		if err := e.queue.Add(ctx, task); err != nil {
			return err
		}
	}

	if e.inHotWindow(dueMs) {
		e.mu.Lock()
		e.inMem[timerID] = task
		e.byTask[taskID] = timerID
		e.mu.Unlock()
		_ = e.delayQ.Reschedule(timerID, time.UnixMilli(dueMs))
	} else {
		_ = e.delayQ.Remove(timerID)
		e.mu.Lock()
		delete(e.inMem, timerID)
		e.byTask[taskID] = timerID
		e.mu.Unlock()
	}
	return nil
}

func (e *Engine[T]) inHotWindow(dueMs int64) bool {
	return e.queue == nil || time.Until(time.UnixMilli(dueMs)) <= e.hotWindow
}

func (e *Engine[T]) forgetTask(taskID int64) uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	timerID := e.byTask[taskID]
	delete(e.byTask, taskID)
	if timerID != 0 {
		delete(e.inMem, timerID)
	}
	return timerID
}

func (e *Engine[T]) onDelayTimerFire(ctx context.Context, timerID uint64) {
	e.mu.Lock()
	task, ok := e.inMem[timerID]
	if !ok {
		e.mu.Unlock()
		skylog.Debugf(ctx, "timer engine: timerID=%d not found", timerID)
		return
	}
	taskID := e.accessor.TaskID(task)
	uid := e.accessor.UID(task)
	skylog.Debugf(ctx, "timer engine: fire timerID=%d uid=%d taskID=%d", timerID, uid, taskID)
	delete(e.inMem, timerID)
	delete(e.byTask, taskID)
	e.mu.Unlock()

	if e.fireSubmit != nil {
		if err := e.fireSubmit(ctx, timerID, task, uid); err != nil {
			skylog.Errorf(ctx, "timer engine: fire submit failed timerID=%d uid=%d: %v", timerID, uid, err)
			return
		}
		skylog.Infof(ctx, "timer engine: fire submit success timerID=%d uid=%d", timerID, uid)
		return
	}

	go e.doFire(ctx, timerID, task, uid)
	skylog.Infof(ctx, "timer engine: async fire timerID=%d uid=%d", timerID, uid)
}

func (e *Engine[T]) doFire(ctx context.Context, timerID uint64, task T, uid uint32) {
	switch e.mode {
	case ModeClaimProcess:
		if e.queue != nil {
			claimCtx, cancel := context.WithTimeout(ctx, e.processingLease)
			defer cancel()
			claimed, err := e.queue.ClaimForProcessing(claimCtx, uid, timerID, e.owner, e.processingLease)
			if err != nil {
				skylog.Errorf(ctx, "Engine.Claim timerID=%d uid=%d: %v", timerID, uid, err)
				return
			}
			if !claimed {
				return
			}
		}
		e.handler(ctx, task)
		if e.queue != nil {
			complCtx, cancel := context.WithTimeout(ctx, e.processingLease)
			defer cancel()
			if _, err := e.queue.CompleteProcessing(complCtx, uid, timerID, e.owner); err != nil {
				skylog.Errorf(ctx, "Engine.Complete timerID=%d uid=%d: %v", timerID, uid, err)
			}
		}
	}
}

func (e *Engine[T]) promotionLoop() {
	interval := e.hotWindow / 10
	if interval < 30*time.Second {
		interval = 30 * time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stopCh:
			return
		case <-ticker.C:
			e.promoteFromRedis()
		}
	}
}

func (e *Engine[T]) promoteFromRedis() {
	ctx, cancel := context.WithTimeout(context.Background(), defaultRestoreTimeout)
	defer cancel()
	skylog.Infof(ctx, "Engine.promoteFromRedis: start")

	if e.mode == ModeClaimProcess {
		if n, err := e.queue.RequeueExpiredProcessing(ctx, time.Now()); err != nil {
			skylog.Errorf(ctx, "Engine.promoteFromRedis: requeue expired: %v", err)
		} else if n > 0 {
			skylog.Debugf(ctx, "Engine.promoteFromRedis: requeued %d expired tasks", n)
		}
	}

	records, err := e.queue.LoadAllInWindow(ctx, e.hotWindow)
	if err != nil {
		skylog.Errorf(ctx, "Engine.promoteFromRedis: load window: %v", err)
		return
	}
	if n := e.promoteRecords(ctx, records); n > 0 {
		skylog.Debugf(ctx, "Engine.promoteFromRedis: promoted %d tasks", n)
	}
}

func (e *Engine[T]) promoteRecords(ctx context.Context, records []T) int {
	now := time.Now()
	promoted := 0
	for _, rec := range records {
		timerID := e.accessor.TimerID(rec)
		if timerID == 0 {
			continue
		}
		taskID := e.accessor.TaskID(rec)
		dueMs := e.accessor.DueTimeMs(rec)

		e.mu.Lock()
		if _, exists := e.byTask[taskID]; exists {
			e.mu.Unlock()
			continue
		}
		e.inMem[timerID] = rec
		e.byTask[taskID] = timerID
		e.mu.Unlock()

		if time.UnixMilli(dueMs).Before(now) {
			go e.onDelayTimerFire(ctx, timerID)
		} else {
			_ = e.delayQ.Add(timerID, time.UnixMilli(dueMs))
		}
		promoted++
	}
	return promoted
}
