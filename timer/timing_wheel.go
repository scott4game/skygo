package timer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scott4game/skygo/skylog"
)

const (
	defaultCallbackTimeout = 3 * time.Second
	wheelSlots             = 3600
)

// ErrTimerOutOfRange reports a deadline beyond the one-hour wheel span.
var ErrTimerOutOfRange = errors.New("timer expire time exceeds timing wheel range")

// TimingWheelOptions configures callback execution, deduplication, and metrics logging.
type TimingWheelOptions struct {
	CallbackTimeout       time.Duration
	MetricsReportInterval time.Duration
	DedupWindow           time.Duration
}

// TimingWheelMetrics is a snapshot of callback activity.
type TimingWheelMetrics struct {
	TasksSubmitted      uint64
	TasksExecuted       uint64
	TasksExecutionError uint64
}

// TimingWheel schedules callbacks within a fixed one-hour, one-second-resolution span.
type TimingWheel struct {
	slots     [wheelSlots][]uint64
	current   int
	callback  func(context.Context, uint64)
	stopCh    chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	mu        sync.RWMutex

	callbackTimeout       time.Duration
	metricsReportInterval time.Duration
	metrics               TimingWheelMetrics

	dedupWindow time.Duration
	dedupMu     sync.Mutex
	lastFired   map[uint64]time.Time
}

// NewTimingWheel creates a timing wheel. Callbacks run asynchronously with a
// bounded context; route the callback into an actor with actor.Send when mailbox
// serialization is required.
func NewTimingWheel(callback func(context.Context, uint64), opts TimingWheelOptions) *TimingWheel {
	if opts.CallbackTimeout <= 0 {
		opts.CallbackTimeout = defaultCallbackTimeout
	}
	return &TimingWheel{
		callback:              callback,
		stopCh:                make(chan struct{}),
		callbackTimeout:       opts.CallbackTimeout,
		metricsReportInterval: opts.MetricsReportInterval,
		dedupWindow:           opts.DedupWindow,
		lastFired:             make(map[uint64]time.Time),
	}
}

// SetDedupWindow changes the duplicate-suppression window and clears its history.
func (tw *TimingWheel) SetDedupWindow(d time.Duration) {
	tw.dedupMu.Lock()
	defer tw.dedupMu.Unlock()
	tw.dedupWindow = d
	tw.lastFired = make(map[uint64]time.Time)
}

func slotsAhead(delay time.Duration) int {
	if delay <= 0 {
		return 0
	}
	n := int(delay / time.Second)
	if n == 0 {
		return 1
	}
	return n
}

// Add schedules id, replacing an existing schedule for the same id.
func (tw *TimingWheel) Add(id uint64, expireTime time.Time) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	delay := time.Until(expireTime)
	if delay <= 0 {
		if tw.callback != nil && !tw.isDeduped(id, time.Now()) {
			tw.submitTask(id)
		}
		return nil
	}
	if delay > time.Hour {
		return ErrTimerOutOfRange
	}
	tw.removeFromSlotsLocked(id)
	slot := (tw.current + slotsAhead(delay)) % wheelSlots
	tw.slots[slot] = append(tw.slots[slot], id)
	return nil
}

// Remove cancels id if it is scheduled.
func (tw *TimingWheel) Remove(id uint64) error {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	tw.clearLastFired(id)
	tw.removeFromSlotsLocked(id)
	return nil
}

// Reschedule atomically replaces id's deadline.
func (tw *TimingWheel) Reschedule(id uint64, expireTime time.Time) error {
	tw.clearLastFired(id)
	return tw.Add(id, expireTime)
}

func (tw *TimingWheel) removeFromSlotsLocked(id uint64) {
	for i := range tw.slots {
		for j, taskID := range tw.slots[i] {
			if taskID == id {
				tw.slots[i] = append(tw.slots[i][:j], tw.slots[i][j+1:]...)
				return
			}
		}
	}
}

// Start starts the wheel. Repeated calls are safe.
func (tw *TimingWheel) Start() {
	tw.startOnce.Do(func() {
		go tw.run()
		if tw.metricsReportInterval > 0 {
			go tw.runMetricsReporter()
		}
	})
}

// Stop releases wheel resources. It is safe to call more than once.
func (tw *TimingWheel) Stop() {
	tw.stopOnce.Do(func() {
		close(tw.stopCh)
	})
}

func (tw *TimingWheel) run() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-tw.stopCh:
			return
		case <-ticker.C:
			tw.tick()
		}
	}
}

func (tw *TimingWheel) runMetricsReporter() {
	ticker := time.NewTicker(tw.metricsReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-tw.stopCh:
			return
		case <-ticker.C:
			m := tw.GetMetrics()
			if m.TasksSubmitted > 0 {
				skylog.Infof(context.Background(), "timer: submitted=%d executed=%d errors=%d",
					m.TasksSubmitted, m.TasksExecuted, m.TasksExecutionError)
			}
		}
	}
}

func (tw *TimingWheel) tick() {
	tw.mu.Lock()
	tw.current = (tw.current + 1) % wheelSlots
	tasks := tw.slots[tw.current]
	tw.slots[tw.current] = nil
	tw.mu.Unlock()

	now := time.Now()
	for _, id := range tasks {
		if tw.callback != nil && !tw.isDeduped(id, now) {
			tw.submitTask(id)
		}
	}
	tw.cleanLastFired(now)
}

func (tw *TimingWheel) submitTask(id uint64) {
	atomic.AddUint64(&tw.metrics.TasksSubmitted, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), tw.callbackTimeout)
		defer cancel()
		defer func() {
			if recovered := recover(); recovered != nil {
				atomic.AddUint64(&tw.metrics.TasksExecutionError, 1)
				skylog.Errorf(ctx, "timer: callback panic id=%d: %v", id, recovered)
				return
			}
			atomic.AddUint64(&tw.metrics.TasksExecuted, 1)
		}()
		tw.callback(ctx, id)
	}()
}

func (tw *TimingWheel) clearLastFired(id uint64) {
	tw.dedupMu.Lock()
	defer tw.dedupMu.Unlock()
	delete(tw.lastFired, id)
}

func (tw *TimingWheel) isDeduped(id uint64, now time.Time) bool {
	tw.dedupMu.Lock()
	defer tw.dedupMu.Unlock()
	if tw.dedupWindow <= 0 {
		return false
	}
	if last, ok := tw.lastFired[id]; ok && now.Sub(last) < tw.dedupWindow {
		return true
	}
	tw.lastFired[id] = now
	return false
}

func (tw *TimingWheel) cleanLastFired(now time.Time) {
	tw.dedupMu.Lock()
	defer tw.dedupMu.Unlock()
	if tw.dedupWindow <= 0 {
		return
	}
	for id, firedAt := range tw.lastFired {
		if now.Sub(firedAt) >= tw.dedupWindow {
			delete(tw.lastFired, id)
		}
	}
}

// GetMetrics returns an atomic metrics snapshot.
func (tw *TimingWheel) GetMetrics() TimingWheelMetrics {
	return TimingWheelMetrics{
		TasksSubmitted:      atomic.LoadUint64(&tw.metrics.TasksSubmitted),
		TasksExecuted:       atomic.LoadUint64(&tw.metrics.TasksExecuted),
		TasksExecutionError: atomic.LoadUint64(&tw.metrics.TasksExecutionError),
	}
}
