package actor

import (
	"context"
	"sync"
	"time"

	"github.com/scott4game/skygo/skylog"
)

// RetryConfig holds parameters for one retry level.
type RetryConfig struct {
	Interval time.Duration
	MaxTries int
}

type retryTask struct {
	label string
	fn    func() error
	tries int
}

// DispatchRetrier retries failed dispatch calls with multi-level backoff.
// Each level has its own interval and max-tries. On exhausting a level the task
// is promoted to the next level; after the last level it is dropped with an error log.
//
// Typical usage:
//
//	r := actor.NewDispatchRetrier([]actor.RetryConfig{
//	    {1 * time.Second, 3},
//	    {3 * time.Second, 3},
//	    {5 * time.Second, 3},
//	}, []int{512, 256, 128})
//	r.Start()
//	defer r.Stop()
type DispatchRetrier struct {
	levels   []RetryConfig
	queues   []chan retryTask
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewDispatchRetrier creates a DispatchRetrier. caps[i] sets the buffer size for
// level i (defaults to 256 if omitted).
func NewDispatchRetrier(levels []RetryConfig, caps []int) *DispatchRetrier {
	if len(levels) == 0 {
		panic("actor: DispatchRetrier requires at least one level")
	}
	queues := make([]chan retryTask, len(levels))
	for i := range levels {
		cap_ := 256
		if i < len(caps) {
			cap_ = caps[i]
		}
		queues[i] = make(chan retryTask, cap_)
	}
	return &DispatchRetrier{
		levels: levels,
		queues: queues,
		stopCh: make(chan struct{}),
	}
}

// Start launches one goroutine per level. Call once at service startup.
func (r *DispatchRetrier) Start() {
	for i := range r.levels {
		go r.runLevel(i)
	}
}

// Stop signals all goroutines to exit. Safe to call multiple times.
func (r *DispatchRetrier) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// Enqueue places a task into the level-0 queue (shortest interval).
// If the queue is full the task is dropped immediately and logged.
func (r *DispatchRetrier) Enqueue(label string, fn func() error) {
	task := retryTask{label: label, fn: fn, tries: r.levels[0].MaxTries}
	select {
	case r.queues[0] <- task:
	default:
		skylog.Errorf(context.Background(), "[DispatchRetrier] level-1 queue full, task dropped immediately label=%s", label)
	}
}

func (r *DispatchRetrier) runLevel(level int) {
	ticker := time.NewTicker(r.levels[level].Interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			r.processBatch(level)
		}
	}
}

// processBatch processes a snapshot of the queue at tick time. Snapshotting the
// length prevents a burst of re-enqueued failures from starving the ticker.
func (r *DispatchRetrier) processBatch(level int) {
	n := len(r.queues[level])
	for i := 0; i < n; i++ {
		var task retryTask
		select {
		case task = <-r.queues[level]:
		default:
			return
		}

		if err := task.fn(); err == nil {
			continue
		}

		task.tries--
		if task.tries > 0 {
			skylog.Warnf(context.Background(), "[DispatchRetrier] level-%d retry failed label=%s tries_left=%d err will retry",
				level+1, task.label, task.tries)
			select {
			case r.queues[level] <- task:
			default:
				skylog.Errorf(context.Background(), "[DispatchRetrier] level-%d queue full on re-enqueue, task dropped label=%s",
					level+1, task.label)
			}
			continue
		}

		nextLevel := level + 1
		if nextLevel >= len(r.levels) {
			skylog.Errorf(context.Background(), "[DispatchRetrier] all retries exhausted label=%s, task dropped", task.label)
			continue
		}

		promoted := retryTask{label: task.label, fn: task.fn, tries: r.levels[nextLevel].MaxTries}
		skylog.Warnf(context.Background(), "[DispatchRetrier] level-%d exhausted label=%s, promoting to level-%d (interval=%v)",
			level+1, task.label, nextLevel+1, r.levels[nextLevel].Interval)
		select {
		case r.queues[nextLevel] <- promoted:
		default:
			skylog.Errorf(context.Background(), "[DispatchRetrier] level-%d queue full, promotion dropped label=%s",
				nextLevel+1, task.label)
		}
	}
}
