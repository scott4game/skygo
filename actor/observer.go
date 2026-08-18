package actor

import (
	"context"
	"time"

	"github.com/scott4game/skygo/skylog"
)

// CallEvent describes one completed synchronous protocol call.
type CallEvent struct {
	Caller   string
	Callee   string
	Protocol string
	Duration time.Duration
	Err      error
}

// Observer receives actor runtime events. Implementations must return quickly.
type Observer interface {
	OnCall(CallEvent)
}

func (s *System) observeCall(event CallEvent) {
	if s == nil || s.observer == nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			skylog.Errorf(context.Background(), "actor: observer panic: %v", recovered)
		}
	}()
	s.observer.OnCall(event)
}
