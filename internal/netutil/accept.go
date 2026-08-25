// Package netutil contains shared network lifecycle helpers.
package netutil

import (
	"net"
	"time"
)

const (
	initialAcceptDelay = 5 * time.Millisecond
	maxAcceptDelay     = time.Second
)

// Accept retries listener errors with bounded exponential backoff. The boolean
// result is false when stopping is closed before a connection is accepted.
func Accept(listener net.Listener, stopping <-chan struct{}, onError func(error, time.Duration)) (net.Conn, bool) {
	var delay time.Duration
	for {
		conn, err := listener.Accept()
		if err == nil {
			return conn, true
		}
		select {
		case <-stopping:
			return nil, false
		default:
		}

		delay = nextAcceptDelay(delay)
		if onError != nil {
			onError(err, delay)
		}
		timer := time.NewTimer(delay)
		select {
		case <-stopping:
			timer.Stop()
			return nil, false
		case <-timer.C:
		}
	}
}

func nextAcceptDelay(current time.Duration) time.Duration {
	if current == 0 {
		return initialAcceptDelay
	}
	if current >= maxAcceptDelay/2 {
		return maxAcceptDelay
	}
	return current * 2
}
