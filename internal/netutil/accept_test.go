package netutil

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type errorListener struct {
	mu    sync.Mutex
	times []time.Time
}

func (l *errorListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	l.times = append(l.times, time.Now())
	l.mu.Unlock()
	return nil, errors.New("accept failed")
}

func (*errorListener) Close() error   { return nil }
func (*errorListener) Addr() net.Addr { return testAddr("listener") }

func (l *errorListener) callTimes() []time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]time.Time(nil), l.times...)
}

type testAddr string

func (a testAddr) Network() string { return "test" }
func (a testAddr) String() string  { return string(a) }

func TestAcceptBacksOffAndStopsPromptly(t *testing.T) {
	listener := &errorListener{}
	stopping := make(chan struct{})
	done := make(chan struct{})
	observed := make(chan time.Duration, 4)
	go func() {
		defer close(done)
		_, _ = Accept(listener, stopping, func(_ error, delay time.Duration) { observed <- delay })
	}()

	for _, want := range []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond} {
		select {
		case got := <-observed:
			if got != want {
				t.Fatalf("retry delay=%v, want %v", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("Accept did not retry")
		}
	}
	times := listener.callTimes()
	if len(times) < 3 || times[1].Sub(times[0]) < 4*time.Millisecond || times[2].Sub(times[1]) < 9*time.Millisecond {
		t.Fatalf("Accept retried without backoff: %v", times)
	}

	close(stopping)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Accept did not stop during backoff")
	}
}

func TestNextAcceptDelayCapsAtOneSecond(t *testing.T) {
	delay := time.Duration(0)
	for i := 0; i < 20; i++ {
		delay = nextAcceptDelay(delay)
	}
	if delay != time.Second {
		t.Fatalf("delay=%v, want 1s", delay)
	}
}
