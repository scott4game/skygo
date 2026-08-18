package tcppool

import (
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	t.Run("double", func(t *testing.T) {
		if got := nextBackoff(500*time.Millisecond, 30*time.Second, 0); got != 1000*time.Millisecond {
			t.Fatalf("nextBackoff(500ms) = %v, want 1000ms", got)
		}
	})
	t.Run("cap", func(t *testing.T) {
		if got := nextBackoff(16*time.Second, 30*time.Second, 0); got != 30*time.Second {
			t.Fatalf("nextBackoff(16s) = %v, want 30s cap", got)
		}
	})
	t.Run("jitter", func(t *testing.T) {
		got := nextBackoff(1*time.Second, 30*time.Second, 0.2)
		maxJitter := time.Duration(float64(2*time.Second) * 0.2)
		if got < 2*time.Second || got > 2*time.Second+maxJitter {
			t.Fatalf("nextBackoff(1s, jitter 0.2) = %v, want in [2s, 2s+maxJitter]", got)
		}
	})
}

func TestNextBackoff_Cap(t *testing.T) {
	// 1s * 2 = 2s, capped at 1500ms
	if got := nextBackoff(1*time.Second, 1500*time.Millisecond, 0); got != 1500*time.Millisecond {
		t.Fatalf("nextBackoff cap: got %v want 1500ms", got)
	}
}

func TestPoolConn_StopBeforeStart(t *testing.T) {
	opts := Options{}
	opts.setDefaults()
	p := &Pool{opts: opts}
	pc := newPoolConn(0, p)
	pc.stop()
	if pc.send([]byte("x")) {
		t.Fatal("send after stop should return false")
	}
}
