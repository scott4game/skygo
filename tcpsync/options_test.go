package tcpsync

import (
	"testing"
	"time"
)

func TestSetDefaults_MaxSizeRequestTimeoutRetryCount(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	if o.MaxSize != 100 {
		t.Fatalf("MaxSize: got %d want 100", o.MaxSize)
	}
	if o.RequestTimeout != 5*time.Second {
		t.Fatalf("RequestTimeout: got %v want 5s", o.RequestTimeout)
	}
	if o.RetryCount != 0 {
		t.Fatalf("RetryCount: got %d want 0", o.RetryCount)
	}

	o = &Options{MaxSize: -1, RequestTimeout: -1, RetryCount: -5}
	o.setDefaults()
	if o.MaxSize != 100 {
		t.Fatalf("MaxSize negative: got %d want 100", o.MaxSize)
	}
	if o.RequestTimeout != 5*time.Second {
		t.Fatalf("RequestTimeout negative: got %v want 5s", o.RequestTimeout)
	}
	if o.RetryCount != 0 {
		t.Fatalf("RetryCount negative: got %d want 0", o.RetryCount)
	}

	o = &Options{MaxSize: 10, RequestTimeout: time.Second, RetryCount: 2}
	o.setDefaults()
	if o.MaxSize != 10 || o.RequestTimeout != time.Second || o.RetryCount != 2 {
		t.Fatalf("preserved positive values: %+v", o)
	}
}

func TestSetDefaults_RetryDelays(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	want := []time.Duration{10 * time.Millisecond, 30 * time.Millisecond, 60 * time.Millisecond}
	if len(o.RetryDelays) != len(want) {
		t.Fatalf("len RetryDelays: got %d want %d", len(o.RetryDelays), len(want))
	}
	for i := range want {
		if o.RetryDelays[i] != want[i] {
			t.Fatalf("RetryDelays[%d]: got %v want %v", i, o.RetryDelays[i], want[i])
		}
	}

	custom := []time.Duration{5 * time.Millisecond, 15 * time.Millisecond}
	o = &Options{RetryDelays: custom}
	o.setDefaults()
	if len(o.RetryDelays) != 2 || o.RetryDelays[0] != custom[0] || o.RetryDelays[1] != custom[1] {
		t.Fatalf("custom RetryDelays: got %v", o.RetryDelays)
	}
}

func TestSetDefaults_HeartbeatRequestTimeout(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	if o.HeartbeatRequestTimeout != 3*time.Second {
		t.Fatalf("HeartbeatRequestTimeout: got %v want 3s", o.HeartbeatRequestTimeout)
	}
	o = &Options{HeartbeatRequestTimeout: 500 * time.Millisecond}
	o.setDefaults()
	if o.HeartbeatRequestTimeout != 500*time.Millisecond {
		t.Fatalf("HeartbeatRequestTimeout preserved: got %v", o.HeartbeatRequestTimeout)
	}
}

func TestSetDefaults_MaintenanceTick(t *testing.T) {
	t.Run("heartbeat", func(t *testing.T) {
		hb := 30 * time.Second
		o := &Options{HeartbeatInterval: hb}
		o.setDefaults()
		want := 15 * time.Second
		if o.MaintenanceTick != want {
			t.Fatalf("MaintenanceTick: got %v want %v", o.MaintenanceTick, want)
		}
	})
	t.Run("heartbeat_small_clamped_to_one_second", func(t *testing.T) {
		o := &Options{HeartbeatInterval: 1500 * time.Millisecond}
		o.setDefaults()
		if o.MaintenanceTick != time.Second {
			t.Fatalf("MaintenanceTick: got %v want 1s", o.MaintenanceTick)
		}
	})
	t.Run("idle_only", func(t *testing.T) {
		o := &Options{IdleTimeout: 50 * time.Second}
		o.setDefaults()
		want := 5 * time.Second
		if o.MaintenanceTick != want {
			t.Fatalf("MaintenanceTick: got %v want %v", o.MaintenanceTick, want)
		}
	})
	t.Run("idle_small_clamped_to_one_second", func(t *testing.T) {
		o := &Options{IdleTimeout: 5 * time.Second}
		o.setDefaults()
		if o.MaintenanceTick != time.Second {
			t.Fatalf("MaintenanceTick: got %v want 1s", o.MaintenanceTick)
		}
	})
	t.Run("neither", func(t *testing.T) {
		o := &Options{}
		o.setDefaults()
		if o.MaintenanceTick != time.Minute {
			t.Fatalf("MaintenanceTick: got %v want 1m", o.MaintenanceTick)
		}
	})
	t.Run("explicit_tick_preserved", func(t *testing.T) {
		o := &Options{HeartbeatInterval: 10 * time.Second, MaintenanceTick: 2 * time.Second}
		o.setDefaults()
		if o.MaintenanceTick != 2*time.Second {
			t.Fatalf("MaintenanceTick: got %v want 2s", o.MaintenanceTick)
		}
	})
}

func TestRetryDelay_EmptySliceUses60ms(t *testing.T) {
	o := &Options{RetryDelays: nil}
	if got := o.retryDelay(0); got != 60*time.Millisecond {
		t.Fatalf("retryDelay(0) with empty: got %v want 60ms", got)
	}
	o = &Options{RetryDelays: []time.Duration{}}
	if got := o.retryDelay(99); got != 60*time.Millisecond {
		t.Fatalf("retryDelay(99) with empty: got %v want 60ms", got)
	}
}

func TestRetryDelay_Indices(t *testing.T) {
	o := &Options{
		RetryDelays: []time.Duration{
			7 * time.Millisecond,
			14 * time.Millisecond,
			21 * time.Millisecond,
		},
	}
	if o.retryDelay(0) != 7*time.Millisecond {
		t.Fatalf("index 0: got %v", o.retryDelay(0))
	}
	if o.retryDelay(2) != 21*time.Millisecond {
		t.Fatalf("index 2: got %v", o.retryDelay(2))
	}
	if o.retryDelay(3) != 21*time.Millisecond {
		t.Fatalf("index past end: got %v want last", o.retryDelay(3))
	}
	if o.retryDelay(100) != 21*time.Millisecond {
		t.Fatalf("index far past: got %v want last", o.retryDelay(100))
	}
}

func TestSetDefaults_DialTimeout(t *testing.T) {
	// zero inherits default RequestTimeout
	o := &Options{}
	o.setDefaults()
	if o.DialTimeout != 5*time.Second {
		t.Fatalf("DialTimeout zero: got %v want 5s (== default RequestTimeout)", o.DialTimeout)
	}

	// negative also inherits
	o = &Options{DialTimeout: -1}
	o.setDefaults()
	if o.DialTimeout != 5*time.Second {
		t.Fatalf("DialTimeout negative: got %v want 5s", o.DialTimeout)
	}

	// custom RequestTimeout propagates when DialTimeout is unset
	o = &Options{RequestTimeout: 2 * time.Second}
	o.setDefaults()
	if o.DialTimeout != 2*time.Second {
		t.Fatalf("DialTimeout follows custom RequestTimeout: got %v want 2s", o.DialTimeout)
	}

	// explicit positive DialTimeout is preserved
	o = &Options{DialTimeout: 3 * time.Second}
	o.setDefaults()
	if o.DialTimeout != 3*time.Second {
		t.Fatalf("DialTimeout explicit: got %v want 3s", o.DialTimeout)
	}
}

func TestSetDefaults_DoesNotMutateRetryDelays(t *testing.T) {
	o := &Options{}
	o.setDefaults()
	if len(o.RetryDelays) == 0 {
		t.Fatal("RetryDelays should be initialized")
	}
}
