package tcppool

import (
	"testing"
	"time"
)

func TestOptions_SetDefaults(t *testing.T) {
	o := Options{}
	o.setDefaults()
	if o.PoolSize != 1 {
		t.Fatalf("PoolSize: %d", o.PoolSize)
	}
	if o.ReconnectInitial != time.Second {
		t.Fatalf("ReconnectInitial: %v", o.ReconnectInitial)
	}
	if o.ReconnectMax != 10*time.Second {
		t.Fatalf("ReconnectMax: %v", o.ReconnectMax)
	}
	if o.StableThreshold != 5*time.Second {
		t.Fatalf("StableThreshold: %v", o.StableThreshold)
	}
	if o.JitterFraction != 0.2 {
		t.Fatalf("JitterFraction: %v", o.JitterFraction)
	}
	if o.SendBufSize != 128 {
		t.Fatalf("SendBufSize: %d", o.SendBufSize)
	}
}

func TestOptions_SetDefaults_Partial(t *testing.T) {
	o := Options{
		PoolSize:         4,
		ReconnectInitial: 2 * time.Second,
		SendBufSize:      64,
	}
	o.setDefaults()
	if o.PoolSize != 4 {
		t.Fatalf("PoolSize should be preserved: %d", o.PoolSize)
	}
	if o.ReconnectInitial != 2*time.Second {
		t.Fatalf("ReconnectInitial should be preserved: %v", o.ReconnectInitial)
	}
	if o.ReconnectMax != 10*time.Second {
		t.Fatalf("ReconnectMax should get default: %v", o.ReconnectMax)
	}
	if o.StableThreshold != 5*time.Second {
		t.Fatalf("StableThreshold should get default: %v", o.StableThreshold)
	}
	if o.JitterFraction != 0.2 {
		t.Fatalf("JitterFraction should get default: %v", o.JitterFraction)
	}
	if o.SendBufSize != 64 {
		t.Fatalf("SendBufSize should be preserved: %d", o.SendBufSize)
	}
}

func TestOptions_SetDefaults_NegativePoolSize(t *testing.T) {
	o := Options{PoolSize: -1}
	o.setDefaults()
	if o.PoolSize != 1 {
		t.Fatalf("negative PoolSize should become 1, got %d", o.PoolSize)
	}
}
