package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
)

func TestConfigLifecycleDefaultsAndValidation(t *testing.T) {
	cfg := Config{NodeID: "a", Listen: "127.0.0.1:1", Secret: testSecret, Registry: NewStaticRegistry(Endpoint{NodeID: "a", Address: "127.0.0.1:1"})}
	if _, err := New(cfg, actor.NewSystem(actor.SystemOptions{})); err != nil {
		t.Fatal(err)
	}
	if err := cfg.defaults(); err != nil {
		t.Fatal(err)
	}
	if cfg.InboundShards != 32 || cfg.InboundQueue != 4096 || cfg.MaxInboundCalls != 1024 || cfg.PingInterval != 10*time.Second || cfg.IdleTimeout != 30*time.Second {
		t.Fatalf("defaults=%+v", cfg)
	}
	bad := cfg
	bad.IdleTimeout = bad.PingInterval
	if err := bad.defaults(); err == nil {
		t.Fatal("idle timeout shorter than two ping intervals was accepted")
	}
}

func TestStaticRegistryRevisionAndReplacement(t *testing.T) {
	registry := NewStaticRegistry(Endpoint{NodeID: "b", Address: "127.0.0.1:2"}, Endpoint{NodeID: "a", Address: "127.0.0.1:1"})
	first, err := registry.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Nodes[0].NodeID != "a" || first.Revision == "" {
		t.Fatalf("snapshot=%+v", first)
	}
	registry.Replace(Endpoint{NodeID: "a", Address: "127.0.0.1:3"})
	second, err := registry.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision == second.Revision {
		t.Fatal("revision did not change")
	}
}
