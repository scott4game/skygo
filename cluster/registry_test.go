package cluster

import (
	"context"
	"testing"
)

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
