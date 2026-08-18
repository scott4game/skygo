package waitgraph

import (
	"errors"
	"testing"
)

func TestMonitorDetectsCyclesAndRemovesEdges(t *testing.T) {
	m := New()
	if err := m.BeginWait(1, 2, "a"); err != nil {
		t.Fatal(err)
	}
	if err := m.BeginWait(2, 3, "b"); err != nil {
		t.Fatal(err)
	}
	if err := m.BeginWait(3, 1, "c"); !errors.Is(err, ErrCycle) {
		t.Fatalf("BeginWait error = %v, want ErrCycle", err)
	}
	m.EndWait(2, 3)
	if err := m.BeginWait(3, 1, "c"); err != nil {
		t.Fatalf("BeginWait after EndWait: %v", err)
	}
}

func TestMonitorIgnoresZeroAndSelfEdges(t *testing.T) {
	m := New()
	for _, edge := range [][2]uint64{{0, 1}, {1, 0}, {1, 1}} {
		if err := m.BeginWait(edge[0], edge[1], "ignored"); err != nil {
			t.Fatal(err)
		}
	}
}
