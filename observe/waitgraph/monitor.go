// Package waitgraph detects cycles in explicitly registered wait relationships.
package waitgraph

import (
	"errors"
	"fmt"
	"sync"
)

// ErrCycle is returned when adding an edge would close a wait cycle.
var ErrCycle = errors.New("waitgraph: wait cycle detected")

// Monitor owns an isolated wait-for graph.
type Monitor struct {
	mu      sync.Mutex
	waitFor map[uint64]uint64
	labels  map[uint64]string
}

// New creates an empty wait graph.
func New() *Monitor {
	return &Monitor{waitFor: make(map[uint64]uint64), labels: make(map[uint64]string)}
}

// BeginWait records from→to. It returns ErrCycle without recording the edge
// when the edge would close a cycle. Zero and self edges are ignored.
func (m *Monitor) BeginWait(from, to uint64, label string) error {
	if m == nil || from == 0 || to == 0 || from == to {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.canReach(to, from) {
		return fmt.Errorf("%w: %s would create cycle %v→%d", ErrCycle, label, m.buildChain(to, from), from)
	}
	m.waitFor[from] = to
	m.labels[from] = label
	return nil
}

// EndWait removes from→to if that exact edge is still registered.
func (m *Monitor) EndWait(from, to uint64) {
	if m == nil || from == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.waitFor[from] == to {
		delete(m.waitFor, from)
		delete(m.labels, from)
	}
}

// Reset clears every registered edge.
func (m *Monitor) Reset() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.waitFor = make(map[uint64]uint64)
	m.labels = make(map[uint64]string)
	m.mu.Unlock()
}

func (m *Monitor) canReach(from, target uint64) bool {
	visited := make(map[uint64]bool)
	for current := from; ; current = m.waitFor[current] {
		if current == target {
			return true
		}
		if visited[current] {
			return false
		}
		visited[current] = true
		if _, ok := m.waitFor[current]; !ok {
			return false
		}
	}
}

func (m *Monitor) buildChain(from, target uint64) []uint64 {
	chain := make([]uint64, 0, 4)
	current := from
	for current != target && len(chain) < 20 {
		chain = append(chain, current)
		next, ok := m.waitFor[current]
		if !ok {
			break
		}
		current = next
	}
	return append(chain, target)
}
