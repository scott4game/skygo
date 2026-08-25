package cluster

import (
	"fmt"
	"sync"

	"github.com/scott4game/skygo/actor"
)

type registryManager struct {
	mu        sync.RWMutex
	localNode string
	endpoints map[string]string
	revision  string
}

func newRegistryManager(localNode string) *registryManager {
	return &registryManager{localNode: localNode, endpoints: make(map[string]string)}
}

func (r *registryManager) install(snapshot Snapshot) (map[string]struct{}, error) {
	next := snapshot.endpoints()
	if next[r.localNode] == "" {
		return nil, fmt.Errorf("cluster: registry does not contain local node %q", r.localNode)
	}
	r.mu.Lock()
	changed := make(map[string]struct{})
	for nodeID, old := range r.endpoints {
		if next[nodeID] == "" || next[nodeID] != old {
			changed[nodeID] = struct{}{}
		}
	}
	for nodeID, endpoint := range next {
		if old := r.endpoints[nodeID]; old != "" && old != endpoint {
			changed[nodeID] = struct{}{}
		}
	}
	r.endpoints = next
	r.revision = snapshot.Revision
	r.mu.Unlock()
	return changed, nil
}

func (r *registryManager) endpoint(nodeID string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.endpoints[nodeID]
}

func (r *registryManager) known(nodeID string) bool { return r.endpoint(nodeID) != "" }
func (r *registryManager) revisionValue() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.revision
}

type peerManager struct {
	node  *Node
	mu    sync.RWMutex
	slots map[string][]*peerSlot
}

func newPeerManager(node *Node) *peerManager {
	return &peerManager{node: node, slots: make(map[string][]*peerSlot)}
}

func (m *peerManager) slot(nodeID string, index, count int) *peerSlot {
	m.mu.Lock()
	defer m.mu.Unlock()
	slots := m.slots[nodeID]
	if len(slots) == 0 {
		slots = make([]*peerSlot, count)
		for i := range slots {
			slots[i] = &peerSlot{state: PeerDisconnected}
		}
		m.slots[nodeID] = slots
	}
	return slots[index]
}

func (m *peerManager) closeChanged(changed map[string]struct{}) {
	var closing []*peer
	m.mu.Lock()
	for nodeID := range changed {
		for _, slot := range m.slots[nodeID] {
			closing = append(closing, detachSlot(slot)...)
		}
		delete(m.slots, nodeID)
	}
	m.mu.Unlock()
	for _, p := range closing {
		p.close(actor.ErrRemoteUnavailable)
	}
}

func (m *peerManager) closeAll() {
	var closing []*peer
	m.mu.Lock()
	for nodeID, slots := range m.slots {
		for _, slot := range slots {
			closing = append(closing, detachSlot(slot)...)
		}
		delete(m.slots, nodeID)
	}
	m.mu.Unlock()
	for _, p := range closing {
		p.close(actor.ErrRemoteUnavailable)
	}
}

func detachSlot(slot *peerSlot) []*peer {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	var result []*peer
	if slot.peer != nil {
		result = append(result, slot.peer)
		slot.peer = nil
	}
	slot.state, slot.endpoint = PeerClosing, ""
	return result
}

func (m *peerManager) snapshots() []PeerStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []PeerStats
	for nodeID, slots := range m.slots {
		for index, slot := range slots {
			result = append(result, slot.snapshot(nodeID, index))
		}
	}
	return result
}
