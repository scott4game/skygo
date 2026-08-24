package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
)

const RegistrySchemaVersion = 1

// Endpoint maps a stable node ID to its cluster listener address.
type Endpoint struct {
	NodeID  string `json:"nodeId"`
	Address string `json:"address"`
}

// Snapshot is an immutable registry view.
type Snapshot struct {
	SchemaVersion int        `json:"schemaVersion"`
	Revision      string     `json:"revision"`
	Nodes         []Endpoint `json:"nodes"`
}

// Registry supplies complete, validated node snapshots. Reload is explicit.
type Registry interface {
	Load(context.Context) (Snapshot, error)
}

// FileRegistry reads a generated cluster JSON file on every Load.
type FileRegistry struct{ Path string }

func (r FileRegistry) Load(context.Context) (Snapshot, error) {
	data, err := os.ReadFile(r.Path)
	if err != nil {
		return Snapshot{}, fmt.Errorf("cluster: read registry: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return Snapshot{}, fmt.Errorf("cluster: parse registry: %w", err)
	}
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot.normalized(), nil
}

// StaticRegistry is useful for tests and embedded deployments. Replace is
// observed by the next explicit Node.Reload call.
type StaticRegistry struct {
	mu       sync.RWMutex
	snapshot Snapshot
}

func NewStaticRegistry(nodes ...Endpoint) *StaticRegistry {
	r := &StaticRegistry{}
	r.Replace(nodes...)
	return r
}

func (r *StaticRegistry) Replace(nodes ...Endpoint) {
	snapshot := Snapshot{SchemaVersion: RegistrySchemaVersion, Nodes: append([]Endpoint(nil), nodes...)}.normalized()
	snapshot.Revision = snapshot.computedRevision()
	r.mu.Lock()
	r.snapshot = snapshot
	r.mu.Unlock()
}

func (r *StaticRegistry) Load(context.Context) (Snapshot, error) {
	r.mu.RLock()
	snapshot := r.snapshot
	r.mu.RUnlock()
	snapshot.Nodes = append([]Endpoint(nil), snapshot.Nodes...)
	if err := snapshot.Validate(); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (s Snapshot) Validate() error {
	if s.SchemaVersion != RegistrySchemaVersion {
		return fmt.Errorf("cluster: registry schemaVersion=%d want=%d", s.SchemaVersion, RegistrySchemaVersion)
	}
	seen := make(map[string]struct{}, len(s.Nodes))
	for i, endpoint := range s.Nodes {
		node := strings.TrimSpace(endpoint.NodeID)
		address := strings.TrimSpace(endpoint.Address)
		if node == "" || address == "" {
			return fmt.Errorf("cluster: registry nodes[%d] requires nodeId and address", i)
		}
		if _, exists := seen[node]; exists {
			return fmt.Errorf("cluster: duplicate node %q", node)
		}
		seen[node] = struct{}{}
	}
	normalized := s.normalized()
	want := normalized.computedRevision()
	if s.Revision == "" || s.Revision != want {
		return fmt.Errorf("cluster: registry revision mismatch got=%q want=%q", s.Revision, want)
	}
	return nil
}

func (s Snapshot) normalized() Snapshot {
	out := Snapshot{SchemaVersion: s.SchemaVersion, Revision: s.Revision, Nodes: append([]Endpoint(nil), s.Nodes...)}
	for i := range out.Nodes {
		out.Nodes[i].NodeID = strings.TrimSpace(out.Nodes[i].NodeID)
		out.Nodes[i].Address = strings.TrimSpace(out.Nodes[i].Address)
	}
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].NodeID < out.Nodes[j].NodeID })
	return out
}

func (s Snapshot) computedRevision() string {
	normalized := s.normalized()
	data, _ := json.Marshal(normalized.Nodes)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (s Snapshot) endpoints() map[string]string {
	result := make(map[string]string, len(s.Nodes))
	for _, endpoint := range s.Nodes {
		result[endpoint.NodeID] = endpoint.Address
	}
	return result
}
