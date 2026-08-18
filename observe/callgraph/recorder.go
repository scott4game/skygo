// Package callgraph aggregates actor call edges through actor.Observer.
package callgraph

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/scott4game/skygo/actor"
)

// Edge is one aggregated actor protocol call edge.
type Edge struct {
	From          string        `json:"from"`
	To            string        `json:"to"`
	Count         uint64        `json:"count"`
	ErrorCount    uint64        `json:"error_count,omitempty"`
	TotalDuration time.Duration `json:"total_duration_ns,omitempty"`
}

// Options configures optional JSONL event output.
type Options struct {
	JSONLPath string
}

// Recorder is a concurrency-safe actor call graph observer.
type Recorder struct {
	mu        sync.Mutex
	edges     map[string]*Edge
	jsonlPath string
}

// New creates an empty Recorder.
func New(opts Options) *Recorder {
	return &Recorder{edges: make(map[string]*Edge), jsonlPath: opts.JSONLPath}
}

// OnCall implements actor.Observer.
func (r *Recorder) OnCall(event actor.CallEvent) {
	if r == nil || event.Callee == "" || event.Protocol == "" {
		return
	}
	from := event.Caller
	if from == "" {
		from = "<external>"
	}
	to := event.Callee + "." + event.Protocol
	key := from + "->" + to
	r.mu.Lock()
	defer r.mu.Unlock()
	edge := r.edges[key]
	if edge == nil {
		edge = &Edge{From: from, To: to}
		r.edges[key] = edge
	}
	edge.Count++
	edge.TotalDuration += event.Duration
	if event.Err != nil {
		edge.ErrorCount++
	}
	if r.jsonlPath != "" {
		_ = appendJSONL(r.jsonlPath, Edge{
			From: from, To: to, Count: 1,
			ErrorCount: boolCount(event.Err != nil), TotalDuration: event.Duration,
		})
	}
}

// Snapshot returns a stable, sorted copy of the recorded edges.
func (r *Recorder) Snapshot() []Edge {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Edge, 0, len(r.edges))
	for _, edge := range r.edges {
		out = append(out, *edge)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].From == out[j].From {
			return out[i].To < out[j].To
		}
		return out[i].From < out[j].From
	})
	return out
}

// Reset clears all aggregated edges.
func (r *Recorder) Reset() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.edges = make(map[string]*Edge)
	r.mu.Unlock()
}

// ExportJSON writes the current snapshot as indented JSON.
func (r *Recorder) ExportJSON(path string) error {
	b, err := json.MarshalIndent(r.Snapshot(), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// LoadJSONL loads and merges Edge records from path.
func LoadJSONL(path string) ([]Edge, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	merged := make(map[string]*Edge)
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var edge Edge
		if json.Unmarshal(scanner.Bytes(), &edge) != nil || edge.From == "" || edge.To == "" {
			continue
		}
		key := edge.From + "->" + edge.To
		if current := merged[key]; current != nil {
			current.Count += edge.Count
			current.ErrorCount += edge.ErrorCount
			current.TotalDuration += edge.TotalDuration
		} else {
			copy := edge
			merged[key] = &copy
		}
	}
	out := make([]Edge, 0, len(merged))
	for _, edge := range merged {
		out = append(out, *edge)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].From+out[i].To < out[j].From+out[j].To })
	return out, scanner.Err()
}

// LoadRuntimeDir loads edges.jsonl from dir.
func LoadRuntimeDir(dir string) ([]Edge, error) {
	return LoadJSONL(filepath.Join(dir, "edges.jsonl"))
}

func appendJSONL(path string, edge Edge) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(edge)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	return err
}

func boolCount(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}
