package callgraph

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
)

func TestRecorderAggregatesAndLoadsJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "edges.jsonl")
	recorder := New(Options{JSONLPath: path})
	recorder.OnCall(actor.CallEvent{Caller: "a", Callee: "b", Protocol: "ping", Duration: time.Millisecond})
	recorder.OnCall(actor.CallEvent{Caller: "a", Callee: "b", Protocol: "ping", Duration: 2 * time.Millisecond, Err: errors.New("boom")})
	edges := recorder.Snapshot()
	if len(edges) != 1 || edges[0].Count != 2 || edges[0].ErrorCount != 1 || edges[0].TotalDuration != 3*time.Millisecond {
		t.Fatalf("Snapshot = %#v", edges)
	}
	loaded, err := LoadJSONL(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].Count != 2 || loaded[0].ErrorCount != 1 {
		t.Fatalf("LoadJSONL = %#v", loaded)
	}
}
