package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestRuntimeOrderAndRollback(t *testing.T) {
	var events []string
	runtime := &Runtime{}
	for _, name := range []string{"one", "two"} {
		name := name
		if err := runtime.Add(name, ComponentFuncs{
			StartFunc: func(context.Context) error { events = append(events, "start "+name); return nil },
			StopFunc:  func(context.Context) error { events = append(events, "stop "+name); return nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !runtime.Ready() {
		t.Fatal("runtime is not ready")
	}
	if err := runtime.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"start one", "start two", "stop two", "stop one"}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events=%v want=%v", events, want)
	}

	events = nil
	runtime = &Runtime{}
	_ = runtime.Add("one", ComponentFuncs{StartFunc: func(context.Context) error { events = append(events, "start one"); return nil }, StopFunc: func(context.Context) error { events = append(events, "stop one"); return nil }})
	_ = runtime.Add("bad", ComponentFuncs{StartFunc: func(context.Context) error { return errors.New("bad") }})
	if err := runtime.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded")
	}
	if !reflect.DeepEqual(events, []string{"start one", "stop one"}) {
		t.Fatalf("rollback events=%v", events)
	}
}
