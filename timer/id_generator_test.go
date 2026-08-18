package timer

import (
	"fmt"
	"sync"
	"testing"
)

func TestIDGeneratorConcurrentUnique(t *testing.T) {
	g := NewIDGenerator(7)
	const n = 1000
	ids := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := g.Next()
			if err != nil {
				t.Errorf("Next() error = %v", err)
				return
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[uint64]bool, n)
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate id %d", id)
		}
		seen[id] = true
	}
	if len(seen) != n {
		t.Fatalf("got %d ids, want %d", len(seen), n)
	}
}

func TestNodeNameToSnowflakeNodeIDStable(t *testing.T) {
	a := NodeNameToSnowflakeNodeID("r1")
	b := NodeNameToSnowflakeNodeID("r1")
	if a != b {
		t.Fatalf("node id not stable: %d != %d", a, b)
	}
	if a > snowflakeMaxNodeID {
		t.Fatalf("node id %d exceeds max %d", a, snowflakeMaxNodeID)
	}
}

func BenchmarkIDGeneratorNext(b *testing.B) {
	g := NewIDGenerator(7)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := g.Next(); err != nil {
			b.Fatalf("Next() error = %v", err)
		}
	}
}

func BenchmarkIDGeneratorNextParallel(b *testing.B) {
	g := NewIDGenerator(7)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := g.Next(); err != nil {
				b.Fatalf("Next() error = %v", err)
			}
		}
	})
}

func BenchmarkIDGeneratorNextParallelContention(b *testing.B) {
	for _, parallelism := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("parallelism_%d", parallelism), func(b *testing.B) {
			g := NewIDGenerator(7)
			b.ReportAllocs()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if _, err := g.Next(); err != nil {
						b.Fatalf("Next() error = %v", err)
					}
				}
			})
		})
	}
}
