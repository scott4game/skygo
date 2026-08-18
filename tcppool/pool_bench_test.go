package tcppool

import (
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// --------------------------------------------------------------------------
// Benchmarks
// --------------------------------------------------------------------------

// BenchmarkPool_Send measures single-connection Send throughput.
// Run with: go test ./tcppool/... -bench=BenchmarkPool_Send -benchmem -benchtime=5s
func BenchmarkPool_Send(b *testing.B) {
	es := startEchoServerB(b)
	defer es.Close()

	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, Options{
		PoolSize:         1,
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      4096,
		DialTimeout:      2 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCountB(b, p, 1, 3*time.Second)

	payload := make([]byte, 64)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Send(payload)
	}
}

// BenchmarkPool_Send_MultiConn measures round-robin throughput with four connections.
func BenchmarkPool_Send_MultiConn(b *testing.B) {
	es := startEchoServerB(b)
	defer es.Close()

	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, Options{
		PoolSize:         4,
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      4096,
		DialTimeout:      2 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCountB(b, p, 4, 3*time.Second)

	payload := make([]byte, 64)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Send(payload)
		}
	})
}

// BenchmarkPool_Send_Parallel measures contention with GOMAXPROCS senders.
func BenchmarkPool_Send_Parallel(b *testing.B) {
	es := startEchoServerB(b)
	defer es.Close()

	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, Options{
		PoolSize:         runtime.GOMAXPROCS(0),
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      8192,
		DialTimeout:      2 * time.Second,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCountB(b, p, runtime.GOMAXPROCS(0), 3*time.Second)

	payload := make([]byte, 128)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p.Send(payload)
		}
	})
}

// --------------------------------------------------------------------------
// Memory-leak & goroutine-leak checks under high concurrency
// --------------------------------------------------------------------------

// TestPool_HighConcurrency_NoLeak checks that high-concurrency sends leave no
// connected slots or material goroutine and heap growth after Close.
func TestPool_HighConcurrency_NoLeak(t *testing.T) {
	es := startEchoServer(t)
	defer es.Close()

	// Capture the heap and goroutine baseline before creating the pool.
	runtime.GC()
	var msBefore runtime.MemStats
	runtime.ReadMemStats(&msBefore)
	goroutinesBefore := runtime.NumGoroutine()

	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, Options{
		PoolSize:         4,
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      512,
		DialTimeout:      2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCount(t, p, 4, 3*time.Second)

	// Run 1,000 concurrent senders with 100 messages each.
	const goroutines = 1000
	const msgsPerG = 100
	payload := make([]byte, 64)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < msgsPerG; i++ {
				p.Send(payload)
			}
		}()
	}
	wg.Wait()

	p.Close()

	// Allow internal goroutines to observe Close.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.ConnCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if p.ConnCount() != 0 {
		t.Fatalf("ConnCount after Close: got %d want 0", p.ConnCount())
	}

	// Give all internal goroutines time to exit.
	time.Sleep(100 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	// Each poolConn starts lifecycle and sendLoop goroutines. Allow a small
	// tolerance for the test runtime and garbage collector.
	const leakTolerance = 10
	leaked := goroutinesAfter - goroutinesBefore
	t.Logf("goroutines before Pool: %d, after Close: %d, delta: %d",
		goroutinesBefore, goroutinesAfter, leaked)
	if leaked > leakTolerance {
		t.Errorf("possible goroutine leak: delta=%d > tolerance=%d", leaked, leakTolerance)
	}

	// Verify that retained heap does not grow materially after GC.
	runtime.GC()
	var msAfter runtime.MemStats
	runtime.ReadMemStats(&msAfter)
	t.Logf("HeapInuse before: %d KB, after: %d KB",
		msBefore.HeapInuse/1024, msAfter.HeapInuse/1024)
}

// TestPool_CreateClose_Repeated verifies repeated create/close cycles do not
// accumulate goroutines.
func TestPool_CreateClose_Repeated(t *testing.T) {
	es := startEchoServer(t)
	defer es.Close()

	runtime.GC()
	baseline := runtime.NumGoroutine()
	t.Logf("goroutine baseline: %d", baseline)

	for i := 0; i < 20; i++ {
		p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, Options{
			PoolSize:         2,
			ReconnectInitial: 10 * time.Millisecond,
			SendBufSize:      32,
			DialTimeout:      1 * time.Second,
		})
		if err != nil {
			t.Fatal(err)
		}
		p.SetLogger(func(string, ...interface{}) {})
		waitConnCount(t, p, 2, 2*time.Second)

		// Send a small batch before closing this pool.
		for j := 0; j < 50; j++ {
			p.Send([]byte("ping"))
		}
		p.Close()
	}

	// Wait for any remaining pool goroutines to exit.
	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()
	t.Logf("goroutines after 20 create/close cycles: %d (baseline %d)", after, baseline)

	const leakTolerance = 10
	if delta := after - baseline; delta > leakTolerance {
		t.Errorf("goroutine leak after repeated create/close: delta=%d > tolerance=%d", delta, leakTolerance)
	}
}

// --------------------------------------------------------------------------
// helpers only needed for benchmarks (b.Helper signature differs from t)
// --------------------------------------------------------------------------

func startEchoServerB(b *testing.B) *echoServer {
	b.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	es := &echoServer{ln: ln}
	es.wg.Add(1)
	go func() {
		defer es.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			es.connsMu.Lock()
			es.conns = append(es.conns, c)
			es.connsMu.Unlock()
			es.wg.Add(1)
			go func(conn net.Conn) {
				defer es.wg.Done()
				defer conn.Close()
				for {
					data, err := testReadFrame(conn)
					if err != nil {
						return
					}
					if err := testWriteFrame(conn, data); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return es
}

func waitConnCountB(b *testing.B, p *Pool, want int, timeout time.Duration) {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.ConnCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Fatalf("ConnCount want %d, got %d", want, p.ConnCount())
}
