package tcppool

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- test frame helpers (4-byte big-endian length + payload) ---

func testWriteFrame(conn net.Conn, data []byte) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func testReadFrame(conn net.Conn) ([]byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr)
	if n > 1<<20 {
		return nil, errors.New("frame too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func testReadLoop(conn net.Conn, onMsg OnMsgFunc) {
	for {
		data, err := testReadFrame(conn)
		if err != nil {
			return
		}
		if err := onMsg(data); err != nil {
			return
		}
	}
}

// echoServer accepts connections and echoes each frame back to the same connection.
type echoServer struct {
	ln      net.Listener
	wg      sync.WaitGroup
	connsMu sync.Mutex
	conns   []net.Conn
}

func startEchoServer(t *testing.T) *echoServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
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

func (es *echoServer) Addr() string {
	return es.ln.Addr().String()
}

func (es *echoServer) Close() {
	es.ln.Close()
	es.connsMu.Lock()
	for _, c := range es.conns {
		_ = c.Close()
	}
	es.conns = nil
	es.connsMu.Unlock()
	es.wg.Wait()
}

func waitConnCount(t *testing.T, p *Pool, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p.ConnCount() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("ConnCount want %d, got %d", want, p.ConnCount())
}

func TestPool_ConnectAndSend(t *testing.T) {
	es := startEchoServer(t)
	defer es.Close()

	received := make(chan []byte, 1)
	opts := Options{
		PoolSize:         1,
		ReconnectInitial: 10 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		JitterFraction:   0,
		SendBufSize:      16,
		DialTimeout:      2 * time.Second,
	}
	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func(data []byte) error {
		select {
		case received <- append([]byte(nil), data...):
		default:
		}
		return nil
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})

	waitConnCount(t, p, 1, 3*time.Second)

	payload := []byte("hello-tcppool")
	if !p.Send(payload) {
		t.Fatal("Send returned false")
	}

	select {
	case got := <-received:
		if string(got) != string(payload) {
			t.Fatalf("echo mismatch: got %q want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for echo")
	}
}

// countingEcho tracks which server connection index received each message (by accept order).
type countingEcho struct {
	ln      net.Listener
	wg      sync.WaitGroup
	mu      sync.Mutex
	perConn []int
}

func startCountingEcho(t *testing.T, n int) *countingEcho {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ce := &countingEcho{ln: ln, perConn: make([]int, n)}
	ce.wg.Add(1)
	go func() {
		defer ce.wg.Done()
		for i := 0; i < n; i++ {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			idx := i
			ce.wg.Add(1)
			go func(conn net.Conn) {
				defer ce.wg.Done()
				defer conn.Close()
				for {
					_, err := testReadFrame(conn)
					if err != nil {
						return
					}
					ce.mu.Lock()
					ce.perConn[idx]++
					ce.mu.Unlock()
				}
			}(c)
		}
	}()
	return ce
}

func (ce *countingEcho) Addr() string { return ce.ln.Addr().String() }

func (ce *countingEcho) Close() {
	_ = ce.ln.Close()
	ce.wg.Wait()
}

func TestPool_MultiConn_RoundRobin(t *testing.T) {
	n := 3
	ce := startCountingEcho(t, n)
	defer ce.Close()

	opts := Options{
		PoolSize:         n,
		ReconnectInitial: 10 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		JitterFraction:   0,
		SendBufSize:      32,
		DialTimeout:      2 * time.Second,
	}
	p, err := New(ce.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})

	waitConnCount(t, p, n, 5*time.Second)

	for i := 0; i < 6; i++ {
		if !p.Send([]byte{byte(i)}) {
			t.Fatalf("Send %d failed", i)
		}
	}
	time.Sleep(200 * time.Millisecond)

	ce.mu.Lock()
	defer ce.mu.Unlock()
	for i := 0; i < n; i++ {
		if ce.perConn[i] < 1 {
			t.Fatalf("conn slot %d got %d messages, want at least 1", i, ce.perConn[i])
		}
	}
}

func TestPool_Reconnect(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- c
	}()

	opts := Options{
		PoolSize:         1,
		ReconnectInitial: 50 * time.Millisecond,
		ReconnectMax:     500 * time.Millisecond,
		JitterFraction:   0,
		SendBufSize:      8,
		DialTimeout:      1 * time.Second,
	}
	p, err := New(addr, testWriteFrame, testReadLoop, func([]byte) error { return nil }, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})

	waitConnCount(t, p, 1, 3*time.Second)

	var srvConn net.Conn
	select {
	case srvConn = <-accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("server accept timeout")
	}
	// Closing the server side makes readFn return and ConnCount fall to zero.
	_ = srvConn.Close()
	waitConnCount(t, p, 0, 5*time.Second)

	_ = ln.Close()

	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer ln2.Close()
	go func() {
		c, err := ln2.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 1)
		for {
			if _, err := c.Read(buf); err != nil {
				return
			}
		}
	}()

	waitConnCount(t, p, 1, 15*time.Second)
}

func TestPool_SendChannelFull(t *testing.T) {
	opts := Options{
		PoolSize:         1,
		ReconnectInitial: 100 * time.Millisecond,
		DialTimeout:      20 * time.Millisecond,
		SendBufSize:      1,
		JitterFraction:   0,
	}
	// Nothing listens on this port — dial fails, no conn; sendLoop blocks in waitConn on first item.
	p, err := New("127.0.0.1:1", testWriteFrame, nil, nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})

	if !p.Send([]byte("a")) {
		t.Fatal("first Send should queue")
	}
	// Wait until sendLoop drains the first item and blocks in waitConn, leaving
	// room in the channel for the second item.
	time.Sleep(30 * time.Millisecond)
	if !p.Send([]byte("b")) {
		t.Fatal("second Send should queue after sendLoop drained first into waitConn")
	}
	if p.Send([]byte("c")) {
		t.Fatal("third Send should fail (channel full)")
	}
}

func TestPool_CloseStopsSend(t *testing.T) {
	es := startEchoServer(t)
	defer es.Close()

	opts := Options{
		PoolSize:         1,
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      16,
		DialTimeout:      2 * time.Second,
	}
	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCount(t, p, 1, 3*time.Second)

	p.Close()
	time.Sleep(50 * time.Millisecond)

	if p.ConnCount() != 0 {
		t.Fatalf("ConnCount after Close: got %d want 0", p.ConnCount())
	}
	if p.Send([]byte("x")) {
		t.Fatal("Send after Close should return false")
	}
}

func TestPool_ConcurrentSend(t *testing.T) {
	es := startEchoServer(t)
	defer es.Close()

	var total atomic.Int64
	opts := Options{
		PoolSize:         2,
		ReconnectInitial: 10 * time.Millisecond,
		SendBufSize:      256,
		DialTimeout:      2 * time.Second,
	}
	p, err := New(es.Addr(), testWriteFrame, testReadLoop, func([]byte) error {
		total.Add(1)
		return nil
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})
	waitConnCount(t, p, 2, 3*time.Second)

	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				p.Send([]byte("ok"))
			}
		}()
	}
	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	if n := total.Load(); n > 1000 {
		t.Fatalf("received %d echos, want <= 1000", n)
	}
}

func TestPool_New_NilWriteFn(t *testing.T) {
	_, err := New("127.0.0.1:1", nil, nil, nil, Options{})
	if err == nil {
		t.Fatal("New with nil writeFn should error")
	}
}

func TestPool_SendTo(t *testing.T) {
	n := 3
	ce := startCountingEcho(t, n)
	defer ce.Close()

	opts := Options{
		PoolSize:         n,
		ReconnectInitial: 10 * time.Millisecond,
		ReconnectMax:     100 * time.Millisecond,
		JitterFraction:   0,
		SendBufSize:      32,
		DialTimeout:      2 * time.Second,
	}
	p, err := New(ce.Addr(), testWriteFrame, testReadLoop, func([]byte) error { return nil }, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	p.SetLogger(func(string, ...interface{}) {})

	waitConnCount(t, p, n, 5*time.Second)

	for slot := 0; slot < n; slot++ {
		if !p.SendTo(slot, []byte{byte(slot)}) {
			t.Fatalf("SendTo(%d) failed", slot)
		}
	}
	time.Sleep(200 * time.Millisecond)

	ce.mu.Lock()
	defer ce.mu.Unlock()
	for i := 0; i < n; i++ {
		if ce.perConn[i] != 1 {
			t.Fatalf("conn slot %d got %d messages, want exactly 1", i, ce.perConn[i])
		}
	}
}
