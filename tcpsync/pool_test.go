package tcpsync

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/frame"
)

func TestPoolExhaustedNoWait(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		// Consume one read so client finishes write, then block forever without closing.
		buf := make([]byte, 4096)
		_, _ = c.Read(buf)
		select {}
	}()

	p, err := New(addr, Options{
		MaxSize:           1,
		DialTimeout:       time.Second,
		RequestTimeout:    time.Second,
		RetryCount:        0,
		HeartbeatInterval: 0,
		IdleTimeout:       0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	req := []byte{1, 2, 3}
	go func() {
		_, _ = p.Do(context.Background(), req)
	}()
	time.Sleep(50 * time.Millisecond)

	_, err = p.Do(context.Background(), req)
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("expected ErrPoolExhausted, got %v", err)
	}
}

func TestDoEcho(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				for {
					b, err := frame.Read(conn)
					if err != nil {
						return
					}
					_ = frame.Write(conn, b)
				}
			}(c)
		}
	}()

	p, err := New(addr, Options{
		MaxSize:           4,
		DialTimeout:       time.Second,
		RequestTimeout:    time.Second,
		RetryCount:        0,
		HeartbeatInterval: 0,
		IdleTimeout:       0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	payload := []byte("hello")
	out, err := p.Do(context.Background(), payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(payload) {
		t.Fatalf("echo mismatch: %q vs %q", out, payload)
	}
}

func TestSweepIdle_PingOnlyAfterHeartbeatInterval(t *testing.T) {
	p := &Pool{
		opts: Options{
			HeartbeatInterval:       50 * time.Millisecond,
			HeartbeatRequestTimeout: time.Second,
			Pinger: func(conn net.Conn, timeout time.Duration) error {
				return nil
			},
		},
		idle:   make(chan *Conn, 1),
		closed: make(chan struct{}),
		total:  1,
	}
	c := &Conn{
		pool:     p,
		lastUsed: time.Now(),
	}
	p.idle <- c

	p.sweepIdle()

	select {
	case got := <-p.idle:
		if !got.lastPinged.IsZero() {
			t.Fatalf("expected no ping before interval, got %v", got.lastPinged)
		}
	default:
		t.Fatal("expected connection to remain idle")
	}
}

func TestSweepIdle_UpdatesLastPingedAfterSuccessfulPing(t *testing.T) {
	var pingCount atomic.Int32
	p := &Pool{
		opts: Options{
			HeartbeatInterval:       20 * time.Millisecond,
			HeartbeatRequestTimeout: time.Second,
			Pinger: func(conn net.Conn, timeout time.Duration) error {
				pingCount.Add(1)
				return nil
			},
		},
		idle:   make(chan *Conn, 1),
		closed: make(chan struct{}),
		total:  1,
	}
	c := &Conn{
		pool:     p,
		lastUsed: time.Now().Add(-time.Second),
	}
	p.idle <- c

	p.sweepIdle()

	if pingCount.Load() != 1 {
		t.Fatalf("expected one ping, got %d", pingCount.Load())
	}
	select {
	case got := <-p.idle:
		if got.lastPinged.IsZero() {
			t.Fatal("expected lastPinged to be updated")
		}
	default:
		t.Fatal("expected connection to be returned to idle")
	}
}

func TestSweepIdle_SkipsHeartbeatWithoutPinger(t *testing.T) {
	p := &Pool{
		opts: Options{
			HeartbeatInterval:       20 * time.Millisecond,
			HeartbeatRequestTimeout: time.Second,
		},
		idle:   make(chan *Conn, 1),
		closed: make(chan struct{}),
		total:  1,
	}
	c := &Conn{
		pool:     p,
		lastUsed: time.Now().Add(-time.Second),
	}
	p.idle <- c

	p.sweepIdle()

	select {
	case got := <-p.idle:
		if !got.lastPinged.IsZero() {
			t.Fatalf("expected no ping timestamp, got %v", got.lastPinged)
		}
	default:
		t.Fatal("expected connection to be returned to idle")
	}
}

func TestSweepIdle_ClosesConnectionWhenPingFails(t *testing.T) {
	p := &Pool{
		opts: Options{
			HeartbeatInterval:       20 * time.Millisecond,
			HeartbeatRequestTimeout: time.Second,
			Pinger: func(conn net.Conn, timeout time.Duration) error {
				return errors.New("ping failed")
			},
		},
		idle:   make(chan *Conn, 1),
		closed: make(chan struct{}),
		total:  1,
	}
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	c := &Conn{
		netConn:  clientConn,
		pool:     p,
		lastUsed: time.Now().Add(-time.Second),
	}
	p.idle <- c

	p.sweepIdle()

	select {
	case <-p.idle:
		t.Fatal("expected broken connection to be removed from idle pool")
	default:
	}
	if p.total != 0 {
		t.Fatalf("expected total=0 after failed ping, got %d", p.total)
	}
}

func TestSweepIdle_UsesLastPingedAsLatestActivity(t *testing.T) {
	var pingCount atomic.Int32
	p := &Pool{
		opts: Options{
			HeartbeatInterval:       100 * time.Millisecond,
			HeartbeatRequestTimeout: time.Second,
			Pinger: func(conn net.Conn, timeout time.Duration) error {
				pingCount.Add(1)
				return nil
			},
		},
		idle:   make(chan *Conn, 1),
		closed: make(chan struct{}),
		total:  1,
	}
	c := &Conn{
		pool:       p,
		lastUsed:   time.Now().Add(-time.Second),
		lastPinged: time.Now(),
	}
	p.idle <- c

	p.sweepIdle()

	if pingCount.Load() != 0 {
		t.Fatalf("expected no ping when lastPinged is recent, got %d", pingCount.Load())
	}
}

func TestDo_CanceledContextAbortsDial(t *testing.T) {
	// Open and immediately close a listener so the address is valid but no server
	// is present. With a pre-canceled context, DialContext must return before any
	// network round-trip.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	p, err := New(addr, Options{
		MaxSize:        1,
		RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	_, err = p.Do(ctx, []byte("ping"))
	elapsed := time.Since(start)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Do took too long with canceled ctx: %v", elapsed)
	}
}

func TestNew_HeartbeatWithoutPingerStillConstructsPool(t *testing.T) {
	p, err := New("127.0.0.1:1", Options{
		MaxSize:           1,
		HeartbeatInterval: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
}
