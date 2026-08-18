package tcpsync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/scott4game/skygo/frame"
)

func TestConnSendRecv_ClosedConnection(t *testing.T) {
	c := &Conn{}
	_, err := c.sendRecv(context.Background(), []byte{1}, time.Second)
	if err == nil || !strings.Contains(err.Error(), "closed connection") {
		t.Fatalf("sendRecv nil conn: got %v", err)
	}
}

func TestConnSendRecv_Echo(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			b, err := frame.Read(conn)
			if err != nil {
				return
			}
			if err := frame.Write(conn, b); err != nil {
				return
			}
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	c := &Conn{netConn: client}
	payload := []byte("round-trip")
	out, err := c.sendRecv(context.Background(), payload, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(payload) {
		t.Fatalf("echo: got %q want %q", out, payload)
	}
	_ = client.Close()
	<-done
}

func TestConnSendRecv_ContextDeadlineShort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		select {} // never respond to read after client writes
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(80*time.Millisecond))
	defer cancel()

	c := &Conn{netConn: client}
	_, err = c.sendRecv(ctx, []byte("x"), time.Minute)
	if err == nil {
		t.Fatal("expected error from deadline")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) && !strings.Contains(err.Error(), "timeout") &&
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConnPing_Success(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	var called bool
	pool := &Pool{opts: Options{
		Pinger: func(conn net.Conn, timeout time.Duration) error {
			called = true
			if conn != client {
				return fmt.Errorf("unexpected conn")
			}
			if timeout != 2*time.Second {
				return fmt.Errorf("unexpected timeout %v", timeout)
			}
			return nil
		},
	}}

	c := &Conn{netConn: client, pool: pool}
	if err := c.ping(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected pinger to be called")
	}
}

func TestConnPing_NoPinger(t *testing.T) {
	c := &Conn{netConn: nil, pool: &Pool{}}
	if err := c.ping(2 * time.Second); err != nil {
		t.Fatalf("expected nil error with no pinger, got %v", err)
	}
}

func TestConnPing_PingerError(t *testing.T) {
	want := errors.New("boom")
	c := &Conn{
		netConn: nil,
		pool: &Pool{opts: Options{
			Pinger: func(conn net.Conn, timeout time.Duration) error {
				return want
			},
		}},
	}
	err := c.ping(2 * time.Second)
	if !errors.Is(err, want) {
		t.Fatalf("want %v, got %v", want, err)
	}
}

func TestConnClose_Idempotent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ch := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			ch <- c
		}
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c := &Conn{netConn: client}
	c.close()
	c.close()
	if c.netConn != nil {
		t.Fatal("netConn should be nil after close")
	}

	srv := <-ch
	_ = srv.Close()
}
