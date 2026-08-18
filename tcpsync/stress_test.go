//go:build stress

package tcpsync

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/scott4game/skygo/frame"
	"github.com/scott4game/skygo/internal/stresstest"
)

func TestStressPoolExhaustionReturnsImmediately(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseServer()
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := frame.Read(conn); err != nil {
			return
		}
		received <- struct{}{}
		<-release
		_ = frame.Write(conn, []byte("ok"))
	}()
	defer listener.Close()

	pool, err := New(listener.Addr().String(), Options{MaxSize: 1, RequestTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	firstDone := make(chan error, 1)
	go func() {
		response, err := pool.Do(context.Background(), []byte("first"))
		if err == nil && string(response) != "ok" {
			err = errors.New("unexpected first response")
		}
		firstDone <- err
	}()
	stresstest.Wait(t, "first synchronous request", time.Second, received)
	started := time.Now()
	_, err = pool.Do(context.Background(), []byte("second"))
	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("second Do error=%v, want ErrPoolExhausted", err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("pool exhaustion took %s, want immediate failure", elapsed)
	}
	releaseServer()
	if err := stresstest.Wait(t, "first synchronous response", time.Second, firstDone); err != nil {
		t.Fatal(err)
	}
	stresstest.Wait(t, "synchronous test server shutdown", time.Second, serverDone)
}

func TestStressServerStallUsesRequestTimeout(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseServer := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseServer()
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = frame.Read(conn)
		<-release
	}()
	pool, err := New(listener.Addr().String(), Options{MaxSize: 1, RequestTimeout: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	defer listener.Close()
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = pool.Do(ctx, []byte("stall"))
	if err == nil || (!errors.Is(err, context.DeadlineExceeded) && !strings.Contains(strings.ToLower(err.Error()), "timeout")) {
		t.Fatalf("Do error=%v, want timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("request timeout took %s", elapsed)
	}
	releaseServer()
	pool.Close()
	_ = listener.Close()
	stresstest.Wait(t, "stalled server shutdown", time.Second, serverDone)
}
