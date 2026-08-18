//go:build stress

package tcppool

import (
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/internal/stresstest"
)

func TestStressConcurrentSendReconnectAndClose(t *testing.T) {
	defer stresstest.LeakCheck(t, stresstest.DefaultLeakTolerance)()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Uint64
	var connMu sync.Mutex
	connections := make(map[net.Conn]struct{})
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			connMu.Lock()
			connections[conn] = struct{}{}
			connMu.Unlock()
			go func() {
				_, _ = io.Copy(io.Discard, conn)
				_ = conn.Close()
				connMu.Lock()
				delete(connections, conn)
				connMu.Unlock()
			}()
		}
	}()
	closeAccepted := func() {
		connMu.Lock()
		defer connMu.Unlock()
		for conn := range connections {
			_ = conn.Close()
		}
	}
	defer func() {
		_ = listener.Close()
		closeAccepted()
		stresstest.Wait(t, "TCP accept loop shutdown", time.Second, serverDone)
	}()

	pool, err := New(listener.Addr().String(), func(conn net.Conn, data []byte) error {
		_, err := conn.Write(data)
		return err
	}, func(conn net.Conn, _ OnMsgFunc) {
		_, _ = io.Copy(io.Discard, conn)
	}, nil, Options{
		PoolSize:         2,
		DialTimeout:      200 * time.Millisecond,
		ReconnectInitial: 5 * time.Millisecond,
		ReconnectMax:     20 * time.Millisecond,
		StableThreshold:  20 * time.Millisecond,
		JitterFraction:   0.01,
		SendBufSize:      1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.SetLogger(func(string, ...interface{}) {})

	stresstest.Eventually(t, "initial pool connections", 2*time.Second, 5*time.Millisecond, func() bool {
		return pool.ConnCount() == 2
	})
	initialAccepted := accepted.Load()

	stopSend := make(chan struct{})
	var senders sync.WaitGroup
	for i := 0; i < 32; i++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			payload := []byte("skygo-stress")
			for {
				select {
				case <-stopSend:
					return
				default:
					_ = pool.Send(payload)
					runtime.Gosched()
				}
			}
		}()
	}
	closeAccepted()
	stresstest.Eventually(t, "pool reconnect", 3*time.Second, 5*time.Millisecond, func() bool {
		return accepted.Load() > initialAccepted
	})
	close(stopSend)
	senders.Wait()
	pool.Close()
	pool.Close()
	if pool.Send([]byte("after-close")) {
		t.Fatal("Send succeeded after Close")
	}

	_ = listener.Close()
	closeAccepted()
	stresstest.Wait(t, "TCP accept loop shutdown", time.Second, serverDone)
}
