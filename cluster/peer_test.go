package cluster

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
)

func TestPeerCloseReportsQueuedNotificationOnce(t *testing.T) {
	failures := make(chan actor.AsyncError, 2)
	system := actor.NewSystem(actor.SystemOptions{AsyncError: func(failure actor.AsyncError) { failures <- failure }})
	left, right := net.Pipe()
	defer right.Close()
	slot := &peerSlot{state: PeerReady}
	node := &Node{system: system}
	p := &peer{node: node, slot: slot, remoteNode: "b", conn: left, send: make(chan outbound, 2), control: make(chan outbound, 1), done: make(chan struct{}), pending: make(map[uint64]chan peerResult)}
	slot.peer = p
	p.send <- outbound{envelope: &wire.Envelope{}, target: actor.RemoteTarget{Node: "b", Service: "delivery"}, protocol: "push", notify: true}
	p.close(actor.ErrRemoteUnavailable)
	select {
	case failure := <-failures:
		if failure.Stage != actor.AsyncErrorTransportWrite || failure.RemoteNode != "b" || !errors.Is(failure.Err, actor.ErrRemoteUnavailable) {
			t.Fatalf("failure=%+v", failure)
		}
	default:
		t.Fatal("queued notification failure was not reported")
	}
	select {
	case failure := <-failures:
		t.Fatalf("duplicate failure=%+v", failure)
	default:
	}
	if got := node.Stats().Counters.SendWriteFailures; got != 1 {
		t.Fatalf("send write failures=%d", got)
	}
}

func TestPeerCloseConcurrentEnqueueReportsEveryAcceptedNotification(t *testing.T) {
	failures := make(chan actor.AsyncError, 2)
	system := actor.NewSystem(actor.SystemOptions{AsyncError: func(failure actor.AsyncError) { failures <- failure }})
	defer system.Stop(context.Background())

	for iteration := 0; iteration < 1000; iteration++ {
		left, right := net.Pipe()
		slot := &peerSlot{state: PeerReady}
		node := &Node{system: system}
		p := &peer{node: node, slot: slot, remoteNode: "b", conn: left, send: make(chan outbound, 1), control: make(chan outbound, 1), done: make(chan struct{}), pending: make(map[uint64]chan peerResult)}
		slot.peer = p
		start := make(chan struct{})
		errResult := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errResult <- p.enqueue(context.Background(), outbound{
				envelope: &wire.Envelope{},
				target:   actor.RemoteTarget{Node: "b", Service: "delivery"},
				protocol: "push",
				notify:   true,
			}, true)
		}()
		go func() {
			defer wg.Done()
			<-start
			p.close(actor.ErrRemoteUnavailable)
		}()
		close(start)
		wg.Wait()
		_ = right.Close()

		err := <-errResult
		switch {
		case err == nil:
			select {
			case failure := <-failures:
				if !errors.Is(failure.Err, actor.ErrRemoteUnavailable) {
					t.Fatalf("iteration %d: failure=%v", iteration, failure.Err)
				}
			default:
				t.Fatalf("iteration %d: accepted notification was not reported", iteration)
			}
		case errors.Is(err, actor.ErrRemoteUnavailable):
			select {
			case failure := <-failures:
				t.Fatalf("iteration %d: rejected notification reported asynchronously: %+v", iteration, failure)
			default:
			}
		default:
			t.Fatalf("iteration %d: enqueue error=%v", iteration, err)
		}
	}
}

func TestPeerHeartbeatClosesSilentConnection(t *testing.T) {
	left, right := net.Pipe()
	slot := &peerSlot{state: PeerReady}
	node := &Node{cfg: Config{NodeID: "a", MaxPayload: 1024, WriteTimeout: 50 * time.Millisecond, PingInterval: 5 * time.Millisecond, IdleTimeout: 20 * time.Millisecond, ConnectBackoffMin: time.Millisecond, ConnectBackoffMax: time.Millisecond}, system: actor.NewSystem(actor.SystemOptions{})}
	p := &peer{node: node, slot: slot, remoteNode: "b", conn: left, send: make(chan outbound, 1), control: make(chan outbound, 1), done: make(chan struct{}), pending: make(map[uint64]chan peerResult)}
	p.lastRead.Store(time.Now().UnixNano())
	p.lastWrite.Store(time.Now().UnixNano())
	slot.peer = p
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, err := readEnvelope(right, 1024); err != nil {
				return
			}
		}
	}()
	go p.writeLoop()
	select {
	case <-p.done:
	case <-time.After(time.Second):
		t.Fatal("silent connection was not closed")
	}
	_ = right.Close()
	<-readerDone
	if got := node.Stats().Counters.HeartbeatTimeouts; got != 1 {
		t.Fatalf("heartbeat timeouts=%d", got)
	}
	_ = node.system.Stop(context.Background())
}
