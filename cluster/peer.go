package cluster

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
)

type peerSlot struct {
	mu                sync.Mutex
	peer              *peer
	state             PeerState
	endpoint          string
	connecting        chan struct{}
	backoffUntil      time.Time
	failures          uint32
	connectFailures   uint64
	handshakeFailures uint64
	reconnects        uint64
	everConnected     bool
}

type peerResult struct {
	envelope *wire.Envelope
	err      error
}

type outbound struct {
	envelope *wire.Envelope
	target   actor.RemoteTarget
	protocol string
	notify   bool
}

type peer struct {
	node              *Node
	slot              *peerSlot
	remoteNode        string
	remoteIncarnation string
	endpoint          string
	conn              net.Conn
	send              chan outbound
	control           chan outbound
	done              chan struct{}
	closeOnce         sync.Once
	pendingMu         sync.Mutex
	pending           map[uint64]chan peerResult
	lastRead          atomic.Int64
	lastWrite         atomic.Int64
}

func (n *Node) dialPeer(ctx context.Context, slot *peerSlot, remoteNode, endpoint string) (*peer, bool, error) {
	dialer := net.Dialer{Timeout: n.cfg.DialTimeout, KeepAlive: n.cfg.PingInterval}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, false, fmt.Errorf("%w: dial %s: %v", actor.ErrRemoteUnavailable, remoteNode, err)
	}
	_ = conn.SetDeadline(time.Now().Add(n.cfg.HandshakeTimeout))
	hello, err := n.handshake(wire.Kind_KIND_HELLO)
	if err == nil {
		err = writeEnvelope(conn, hello, n.cfg.MaxPayload)
	}
	var ack *wire.Envelope
	if err == nil {
		ack, err = readEnvelope(conn, n.cfg.MaxPayload)
	}
	if err == nil {
		err = n.verifyHandshake(ack, wire.Kind_KIND_HELLO_ACK)
	}
	if err == nil && ack.GetSourceNode() != remoteNode {
		err = fmt.Errorf("cluster: connected node=%s want=%s", ack.GetSourceNode(), remoteNode)
	}
	if err != nil {
		_ = conn.Close()
		return nil, true, fmt.Errorf("%w: handshake %s: %v", actor.ErrRemoteUnavailable, remoteNode, err)
	}
	_ = conn.SetDeadline(time.Time{})
	now := time.Now().UnixNano()
	p := &peer{
		node: n, slot: slot, remoteNode: remoteNode, remoteIncarnation: ack.GetIncarnation(), endpoint: endpoint,
		conn: conn, send: make(chan outbound, n.cfg.SendQueue), control: make(chan outbound, 32),
		done: make(chan struct{}), pending: make(map[uint64]chan peerResult),
	}
	p.lastRead.Store(now)
	p.lastWrite.Store(now)
	n.wg.Add(2)
	go func() { defer n.wg.Done(); p.writeLoop() }()
	go func() { defer n.wg.Done(); p.readLoop() }()
	return p, false, nil
}

func (p *peer) isClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *peer) enqueue(ctx context.Context, item outbound, nonBlocking bool) error {
	if nonBlocking {
		select {
		case <-p.done:
			return actor.ErrRemoteUnavailable
		default:
		}
		select {
		case p.send <- item:
			return nil
		default:
			return actor.ErrTransportBackpressure
		}
	}
	select {
	case <-p.done:
		return actor.ErrRemoteUnavailable
	case <-ctx.Done():
		return ctx.Err()
	case p.send <- item:
		return nil
	}
}

func (p *peer) enqueueControl(envelope *wire.Envelope) {
	select {
	case <-p.done:
	case p.control <- outbound{envelope: envelope}:
	default:
		p.close(actor.ErrRemoteUnavailable)
	}
}

func (p *peer) addPending(id uint64, response chan peerResult) {
	p.pendingMu.Lock()
	p.pending[id] = response
	p.pendingMu.Unlock()
}

func (p *peer) removePending(id uint64) {
	p.pendingMu.Lock()
	delete(p.pending, id)
	p.pendingMu.Unlock()
}

func (p *peer) complete(envelope *wire.Envelope) {
	p.pendingMu.Lock()
	response := p.pending[envelope.GetRequestId()]
	delete(p.pending, envelope.GetRequestId())
	p.pendingMu.Unlock()
	if response != nil {
		response <- peerResult{envelope: envelope}
	} else {
		p.node.counters.lateResponses.Add(1)
	}
}

func (p *peer) reportSendFailure(item outbound, err error) {
	if !item.notify {
		return
	}
	p.node.counters.sendWriteFailures.Add(1)
	p.node.system.ReportAsyncError(actor.AsyncError{
		Service: item.target.Service, Protocol: item.protocol, RemoteNode: item.target.Node,
		Stage: actor.AsyncErrorTransportWrite, Err: err,
	})
}

func (p *peer) close(err error) {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.conn.Close()
		p.pendingMu.Lock()
		pending := p.pending
		p.pending = make(map[uint64]chan peerResult)
		p.pendingMu.Unlock()
		for _, response := range pending {
			response <- peerResult{err: err}
		}
		for {
			select {
			case item := <-p.send:
				p.reportSendFailure(item, err)
			default:
				p.slot.peerClosed(p)
				return
			}
		}
	})
}

func (p *peer) write(item outbound) bool {
	_ = p.conn.SetWriteDeadline(time.Now().Add(p.node.cfg.WriteTimeout))
	if err := writeEnvelope(p.conn, item.envelope, p.node.cfg.MaxPayload); err != nil {
		p.reportSendFailure(item, actor.ErrRemoteUnavailable)
		p.close(actor.ErrRemoteUnavailable)
		return false
	}
	p.lastWrite.Store(time.Now().UnixNano())
	return true
}

func (p *peer) writeLoop() {
	ticker := time.NewTicker(p.node.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case item := <-p.control:
			if !p.write(item) {
				return
			}
		default:
		}
		select {
		case <-p.done:
			return
		case item := <-p.control:
			if !p.write(item) {
				return
			}
		case item := <-p.send:
			if !p.write(item) {
				return
			}
		case now := <-ticker.C:
			if now.Sub(atomicTime(&p.lastRead)) >= p.node.cfg.IdleTimeout {
				p.node.counters.heartbeatTimeouts.Add(1)
				p.close(actor.ErrRemoteUnavailable)
				return
			}
			ping := &wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PING, SourceNode: p.node.cfg.NodeID, RequestId: p.node.nextRequest.Add(1)}
			if !p.write(outbound{envelope: ping}) {
				return
			}
		}
	}
}

func (p *peer) readLoop() {
	for {
		envelope, err := readEnvelope(p.conn, p.node.cfg.MaxPayload)
		if err != nil {
			p.close(actor.ErrRemoteUnavailable)
			return
		}
		p.lastRead.Store(time.Now().UnixNano())
		if envelope.GetVersion() != protocolVersion || envelope.GetSourceNode() != p.remoteNode {
			p.node.counters.protocolErrors.Add(1)
			p.close(actor.ErrRemoteUnavailable)
			return
		}
		switch envelope.GetKind() {
		case wire.Kind_KIND_RESOLVE_RESPONSE, wire.Kind_KIND_RESPONSE:
			p.complete(envelope)
		case wire.Kind_KIND_PING:
			p.enqueueControl(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: p.node.cfg.NodeID, RequestId: envelope.GetRequestId()})
		case wire.Kind_KIND_PONG:
		default:
			p.node.counters.protocolErrors.Add(1)
		}
	}
}

func (slot *peerSlot) peerClosed(p *peer) {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.peer != p {
		return
	}
	slot.peer = nil
	slot.state = PeerBackoff
	slot.failures++
	slot.backoffUntil = time.Now().Add(p.node.backoff(slot.failures))
}

func (slot *peerSlot) snapshot(nodeID string, index int) PeerStats {
	slot.mu.Lock()
	defer slot.mu.Unlock()
	result := PeerStats{NodeID: nodeID, Slot: index, Endpoint: slot.endpoint, State: slot.state, BackoffUntil: slot.backoffUntil, ConnectFailures: slot.connectFailures, HandshakeFailures: slot.handshakeFailures, Reconnects: slot.reconnects}
	if slot.peer != nil {
		result.Incarnation = slot.peer.remoteIncarnation
		result.QueueDepth = len(slot.peer.send)
		result.QueueCapacity = cap(slot.peer.send)
		slot.peer.pendingMu.Lock()
		result.PendingCalls = len(slot.peer.pending)
		slot.peer.pendingMu.Unlock()
		result.LastRead = atomicTime(&slot.peer.lastRead)
		result.LastWrite = atomicTime(&slot.peer.lastWrite)
	}
	return result
}

func (n *Node) backoff(failures uint32) time.Duration {
	delay := n.cfg.ConnectBackoffMin
	for i := uint32(1); i < failures && delay < n.cfg.ConnectBackoffMax/2; i++ {
		delay *= 2
	}
	if delay > n.cfg.ConnectBackoffMax {
		delay = n.cfg.ConnectBackoffMax
	}
	jitter := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(delay) * jitter)
}
