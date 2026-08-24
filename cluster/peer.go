package cluster

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
)

type peerSlot struct {
	mu   sync.Mutex
	peer *peer
}
type peerResult struct {
	envelope *wire.Envelope
	err      error
}

type peer struct {
	node              *Node
	remoteNode        string
	remoteIncarnation string
	endpoint          string
	conn              net.Conn
	send              chan *wire.Envelope
	done              chan struct{}
	closeOnce         sync.Once
	pendingMu         sync.Mutex
	pending           map[uint64]chan peerResult
}

func (n *Node) dialPeer(ctx context.Context, remoteNode, endpoint string) (*peer, error) {
	dialer := net.Dialer{Timeout: n.cfg.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", endpoint)
	if err != nil {
		return nil, fmt.Errorf("%w: dial %s: %v", actor.ErrRemoteUnavailable, remoteNode, err)
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
		return nil, fmt.Errorf("%w: handshake %s: %v", actor.ErrRemoteUnavailable, remoteNode, err)
	}
	_ = conn.SetDeadline(time.Time{})
	peer := &peer{
		node: n, remoteNode: remoteNode, remoteIncarnation: ack.GetIncarnation(), endpoint: endpoint,
		conn: conn, send: make(chan *wire.Envelope, n.cfg.SendQueue), done: make(chan struct{}),
		pending: make(map[uint64]chan peerResult),
	}
	n.wg.Add(2)
	go func() { defer n.wg.Done(); peer.writeLoop() }()
	go func() { defer n.wg.Done(); peer.readLoop() }()
	return peer, nil
}

func (p *peer) isClosed() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

func (p *peer) enqueue(ctx context.Context, envelope *wire.Envelope, nonBlocking bool) error {
	if nonBlocking {
		select {
		case <-p.done:
			return actor.ErrRemoteUnavailable
		default:
		}
		select {
		case p.send <- envelope:
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
	case p.send <- envelope:
		return nil
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
	}
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
	})
}

func (p *peer) writeLoop() {
	for {
		select {
		case <-p.done:
			return
		case envelope := <-p.send:
			_ = p.conn.SetWriteDeadline(time.Now().Add(p.node.cfg.WriteTimeout))
			if err := writeEnvelope(p.conn, envelope, p.node.cfg.MaxPayload); err != nil {
				p.close(actor.ErrRemoteUnavailable)
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
		if envelope.GetVersion() != protocolVersion || envelope.GetSourceNode() != p.remoteNode {
			p.close(actor.ErrRemoteUnavailable)
			return
		}
		switch envelope.GetKind() {
		case wire.Kind_KIND_RESOLVE_RESPONSE, wire.Kind_KIND_RESPONSE:
			p.complete(envelope)
		case wire.Kind_KIND_PING:
			_ = p.enqueue(context.Background(), &wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: p.node.cfg.NodeID, RequestId: envelope.GetRequestId()}, true)
		}
	}
}
