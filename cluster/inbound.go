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

type inboundConn struct {
	node              *Node
	conn              net.Conn
	remoteNode        string
	remoteIncarnation string
	writeMu           sync.Mutex
	closeOnce         sync.Once
}

func (n *Node) acceptLoop(listener net.Listener) {
	defer n.wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			n.mu.RLock()
			stopping := n.stopping
			n.mu.RUnlock()
			if stopping {
				return
			}
			continue
		}
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.acceptConn(conn) }()
	}
}

func (n *Node) acceptConn(raw net.Conn) {
	_ = raw.SetDeadline(time.Now().Add(n.cfg.HandshakeTimeout))
	hello, err := readEnvelope(raw, n.cfg.MaxPayload)
	if err == nil {
		err = n.verifyHandshake(hello, wire.Kind_KIND_HELLO)
	}
	if err == nil {
		n.mu.RLock()
		_, known := n.endpoints[hello.GetSourceNode()]
		n.mu.RUnlock()
		if !known || hello.GetSourceNode() == n.cfg.NodeID {
			err = fmt.Errorf("cluster: unknown or self peer %q", hello.GetSourceNode())
		}
	}
	if err != nil {
		_ = raw.Close()
		return
	}
	ack, err := n.handshake(wire.Kind_KIND_HELLO_ACK)
	if err == nil {
		err = writeEnvelope(raw, ack, n.cfg.MaxPayload)
	}
	if err != nil {
		_ = raw.Close()
		return
	}
	_ = raw.SetDeadline(time.Time{})
	conn := &inboundConn{node: n, conn: raw, remoteNode: hello.GetSourceNode(), remoteIncarnation: hello.GetIncarnation()}
	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		conn.close()
		return
	}
	n.inbound[conn] = struct{}{}
	n.mu.Unlock()
	defer func() { conn.close(); n.mu.Lock(); delete(n.inbound, conn); n.mu.Unlock() }()
	for {
		envelope, err := readEnvelope(raw, n.cfg.MaxPayload)
		if err != nil {
			return
		}
		if envelope.GetVersion() != protocolVersion || envelope.GetSourceNode() != conn.remoteNode {
			return
		}
		switch envelope.GetKind() {
		case wire.Kind_KIND_RESOLVE_REQUEST, wire.Kind_KIND_CALL, wire.Kind_KIND_SEND:
			n.wg.Add(1)
			go func() { defer n.wg.Done(); conn.handle(envelope) }()
		case wire.Kind_KIND_PING:
			_ = conn.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: n.cfg.NodeID, RequestId: envelope.GetRequestId()})
		}
	}
}

func (c *inboundConn) close() { c.closeOnce.Do(func() { _ = c.conn.Close() }) }

func (c *inboundConn) write(envelope *wire.Envelope) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.node.cfg.WriteTimeout))
	return writeEnvelope(c.conn, envelope, c.node.cfg.MaxPayload)
}

func (c *inboundConn) handle(envelope *wire.Envelope) {
	switch envelope.GetKind() {
	case wire.Kind_KIND_RESOLVE_REQUEST:
		response := &wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_RESOLVE_RESPONSE, SourceNode: c.node.cfg.NodeID, RequestId: envelope.GetRequestId()}
		ref, err := c.node.system.Resolve(envelope.GetService())
		if err != nil {
			response.Error = encodeError(err)
		} else {
			response.Target = &wire.Target{Node: c.node.cfg.NodeID, Incarnation: c.node.incarnation, Service: envelope.GetService(), Address: uint64(ref.Address), Generation: ref.Generation}
		}
		if err := c.write(response); err != nil {
			c.close()
		}
	case wire.Kind_KIND_CALL, wire.Kind_KIND_SEND:
		c.dispatch(envelope)
	}
}

func (c *inboundConn) dispatch(envelope *wire.Envelope) {
	target := envelope.GetTarget()
	if target == nil || target.GetNode() != c.node.cfg.NodeID || target.GetIncarnation() != c.node.incarnation {
		if envelope.GetKind() == wire.Kind_KIND_CALL {
			_ = c.writeResponse(envelope.GetRequestId(), nil, actor.ErrStaleRef)
		}
		return
	}
	ctx := actor.WithCallPath(context.Background(), fromWirePath(envelope.GetCallPath()))
	ctx = withPeer(ctx, c.remoteNode)
	if envelope.GetTraceId() != "" {
		ctx = WithTraceID(ctx, envelope.GetTraceId())
	}
	if deadline := envelope.GetDeadlineUnixNano(); deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, deadline))
		defer cancel()
	}
	payload, err := c.node.system.DispatchRemote(ctx, fromWireTarget(target), envelope.GetProtocol(), envelope.GetFingerprint(), envelope.GetPayload(), envelope.GetKind() == wire.Kind_KIND_SEND)
	if envelope.GetKind() == wire.Kind_KIND_CALL {
		if writeErr := c.writeResponse(envelope.GetRequestId(), payload, err); writeErr != nil {
			c.close()
		}
	} else if err != nil {
		c.node.logAsync(fmt.Errorf("send from %s protocol=%s: %w", c.remoteNode, envelope.GetProtocol(), err))
	}
}

func (c *inboundConn) writeResponse(requestID uint64, payload []byte, err error) error {
	return c.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_RESPONSE, SourceNode: c.node.cfg.NodeID, RequestId: requestID, Payload: payload, Error: encodeError(err)})
}
