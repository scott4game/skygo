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
	calls             sync.Map // request id -> *inboundJob
	ctx               context.Context
	cancel            context.CancelFunc
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
		known := n.registry.known(hello.GetSourceNode())
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
	connCtx, connCancel := context.WithCancel(n.ctx)
	conn := &inboundConn{node: n, conn: raw, remoteNode: hello.GetSourceNode(), remoteIncarnation: hello.GetIncarnation(), ctx: connCtx, cancel: connCancel}
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
		envelope, readErr := readEnvelope(raw, n.cfg.MaxPayload)
		if readErr != nil {
			return
		}
		if envelope.GetVersion() != protocolVersion || envelope.GetSourceNode() != conn.remoteNode {
			n.counters.protocolErrors.Add(1)
			return
		}
		switch envelope.GetKind() {
		case wire.Kind_KIND_RESOLVE_REQUEST:
			conn.resolve(envelope)
		case wire.Kind_KIND_CALL:
			job := &inboundJob{conn: conn, envelope: envelope}
			conn.calls.Store(envelope.GetRequestId(), job)
			if !n.dispatcher.submit(job) {
				conn.calls.Delete(envelope.GetRequestId())
				n.counters.inboundRejected.Add(1)
				_ = conn.writeResponse(envelope.GetRequestId(), nil, actor.ErrTransportBackpressure)
			}
		case wire.Kind_KIND_SEND:
			job := &inboundJob{conn: conn, envelope: envelope}
			if !n.dispatcher.submit(job) {
				n.counters.inboundRejected.Add(1)
				n.counters.sendsDropped.Add(1)
				service := ""
				if envelope.GetTarget() != nil {
					service = envelope.GetTarget().GetService()
				}
				n.system.ReportAsyncError(actor.AsyncError{Service: service, Protocol: envelope.GetProtocol(), RemoteNode: conn.remoteNode, Stage: actor.AsyncErrorTransportAdmission, Err: actor.ErrTransportBackpressure})
			}
		case wire.Kind_KIND_CANCEL:
			n.counters.cancelsReceived.Add(1)
			if value, ok := conn.calls.Load(envelope.GetRequestId()); ok {
				value.(*inboundJob).canceled.Store(true)
			}
		case wire.Kind_KIND_PING:
			_ = conn.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: n.cfg.NodeID, RequestId: envelope.GetRequestId()})
		case wire.Kind_KIND_PONG:
		default:
			n.counters.protocolErrors.Add(1)
		}
	}
}

func (c *inboundConn) close() {
	c.closeOnce.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
		c.calls.Range(func(_, value any) bool { value.(*inboundJob).canceled.Store(true); return true })
		_ = c.conn.Close()
	})
}

func (c *inboundConn) write(envelope *wire.Envelope) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(c.node.cfg.WriteTimeout))
	return writeEnvelope(c.conn, envelope, c.node.cfg.MaxPayload)
}

func (c *inboundConn) resolve(envelope *wire.Envelope) {
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
}

func (c *inboundConn) forgetCall(requestID uint64) { c.calls.Delete(requestID) }

func (c *inboundConn) writeResponse(requestID uint64, payload []byte, err error) error {
	return c.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_RESPONSE, SourceNode: c.node.cfg.NodeID, RequestId: requestID, Payload: payload, Error: encodeError(err)})
}
