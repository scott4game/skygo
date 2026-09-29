package cluster

import (
	"context"
	"errors"
	"github.com/scott4game/skygo/cluster/internal/wire"
	"time"
)

const admissionBarrierProtocol = "__skygo_admission_barrier_v1"

func (c *inboundConn) beginAdmission() uint64 {
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.admissionNext++
	if c.admissionChanged == nil {
		c.admissionChanged = make(chan struct{})
	}
	if c.admissionFinished == nil {
		c.admissionFinished = map[uint64]bool{}
	}
	return c.admissionNext
}
func (c *inboundConn) finishAdmission(sequence uint64) {
	if sequence == 0 {
		return
	}
	c.admissionMu.Lock()
	defer c.admissionMu.Unlock()
	c.admissionFinished[sequence] = true
	for c.admissionFinished[c.admissionDone+1] {
		delete(c.admissionFinished, c.admissionDone+1)
		c.admissionDone++
	}
	close(c.admissionChanged)
	c.admissionChanged = make(chan struct{})
}
func (c *inboundConn) barrier(request *wire.Envelope) {
	c.admissionMu.Lock()
	sequence := c.admissionNext
	if c.barrierPending {
		c.admissionMu.Unlock()
		_ = c.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: c.node.cfg.NodeID, RequestId: request.GetRequestId(), Protocol: "barrier_busy"})
		return
	}
	c.barrierPending = true
	c.admissionMu.Unlock()
	c.node.wg.Add(1)
	go func() {
		defer c.node.wg.Done()
		defer func() { c.admissionMu.Lock(); c.barrierPending = false; c.admissionMu.Unlock() }()
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		defer cancel()
		for {
			c.admissionMu.Lock()
			complete := c.admissionDone >= sequence
			changed := c.admissionChanged
			c.admissionMu.Unlock()
			if complete {
				_ = c.write(&wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PONG, SourceNode: c.node.cfg.NodeID, RequestId: request.GetRequestId(), Protocol: admissionBarrierProtocol})
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-changed:
			}
		}
	}()
}

// FlushOutgoing waits until every frame preceding its marker has been admitted
// by each connected peer. It must follow local producer/actor draining. A peer
// without barrier support is explicitly rejected, never treated as drained.
func (n *Node) FlushOutgoing(ctx context.Context) error {
	n.peers.mu.RLock()
	slots := []*peerSlot{}
	for _, group := range n.peers.slots {
		slots = append(slots, group...)
	}
	n.peers.mu.RUnlock()
	peers := []*peer{}
	for _, slot := range slots {
		slot.mu.Lock()
		p, used := slot.peer, slot.everConnected
		slot.mu.Unlock()
		if p == nil {
			if used {
				return errors.New("cluster: cannot prove admission on disconnected peer")
			}
			continue
		}
		peers = append(peers, p)
	}
	for _, p := range peers {
		id := n.nextRequest.Add(1)
		response := make(chan peerResult, 1)
		p.addPending(id, response)
		ping := &wire.Envelope{Version: protocolVersion, Kind: wire.Kind_KIND_PING, SourceNode: n.cfg.NodeID, RequestId: id, Protocol: admissionBarrierProtocol}
		if err := p.enqueue(ctx, outbound{envelope: ping}, false); err != nil {
			p.removePending(id)
			return err
		}
		select {
		case result := <-response:
			if result.err != nil {
				return result.err
			}
			if result.envelope.GetProtocol() != admissionBarrierProtocol {
				return errors.New("cluster: peer does not support admission barriers")
			}
		case <-ctx.Done():
			p.removePending(id)
			return ctx.Err()
		}
	}
	return nil
}
