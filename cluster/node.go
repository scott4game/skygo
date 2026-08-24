package cluster

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
	"github.com/scott4game/skygo/skylog"
)

// Node is a Skynet-style named-node transport for one actor System.
type Node struct {
	cfg         Config
	system      *actor.System
	incarnation string

	mu        sync.RWMutex
	listener  net.Listener
	endpoints map[string]string
	revision  string
	slots     map[string][]*peerSlot
	inbound   map[*inboundConn]struct{}
	started   bool
	stopping  bool

	nextRequest atomic.Uint64
	nonceMu     sync.Mutex
	nonces      map[string]time.Time
	wg          sync.WaitGroup
}

func New(cfg Config, system *actor.System) (*Node, error) {
	if system == nil {
		return nil, fmt.Errorf("cluster: actor system is required")
	}
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	incarnation, err := newIncarnation()
	if err != nil {
		return nil, fmt.Errorf("cluster: incarnation: %w", err)
	}
	node := &Node{
		cfg: cfg, system: system, incarnation: incarnation,
		endpoints: make(map[string]string), slots: make(map[string][]*peerSlot),
		inbound: make(map[*inboundConn]struct{}), nonces: make(map[string]time.Time),
	}
	if err := system.AttachRemoteTransport(cfg.NodeID, node); err != nil {
		return nil, err
	}
	return node, nil
}

func (n *Node) Start(ctx context.Context) error {
	snapshot, err := n.cfg.Registry.Load(ctx)
	if err != nil {
		return err
	}
	if snapshot.endpoints()[n.cfg.NodeID] == "" {
		return fmt.Errorf("cluster: registry does not contain local node %q", n.cfg.NodeID)
	}
	listener, err := net.Listen("tcp", n.cfg.Listen)
	if err != nil {
		return fmt.Errorf("cluster: listen: %w", err)
	}
	n.mu.Lock()
	if n.started || n.stopping {
		n.mu.Unlock()
		_ = listener.Close()
		return fmt.Errorf("cluster: node already started")
	}
	n.endpoints = snapshot.endpoints()
	n.revision = snapshot.Revision
	n.listener = listener
	n.started = true
	n.mu.Unlock()
	n.wg.Add(1)
	go n.acceptLoop(listener)
	return nil
}

func (n *Node) Addr() net.Addr {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.listener == nil {
		return nil
	}
	return n.listener.Addr()
}

func (n *Node) Revision() string { n.mu.RLock(); defer n.mu.RUnlock(); return n.revision }

// Reload atomically installs a complete registry snapshot and closes cached
// senders and inbound connections for changed or removed nodes.
func (n *Node) Reload(ctx context.Context) error {
	snapshot, err := n.cfg.Registry.Load(ctx)
	if err != nil {
		return err
	}
	next := snapshot.endpoints()
	if next[n.cfg.NodeID] == "" {
		return fmt.Errorf("cluster: registry does not contain local node %q", n.cfg.NodeID)
	}
	var closePeers []*peer
	var closeInbound []*inboundConn
	n.mu.Lock()
	for nodeID, slots := range n.slots {
		if n.endpoints[nodeID] == next[nodeID] && next[nodeID] != "" {
			continue
		}
		for _, slot := range slots {
			slot.mu.Lock()
			if slot.peer != nil {
				closePeers = append(closePeers, slot.peer)
				slot.peer = nil
			}
			slot.mu.Unlock()
		}
		delete(n.slots, nodeID)
	}
	for conn := range n.inbound {
		if n.endpoints[conn.remoteNode] != next[conn.remoteNode] || next[conn.remoteNode] == "" {
			closeInbound = append(closeInbound, conn)
		}
	}
	n.endpoints = next
	n.revision = snapshot.Revision
	n.mu.Unlock()
	for _, peer := range closePeers {
		peer.close(actor.ErrRemoteUnavailable)
	}
	for _, conn := range closeInbound {
		conn.close()
	}
	return nil
}

func (n *Node) Stop(ctx context.Context) error {
	n.mu.Lock()
	if n.stopping {
		n.mu.Unlock()
		return nil
	}
	n.stopping = true
	listener := n.listener
	var peers []*peer
	for _, slots := range n.slots {
		for _, slot := range slots {
			slot.mu.Lock()
			if slot.peer != nil {
				peers = append(peers, slot.peer)
				slot.peer = nil
			}
			slot.mu.Unlock()
		}
	}
	var inbound []*inboundConn
	for conn := range n.inbound {
		inbound = append(inbound, conn)
	}
	n.mu.Unlock()
	if listener != nil {
		_ = listener.Close()
	}
	for _, peer := range peers {
		peer.close(actor.ErrRemoteUnavailable)
	}
	for _, conn := range inbound {
		conn.close()
	}
	done := make(chan struct{})
	go func() { n.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Resolve returns a remote actor reference. Method.Call and Notification.Send
// use the same API for this Ref as for a local one.
func (n *Node) Resolve(ctx context.Context, nodeID, service string) (actor.Ref, error) {
	return n.system.ResolveRemote(ctx, nodeID, service)
}

// ResolveTarget implements actor.RemoteTransport.
func (n *Node) ResolveTarget(ctx context.Context, nodeID, service string) (actor.RemoteTarget, error) {
	if nodeID == n.cfg.NodeID {
		ref, err := n.system.Resolve(service)
		if err != nil {
			return actor.RemoteTarget{}, err
		}
		return actor.RemoteTarget{Node: nodeID, Incarnation: n.incarnation, Service: service, Address: ref.Address, Generation: ref.Generation}, nil
	}
	response, err := n.request(ctx, nodeID, 0, &wire.Envelope{Kind: wire.Kind_KIND_RESOLVE_REQUEST, Service: service})
	if err != nil {
		return actor.RemoteTarget{}, err
	}
	if err := decodeError(response.GetError()); err != nil {
		return actor.RemoteTarget{}, err
	}
	target := response.GetTarget()
	if target == nil {
		return actor.RemoteTarget{}, actor.ErrServiceNotFound
	}
	return fromWireTarget(target), nil
}

func (n *Node) Call(ctx context.Context, target actor.RemoteTarget, protocol, fingerprint string, payload []byte, path []actor.CallFrame) ([]byte, error) {
	if target.Node == n.cfg.NodeID {
		if target.Incarnation != n.incarnation {
			return nil, actor.ErrStaleRef
		}
		ctx = actor.WithCallPath(ctx, path)
		return n.system.DispatchRemote(ctx, target, protocol, fingerprint, payload, false)
	}
	envelope := &wire.Envelope{
		Kind: wire.Kind_KIND_CALL, Target: toWireTarget(target), Protocol: protocol,
		Fingerprint: fingerprint, Payload: payload, CallPath: toWirePath(path),
	}
	if traceID, ok := TraceIDFromContext(ctx); ok {
		envelope.TraceId = traceID
	}
	if deadline, ok := ctx.Deadline(); ok {
		envelope.DeadlineUnixNano = deadline.UnixNano()
	}
	response, err := n.request(ctx, target.Node, uint64(target.Address), envelope)
	if err != nil {
		return nil, err
	}
	if err := decodeError(response.GetError()); err != nil {
		return nil, err
	}
	return response.GetPayload(), nil
}

func (n *Node) Send(ctx context.Context, target actor.RemoteTarget, protocol, fingerprint string, payload []byte, path []actor.CallFrame, nonBlocking bool) error {
	if target.Node == n.cfg.NodeID {
		if target.Incarnation != n.incarnation {
			return actor.ErrStaleRef
		}
		ctx = actor.WithCallPath(ctx, path)
		_, err := n.system.DispatchRemote(ctx, target, protocol, fingerprint, payload, true)
		return err
	}
	envelope := &wire.Envelope{
		Version: protocolVersion, Kind: wire.Kind_KIND_SEND, SourceNode: n.cfg.NodeID,
		Target: toWireTarget(target), Protocol: protocol, Fingerprint: fingerprint,
		Payload: payload, CallPath: toWirePath(path),
	}
	if traceID, ok := TraceIDFromContext(ctx); ok {
		envelope.TraceId = traceID
	}
	if deadline, ok := ctx.Deadline(); ok {
		envelope.DeadlineUnixNano = deadline.UnixNano()
	}
	peer, err := n.peerFor(ctx, target.Node, uint64(target.Address))
	if err != nil {
		return err
	}
	if target.Incarnation != peer.remoteIncarnation {
		return actor.ErrStaleRef
	}
	if nonBlocking {
		return peer.enqueue(ctx, envelope, true)
	}
	enqueueCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		enqueueCtx, cancel = context.WithTimeout(ctx, n.cfg.SendTimeout)
		defer cancel()
	}
	err = peer.enqueue(enqueueCtx, envelope, false)
	if errors.Is(err, context.DeadlineExceeded) {
		return actor.ErrTransportBackpressure
	}
	return err
}

func (n *Node) request(ctx context.Context, nodeID string, shard uint64, envelope *wire.Envelope) (*wire.Envelope, error) {
	if ctx == nil {
		return nil, fmt.Errorf("cluster: nil context")
	}
	peer, err := n.peerFor(ctx, nodeID, shard)
	if err != nil {
		return nil, err
	}
	requestID := n.nextRequest.Add(1)
	envelope.Version = protocolVersion
	envelope.RequestId = requestID
	envelope.SourceNode = n.cfg.NodeID
	if traceID, ok := TraceIDFromContext(ctx); ok {
		envelope.TraceId = traceID
	}
	response := make(chan peerResult, 1)
	peer.addPending(requestID, response)
	if err := peer.enqueue(ctx, envelope, false); err != nil {
		peer.removePending(requestID)
		return nil, err
	}
	waitCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, n.cfg.CallTimeout)
		defer cancel()
	}
	select {
	case result := <-response:
		return result.envelope, result.err
	case <-waitCtx.Done():
		peer.removePending(requestID)
		if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
			return nil, actor.ErrCallTimeout
		}
		return nil, waitCtx.Err()
	case <-peer.done:
		peer.removePending(requestID)
		return nil, actor.ErrRemoteUnavailable
	}
}

func (n *Node) endpoint(nodeID string) (string, error) {
	n.mu.RLock()
	endpoint := n.endpoints[nodeID]
	stopping := n.stopping
	n.mu.RUnlock()
	if stopping || endpoint == "" {
		return "", fmt.Errorf("%w: node=%s", actor.ErrRemoteUnavailable, nodeID)
	}
	return endpoint, nil
}

func (n *Node) peerFor(ctx context.Context, nodeID string, shard uint64) (*peer, error) {
	endpoint, err := n.endpoint(nodeID)
	if err != nil {
		return nil, err
	}
	index := int(shard % uint64(n.cfg.ConnectionsPerPeer))
	n.mu.Lock()
	slots := n.slots[nodeID]
	if len(slots) == 0 {
		slots = make([]*peerSlot, n.cfg.ConnectionsPerPeer)
		for i := range slots {
			slots[i] = &peerSlot{}
		}
		n.slots[nodeID] = slots
	}
	slot := slots[index]
	n.mu.Unlock()
	slot.mu.Lock()
	defer slot.mu.Unlock()
	if slot.peer != nil && !slot.peer.isClosed() && slot.peer.endpoint == endpoint {
		return slot.peer, nil
	}
	if slot.peer != nil {
		slot.peer.close(actor.ErrRemoteUnavailable)
	}
	peer, err := n.dialPeer(ctx, nodeID, endpoint)
	if err != nil {
		slot.peer = nil
		return nil, err
	}
	slot.peer = peer
	return peer, nil
}

func toWireTarget(target actor.RemoteTarget) *wire.Target {
	return &wire.Target{Node: target.Node, Incarnation: target.Incarnation, Service: target.Service, Address: uint64(target.Address), Generation: target.Generation}
}

func fromWireTarget(target *wire.Target) actor.RemoteTarget {
	return actor.RemoteTarget{Node: target.GetNode(), Incarnation: target.GetIncarnation(), Service: target.GetService(), Address: actor.Address(target.GetAddress()), Generation: target.GetGeneration()}
}

func toWirePath(path []actor.CallFrame) []*wire.CallFrame {
	result := make([]*wire.CallFrame, 0, len(path))
	for _, frame := range path {
		result = append(result, &wire.CallFrame{Node: frame.Node, Service: frame.Service, Protocol: frame.Protocol})
	}
	return result
}

func fromWirePath(path []*wire.CallFrame) []actor.CallFrame {
	result := make([]actor.CallFrame, 0, len(path))
	for _, frame := range path {
		result = append(result, actor.CallFrame{Node: frame.GetNode(), Service: frame.GetService(), Protocol: frame.GetProtocol()})
	}
	return result
}

func (n *Node) logAsync(err error) {
	if err != nil {
		skylog.Errorf(context.Background(), "cluster node %s: %v", n.cfg.NodeID, err)
	}
}
