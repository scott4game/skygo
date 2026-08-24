package cluster

import (
	"sort"
	"sync/atomic"
	"time"
)

// PeerState describes the lifecycle of one fixed-hash connection lane.
type PeerState string

const (
	PeerDisconnected PeerState = "disconnected"
	PeerConnecting   PeerState = "connecting"
	PeerReady        PeerState = "ready"
	PeerBackoff      PeerState = "backoff"
	PeerClosing      PeerState = "closing"
)

// PeerStats is a point-in-time view of one outbound connection lane.
type PeerStats struct {
	NodeID            string
	Slot              int
	Endpoint          string
	Incarnation       string
	State             PeerState
	QueueDepth        int
	QueueCapacity     int
	PendingCalls      int
	LastRead          time.Time
	LastWrite         time.Time
	BackoffUntil      time.Time
	ConnectFailures   uint64
	HandshakeFailures uint64
	Reconnects        uint64
}

// ClusterCounters contains cumulative node transport counters.
type ClusterCounters struct {
	Reconnects        uint64
	HeartbeatTimeouts uint64
	CallTimeouts      uint64
	LateResponses     uint64
	InboundRejected   uint64
	SendsDropped      uint64
	SendWriteFailures uint64
	ProtocolErrors    uint64
	CancelsSent       uint64
	CancelsReceived   uint64
}

// ClusterStats is an immutable snapshot of Node state.
type ClusterStats struct {
	Revision           string
	InboundConnections int
	InboundQueued      int
	InboundActiveCalls int
	Counters           ClusterCounters
	Peers              []PeerStats
}

type clusterCounters struct {
	reconnects, heartbeatTimeouts, callTimeouts, lateResponses       atomic.Uint64
	inboundRejected, sendsDropped, sendWriteFailures, protocolErrors atomic.Uint64
	cancelsSent, cancelsReceived                                     atomic.Uint64
}

func (n *Node) Stats() ClusterStats {
	n.mu.RLock()
	result := ClusterStats{Revision: n.revision, InboundConnections: len(n.inbound)}
	for nodeID, slots := range n.slots {
		for index, slot := range slots {
			result.Peers = append(result.Peers, slot.snapshot(nodeID, index))
		}
	}
	n.mu.RUnlock()
	if n.dispatcher != nil {
		result.InboundQueued, result.InboundActiveCalls = n.dispatcher.stats()
	}
	result.Counters = ClusterCounters{
		Reconnects: n.counters.reconnects.Load(), HeartbeatTimeouts: n.counters.heartbeatTimeouts.Load(),
		CallTimeouts: n.counters.callTimeouts.Load(), LateResponses: n.counters.lateResponses.Load(),
		InboundRejected: n.counters.inboundRejected.Load(), SendsDropped: n.counters.sendsDropped.Load(),
		SendWriteFailures: n.counters.sendWriteFailures.Load(), ProtocolErrors: n.counters.protocolErrors.Load(),
		CancelsSent: n.counters.cancelsSent.Load(), CancelsReceived: n.counters.cancelsReceived.Load(),
	}
	sort.Slice(result.Peers, func(i, j int) bool {
		if result.Peers[i].NodeID == result.Peers[j].NodeID {
			return result.Peers[i].Slot < result.Peers[j].Slot
		}
		return result.Peers[i].NodeID < result.Peers[j].NodeID
	})
	return result
}

func atomicTime(value *atomic.Int64) time.Time {
	if nanos := value.Load(); nanos > 0 {
		return time.Unix(0, nanos)
	}
	return time.Time{}
}
