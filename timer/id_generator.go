package timer

import (
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

const (
	snowflakeEpochMs      int64  = 1704067200000 // 2024-01-01T00:00:00Z
	snowflakeNodeBits            = 10
	snowflakeSequenceBits        = 12
	snowflakeMaxNodeID    uint16 = (1 << snowflakeNodeBits) - 1
	snowflakeMaxSequence  uint16 = (1 << snowflakeSequenceBits) - 1
	maxClockBackoff              = 5 * time.Millisecond
)

// IDGenerator generates globally unique timer IDs with a Snowflake-like layout:
// 41 bits timestamp(ms), 10 bits node id, and 12 bits sequence.
type IDGenerator struct {
	mu        sync.Mutex
	nodeID    uint16
	lastMs    int64
	sequence  uint16
	now       func() time.Time
	sleep     func(time.Duration)
	maxBack   time.Duration
	epochMs   int64
	seqMask   uint16
	nodeShift uint
	timeShift uint
}

func NewIDGenerator(nodeID uint16) *IDGenerator {
	if nodeID > snowflakeMaxNodeID {
		nodeID = nodeID & snowflakeMaxNodeID
	}
	return &IDGenerator{
		nodeID:    nodeID,
		now:       time.Now,
		sleep:     time.Sleep,
		maxBack:   maxClockBackoff,
		epochMs:   snowflakeEpochMs,
		seqMask:   snowflakeMaxSequence,
		nodeShift: snowflakeSequenceBits,
		timeShift: snowflakeNodeBits + snowflakeSequenceBits,
	}
}

func NewIDGeneratorFromNodeName(nodeName string) *IDGenerator {
	return NewIDGenerator(NodeNameToSnowflakeNodeID(nodeName))
}

func NodeNameToSnowflakeNodeID(nodeName string) uint16 {
	if nodeName == "" {
		nodeName = "default"
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeName))
	return uint16(h.Sum32() & uint32(snowflakeMaxNodeID))
}

func (g *IDGenerator) Next() (uint64, error) {
	if g == nil {
		return 0, fmt.Errorf("timer id generator is nil")
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	nowMs := g.now().UnixMilli()
	if nowMs < g.lastMs {
		backoff := time.Duration(g.lastMs-nowMs) * time.Millisecond
		if backoff > g.maxBack {
			return 0, fmt.Errorf("clock moved backwards by %s", backoff)
		}
		g.sleep(backoff)
		nowMs = g.now().UnixMilli()
		if nowMs < g.lastMs {
			nowMs = g.lastMs
		}
	}

	if nowMs == g.lastMs {
		g.sequence = (g.sequence + 1) & g.seqMask
		if g.sequence == 0 {
			for nowMs <= g.lastMs {
				g.sleep(time.Millisecond)
				nowMs = g.now().UnixMilli()
			}
		}
	} else {
		g.sequence = 0
	}
	g.lastMs = nowMs

	elapsed := nowMs - g.epochMs
	if elapsed < 0 {
		return 0, fmt.Errorf("current time is before timer id epoch")
	}
	return (uint64(elapsed) << g.timeShift) | (uint64(g.nodeID) << g.nodeShift) | uint64(g.sequence), nil
}
