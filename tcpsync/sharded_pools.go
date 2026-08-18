package tcpsync

import "fmt"

// ShardedPools is a fixed set of Pools sharded by UID.
// Callers use For(uid) to get the pool for a given user; uid==0 always routes to shard 0.
type ShardedPools struct {
	shards []*Pool
}

// NewShardedPools creates one Pool per address.
func NewShardedPools(addrs []string, opts Options) (*ShardedPools, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("tcpsync: at least one address required")
	}
	shards := make([]*Pool, len(addrs))
	for i, addr := range addrs {
		pool, err := New(addr, opts)
		if err != nil {
			for j := 0; j < i; j++ {
				shards[j].Close()
			}
			return nil, fmt.Errorf("tcpsync: shard %d addr %q: %w", i, addr, err)
		}
		shards[i] = pool
	}
	return &ShardedPools{shards: shards}, nil
}

// For returns the Pool for the given uid (uid%len(shards); uid==0 → shard 0).
func (s *ShardedPools) For(uid uint32) *Pool {
	if len(s.shards) == 1 || uid == 0 {
		return s.shards[0]
	}
	return s.shards[uid%uint32(len(s.shards))]
}

// Len returns the number of shards.
func (s *ShardedPools) Len() int { return len(s.shards) }

// At returns the Pool at a specific shard index (used for broadcast-to-all-shards patterns).
// Panics if idx is out of range.
func (s *ShardedPools) At(idx int) *Pool { return s.shards[idx] }

// Close closes all shard pools.
func (s *ShardedPools) Close() {
	for _, p := range s.shards {
		p.Close()
	}
}
