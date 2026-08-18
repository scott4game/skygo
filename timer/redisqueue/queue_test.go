package redisqueue

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

type testRecord struct {
	TimerID            uint64 `json:"timer_id"`
	BusinessID         string `json:"business_id"`
	DueMs              int64  `json:"due_ms"`
	TimerStatus        string `json:"timer_status"`
	ProcessingOwner    string `json:"processing_owner"`
	ProcessingDeadline int64  `json:"processing_deadline"`
}

type testCodec struct{}

func (testCodec) Marshal(rec *testRecord) ([]byte, error) {
	return json.Marshal(rec)
}

func (testCodec) Unmarshal(data []byte) (*testRecord, error) {
	var rec testRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (testCodec) TimerID(rec *testRecord) uint64 {
	if rec == nil {
		return 0
	}
	return rec.TimerID
}

func (testCodec) BusinessID(rec *testRecord) string {
	if rec == nil {
		return ""
	}
	return rec.BusinessID
}

func (testCodec) DueTimeMs(rec *testRecord) int64 {
	if rec == nil {
		return 0
	}
	return rec.DueMs
}

func (testCodec) MarkPending(rec *testRecord) {
	if rec == nil {
		return
	}
	rec.TimerStatus = StatusPending
	rec.ProcessingOwner = ""
	rec.ProcessingDeadline = 0
}

func newTestQueue(t *testing.T) (*Queue[*testRecord], Keys, *redis.Client) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	const prefix = "slg:test:rt"
	keysFn := func(uid uint32) Keys {
		u := strconv.FormatUint(uint64(uid), 10)
		return Keys{
			Pending:    prefix + ":pending:" + u,
			Processing: prefix + ":processing:" + u,
			Data:       prefix + ":data:" + u,
			Index:      prefix + ":index:" + u,
		}
	}
	q := NewQueue(rdb, keysFn, testCodec{})
	return q, keysFn(1001), rdb
}

// TestVerify_CompleteProcessing_NeverClaimedPendingMustBeRemoved:
// Add a far-future pending timer, call CompleteProcessing without Claim.
// The ledger row (pending ZSET + data + business index) must be gone.
func TestVerify_CompleteProcessing_NeverClaimedPendingMustBeRemoved(t *testing.T) {
	const (
		uid        = uint32(1001)
		timerID    = uint64(9001)
		businessID = "troop-9001"
		owner      = "node-test"
	)
	q, keys, rdb := newTestQueue(t)
	ctx := context.Background()

	rec := &testRecord{
		TimerID:    timerID,
		BusinessID: businessID,
		DueMs:      time.Now().Add(2 * time.Hour).UnixMilli(),
	}
	if err := q.Add(ctx, uid, rec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	active, err := q.LoadActiveByUID(ctx, uid)
	if err != nil || len(active) != 1 {
		t.Fatalf("precondition LoadActiveByUID: n=%d err=%v", len(active), err)
	}
	if active[0].TimerStatus != StatusPending {
		t.Fatalf("precondition: want pending, got %q", active[0].TimerStatus)
	}

	// Complete a manually cancelled timer without claiming it first.
	completed, err := q.CompleteProcessing(ctx, uid, timerID, owner)
	if err != nil {
		t.Fatalf("CompleteProcessing: %v", err)
	}
	if !completed {
		t.Fatal("CompleteProcessing returned false for a never-claimed pending timer")
	}

	active, err = q.LoadActiveByUID(ctx, uid)
	if err != nil {
		t.Fatalf("LoadActiveByUID after complete: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("BUG: pending timer still in ledger after CompleteProcessing: n=%d status=%q",
			len(active), active[0].TimerStatus)
	}

	nPending, err := rdb.ZCard(ctx, keys.Pending).Result()
	if err != nil {
		t.Fatalf("ZCard pending: %v", err)
	}
	if nPending != 0 {
		t.Fatalf("BUG: pending ZSET still has %d members after CompleteProcessing", nPending)
	}
	exists, err := rdb.HExists(ctx, keys.Data, strconv.FormatUint(timerID, 10)).Result()
	if err != nil {
		t.Fatalf("HExists data: %v", err)
	}
	if exists {
		t.Fatal("BUG: data hash still holds timer after CompleteProcessing")
	}
	idx, err := rdb.HGet(ctx, keys.Index, businessID).Result()
	if err != redis.Nil && err != nil {
		t.Fatalf("HGet index: %v", err)
	}
	if idx != "" {
		t.Fatalf("BUG: business index still maps %s → %s after CompleteProcessing", businessID, idx)
	}
}

// TestAdd_SameBusinessIDReplacesOldTimerID: re-scheduling the same business ID
// with a new timerID must drop the previous pending/data row.
func TestAdd_SameBusinessIDReplacesOldTimerID(t *testing.T) {
	const (
		uid        = uint32(1003)
		businessID = "troop-9003"
		oldTimer   = uint64(9003)
		newTimer   = uint64(9004)
	)
	q, _, _ := newTestQueue(t)
	ctx := context.Background()

	if err := q.Add(ctx, uid, &testRecord{
		TimerID: oldTimer, BusinessID: businessID,
		DueMs: time.Now().Add(2 * time.Hour).UnixMilli(),
	}); err != nil {
		t.Fatalf("Add old: %v", err)
	}
	if err := q.Add(ctx, uid, &testRecord{
		TimerID: newTimer, BusinessID: businessID,
		DueMs: time.Now().Add(30 * time.Second).UnixMilli(),
	}); err != nil {
		t.Fatalf("Add new: %v", err)
	}
	active, err := q.LoadActiveByUID(ctx, uid)
	if err != nil {
		t.Fatalf("LoadActiveByUID: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("BUG: Add same businessID left %d rows, want 1 (old timer orphan)", len(active))
	}
	if active[0].TimerID != newTimer {
		t.Fatalf("want timerID=%d, got %d", newTimer, active[0].TimerID)
	}
}

// TestCompleteProcessing_ClaimedThenComplete_StillWorks locks the happy path
// that already works: ClaimForProcessing then CompleteProcessing with same owner.
func TestCompleteProcessing_ClaimedThenComplete_StillWorks(t *testing.T) {
	const (
		uid        = uint32(1002)
		timerID    = uint64(9002)
		businessID = "troop-9002"
		owner      = "node-test"
	)
	q, _, _ := newTestQueue(t)
	ctx := context.Background()

	rec := &testRecord{
		TimerID:    timerID,
		BusinessID: businessID,
		DueMs:      time.Now().Add(time.Minute).UnixMilli(),
	}
	if err := q.Add(ctx, uid, rec); err != nil {
		t.Fatalf("Add: %v", err)
	}
	claimed, err := q.ClaimForProcessing(ctx, uid, timerID, owner, 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimForProcessing: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimForProcessing returned false")
	}

	completed, err := q.CompleteProcessing(ctx, uid, timerID, owner)
	if err != nil {
		t.Fatalf("CompleteProcessing: %v", err)
	}
	if !completed {
		t.Fatal("CompleteProcessing returned false after successful claim")
	}

	active, err := q.LoadActiveByUID(ctx, uid)
	if err != nil {
		t.Fatalf("LoadActiveByUID: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("want empty ledger after claim+complete, got n=%d", len(active))
	}
}
