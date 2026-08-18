package redisqueue

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/scott4game/skygo/skylog"
)

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
)

type Keys struct {
	Pending    string
	Processing string
	Data       string
	Index      string
}

type Codec[T any] interface {
	Marshal(T) ([]byte, error)
	Unmarshal([]byte) (T, error)
	TimerID(T) uint64
	BusinessID(T) string
	DueTimeMs(T) int64
	MarkPending(T)
}

type Queue[T any] struct {
	rdb   *redis.Client
	keys  func(uid uint32) Keys
	codec Codec[T]
}

type timerBusinessUnmarshaler[T any] interface {
	UnmarshalWithTimerIDAndBusinessID([]byte, string, string) (T, error)
}

var claimScript = redis.NewScript(`
local val = redis.call('HGET', KEYS[1], ARGV[1])
if not val then return 0 end

if redis.call('ZREM', KEYS[2], ARGV[1]) == 0 then return 0 end

redis.call('HSET', KEYS[1], '__processing__:' .. ARGV[1], cjson.encode({
	owner = ARGV[2],
	deadline = tonumber(ARGV[3])
}))
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 1
`)

var completeScript = redis.NewScript(`
local val = redis.call('HGET', KEYS[1], ARGV[1])
if not val then return 0 end

local meta = redis.call('HGET', KEYS[1], '__processing__:' .. ARGV[1])
if meta then
	local processing = cjson.decode(meta)
	if processing['owner'] ~= ARGV[2] then return 0 end
else
	local rec = cjson.decode(val)
	-- Only enforce owner when the record is already claimed (processing).
	-- A still-pending timer has no claimant. Completing it through a manual
	-- cancellation path must remove the ledger row to prevent restart orphans.
	if rec['timer_status'] == 'processing' then
		if rec['processing_owner'] ~= ARGV[2] then return 0 end
	end
end

redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[1], '__processing__:' .. ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if ARGV[3] ~= '' then
	local idx = redis.call('HGET', KEYS[4], ARGV[3])
	if idx == ARGV[1] then
		redis.call('HDEL', KEYS[4], ARGV[3])
	end
end
return 1
`)

var requeueScript = redis.NewScript(`
local val = redis.call('HGET', KEYS[1], ARGV[1])
if not val then
	redis.call('ZREM', KEYS[3], ARGV[1])
	redis.call('HDEL', KEYS[1], '__processing__:' .. ARGV[1])
	return 0
end

local deadline = 0
local meta = redis.call('HGET', KEYS[1], '__processing__:' .. ARGV[1])
if meta then
	local processing = cjson.decode(meta)
	deadline = tonumber(processing['deadline']) or 0
else
	local rec = cjson.decode(val)
	if rec['timer_status'] ~= 'processing' then return 0 end
	deadline = tonumber(rec['processing_deadline']) or 0
end
if deadline > tonumber(ARGV[2]) then return 0 end

redis.call('HDEL', KEYS[1], '__processing__:' .. ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1
`)

func NewQueue[T any](rdb *redis.Client, keys func(uid uint32) Keys, codec Codec[T]) *Queue[T] {
	return &Queue[T]{rdb: rdb, keys: keys, codec: codec}
}

func (q *Queue[T]) Add(ctx context.Context, uid uint32, rec T) error {
	timerIDStr := strconv.FormatUint(q.codec.TimerID(rec), 10)
	businessID := q.codec.BusinessID(rec)
	q.codec.MarkPending(rec)
	data, err := q.codec.Marshal(rec)
	if err != nil {
		return err
	}
	keys := q.keys(uid)
	// When a business ID is rescheduled with a new timer ID, remove the previous
	// timer from all stores so a reload cannot return duplicate active records.
	if businessID != "" {
		if oldTimerIDStr, err := q.rdb.HGet(ctx, keys.Index, businessID).Result(); err == nil &&
			oldTimerIDStr != "" && oldTimerIDStr != timerIDStr {
			pipe := q.rdb.Pipeline()
			pipe.ZRem(ctx, keys.Pending, oldTimerIDStr)
			pipe.ZRem(ctx, keys.Processing, oldTimerIDStr)
			pipe.HDel(ctx, keys.Data, oldTimerIDStr)
			pipe.HDel(ctx, keys.Data, "__processing__:"+oldTimerIDStr)
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("replace old timer %s for business %s: %w", oldTimerIDStr, businessID, err)
			}
		} else if err != nil && err != redis.Nil {
			return err
		}
	}
	pipe := q.rdb.Pipeline()
	pipe.ZAdd(ctx, keys.Pending, &redis.Z{Score: float64(q.codec.DueTimeMs(rec)), Member: timerIDStr})
	pipe.ZRem(ctx, keys.Processing, timerIDStr)
	pipe.HSet(ctx, keys.Data, timerIDStr, data)
	if businessID != "" {
		pipe.HSet(ctx, keys.Index, businessID, timerIDStr)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (q *Queue[T]) TimerIDByBusinessID(ctx context.Context, uid uint32, businessID string) (uint64, error) {
	if businessID == "" {
		return 0, nil
	}
	keys := q.keys(uid)
	val, err := q.rdb.HGet(ctx, keys.Index, businessID).Result()
	if err == redis.Nil {
		exists, existsErr := q.rdb.HExists(ctx, keys.Data, businessID).Result()
		if existsErr != nil {
			return 0, existsErr
		}
		if exists {
			return strconv.ParseUint(businessID, 10, 64)
		}
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(val, 10, 64)
}

func (q *Queue[T]) RemoveByBusinessID(ctx context.Context, uid uint32, businessID string) (bool, error) {
	timerID, err := q.TimerIDByBusinessID(ctx, uid, businessID)
	if err != nil || timerID == 0 {
		return false, err
	}
	return q.RemoveByTimerID(ctx, uid, timerID)
}

func (q *Queue[T]) RemoveByTimerID(ctx context.Context, uid uint32, timerID uint64) (bool, error) {
	timerIDStr := strconv.FormatUint(timerID, 10)
	keys := q.keys(uid)
	rec, found, _ := q.loadRecordByTimerID(ctx, uid, timerIDStr)
	hdel, err := q.rdb.HDel(ctx, keys.Data, timerIDStr).Result()
	if err != nil {
		return false, err
	}
	pipe := q.rdb.Pipeline()
	pipe.ZRem(ctx, keys.Pending, timerIDStr)
	pipe.ZRem(ctx, keys.Processing, timerIDStr)
	if found {
		if businessID := q.codec.BusinessID(rec); businessID != "" {
			pipe.HDel(ctx, keys.Index, businessID)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return hdel > 0, err
	}
	return hdel > 0, nil
}

func (q *Queue[T]) ClaimForProcessing(ctx context.Context, uid uint32, timerID uint64, owner string, lease time.Duration) (bool, error) {
	if owner == "" {
		panic("redisqueue: owner must be configured")
	}
	if lease <= 0 {
		panic("redisqueue: lease must be positive")
	}
	timerIDStr := strconv.FormatUint(timerID, 10)
	keys := q.keys(uid)
	deadline := time.Now().Add(lease).UnixMilli()
	res, err := claimScript.Run(ctx, q.rdb,
		[]string{keys.Data, keys.Pending, keys.Processing},
		timerIDStr, owner, strconv.FormatInt(deadline, 10),
	).Int()
	return res == 1, err
}

func (q *Queue[T]) CompleteProcessing(ctx context.Context, uid uint32, timerID uint64, owner string) (bool, error) {
	if owner == "" {
		panic("redisqueue: owner must be configured")
	}
	timerIDStr := strconv.FormatUint(timerID, 10)
	keys := q.keys(uid)
	rec, found, err := q.loadRecordByTimerID(ctx, uid, timerIDStr)
	if err != nil {
		return false, err
	}
	businessID := ""
	if found {
		businessID = q.codec.BusinessID(rec)
	}
	res, err := completeScript.Run(ctx, q.rdb,
		[]string{keys.Data, keys.Pending, keys.Processing, keys.Index},
		timerIDStr, owner, businessID,
	).Int()
	return res == 1, err
}

func (q *Queue[T]) RequeueExpiredProcessing(ctx context.Context, uids []uint32, now time.Time) (int, error) {
	total := 0
	maxScore := strconv.FormatInt(now.UnixMilli(), 10)
	for _, uid := range uids {
		keys := q.keys(uid)
		timerIDs, err := q.rdb.ZRangeByScore(ctx, keys.Processing, &redis.ZRangeBy{Min: "0", Max: maxScore}).Result()
		if err != nil {
			return total, fmt.Errorf("zrangebyscore processing uid=%d: %w", uid, err)
		}
		for _, timerIDStr := range timerIDs {
			ok, err := q.requeueProcessingTimer(ctx, uid, timerIDStr, now)
			if err != nil {
				return total, err
			}
			if ok {
				total++
			}
		}
	}
	return total, nil
}

func (q *Queue[T]) LoadActiveByUID(ctx context.Context, uid uint32) ([]T, error) {
	keys := q.keys(uid)
	ids, err := q.rdb.ZRange(ctx, keys.Pending, 0, -1).Result()
	if err != nil {
		skylog.Errorf(ctx, "redisqueue.LoadActiveByUID zrange pending uid=%d: %v", uid, err)
		return nil, fmt.Errorf("zrange pending uid=%d: %w", uid, err)
	}
	processingIDs, err := q.rdb.ZRange(ctx, keys.Processing, 0, -1).Result()
	if err != nil {
		skylog.Errorf(ctx, "redisqueue.LoadActiveByUID zrange processing uid=%d: %v", uid, err)
		return nil, fmt.Errorf("zrange processing uid=%d: %w", uid, err)
	}
	skylog.Debugf(ctx, "redisqueue.LoadActiveByUID uid=%d pending=%d processing=%d",
		uid, len(ids), len(processingIDs))
	ids = append(ids, processingIDs...)
	return q.LoadByTimerIDs(ctx, uid, ids)
}

func (q *Queue[T]) LoadWindowByUID(ctx context.Context, uid uint32, window time.Duration) ([]T, error) {
	keys := q.keys(uid)
	maxScore := strconv.FormatFloat(float64(time.Now().Add(window).UnixMilli()), 'f', 0, 64)
	ids, err := q.rdb.ZRangeByScore(ctx, keys.Pending, &redis.ZRangeBy{Min: "0", Max: maxScore}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore pending uid=%d: %w", uid, err)
	}
	return q.LoadByTimerIDs(ctx, uid, ids)
}

func (q *Queue[T]) LoadByTimerIDs(ctx context.Context, uid uint32, ids []string) ([]T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	keys := q.keys(uid)
	vals, err := q.rdb.HMGet(ctx, keys.Data, ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("hmget timer data uid=%d: %w", uid, err)
	}
	businessIDsByTimer := map[string]string(nil)
	if _, ok := q.codec.(timerBusinessUnmarshaler[T]); ok {
		businessIDsByTimer, err = q.businessIDsByTimerID(ctx, keys)
		if err != nil {
			return nil, err
		}
	}
	out := make([]T, 0, len(vals))
	for i, v := range vals {
		if v == nil {
			_ = q.rdb.ZRem(ctx, keys.Pending, ids[i]).Err()
			_ = q.rdb.ZRem(ctx, keys.Processing, ids[i]).Err()
			continue
		}
		s, ok := v.(string)
		if !ok {
			continue
		}
		var rec T
		if keyed, ok := q.codec.(timerBusinessUnmarshaler[T]); ok {
			rec, err = keyed.UnmarshalWithTimerIDAndBusinessID([]byte(s), ids[i], businessIDsByTimer[ids[i]])
		} else {
			rec, err = q.codec.Unmarshal([]byte(s))
		}
		if err != nil {
			return nil, fmt.Errorf("unmarshal timer uid=%d timer=%s: %w", uid, ids[i], err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func (q *Queue[T]) CountActiveByUID(ctx context.Context, uid uint32) (int64, error) {
	keys := q.keys(uid)
	n, err := q.rdb.ZCard(ctx, keys.Pending).Result()
	if err != nil {
		return 0, fmt.Errorf("zcard pending uid=%d: %w", uid, err)
	}
	processing, err := q.rdb.ZCard(ctx, keys.Processing).Result()
	if err != nil {
		return 0, fmt.Errorf("zcard processing uid=%d: %w", uid, err)
	}
	return n + processing, nil
}

func (q *Queue[T]) loadRecordByTimerID(ctx context.Context, uid uint32, timerIDStr string) (T, bool, error) {
	var zero T
	val, err := q.rdb.HGet(ctx, q.keys(uid).Data, timerIDStr).Result()
	if err == redis.Nil {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	var rec T
	if keyed, ok := q.codec.(timerBusinessUnmarshaler[T]); ok {
		keys := q.keys(uid)
		businessIDsByTimer, idxErr := q.businessIDsByTimerID(ctx, keys)
		if idxErr != nil {
			return zero, false, idxErr
		}
		rec, err = keyed.UnmarshalWithTimerIDAndBusinessID([]byte(val), timerIDStr, businessIDsByTimer[timerIDStr])
	} else {
		rec, err = q.codec.Unmarshal([]byte(val))
	}
	return rec, err == nil, err
}

func (q *Queue[T]) businessIDsByTimerID(ctx context.Context, keys Keys) (map[string]string, error) {
	if keys.Index == "" {
		return nil, nil
	}
	index, err := q.rdb.HGetAll(ctx, keys.Index).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall timer index: %w", err)
	}
	out := make(map[string]string, len(index))
	for businessID, timerID := range index {
		out[timerID] = businessID
	}
	return out, nil
}

func (q *Queue[T]) requeueProcessingTimer(ctx context.Context, uid uint32, timerIDStr string, now time.Time) (bool, error) {
	keys := q.keys(uid)
	rec, found, err := q.loadRecordByTimerID(ctx, uid, timerIDStr)
	if err != nil {
		return false, err
	}
	dueTimeMs := int64(0)
	if found {
		dueTimeMs = q.codec.DueTimeMs(rec)
	}
	res, err := requeueScript.Run(ctx, q.rdb,
		[]string{keys.Data, keys.Pending, keys.Processing},
		timerIDStr,
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatInt(dueTimeMs, 10),
	).Int()
	return res == 1, err
}
