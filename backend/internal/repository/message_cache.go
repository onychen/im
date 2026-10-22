package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/model"
)

// ──────────────────────────────────────────────────────
// MessageCache — 消息链路使用的 Redis 缓存与计数器
//
// Redis 在新链路中只承担三件事，任何一项丢失都可以从 MySQL 恢复：
//   1. seq 计数器   seq:{convId}                 STRING，INCR 分配会话内 seq
//   2. 发送去重     msg_dedup:{uid}:{clientMsgId} STRING，"pending" 或 serverAck JSON，TTL 5 分钟
//   3. 热消息窗口   conv_msgs:{convId}            ZSET，score=seq，member=消息 JSON，保留最近 200 条，TTL 7 天
//      会话元数据   conv_meta:{convId}            HASH，seq + 最后一条消息 JSON，只在 seq 变大时更新
//
// 热消息窗口与会话元数据由 deliver 消费者写入，读路径缓存不完整时回源 MySQL。
// ──────────────────────────────────────────────────────

const (
	convMsgsKeep   = 200
	convMsgsTTL    = 7 * 24 * time.Hour
	convMetaTTL    = 7 * 24 * time.Hour
	msgDedupTTL    = 5 * time.Minute
	msgDedupMarker = "pending"
)

// ErrSeqNotInitialized 表示 seq 计数器不存在，需要先从 MySQL 校正后再分配。
var ErrSeqNotInitialized = errors.New("seq 计数器未初始化")

// ConvMeta 是 conv_meta 缓存中的会话最新状态。
type ConvMeta struct {
	Seq     int64
	LastMsg *model.Message
}

// MessageCache 定义消息链路的 Redis 操作，由 *MessageCacheImpl 实现。
type MessageCache interface {
	// NextSeq 原子分配下一个 seq；计数器不存在时返回 ErrSeqNotInitialized。
	NextSeq(ctx context.Context, convID string) (int64, error)
	// InitSeq 在计数器不存在时把它设置为 base（SET NX），已存在则不变。
	InitSeq(ctx context.Context, convID string, base int64) error
	// CurrentSeq 返回当前已分配的最大 seq；计数器不存在时 ok=false。
	CurrentSeq(ctx context.Context, convID string) (seq int64, ok bool, err error)

	// ReserveClientMsg 占用去重 key。reserved=true 表示首次发送；
	// reserved=false 且 ack!=nil 表示之前已成功，应直接回放 ack；两者都为空表示同一消息仍在处理中。
	ReserveClientMsg(ctx context.Context, senderID int64, clientMsgID string) (reserved bool, ack *model.ServerAck, err error)
	// CompleteClientMsg 把去重 key 的值替换为 serverAck，供客户端重发时回放。
	CompleteClientMsg(ctx context.Context, senderID int64, ack *model.ServerAck) error
	// ReleaseClientMsg 删除去重 key，发送失败时调用，允许客户端重试。
	ReleaseClientMsg(ctx context.Context, senderID int64, clientMsgID string) error

	// CacheMessage 把消息写入热消息窗口并推进会话元数据。
	CacheMessage(ctx context.Context, msg *model.Message) error
	// MarkCachedRevoked 把热消息窗口中的某条消息改为已撤回；不在窗口中时忽略。
	MarkCachedRevoked(ctx context.Context, convID string, seq int64) error
	// GetCachedRange 返回热消息窗口中 [fromSeq, toSeq] 内的消息，按 seq 升序。
	GetCachedRange(ctx context.Context, convID string, fromSeq, toSeq int64) ([]model.Message, error)
	// GetCachedMessage 返回窗口中的单条消息；不存在时返回 nil, nil。
	GetCachedMessage(ctx context.Context, convID string, seq int64) (*model.Message, error)
	// GetConvMetas 批量读取会话元数据，缓存缺失的会话不出现在结果中。
	GetConvMetas(ctx context.Context, convIDs []string) (map[string]ConvMeta, error)
}

type MessageCacheImpl struct {
	rdb *goredis.Client
}

func NewMessageCache(rdb *goredis.Client) *MessageCacheImpl {
	return &MessageCacheImpl{rdb: rdb}
}

var _ MessageCache = (*MessageCacheImpl)(nil)

func seqKeyOf(convID string) string      { return "seq:" + convID }
func convMsgsKey(convID string) string   { return "conv_msgs:" + convID }
func convMetaKey(convID string) string   { return "conv_meta:" + convID }
func dedupKey(uid int64, cid string) string { return fmt.Sprintf("msg_dedup:%d:%s", uid, cid) }

// luaNextSeq：计数器存在才 INCR，避免 key 丢失后从 1 重新分配造成 seq 冲突。
var luaNextSeq = goredis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
return redis.call('INCR', KEYS[1])
`)

// luaUpdateConvMeta：只有新 seq 更大时才覆盖，保证重复消费或乱序写入不会让会话元数据倒退。
var luaUpdateConvMeta = goredis.NewScript(`
local cur = tonumber(redis.call('HGET', KEYS[1], 'seq') or '0')
if tonumber(ARGV[1]) <= cur then
  return 0
end
redis.call('HSET', KEYS[1], 'seq', ARGV[1], 'msg', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`)

// ── seq ──

func (c *MessageCacheImpl) NextSeq(ctx context.Context, convID string) (int64, error) {
	seq, err := luaNextSeq.Run(ctx, c.rdb, []string{seqKeyOf(convID)}).Int64()
	if err != nil {
		return 0, fmt.Errorf("分配 seq: %w", err)
	}
	if seq < 0 {
		return 0, ErrSeqNotInitialized
	}
	return seq, nil
}

func (c *MessageCacheImpl) InitSeq(ctx context.Context, convID string, base int64) error {
	if err := c.rdb.SetNX(ctx, seqKeyOf(convID), base, 0).Err(); err != nil {
		return fmt.Errorf("初始化 seq: %w", err)
	}
	return nil
}

func (c *MessageCacheImpl) CurrentSeq(ctx context.Context, convID string) (int64, bool, error) {
	seq, err := c.rdb.Get(ctx, seqKeyOf(convID)).Int64()
	if errors.Is(err, goredis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("读取 seq: %w", err)
	}
	return seq, true, nil
}

// ── 去重 ──

func (c *MessageCacheImpl) ReserveClientMsg(ctx context.Context, senderID int64, clientMsgID string) (bool, *model.ServerAck, error) {
	key := dedupKey(senderID, clientMsgID)
	ok, err := c.rdb.SetNX(ctx, key, msgDedupMarker, msgDedupTTL).Result()
	if err != nil {
		return false, nil, fmt.Errorf("占用去重 key: %w", err)
	}
	if ok {
		return true, nil, nil
	}
	val, err := c.rdb.Get(ctx, key).Result()
	if errors.Is(err, goredis.Nil) {
		// 刚好过期或被释放：再抢一次
		ok, err = c.rdb.SetNX(ctx, key, msgDedupMarker, msgDedupTTL).Result()
		if err != nil {
			return false, nil, fmt.Errorf("占用去重 key: %w", err)
		}
		return ok, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("读取去重 key: %w", err)
	}
	if val == msgDedupMarker {
		return false, nil, nil
	}
	var ack model.ServerAck
	if err := json.Unmarshal([]byte(val), &ack); err != nil {
		return false, nil, nil
	}
	return false, &ack, nil
}

func (c *MessageCacheImpl) CompleteClientMsg(ctx context.Context, senderID int64, ack *model.ServerAck) error {
	body, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("序列化 serverAck: %w", err)
	}
	if err := c.rdb.Set(ctx, dedupKey(senderID, ack.ClientMsgID), body, msgDedupTTL).Err(); err != nil {
		return fmt.Errorf("写入去重结果: %w", err)
	}
	return nil
}

func (c *MessageCacheImpl) ReleaseClientMsg(ctx context.Context, senderID int64, clientMsgID string) error {
	if err := c.rdb.Del(ctx, dedupKey(senderID, clientMsgID)).Err(); err != nil {
		return fmt.Errorf("释放去重 key: %w", err)
	}
	return nil
}

// ── 热消息窗口 ──

func (c *MessageCacheImpl) CacheMessage(ctx context.Context, msg *model.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息: %w", err)
	}
	key := convMsgsKey(msg.ConvID)
	score := strconv.FormatInt(msg.Seq, 10)
	_, err = c.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		// 先按 score 清掉同 seq 的旧版本，重复消费时不会出现两条同 seq 的成员
		p.ZRemRangeByScore(ctx, key, score, score)
		p.ZAdd(ctx, key, goredis.Z{Score: float64(msg.Seq), Member: body})
		p.ZRemRangeByRank(ctx, key, 0, -(convMsgsKeep + 1))
		p.Expire(ctx, key, convMsgsTTL)
		return nil
	})
	if err != nil {
		return fmt.Errorf("写入消息缓存: %w", err)
	}
	if err := luaUpdateConvMeta.Run(ctx, c.rdb, []string{convMetaKey(msg.ConvID)},
		msg.Seq, body, int64(convMetaTTL/time.Second)).Err(); err != nil {
		return fmt.Errorf("更新会话元数据缓存: %w", err)
	}
	return nil
}

// MarkCachedRevoked 在 Go 侧完成读改写，而不是用 Lua 的 cjson：
// cjson 会把雪花 ID 这类大整数编码成浮点数，导致 msgId 精度丢失。
// 同一会话只会由 deliver 消费者的同一个分区串行写入，因此这里不存在并发覆盖。
func (c *MessageCacheImpl) MarkCachedRevoked(ctx context.Context, convID string, seq int64) error {
	msg, err := c.GetCachedMessage(ctx, convID, seq)
	if err != nil || msg == nil {
		return err
	}
	if msg.Status == model.MsgStatusRevoked {
		return nil
	}
	msg.Status = model.MsgStatusRevoked
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("序列化消息: %w", err)
	}
	key := convMsgsKey(convID)
	score := strconv.FormatInt(seq, 10)
	_, err = c.rdb.TxPipelined(ctx, func(p goredis.Pipeliner) error {
		p.ZRemRangeByScore(ctx, key, score, score)
		p.ZAdd(ctx, key, goredis.Z{Score: float64(seq), Member: body})
		return nil
	})
	if err != nil {
		return fmt.Errorf("更新撤回缓存: %w", err)
	}
	return nil
}

func (c *MessageCacheImpl) GetCachedRange(ctx context.Context, convID string, fromSeq, toSeq int64) ([]model.Message, error) {
	items, err := c.rdb.ZRangeByScore(ctx, convMsgsKey(convID), &goredis.ZRangeBy{
		Min: strconv.FormatInt(fromSeq, 10),
		Max: strconv.FormatInt(toSeq, 10),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("读取消息缓存: %w", err)
	}
	out := make([]model.Message, 0, len(items))
	for _, item := range items {
		var msg model.Message
		if err := json.Unmarshal([]byte(item), &msg); err != nil {
			// 缓存脏数据：返回不完整结果，调用方会回源 MySQL
			continue
		}
		out = append(out, msg)
	}
	return out, nil
}

func (c *MessageCacheImpl) GetCachedMessage(ctx context.Context, convID string, seq int64) (*model.Message, error) {
	msgs, err := c.GetCachedRange(ctx, convID, seq, seq)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, nil
	}
	return &msgs[0], nil
}

func (c *MessageCacheImpl) GetConvMetas(ctx context.Context, convIDs []string) (map[string]ConvMeta, error) {
	out := make(map[string]ConvMeta, len(convIDs))
	if len(convIDs) == 0 {
		return out, nil
	}
	cmds := make([]*goredis.MapStringStringCmd, len(convIDs))
	_, err := c.rdb.Pipelined(ctx, func(p goredis.Pipeliner) error {
		for i, id := range convIDs {
			cmds[i] = p.HGetAll(ctx, convMetaKey(id))
		}
		return nil
	})
	if err != nil && !errors.Is(err, goredis.Nil) {
		return nil, fmt.Errorf("批量读取会话元数据: %w", err)
	}
	for i, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		seq, err := strconv.ParseInt(fields["seq"], 10, 64)
		if err != nil {
			continue
		}
		meta := ConvMeta{Seq: seq}
		if raw := fields["msg"]; raw != "" {
			var msg model.Message
			if json.Unmarshal([]byte(raw), &msg) == nil {
				meta.LastMsg = &msg
			}
		}
		out[convIDs[i]] = meta
	}
	return out, nil
}
