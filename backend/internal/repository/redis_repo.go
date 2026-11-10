package repository

import (
	"context"
	"fmt"
	"strconv"

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/model"
)

// RedisRepo 定义朋友圈时间线需要的 Redis 操作。
// 消息相关的缓存（seq 计数器、消息窗口、会话元数据、去重）在 MessageCache 中，
// 好友与群成员的旁路缓存在 RelationCache 中。
type RedisRepo interface {
	// ── 修剪（用于清理任务）──
	TrimTimelineByTime(ctx context.Context, userID int64, beforeTimestamp int64) error

	// ── 动态流（推拉结合）──
	PublishMomentFeed(ctx context.Context, userID int64, momentID int64, timestamp int64) error
	GetMomentFeed(ctx context.Context, userID int64, lastSyncTime int64, limit int) ([]int64, error)
	// 写扩散：批量将动态推入多个好友收件箱（pipeline），并按 maxLen 裁剪各收件箱。
	FanoutMomentFeed(ctx context.Context, friendIDs []int64, momentID int64, timestamp int64, maxLen int) error
	// 寄件箱：作者发布的每条动态都写入自己的寄件箱（拉取源）。
	AddToOutbox(ctx context.Context, authorID int64, momentID int64, timestamp int64, maxLen int) error
	// 大V集合：标记/批量筛选大V用户（好友数超阈值，其动态走拉模式）。
	MarkBigUser(ctx context.Context, userID int64) error
	FilterBigUsers(ctx context.Context, userIDs []int64) ([]int64, error)
	// 分页读取收件箱/寄件箱，按 score 降序、复合游标 (maxTs,maxID) 分页。
	// maxTs<0 表示首页（从最新开始）。
	GetTimelinePage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error)
	GetOutboxPage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error)
}

// ──────────────────────────────────────────────────────
// RedisRepoImpl — 使用go-redis的具体实现
// ──────────────────────────────────────────────────────

type RedisRepoImpl struct {
	rdb *goredis.Client
}

func NewRedisRepo(rdb *goredis.Client) *RedisRepoImpl {
	return &RedisRepoImpl{rdb: rdb}
}

// ── 修剪（用于清理任务）──

func (r *RedisRepoImpl) TrimTimelineByTime(ctx context.Context, userID int64, beforeTimestamp int64) error {
	key := fmt.Sprintf("timeline:%d", userID)
	max := fmt.Sprintf("%d", beforeTimestamp)
	if err := r.rdb.ZRemRangeByScore(ctx, key, "0", max).Err(); err != nil {
		return fmt.Errorf("ZRemRangeByScore时间线: %w", err)
	}
	return nil
}

// ── 动态流 ──

func (r *RedisRepoImpl) PublishMomentFeed(ctx context.Context, userID int64, momentID int64, timestamp int64) error {
	key := fmt.Sprintf("timeline:%d", userID)
	return r.rdb.ZAdd(ctx, key, goredis.Z{
		Score:  float64(timestamp),
		Member: strconv.FormatInt(momentID, 10),
	}).Err()
}

func (r *RedisRepoImpl) GetMomentFeed(ctx context.Context, userID int64, lastSyncTime int64, limit int) ([]int64, error) {
	key := fmt.Sprintf("timeline:%d", userID)
	min := fmt.Sprintf("%d", lastSyncTime)
	results, err := r.rdb.ZRangeByScore(ctx, key, &goredis.ZRangeBy{
		Min:    min,
		Max:    "+inf",
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRangeByScore时间线: %w", err)
	}

	momentIDs := make([]int64, 0, len(results))
	for _, raw := range results {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue // 跳过损坏的条目
		}
		momentIDs = append(momentIDs, id)
	}
	return momentIDs, nil
}

// timelineKey / momentOutboxKey 集中管理 Feed 相关 Redis key，避免与群消息 outbox:{groupID} 冲突。
func timelineKey(userID int64) string     { return fmt.Sprintf("timeline:%d", userID) }
func momentOutboxKey(userID int64) string { return fmt.Sprintf("moment_outbox:%d", userID) }

const bigUsersKey = "moment:big_users"

// FanoutMomentFeed 用 pipeline 批量把动态推入多个好友收件箱，并按 maxLen 裁剪各收件箱。
func (r *RedisRepoImpl) FanoutMomentFeed(ctx context.Context, friendIDs []int64, momentID int64, timestamp int64, maxLen int) error {
	if len(friendIDs) == 0 {
		return nil
	}
	member := strconv.FormatInt(momentID, 10)
	pipe := r.rdb.Pipeline()
	for _, fid := range friendIDs {
		key := timelineKey(fid)
		pipe.ZAdd(ctx, key, goredis.Z{Score: float64(timestamp), Member: member})
		if maxLen > 0 {
			// 保留最新的 maxLen 条：删除 rank [0, -(maxLen+1)]（最旧的一批）
			pipe.ZRemRangeByRank(ctx, key, 0, int64(-(maxLen + 1)))
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("扇出动态到好友收件箱: %w", err)
	}
	return nil
}

// AddToOutbox 将动态写入作者自己的寄件箱 ZSet，并按 maxLen 裁剪。
func (r *RedisRepoImpl) AddToOutbox(ctx context.Context, authorID int64, momentID int64, timestamp int64, maxLen int) error {
	key := momentOutboxKey(authorID)
	pipe := r.rdb.Pipeline()
	pipe.ZAdd(ctx, key, goredis.Z{Score: float64(timestamp), Member: strconv.FormatInt(momentID, 10)})
	if maxLen > 0 {
		pipe.ZRemRangeByRank(ctx, key, 0, int64(-(maxLen + 1)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("写入作者寄件箱: %w", err)
	}
	return nil
}

// MarkBigUser 将用户加入大V集合（sticky，只增不删）。
func (r *RedisRepoImpl) MarkBigUser(ctx context.Context, userID int64) error {
	if err := r.rdb.SAdd(ctx, bigUsersKey, strconv.FormatInt(userID, 10)).Err(); err != nil {
		return fmt.Errorf("标记大V用户: %w", err)
	}
	return nil
}

// FilterBigUsers 从给定用户 ID 中筛出属于大V集合的那些（SMISMEMBER 一次批量判定）。
func (r *RedisRepoImpl) FilterBigUsers(ctx context.Context, userIDs []int64) ([]int64, error) {
	if len(userIDs) == 0 {
		return []int64{}, nil
	}
	members := make([]interface{}, len(userIDs))
	for i, id := range userIDs {
		members[i] = strconv.FormatInt(id, 10)
	}
	flags, err := r.rdb.SMIsMember(ctx, bigUsersKey, members...).Result()
	if err != nil {
		return nil, fmt.Errorf("批量判定大V用户: %w", err)
	}
	bigUsers := make([]int64, 0, len(userIDs))
	for i, isBig := range flags {
		if isBig {
			bigUsers = append(bigUsers, userIDs[i])
		}
	}
	return bigUsers, nil
}

// GetTimelinePage 读取用户收件箱的一页（推来的动态）。
func (r *RedisRepoImpl) GetTimelinePage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return r.getFeedPage(ctx, timelineKey(userID), maxTs, maxID, limit)
}

// GetOutboxPage 读取用户寄件箱的一页（其发布的动态，拉取源）。
func (r *RedisRepoImpl) GetOutboxPage(ctx context.Context, userID int64, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	return r.getFeedPage(ctx, momentOutboxKey(userID), maxTs, maxID, limit)
}

// getFeedPage 按 (ts,id) 降序读取一个 Feed ZSet 中"严格早于游标"的一页。
// maxTs<0 表示首页（无游标，从最新开始）。为规避同一 score(ts) 下多条动态的边界
// 重复/漏读，游标为复合键 (maxTs,maxID)：只返回 ts<maxTs，或 (ts==maxTs 且 id<maxID) 的条目。
// 由于 ZSet 的 member 是 momentID 字符串（非按 id 排序），同 score 的边界过滤在本函数内完成。
// 返回最多 limit 条，已严格排除游标本身，因此调用方无需再做边界去重。
func (r *RedisRepoImpl) getFeedPage(ctx context.Context, key string, maxTs int64, maxID int64, limit int) ([]model.FeedEntry, error) {
	if maxTs < 0 {
		// 首页：直接取 score 最大的 limit 条
		return r.zrevPage(ctx, key, "+inf", "-inf", limit)
	}

	entries := make([]model.FeedEntry, 0, limit)

	// 第一段：与游标同 score(ts==maxTs) 但 id<maxID 的条目（同毫秒内继续翻页）。
	sameScore, err := r.zrevPage(ctx, key, strconv.FormatInt(maxTs, 10), strconv.FormatInt(maxTs, 10), -1)
	if err != nil {
		return nil, err
	}
	for _, e := range sameScore {
		if e.MomentID < maxID {
			entries = append(entries, e)
			if len(entries) >= limit {
				return entries, nil
			}
		}
	}

	// 第二段：score 严格小于 maxTs 的条目（开区间上界）。
	older, err := r.zrevPage(ctx, key, "("+strconv.FormatInt(maxTs, 10), "-inf", limit-len(entries))
	if err != nil {
		return nil, err
	}
	entries = append(entries, older...)
	return entries, nil
}

// zrevPage 执行一次 ZREVRANGEBYSCORE（带 score），count<0 表示不限条数。
func (r *RedisRepoImpl) zrevPage(ctx context.Context, key, max, min string, count int) ([]model.FeedEntry, error) {
	opt := &goredis.ZRangeBy{Min: min, Max: max, Offset: 0}
	if count >= 0 {
		opt.Count = int64(count)
	}
	results, err := r.rdb.ZRevRangeByScoreWithScores(ctx, key, opt).Result()
	if err != nil {
		return nil, fmt.Errorf("ZRevRangeByScore读取Feed: %w", err)
	}
	entries := make([]model.FeedEntry, 0, len(results))
	for _, z := range results {
		raw, ok := z.Member.(string)
		if !ok {
			continue
		}
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			continue // 跳过损坏的条目
		}
		entries = append(entries, model.FeedEntry{MomentID: id, Ts: int64(z.Score)})
	}
	return entries, nil
}
