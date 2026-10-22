package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/model"
)

// RelationCache 为发送链路上的关系校验提供 Cache-Aside 缓存：
// 读时先查 Redis，缺失则回源 MySQL 并回填；写路径先改 MySQL 再调用 Invalidate* 删除缓存。
//
//	rel:friend:{a}:{b}      "1"/"0"，a 的好友列表里是否有 b
//	rel:block:{a}:{b}       "1"/"0"，a 是否拉黑了 b
//	rel:group_members:{gid} 群成员列表 JSON（含角色、禁言截止时间）
//
// 统一加 rel: 前缀，避免与改造前的 group_members:{gid}（Set 类型）等旧 key 发生类型冲突。
// 否定结果同样缓存，避免对不存在的关系反复穿透到 MySQL。
type RelationCache interface {
	IsFriend(ctx context.Context, userID, friendID int64) (bool, error)
	IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error)
	GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error)

	InvalidateFriend(ctx context.Context, userA, userB int64) error
	InvalidateBlock(ctx context.Context, userID, blockedID int64) error
	InvalidateGroupMembers(ctx context.Context, groupID int64) error
}

const (
	relationCacheTTL     = time.Hour
	groupMembersCacheTTL = 10 * time.Minute
	// invalidateDelay 是延迟双删的间隔：覆盖"读请求在写库前读到旧值、在删缓存后才回填"的竞态窗口。
	invalidateDelay = 500 * time.Millisecond
)

type RelationCacheImpl struct {
	rdb   *goredis.Client
	mysql MySQLRepo
}

func NewRelationCache(rdb *goredis.Client, mysql MySQLRepo) *RelationCacheImpl {
	return &RelationCacheImpl{rdb: rdb, mysql: mysql}
}

var _ RelationCache = (*RelationCacheImpl)(nil)

func friendCacheKey(a, b int64) string    { return fmt.Sprintf("rel:friend:%d:%d", a, b) }
func blockCacheKey(a, b int64) string     { return fmt.Sprintf("rel:block:%d:%d", a, b) }
func groupMembersCacheKey(g int64) string { return fmt.Sprintf("rel:group_members:%d", g) }

// cachedBool 是布尔关系的通用 Cache-Aside 读取。Redis 故障时直接回源，不影响发送。
func (c *RelationCacheImpl) cachedBool(ctx context.Context, key string, load func() (bool, error)) (bool, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	if err == nil {
		return val == "1", nil
	}
	ok, loadErr := load()
	if loadErr != nil {
		return false, loadErr
	}
	if errors.Is(err, goredis.Nil) {
		v := "0"
		if ok {
			v = "1"
		}
		_ = c.rdb.Set(ctx, key, v, relationCacheTTL).Err()
	}
	return ok, nil
}

func (c *RelationCacheImpl) IsFriend(ctx context.Context, userID, friendID int64) (bool, error) {
	return c.cachedBool(ctx, friendCacheKey(userID, friendID), func() (bool, error) {
		return c.mysql.IsFriend(ctx, userID, friendID)
	})
}

func (c *RelationCacheImpl) IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error) {
	return c.cachedBool(ctx, blockCacheKey(userID, blockedID), func() (bool, error) {
		return c.mysql.IsBlocked(ctx, userID, blockedID)
	})
}

func (c *RelationCacheImpl) GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error) {
	key := groupMembersCacheKey(groupID)
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		var members []model.GroupMember
		if json.Unmarshal(raw, &members) == nil {
			return members, nil
		}
	}
	members, loadErr := c.mysql.GetGroupMembers(ctx, groupID)
	if loadErr != nil {
		return nil, loadErr
	}
	if err == nil || errors.Is(err, goredis.Nil) {
		if body, mErr := json.Marshal(members); mErr == nil {
			_ = c.rdb.Set(ctx, key, body, groupMembersCacheTTL).Err()
		}
	}
	return members, nil
}

func (c *RelationCacheImpl) InvalidateFriend(ctx context.Context, userA, userB int64) error {
	return c.invalidate(ctx, friendCacheKey(userA, userB), friendCacheKey(userB, userA))
}

func (c *RelationCacheImpl) InvalidateBlock(ctx context.Context, userID, blockedID int64) error {
	return c.invalidate(ctx, blockCacheKey(userID, blockedID))
}

func (c *RelationCacheImpl) InvalidateGroupMembers(ctx context.Context, groupID int64) error {
	return c.invalidate(ctx, groupMembersCacheKey(groupID))
}

// invalidate 执行延迟双删：立即删一次（失败返回错误），invalidateDelay 后再删一次（失败只能等 TTL 兜底）。
// 第二次删除清掉并发读请求用旧 MySQL 数据回填的缓存。
func (c *RelationCacheImpl) invalidate(ctx context.Context, keys ...string) error {
	if err := c.rdb.Del(ctx, keys...).Err(); err != nil {
		return err
	}
	time.AfterFunc(invalidateDelay, func() {
		delCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = c.rdb.Del(delCtx, keys...).Err()
	})
	return nil
}
