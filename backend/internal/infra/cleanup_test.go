package infra

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/goim/goim/internal/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// ──────────────────────────────────────────────────────
// 辅助函数：创建测试 Redis 客户端
// ──────────────────────────────────────────────────────

func newTestRedisClient(t *testing.T) *redis.Client {
	cfg, err := config.LoadConfig("../../configs/config.test.yaml")
	if err != nil {
		t.Skip("无法加载测试配置")
	}

	rdb, err := NewRedisClient(&cfg.Redis)
	require.NoError(t, err)

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		rdb.Close()
		t.Skip("Redis 不可用，跳过集成测试")
	}

	return rdb
}

// ──────────────────────────────────────────────────────
// extractIDFromKey 测试（单元测试，不需要 Redis）
// ──────────────────────────────────────────────────────

func TestExtractIDFromKey(t *testing.T) {
	tests := []struct {
		key     string
		want    int64
		wantErr bool
	}{
		{"inbox:123", 123, false},
		{"outbox:456", 456, false},
		{"timeline:789", 789, false},
		{"conv_list:100", 100, false},
		{"inbox:0", 0, false},
		{"inbox:", 0, true},
		{"inbox:abc", 0, true},
		{"unknown:123", 123, false},
	}

	for _, tt := range tests {
		got, err := extractIDFromKey(tt.key)
		if tt.wantErr {
			assert.Error(t, err, "key=%s", tt.key)
		} else {
			assert.NoError(t, err, "key=%s", tt.key)
			assert.Equal(t, tt.want, got, "key=%s", tt.key)
		}
	}
}

func TestMaxEntriesForPrefix(t *testing.T) {
	assert.Equal(t, maxInboxEntries, maxEntriesForPrefix("inbox"))
	assert.Equal(t, maxOutboxEntries, maxEntriesForPrefix("outbox"))
	assert.Equal(t, maxTimelineEntries, maxEntriesForPrefix("timeline"))
	assert.Equal(t, 0, maxEntriesForPrefix("conv_list"))
	assert.Equal(t, 0, maxEntriesForPrefix("unknown"))
}

// ──────────────────────────────────────────────────────
// 集成测试（需要 Redis，不可用时跳过）
// ──────────────────────────────────────────────────────

func TestCleanupExpiredData_RemovesOldInboxEntries(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90001)
	inboxKey := fmt.Sprintf("inbox:%d", userID)
	now := time.Now().UnixMilli()

	// 种子数据：10 条旧记录（5 天前）+ 10 条新记录
	for i := 0; i < 10; i++ {
		oldTS := now - int64(5*24*3600*1000) + int64(i*3600*1000)
		rdb.ZAdd(ctx, inboxKey, redis.Z{Score: float64(oldTS), Member: fmt.Sprintf("old_msg_%d", i)})
	}
	for i := 0; i < 10; i++ {
		newTS := now - int64(i*60*1000)
		rdb.ZAdd(ctx, inboxKey, redis.Z{Score: float64(newTS), Member: fmt.Sprintf("new_msg_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, inboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(20), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, inboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(10), cardAfter)

	rdb.Del(ctx, inboxKey)
}

func TestCleanupExpiredData_TrimsInboxByMaxCount(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90002)
	inboxKey := fmt.Sprintf("inbox:%d", userID)
	now := time.Now().UnixMilli()

	// 种子数据：1200 条近期记录 — 时间裁剪不会移除它们
	for i := 0; i < 1200; i++ {
		ts := now - int64(i)
		rdb.ZAdd(ctx, inboxKey, redis.Z{Score: float64(ts), Member: fmt.Sprintf("msg_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, inboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1200), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, inboxKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, cardAfter, int64(maxInboxEntries))

	rdb.Del(ctx, inboxKey)
}

func TestCleanupExpiredData_RemovesOldOutboxEntries(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	groupID := int64(90003)
	outboxKey := fmt.Sprintf("outbox:%d", groupID)
	now := time.Now().UnixMilli()

	for i := 0; i < 5; i++ {
		oldTS := now - int64(5*24*3600*1000) + int64(i*3600*1000)
		rdb.ZAdd(ctx, outboxKey, redis.Z{Score: float64(oldTS), Member: fmt.Sprintf("old_%d", i)})
	}
	for i := 0; i < 5; i++ {
		newTS := now - int64(i*60*1000)
		rdb.ZAdd(ctx, outboxKey, redis.Z{Score: float64(newTS), Member: fmt.Sprintf("new_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, outboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(10), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, outboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(5), cardAfter)

	rdb.Del(ctx, outboxKey)
}

func TestCleanupExpiredData_TrimsOutboxByMaxCount(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	groupID := int64(90004)
	outboxKey := fmt.Sprintf("outbox:%d", groupID)
	now := time.Now().UnixMilli()

	for i := 0; i < 600; i++ {
		ts := now - int64(i)
		rdb.ZAdd(ctx, outboxKey, redis.Z{Score: float64(ts), Member: fmt.Sprintf("msg_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, outboxKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(600), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, outboxKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, cardAfter, int64(maxOutboxEntries))

	rdb.Del(ctx, outboxKey)
}

func TestCleanupExpiredData_RemovesOldTimelineEntries(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90005)
	timelineKey := fmt.Sprintf("timeline:%d", userID)
	now := time.Now().UnixMilli()

	for i := 0; i < 5; i++ {
		oldTS := now - int64(5*24*3600*1000) + int64(i*3600*1000)
		rdb.ZAdd(ctx, timelineKey, redis.Z{Score: float64(oldTS), Member: fmt.Sprintf("old_%d", i)})
	}
	for i := 0; i < 5; i++ {
		newTS := now - int64(i*60*1000)
		rdb.ZAdd(ctx, timelineKey, redis.Z{Score: float64(newTS), Member: fmt.Sprintf("new_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, timelineKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(10), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, timelineKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(5), cardAfter)

	rdb.Del(ctx, timelineKey)
}

func TestCleanupExpiredData_TrimsTimelineByMaxCount(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90006)
	timelineKey := fmt.Sprintf("timeline:%d", userID)
	now := time.Now().UnixMilli()

	for i := 0; i < 150; i++ {
		ts := now - int64(i)
		rdb.ZAdd(ctx, timelineKey, redis.Z{Score: float64(ts), Member: fmt.Sprintf("m_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, timelineKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(150), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, timelineKey).Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, cardAfter, int64(maxTimelineEntries))

	rdb.Del(ctx, timelineKey)
}

func TestCleanupExpiredData_RemovesOldConvListEntries(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90007)
	convKey := fmt.Sprintf("conv_list:%d", userID)
	now := time.Now().UnixMilli()

	for i := 0; i < 5; i++ {
		oldTS := now - int64(5*24*3600*1000) + int64(i*3600*1000)
		rdb.ZAdd(ctx, convKey, redis.Z{Score: float64(oldTS), Member: fmt.Sprintf("conv_old_%d", i)})
	}
	for i := 0; i < 5; i++ {
		newTS := now - int64(i*60*1000)
		rdb.ZAdd(ctx, convKey, redis.Z{Score: float64(newTS), Member: fmt.Sprintf("conv_new_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, convKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(10), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, convKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(5), cardAfter)

	rdb.Del(ctx, convKey)
}

func TestCleanupExpiredData_ScanFindsMultipleKeys(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()
	now := time.Now().UnixMilli()

	// 为多个用户创建收件箱键
	for _, uid := range []int64{90010, 90011, 90012} {
		key := fmt.Sprintf("inbox:%d", uid)
		for i := 0; i < 5; i++ {
			oldTS := now - int64(5*24*3600*1000)
			rdb.ZAdd(ctx, key, redis.Z{Score: float64(oldTS), Member: fmt.Sprintf("old_%d_%d", uid, i)})
		}
		rdb.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: fmt.Sprintf("new_%d", uid)})
	}

	CleanupExpiredData(rdb, logger)

	for _, uid := range []int64{90010, 90011, 90012} {
		key := fmt.Sprintf("inbox:%d", uid)
		card, err := rdb.ZCard(ctx, key).Result()
		require.NoError(t, err)
		assert.Equal(t, int64(1), card, "用户 %d 的收件箱清理后应有 1 条记录", uid)
		rdb.Del(ctx, key)
	}
}

func TestCleanupExpiredData_ConvListNoRankTrim(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	userID := int64(90008)
	convKey := fmt.Sprintf("conv_list:%d", userID)
	now := time.Now().UnixMilli()

	// 添加 50 条近期 conv_list 记录 — conv_list 无排名上限，因此全部保留
	for i := 0; i < 50; i++ {
		ts := now - int64(i*60*1000) // 全部为近期
		rdb.ZAdd(ctx, convKey, redis.Z{Score: float64(ts), Member: fmt.Sprintf("conv_%d", i)})
	}

	cardBefore, err := rdb.ZCard(ctx, convKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(50), cardBefore)

	CleanupExpiredData(rdb, logger)

	cardAfter, err := rdb.ZCard(ctx, convKey).Result()
	require.NoError(t, err)
	assert.Equal(t, int64(50), cardAfter, "conv_list 不应被排名裁剪")

	rdb.Del(ctx, convKey)
}

// ──────────────────────────────────────────────────────
// StartCleanupTask 测试 — 验证它不会阻塞
// ──────────────────────────────────────────────────────

func TestStartCleanupTask_DoesNotBlock(t *testing.T) {
	rdb := newTestRedisClient(t)
	defer rdb.Close()

	logger := zaptest.NewLogger(t)

	done := make(chan struct{})
	go func() {
		StartCleanupTask(rdb, logger, 100*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		// StartCleanupTask 立即返回 — 正确
	case <-time.After(2 * time.Second):
		t.Fatal("StartCleanupTask 阻塞了 — 应立即返回")
	}
}
