package infra

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ──────────────────────────────────────────────────────
// 定期清理
//
// 消息相关的 Redis 数据（seq 计数器、conv_msgs 窗口、conv_meta、去重 key）
// 在写入时已自带长度上限与 TTL，不再需要后台扫描清理；
// 这里只负责朋友圈收件箱 timeline:{uid} 的按时间与数量修剪。
// ──────────────────────────────────────────────────────

const (
	// 删除超过 cleanupAgeDays 天的 timeline 条目。
	cleanupAgeDays = 3
	// 每个 timeline 最多保留的条目数。
	maxTimelineEntries = 100
	// SCAN 批次大小。
	scanBatchSize = 100

	timelinePattern = "timeline:*"
)

// StartCleanupTask 启动一个后台 goroutine，按给定间隔运行 CleanupExpiredData，立即返回。
func StartCleanupTask(rdb *redis.Client, logger *zap.Logger, interval time.Duration) {
	go func() {
		CleanupExpiredData(rdb, logger)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			CleanupExpiredData(rdb, logger)
		}
	}()
}

// CleanupExpiredData 扫描所有 timeline:* 键，删除过期条目并限制最大条目数。
// timeline 的 ZSet score 为 Unix 毫秒时间戳。
func CleanupExpiredData(rdb *redis.Client, logger *zap.Logger) {
	ctx := context.Background()
	threshold := time.Now().Add(-cleanupAgeDays * 24 * time.Hour).UnixMilli()

	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, timelinePattern, scanBatchSize).Result()
		if err != nil {
			logger.Error("SCAN 失败", zap.String("pattern", timelinePattern), zap.Error(err))
			return
		}
		for _, key := range keys {
			trimTimeline(ctx, rdb, logger, key, threshold)
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

func trimTimeline(ctx context.Context, rdb *redis.Client, logger *zap.Logger, key string, threshold int64) {
	if err := rdb.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(threshold, 10)).Err(); err != nil {
		logger.Error("ZRemRangeByScore 失败", zap.String("key", key), zap.Error(err))
		return
	}
	// 保留 score 最大的 maxTimelineEntries 条
	if err := rdb.ZRemRangeByRank(ctx, key, 0, -int64(maxTimelineEntries)-1).Err(); err != nil {
		logger.Error("ZRemRangeByRank 失败", zap.String("key", key), zap.Error(err))
	}
}
