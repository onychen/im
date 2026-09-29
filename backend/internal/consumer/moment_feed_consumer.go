package consumer

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// MomentFeedConsumer 处理来自 moment_push Topic 的消息。
// 对于每条动态发布事件，它将动态分发到所有好友的时间线 ZSet 中，
// 并将动态添加到作者自己的时间线中。
type MomentFeedConsumer struct {
	reader    *kafka.Reader
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	logger    *zap.Logger

	// bigUserThreshold 是"大V"判定阈值：好友数 > 阈值时不写扩散，仅存寄件箱。
	bigUserThreshold int
	// timelineMaxLen 是每个收件箱/寄件箱扇出后裁剪保留的最大条数。
	timelineMaxLen int

	dlq DeadLetterPolicy
}

// NewMomentFeedConsumer 创建一个新的 MomentFeedConsumer。
// bigUserThreshold/timelineMaxLen 来自配置（config.MomentConfig）。
func NewMomentFeedConsumer(reader *kafka.Reader, mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, logger *zap.Logger, bigUserThreshold, timelineMaxLen int) *MomentFeedConsumer {
	return &MomentFeedConsumer{
		reader:           reader,
		mysqlRepo:        mysqlRepo,
		redisRepo:        redisRepo,
		logger:           logger,
		bigUserThreshold: bigUserThreshold,
		timelineMaxLen:   timelineMaxLen,
	}
}

// WithDeadLetter 设置重试上限与死信 Topic；不调用则失败消息一直原地重试。
func (c *MomentFeedConsumer) WithDeadLetter(p DeadLetterPolicy) *MomentFeedConsumer {
	c.dlq = p
	return c
}

// Start 在后台 goroutine 中开始从 moment_push Topic 消费。
func (c *MomentFeedConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "moment feed consumer"); err != nil {
		return err
	}
	c.logger.Info("动态消息消费者已启动")
	go runReader(ctx, c.reader, c.logger, "moment-feed", c.handleMessage, c.dlq)
	return nil
}

// handleMessage 处理单条 Kafka 消息。
// 成功：提交 offset。失败：退避重试（寄件箱/扇出写入均幂等）。
func (c *MomentFeedConsumer) handleMessage(ctx context.Context, m kafka.Message) error {
	moment, err := deserializeMoment(m.Value)
	if err != nil {
		// 消息格式错误 — 丢弃
		return drop(err)
	}

	if err := c.process(ctx, moment); err != nil {
		c.logger.Error("动态分发处理失败",
			zap.Int64("momentID", moment.ID),
			zap.Int64("authorID", moment.AuthorID),
			zap.Error(err),
		)
		return err
	}
	return nil
}

// process 执行推拉结合的分发逻辑：
//  1. 每条动态都写入作者自己的寄件箱（moment_outbox），作为拉取源与作者自见来源。
//  2. 私密动态(3) 不扩散，仅留在寄件箱。
//  3. 好友数 > 阈值的作者标记为大V并跳过写扩散（其动态由好友读取时拉取合并）。
//  4. 好友数 ≤ 阈值时，用 pipeline 批量写扩散到各好友收件箱并裁剪。
func (c *MomentFeedConsumer) process(ctx context.Context, moment *model.Moment) error {
	timestamp := moment.CreatedAt.UnixMilli()

	// ── 1. 写入作者自己的寄件箱 ──
	// 关键操作：失败则返回错误触发重试（ZADD 幂等，重试安全）。
	if err := c.redisRepo.AddToOutbox(ctx, moment.AuthorID, moment.ID, timestamp, c.timelineMaxLen); err != nil {
		return fmt.Errorf("写入作者 %d 寄件箱失败: %w", moment.AuthorID, err)
	}

	// ── 2. 私密动态不扩散 ──
	if moment.Visibility == 3 {
		c.logger.Debug("私密动态 — 跳过扩散",
			zap.Int64("momentID", moment.ID),
			zap.Int64("authorID", moment.AuthorID),
		)
		return nil
	}

	// ── 3. 按好友数分流：大V走拉模式 ──
	friendCount, err := c.mysqlRepo.CountFriends(ctx, moment.AuthorID)
	if err != nil {
		return fmt.Errorf("统计作者 %d 好友数失败: %w", moment.AuthorID, err)
	}

	if friendCount > c.bigUserThreshold {
		// 标记为大V（sticky），跳过写扩散
		if err := c.redisRepo.MarkBigUser(ctx, moment.AuthorID); err != nil {
			// 标记失败会导致好友读取时拉不到该作者，需重试
			return fmt.Errorf("标记大V用户 %d 失败: %w", moment.AuthorID, err)
		}
		c.logger.Debug("大V动态 — 仅存寄件箱，跳过写扩散",
			zap.Int64("momentID", moment.ID),
			zap.Int64("authorID", moment.AuthorID),
			zap.Int("friendCount", friendCount),
		)
		return nil
	}

	// ── 4. 普通用户：批量写扩散到好友收件箱 ──
	friends, err := c.mysqlRepo.GetFriendList(ctx, moment.AuthorID)
	if err != nil {
		return fmt.Errorf("获取作者 %d 的好友列表失败: %w", moment.AuthorID, err)
	}

	friendIDs := make([]int64, 0, len(friends))
	for _, fs := range friends {
		if fs.FriendID == moment.AuthorID {
			continue // 作者自己的动态已在寄件箱，不重复推给自己
		}
		friendIDs = append(friendIDs, fs.FriendID)
	}

	if err := c.redisRepo.FanoutMomentFeed(ctx, friendIDs, moment.ID, timestamp, c.timelineMaxLen); err != nil {
		return fmt.Errorf("扇出动态 %d 到好友收件箱失败: %w", moment.ID, err)
	}

	c.logger.Debug("动态写扩散完成",
		zap.Int64("momentID", moment.ID),
		zap.Int64("authorID", moment.AuthorID),
		zap.Int("friendCount", len(friendIDs)),
	)

	return nil
}

// ── 辅助函数 ──

// deserializeMoment 将 Kafka 消息体解析为 Moment 结构体。
func deserializeMoment(body []byte) (*model.Moment, error) {
	var moment model.Moment
	if err := json.Unmarshal(body, &moment); err != nil {
		return nil, fmt.Errorf("反序列化动态: %w", err)
	}
	if moment.AuthorID == 0 || moment.ID == 0 {
		return nil, fmt.Errorf("无效的动态: authorID=%d, id=%d", moment.AuthorID, moment.ID)
	}
	return &moment, nil
}
