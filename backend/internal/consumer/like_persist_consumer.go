package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// LikePersistConsumer 消费 like_persist Topic，把点赞/取消赞事件**批量削峰**镜像写入 MySQL。
//
// 削峰策略：不逐条落库，而是攒批——攒够 batchSize 条 或 每 flushInterval 触发一次 flush，
// 把一批事件按 (momentID,userID) 折叠为最终动作（last-action-wins），再分成
// 批量 INSERT IGNORE（点赞）与批量 DELETE（取消赞）各一条 SQL 落库，显著降低写库 QPS。
//
// 一致性：INSERT IGNORE / DELETE 均幂等，整批落库成功后统一提交 offset；
// 失败则整批退避重试（Kafka 无法单条 requeue，且整批重试天然保持顺序）。
// 生产端以 momentID 为分区 Key，同一动态的事件落在同一分区，
// 单 batcher goroutine 顺序 flush，保证 last-action-wins 的最终态正确。
type LikePersistConsumer struct {
	reader    *kafka.Reader
	mysqlRepo repository.MySQLRepo
	logger    *zap.Logger

	batchSize     int
	flushInterval time.Duration

	dlq DeadLetterPolicy
}

// NewLikePersistConsumer 创建 LikePersistConsumer。
// batchSize/flushMs 来自配置（config.MomentConfig）。
func NewLikePersistConsumer(reader *kafka.Reader, mysqlRepo repository.MySQLRepo, logger *zap.Logger, batchSize, flushMs int) *LikePersistConsumer {
	if batchSize <= 0 {
		batchSize = 200
	}
	if flushMs <= 0 {
		flushMs = 500
	}
	return &LikePersistConsumer{
		reader:        reader,
		mysqlRepo:     mysqlRepo,
		logger:        logger,
		batchSize:     batchSize,
		flushInterval: time.Duration(flushMs) * time.Millisecond,
	}
}

// WithDeadLetter 设置整批重试上限与死信 Topic；不调用则失败批次一直退避重试。
func (c *LikePersistConsumer) WithDeadLetter(p DeadLetterPolicy) *LikePersistConsumer {
	c.dlq = p
	return c
}

// Start 开始从 like_persist Topic 消费。拉取与攒批分别在两个 goroutine 中运行。
func (c *LikePersistConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "like persist consumer"); err != nil {
		return err
	}

	c.logger.Info("点赞持久化消费者已启动",
		zap.Int("batchSize", c.batchSize),
		zap.Duration("flushInterval", c.flushInterval),
	)

	// 缓冲区与批大小一致：batcher 忙于 flush/重试时，fetcher 会自然被背压阻塞。
	messages := make(chan kafka.Message, c.batchSize)
	go c.runFetcher(ctx, messages)
	go c.runBatcher(ctx, messages)
	return nil
}

// runFetcher 持续从 Kafka 拉取消息投递给 batcher，ctx 取消时关闭通道并关闭 reader。
func (c *LikePersistConsumer) runFetcher(ctx context.Context, out chan<- kafka.Message) {
	defer close(out)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			c.logger.Warn("拉取点赞事件失败", zap.Error(err))
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}
		select {
		case out <- msg:
		case <-ctx.Done():
			return
		}
	}
}

// runBatcher 攒批循环：攒够 batchSize 或到 flushInterval 就 flush；通道关闭时尽力 flush 残余后退出。
func (c *LikePersistConsumer) runBatcher(ctx context.Context, messages <-chan kafka.Message) {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.logger.Warn("关闭点赞 Kafka reader 失败", zap.Error(err))
		}
	}()

	batch := make([]kafka.Message, 0, c.batchSize)
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		c.flushWithRetry(flushCtx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-messages:
			if !ok {
				// ctx 已取消：用短超时 context 尽力把残余批次落库并提交，
				// 失败也无妨——offset 未提交，重启后会重新消费（落库幂等）。
				drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				flush(drainCtx)
				cancel()
				c.logger.Info("点赞持久化消费者已停止")
				return
			}
			batch = append(batch, msg)
			if len(batch) >= c.batchSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		}
	}
}

// flushWithRetry 反复尝试落库并提交 offset，直到成功、达到上限转投 DLQ 或 ctx 结束。
func (c *LikePersistConsumer) flushWithRetry(ctx context.Context, batch []kafka.Message) {
	backoff := retryBackoffMin
	for attempt := 1; ; attempt++ {
		err := c.flush(ctx, batch)
		if err == nil {
			c.commit(ctx, batch)
			return
		}
		if c.dlq.enabled() && attempt >= c.dlq.MaxAttempts {
			// 整批写入 DLQ 成功后才提交 offset；ctx 取消则不提交，重启后重新消费。
			if publishDeadLetter(ctx, c.logger, "like-persist", c.dlq, attempt, err, batch...) {
				c.commit(ctx, batch)
			}
			return
		}
		c.logger.Error("批量落库点赞失败，整批退避重试",
			zap.Int("events", len(batch)), zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.Error(err))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func (c *LikePersistConsumer) commit(ctx context.Context, batch []kafka.Message) {
	if err := c.reader.CommitMessages(ctx, batch...); err != nil {
		c.logger.Warn("提交点赞事件 offset 失败（重启后将重复消费，落库幂等）", zap.Error(err))
	}
}

// flush 落库一批事件：解析（跳过格式错误）→ 折叠 last-action-wins → 批量 upsert/delete。
func (c *LikePersistConsumer) flush(ctx context.Context, batch []kafka.Message) error {
	type folded struct {
		action string
		ts     int64
	}
	// map 覆盖天然实现"按到达顺序 last-action-wins"（batch 保持分区内到达顺序）。
	final := make(map[model.MomentLikeKey]folded, len(batch))
	valid := 0

	for _, m := range batch {
		evt, err := deserializeLikeEvent(m.Value)
		if err != nil {
			c.logger.Error("反序列化点赞事件失败，跳过", zap.Int64("offset", m.Offset), zap.Error(err))
			continue
		}
		valid++
		final[model.MomentLikeKey{MomentID: evt.MomentID, UserID: evt.UserID}] = folded{action: evt.Action, ts: evt.Ts}
	}

	if valid == 0 {
		return nil
	}

	upserts := make([]model.MomentLike, 0, len(final))
	deletes := make([]model.MomentLikeKey, 0, len(final))
	for key, f := range final {
		if f.action == model.LikeActionUnlike {
			deletes = append(deletes, key)
		} else {
			upserts = append(upserts, model.MomentLike{
				MomentID:  key.MomentID,
				UserID:    key.UserID,
				CreatedAt: time.UnixMilli(f.ts),
			})
		}
	}

	if err := c.mysqlRepo.BatchUpsertMomentLikes(ctx, upserts); err != nil {
		return fmt.Errorf("批量落库点赞(%d): %w", len(upserts), err)
	}
	if err := c.mysqlRepo.BatchDeleteMomentLikes(ctx, deletes); err != nil {
		return fmt.Errorf("批量落库取消赞(%d): %w", len(deletes), err)
	}

	c.logger.Debug("点赞事件批量落库完成",
		zap.Int("events", valid),
		zap.Int("upserts", len(upserts)),
		zap.Int("deletes", len(deletes)),
	)
	return nil
}

// deserializeLikeEvent 将 Kafka 消息体解析为 LikeEvent 并校验。
func deserializeLikeEvent(body []byte) (*model.LikeEvent, error) {
	var evt model.LikeEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return nil, fmt.Errorf("反序列化点赞事件: %w", err)
	}
	if evt.MomentID == 0 || evt.UserID == 0 {
		return nil, fmt.Errorf("无效点赞事件: momentID=%d, userID=%d", evt.MomentID, evt.UserID)
	}
	if evt.Action != model.LikeActionLike && evt.Action != model.LikeActionUnlike {
		return nil, fmt.Errorf("未知点赞动作: %q", evt.Action)
	}
	return &evt, nil
}
