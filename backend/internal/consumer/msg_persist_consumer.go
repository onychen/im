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

// MsgPersistConsumer 以 persist 消费者组消费 msg_topic，把消息批量写入 MySQL。
//
// 写入语义：
//   - 生产端以 convId 为分区 Key，同一会话的消息在同一分区内有序，单 batcher 顺序 flush；
//   - PersistMessages 在一个事务里完成 INSERT IGNORE messages、撤回状态更新、
//     conversations / user_conversations 维护，整批成功后统一提交 offset；
//   - 重复消费（同 convId+seq 且 msgId 相同）被视为成功，天然幂等；
//   - 冲突消息（同 convId+seq 但 msgId 不同，或 clientMsgId 唯一键冲突）单独投递 DLQ，不阻塞整批；
//   - 整批失败则退避重试，达到上限整批投 DLQ。
type MsgPersistConsumer struct {
	reader *kafka.Reader
	store  repository.MessageStore
	logger *zap.Logger

	batchSize     int
	flushInterval time.Duration

	dlq DeadLetterPolicy
}

// NewMsgPersistConsumer 创建消息持久化消费者，batchSize/flushMs 来自 config.MessageConfig。
func NewMsgPersistConsumer(reader *kafka.Reader, store repository.MessageStore, logger *zap.Logger, batchSize, flushMs int) *MsgPersistConsumer {
	if batchSize <= 0 {
		batchSize = 200
	}
	if flushMs <= 0 {
		flushMs = 100
	}
	return &MsgPersistConsumer{
		reader:        reader,
		store:         store,
		logger:        logger,
		batchSize:     batchSize,
		flushInterval: time.Duration(flushMs) * time.Millisecond,
	}
}

// WithDeadLetter 设置重试上限与死信 Topic。
func (c *MsgPersistConsumer) WithDeadLetter(p DeadLetterPolicy) *MsgPersistConsumer {
	c.dlq = p
	return c
}

// Start 启动拉取与攒批两个 goroutine。
func (c *MsgPersistConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "msg persist consumer"); err != nil {
		return err
	}
	c.logger.Info("消息持久化消费者已启动",
		zap.Int("batchSize", c.batchSize),
		zap.Duration("flushInterval", c.flushInterval),
	)
	messages := make(chan kafka.Message, c.batchSize)
	go c.runFetcher(ctx, messages)
	go c.runBatcher(ctx, messages)
	return nil
}

func (c *MsgPersistConsumer) runFetcher(ctx context.Context, out chan<- kafka.Message) {
	defer close(out)
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			c.logger.Warn("拉取消息事件失败", zap.Error(err))
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

func (c *MsgPersistConsumer) runBatcher(ctx context.Context, messages <-chan kafka.Message) {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	defer func() {
		if err := c.reader.Close(); err != nil {
			c.logger.Warn("关闭消息持久化 Kafka reader 失败", zap.Error(err))
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
				drainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				flush(drainCtx)
				cancel()
				c.logger.Info("消息持久化消费者已停止")
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

func (c *MsgPersistConsumer) flushWithRetry(ctx context.Context, batch []kafka.Message) {
	backoff := retryBackoffMin
	for attempt := 1; ; attempt++ {
		rejected, err := c.flush(ctx, batch)
		if err == nil {
			if !c.deadLetterRejected(ctx, attempt, rejected) {
				return // ctx 取消，不提交，重启后重新消费
			}
			c.commit(ctx, batch)
			return
		}
		if c.dlq.enabled() && attempt >= c.dlq.MaxAttempts {
			if publishDeadLetter(ctx, c.logger, "msg-persist", c.dlq, attempt, err, batch...) {
				c.commit(ctx, batch)
			}
			return
		}
		c.logger.Error("批量落库消息失败，整批退避重试",
			zap.Int("messages", len(batch)), zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.Error(err))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// rejectedMessages 是一批中不能落库、需要单独转投 DLQ 的消息，按原因分开以便排查。
type rejectedMessages struct {
	invalid   []kafka.Message // 无法解析或缺少必填字段
	conflicts []kafka.Message // seq 或 clientMsgId 冲突
}

// deadLetterRejected 把被拒绝的消息投 DLQ；返回 false 表示 ctx 已取消，调用方不能提交 offset。
// 未配置 DLQ 时只记日志并跳过，避免一条坏消息卡住整个分区。
func (c *MsgPersistConsumer) deadLetterRejected(ctx context.Context, attempt int, r rejectedMessages) bool {
	groups := []struct {
		msgs  []kafka.Message
		cause error
	}{
		{r.invalid, errors.New("消息事件无法解析")},
		{r.conflicts, errors.New("消息 seq 或 clientMsgId 冲突")},
	}
	for _, g := range groups {
		if len(g.msgs) == 0 {
			continue
		}
		if !c.dlq.enabled() {
			c.logger.Error("消息无法落库且未配置 DLQ，已跳过", zap.Int("messages", len(g.msgs)), zap.Error(g.cause))
			continue
		}
		if !publishDeadLetter(ctx, c.logger, "msg-persist", c.dlq, attempt, g.cause, g.msgs...) {
			return false
		}
	}
	return true
}
		if c.dlq.enabled() && attempt >= c.dlq.MaxAttempts {
			if publishDeadLetter(ctx, c.logger, "msg-persist", c.dlq, attempt, err, batch...) {
				c.commit(ctx, batch)
			}
			return
		}
		c.logger.Error("批量落库消息失败，整批退避重试",
			zap.Int("messages", len(batch)), zap.Int("attempt", attempt), zap.Duration("backoff", backoff), zap.Error(err))
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func (c *MsgPersistConsumer) commit(ctx context.Context, batch []kafka.Message) {
	if err := c.reader.CommitMessages(ctx, batch...); err != nil {
		c.logger.Warn("提交消息 offset 失败（重启后将重复消费，落库幂等）", zap.Error(err))
	}
}

// flush 解析并落库一批消息，返回需要投 DLQ 的消息（原始 Kafka 消息）。
func (c *MsgPersistConsumer) flush(ctx context.Context, batch []kafka.Message) (rejectedMessages, error) {
	type key struct {
		convID string
		seq    int64
	}
	var rejected rejectedMessages
	msgs := make([]model.Message, 0, len(batch))
	origin := make(map[key]kafka.Message, len(batch))
	for _, m := range batch {
		msg, err := decodeMessageEvent(m.Value)
		if err != nil {
			c.logger.Error("反序列化消息事件失败，转投 DLQ", zap.Int("partition", m.Partition), zap.Int64("offset", m.Offset), zap.Error(err))
			rejected.invalid = append(rejected.invalid, m)
			continue
		}
		msgs = append(msgs, *msg)
		origin[key{msg.ConvID, msg.Seq}] = m
	}
	if len(msgs) == 0 {
		return rejected, nil
	}

	conflicts, err := c.store.PersistMessages(ctx, msgs)
	if err != nil {
		return rejectedMessages{}, fmt.Errorf("批量落库消息(%d): %w", len(msgs), err)
	}

	for _, cm := range conflicts {
		if km, ok := origin[key{cm.ConvID, cm.Seq}]; ok {
			rejected.conflicts = append(rejected.conflicts, km)
		}
		c.logger.Error("消息落库冲突",
			zap.String("convId", cm.ConvID),
			zap.Int64("seq", cm.Seq),
			zap.Int64("msgId", cm.MsgID),
			zap.String("clientMsgId", cm.ClientMsgID),
		)
	}
	c.logger.Debug("消息批量落库完成", zap.Int("messages", len(msgs)), zap.Int("conflicts", len(conflicts)))
	return rejected, nil
}

// decodeMessageEvent 解析 msg_topic 中的消息事件并做基本校验。
func decodeMessageEvent(body []byte) (*model.Message, error) {
	var msg model.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, fmt.Errorf("反序列化消息事件: %w", err)
	}
	if msg.ConvID == "" || msg.Seq <= 0 || msg.MsgID == 0 || msg.SenderID == 0 {
		return nil, fmt.Errorf("无效消息事件: convId=%q seq=%d msgId=%d sender=%d",
			msg.ConvID, msg.Seq, msg.MsgID, msg.SenderID)
	}
	return &msg, nil
}
