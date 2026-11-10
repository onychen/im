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

const msgPersistConsumerName = "msg-persist"

// MessageDeliverer 更新已落库消息的缓存并推送给在线接收方。
type MessageDeliverer interface {
	DeliverMessage(ctx context.Context, msg *model.Message) error
}

// MsgPersistConsumer 按顺序逐条落库，事务提交后再尝试在线投递。
type MsgPersistConsumer struct {
	reader    *kafka.Reader
	store     repository.MessageStore
	deliverer MessageDeliverer
	logger    *zap.Logger
	dlq       DeadLetterPolicy
}

func NewMsgPersistConsumer(reader *kafka.Reader, store repository.MessageStore, deliverer MessageDeliverer, logger *zap.Logger) *MsgPersistConsumer {
	return &MsgPersistConsumer{reader: reader, store: store, deliverer: deliverer, logger: logger}
}

// WithDeadLetter 配置无效事件和写入冲突的死信目标。
func (c *MsgPersistConsumer) WithDeadLetter(p DeadLetterPolicy) *MsgPersistConsumer {
	c.dlq = p
	return c
}

func (c *MsgPersistConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "msg persist consumer"); err != nil {
		return err
	}
	c.logger.Info("消息消费者已启动，逐条落库后投递")
	// MySQL 失败时原地重试。后续消息落库前，当前事件必须成功或被判定为无效/冲突。
	go runReader(ctx, c.reader, c.logger, msgPersistConsumerName, c.handle, DeadLetterPolicy{})
	return nil
}

func (c *MsgPersistConsumer) handle(ctx context.Context, event kafka.Message) error {
	msg, err := decodeMessageEvent(event.Value)
	if err != nil {
		return c.reject(ctx, event, err)
	}

	conflicts, err := c.store.PersistMessages(ctx, []model.Message{*msg})
	if err != nil {
		return fmt.Errorf("落库消息 %s#%d: %w", msg.ConvID, msg.Seq, err)
	}
	if len(conflicts) > 0 {
		return c.reject(ctx, event, fmt.Errorf("消息 seq 或 clientMsgId 冲突: %s#%d", msg.ConvID, msg.Seq))
	}

	// offset 可能重放。读取当前记录，避免旧事件把已撤回的消息重新写成正常状态。
	stored, err := c.store.GetMessage(ctx, msg.ConvID, msg.Seq)
	if err != nil || stored == nil {
		c.logger.Warn("读取已落库消息失败，接收方可从 MySQL 补拉",
			zap.String("convID", msg.ConvID), zap.Int64("seq", msg.Seq), zap.Error(err))
		return nil
	}
	if err := c.deliverer.DeliverMessage(ctx, stored); err != nil {
		c.logger.Warn("在线投递失败，接收方可从 MySQL 补拉",
			zap.String("convID", msg.ConvID), zap.Int64("seq", msg.Seq), zap.Error(err))
	}
	return nil
}

func (c *MsgPersistConsumer) reject(ctx context.Context, event kafka.Message, cause error) error {
	if !c.dlq.enabled() {
		return drop(cause)
	}
	if !publishDeadLetter(ctx, c.logger, msgPersistConsumerName, c.dlq, 1, cause, event) {
		return ctx.Err()
	}
	return nil
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
