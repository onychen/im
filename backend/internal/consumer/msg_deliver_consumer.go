package consumer

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
)

// MessageDeliverer 由 service.MsgService 实现：更新消息缓存并推送给在线成员。
type MessageDeliverer interface {
	DeliverMessage(ctx context.Context, msg *model.Message) error
}

// MsgDeliverConsumer 以 deliver 消费者组消费 msg_topic，负责缓存更新与在线推送。
// 与 persist 组相互独立：推送不等待落库，离线或丢推送由客户端 convSync + pullMsgs 补齐。
type MsgDeliverConsumer struct {
	reader    *kafka.Reader
	deliverer MessageDeliverer
	logger    *zap.Logger
	dlq       DeadLetterPolicy
}

// NewMsgDeliverConsumer 创建消息投递消费者。
func NewMsgDeliverConsumer(reader *kafka.Reader, deliverer MessageDeliverer, logger *zap.Logger) *MsgDeliverConsumer {
	return &MsgDeliverConsumer{reader: reader, deliverer: deliverer, logger: logger}
}

// WithDeadLetter 设置重试上限与死信 Topic。
func (c *MsgDeliverConsumer) WithDeadLetter(p DeadLetterPolicy) *MsgDeliverConsumer {
	c.dlq = p
	return c
}

// Start 在后台 goroutine 中运行消费循环。
func (c *MsgDeliverConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "msg deliver consumer"); err != nil {
		return err
	}
	c.logger.Info("消息投递消费者已启动")
	go runReader(ctx, c.reader, c.logger, "msg-deliver", c.handle, c.dlq)
	return nil
}

func (c *MsgDeliverConsumer) handle(ctx context.Context, km kafka.Message) error {
	msg, err := decodeMessageEvent(km.Value)
	if err != nil {
		return drop(err)
	}
	return c.deliverer.DeliverMessage(ctx, msg)
}
