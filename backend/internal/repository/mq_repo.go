package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/goim/goim/internal/model"
)

// Topic 名称与 infra 包保持一致；这里单独声明是为了避免 repository → infra 的反向依赖。
const (
	topicMessagePersist    = "message_persist"
	topicPrivateMsgPersist = "private_msg_persist"
	topicGroupMsgFanout    = "group_msg_fanout"
	topicMomentPush        = "moment_push"
	topicLikePersist       = "like_persist"
)

// MQRepo 定义了消息服务所需的 MQ 发布操作。
// 该接口便于在测试中进行 mock。
type MQRepo interface {
	PublishPrivateMsg(ctx context.Context, msg *model.PrivateMessage) error
	PublishGroupMsg(ctx context.Context, msg *model.GroupMessage) error
	PublishMomentPush(ctx context.Context, moment *model.Moment) error
	PublishLikeEvent(ctx context.Context, evt *model.LikeEvent) error
}

// KafkaWriter 抽象出 *kafka.Writer 中用到的方法，便于单测替换。
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

// ──────────────────────────────────────────────────────
// MQRepoImpl — 基于 segmentio/kafka-go 的具体实现
// ──────────────────────────────────────────────────────

type MQRepoImpl struct {
	writer KafkaWriter
}

// NewMQRepo 使用给定 Writer 创建 MQ 仓储。Writer 需配置 RequiredAcks=RequireAll 且 Topic 为空。
func NewMQRepo(writer KafkaWriter) *MQRepoImpl {
	return &MQRepoImpl{writer: writer}
}

const mqPublishTimeout = 5 * time.Second

// publish 同步写入一条消息。Writer 为 RequireAll + 同步模式，
// 返回 nil 即代表所有 ISR 副本已确认，语义等价于 RabbitMQ publisher confirm。
func (m *MQRepoImpl) publish(ctx context.Context, topic, key string, body []byte, headers ...kafka.Header) error {
	if m.writer == nil {
		return fmt.Errorf("kafka writer 为空")
	}
	pubCtx, cancel := context.WithTimeout(ctx, mqPublishTimeout)
	defer cancel()
	return m.writer.WriteMessages(pubCtx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   body,
		Headers: append([]kafka.Header{{Key: "content-type", Value: []byte("application/json")}}, headers...),
		Time:    time.Now(),
	})
}

// PublishPersistEvent 把 Redis Stream 中的持久化事件投递到 message_persist。
// 以会话 ID 为分区 Key，保证同一会话的落库事件有序；
// 写入成功（ISR 全部确认）才会让 relay XACK/XDEL Stream 条目。
func (m *MQRepoImpl) PublishPersistEvent(ctx context.Context, evt *model.MessagePersistEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal persist event: %w", err)
	}
	key := persistEventKey(evt)
	msgID := strconv.FormatInt(evt.ServerMsgID, 10)
	if err := m.publish(ctx, topicMessagePersist, key, body, kafka.Header{Key: "message-id", Value: []byte(msgID)}); err != nil {
		return fmt.Errorf("kafka publish persist event %d: %w", evt.ServerMsgID, err)
	}
	return nil
}

func persistEventKey(evt *model.MessagePersistEvent) string {
	if evt.ConvType == model.ConvTypeGroup {
		return model.BuildConvID(model.ConvTypeGroup, evt.GroupID, 0)
	}
	return model.BuildConvID(model.ConvTypePrivate, evt.SenderID, evt.ReceiverID)
}

func (m *MQRepoImpl) PublishPrivateMsg(ctx context.Context, msg *model.PrivateMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal 私聊消息失败: %w", err)
	}
	key := model.BuildConvID(model.ConvTypePrivate, msg.SenderID, msg.ReceiverID)
	return m.publish(ctx, topicPrivateMsgPersist, key, body)
}

func (m *MQRepoImpl) PublishGroupMsg(ctx context.Context, msg *model.GroupMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal 群聊消息失败: %w", err)
	}
	key := model.BuildConvID(model.ConvTypeGroup, msg.GroupID, 0)
	return m.publish(ctx, topicGroupMsgFanout, key, body)
}

func (m *MQRepoImpl) PublishMomentPush(ctx context.Context, moment *model.Moment) error {
	body, err := json.Marshal(moment)
	if err != nil {
		return fmt.Errorf("marshal 朋友圈消息失败: %w", err)
	}
	return m.publish(ctx, topicMomentPush, strconv.FormatInt(moment.AuthorID, 10), body)
}

// PublishLikeEvent 将点赞/取消赞事件投递到 like_persist，由消费者异步批量削峰写入 MySQL。
// 以 momentID 为 Key，同一动态的点赞/取消赞落在同一分区，保证 last-action-wins 的顺序正确。
func (m *MQRepoImpl) PublishLikeEvent(ctx context.Context, evt *model.LikeEvent) error {
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal 点赞事件失败: %w", err)
	}
	return m.publish(ctx, topicLikePersist, strconv.FormatInt(evt.MomentID, 10), body)
}
