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
	topicMsg         = "msg_topic"
	topicMomentPush  = "moment_push"
	topicLikePersist = "like_persist"
)

// MQRepo 定义了业务服务所需的 MQ 发布操作。
// 该接口便于在测试中进行 mock。
type MQRepo interface {
	// PublishMessage 把一条已分配 seq 的聊天消息写入 msg_topic。
	// 返回 nil 即代表 ISR 全部确认，消息已提交。
	PublishMessage(ctx context.Context, msg *model.Message) error
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
// 返回 nil 即代表所有 ISR 副本已确认。
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

// PublishMessage 以 convId 为分区 Key，保证同一会话的消息在同一分区内按 seq 有序。
func (m *MQRepoImpl) PublishMessage(ctx context.Context, msg *model.Message) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal 消息失败: %w", err)
	}
	msgID := strconv.FormatInt(msg.MsgID, 10)
	if err := m.publish(ctx, topicMsg, msg.ConvID, body, kafka.Header{Key: "message-id", Value: []byte(msgID)}); err != nil {
		return fmt.Errorf("kafka publish 消息 %s#%d: %w", msg.ConvID, msg.Seq, err)
	}
	return nil
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
