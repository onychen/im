package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

const (
	persistMessageTopic = "message_persist"
	persistRetryTopic   = "message_persist_retry"
	persistDLQTopic     = "message_persist_dlq"

	headerRetryCount = "x-retry-count"
	headerRetryAt    = "x-retry-at" // 毫秒时间戳，到点才允许回流主 Topic
	headerLastError  = "x-last-error"

	persistRetryDelay = 5 * time.Second
)

// MessagePersistConsumer has one responsibility: idempotently archive the
// immutable event in MySQL.  Redis read-model writes and WebSocket fanout are
// intentionally absent; they already happened in the sending Lua script and
// relay respectively.
//
// 重试策略（替代 RabbitMQ 的 TTL 延迟队列 + 死信交换器）：
//
//	message_persist ──失败──▶ message_persist_retry（带 x-retry-at）
//	      ▲                          │ 到期后由 retry 消费者回流
//	      └──────────────────────────┘
//	重试次数达到上限 ──▶ message_persist_dlq
//
// 这样主 Topic 的分区不会因为一条坏消息被长时间阻塞。
type MessagePersistConsumer struct {
	reader      *kafka.Reader
	retryReader *kafka.Reader
	writer      repository.KafkaWriter
	mysqlRepo   repository.MySQLRepo
	logger      *zap.Logger
	maxRetries  int
	retryDelay  time.Duration
}

func NewMessagePersistConsumer(reader, retryReader *kafka.Reader, writer repository.KafkaWriter, mysqlRepo repository.MySQLRepo, logger *zap.Logger) *MessagePersistConsumer {
	return &MessagePersistConsumer{
		reader:      reader,
		retryReader: retryReader,
		writer:      writer,
		mysqlRepo:   mysqlRepo,
		logger:      logger,
		maxRetries:  5,
		retryDelay:  persistRetryDelay,
	}
}

func (c *MessagePersistConsumer) Start(ctx context.Context) error {
	if err := requireReader(c.reader, "message persist consumer"); err != nil {
		return err
	}
	if err := requireReader(c.retryReader, "message persist retry consumer"); err != nil {
		return err
	}
	if c.writer == nil {
		return fmt.Errorf("message persist consumer writer is nil")
	}
	// 主 Topic 自带 retry Topic + DLQ；这里只有转投失败才会原地重试，必须重试到成功，
	// 因此不启用通用死信策略（零值）。
	go runReader(ctx, c.reader, c.logger, "message-persist", c.handle, DeadLetterPolicy{})
	go runReader(ctx, c.retryReader, c.logger, "message-persist-retry", c.handleRetry, DeadLetterPolicy{})
	c.logger.Info("message persistence consumer started")
	return nil
}

// handle 消费主 Topic：幂等落库；失败时转投 retry Topic，然后提交 offset。
func (c *MessagePersistConsumer) handle(ctx context.Context, msg kafka.Message) error {
	var evt model.MessagePersistEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil || evt.ServerMsgID == 0 {
		if err == nil {
			err = fmt.Errorf("serverMsgId is zero")
		}
		return drop(fmt.Errorf("invalid message persistence event: %w", err))
	}
	if c.mysqlRepo == nil {
		return nil
	}
	var err error
	switch evt.ConvType {
	case model.ConvTypePrivate:
		err = c.mysqlRepo.InsertPrivateMessage(ctx, &model.PrivateMessage{ID: evt.ServerMsgID, ClientMsgID: evt.ClientMsgID, SenderID: evt.SenderID, ReceiverID: evt.ReceiverID, Content: evt.Content, MsgType: evt.MsgType, CreatedAt: time.UnixMilli(evt.ServerTimestamp)})
	case model.ConvTypeGroup:
		err = c.mysqlRepo.InsertGroupMessage(ctx, &model.GroupMessage{ID: evt.ServerMsgID, ClientMsgID: evt.ClientMsgID, GroupID: evt.GroupID, SenderID: evt.SenderID, Content: evt.Content, MsgType: evt.MsgType, GroupSeq: evt.GroupSeq, CreatedAt: time.UnixMilli(evt.ServerTimestamp)})
	default:
		return drop(fmt.Errorf("unknown convType %d", evt.ConvType))
	}
	if err == nil {
		return nil
	}
	return c.retryOrDeadLetter(ctx, msg, err)
}

// retryOrDeadLetter 把失败的消息转投 retry Topic（或 DLQ）。
// 只有转投成功才返回 nil 让主 Topic 提交 offset；转投失败则返回错误，
// 由 runReader 原地退避重试，保证消息不会丢。
func (c *MessagePersistConsumer) retryOrDeadLetter(ctx context.Context, msg kafka.Message, cause error) error {
	count := retryCount(msg)
	headers := setHeader(msg.Headers, headerLastError, truncateContent(cause.Error(), 200))

	if count >= c.maxRetries {
		c.logger.Error("message persistence moved to dead letter", zap.Int("retries", count), zap.Error(cause))
		if err := c.writer.WriteMessages(ctx, kafka.Message{Topic: persistDLQTopic, Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
			return fmt.Errorf("publish to dlq: %w", err)
		}
		return nil
	}

	headers = setHeader(headers, headerRetryCount, strconv.Itoa(count+1))
	headers = setHeader(headers, headerRetryAt, strconv.FormatInt(time.Now().Add(c.retryDelay).UnixMilli(), 10))
	if err := c.writer.WriteMessages(ctx, kafka.Message{Topic: persistRetryTopic, Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
		c.logger.Error("republish message persistence retry failed", zap.Error(err))
		return fmt.Errorf("publish to retry topic: %w (cause: %v)", err, cause)
	}
	c.logger.Warn("message persistence failed; scheduled retry", zap.Int("retry", count+1), zap.Error(cause))
	return nil
}

// handleRetry 消费 retry Topic：等到 x-retry-at 到期后把消息写回主 Topic。
// retry Topic 内消息按写入顺序排列、延迟相同，因此队头等待不会拖慢后续消息。
func (c *MessagePersistConsumer) handleRetry(ctx context.Context, msg kafka.Message) error {
	if raw, ok := headerValue(msg, headerRetryAt); ok {
		if dueMs, err := strconv.ParseInt(raw, 10, 64); err == nil {
			if wait := time.Until(time.UnixMilli(dueMs)); wait > 0 && !sleepCtx(ctx, wait) {
				return ctx.Err()
			}
		}
	}
	if err := c.writer.WriteMessages(ctx, kafka.Message{Topic: persistMessageTopic, Key: msg.Key, Value: msg.Value, Headers: msg.Headers}); err != nil {
		return fmt.Errorf("requeue persistence event: %w", err)
	}
	return nil
}

func retryCount(msg kafka.Message) int {
	raw, ok := headerValue(msg, headerRetryCount)
	if !ok {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		return 0
	}
	return n
}
