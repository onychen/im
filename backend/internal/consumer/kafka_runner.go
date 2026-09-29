package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/repository"
)

// ──────────────────────────────────────────────────────
// Kafka 消费循环的公共实现
//
// RabbitMQ → Kafka 的语义映射：
//   ack            → CommitMessages（Reader 配置了 CommitInterval，offset 异步批量提交）
//   nack(requeue)  → 不提交 offset，原地退避重试同一条消息
//   nack(discard)  → 返回 dropError，记录日志后直接提交 offset 跳过
//   死信队列        → 配置 DeadLetterPolicy 后，重试达到上限的消息转投 DLQ Topic 再提交
//
// Kafka 分区内是严格有序的，无法像 RabbitMQ 那样把单条消息"放回队尾"，
// 因此临时性失败采用分区内阻塞重试；重试有上限并转投 DLQ，
// 避免一条坏消息无限期卡住整个分区。
// ──────────────────────────────────────────────────────

// dropError 表示消息不可恢复（如格式错误），应跳过而非重试。
type dropError struct{ err error }

func (e *dropError) Error() string { return e.err.Error() }
func (e *dropError) Unwrap() error { return e.err }

// drop 把错误标记为"丢弃"。
func drop(err error) error { return &dropError{err: err} }

func isDrop(err error) bool {
	var d *dropError
	return errors.As(err, &d)
}

const (
	retryBackoffMin = 200 * time.Millisecond
	retryBackoffMax = 10 * time.Second

	// DefaultDeadLetterAttempts 是推荐的死信前最大处理次数。
	// 配合 200ms→10s 指数退避，约 40s 后才进入 DLQ，
	// 能扛住短暂的 MySQL/Redis 抖动，又不会让坏消息长期阻塞分区。
	DefaultDeadLetterAttempts = 10

	headerDLQOriginalTopic     = "x-original-topic"
	headerDLQOriginalPartition = "x-original-partition"
	headerDLQOriginalOffset    = "x-original-offset"
	headerDLQConsumer          = "x-consumer"
	headerDLQAttempts          = "x-attempts"
)

// DeadLetterPolicy 控制消息处理的重试上限与死信投递。
// 零值表示不启用：失败的消息会一直原地退避重试。
type DeadLetterPolicy struct {
	// MaxAttempts 是转投 DLQ 前的最大处理次数（含首次），<=0 表示不启用。
	MaxAttempts int
	// Writer 用于写入 DLQ，通常复用业务 Writer（RequireAll、Topic 为空）。
	Writer repository.KafkaWriter
	// Topic 是死信 Topic 名称，如 moment_push_dlq。
	Topic string
}

func (p DeadLetterPolicy) enabled() bool {
	return p.MaxAttempts > 0 && p.Writer != nil && p.Topic != ""
}

// messageHandler 处理单条 Kafka 消息。
// 返回 nil：提交 offset；返回 drop(err)：记录并提交 offset；其它错误：退避后重试。
type messageHandler func(ctx context.Context, msg kafka.Message) error

// runReader 在当前 goroutine 中运行消费循环，直到 ctx 取消或 reader 被关闭。
func runReader(ctx context.Context, reader *kafka.Reader, logger *zap.Logger, name string, handle messageHandler, dlq DeadLetterPolicy) {
	defer func() {
		// 使用异步提交时，Close 会把尚未提交的 offset 刷到 broker。
		if err := reader.Close(); err != nil {
			logger.Warn("关闭 Kafka reader 失败", zap.String("consumer", name), zap.Error(err))
		}
		logger.Info("Kafka 消费者已停止", zap.String("consumer", name))
	}()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			logger.Warn("拉取 Kafka 消息失败", zap.String("consumer", name), zap.Error(err))
			if !sleepCtx(ctx, time.Second) {
				return
			}
			continue
		}

		if !handleWithRetry(ctx, logger, name, msg, handle, dlq) {
			return // ctx 已取消，不提交 offset，重启后会重新消费
		}
		if err := commitWithRetry(ctx, reader, msg); err != nil {
			return
		}
	}
}

// handleWithRetry 反复调用 handle 直到成功、被判定为丢弃、转投 DLQ 或 ctx 取消。
// 返回 true 表示可以提交 offset；返回 false 表示 ctx 已取消。
func handleWithRetry(ctx context.Context, logger *zap.Logger, name string, msg kafka.Message, handle messageHandler, dlq DeadLetterPolicy) bool {
	backoff := retryBackoffMin
	for attempt := 1; ; attempt++ {
		err := handle(ctx, msg)
		if err == nil {
			return true
		}
		if isDrop(err) {
			logger.Error("丢弃不可处理的 Kafka 消息",
				zap.String("consumer", name),
				zap.String("topic", msg.Topic),
				zap.Int("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			return true
		}
		if dlq.enabled() && attempt >= dlq.MaxAttempts {
			return publishDeadLetter(ctx, logger, name, dlq, attempt, err, msg)
		}
		logger.Warn("处理 Kafka 消息失败，退避后重试",
			zap.String("consumer", name),
			zap.String("topic", msg.Topic),
			zap.Int("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff),
			zap.Error(err),
		)
		if !sleepCtx(ctx, backoff) {
			return false
		}
		backoff = nextBackoff(backoff)
	}
}

// publishDeadLetter 把消息写入 DLQ，写入失败时退避重试直到成功或 ctx 取消。
// 只有写入成功才返回 true 允许提交原 offset，保证消息不会丢。
func publishDeadLetter(ctx context.Context, logger *zap.Logger, name string, dlq DeadLetterPolicy, attempts int, cause error, msgs ...kafka.Message) bool {
	out := make([]kafka.Message, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, deadLetterMessage(dlq.Topic, name, attempts, cause, m))
	}

	backoff := retryBackoffMin
	for {
		err := dlq.Writer.WriteMessages(ctx, out...)
		if err == nil {
			logger.Error("Kafka 消息重试达到上限，已转投死信队列",
				zap.String("consumer", name),
				zap.String("dlqTopic", dlq.Topic),
				zap.Int("messages", len(msgs)),
				zap.Int("attempts", attempts),
				zap.Error(cause),
			)
			return true
		}
		logger.Warn("写入死信队列失败，退避后重试",
			zap.String("consumer", name),
			zap.String("dlqTopic", dlq.Topic),
			zap.Duration("backoff", backoff),
			zap.Error(err),
		)
		if !sleepCtx(ctx, backoff) {
			return false
		}
		backoff = nextBackoff(backoff)
	}
}

// deadLetterMessage 构造死信消息：保留原 Key/Value/Headers，并附加来源与失败原因，便于排查和人工回放。
func deadLetterMessage(topic, consumerName string, attempts int, cause error, m kafka.Message) kafka.Message {
	headers := setHeader(m.Headers, headerDLQOriginalTopic, m.Topic)
	headers = setHeader(headers, headerDLQOriginalPartition, strconv.Itoa(m.Partition))
	headers = setHeader(headers, headerDLQOriginalOffset, strconv.FormatInt(m.Offset, 10))
	headers = setHeader(headers, headerDLQConsumer, consumerName)
	headers = setHeader(headers, headerDLQAttempts, strconv.Itoa(attempts))
	headers = setHeader(headers, headerLastError, truncateContent(cause.Error(), 200))
	return kafka.Message{Topic: topic, Key: m.Key, Value: m.Value, Headers: headers}
}

// commitWithRetry 提交 offset，失败时退避重试；仅在 ctx 取消时返回错误。
// Reader 配置了 CommitInterval 时这里只是入队，真正的提交由 kafka-go 后台定期完成。
func commitWithRetry(ctx context.Context, reader *kafka.Reader, msgs ...kafka.Message) error {
	backoff := retryBackoffMin
	for {
		err := reader.CommitMessages(ctx, msgs...)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !sleepCtx(ctx, backoff) {
			return ctx.Err()
		}
		backoff = nextBackoff(backoff)
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > retryBackoffMax {
		return retryBackoffMax
	}
	return d
}

// sleepCtx 睡眠 d；ctx 取消时提前返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// headerValue 读取 Kafka 消息头。
func headerValue(msg kafka.Message, key string) (string, bool) {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// setHeader 返回设置（或覆盖）了指定 key 的新 header 列表。
func setHeader(headers []kafka.Header, key, value string) []kafka.Header {
	out := make([]kafka.Header, 0, len(headers)+1)
	for _, h := range headers {
		if h.Key != key {
			out = append(out, h)
		}
	}
	return append(out, kafka.Header{Key: key, Value: []byte(value)})
}

func requireReader(reader *kafka.Reader, name string) error {
	if reader == nil {
		return fmt.Errorf("%s: kafka reader 为空", name)
	}
	return nil
}
