package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// ──────────────────────────────────────────────────────
// Kafka 消费循环的公共实现
//
// RabbitMQ → Kafka 的语义映射：
//   ack            → CommitMessages（offset 前移）
//   nack(requeue)  → 不提交 offset，原地退避重试同一条消息
//   nack(discard)  → 返回 dropError，记录日志后直接提交 offset 跳过
//
// Kafka 分区内是严格有序的，无法像 RabbitMQ 那样把单条消息"放回队尾"，
// 因此临时性失败采用分区内阻塞重试；这恰好也保证了同一 Key（会话/动态）
// 的事件不会被乱序处理。
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
)

// messageHandler 处理单条 Kafka 消息。
// 返回 nil：提交 offset；返回 drop(err)：记录并提交 offset；其它错误：退避后重试。
type messageHandler func(ctx context.Context, msg kafka.Message) error

// runReader 在当前 goroutine 中运行消费循环，直到 ctx 取消或 reader 被关闭。
func runReader(ctx context.Context, reader *kafka.Reader, logger *zap.Logger, name string, handle messageHandler) {
	defer func() {
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

		if !handleWithRetry(ctx, logger, name, msg, handle) {
			return // ctx 已取消，不提交 offset，重启后会重新消费
		}
		if err := commitWithRetry(ctx, reader, msg); err != nil {
			return
		}
	}
}

// handleWithRetry 反复调用 handle 直到成功、被判定为丢弃或 ctx 取消。
// 返回 false 表示 ctx 已取消。
func handleWithRetry(ctx context.Context, logger *zap.Logger, name string, msg kafka.Message, handle messageHandler) bool {
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
		backoff *= 2
		if backoff > retryBackoffMax {
			backoff = retryBackoffMax
		}
	}
}

// commitWithRetry 提交 offset，失败时退避重试；仅在 ctx 取消时返回错误。
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
		backoff *= 2
		if backoff > retryBackoffMax {
			backoff = retryBackoffMax
		}
	}
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
