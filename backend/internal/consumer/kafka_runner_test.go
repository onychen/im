package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type captureWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
	err  error
}

func (w *captureWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func TestDropErrorIsDetected(t *testing.T) {
	err := drop(fmt.Errorf("bad"))
	assert.True(t, isDrop(err))
	assert.True(t, isDrop(fmt.Errorf("wrap: %w", err)))
	assert.False(t, isDrop(errors.New("transient")))
}

func TestHandleWithRetry_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	ok := handleWithRetry(context.Background(), zap.NewNop(), "test", kafka.Message{}, func(context.Context, kafka.Message) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})
	assert.True(t, ok)
	assert.Equal(t, 2, calls)
}

func TestHandleWithRetry_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := handleWithRetry(ctx, zap.NewNop(), "test", kafka.Message{}, func(context.Context, kafka.Message) error {
		return errors.New("always fail")
	})
	assert.False(t, ok)
}

func TestSetHeaderOverrides(t *testing.T) {
	h := setHeader([]kafka.Header{{Key: "a", Value: []byte("1")}}, "a", "2")
	require.Len(t, h, 1)
	assert.Equal(t, "2", string(h[0].Value))
}

func TestMessagePersist_RetryThenDeadLetter(t *testing.T) {
	w := &captureWriter{}
	c := &MessagePersistConsumer{writer: w, logger: zap.NewNop(), maxRetries: 2, retryDelay: time.Second}

	msg := kafka.Message{Key: []byte("p_1_2"), Value: []byte(`{}`)}
	require.NoError(t, c.retryOrDeadLetter(context.Background(), msg, errors.New("mysql down")))
	require.Len(t, w.msgs, 1)
	assert.Equal(t, persistRetryTopic, w.msgs[0].Topic)
	assert.Equal(t, 1, retryCount(w.msgs[0]))

	// 已达上限 → 进入 DLQ
	exhausted := kafka.Message{Key: msg.Key, Value: msg.Value, Headers: []kafka.Header{{Key: headerRetryCount, Value: []byte(strconv.Itoa(2))}}}
	require.NoError(t, c.retryOrDeadLetter(context.Background(), exhausted, errors.New("mysql down")))
	require.Len(t, w.msgs, 2)
	assert.Equal(t, persistDLQTopic, w.msgs[1].Topic)
}

func TestMessagePersist_RetryPublishFailureKeepsOffset(t *testing.T) {
	w := &captureWriter{err: errors.New("broker down")}
	c := &MessagePersistConsumer{writer: w, logger: zap.NewNop(), maxRetries: 5, retryDelay: time.Second}
	err := c.retryOrDeadLetter(context.Background(), kafka.Message{}, errors.New("mysql down"))
	require.Error(t, err, "转投失败时必须返回错误，避免提交 offset 导致丢消息")
	assert.False(t, isDrop(err))
}

func TestMessagePersist_HandleRetryRequeuesToMainTopic(t *testing.T) {
	w := &captureWriter{}
	c := &MessagePersistConsumer{writer: w, logger: zap.NewNop()}
	due := strconv.FormatInt(time.Now().Add(-time.Second).UnixMilli(), 10)
	msg := kafka.Message{Key: []byte("k"), Value: []byte("v"), Headers: []kafka.Header{{Key: headerRetryAt, Value: []byte(due)}}}
	require.NoError(t, c.handleRetry(context.Background(), msg))
	require.Len(t, w.msgs, 1)
	assert.Equal(t, persistMessageTopic, w.msgs[0].Topic)
}

func TestMessagePersist_InvalidEventIsDropped(t *testing.T) {
	c := &MessagePersistConsumer{writer: &captureWriter{}, logger: zap.NewNop()}
	err := c.handle(context.Background(), kafka.Message{Value: []byte("not json")})
	assert.True(t, isDrop(err))
}
