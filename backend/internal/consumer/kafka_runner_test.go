package consumer

import (
	"context"
	"errors"
	"fmt"
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
	}, DeadLetterPolicy{})
	assert.True(t, ok)
	assert.Equal(t, 2, calls)
}

func TestHandleWithRetry_StopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok := handleWithRetry(ctx, zap.NewNop(), "test", kafka.Message{}, func(context.Context, kafka.Message) error {
		return errors.New("always fail")
	}, DeadLetterPolicy{})
	assert.False(t, ok)
}

func TestHandleWithRetry_DeadLettersAfterMaxAttempts(t *testing.T) {
	w := &captureWriter{}
	calls := 0
	msg := kafka.Message{Topic: "moment_push", Partition: 2, Offset: 42, Key: []byte("k"), Value: []byte("v")}
	ok := handleWithRetry(context.Background(), zap.NewNop(), "moment-feed", msg, func(context.Context, kafka.Message) error {
		calls++
		return errors.New("redis down")
	}, DeadLetterPolicy{MaxAttempts: 2, Writer: w, Topic: "moment_push_dlq"})

	assert.True(t, ok, "转投 DLQ 成功后应允许提交 offset")
	assert.Equal(t, 2, calls)
	require.Len(t, w.msgs, 1)
	dl := w.msgs[0]
	assert.Equal(t, "moment_push_dlq", dl.Topic)
	assert.Equal(t, []byte("k"), dl.Key)
	origTopic, _ := headerValue(dl, headerDLQOriginalTopic)
	origOffset, _ := headerValue(dl, headerDLQOriginalOffset)
	lastErr, _ := headerValue(dl, headerLastError)
	assert.Equal(t, "moment_push", origTopic)
	assert.Equal(t, "42", origOffset)
	assert.Equal(t, "redis down", lastErr)
}

func TestHandleWithRetry_DeadLetterWriteFailureDoesNotCommit(t *testing.T) {
	w := &captureWriter{err: errors.New("broker down")}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	ok := handleWithRetry(ctx, zap.NewNop(), "test", kafka.Message{}, func(context.Context, kafka.Message) error {
		return errors.New("mysql down")
	}, DeadLetterPolicy{MaxAttempts: 1, Writer: w, Topic: "dlq"})
	assert.False(t, ok, "DLQ 写入失败时不能提交 offset，否则会丢消息")
}

func TestDeadLetterPolicyDisabledByDefault(t *testing.T) {
	assert.False(t, DeadLetterPolicy{}.enabled())
	assert.False(t, DeadLetterPolicy{MaxAttempts: 3, Topic: "dlq"}.enabled())
}

func TestSetHeaderOverrides(t *testing.T) {
	h := setHeader([]kafka.Header{{Key: "a", Value: []byte("1")}}, "a", "2")
	require.Len(t, h, 1)
	assert.Equal(t, "2", string(h[0].Value))
}
