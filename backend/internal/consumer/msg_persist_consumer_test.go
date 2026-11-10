package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

type persistStoreStub struct {
	repository.MessageStore
	got      []model.Message
	attempts int
	err      error
	conflict bool
	stored   *model.Message
}

func (s *persistStoreStub) PersistMessages(_ context.Context, msgs []model.Message) ([]model.Message, error) {
	s.attempts++
	if s.err != nil {
		return nil, s.err
	}
	s.got = append(s.got, msgs...)
	if s.conflict {
		return msgs, nil
	}
	return nil, nil
}

func (s *persistStoreStub) GetMessage(context.Context, string, int64) (*model.Message, error) {
	return s.stored, nil
}

type deliverMessageFunc func(context.Context, *model.Message) error

func (f deliverMessageFunc) DeliverMessage(ctx context.Context, msg *model.Message) error {
	return f(ctx, msg)
}

func encodeEvent(t *testing.T, msg model.Message) kafka.Message {
	t.Helper()
	b, err := json.Marshal(msg)
	require.NoError(t, err)
	return kafka.Message{Topic: "msg_topic", Key: []byte(msg.ConvID), Value: b}
}

func newPersistForTest(store repository.MessageStore, deliverer MessageDeliverer, w *captureWriter) *MsgPersistConsumer {
	return NewMsgPersistConsumer(nil, store, deliverer, zap.NewNop()).
		WithDeadLetter(DeadLetterPolicy{MaxAttempts: 3, Writer: w, Topic: "msg_topic_dlq"})
}

func TestDecodeMessageEvent_MsgIDAsString(t *testing.T) {
	msg, err := decodeMessageEvent([]byte(`{"convId":"p_1_2","seq":3,"msgId":"372036854775807123","fromId":1}`))
	require.NoError(t, err)
	assert.Equal(t, int64(372036854775807123), msg.MsgID, "雪花 ID 以字符串传输，不能丢精度")
}

func TestMsgPersist_InvalidEventGoesToDLQ(t *testing.T) {
	store := &persistStoreStub{}
	w := &captureWriter{}
	c := newPersistForTest(store, nil, w)

	bad := kafka.Message{Topic: "msg_topic", Offset: 7, Value: []byte("not json")}
	require.NoError(t, c.handle(context.Background(), bad))
	assert.Zero(t, store.attempts)
	require.Len(t, w.msgs, 1, "解析失败的消息必须进 DLQ，不能只打日志跳过")
	assert.Equal(t, "msg_topic_dlq", w.msgs[0].Topic)
	offset, _ := headerValue(w.msgs[0], headerDLQOriginalOffset)
	assert.Equal(t, "7", offset)
}

func TestMsgPersist_ConflictGoesToDLQ(t *testing.T) {
	store := &persistStoreStub{conflict: true}
	w := &captureWriter{}
	c := newPersistForTest(store, nil, w)

	a := encodeEvent(t, model.Message{ConvID: "p_1_2", Seq: 1, MsgID: 100, SenderID: 1, Content: "a"})
	require.NoError(t, c.handle(context.Background(), a))
	assert.Len(t, w.msgs, 1)
	assert.Equal(t, a.Value, w.msgs[0].Value)
}

func TestMsgPersist_NoDLQConfiguredSkipsWithoutBlocking(t *testing.T) {
	c := NewMsgPersistConsumer(nil, &persistStoreStub{}, nil, zap.NewNop())
	assert.True(t, isDrop(c.handle(context.Background(), kafka.Message{Value: []byte("x")})))
}

func TestMsgPersist_OnlyDeliversAfterWriteAndUsesStoredStatus(t *testing.T) {
	msg := model.Message{ConvID: "p_1_2", Seq: 1, MsgID: 100, SenderID: 1, Content: "hello"}
	stored := msg
	stored.Status = model.MsgStatusRevoked
	writeErr := errors.New("mysql unavailable")
	store := &persistStoreStub{err: writeErr, stored: &stored}
	w := &captureWriter{}
	var delivered []model.Message
	c := newPersistForTest(store, deliverMessageFunc(func(_ context.Context, m *model.Message) error {
		require.Equal(t, []model.Message{msg}, store.got)
		delivered = append(delivered, *m)
		return errors.New("push unavailable")
	}), w)
	event := encodeEvent(t, msg)

	for i := 0; i < 4; i++ {
		require.ErrorIs(t, c.handle(context.Background(), event), writeErr)
	}
	require.Empty(t, delivered)
	require.Empty(t, w.msgs, "数据库故障在原 offset 重试，不转死信")

	store.err = nil
	require.NoError(t, c.handle(context.Background(), event), "已落库后，推送失败不要求再次写库")
	require.Equal(t, []model.Message{stored}, delivered, "重放也以数据库中的撤回状态为准")
}
