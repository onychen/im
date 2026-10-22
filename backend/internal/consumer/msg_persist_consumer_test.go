package consumer

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// persistStoreStub 只实现 PersistMessages，其它方法未被 persist 消费者使用。
type persistStoreStub struct {
	repository.MessageStore
	got       []model.Message
	conflicts func(msgs []model.Message) []model.Message
}

func (s *persistStoreStub) PersistMessages(_ context.Context, msgs []model.Message) ([]model.Message, error) {
	s.got = append(s.got, msgs...)
	if s.conflicts != nil {
		return s.conflicts(msgs), nil
	}
	return nil, nil
}

func encodeEvent(t *testing.T, msg model.Message) kafka.Message {
	t.Helper()
	b, err := json.Marshal(msg)
	require.NoError(t, err)
	return kafka.Message{Topic: "msg_topic", Key: []byte(msg.ConvID), Value: b}
}

func newPersistForTest(store repository.MessageStore, w *captureWriter) *MsgPersistConsumer {
	return NewMsgPersistConsumer(nil, store, zap.NewNop(), 10, 10).
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
	c := newPersistForTest(store, w)

	bad := kafka.Message{Topic: "msg_topic", Offset: 7, Value: []byte("not json")}
	good := encodeEvent(t, model.Message{ConvID: "p_1_2", Seq: 1, MsgID: 100, SenderID: 1, MsgType: model.MsgTypeText, Content: "hi"})

	rejected, err := c.flush(context.Background(), []kafka.Message{bad, good})
	require.NoError(t, err)
	assert.Len(t, store.got, 1, "合法消息照常落库")
	require.Len(t, rejected.invalid, 1)
	assert.Empty(t, rejected.conflicts)

	assert.True(t, c.deadLetterRejected(context.Background(), 1, rejected))
	require.Len(t, w.msgs, 1, "解析失败的消息必须进 DLQ，不能只打日志跳过")
	assert.Equal(t, "msg_topic_dlq", w.msgs[0].Topic)
	offset, _ := headerValue(w.msgs[0], headerDLQOriginalOffset)
	assert.Equal(t, "7", offset)
}

func TestMsgPersist_ConflictGoesToDLQ(t *testing.T) {
	store := &persistStoreStub{conflicts: func(msgs []model.Message) []model.Message { return msgs[:1] }}
	w := &captureWriter{}
	c := newPersistForTest(store, w)

	a := encodeEvent(t, model.Message{ConvID: "p_1_2", Seq: 1, MsgID: 100, SenderID: 1, Content: "a"})
	b := encodeEvent(t, model.Message{ConvID: "p_1_2", Seq: 2, MsgID: 101, SenderID: 1, Content: "b"})

	rejected, err := c.flush(context.Background(), []kafka.Message{a, b})
	require.NoError(t, err)
	require.Len(t, rejected.conflicts, 1)
	assert.Equal(t, a.Value, rejected.conflicts[0].Value, "按 (convId, seq) 找回原始 Kafka 消息")

	assert.True(t, c.deadLetterRejected(context.Background(), 1, rejected))
	assert.Len(t, w.msgs, 1)
}

func TestMsgPersist_NoDLQConfiguredSkipsWithoutBlocking(t *testing.T) {
	c := NewMsgPersistConsumer(nil, &persistStoreStub{}, zap.NewNop(), 10, 10)
	ok := c.deadLetterRejected(context.Background(), 1, rejectedMessages{invalid: []kafka.Message{{Value: []byte("x")}}})
	assert.True(t, ok, "未配置 DLQ 时跳过坏消息，不能卡住分区")
}
