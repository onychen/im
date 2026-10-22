package service

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/idgen"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

type msgTestEnv struct {
	svc   *MsgService
	store *recordingMessageStore
	cache *fakeMessageCache
	mq    *fakeMQ
}

func newMsgTestEnv(t *testing.T) *msgTestEnv {
	t.Helper()
	ids, err := idgen.NewSnowflake(1)
	require.NoError(t, err)
	env := &msgTestEnv{store: newRecordingMessageStore(), cache: newFakeMessageCache(), mq: &fakeMQ{}}
	env.svc = NewMsgService(nil, env.store, env.cache, &recordingRelations{}, env.mq, ids, conn.NewConnectionManager(), zap.NewNop())
	return env
}

func (e *msgTestEnv) join(userID int64, convID string, readSeq, minSeq int64) {
	e.store.ucs[ucKey{userID, convID}] = model.UserConversation{UserID: userID, ConvID: convID, ReadSeq: readSeq, MinSeq: minSeq}
}

func textTo(toID int64, clientID string) *model.SendMessage {
	return &model.SendMessage{ClientMsgID: clientID, ConvType: model.ConvTypePrivate, ToID: toID, MsgType: model.MsgTypeText, Content: "hi"}
}

// ── 发送 ──

func TestSend_ResendReplaysAck(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()

	first, err := env.svc.SendMessage(ctx, 1, textTo(2, "c1"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), first.Seq)

	again, err := env.svc.SendMessage(ctx, 1, textTo(2, "c1"))
	require.NoError(t, err)
	assert.Equal(t, first, again, "重发回放同一个 serverAck")
	assert.Len(t, env.mq.published, 1, "重发不能重复写 Kafka")
}

func TestSend_PendingDuplicateRejected(t *testing.T) {
	env := newMsgTestEnv(t)
	_, _, _ = env.cache.ReserveClientMsg(context.Background(), 1, "c1") // 模拟同一消息仍在处理中

	_, err := env.svc.SendMessage(context.Background(), 1, textTo(2, "c1"))
	assert.Equal(t, CodeMsgDuplicate, AsMsgError(err).Code)
}

func TestSend_KafkaFailureReleasesDedupAndLeavesHole(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	env.mq.failNext = 1

	_, err := env.svc.SendMessage(ctx, 1, textTo(2, "c1"))
	require.Error(t, err)
	assert.Equal(t, ErrMsgSendFailed, AsMsgError(err).Message)

	ack, err := env.svc.SendMessage(ctx, 1, textTo(2, "c1"))
	require.NoError(t, err, "失败后去重 key 已释放，客户端可以重试")
	assert.Equal(t, int64(2), ack.Seq, "seq=1 成为空洞")
}

func TestSend_SeqCounterRecoveredFromMaxOfMySQLAndMeta(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.store.maxSeq[convID] = 10
	env.cache.metas[convID] = repository.ConvMeta{Seq: 12}

	ack, err := env.svc.SendMessage(context.Background(), 1, textTo(2, "c1"))
	require.NoError(t, err)
	assert.Equal(t, int64(13), ack.Seq)
}

func TestSend_ConcurrentSendsPublishInSeqOrder(t *testing.T) {
	env := newMsgTestEnv(t)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := env.svc.SendMessage(context.Background(), 1, textTo(2, fmt.Sprintf("c%d", i)))
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()

	require.Len(t, env.mq.published, 50)
	for i, m := range env.mq.published {
		assert.Equal(t, int64(i+1), m.Seq, "Kafka 中的顺序必须与 seq 顺序一致")
	}
}

func TestSend_RejectsServerOnlyMsgTypes(t *testing.T) {
	env := newMsgTestEnv(t)
	req := textTo(2, "c1")
	req.MsgType = model.MsgTypeRevoked
	_, err := env.svc.SendMessage(context.Background(), 1, req)
	assert.Equal(t, CodeMsgBadRequest, AsMsgError(err).Code)
}

// ── 补拉 ──

func TestPull_ClampsToDeliveredWatermark(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.join(2, convID, 0, 1)
	env.cache.seqs[convID] = 5 // 已分配到 5，但 4、5 还在写 Kafka
	for seq := int64(1); seq <= 3; seq++ {
		require.NoError(t, env.cache.CacheMessage(ctx, &model.Message{ConvID: convID, Seq: seq, MsgID: seq, SenderID: 1}))
	}

	resp, err := env.svc.PullMessages(ctx, 2, convID, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.ToSeq, "toSeq 不能越过已投递水位")
	assert.Equal(t, int64(3), resp.Watermark)
	assert.Len(t, resp.Msgs, 3)

	resp, err = env.svc.PullMessages(ctx, 2, convID, 4, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.ToSeq, "显式请求超过水位的区间也要截断，客户端不会把 4、5 当作空洞跳过")
	assert.Empty(t, resp.Msgs)
}

func TestPull_MergesMySQLAndCacheAndRespectsMinSeq(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := model.BuildConvID(model.ConvTypeGroup, 9, 0)
	env.join(2, convID, 0, 3)
	env.store.maxSeq[convID] = 4
	for seq := int64(1); seq <= 4; seq++ {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq, MsgID: seq})
	}
	require.NoError(t, env.cache.CacheMessage(ctx, &model.Message{ConvID: convID, Seq: 5, MsgID: 5})) // 尚未落库

	resp, err := env.svc.PullMessages(ctx, 2, convID, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.FromSeq, "fromSeq 被抬到 min_seq")
	assert.Equal(t, int64(5), resp.ToSeq)
	seqs := make([]int64, 0, len(resp.Msgs))
	for _, m := range resp.Msgs {
		seqs = append(seqs, m.Seq)
	}
	assert.Equal(t, []int64{3, 4, 5}, seqs)
}

func TestPull_NonMemberRejected(t *testing.T) {
	env := newMsgTestEnv(t)
	_, err := env.svc.PullMessages(context.Background(), 3, model.BuildConvID(model.ConvTypePrivate, 1, 2), 1, 0)
	assert.Equal(t, CodeMsgConvNotFound, AsMsgError(err).Code)
}

// ── 会话列表 ──

func TestListConversations_UnreadExcludesRevokeAndOwnMessages(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.join(1, convID, 2, 1)
	env.store.convs[convID] = model.Conversation{ConvID: convID, ConvType: model.ConvTypePrivate, MaxSeq: 6, LastMsgAt: 1}
	env.store.nonUnread[convID] = 2 // (2, 6] 内有一条撤回通知和它撤回的原消息
	env.store.briefs[2] = repository.UserBrief{Name: "bob", AvatarURL: "a.png"}

	views, err := env.svc.ListConversations(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, int64(2), views[0].Unread)
	assert.Equal(t, int64(2), views[0].TargetID)
	assert.Equal(t, "bob", views[0].TargetName)
	assert.Equal(t, "a.png", views[0].TargetAvatar)
	assert.Equal(t, []repository.SeqRange{{ConvID: convID, From: 3, To: 6}}, env.store.rangeCalls)
}

func TestListConversations_SkipsCorrectionForHugeUnread(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypeGroup, 9, 0)
	env.join(1, convID, 0, 1)
	env.store.convs[convID] = model.Conversation{ConvID: convID, ConvType: model.ConvTypeGroup, MaxSeq: unreadExactLimit + 5}
	env.store.groupNames[9] = "team"

	views, err := env.svc.ListConversations(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, int64(unreadExactLimit+5), views[0].Unread)
	assert.Equal(t, "team", views[0].TargetName)
	assert.Empty(t, env.store.rangeCalls, "超过上限不做区间扫描")
}

func TestClampLimit(t *testing.T) {
	assert.Equal(t, 20, clampLimit(0, 100))
	assert.Equal(t, 50, clampLimit(50, 100))
	assert.Equal(t, 100, clampLimit(500, 100), "超过上限截到上限，而不是退回默认值")
}
