package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

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
	env.svc.WithSeqSegmentSize(7) // 并发发送期间跨越多个号段
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

func TestPull_ClampsToPersistedWatermarkEvenWhenCacheIsAhead(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.join(2, convID, 0, 1)
	segment := &seqSegment{end: 1000}
	segment.last.Store(5) // 已分配到 5，4/5 尚未处理
	env.svc.segments.Store(convID, segment)
	env.store.maxSeq[convID] = 3
	for seq := int64(1); seq <= 3; seq++ {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq, MsgID: seq, SenderID: 1})
	}
	require.NoError(t, env.cache.CacheMessage(ctx, &model.Message{ConvID: convID, Seq: 5, MsgID: 5, SenderID: 1}))

	resp, err := env.svc.PullMessages(ctx, 2, convID, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.ToSeq, "缓存不能推进 MySQL 水位")
	assert.Equal(t, int64(3), resp.Watermark)
	assert.Len(t, resp.Msgs, 3)

	resp, err = env.svc.PullMessages(ctx, 2, convID, 4, 5)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.ToSeq, "显式请求超过水位的区间也要截断，客户端不会把 4、5 当作空洞跳过")
	assert.Empty(t, resp.Msgs)

	env.store.maxSeq[convID] = 5
	env.store.messages[convID] = append(env.store.messages[convID], model.Message{Seq: 4}, model.Message{Seq: 5})
	resp, err = env.svc.PullMessages(ctx, 2, convID, 4, 5)
	require.NoError(t, err)
	require.Len(t, resp.Msgs, 2)
	assert.EqualValues(t, 4, resp.Msgs[0].Seq, "较小的消息落库后仍可按原游标补齐")
}

func TestPull_IgnoresCacheOnlyRowsAndRespectsMinSeq(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := model.BuildConvID(model.ConvTypeGroup, 9, 0)
	env.join(2, convID, 0, 3)
	env.store.maxSeq[convID] = 4
	for _, seq := range []int64{1, 2, 4} {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq, MsgID: seq})
	}
	require.NoError(t, env.cache.CacheMessage(ctx, &model.Message{ConvID: convID, Seq: 3, MsgID: 3}))
	require.NoError(t, env.cache.CacheMessage(ctx, &model.Message{ConvID: convID, Seq: 5, MsgID: 5})) // 尚未落库

	resp, err := env.svc.PullMessages(ctx, 2, convID, 1, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(3), resp.FromSeq, "fromSeq 被抬到 min_seq")
	assert.Equal(t, int64(4), resp.ToSeq)
	seqs := make([]int64, 0, len(resp.Msgs))
	for _, m := range resp.Msgs {
		seqs = append(seqs, m.Seq)
	}
	assert.Equal(t, []int64{4}, seqs)
}

func TestPull_NonMemberRejected(t *testing.T) {
	env := newMsgTestEnv(t)
	_, err := env.svc.PullMessages(context.Background(), 3, model.BuildConvID(model.ConvTypePrivate, 1, 2), 1, 0)
	assert.Equal(t, CodeMsgConvNotFound, AsMsgError(err).Code)
}

func TestPull_ContinuesAcrossUnusedSegmentPages(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	const convID = "p_1_2"
	env.join(2, convID, 12, 1)
	env.store.maxSeq[convID] = 1001
	env.store.messages[convID] = []model.Message{{ConvID: convID, Seq: 1001, MsgID: 1001, SenderID: 1}}
	from := int64(13)
	var got []model.Message
	for page := 0; page < 10; page++ {
		resp, err := env.svc.PullMessages(ctx, 2, convID, from, 0)
		require.NoError(t, err)
		require.GreaterOrEqual(t, resp.ToSeq, from)
		got = append(got, resp.Msgs...)
		if !resp.HasMore {
			require.EqualValues(t, 1001, resp.ToSeq)
			break
		}
		require.Empty(t, resp.Msgs, "未使用的号段页为空，但仍需继续")
		from = resp.ToSeq + 1
	}
	require.Len(t, got, 1)
	require.EqualValues(t, 1001, got[0].Seq)
}

// ── 会话列表 ──

func TestListConversations_UnreadExcludesRevokeAndOwnMessages(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.join(1, convID, 2, 1)
	env.store.convs[convID] = model.Conversation{ConvID: convID, ConvType: model.ConvTypePrivate, MaxSeq: 6, LastMsgAt: 1}
	env.store.unread[convID] = 2 // 区间内实际有效的他人消息有两条
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

func TestListConversations_CountsActualMessagesAcrossLargeHoles(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypeGroup, 9, 0)
	env.join(1, convID, 0, 1)
	env.store.convs[convID] = model.Conversation{ConvID: convID, ConvType: model.ConvTypeGroup, MaxSeq: 1005}
	require.NoError(t, env.cache.CacheMessage(context.Background(), &model.Message{ConvID: convID, Seq: 2000, MsgID: 2000}))
	env.store.unread[convID] = 2
	env.store.groupNames[9] = "team"

	views, err := env.svc.ListConversations(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, int64(2), views[0].Unread)
	assert.EqualValues(t, 1005, views[0].MaxSeq)
	assert.Nil(t, views[0].LastMsg)
	assert.Equal(t, "team", views[0].TargetName)
	assert.Equal(t, []repository.SeqRange{{ConvID: convID, From: 1, To: 1005}}, env.store.rangeCalls)
}

func TestListConversations_UnreadFailureDoesNotUseSeqDifference(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := "p_1_2"
	env.join(2, convID, 10, 1)
	env.store.convs[convID] = model.Conversation{ConvID: convID, MaxSeq: 1005}
	env.store.unreadErr = fmt.Errorf("database unavailable")
	_, err := env.svc.ListConversations(context.Background(), 2)
	require.Error(t, err)
}

func TestSend_SegmentsRenewAndRestartAbandonsUnusedNumbers(t *testing.T) {
	env := newMsgTestEnv(t)
	env.svc.WithSeqSegmentSize(3)
	ctx := context.Background()
	for i := 1; i <= 4; i++ {
		ack, err := env.svc.SendMessage(ctx, 1, textTo(2, fmt.Sprintf("c%d", i)))
		require.NoError(t, err)
		require.EqualValues(t, i, ack.Seq)
	}
	require.Equal(t, 2, env.store.reserveCalls)
	require.EqualValues(t, 6, env.store.allocated["p_1_2"])
	// 模拟重启：持久化号段保留，进程内状态丢失；旧 Redis seq 缓存也丢失。
	env.svc.segments = sync.Map{}
	env.cache.seqs = make(map[string]int64)
	ack, err := env.svc.SendMessage(ctx, 1, textTo(2, "after-restart"))
	require.NoError(t, err)
	require.EqualValues(t, 7, ack.Seq)
}

func TestSend_LegacyCounterIsOnlyAMigrationFloor(t *testing.T) {
	env := newMsgTestEnv(t)
	env.cache.seqs["p_1_2"] = 20
	ack, err := env.svc.SendMessage(context.Background(), 1, textTo(2, "legacy"))
	require.NoError(t, err)
	require.EqualValues(t, 21, ack.Seq)
	require.EqualValues(t, 20, env.cache.seqs["p_1_2"], "不能再更新旧计数器")
}

func TestSend_FailedReservationIsNotUsedAndCanRetry(t *testing.T) {
	env := newMsgTestEnv(t)
	env.store.reserveErr = fmt.Errorf("commit outcome unknown")
	_, err := env.svc.SendMessage(context.Background(), 1, textTo(2, "reserve"))
	require.Error(t, err)
	require.Empty(t, env.mq.published)
	// 数据库可能已经提交，重试必须重新申请而非复用猜测的号段。
	env.store.allocated["p_1_2"] = 1000
	env.store.reserveErr = nil
	ack, err := env.svc.SendMessage(context.Background(), 1, textTo(2, "reserve"))
	require.NoError(t, err)
	require.EqualValues(t, 1001, ack.Seq)
}

type blockingMessagePublisher struct {
	repository.MQRepo
	started chan struct{}
	resume  chan struct{}
}

func (p blockingMessagePublisher) PublishMessage(ctx context.Context, msg *model.Message) error {
	close(p.started)
	select {
	case <-p.resume:
		return p.MQRepo.PublishMessage(ctx, msg)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestMarkReadCompletesWhileSendWaitsForKafka(t *testing.T) {
	env := newMsgTestEnv(t)
	convID := model.BuildConvID(model.ConvTypePrivate, 1, 2)
	env.join(2, convID, 40, 1)
	env.store.maxSeq[convID] = 41
	publisher := blockingMessagePublisher{MQRepo: env.mq, started: make(chan struct{}), resume: make(chan struct{})}
	env.svc.mq = publisher

	sendCtx, cancelSend := context.WithCancel(context.Background())
	defer cancelSend()
	sent := make(chan error, 1)
	go func() {
		_, err := env.svc.SendMessage(sendCtx, 1, textTo(2, "blocked-send"))
		sent <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-publisher.started:
	case <-ctx.Done():
		t.Fatal("发送未进入 Kafka 写入")
	}

	require.NoError(t, env.svc.MarkRead(ctx, 2, convID, 41))
	require.EqualValues(t, 41, env.store.ucs[ucKey{2, convID}].ReadSeq)
	close(publisher.resume)
	select {
	case err := <-sent:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("解除 Kafka 阻塞后发送未完成")
	}
}

func TestBoundariesUseActualPositionInsteadOfReservedEnd(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := "p_1_2"
	env.join(2, convID, 0, 1)
	_, err := env.svc.SendMessage(ctx, 1, textTo(2, "before-clear"))
	require.NoError(t, err)
	require.EqualValues(t, 1000, env.store.allocated[convID])
	require.NoError(t, env.svc.MarkRead(ctx, 2, convID, 1000))
	require.EqualValues(t, 1, env.store.ucs[ucKey{2, convID}].ReadSeq)
	require.NoError(t, env.svc.ClearHistory(ctx, 2, convID))
	require.EqualValues(t, 2, env.store.ucs[ucKey{2, convID}].MinSeq)
	ack, err := env.svc.SendMessage(ctx, 1, textTo(2, "after-clear"))
	require.NoError(t, err)
	require.EqualValues(t, 2, ack.Seq)
	require.NoError(t, env.svc.DeleteConversation(ctx, 2, convID))
	require.EqualValues(t, 3, env.store.ucs[ucKey{2, convID}].MinSeq)
	require.True(t, env.store.ucs[ucKey{2, convID}].Hidden)
}

func TestHistoryPaginatesActualMessagesAcrossHolesAndDeletedRows(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	convID := "p_1_2"
	env.join(2, convID, 0, 2)
	for _, seq := range []int64{1, 2, 3, 1001, 1005} {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq})
	}
	env.store.deleted[ucKey{2, convID}] = map[int64]struct{}{1001: {}}
	msgs, more, err := env.svc.GetHistory(ctx, 2, convID, 0, 2)
	require.NoError(t, err)
	require.True(t, more)
	require.EqualValues(t, 3, msgs[0].Seq)
	require.EqualValues(t, 1005, msgs[1].Seq)
	msgs, more, err = env.svc.GetHistory(ctx, 2, convID, msgs[0].Seq, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, msgs, 1)
	require.EqualValues(t, 2, msgs[0].Seq)
}

func TestGroupHistorySharesWindowAndAppliesMemberVisibility(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	const convID = "g_9"
	env.join(1, convID, 0, 0)
	env.join(2, convID, 0, 1001)
	env.store.historyVersions[convID] = 1
	for _, seq := range []int64{2, 1001, 1005, 1007} {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq})
	}
	env.store.deleted[ucKey{2, convID}] = map[int64]struct{}{1007: {}}

	first, more, err := env.svc.GetHistory(ctx, 1, convID, 0, 2)
	require.NoError(t, err)
	require.True(t, more)
	require.EqualValues(t, 1005, first[0].Seq)
	require.EqualValues(t, 1007, first[1].Seq)

	second, more, err := env.svc.GetHistory(ctx, 2, convID, 0, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, second, 2)
	require.EqualValues(t, 1001, second[0].Seq)
	require.EqualValues(t, 1005, second[1].Seq)
	_, _, err = env.svc.GetHistory(ctx, 3, convID, 0, 2)
	require.ErrorIs(t, err, errConvNotFound)
	require.Equal(t, 1, env.store.historyWindowReads)
	require.Zero(t, env.store.historyPageReads)

	// 撤回与迟到的低 seq 写入都可能改变历史而不改变 maxSeq。
	env.store.historyVersions[convID]++
	env.store.messages[convID][2].Status = model.MsgStatusRevoked
	refreshed, more, err := env.svc.GetHistory(ctx, 1, convID, 0, 2)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, model.MsgStatusRevoked, refreshed[0].Status)
	require.Equal(t, 2, env.store.historyWindowReads)
}

func TestGroupHistoryFallsBackWhenCachedWindowCannotAnswer(t *testing.T) {
	env := newMsgTestEnv(t)
	ctx := context.Background()
	const convID = "g_9"
	env.join(2, convID, 0, 0)
	for seq := int64(1); seq <= groupHistoryWindowSize+1; seq++ {
		env.store.messages[convID] = append(env.store.messages[convID], model.Message{ConvID: convID, Seq: seq})
	}
	_, _, err := env.svc.GetHistory(ctx, 2, convID, 0, 2)
	require.NoError(t, err)
	require.Zero(t, env.store.historyPageReads)

	page, more, err := env.svc.GetHistory(ctx, 2, convID, 5, 2)
	require.NoError(t, err)
	require.True(t, more)
	require.EqualValues(t, 3, page[0].Seq)
	require.EqualValues(t, 4, page[1].Seq)
	require.Zero(t, env.store.historyPageReads)

	page, more, err = env.svc.GetHistory(ctx, 2, convID, 3, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, page, 2)
	require.EqualValues(t, 1, page[0].Seq)
	require.Equal(t, 1, env.store.historyPageReads)

	env.store.deleted[ucKey{2, convID}] = map[int64]struct{}{groupHistoryWindowSize + 1: {}}
	env.store.deletedErr = fmt.Errorf("deleted-seq lookup unavailable")
	page, _, err = env.svc.GetHistory(ctx, 2, convID, 0, 2)
	require.NoError(t, err)
	require.EqualValues(t, groupHistoryWindowSize, page[1].Seq)
	require.Equal(t, 2, env.store.historyPageReads)

	env.cache.historyErr = fmt.Errorf("redis unavailable")
	page, _, err = env.svc.GetHistory(ctx, 2, convID, 0, 2)
	require.NoError(t, err)
	require.EqualValues(t, groupHistoryWindowSize, page[1].Seq)
	require.Equal(t, 3, env.store.historyPageReads)
	require.Equal(t, 1, env.store.historyWindowReads)
}

func TestClampLimit(t *testing.T) {
	assert.Equal(t, 20, clampLimit(0, 100))
	assert.Equal(t, 50, clampLimit(50, 100))
	assert.Equal(t, 100, clampLimit(500, 100), "超过上限截到上限，而不是退回默认值")
}
