//go:build integration

package repository

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goim/goim/internal/model"
	"github.com/stretchr/testify/require"
)

func TestMessageSeqMigrationBackfillsExistingConversations(t *testing.T) {
	_, db := newGORMTestRepo(t)
	// 在测试独占的临时数据库模拟旧表结构和历史消息。
	require.NoError(t, db.Exec("ALTER TABLE conversations DROP COLUMN seq_alloc_max").Error)
	require.NoError(t, db.Exec("INSERT INTO conversations (conv_id, conv_type, max_seq) VALUES ('p_1_2', 1, 12)").Error)
	require.NoError(t, db.Exec("INSERT INTO messages (conv_id, seq, msg_id, conv_type, sender_id, content, created_at) VALUES ('p_1_2', 20, 20, 1, 1, '', NOW(3))").Error)
	data, err := os.ReadFile(filepath.Join("..", "..", "scripts", "migrations", "016_conversation_seq_segments.sql"))
	require.NoError(t, err)
	for _, statement := range strings.Split(string(data), ";") {
		if strings.TrimSpace(statement) != "" {
			require.NoError(t, db.Exec(statement).Error)
		}
	}
	var row conversationRecord
	require.NoError(t, db.Where("conv_id = ?", "p_1_2").Take(&row).Error)
	require.EqualValues(t, 20, row.SeqAllocMax)
	require.EqualValues(t, 12, row.MaxSeq, "迁移只回填号段上限，不改历史元数据")
}

func TestMessageSeqSegmentsAreDurableAndDoNotChangeMessageMax(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx := context.Background()
	const convID = "p_1_2"
	start, end, err := repo.ReserveSeqRange(ctx, convID, 1, 1000, 12)
	require.NoError(t, err)
	require.EqualValues(t, 13, start)
	require.EqualValues(t, 1012, end)
	convs, err := repo.GetConversations(ctx, []string{convID})
	require.NoError(t, err)
	require.Zero(t, convs[convID].MaxSeq)
	msg := model.Message{ConvID: convID, ConvType: 1, Seq: 13, MsgID: 13, SenderID: 1, MsgType: 1, Timestamp: time.Now().UnixMilli()}
	_, err = repo.PersistMessages(ctx, []model.Message{msg})
	require.NoError(t, err)
	_, err = repo.PersistMessages(ctx, []model.Message{msg})
	require.NoError(t, err)
	require.NoError(t, repo.EnsureConversation(ctx, convID, 1))
	var row conversationRecord
	require.NoError(t, db.Where("conv_id = ?", convID).Take(&row).Error)
	require.EqualValues(t, 13, row.MaxSeq)
	require.EqualValues(t, 1012, row.SeqAllocMax, "摘要更新/重复消费不能重置预留上限")
	// 新仓储实例不依赖 Redis，继续从数据库预留上限之后取号。
	start, end, err = NewMySQLRepo(db).ReserveSeqRange(ctx, convID, 1, 1000, 13)
	require.NoError(t, err)
	require.EqualValues(t, 1013, start)
	require.EqualValues(t, 2012, end)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, err = repo.ReserveSeqRange(canceled, convID, 1, 1000, 13)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMessageSeqConcurrentReservationsDoNotOverlap(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	const workers = 12
	type result struct {
		start, end int64
		err        error
	}
	results := make(chan result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start, end, err := repo.ReserveSeqRange(ctx, "g_1", 2, 1000, 0)
			results <- result{start, end, err}
		}()
	}
	wg.Wait()
	close(results)
	var ranges []result
	for r := range results {
		require.NoError(t, r.err)
		ranges = append(ranges, r)
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].start < ranges[j].start })
	for i, r := range ranges {
		require.EqualValues(t, i*1000+1, r.start)
		require.EqualValues(t, (i+1)*1000, r.end)
	}
}

func TestMessageUnreadAndHistoryCountRealVisibleRows(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	const convID = "p_1_2"
	msgs := []model.Message{
		{Seq: 1, SenderID: 1, MsgType: 1},
		{Seq: 1001, SenderID: 1, MsgType: 1},
		{Seq: 1005, SenderID: 1, MsgType: 1},
		{Seq: 1006, SenderID: 2, MsgType: 1},
		{Seq: 1007, SenderID: 1, MsgType: 1, Status: model.MsgStatusRevoked},
		{Seq: 1008, SenderID: 1, MsgType: model.MsgTypeRevoked, Content: `{}`},
		{Seq: 1009, SenderID: 1, MsgType: 1},
	}
	for i := range msgs {
		msgs[i].ConvID, msgs[i].ConvType = convID, 1
		msgs[i].MsgID, msgs[i].Timestamp = msgs[i].Seq, time.Now().UnixMilli()
	}
	_, err := repo.PersistMessages(ctx, msgs)
	require.NoError(t, err)
	require.NoError(t, repo.AddUserDeletedMessage(ctx, 2, convID, 1009))
	counts, err := repo.CountUnread(ctx, 2, []SeqRange{{ConvID: convID, From: 11, To: 1009}})
	require.NoError(t, err)
	require.EqualValues(t, 2, counts[convID], "空洞/自己发送/撤回/个人删除不能计入未读")
	counts, err = repo.CountUnread(ctx, 2, []SeqRange{{ConvID: convID, From: 1002, To: 1009}})
	require.NoError(t, err)
	require.EqualValues(t, 1, counts[convID])
	// 多条较新的个人删除记录不能形成空页阻断更早历史。
	for _, seq := range []int64{1006, 1007, 1008} {
		require.NoError(t, repo.AddUserDeletedMessage(ctx, 2, convID, seq))
	}
	page, err := repo.GetMessagesBefore(ctx, 2, convID, 2000, 2, 2)
	require.NoError(t, err)
	require.Len(t, page, 2)
	require.EqualValues(t, 1005, page[0].Seq)
	require.EqualValues(t, 1001, page[1].Seq)
}
