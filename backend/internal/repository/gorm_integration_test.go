//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/goim/goim/internal/model"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GOIM_TEST_MYSQL_DSN 指向可创建临时库的测试 MySQL；每个测试独占一个库，结束后删除。
func newGORMTestRepo(t *testing.T) (*MySQLRepoImpl, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("GOIM_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set GOIM_TEST_MYSQL_DSN to run MySQL integration tests")
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	require.NoError(t, err)
	cfg.ParseTime = true
	cfg.DBName = ""
	admin, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	adminPool, err := admin.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = adminPool.Close() })
	name := fmt.Sprintf("goim_gorm_test_%d", time.Now().UnixNano())
	require.NoError(t, admin.Exec("CREATE DATABASE `"+name+"` CHARACTER SET utf8mb4").Error)
	t.Cleanup(func() { require.NoError(t, admin.Exec("DROP DATABASE `"+name+"`").Error) })
	cfg.DBName = name
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	for _, migration := range []string{
		"001_create_users.sql", "002_create_friendships.sql", "003_create_groups.sql",
		"005_create_moments.sql", "006_create_misc.sql", "008_create_user_settings.sql", "013_create_conversation_messages.sql", "016_conversation_seq_segments.sql",
		"017_create_moment_outbox.sql", "018_group_history_version.sql",
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "scripts", "migrations", migration))
		require.NoError(t, err)
		for _, statement := range strings.Split(string(data), ";") {
			if strings.TrimSpace(statement) != "" {
				require.NoError(t, db.Exec(statement).Error, migration)
			}
		}
	}
	return NewMySQLRepo(db), db
}

func TestGroupHistoryVersionCoversOutOfOrderWritesAndRevokes(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	convID := fmt.Sprintf("g_%d", time.Now().UnixNano())
	require.NoError(t, repo.EnsureConversation(ctx, convID, model.ConvTypeGroup))
	require.NoError(t, repo.JoinConversation(ctx, convID, []int64{2}, 9))
	state, err := repo.GetHistoryState(ctx, 2, convID, model.ConvTypeGroup)
	require.NoError(t, err)
	require.Equal(t, &HistoryState{MinSeq: 9}, state)
	missing, err := repo.GetHistoryState(ctx, 3, convID, model.ConvTypeGroup)
	require.NoError(t, err)
	require.Nil(t, missing)

	msg := model.Message{ConvID: convID, ConvType: model.ConvTypeGroup, Seq: 20, MsgID: 20,
		SenderID: 1, MsgType: model.MsgTypeText, Content: "latest", Timestamp: time.Now().UnixMilli()}
	conflicts, err := repo.PersistMessages(ctx, []model.Message{msg})
	require.NoError(t, err)
	require.Empty(t, conflicts)
	state, err = repo.GetHistoryState(ctx, 2, convID, model.ConvTypeGroup)
	require.NoError(t, err)
	require.EqualValues(t, 1, state.Version)
	window, err := repo.GetGroupHistoryWindow(ctx, convID, state.Version, 200)
	require.NoError(t, err)
	require.Equal(t, []model.Message{msg}, window)

	notice := msg
	notice.Seq, notice.MsgID, notice.MsgType = 11, 11, model.MsgTypeRevoked
	notice.Content = `{"targetSeq":20,"targetMsgId":"20"}`
	conflicts, err = repo.PersistMessages(ctx, []model.Message{notice})
	require.NoError(t, err)
	require.Empty(t, conflicts)
	maxSeq, err := repo.GetMaxSeq(ctx, convID)
	require.NoError(t, err)
	require.EqualValues(t, 20, maxSeq)
	updated, err := repo.GetHistoryState(ctx, 2, convID, model.ConvTypeGroup)
	require.NoError(t, err)
	require.EqualValues(t, 2, updated.Version)
	stale, err := repo.GetGroupHistoryWindow(ctx, convID, state.Version, 200)
	require.NoError(t, err)
	require.Empty(t, stale)
	newWindow, err := repo.GetGroupHistoryWindow(ctx, convID, updated.Version, 200)
	require.NoError(t, err)
	require.Len(t, newWindow, 2)
	require.Equal(t, model.MsgStatusRevoked, newWindow[0].Status)
	require.Equal(t, notice, newWindow[1])

	t.Run("Redis window matches the requested version", func(t *testing.T) {
		addr := os.Getenv("GOIM_TEST_REDIS_ADDR")
		if addr == "" {
			t.Skip("set GOIM_TEST_REDIS_ADDR to run Redis integration test")
		}
		redis := goredis.NewClient(&goredis.Options{Addr: addr})
		t.Cleanup(func() {
			_ = redis.Del(ctx, groupHistoryKey(convID)).Err()
			_ = redis.Close()
		})
		cache := NewMessageCache(redis)
		require.NoError(t, cache.SetGroupHistory(ctx, convID, state.Version, window))
		cached, err := cache.GetGroupHistory(ctx, convID, state.Version)
		require.NoError(t, err)
		require.Equal(t, window, cached)
		cached, err = cache.GetGroupHistory(ctx, convID, updated.Version)
		require.NoError(t, err)
		require.Nil(t, cached)
		require.NoError(t, cache.SetGroupHistory(ctx, convID, updated.Version, newWindow))
		cached, err = cache.GetGroupHistory(ctx, convID, updated.Version)
		require.NoError(t, err)
		require.Equal(t, newWindow, cached)
		require.Positive(t, redis.TTL(ctx, groupHistoryKey(convID)).Val())
	})
}

func TestGORMUsersAndSettings(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	missing, err := repo.GetUserByID(ctx, 99)
	require.NoError(t, err)
	require.Nil(t, missing)
	user := &model.User{Username: "alice", PasswordHash: "hash", Nickname: "Alice", Gender: 1}
	require.NoError(t, repo.CreateUser(ctx, user))
	require.Positive(t, user.ID)
	got, err := repo.GetUserByUsername(ctx, "alice")
	require.NoError(t, err)
	require.Equal(t, user.ID, got.ID)
	require.False(t, got.CreatedAt.IsZero())
	user.Nickname, user.Sign, user.Gender = "", "", 0
	require.NoError(t, repo.UpdateUser(ctx, user))
	got, err = repo.GetUserByID(ctx, user.ID)
	require.NoError(t, err)
	require.Empty(t, got.Nickname)
	require.Zero(t, got.Gender)
	settings := &model.UserSettings{UserID: user.ID, NotificationEnabled: true, MsgPreviewEnabled: true, MuteList: `["p_1_2"]`}
	require.NoError(t, repo.CreateOrUpdateUserSettings(ctx, settings))
	require.Positive(t, settings.ID)
	id := settings.ID
	settings.NotificationEnabled, settings.MsgPreviewEnabled, settings.MuteList = false, false, ""
	require.NoError(t, repo.CreateOrUpdateUserSettings(ctx, settings))
	require.Equal(t, id, settings.ID)
	// 相同设置重复写入也是无操作更新，仍应返回原主键。
	require.NoError(t, repo.CreateOrUpdateUserSettings(ctx, settings))
	require.Equal(t, id, settings.ID)
	loaded, err := repo.GetUserSettings(ctx, user.ID)
	require.NoError(t, err)
	require.False(t, loaded.NotificationEnabled)
	require.False(t, loaded.MsgPreviewEnabled)
	require.Empty(t, loaded.MuteList)
	briefs, err := repo.GetUserBriefs(ctx, []int64{user.ID})
	require.NoError(t, err)
	require.Equal(t, "alice", briefs[user.ID].Name)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = repo.GetUserByID(canceled, user.ID)
	require.ErrorIs(t, err, context.Canceled)
}

func TestGORMRelationsAndGroups(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx := context.Background()
	request := &model.FriendRequest{FromUserID: 1, ToUserID: 2, Message: "first"}
	require.NoError(t, repo.CreateFriendRequest(ctx, request))
	require.Positive(t, request.ID)
	id := request.ID
	oldTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, db.Model(&model.FriendRequest{}).Where("id = ?", id).UpdateColumns(map[string]any{"created_at": oldTime, "updated_at": oldTime}).Error)
	request = &model.FriendRequest{FromUserID: 1, ToUserID: 2, Message: "second"}
	require.NoError(t, repo.CreateFriendRequest(ctx, request))
	require.Equal(t, id, request.ID)
	loaded, err := repo.GetFriendRequestByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "first", loaded.Message)
	require.True(t, loaded.CreatedAt.Equal(oldTime))
	require.True(t, loaded.UpdatedAt.Equal(oldTime))
	request.Status = 1
	require.NoError(t, repo.UpdateFriendRequest(ctx, request))
	request = &model.FriendRequest{FromUserID: 1, ToUserID: 2, Message: "retry"}
	require.NoError(t, repo.CreateFriendRequest(ctx, request))
	require.Equal(t, id, request.ID)
	loaded, err = repo.GetFriendRequestByID(ctx, id)
	require.NoError(t, err)
	require.Equal(t, "retry", loaded.Message)
	require.Zero(t, loaded.Status)
	require.True(t, loaded.CreatedAt.After(oldTime))
	requests, err := repo.GetFriendRequestsByUser(ctx, 2)
	require.NoError(t, err)
	require.Len(t, requests, 1)

	require.NoError(t, repo.CreateFriendship(ctx, &model.Friendship{UserID: 1, FriendID: 2}))
	count, err := repo.CountFriends(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	friends, err := repo.GetFriendList(ctx, 2)
	require.NoError(t, err)
	require.Len(t, friends, 1)
	ok, err := repo.IsFriend(ctx, 2, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.DeleteFriendship(ctx, 1, 2))
	ok, err = repo.IsFriend(ctx, 2, 1)
	require.NoError(t, err)
	require.False(t, ok)
	// 第二个方向冲突时不能留下第一个方向。
	require.NoError(t, db.Create(&model.Friendship{UserID: 4, FriendID: 3}).Error)
	require.Error(t, repo.CreateFriendship(ctx, &model.Friendship{UserID: 3, FriendID: 4}))
	ok, err = repo.IsFriend(ctx, 3, 4)
	require.NoError(t, err)
	require.False(t, ok)
	bl := &model.Blacklist{UserID: 1, BlockedID: 2}
	require.NoError(t, repo.CreateBlacklist(ctx, bl))
	require.Positive(t, bl.ID)
	ok, err = repo.IsBlocked(ctx, 1, 2)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, repo.DeleteBlacklist(ctx, 1, 2))

	groupID, err := repo.CreateGroup(ctx, &model.Group{Name: "chat", OwnerID: 1})
	require.NoError(t, err)
	group, err := repo.GetGroupByID(ctx, groupID)
	require.NoError(t, err)
	require.Equal(t, 500, group.MaxMembers)
	group.Notice = "new notice"
	require.NoError(t, repo.UpdateGroup(ctx, group))
	require.NoError(t, repo.AddGroupMember(ctx, &model.GroupMember{GroupID: groupID, UserID: 1, Role: 2}))
	require.NoError(t, repo.UpdateGroupMemberRole(ctx, int(groupID), 1, 0))
	members, err := repo.GetGroupMembers(ctx, groupID)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Zero(t, members[0].Role)
	require.Nil(t, members[0].MutedUntil)
	require.False(t, members[0].JoinedAt.IsZero())
	ids, err := repo.ListUserGroupIDs(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{groupID}, ids)
	names, err := repo.GetGroupNames(ctx, ids)
	require.NoError(t, err)
	require.Equal(t, "chat", names[groupID])
	require.NoError(t, repo.RemoveGroupMember(ctx, groupID, 1))
}

func TestGORMMoments(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	user := &model.User{Username: "author", PasswordHash: "hash", AvatarURL: "/avatar"}
	require.NoError(t, repo.CreateUser(ctx, user))
	moment := &model.Moment{AuthorID: user.ID, Content: "hello", Visibility: 2, CreatedAt: time.Now().UTC().Truncate(time.Second), LikeCount: 99, Comments: []model.MomentComment{{Content: "view only"}}}
	require.NoError(t, repo.CreateMoment(ctx, moment))
	require.Positive(t, moment.ID)
	loaded, err := repo.GetMomentByID(ctx, moment.ID)
	require.NoError(t, err)
	require.Nil(t, loaded.MediaUrls)
	require.Zero(t, loaded.LikeCount)
	comment := &model.MomentComment{MomentID: moment.ID, UserID: user.ID, Content: "comment", Username: "view only", CreatedAt: moment.CreatedAt}
	require.NoError(t, repo.CreateMomentComment(ctx, comment))
	require.Positive(t, comment.ID)
	loadedComment, err := repo.GetMomentCommentByID(ctx, comment.ID)
	require.NoError(t, err)
	require.Equal(t, "comment", loadedComment.Content)
	comments, err := repo.GetMomentComments(ctx, moment.ID)
	require.NoError(t, err)
	require.Len(t, comments, 1)
	require.Equal(t, "author", comments[0].Username)
	require.Equal(t, "/avatar", comments[0].AvatarURL)
	count, err := repo.SetMomentLike(ctx, moment.ID, user.ID, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	likers, err := repo.ListMomentLikerProfiles(ctx, moment.ID)
	require.NoError(t, err)
	require.Equal(t, []model.MomentLiker{{UserID: user.ID, Username: "author", AvatarURL: "/avatar"}}, likers)
	moments, err := repo.GetMomentsByIDs(ctx, []int64{moment.ID})
	require.NoError(t, err)
	require.Len(t, moments, 1)
	moments, err = repo.GetMomentsByUser(ctx, user.ID, 10, 0)
	require.NoError(t, err)
	require.Len(t, moments, 1)
	require.NoError(t, repo.DeleteMoment(ctx, moment.ID))
	loaded, err = repo.GetMomentByID(ctx, moment.ID)
	require.NoError(t, err)
	require.Nil(t, loaded)
	comments, err = repo.GetMomentComments(ctx, moment.ID)
	require.NoError(t, err)
	require.Empty(t, comments)
	likers, err = repo.ListMomentLikerProfiles(ctx, moment.ID)
	require.NoError(t, err)
	require.Empty(t, likers)
}

func TestGORMMessagePersistence(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx := context.Background()
	stamp := int64(1735689600123)
	msgs := []model.Message{
		{ConvID: "p_1_2", Seq: 1, MsgID: 101, ConvType: 1, SenderID: 1, MsgType: 1, Content: "hello one", Timestamp: stamp},
		{ConvID: "p_1_2", Seq: 3, MsgID: 103, ConvType: 1, SenderID: 1, MsgType: 1, Content: "hello three", Timestamp: stamp + 2},
		{ConvID: "p_1_2", Seq: 4, MsgID: 104, ConvType: 1, SenderID: 2, MsgType: 1, Content: "hello four", ClientMsgID: "client-4", Timestamp: stamp + 3},
	}
	conflicts, err := repo.PersistMessages(ctx, msgs)
	require.NoError(t, err)
	require.Empty(t, conflicts)
	conflicts, err = repo.PersistMessages(ctx, msgs)
	require.NoError(t, err)
	require.Empty(t, conflicts)
	got, err := repo.GetMessagesBySeqRange(ctx, "p_1_2", 1, 4, 10)
	require.NoError(t, err)
	require.Equal(t, msgs, got, "毫秒时间和 NULL clientMsgId 应当无损往返")
	maxSeq, err := repo.GetMaxSeq(ctx, "p_1_2")
	require.NoError(t, err)
	require.EqualValues(t, 4, maxSeq)

	// seq 冲突、clientMsgId 冲突都不能推进会话元数据。
	badSeq, badClient := msgs[0], msgs[2]
	badSeq.MsgID = 999
	badClient.Seq, badClient.MsgID = 6, 106
	conflicts, err = repo.PersistMessages(ctx, []model.Message{badSeq, badClient})
	require.NoError(t, err)
	require.Len(t, conflicts, 2)
	old := msgs[0]
	old.Seq, old.MsgID, old.Content = 2, 102, "older"
	conflicts, err = repo.PersistMessages(ctx, []model.Message{old})
	require.NoError(t, err)
	require.Empty(t, conflicts)
	convs, err := repo.GetConversations(ctx, []string{"p_1_2"})
	require.NoError(t, err)
	require.EqualValues(t, 4, convs["p_1_2"].MaxSeq)
	require.EqualValues(t, 104, convs["p_1_2"].LastMsgID)
	require.Equal(t, stamp+3, convs["p_1_2"].LastMsgAt)
	uc, err := repo.GetUserConversation(ctx, 1, "p_1_2")
	require.NoError(t, err)
	require.EqualValues(t, 3, uc.ReadSeq)
	require.NoError(t, repo.UpdateReadSeq(ctx, 1, "p_1_2", 1))
	uc, err = repo.GetUserConversation(ctx, 1, "p_1_2")
	require.NoError(t, err)
	require.EqualValues(t, 3, uc.ReadSeq)
	before, err := repo.GetMessagesBefore(ctx, 2, "p_1_2", 4, 2, 1)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.EqualValues(t, 3, before[0].Seq)

	notice, err := json.Marshal(model.RevokeNotice{TargetSeq: 1, TargetMsgID: 101, OperatorID: 1})
	require.NoError(t, err)
	_, err = repo.PersistMessages(ctx, []model.Message{{ConvID: "p_1_2", Seq: 5, MsgID: 105, ConvType: 1, SenderID: 1, MsgType: model.MsgTypeRevoked, Content: string(notice), Timestamp: stamp + 4}})
	require.NoError(t, err)
	message, err := repo.GetMessage(ctx, "p_1_2", 1)
	require.NoError(t, err)
	require.Equal(t, model.MsgStatusRevoked, message.Status)
	counts, err := repo.CountUnread(ctx, 2, []SeqRange{{ConvID: "p_1_2", From: 1, To: 5}})
	require.NoError(t, err)
	require.EqualValues(t, 2, counts["p_1_2"])

	// 搜索只包含可见、未撤回且未被个人删除的消息。
	search, err := repo.SearchMessages(ctx, 2, "hello", "p_1_2", 20)
	require.NoError(t, err)
	require.Len(t, search, 2)
	require.NoError(t, repo.AddUserDeletedMessage(ctx, 2, "p_1_2", 3))
	require.NoError(t, repo.AddUserDeletedMessage(ctx, 2, "p_1_2", 3))
	deleted, err := repo.GetUserDeletedSeqs(ctx, 2, "p_1_2", 1, 5)
	require.NoError(t, err)
	require.Contains(t, deleted, int64(3))
	require.NoError(t, repo.UpdateMinSeq(ctx, 2, "p_1_2", 4, true))
	search, err = repo.SearchMessages(ctx, 2, "hello", "", 20)
	require.NoError(t, err)
	require.Len(t, search, 1)
	require.EqualValues(t, 4, search[0].Seq)
	require.NoError(t, repo.EnsureUserConversations(ctx, "p_1_2", []int64{2}))
	uc, err = repo.GetUserConversation(ctx, 2, "p_1_2")
	require.NoError(t, err)
	require.False(t, uc.Hidden)
	require.EqualValues(t, 4, uc.MinSeq)
	require.NoError(t, repo.JoinConversation(ctx, "g_1", []int64{2}, 9))
	require.NoError(t, repo.JoinConversation(ctx, "g_1", []int64{2}, 99))
	uc, err = repo.GetUserConversation(ctx, 2, "g_1")
	require.NoError(t, err)
	require.EqualValues(t, 9, uc.MinSeq)
	require.EqualValues(t, 8, uc.ReadSeq)
	require.NoError(t, repo.EnsureConversation(ctx, "g_1", 2))
	convs, err = repo.GetConversations(ctx, []string{"g_1"})
	require.NoError(t, err)
	require.Zero(t, convs["g_1"].LastMsgAt)
	require.NoError(t, repo.DeleteUserConversation(ctx, 2, "g_1"))
	uc, err = repo.GetUserConversation(ctx, 2, "g_1")
	require.NoError(t, err)
	require.Nil(t, uc)
}

func TestGORMMessageTransactionRollback(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx := context.Background()
	_, err := repo.PersistMessages(ctx, []model.Message{{ConvID: "p_1_2", Seq: 1, MsgID: 1, ConvType: 1, SenderID: 1, MsgType: 1, Content: "hello", Timestamp: 1735689600123}})
	require.NoError(t, err)
	// 让会话写入失败，确认此前已插入的消息也被回滚。
	require.NoError(t, db.Exec(`CREATE TRIGGER fail_conversation BEFORE INSERT ON conversations
		FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test rollback'`).Error)
	_, err = repo.PersistMessages(ctx, []model.Message{{ConvID: "p_1_2", Seq: 2, MsgID: 2, ConvType: 1, SenderID: 1, MsgType: model.MsgTypeRevoked, Content: `{"targetSeq":1,"targetMsgId":"1","operatorId":1}`, Timestamp: 1735689600124}})
	require.Error(t, err)
	message, err := repo.GetMessage(ctx, "p_1_2", 2)
	require.NoError(t, err)
	require.Nil(t, message)
	message, err = repo.GetMessage(ctx, "p_1_2", 1)
	require.NoError(t, err)
	require.Equal(t, model.MsgStatusNormal, message.Status, "撤回状态也必须回滚")
}
