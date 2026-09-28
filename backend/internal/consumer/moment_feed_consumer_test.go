package consumer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
)

// ──────────────────────────────────────────────────────
// momentConsumerMySQLMock — 实现 repository.MySQLRepo 接口
// 仅 CountFriends 与 GetFriendList 有实际逻辑，其余方法 panic。
// ──────────────────────────────────────────────────────

type momentConsumerMySQLMock struct {
	friendCount map[int64]int     // userID -> 好友数
	friends     map[int64][]int64 // userID -> 好友ID列表
}

func newMomentConsumerMySQLMock() *momentConsumerMySQLMock {
	return &momentConsumerMySQLMock{
		friendCount: make(map[int64]int),
		friends:     make(map[int64][]int64),
	}
}

func (m *momentConsumerMySQLMock) CountFriends(_ context.Context, userID int64) (int, error) {
	return m.friendCount[userID], nil
}

func (m *momentConsumerMySQLMock) GetFriendList(_ context.Context, userID int64) ([]model.Friendship, error) {
	fids := m.friends[userID]
	out := make([]model.Friendship, 0, len(fids))
	for _, fid := range fids {
		out = append(out, model.Friendship{UserID: userID, FriendID: fid})
	}
	return out, nil
}

// ── 其余接口方法（消费者未使用，未实现）──

func (m *momentConsumerMySQLMock) InsertPrivateMessage(context.Context, *model.PrivateMessage) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) InsertGroupMessage(context.Context, *model.GroupMessage) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) InsertMsgRevoked(context.Context, *model.MsgRevoked) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetUserByID(context.Context, int64) (*model.User, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetUserByUsername(context.Context, string) (*model.User, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateUser(context.Context, *model.User) error { panic("未实现") }
func (m *momentConsumerMySQLMock) UpdateUser(context.Context, *model.User) error { panic("未实现") }
func (m *momentConsumerMySQLMock) DeleteMoment(context.Context, int64) error     { panic("未实现") }
func (m *momentConsumerMySQLMock) CreateFriendRequest(context.Context, *model.FriendRequest) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) UpdateFriendRequest(context.Context, *model.FriendRequest) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetFriendRequestByID(context.Context, int64) (*model.FriendRequest, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetFriendRequestsByUser(context.Context, int64) ([]model.FriendRequest, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateFriendship(context.Context, *model.Friendship) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) DeleteFriendship(context.Context, int64, int64) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) IsFriend(context.Context, int64, int64) (bool, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateBlacklist(context.Context, *model.Blacklist) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) DeleteBlacklist(context.Context, int64, int64) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) IsBlocked(context.Context, int64, int64) (bool, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateGroup(context.Context, *model.Group) (int64, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) UpdateGroup(context.Context, *model.Group) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetGroupByID(context.Context, int64) (*model.Group, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) AddGroupMember(context.Context, *model.GroupMember) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) RemoveGroupMember(context.Context, int64, int64) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetGroupMembers(context.Context, int64) ([]model.GroupMember, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) UpdateGroupMemberRole(context.Context, int, int, int) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateMoment(context.Context, *model.Moment) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentByID(context.Context, int64) (*model.Moment, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentsByIDs(context.Context, []int64) ([]model.Moment, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentsByUser(context.Context, int64, int, int) ([]model.Moment, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateMomentLike(context.Context, *model.MomentLike) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) DeleteMomentLike(context.Context, int64, int64) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentLikers(context.Context, int64) ([]int64, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) BatchUpsertMomentLikes(context.Context, []model.MomentLike) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) BatchDeleteMomentLikes(context.Context, []model.MomentLikeKey) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateMomentComment(context.Context, *model.MomentComment) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentCommentByID(context.Context, int64) (*model.MomentComment, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetMomentComments(context.Context, int64) ([]model.MomentComment, error) {
	return nil, nil
}
func (m *momentConsumerMySQLMock) DeleteMomentComment(context.Context, int64) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) GetUserSettings(context.Context, int64) (*model.UserSettings, error) {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) CreateOrUpdateUserSettings(context.Context, *model.UserSettings) error {
	panic("未实现")
}
func (m *momentConsumerMySQLMock) SearchPrivateMessages(context.Context, int64, string, int, int) ([]model.PrivateMessage, error) {
	panic("未实现")
}

// ──────────────────────────────────────────────────────
// MomentFeedConsumer 测试
// ──────────────────────────────────────────────────────

// 1. 普通用户：写入寄件箱 + 扇出到好友收件箱。
func TestMomentFeedConsumer_NormalUser_Fanout(t *testing.T) {
	redisRepo := newMockRedisRepo()
	mysqlRepo := newMomentConsumerMySQLMock()
	mysqlRepo.friendCount[100] = 2
	mysqlRepo.friends[100] = []int64{201, 202}

	c := &MomentFeedConsumer{
		redisRepo:        redisRepo,
		mysqlRepo:        mysqlRepo,
		logger:           zap.NewNop(),
		bigUserThreshold: 3,
		timelineMaxLen:   100,
	}

	moment := &model.Moment{ID: 10, AuthorID: 100, Visibility: 1, CreatedAt: time.Now()}
	err := c.process(context.Background(), moment)
	require.NoError(t, err)

	redisRepo.mu.Lock()
	defer redisRepo.mu.Unlock()
	assert.Contains(t, redisRepo.momentOutbox[100], int64(10), "作者寄件箱应包含动态")
	assert.Contains(t, redisRepo.fanoutInbox[201], int64(10), "好友201收件箱应扇出该动态")
	assert.Contains(t, redisRepo.fanoutInbox[202], int64(10), "好友202收件箱应扇出该动态")
	assert.False(t, redisRepo.bigUsers[100], "普通用户不应被标记为大V")
}

// 2. 大V用户：好友数超阈值，标记大V并跳过扇出。
func TestMomentFeedConsumer_BigUser_SkipsFanout(t *testing.T) {
	redisRepo := newMockRedisRepo()
	mysqlRepo := newMomentConsumerMySQLMock()
	mysqlRepo.friendCount[100] = 5 // > 阈值 3

	c := &MomentFeedConsumer{
		redisRepo:        redisRepo,
		mysqlRepo:        mysqlRepo,
		logger:           zap.NewNop(),
		bigUserThreshold: 3,
		timelineMaxLen:   100,
	}

	moment := &model.Moment{ID: 20, AuthorID: 100, Visibility: 1, CreatedAt: time.Now()}
	err := c.process(context.Background(), moment)
	require.NoError(t, err)

	redisRepo.mu.Lock()
	defer redisRepo.mu.Unlock()
	assert.Contains(t, redisRepo.momentOutbox[100], int64(20), "作者寄件箱应包含动态")
	assert.True(t, redisRepo.bigUsers[100], "大V用户应被标记")
	assert.Len(t, redisRepo.fanoutInbox, 0, "大V不应发生写扩散")
}

// 3. 私密动态：仅写寄件箱，不扇出。
func TestMomentFeedConsumer_Private_NoFanout(t *testing.T) {
	redisRepo := newMockRedisRepo()
	mysqlRepo := newMomentConsumerMySQLMock()
	mysqlRepo.friends[100] = []int64{201}

	c := &MomentFeedConsumer{
		redisRepo:        redisRepo,
		mysqlRepo:        mysqlRepo,
		logger:           zap.NewNop(),
		bigUserThreshold: 3,
		timelineMaxLen:   100,
	}

	moment := &model.Moment{ID: 30, AuthorID: 100, Visibility: 3, CreatedAt: time.Now()}
	err := c.process(context.Background(), moment)
	require.NoError(t, err)

	redisRepo.mu.Lock()
	defer redisRepo.mu.Unlock()
	assert.Contains(t, redisRepo.momentOutbox[100], int64(30), "私密动态仍应写入作者寄件箱")
	assert.Len(t, redisRepo.fanoutInbox, 0, "私密动态不应扇出")
	assert.False(t, redisRepo.bigUsers[100], "私密动态不应标记大V")
}

// 4. 寄件箱写入失败：返回错误以触发 nack 重试。
func TestMomentFeedConsumer_OutboxError_Retries(t *testing.T) {
	redisRepo := newMockRedisRepo()
	redisRepo.addOutboxErr = fmt.Errorf("redis down")
	mysqlRepo := newMomentConsumerMySQLMock()

	c := &MomentFeedConsumer{
		redisRepo:        redisRepo,
		mysqlRepo:        mysqlRepo,
		logger:           zap.NewNop(),
		bigUserThreshold: 3,
		timelineMaxLen:   100,
	}

	moment := &model.Moment{ID: 40, AuthorID: 100, Visibility: 1, CreatedAt: time.Now()}
	err := c.process(context.Background(), moment)
	require.Error(t, err, "寄件箱写入失败应返回错误以便重试")
}
