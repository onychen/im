package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
)

// ──────────────────────────────────────────────────────
// Mock MySQLRepo，用于 settings 测试
// ──────────────────────────────────────────────────────

type mockSettingsRepo struct {
	mu sync.Mutex

	// 存储的用户设置，以 userID 为键
	settings map[int64]*model.UserSettings
	// 下一个自增 ID
	nextID int64
	// 错误覆盖
	getSettingsErr    error
	createOrUpdateErr error
	friends           map[string]bool
}

func newMockSettingsRepo() *mockSettingsRepo {
	return &mockSettingsRepo{
		settings: make(map[int64]*model.UserSettings),
		friends:  make(map[string]bool),
		nextID:   1,
	}
}

// ── Settings 相关方法 ──

func (m *mockSettingsRepo) GetUserSettings(_ context.Context, userID int64) (*model.UserSettings, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getSettingsErr != nil {
		return nil, m.getSettingsErr
	}
	s, ok := m.settings[userID]
	if !ok {
		return nil, nil
	}
	return s, nil
}

func (m *mockSettingsRepo) CreateOrUpdateUserSettings(_ context.Context, settings *model.UserSettings) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createOrUpdateErr != nil {
		return m.createOrUpdateErr
	}
	// ON DUPLICATE KEY UPDATE semantics: update if exists, insert if not
	if existing, ok := m.settings[settings.UserID]; ok {
		existing.NotificationEnabled = settings.NotificationEnabled
		existing.MsgPreviewEnabled = settings.MsgPreviewEnabled
		existing.MuteList = settings.MuteList
		existing.UpdatedAt = time.Now()
		settings.ID = existing.ID
	} else {
		settings.ID = m.nextID
		m.nextID++
		settings.CreatedAt = time.Now()
		settings.UpdatedAt = time.Now()
		m.settings[settings.UserID] = settings
	}
	return nil
}

// ── Stub out all other MySQLRepo methods ──

func (m *mockSettingsRepo) InsertPrivateMessage(_ context.Context, _ *model.PrivateMessage) error {
	return nil
}
func (m *mockSettingsRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error {
	return nil
}
func (m *mockSettingsRepo) InsertMsgRevoked(_ context.Context, _ *model.MsgRevoked) error { return nil }
func (m *mockSettingsRepo) GetUserByID(_ context.Context, _ int64) (*model.User, error) {
	return nil, nil
}
func (m *mockSettingsRepo) GetUserByUsername(_ context.Context, _ string) (*model.User, error) {
	return nil, nil
}
func (m *mockSettingsRepo) CreateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockSettingsRepo) UpdateUser(_ context.Context, _ *model.User) error { return nil }
func (m *mockSettingsRepo) DeleteMoment(_ context.Context, _ int64) error     { return nil }

func (m *mockSettingsRepo) CreateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockSettingsRepo) UpdateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockSettingsRepo) GetFriendRequestByID(_ context.Context, _ int64) (*model.FriendRequest, error) {
	return nil, nil
}
func (m *mockSettingsRepo) GetFriendRequestsByUser(_ context.Context, _ int64) ([]model.FriendRequest, error) {
	return nil, nil
}
func (m *mockSettingsRepo) CreateFriendship(_ context.Context, _ *model.Friendship) error { return nil }
func (m *mockSettingsRepo) DeleteFriendship(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockSettingsRepo) GetFriendList(_ context.Context, _ int64) ([]model.Friendship, error) {
	return nil, nil
}
func (m *mockSettingsRepo) IsFriend(_ context.Context, userID, friendID int64) (bool, error) {
	return m.friends[fmt.Sprintf("%d:%d", userID, friendID)], nil
}
func (m *mockSettingsRepo) CreateBlacklist(_ context.Context, _ *model.Blacklist) error { return nil }
func (m *mockSettingsRepo) DeleteBlacklist(_ context.Context, _ int64, _ int64) error   { return nil }
func (m *mockSettingsRepo) IsBlocked(_ context.Context, _ int64, _ int64) (bool, error) {
	return false, nil
}

func (m *mockSettingsRepo) CreateGroup(_ context.Context, _ *model.Group) (int64, error) {
	return 0, nil
}
func (m *mockSettingsRepo) UpdateGroup(_ context.Context, _ *model.Group) error { return nil }
func (m *mockSettingsRepo) GetGroupByID(_ context.Context, _ int64) (*model.Group, error) {
	return nil, nil
}
func (m *mockSettingsRepo) AddGroupMember(_ context.Context, _ *model.GroupMember) error { return nil }
func (m *mockSettingsRepo) RemoveGroupMember(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockSettingsRepo) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	return nil, nil
}
func (m *mockSettingsRepo) UpdateGroupMemberRole(_ context.Context, _ int, _ int, _ int) error {
	return nil
}

func (m *mockSettingsRepo) CreateMoment(_ context.Context, _ *model.Moment) error { return nil }
func (m *mockSettingsRepo) GetMomentByID(_ context.Context, _ int64) (*model.Moment, error) {
	return nil, nil
}
func (m *mockSettingsRepo) GetMomentsByUser(_ context.Context, _ int64, _ int, _ int) ([]model.Moment, error) {
	return nil, nil
}
func (m *mockSettingsRepo) CreateMomentLike(_ context.Context, _ *model.MomentLike) error { return nil }
func (m *mockSettingsRepo) DeleteMomentLike(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockSettingsRepo) CreateMomentComment(_ context.Context, _ *model.MomentComment) error {
	return nil
}
func (m *mockSettingsRepo) GetMomentCommentByID(_ context.Context, _ int64) (*model.MomentComment, error) {
	return nil, nil
}
func (m *mockSettingsRepo) GetMomentComments(_ context.Context, _ int64) ([]model.MomentComment, error) {
	return nil, nil
}
func (m *mockSettingsRepo) DeleteMomentComment(_ context.Context, _ int64) error { return nil }
func (m *mockSettingsRepo) CountFriends(_ context.Context, _ int64) (int, error) { return 0, nil }
func (m *mockSettingsRepo) GetMomentsByIDs(_ context.Context, _ []int64) ([]model.Moment, error) {
	return nil, nil
}

func (m *mockSettingsRepo) SearchPrivateMessages(_ context.Context, _ int64, _ string, _ int, _ int) ([]model.PrivateMessage, error) {
	return nil, nil
}

// ──────────────────────────────────────────────────────
// Helper: new test SettingsService
// ──────────────────────────────────────────────────────

func newTestSettingsService(repo *mockSettingsRepo) *SettingsService {
	logger := zap.NewNop()
	return NewSettingsService(repo, logger)
}

// ──────────────────────────────────────────────────────
// GetSettings tests
// ──────────────────────────────────────────────────────

func TestSettings_GetSettings_Default(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	settings, err := svc.GetSettings(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, settings)
	assert.Equal(t, int64(1), settings.UserID)
	assert.True(t, settings.NotificationEnabled)
	assert.True(t, settings.MsgPreviewEnabled)
	assert.Equal(t, "[]", settings.MuteList)
}

func TestSettings_GetSettings_Existing(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.mu.Lock()
	repo.settings[1] = &model.UserSettings{
		ID:                  1,
		UserID:              1,
		NotificationEnabled: false,
		MsgPreviewEnabled:   true,
		MuteList:            `["g_5","p_1_3"]`,
		CreatedAt:           time.Now(),
		UpdatedAt:           time.Now(),
	}
	repo.mu.Unlock()

	svc := newTestSettingsService(repo)
	settings, err := svc.GetSettings(context.Background(), 1)
	assert.NoError(t, err)
	assert.NotNil(t, settings)
	assert.False(t, settings.NotificationEnabled)
	assert.Equal(t, `["g_5","p_1_3"]`, settings.MuteList)
}

func TestSettings_GetSettings_Error(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.getSettingsErr = fmt.Errorf("db error")
	svc := newTestSettingsService(repo)

	settings, err := svc.GetSettings(context.Background(), 1)
	assert.Error(t, err)
	assert.Nil(t, settings)
}

// ──────────────────────────────────────────────────────
// UpdateSettings tests
// ──────────────────────────────────────────────────────

func TestSettings_UpdateSettings_Create(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	settings := &model.UserSettings{
		NotificationEnabled: true,
		MsgPreviewEnabled:   false,
		MuteList:            "[]",
	}
	err := svc.UpdateSettings(context.Background(), 1, settings)
	assert.NoError(t, err)

	// Verify it was stored
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.NotNil(t, stored)
	assert.True(t, stored.NotificationEnabled)
	assert.False(t, stored.MsgPreviewEnabled)
}

func TestSettings_UpdateSettings_Update(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	// First create
	settings := &model.UserSettings{
		NotificationEnabled: true,
		MsgPreviewEnabled:   true,
		MuteList:            "[]",
	}
	err := svc.UpdateSettings(context.Background(), 1, settings)
	assert.NoError(t, err)

	// Then update
	settings2 := &model.UserSettings{
		NotificationEnabled: false,
		MsgPreviewEnabled:   false,
		MuteList:            `["g_1"]`,
	}
	err = svc.UpdateSettings(context.Background(), 1, settings2)
	assert.NoError(t, err)

	// Verify it was updated
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.NotNil(t, stored)
	assert.False(t, stored.NotificationEnabled)
	assert.False(t, stored.MsgPreviewEnabled)
	assert.Equal(t, `["g_1"]`, stored.MuteList)
}

func TestSettings_UpdateSettings_Error(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.createOrUpdateErr = fmt.Errorf("db error")
	svc := newTestSettingsService(repo)

	err := svc.UpdateSettings(context.Background(), 1, &model.UserSettings{})
	assert.Error(t, err)
}

// ──────────────────────────────────────────────────────
// AddMuteConv tests
// ──────────────────────────────────────────────────────

func TestSettings_AddMuteConv_Success(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	err := svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)

	// Verify mute list contains the convID
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.NotNil(t, stored)
	assert.Contains(t, stored.MuteList, "g_5")
}

func TestSettings_AddMuteConv_Duplicate(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	// Add first
	err := svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)

	// Add same again
	err = svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.Error(t, err)
	assert.Equal(t, ErrMuteConvExists, err.Error())
}

func TestSettings_AddMuteConv_MultipleConvs(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.friends["1:3"] = true
	svc := newTestSettingsService(repo)

	err := svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)
	err = svc.AddMuteConv(context.Background(), 1, "p_1_3")
	assert.NoError(t, err)

	// Verify both are in the mute list
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.Contains(t, stored.MuteList, "g_5")
	assert.Contains(t, stored.MuteList, "p_1_3")
}

func TestSettings_AddMuteConv_RejectsDeletedPrivateConversation(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	assert.EqualError(t, svc.AddMuteConv(context.Background(), 1, "p_1_3"), ErrInvalidMuteConv)
	repo.friends["1:3"] = true
	assert.NoError(t, svc.AddMuteConv(context.Background(), 1, "p_1_3"))
}

// ──────────────────────────────────────────────────────
// RemoveMuteConv tests
// ──────────────────────────────────────────────────────

func TestSettings_RemoveMuteConv_Success(t *testing.T) {
	repo := newMockSettingsRepo()
	repo.friends["1:3"] = true
	svc := newTestSettingsService(repo)

	// Add first
	err := svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)
	err = svc.AddMuteConv(context.Background(), 1, "p_1_3")
	assert.NoError(t, err)

	// Remove one
	err = svc.RemoveMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)

	// Verify only the remaining one is in the mute list
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.NotContains(t, stored.MuteList, "g_5")
	assert.Contains(t, stored.MuteList, "p_1_3")
}

func TestSettings_RemoveMuteConv_NotFound(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	err := svc.RemoveMuteConv(context.Background(), 1, "g_5")
	assert.Error(t, err)
	assert.Equal(t, ErrMuteConvNotFound, err.Error())
}

func TestSettings_RemoveMuteConv_RemoveLast(t *testing.T) {
	repo := newMockSettingsRepo()
	svc := newTestSettingsService(repo)

	// Add one conv
	err := svc.AddMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)

	// Remove it
	err = svc.RemoveMuteConv(context.Background(), 1, "g_5")
	assert.NoError(t, err)

	// Verify mute list is empty
	repo.mu.Lock()
	stored := repo.settings[1]
	repo.mu.Unlock()
	assert.Equal(t, "[]", stored.MuteList)
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockSettingsRepo) GetMomentLikers(_ context.Context, _ int64) ([]int64, error) {
	return nil, nil
}
func (m *mockSettingsRepo) BatchUpsertMomentLikes(_ context.Context, _ []model.MomentLike) error {
	return nil
}
func (m *mockSettingsRepo) BatchDeleteMomentLikes(_ context.Context, _ []model.MomentLikeKey) error {
	return nil
}
