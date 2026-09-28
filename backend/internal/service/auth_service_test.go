package service

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/crypto/bcrypt"

	"github.com/goim/goim/internal/middleware"
	"github.com/goim/goim/internal/model"
)

// ──────────────────────────────────────────────────────
// 用于认证测试的模拟 MySQLRepo
// ──────────────────────────────────────────────────────

type mockAuthRepo struct {
	mu sync.Mutex

	// 按用户名存储的用户
	usersByUsername map[string]*model.User
	// 按 ID 存储的用户
	usersByID map[int64]*model.User
	// 下一个自增 ID
	nextID int64

	// 错误覆盖（在调用服务方法之前设置）
	getByUsernameErr error
	getByIDErr       error
	createErr        error
}

func newMockAuthRepo() *mockAuthRepo {
	return &mockAuthRepo{
		usersByUsername: make(map[string]*model.User),
		usersByID:       make(map[int64]*model.User),
		nextID:          1,
	}
}

func (m *mockAuthRepo) GetUserByUsername(_ context.Context, username string) (*model.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getByUsernameErr != nil {
		return nil, m.getByUsernameErr
	}
	u, ok := m.usersByUsername[username]
	if !ok {
		return nil, nil // 未找到
	}
	return u, nil
}

func (m *mockAuthRepo) GetUserByID(_ context.Context, userID int64) (*model.User, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getByIDErr != nil {
		return nil, m.getByIDErr
	}
	u, ok := m.usersByID[userID]
	if !ok {
		return nil, nil // 未找到
	}
	return u, nil
}

func (m *mockAuthRepo) CreateUser(_ context.Context, user *model.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	user.ID = m.nextID
	m.nextID++
	user.CreatedAt = time.Now()
	user.UpdatedAt = time.Now()
	m.usersByUsername[user.Username] = user
	m.usersByID[user.ID] = user
	return nil
}

// 桩代码：实现其他所有 MySQLRepo 方法（认证测试中不使用）

func (m *mockAuthRepo) UpdateUser(_ context.Context, user *model.User) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for username, stored := range m.usersByUsername {
		if stored.ID == user.ID && username != user.Username {
			delete(m.usersByUsername, username)
		}
	}
	m.usersByUsername[user.Username] = user
	m.usersByID[user.ID] = user
	return nil
}
func (m *mockAuthRepo) DeleteMoment(_ context.Context, _ int64) error { return nil }

func (m *mockAuthRepo) InsertPrivateMessage(_ context.Context, _ *model.PrivateMessage) error {
	return nil
}
func (m *mockAuthRepo) InsertGroupMessage(_ context.Context, _ *model.GroupMessage) error { return nil }
func (m *mockAuthRepo) InsertMsgRevoked(_ context.Context, _ *model.MsgRevoked) error     { return nil }

func (m *mockAuthRepo) CreateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockAuthRepo) UpdateFriendRequest(_ context.Context, _ *model.FriendRequest) error {
	return nil
}
func (m *mockAuthRepo) GetFriendRequestByID(_ context.Context, _ int64) (*model.FriendRequest, error) {
	return nil, nil
}
func (m *mockAuthRepo) GetFriendRequestsByUser(_ context.Context, _ int64) ([]model.FriendRequest, error) {
	return nil, nil
}
func (m *mockAuthRepo) CreateFriendship(_ context.Context, _ *model.Friendship) error { return nil }
func (m *mockAuthRepo) DeleteFriendship(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockAuthRepo) GetFriendList(_ context.Context, _ int64) ([]model.Friendship, error) {
	return nil, nil
}
func (m *mockAuthRepo) IsFriend(_ context.Context, _ int64, _ int64) (bool, error)  { return false, nil }
func (m *mockAuthRepo) CreateBlacklist(_ context.Context, _ *model.Blacklist) error { return nil }
func (m *mockAuthRepo) DeleteBlacklist(_ context.Context, _ int64, _ int64) error   { return nil }
func (m *mockAuthRepo) IsBlocked(_ context.Context, _ int64, _ int64) (bool, error) {
	return false, nil
}

func (m *mockAuthRepo) CreateGroup(_ context.Context, _ *model.Group) (int64, error) { return 0, nil }
func (m *mockAuthRepo) UpdateGroup(_ context.Context, _ *model.Group) error          { return nil }
func (m *mockAuthRepo) GetGroupByID(_ context.Context, _ int64) (*model.Group, error) {
	return nil, nil
}
func (m *mockAuthRepo) AddGroupMember(_ context.Context, _ *model.GroupMember) error { return nil }
func (m *mockAuthRepo) RemoveGroupMember(_ context.Context, _ int64, _ int64) error  { return nil }
func (m *mockAuthRepo) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	return nil, nil
}
func (m *mockAuthRepo) UpdateGroupMemberRole(_ context.Context, _ int, _ int, _ int) error {
	return nil
}

func (m *mockAuthRepo) CreateMoment(_ context.Context, _ *model.Moment) error { return nil }
func (m *mockAuthRepo) GetMomentByID(_ context.Context, _ int64) (*model.Moment, error) {
	return nil, nil
}
func (m *mockAuthRepo) GetMomentsByUser(_ context.Context, _ int64, _ int, _ int) ([]model.Moment, error) {
	return nil, nil
}
func (m *mockAuthRepo) CreateMomentLike(_ context.Context, _ *model.MomentLike) error { return nil }
func (m *mockAuthRepo) DeleteMomentLike(_ context.Context, _ int64, _ int64) error    { return nil }
func (m *mockAuthRepo) CreateMomentComment(_ context.Context, _ *model.MomentComment) error {
	return nil
}
func (m *mockAuthRepo) GetMomentCommentByID(_ context.Context, _ int64) (*model.MomentComment, error) {
	return nil, nil
}
func (m *mockAuthRepo) GetMomentComments(_ context.Context, _ int64) ([]model.MomentComment, error) {
	return nil, nil
}
func (m *mockAuthRepo) DeleteMomentComment(_ context.Context, _ int64) error { return nil }
func (m *mockAuthRepo) CountFriends(_ context.Context, _ int64) (int, error) { return 0, nil }
func (m *mockAuthRepo) GetMomentsByIDs(_ context.Context, _ []int64) ([]model.Moment, error) {
	return nil, nil
}

func (m *mockAuthRepo) GetUserSettings(_ context.Context, _ int64) (*model.UserSettings, error) {
	return nil, nil
}
func (m *mockAuthRepo) CreateOrUpdateUserSettings(_ context.Context, _ *model.UserSettings) error {
	return nil
}
func (m *mockAuthRepo) SearchPrivateMessages(_ context.Context, _ int64, _ string, _ int, _ int) ([]model.PrivateMessage, error) {
	return nil, nil
}

// ──────────────────────────────────────────────────────
// 辅助函数：创建新的测试 AuthService
// ──────────────────────────────────────────────────────

func newTestAuthService(repo *mockAuthRepo) *AuthService {
	return NewAuthService(repo, "test-secret-key", 2, 7)
}

// ──────────────────────────────────────────────────────
// 注册测试
// ──────────────────────────────────────────────────────

func TestAuth_Register_Success(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	userID, username, err := svc.Register(context.Background(), "alice", "password123")
	assert.NoError(t, err)
	assert.Equal(t, "alice", username)
	assert.Equal(t, int64(1), userID)

	// 验证存储的用户
	stored, ok := repo.usersByUsername["alice"]
	assert.True(t, ok)
	assert.NotEmpty(t, stored.PasswordHash)

	// 验证 bcrypt 哈希是否有效
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(stored.PasswordHash), []byte("password123")))
}

func TestAuth_Register_DuplicateUsername(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, err := svc.Register(context.Background(), "bob", "password123")
	assert.NoError(t, err)

	_, _, err = svc.Register(context.Background(), "bob", "different123")
	assert.Error(t, err)
	assert.Equal(t, ErrUsernameTaken, err.Error())
}

func TestAuth_Register_UsernameTooShort(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, err := svc.Register(context.Background(), "ab", "password123")
	assert.Error(t, err)
	assert.Equal(t, ErrUsernameTooShort, err.Error())
}

func TestAuth_Register_UsernameTooLong(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	longName := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" // 51 个字符
	_, _, err := svc.Register(context.Background(), longName, "password123")
	assert.Error(t, err)
	assert.Equal(t, ErrUsernameTooShort, err.Error())
}

func TestAuth_Register_PasswordTooShort(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, err := svc.Register(context.Background(), "charlie", "12345")
	assert.Error(t, err)
	assert.Equal(t, ErrPasswordTooShort, err.Error())
}

func TestAuth_Register_RepoCreateError(t *testing.T) {
	repo := newMockAuthRepo()
	repo.createErr = fmt.Errorf("db error")
	svc := newTestAuthService(repo)

	_, _, err := svc.Register(context.Background(), "dave", "password123")
	assert.Error(t, err)
	// 应包含底层错误信息
	assert.Contains(t, err.Error(), "创建用户")
}

// ──────────────────────────────────────────────────────
// 账户资料修改测试
// ──────────────────────────────────────────────────────

func TestAuth_UpdateUsername_Success(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)
	userID, _, err := svc.Register(context.Background(), "alice", "password123")
	assert.NoError(t, err)

	accessToken, refreshToken, expiresIn, err := svc.UpdateUsername(context.Background(), userID, "alice_new")
	assert.NoError(t, err)
	assert.NotEmpty(t, refreshToken)
	assert.Equal(t, int64(2*3600), expiresIn)
	_, claims, err := middleware.ParseToken(accessToken, "test-secret-key")
	assert.NoError(t, err)
	assert.Equal(t, "alice_new", claims.Username)
	assert.Nil(t, repo.usersByUsername["alice"])
	assert.Equal(t, userID, repo.usersByUsername["alice_new"].ID)
}

func TestAuth_UpdateUsername_RejectsTakenOrInvalidName(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)
	aliceID, _, err := svc.Register(context.Background(), "alice", "password123")
	assert.NoError(t, err)
	_, _, err = svc.Register(context.Background(), "bob", "password123")
	assert.NoError(t, err)

	_, _, _, err = svc.UpdateUsername(context.Background(), aliceID, "bob")
	assert.Equal(t, ErrUsernameTaken, err.Error())
	_, _, _, err = svc.UpdateUsername(context.Background(), aliceID, "ab")
	assert.Equal(t, ErrUsernameTooShort, err.Error())
}

func TestAuth_UpdatePassword_SuccessAndCurrentPasswordValidation(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)
	userID, _, err := svc.Register(context.Background(), "password_user", "oldpassword")
	assert.NoError(t, err)

	err = svc.UpdatePassword(context.Background(), userID, "wrong-password", "newpassword")
	assert.Equal(t, ErrWrongPassword, err.Error())
	err = svc.UpdatePassword(context.Background(), userID, "oldpassword", "short")
	assert.Equal(t, ErrPasswordTooShort, err.Error())
	err = svc.UpdatePassword(context.Background(), userID, "oldpassword", "newpassword")
	assert.NoError(t, err)
	_, _, _, err = svc.Login(context.Background(), "password_user", "oldpassword")
	assert.Equal(t, ErrWrongPassword, err.Error())
	_, _, _, err = svc.Login(context.Background(), "password_user", "newpassword")
	assert.NoError(t, err)
}

// ──────────────────────────────────────────────────────
// 登录测试
// ──────────────────────────────────────────────────────

func TestAuth_Login_Success(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	// 先注册一个用户
	userID, _, err := svc.Register(context.Background(), "loginuser", "mypassword")
	assert.NoError(t, err)

	// 现在登录
	accessToken, refreshToken, expiresIn, err := svc.Login(context.Background(), "loginuser", "mypassword")
	assert.NoError(t, err)
	assert.NotEmpty(t, accessToken)
	assert.NotEmpty(t, refreshToken)
	assert.Equal(t, int64(2*3600), expiresIn)

	// 验证访问令牌包含正确的 userID
	_, claims, err := middleware.ParseToken(accessToken, "test-secret-key")
	assert.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
	assert.Equal(t, "loginuser", claims.Username)

	// 验证刷新令牌包含正确的 userID
	_, claims, err = middleware.ParseToken(refreshToken, "test-secret-key")
	assert.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
}

func TestAuth_Login_WrongPassword(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, err := svc.Register(context.Background(), "loginuser", "mypassword")
	assert.NoError(t, err)

	_, _, _, err = svc.Login(context.Background(), "loginuser", "wrongpassword")
	assert.Error(t, err)
	assert.Equal(t, ErrWrongPassword, err.Error())
}

func TestAuth_Login_NonexistentUser(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, _, err := svc.Login(context.Background(), "ghost", "password123")
	assert.Error(t, err)
	assert.Equal(t, ErrUserNotFound, err.Error())
}

// ──────────────────────────────────────────────────────
// 刷新令牌测试
// ──────────────────────────────────────────────────────

func TestAuth_Refresh_Success(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	// 注册并登录以获取刷新令牌
	userID, _, err := svc.Register(context.Background(), "refreshuser", "password123")
	assert.NoError(t, err)
	_, refreshToken, _, err := svc.Login(context.Background(), "refreshuser", "password123")
	assert.NoError(t, err)

	// 刷新令牌
	accessToken, expiresIn, err := svc.Refresh(context.Background(), refreshToken)
	assert.NoError(t, err)
	assert.NotEmpty(t, accessToken)
	assert.Equal(t, int64(2*3600), expiresIn)

	// 验证新的访问令牌
	_, claims, err := middleware.ParseToken(accessToken, "test-secret-key")
	assert.NoError(t, err)
	assert.Equal(t, userID, claims.UserID)
	assert.Equal(t, "refreshuser", claims.Username)
}

func TestAuth_Refresh_InvalidToken(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	_, _, err := svc.Refresh(context.Background(), "garbage-token-string")
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidToken, err.Error())
}

func TestAuth_Refresh_UserDeleted(t *testing.T) {
	repo := newMockAuthRepo()
	svc := newTestAuthService(repo)

	// 注册并登录
	_, _, err := svc.Register(context.Background(), "deleteduser", "password123")
	assert.NoError(t, err)
	_, refreshToken, _, err := svc.Login(context.Background(), "deleteduser", "password123")
	assert.NoError(t, err)

	// 模拟用户被删除 —— 从 mock 仓库中移除
	repo.mu.Lock()
	delete(repo.usersByID, 1)
	delete(repo.usersByUsername, "deleteduser")
	repo.mu.Unlock()

	// 刷新应失败
	_, _, err = svc.Refresh(context.Background(), refreshToken)
	assert.Error(t, err)
	assert.Equal(t, ErrUserNotFound, err.Error())
}

func TestAuth_Refresh_ExpiredRefreshToken(t *testing.T) {
	repo := newMockAuthRepo()
	// 使用极短的刷新令牌有效期（0 天 = 已过期）
	svc := NewAuthService(repo, "test-secret-key", 2, 0)

	_, _, err := svc.Register(context.Background(), "expireduser", "password123")
	assert.NoError(t, err)

	// 生成一个已过期的刷新令牌（0 天）
	_, refreshToken, _, err := svc.Login(context.Background(), "expireduser", "password123")
	assert.NoError(t, err)

	// 刷新令牌应该已过期；解析应失败
	_, _, err = middleware.ParseToken(refreshToken, "test-secret-key")
	assert.Error(t, err) // 令牌应已过期
}

// ── 高并发点赞新增接口的 mock 桩 ──

func (m *mockAuthRepo) GetMomentLikers(_ context.Context, _ int64) ([]int64, error) { return nil, nil }
func (m *mockAuthRepo) BatchUpsertMomentLikes(_ context.Context, _ []model.MomentLike) error {
	return nil
}
func (m *mockAuthRepo) BatchDeleteMomentLikes(_ context.Context, _ []model.MomentLikeKey) error {
	return nil
}
