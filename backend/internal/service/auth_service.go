package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"

	"github.com/goim/goim/internal/middleware"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// ── 验证 / 业务错误常量 ──

const (
	ErrUsernameTooShort = "用户名必须为3-50个字符"
	ErrPasswordTooShort = "密码必须至少为6个字符"
	ErrUsernameTaken    = "用户名已被占用"
	ErrUserNotFound     = "用户未找到"
	ErrWrongPassword    = "密码错误"
	ErrInvalidToken     = "刷新令牌无效或已过期"
)

// AuthService 处理用户注册、登录和令牌刷新。
type AuthService struct {
	repo           repository.MySQLRepo
	jwtSecret      string
	bcryptCost     int
	accessExpHours int
	refreshExpDays int
}

// NewAuthService 使用给定的 MySQL 仓库和 JWT 配置创建 AuthService。
func NewAuthService(repo repository.MySQLRepo, jwtSecret string, accessExpHours, refreshExpDays int) *AuthService {
	return &AuthService{
		repo:           repo,
		jwtSecret:      jwtSecret,
		bcryptCost:     10,
		accessExpHours: accessExpHours,
		refreshExpDays: refreshExpDays,
	}
}

// Register 验证输入，对密码进行哈希处理，并创建新用户。
// 成功时返回新用户的 ID 和用户名。
func (s *AuthService) Register(ctx context.Context, username, password string) (int64, string, error) {
	// 验证用户名长度
	if len(username) < 3 || len(username) > 50 {
		return 0, "", fmt.Errorf(ErrUsernameTooShort)
	}
	// 验证密码长度
	if len(password) < 6 {
		return 0, "", fmt.Errorf(ErrPasswordTooShort)
	}

	// 检查用户名唯一性
	existing, err := s.repo.GetUserByUsername(ctx, username)
	if err != nil {
		return 0, "", fmt.Errorf("检查用户名: %w", err)
	}
	if existing != nil {
		return 0, "", fmt.Errorf(ErrUsernameTaken)
	}

	// 哈希密码
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return 0, "", fmt.Errorf("哈希密码: %w", err)
	}

	// 创建用户记录
	user := &model.User{
		Username:     username,
		PasswordHash: string(hash),
		Nickname:     username, // 默认昵称 = 用户名
	}
	if err := s.repo.CreateUser(ctx, user); err != nil {
		return 0, "", fmt.Errorf("创建用户: %w", err)
	}

	return user.ID, user.Username, nil
}

// Login 验证凭据并返回 JWT 令牌。
// 返回 accessToken、refreshToken、expiresIn（秒）、error。
func (s *AuthService) Login(ctx context.Context, username, password string) (string, string, int64, error) {
	user, err := s.repo.GetUserByUsername(ctx, username)
	if err != nil {
		return "", "", 0, fmt.Errorf("查找用户: %w", err)
	}
	if user == nil {
		return "", "", 0, fmt.Errorf(ErrUserNotFound)
	}

	// 验证密码
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return "", "", 0, fmt.Errorf(ErrWrongPassword)
	}

	return s.issueTokens(user)
}

func (s *AuthService) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	return s.repo.GetUserByUsername(ctx, username)
}

// GetUserByID 返回指定用户资料，供受保护的账户接口使用。
func (s *AuthService) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
	return s.repo.GetUserByID(ctx, userID)
}

// UpdateUsername 修改当前用户的用户名，并签发包含新用户名的令牌。
// users.username 的唯一索引是并发修改时的最终保障。
func (s *AuthService) UpdateUsername(ctx context.Context, userID int64, username string) (string, string, int64, error) {
	if len(username) < 3 || len(username) > 50 {
		return "", "", 0, fmt.Errorf(ErrUsernameTooShort)
	}

	user, err := s.repo.GetUserByID(ctx, userID)
	if err != nil {
		return "", "", 0, fmt.Errorf("查找用户: %w", err)
	}
	if user == nil {
		return "", "", 0, fmt.Errorf(ErrUserNotFound)
	}

	existing, err := s.repo.GetUserByUsername(ctx, username)
	if err != nil {
		return "", "", 0, fmt.Errorf("检查用户名: %w", err)
	}
	if existing != nil && existing.ID != userID {
		return "", "", 0, fmt.Errorf(ErrUsernameTaken)
	}

	oldUsername := user.Username
	user.Username = username
	if user.Nickname == oldUsername {
		user.Nickname = username
	}
	if err := s.repo.UpdateUser(ctx, user); err != nil {
		var mysqlErr *mysql.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return "", "", 0, fmt.Errorf(ErrUsernameTaken)
		}
		return "", "", 0, fmt.Errorf("更新用户名: %w", err)
	}

	return s.issueTokens(user)
}

// UpdatePassword 校验当前密码后，以 bcrypt 哈希替换为新密码。
func (s *AuthService) UpdatePassword(ctx context.Context, userID int64, currentPassword, newPassword string) error {
	if len(newPassword) < 6 {
		return fmt.Errorf(ErrPasswordTooShort)
	}

	user, err := s.repo.GetUserByID(ctx, userID)
	if err != nil {
		return fmt.Errorf("查找用户: %w", err)
	}
	if user == nil {
		return fmt.Errorf(ErrUserNotFound)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(currentPassword)); err != nil {
		return fmt.Errorf(ErrWrongPassword)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), s.bcryptCost)
	if err != nil {
		return fmt.Errorf("哈希密码: %w", err)
	}
	user.PasswordHash = string(hash)
	if err := s.repo.UpdateUser(ctx, user); err != nil {
		return fmt.Errorf("更新密码: %w", err)
	}
	return nil
}

// Refresh 验证刷新令牌并颁发新的访问令牌。
// 返回 accessToken、expiresIn（秒）、error。
func (s *AuthService) Refresh(ctx context.Context, refreshToken string) (string, int64, error) {
	_, claims, err := middleware.ParseToken(refreshToken, s.jwtSecret)
	if err != nil {
		return "", 0, fmt.Errorf(ErrInvalidToken)
	}

	// 验证用户是否仍然存在
	user, err := s.repo.GetUserByID(ctx, claims.UserID)
	if err != nil {
		return "", 0, fmt.Errorf("查找用户: %w", err)
	}
	if user == nil {
		return "", 0, fmt.Errorf(ErrUserNotFound)
	}

	accessToken, _, expiresIn, err := s.issueTokens(user)
	return accessToken, expiresIn, err
}

func (s *AuthService) issueTokens(user *model.User) (string, string, int64, error) {
	accessToken, err := middleware.GenerateAccessToken(user.ID, user.Username, s.jwtSecret, s.accessExpHours)
	if err != nil {
		return "", "", 0, fmt.Errorf("生成访问令牌: %w", err)
	}
	refreshToken, err := middleware.GenerateRefreshToken(user.ID, s.jwtSecret, s.refreshExpDays)
	if err != nil {
		return "", "", 0, fmt.Errorf("生成刷新令牌: %w", err)
	}
	return accessToken, refreshToken, int64(s.accessExpHours * 3600), nil
}

// TokenExpiry 返回配置的访问令牌过期时长。
func (s *AuthService) TokenExpiry() time.Duration {
	return time.Duration(s.accessExpHours) * time.Hour
}
