package service

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

const friendMomentBackfillLimit = 1000

// ── 好友服务错误常量 ──

const (
	ErrSelfRequest      = "不能给自己发送好友请求"
	ErrAlreadyFriends   = "已经是该用户的好友"
	ErrFriendBlocked    = "你已拉黑该用户或已被该用户拉黑"
	ErrDuplicateRequest = "已存在待处理的好友请求"
	ErrRequestNotFound  = "好友请求未找到"
	ErrNotRequestTarget = "你不是该好友请求的接收者"
	ErrAlreadyBlocked   = "你已经拉黑了该用户"
)

// FriendService 处理好友相关的业务逻辑：发送/接受/拒绝好友请求、管理好友关系以及拉黑/取消拉黑用户。
type FriendService struct {
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	logger    *zap.Logger

	// store 用于在成为好友时创建私聊会话；relations 用于关系变更后失效缓存。
	// 二者通过 WithMessaging 注入，为空时跳过对应步骤（便于单元测试）。
	store     repository.MessageStore
	relations repository.RelationCache
}

// NewFriendService 创建一个包含所有必要依赖的 FriendService。
func NewFriendService(mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, logger *zap.Logger) *FriendService {
	return &FriendService{
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		logger:    logger,
	}
}

// WithMessaging 注入会话存储与关系缓存。
func (s *FriendService) WithMessaging(store repository.MessageStore, relations repository.RelationCache) *FriendService {
	s.store = store
	s.relations = relations
	return s
}

// invalidateFriend 在好友关系变更后失效缓存；失败只记日志，缓存带 TTL 会自然过期。
func (s *FriendService) invalidateFriend(ctx context.Context, a, b int64) {
	if s.relations == nil {
		return
	}
	if err := s.relations.InvalidateFriend(ctx, a, b); err != nil {
		s.logger.Warn("失效好友缓存失败", zap.Int64("a", a), zap.Int64("b", b), zap.Error(err))
	}
}

// invalidateBlock 在黑名单变更后失效缓存。
func (s *FriendService) invalidateBlock(ctx context.Context, userID, blockedID int64) {
	if s.relations == nil {
		return
	}
	if err := s.relations.InvalidateBlock(ctx, userID, blockedID); err != nil {
		s.logger.Warn("失效黑名单缓存失败", zap.Int64("userID", userID), zap.Int64("blockedID", blockedID), zap.Error(err))
	}
}

func (s *FriendService) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
	return s.mysqlRepo.GetUserByID(ctx, userID)
}

// SendFriendRequest 创建好友请求，并进行以下验证：
// - 不能给自己发请求
// - 不能已经是好友
// - 未被拉黑（任一方向）
// - 不存在重复的待处理请求
func (s *FriendService) SendFriendRequest(ctx context.Context, fromUserID, toUserID int64, message string) (*model.FriendRequest, error) {
	// 1. 不能给自己发请求
	if fromUserID == toUserID {
		return nil, fmt.Errorf(ErrSelfRequest)
	}
	target, err := s.mysqlRepo.GetUserByID(ctx, toUserID)
	if err != nil {
		return nil, fmt.Errorf("检查目标用户: %w", err)
	}
	if target == nil {
		return nil, fmt.Errorf(ErrUserNotFound)
	}

	// 2. 不能已经是好友
	isFriend, err := s.mysqlRepo.IsFriend(ctx, fromUserID, toUserID)
	if err != nil {
		return nil, fmt.Errorf("检查好友关系: %w", err)
	}
	if isFriend {
		return nil, fmt.Errorf(ErrAlreadyFriends)
	}

	// 3. 未被拉黑（检查双向）
	blockedBySender, err := s.mysqlRepo.IsBlocked(ctx, fromUserID, toUserID)
	if err != nil {
		return nil, fmt.Errorf("检查发送者是否被拉黑: %w", err)
	}
	if blockedBySender {
		return nil, fmt.Errorf(ErrFriendBlocked)
	}
	blockedByTarget, err := s.mysqlRepo.IsBlocked(ctx, toUserID, fromUserID)
	if err != nil {
		return nil, fmt.Errorf("检查接收者是否被拉黑: %w", err)
	}
	if blockedByTarget {
		return nil, fmt.Errorf(ErrFriendBlocked)
	}

	// 4. 不存在重复的待处理请求
	existingRequests, err := s.mysqlRepo.GetFriendRequestsByUser(ctx, fromUserID)
	if err != nil {
		return nil, fmt.Errorf("检查已有请求: %w", err)
	}
	for _, req := range existingRequests {
		if req.Status == 0 && // 待处理
			((req.FromUserID == fromUserID && req.ToUserID == toUserID) ||
				(req.FromUserID == toUserID && req.ToUserID == fromUserID)) {
			return nil, fmt.Errorf(ErrDuplicateRequest)
		}
	}

	// 5. 创建好友请求；同方向的历史已处理记录由仓储层原子重置为待处理。
	req := &model.FriendRequest{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		Message:    message,
		Status:     0, // 待处理
	}
	if err := s.mysqlRepo.CreateFriendRequest(ctx, req); err != nil {
		return nil, fmt.Errorf("创建好友请求: %w", err)
	}

	s.logger.Debug("好友请求已发送",
		zap.Int64("fromUserID", fromUserID),
		zap.Int64("toUserID", toUserID),
		zap.Int64("requestID", req.ID),
	)

	return req, nil
}

// AcceptFriendRequest 接受好友请求。验证：
// - 请求存在
// - 调用者是请求的接收者（toUserID）
// 然后更新请求状态为已接受（1）并创建双向好友关系。
func (s *FriendService) AcceptFriendRequest(ctx context.Context, userID, requestID int64) (*model.Friendship, error) {
	// 1. 获取请求
	req, err := s.mysqlRepo.GetFriendRequestByID(ctx, requestID)
	if err != nil {
		return nil, fmt.Errorf("获取好友请求: %w", err)
	}
	if req == nil {
		return nil, fmt.Errorf(ErrRequestNotFound)
	}

	// 2. 验证调用者是请求的接收者
	if req.ToUserID != userID {
		return nil, fmt.Errorf(ErrNotRequestTarget)
	}

	// 3. 更新请求状态为已接受
	req.Status = 1
	if err := s.mysqlRepo.UpdateFriendRequest(ctx, req); err != nil {
		return nil, fmt.Errorf("接受好友请求: %w", err)
	}

	// 4. 创建双向好友关系
	fs := &model.Friendship{
		UserID:   req.FromUserID,
		FriendID: req.ToUserID,
	}
	if err := s.mysqlRepo.CreateFriendship(ctx, fs); err != nil {
		return nil, fmt.Errorf("创建好友关系: %w", err)
	}

	// 5. 先写 DB 再失效缓存（Cache-Aside），之前缓存的"非好友"否定结果需要清掉
	s.invalidateFriend(ctx, req.FromUserID, req.ToUserID)

	// 成为好友后立即创建私聊会话，双方无需等第一条消息就能在会话列表中看到对方。
	s.createPrivateConversation(ctx, req.FromUserID, req.ToUserID)

	// 动态流在发布时只分发给当时已是好友的用户。建立好友关系后，
	// 回填双方最近可见的历史动态，避免新好友只能看到之后发布的内容。
	// 回填失败不应回滚已成功创建的好友关系，后续新动态仍可正常分发。
	for _, pair := range [][2]int64{{req.FromUserID, req.ToUserID}, {req.ToUserID, req.FromUserID}} {
		if err := s.backfillFriendMoments(ctx, pair[0], pair[1]); err != nil {
			s.logger.Warn("回填新好友的历史朋友圈失败",
				zap.Int64("authorID", pair[0]),
				zap.Int64("recipientID", pair[1]),
				zap.Error(err),
			)
		}
	}

	s.logger.Debug("好友请求已接受",
		zap.Int64("userID", userID),
		zap.Int64("requestID", requestID),
	)

	return fs, nil
}

// createPrivateConversation 创建私聊会话及双方的 user_conversations 记录（均幂等）。
// 失败只记日志：首条消息落库时 persist 消费者会自动补建双方记录。
func (s *FriendService) createPrivateConversation(ctx context.Context, userA, userB int64) {
	if s.store == nil {
		return
	}
	convID := model.BuildConvID(model.ConvTypePrivate, userA, userB)
	if err := s.store.EnsureConversation(ctx, convID, model.ConvTypePrivate); err != nil {
		s.logger.Warn("创建私聊会话失败", zap.String("convId", convID), zap.Error(err))
		return
	}
	if err := s.store.EnsureUserConversations(ctx, convID, []int64{userA, userB}); err != nil {
		s.logger.Warn("创建私聊用户会话失败", zap.String("convId", convID), zap.Error(err))
	}
}

// backfillFriendMoments writes the author's recent friend-visible moments to
// one newly-added friend's timeline. Private moments are deliberately omitted.
func (s *FriendService) backfillFriendMoments(ctx context.Context, authorID, recipientID int64) error {
	moments, err := s.mysqlRepo.GetMomentsByUser(ctx, authorID, friendMomentBackfillLimit, 0)
	if err != nil {
		return fmt.Errorf("读取历史朋友圈: %w", err)
	}
	for _, moment := range moments {
		if moment.Visibility == 3 {
			continue
		}
		if err := s.redisRepo.FanoutMomentFeed(ctx, []int64{recipientID}, moment.ID, moment.CreatedAt.UnixMilli(), friendMomentBackfillLimit); err != nil {
			return fmt.Errorf("写入朋友圈时间线: %w", err)
		}
	}
	return nil
}

// RejectFriendRequest 拒绝好友请求。验证：
// - 请求存在
// - 调用者是请求的接收者（toUserID）
// 然后更新请求状态为已拒绝（2）。
func (s *FriendService) RejectFriendRequest(ctx context.Context, userID, requestID int64) error {
	// 1. 获取请求
	req, err := s.mysqlRepo.GetFriendRequestByID(ctx, requestID)
	if err != nil {
		return fmt.Errorf("获取好友请求: %w", err)
	}
	if req == nil {
		return fmt.Errorf(ErrRequestNotFound)
	}

	// 2. 验证调用者是请求的接收者
	if req.ToUserID != userID {
		return fmt.Errorf(ErrNotRequestTarget)
	}

	// 3. 更新请求状态为已拒绝
	req.Status = 2
	if err := s.mysqlRepo.UpdateFriendRequest(ctx, req); err != nil {
		return fmt.Errorf("拒绝好友请求: %w", err)
	}

	s.logger.Debug("好友请求已拒绝",
		zap.Int64("userID", userID),
		zap.Int64("requestID", requestID),
	)

	return nil
}

// GetFriendRequests 返回涉及指定用户的待处理好友请求。
func (s *FriendService) GetFriendRequests(ctx context.Context, userID int64) ([]model.FriendRequest, error) {
	requests, err := s.mysqlRepo.GetFriendRequestsByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("获取好友请求: %w", err)
	}
	return requests, nil
}

// FriendRequestListItem adds the applicant's public profile to a friend request.
// The embedded request keeps the existing response fields flat in JSON.
type FriendRequestListItem struct {
	model.FriendRequest
	Username  string `json:"username"`
	AvatarURL string `json:"avatar_url"`
}

// GetFriendRequestList returns pending incoming friend requests with applicant
// usernames and avatar URLs for display in the client.
func (s *FriendService) GetFriendRequestList(ctx context.Context, userID int64) ([]FriendRequestListItem, error) {
	requests, err := s.GetFriendRequests(ctx, userID)
	if err != nil {
		return nil, err
	}

	items := make([]FriendRequestListItem, 0, len(requests))
	for _, request := range requests {
		user, err := s.mysqlRepo.GetUserByID(ctx, request.FromUserID)
		if err != nil {
			return nil, fmt.Errorf("获取申请人资料: %w", err)
		}
		if user == nil {
			return nil, fmt.Errorf("获取申请人资料: 用户 %d 不存在", request.FromUserID)
		}
		items = append(items, FriendRequestListItem{
			FriendRequest: request,
			Username:      user.Username,
			AvatarURL:     user.AvatarURL,
		})
	}
	return items, nil
}

// FriendListItem 使用用户个人资料数据丰富 Friendship 信息。
type FriendListItem struct {
	model.Friendship
	Nickname  string `json:"nickname"`
	AvatarURL string `json:"avatar_url"`
	IsBlocked bool   `json:"is_blocked"`
}

// GetFriendList 返回用户的好友列表，包含昵称和头像。
func (s *FriendService) GetFriendList(ctx context.Context, userID int64) ([]FriendListItem, error) {
	friendships, err := s.mysqlRepo.GetFriendList(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("获取好友列表: %w", err)
	}

	items := make([]FriendListItem, 0, len(friendships))
	for _, fs := range friendships {
		isBlocked, err := s.mysqlRepo.IsBlocked(ctx, userID, fs.FriendID)
		if err != nil {
			return nil, fmt.Errorf("获取好友黑名单状态: %w", err)
		}
		user, err := s.mysqlRepo.GetUserByID(ctx, fs.FriendID)
		if err != nil {
			s.logger.Warn("获取好友个人资料失败",
				zap.Int64("friendID", fs.FriendID),
				zap.Error(err),
			)
			// 仍然包含好友关系，只是没有个人资料信息
			items = append(items, FriendListItem{Friendship: fs, IsBlocked: isBlocked})
			continue
		}
		if user == nil {
			items = append(items, FriendListItem{Friendship: fs, IsBlocked: isBlocked})
			continue
		}
		items = append(items, FriendListItem{
			Friendship: fs,
			Nickname:   user.Nickname,
			AvatarURL:  user.AvatarURL,
			IsBlocked:  isBlocked,
		})
	}

	return items, nil
}

// DeleteFriend 删除两个用户之间的双向好友关系。
func (s *FriendService) DeleteFriend(ctx context.Context, userID, friendID int64) error {
	if err := s.mysqlRepo.DeleteFriendship(ctx, userID, friendID); err != nil {
		return fmt.Errorf("删除好友关系: %w", err)
	}
	s.invalidateFriend(ctx, userID, friendID)

	s.logger.Debug("好友已删除",
		zap.Int64("userID", userID),
		zap.Int64("friendID", friendID),
	)

	return nil
}

// BlockUser 将用户添加到调用者的黑名单中。
func (s *FriendService) BlockUser(ctx context.Context, userID, blockedID int64) error {
	// 检查是否已经拉黑
	isBlocked, err := s.mysqlRepo.IsBlocked(ctx, userID, blockedID)
	if err != nil {
		return fmt.Errorf("检查是否已拉黑: %w", err)
	}
	if isBlocked {
		return fmt.Errorf(ErrAlreadyBlocked)
	}

	bl := &model.Blacklist{
		UserID:    userID,
		BlockedID: blockedID,
	}
	if err := s.mysqlRepo.CreateBlacklist(ctx, bl); err != nil {
		return fmt.Errorf("创建黑名单记录: %w", err)
	}
	s.invalidateBlock(ctx, userID, blockedID)

	s.logger.Debug("用户已拉黑",
		zap.Int64("userID", userID),
		zap.Int64("blockedID", blockedID),
	)

	return nil
}

// UnblockUser 将用户从调用者的黑名单中移除。
func (s *FriendService) UnblockUser(ctx context.Context, userID, blockedID int64) error {
	if err := s.mysqlRepo.DeleteBlacklist(ctx, userID, blockedID); err != nil {
		return fmt.Errorf("删除黑名单记录: %w", err)
	}
	s.invalidateBlock(ctx, userID, blockedID)

	s.logger.Debug("用户已取消拉黑",
		zap.Int64("userID", userID),
		zap.Int64("blockedID", blockedID),
	)

	return nil
}

// IsBlocked 检查调用者是否已拉黑指定用户。
func (s *FriendService) IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error) {
	isBlocked, err := s.mysqlRepo.IsBlocked(ctx, userID, blockedID)
	if err != nil {
		return false, fmt.Errorf("检查是否已拉黑: %w", err)
	}
	return isBlocked, nil
}

// HandleFriendApply 通过 WebSocket 处理好友申请。
// 此方法作为 WS MessageDispatcher 的占位符保留——HTTP 处理器是好友操作的主要接口。
func (s *FriendService) HandleFriendApply(userID int64, data []byte) {
	// TODO: 如有需要，实现基于 WebSocket 的好友申请；HTTP 处理器是首选方式
}
