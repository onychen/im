package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// ── MsgOpService 错误常量 ──

const (
	ErrMsgNotRevocable    = "消息无法撤回（未找到或已过时效）"
	ErrMsgRevokeNotOwner  = "仅发送者可撤回消息"
	ErrMsgDeleteFailed    = "删除消息失败"
)

// MsgOpService 处理消息操作：撤回、删除和搜索。
// RevokeMessage 使用 Redis Lua ExecRevokeMsg 进行原子撤回，并
// 通过 InsertMsgRevoked 将撤回记录持久化到 MySQL。
type MsgOpService struct {
	mysqlRepo repository.MySQLRepo
	redisRepo repository.RedisRepo
	logger    *zap.Logger
}

// NewMsgOpService creates a MsgOpService with all required dependencies.
func NewMsgOpService(mysqlRepo repository.MySQLRepo, redisRepo repository.RedisRepo, logger *zap.Logger) *MsgOpService {
	return &MsgOpService{
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		logger:    logger,
	}
}

// RevokeMessage revokes a message. It calls the Redis Lua ExecRevokeMsg script
// to atomically replace the original message in the inbox/outbox ZSet with a
// revoked placeholder (msgType=6). If the Lua script succeeds, it persists
// the revoke record to MySQL.
func (s *MsgOpService) RevokeMessage(ctx context.Context, userID int64, convID string, msgID int64) error {
	// Build the revoked placeholder message for the Lua script
	revokedMsg := model.InboxMessage{
		MsgID:     msgID,
		ConvID:    convID,
		FromID:    userID,
		MsgType:   model.MsgTypeRevoked,
		Content:   "This message has been revoked",
		Timestamp: time.Now().UnixMilli(),
	}
	revokedJSON, err := json.Marshal(revokedMsg)
	if err != nil {
		return fmt.Errorf("marshal revoked message: %w", err)
	}

	// Execute the Redis Lua script
	success, err := s.redisRepo.ExecRevokeMsg(ctx, userID, convID, msgID, string(revokedJSON), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("exec revoke lua: %w", err)
	}
	if !success {
		return fmt.Errorf(ErrMsgNotRevocable)
	}

	// Persist the revoke record to MySQL
	revoked := &model.MsgRevoked{
		MsgID:      msgID,
		ConvID:     convID,
		OperatorID: userID,
		RevokedAt:  time.Now(),
	}
	if err := s.mysqlRepo.InsertMsgRevoked(ctx, revoked); err != nil {
		s.logger.Error("failed to persist revoke record to MySQL",
			zap.Int64("msgID", msgID),
			zap.String("convID", convID),
			zap.Error(err),
		)
		// Redis already succeeded, so we don't fail the request; just log the error
	}

	s.logger.Debug("message revoked",
		zap.Int64("userID", userID),
		zap.String("convID", convID),
		zap.Int64("msgID", msgID),
	)

	return nil
}

// DeleteMessage soft-deletes a message by marking it as deleted in Redis.
// The message is not actually removed from the inbox/outbox; instead a
// deleted placeholder replaces it.
func (s *MsgOpService) DeleteMessage(ctx context.Context, userID int64, convID string, msgID int64) error {
	// Soft delete: mark as deleted by replacing the message with a placeholder
	deletedMsg := model.InboxMessage{
		MsgID:     msgID,
		ConvID:    convID,
		FromID:    userID,
		MsgType:   model.MsgTypeSystem,
		Content:   "This message has been deleted",
		Timestamp: time.Now().UnixMilli(),
	}
	deletedJSON, err := json.Marshal(deletedMsg)
	if err != nil {
		return fmt.Errorf(ErrMsgDeleteFailed+": %w", err)
	}

	// Use the same ExecRevokeMsg Lua to atomically swap the message
	success, err := s.redisRepo.ExecRevokeMsg(ctx, userID, convID, msgID, string(deletedJSON), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf(ErrMsgDeleteFailed+": %w", err)
	}
	if !success {
		return fmt.Errorf(ErrMsgDeleteFailed)
	}

	s.logger.Debug("message deleted (soft)",
		zap.Int64("userID", userID),
		zap.String("convID", convID),
		zap.Int64("msgID", msgID),
	)

	return nil
}

// SearchMessages searches private messages by content for the given user.
func (s *MsgOpService) SearchMessages(ctx context.Context, userID int64, query string, limit, offset int) ([]model.PrivateMessage, error) {
	if query == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	msgs, err := s.mysqlRepo.SearchPrivateMessages(ctx, userID, query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("search private messages: %w", err)
	}

	return msgs, nil
}
