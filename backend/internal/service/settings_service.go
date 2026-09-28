package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// ── SettingsService 错误常量 ──

const (
	ErrSettingsNotFound = "设置未找到"
	ErrMuteConvExists   = "会话已被静音"
	ErrMuteConvNotFound = "会话不在静音列表中"
	ErrInvalidMuteConv  = "会话已不可用"
)

// SettingsService handles user settings: notification preferences and mute list.
type SettingsService struct {
	mysqlRepo repository.MySQLRepo
	logger    *zap.Logger
}

// NewSettingsService creates a SettingsService with all required dependencies.
func NewSettingsService(mysqlRepo repository.MySQLRepo, logger *zap.Logger) *SettingsService {
	return &SettingsService{
		mysqlRepo: mysqlRepo,
		logger:    logger,
	}
}

// GetSettings returns the user settings. If no settings row exists, it returns
// default values (notifications enabled, preview enabled, empty mute list).
func (s *SettingsService) GetSettings(ctx context.Context, userID int64) (*model.UserSettings, error) {
	settings, err := s.mysqlRepo.GetUserSettings(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("get user settings: %w", err)
	}
	if settings != nil {
		return settings, nil
	}

	// Return default settings if no row exists
	return &model.UserSettings{
		UserID:              userID,
		NotificationEnabled: true,
		MsgPreviewEnabled:   true,
		MuteList:            "[]",
	}, nil
}

// UpdateSettings creates or updates user settings.
func (s *SettingsService) UpdateSettings(ctx context.Context, userID int64, settings *model.UserSettings) error {
	settings.UserID = userID
	if err := s.mysqlRepo.CreateOrUpdateUserSettings(ctx, settings); err != nil {
		return fmt.Errorf("update user settings: %w", err)
	}

	s.logger.Debug("user settings updated",
		zap.Int64("userID", userID),
	)

	return nil
}

// AddMuteConv adds a conversation ID to the user's mute list.
func (s *SettingsService) AddMuteConv(ctx context.Context, userID int64, convID string) error {
	if err := s.validateMuteConversation(ctx, userID, convID); err != nil {
		return err
	}
	settings, err := s.GetSettings(ctx, userID)
	if err != nil {
		return fmt.Errorf("get settings for mute: %w", err)
	}

	var muteList []string
	if settings.MuteList != "" && settings.MuteList != "[]" {
		if err := json.Unmarshal([]byte(settings.MuteList), &muteList); err != nil {
			muteList = []string{} // reset if malformed
		}
	} else {
		muteList = []string{}
	}

	// Check for duplicate
	for _, id := range muteList {
		if id == convID {
			return fmt.Errorf(ErrMuteConvExists)
		}
	}

	muteList = append(muteList, convID)
	muteJSON, err := json.Marshal(muteList)
	if err != nil {
		return fmt.Errorf("marshal mute list: %w", err)
	}

	settings.MuteList = string(muteJSON)
	if err := s.UpdateSettings(ctx, userID, settings); err != nil {
		return fmt.Errorf("save mute list: %w", err)
	}

	s.logger.Debug("conversation muted",
		zap.Int64("userID", userID),
		zap.String("convID", convID),
	)

	return nil
}

func (s *SettingsService) validateMuteConversation(ctx context.Context, userID int64, convID string) error {
	if !strings.HasPrefix(convID, "p_") {
		return nil
	}
	parts := strings.Split(convID, "_")
	if len(parts) != 3 {
		return fmt.Errorf(ErrInvalidMuteConv)
	}
	firstID, firstErr := strconv.ParseInt(parts[1], 10, 64)
	secondID, secondErr := strconv.ParseInt(parts[2], 10, 64)
	if firstErr != nil || secondErr != nil || firstID <= 0 || secondID <= 0 || (firstID != userID && secondID != userID) {
		return fmt.Errorf(ErrInvalidMuteConv)
	}
	otherID := firstID
	if otherID == userID {
		otherID = secondID
	}
	isFriend, err := s.mysqlRepo.IsFriend(ctx, userID, otherID)
	if err != nil {
		return fmt.Errorf("validate private conversation: %w", err)
	}
	if !isFriend {
		return fmt.Errorf(ErrInvalidMuteConv)
	}
	return nil
}

// RemoveMuteConv removes a conversation ID from the user's mute list.
func (s *SettingsService) RemoveMuteConv(ctx context.Context, userID int64, convID string) error {
	settings, err := s.GetSettings(ctx, userID)
	if err != nil {
		return fmt.Errorf("get settings for unmute: %w", err)
	}

	var muteList []string
	if settings.MuteList != "" && settings.MuteList != "[]" {
		if err := json.Unmarshal([]byte(settings.MuteList), &muteList); err != nil {
			muteList = []string{} // reset if malformed
		}
	} else {
		muteList = []string{}
	}

	// Find and remove the convID
	found := false
	newList := make([]string, 0, len(muteList))
	for _, id := range muteList {
		if id == convID {
			found = true
			continue
		}
		newList = append(newList, id)
	}
	if !found {
		return fmt.Errorf(ErrMuteConvNotFound)
	}

	muteJSON, err := json.Marshal(newList)
	if err != nil {
		return fmt.Errorf("marshal mute list: %w", err)
	}

	settings.MuteList = string(muteJSON)
	if err := s.UpdateSettings(ctx, userID, settings); err != nil {
		return fmt.Errorf("save mute list: %w", err)
	}

	s.logger.Debug("conversation unmuted",
		zap.Int64("userID", userID),
		zap.String("convID", convID),
	)

	return nil
}
