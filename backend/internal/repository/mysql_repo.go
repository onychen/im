package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/goim/goim/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MySQLRepo 定义了服务和消费者所需的所有 MySQL CRUD 操作。
// 该接口允许在测试中进行模拟。
type MySQLRepo interface {
	// ── 用户 ──
	GetUserByID(ctx context.Context, userID int64) (*model.User, error)
	GetUserByUsername(ctx context.Context, username string) (*model.User, error)
	CreateUser(ctx context.Context, user *model.User) error
	UpdateUser(ctx context.Context, user *model.User) error

	// ── 好友 ──
	CreateFriendRequest(ctx context.Context, req *model.FriendRequest) error
	UpdateFriendRequest(ctx context.Context, req *model.FriendRequest) error
	GetFriendRequestByID(ctx context.Context, id int64) (*model.FriendRequest, error)
	GetFriendRequestsByUser(ctx context.Context, userID int64) ([]model.FriendRequest, error)
	CreateFriendship(ctx context.Context, fs *model.Friendship) error
	DeleteFriendship(ctx context.Context, userID, friendID int64) error
	GetFriendList(ctx context.Context, userID int64) ([]model.Friendship, error)
	CountFriends(ctx context.Context, userID int64) (int, error)
	IsFriend(ctx context.Context, userID, friendID int64) (bool, error)
	CreateBlacklist(ctx context.Context, bl *model.Blacklist) error
	DeleteBlacklist(ctx context.Context, userID, blockedID int64) error
	IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error)

	// ── 群组 ──
	CreateGroup(ctx context.Context, group *model.Group) (int64, error)
	UpdateGroup(ctx context.Context, group *model.Group) error
	GetGroupByID(ctx context.Context, groupID int64) (*model.Group, error)
	AddGroupMember(ctx context.Context, member *model.GroupMember) error
	RemoveGroupMember(ctx context.Context, groupID, userID int64) error
	GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error)
	UpdateGroupMemberRole(ctx context.Context, groupID, userID, role int) error

	// ── 朋友圈 ──
	CreateMoment(ctx context.Context, moment *model.Moment) error
	GetMomentByID(ctx context.Context, id int64) (*model.Moment, error)
	DeleteMoment(ctx context.Context, id int64) error
	GetMomentsByIDs(ctx context.Context, ids []int64) ([]model.Moment, error)
	GetMomentsByUser(ctx context.Context, userID int64, limit, offset int) ([]model.Moment, error)
	CreateMomentComment(ctx context.Context, comment *model.MomentComment) error
	GetMomentCommentByID(ctx context.Context, id int64) (*model.MomentComment, error)
	GetMomentComments(ctx context.Context, momentID int64) ([]model.MomentComment, error)
	DeleteMomentComment(ctx context.Context, id int64) error

	// ── 用户设置 ──
	GetUserSettings(ctx context.Context, userID int64) (*model.UserSettings, error)
	CreateOrUpdateUserSettings(ctx context.Context, settings *model.UserSettings) error
}

// MySQLRepoImpl 使用 GORM 访问现有 MySQL 表，接口保持不变。
type MySQLRepoImpl struct{ db *gorm.DB }

var _ MySQLRepo = (*MySQLRepoImpl)(nil)

func NewMySQLRepo(db *gorm.DB) *MySQLRepoImpl { return &MySQLRepoImpl{db: db} }

func repoError(operation string, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return nil
}

// findOne 保留仓储约定：未找到记录返回 nil, nil。
func findOne[T any](query *gorm.DB, operation string) (*T, error) {
	var value T
	err := query.Take(&value).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, repoError(operation, err)
	}
	return &value, nil
}

// ── 用户 ──
func (m *MySQLRepoImpl) GetUserByID(ctx context.Context, userID int64) (*model.User, error) {
	return findOne[model.User](m.db.WithContext(ctx).Where("id = ?", userID), "按ID获取用户")
}

func (m *MySQLRepoImpl) GetUserByUsername(ctx context.Context, username string) (*model.User, error) {
	return findOne[model.User](m.db.WithContext(ctx).Where("username = ?", username), "按用户名获取用户")
}

func (m *MySQLRepoImpl) CreateUser(ctx context.Context, user *model.User) error {
	return repoError("创建用户", m.db.WithContext(ctx).Select("Username", "PasswordHash", "Nickname", "AvatarURL", "Sign", "Gender").Create(user).Error)
}

func (m *MySQLRepoImpl) UpdateUser(ctx context.Context, user *model.User) error {
	// map 显式包含空字符串和 gender=0，避免 GORM 忽略零值。
	return repoError("更新用户", m.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", user.ID).Updates(map[string]any{
		"username": user.Username, "password_hash": user.PasswordHash, "nickname": user.Nickname,
		"avatar_url": user.AvatarURL, "sign": user.Sign, "gender": user.Gender,
	}).Error)
}

// ── 好友 ──
func (m *MySQLRepoImpl) CreateFriendRequest(ctx context.Context, req *model.FriendRequest) error {
	// MySQL 按赋值顺序更新：status 必须最后写，待处理申请保留原消息和时间。
	updates := clause.Set{
		{Column: clause.Column{Name: "id"}, Value: gorm.Expr("LAST_INSERT_ID(id)")},
		{Column: clause.Column{Name: "message"}, Value: gorm.Expr("IF(status = 0, message, VALUES(message))")},
		{Column: clause.Column{Name: "created_at"}, Value: gorm.Expr("IF(status = 0, created_at, NOW())")},
		{Column: clause.Column{Name: "updated_at"}, Value: gorm.Expr("IF(status = 0, updated_at, NOW())")},
		{Column: clause.Column{Name: "status"}, Value: gorm.Expr("IF(status = 0, status, VALUES(status))")},
	}
	return repoError("插入好友请求", m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Select("FromUserID", "ToUserID", "Message", "Status").Clauses(clause.OnConflict{DoUpdates: updates}).Create(req).Error; err != nil {
			return err
		}
		// 完全未变化的重复申请 RowsAffected=0，GORM 不会回填 LastInsertId。
		if req.ID == 0 {
			var existing model.FriendRequest
			if err := tx.Select("id").Where("from_user_id = ? AND to_user_id = ?", req.FromUserID, req.ToUserID).Take(&existing).Error; err != nil {
				return err
			}
			req.ID = existing.ID
		}
		return nil
	}))
}

func (m *MySQLRepoImpl) UpdateFriendRequest(ctx context.Context, req *model.FriendRequest) error {
	return repoError("更新好友请求", m.db.WithContext(ctx).Model(&model.FriendRequest{}).Where("id = ?", req.ID).Updates(map[string]any{"status": req.Status, "updated_at": gorm.Expr("NOW()")}).Error)
}

func (m *MySQLRepoImpl) GetFriendRequestByID(ctx context.Context, id int64) (*model.FriendRequest, error) {
	return findOne[model.FriendRequest](m.db.WithContext(ctx).Where("id = ?", id), "按ID获取好友请求")
}

func (m *MySQLRepoImpl) GetFriendRequestsByUser(ctx context.Context, userID int64) ([]model.FriendRequest, error) {
	var requests []model.FriendRequest
	err := m.db.WithContext(ctx).Where("to_user_id = ? AND status = ?", userID, 0).Order("created_at DESC").Find(&requests).Error
	return requests, repoError("按用户获取好友请求", err)
}

func (m *MySQLRepoImpl) CreateFriendship(ctx context.Context, fs *model.Friendship) error {
	// 双向关系必须一起成功；任一方向冲突时整个事务回滚。
	return repoError("插入双向好友关系", m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pairs := []model.Friendship{{UserID: fs.UserID, FriendID: fs.FriendID}, {UserID: fs.FriendID, FriendID: fs.UserID}}
		return tx.Select("UserID", "FriendID").Create(&pairs).Error
	}))
}

func (m *MySQLRepoImpl) DeleteFriendship(ctx context.Context, userID, friendID int64) error {
	return repoError("删除好友关系", m.db.WithContext(ctx).Where("(user_id = ? AND friend_id = ?) OR (user_id = ? AND friend_id = ?)", userID, friendID, friendID, userID).Delete(&model.Friendship{}).Error)
}

func (m *MySQLRepoImpl) GetFriendList(ctx context.Context, userID int64) ([]model.Friendship, error) {
	var friends []model.Friendship
	err := m.db.WithContext(ctx).Where("user_id = ?", userID).Find(&friends).Error
	return friends, repoError("获取好友列表", err)
}

func (m *MySQLRepoImpl) CountFriends(ctx context.Context, userID int64) (int, error) {
	var count int64
	err := m.db.WithContext(ctx).Model(&model.Friendship{}).Where("user_id = ?", userID).Count(&count).Error
	return int(count), repoError("统计好友数", err)
}

func (m *MySQLRepoImpl) IsFriend(ctx context.Context, userID, friendID int64) (bool, error) {
	var count int64
	err := m.db.WithContext(ctx).Model(&model.Friendship{}).Where("user_id = ? AND friend_id = ?", userID, friendID).Count(&count).Error
	return count > 0, repoError("检查是否为好友", err)
}

func (m *MySQLRepoImpl) CreateBlacklist(ctx context.Context, bl *model.Blacklist) error {
	return repoError("插入黑名单", m.db.WithContext(ctx).Select("UserID", "BlockedID").Create(bl).Error)
}

func (m *MySQLRepoImpl) DeleteBlacklist(ctx context.Context, userID, blockedID int64) error {
	return repoError("删除黑名单", m.db.WithContext(ctx).Where("user_id = ? AND blocked_id = ?", userID, blockedID).Delete(&model.Blacklist{}).Error)
}

func (m *MySQLRepoImpl) IsBlocked(ctx context.Context, userID, blockedID int64) (bool, error) {
	var count int64
	err := m.db.WithContext(ctx).Model(&model.Blacklist{}).Where("user_id = ? AND blocked_id = ?", userID, blockedID).Count(&count).Error
	return count > 0, repoError("检查是否已拉黑", err)
}

// ── 群组 ──
func (m *MySQLRepoImpl) CreateGroup(ctx context.Context, group *model.Group) (int64, error) {
	row := model.Group{Name: group.Name, Notice: group.Notice, OwnerID: group.OwnerID, MaxMembers: 500}
	err := m.db.WithContext(ctx).Create(&row).Error
	return row.ID, repoError("创建群组", err)
}

func (m *MySQLRepoImpl) UpdateGroup(ctx context.Context, group *model.Group) error {
	return repoError("更新群组", m.db.WithContext(ctx).Model(&model.Group{}).Where("id = ?", group.ID).Updates(map[string]any{"name": group.Name, "notice": group.Notice, "owner_id": group.OwnerID}).Error)
}

func (m *MySQLRepoImpl) GetGroupByID(ctx context.Context, groupID int64) (*model.Group, error) {
	return findOne[model.Group](m.db.WithContext(ctx).Where("id = ?", groupID), "按ID获取群组")
}

func (m *MySQLRepoImpl) AddGroupMember(ctx context.Context, member *model.GroupMember) error {
	// joined_at 由数据库默认值填充。
	return repoError("添加群成员", m.db.WithContext(ctx).Select("GroupID", "UserID", "Role", "MutedUntil").Create(member).Error)
}

func (m *MySQLRepoImpl) RemoveGroupMember(ctx context.Context, groupID, userID int64) error {
	return repoError("移除群成员", m.db.WithContext(ctx).Where("group_id = ? AND user_id = ?", groupID, userID).Delete(&model.GroupMember{}).Error)
}

func (m *MySQLRepoImpl) GetGroupMembers(ctx context.Context, groupID int64) ([]model.GroupMember, error) {
	var members []model.GroupMember
	err := m.db.WithContext(ctx).Where("group_id = ?", groupID).Find(&members).Error
	return members, repoError("获取群成员", err)
}

func (m *MySQLRepoImpl) UpdateGroupMemberRole(ctx context.Context, groupID, userID, role int) error {
	return repoError("更新群成员角色", m.db.WithContext(ctx).Model(&model.GroupMember{}).Where("group_id = ? AND user_id = ?", groupID, userID).Update("role", role).Error)
}

// ── 朋友圈 ──
func (m *MySQLRepoImpl) CreateMoment(ctx context.Context, moment *model.Moment) error {
	return repoError("插入朋友圈动态", m.db.WithContext(ctx).Create(moment).Error)
}

func (m *MySQLRepoImpl) GetMomentByID(ctx context.Context, id int64) (*model.Moment, error) {
	return findOne[model.Moment](m.db.WithContext(ctx).Where("id = ?", id), "按ID获取朋友圈动态")
}

func (m *MySQLRepoImpl) DeleteMoment(ctx context.Context, id int64) error {
	return repoError("删除动态及关联数据", m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockMoment(tx, id); err != nil {
			return err
		}
		if err := tx.Where("moment_id = ?", id).Delete(&model.MomentComment{}).Error; err != nil {
			return err
		}
		if err := tx.Where("moment_id = ?", id).Delete(&model.MomentLike{}).Error; err != nil {
			return err
		}
		return tx.Where("id = ?", id).Delete(&model.Moment{}).Error
	}))
}

func (m *MySQLRepoImpl) GetMomentsByIDs(ctx context.Context, ids []int64) ([]model.Moment, error) {
	moments := make([]model.Moment, 0)
	if len(ids) == 0 {
		return moments, nil
	}
	err := m.db.WithContext(ctx).Where("id IN ?", ids).Find(&moments).Error
	return moments, repoError("按ID批量获取朋友圈动态", err)
}

func (m *MySQLRepoImpl) GetMomentsByUser(ctx context.Context, userID int64, limit, offset int) ([]model.Moment, error) {
	moments := make([]model.Moment, 0)
	err := m.db.WithContext(ctx).Where("author_id = ?", userID).Order("created_at DESC").Limit(limit).Offset(offset).Find(&moments).Error
	return moments, repoError("按用户获取朋友圈动态", err)
}

func (m *MySQLRepoImpl) CreateMomentComment(ctx context.Context, comment *model.MomentComment) error {
	return repoError("插入朋友圈评论", m.db.WithContext(ctx).Create(comment).Error)
}

func (m *MySQLRepoImpl) GetMomentCommentByID(ctx context.Context, id int64) (*model.MomentComment, error) {
	return findOne[model.MomentComment](m.db.WithContext(ctx).Select("id", "moment_id", "user_id", "content", "created_at").Where("id = ?", id), "按ID获取朋友圈评论")
}

func (m *MySQLRepoImpl) GetMomentComments(ctx context.Context, momentID int64) ([]model.MomentComment, error) {
	comments := make([]model.MomentComment, 0)
	err := m.db.WithContext(ctx).Table("moment_comments AS c").Select("c.id, c.moment_id, c.user_id, c.content, c.created_at, u.username, u.avatar_url").Joins("JOIN users u ON u.id = c.user_id").Where("c.moment_id = ?", momentID).Order("c.id ASC").Scan(&comments).Error
	return comments, repoError("获取朋友圈评论列表", err)
}

func (m *MySQLRepoImpl) DeleteMomentComment(ctx context.Context, id int64) error {
	return repoError("删除朋友圈评论", m.db.WithContext(ctx).Where("id = ?", id).Delete(&model.MomentComment{}).Error)
}

// settingsRecord 将 API 中的空字符串转换为数据库 JSON NULL。
type settingsRecord struct {
	ID                  int64
	UserID              int64
	NotificationEnabled bool
	MsgPreviewEnabled   bool
	MuteList            *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

func (settingsRecord) TableName() string { return "user_settings" }

func (m *MySQLRepoImpl) GetUserSettings(ctx context.Context, userID int64) (*model.UserSettings, error) {
	row, err := findOne[settingsRecord](m.db.WithContext(ctx).Where("user_id = ?", userID), "获取用户设置")
	if err != nil || row == nil {
		return nil, err
	}
	settings := &model.UserSettings{ID: row.ID, UserID: row.UserID, NotificationEnabled: row.NotificationEnabled, MsgPreviewEnabled: row.MsgPreviewEnabled, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}
	if row.MuteList != nil {
		settings.MuteList = *row.MuteList
	}
	return settings, nil
}

func (m *MySQLRepoImpl) CreateOrUpdateUserSettings(ctx context.Context, settings *model.UserSettings) error {
	row := settingsRecord{UserID: settings.UserID, NotificationEnabled: settings.NotificationEnabled, MsgPreviewEnabled: settings.MsgPreviewEnabled}
	if settings.MuteList != "" {
		row.MuteList = &settings.MuteList
	}
	updates := clause.Set{
		{Column: clause.Column{Name: "id"}, Value: gorm.Expr("LAST_INSERT_ID(id)")},
		{Column: clause.Column{Name: "notification_enabled"}, Value: gorm.Expr("VALUES(notification_enabled)")},
		{Column: clause.Column{Name: "msg_preview_enabled"}, Value: gorm.Expr("VALUES(msg_preview_enabled)")},
		{Column: clause.Column{Name: "mute_list"}, Value: gorm.Expr("VALUES(mute_list)")},
	}
	err := m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Select("UserID", "NotificationEnabled", "MsgPreviewEnabled", "MuteList").Clauses(clause.OnConflict{DoUpdates: updates}).Create(&row).Error; err != nil {
			return err
		}
		if row.ID == 0 {
			var existing settingsRecord
			if err := tx.Select("id").Where("user_id = ?", row.UserID).Take(&existing).Error; err != nil {
				return err
			}
			row.ID = existing.ID
		}
		return nil
	})
	if err == nil {
		settings.ID = row.ID
	}
	return repoError("创建或更新用户设置", err)
}
