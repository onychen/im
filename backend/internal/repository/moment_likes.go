package repository

import (
	"context"
	"errors"

	"github.com/goim/goim/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrMomentNotFound = errors.New("动态不存在")

type MomentLikeStats struct {
	MomentID  int64
	LikeCount int64
	LikedByMe bool
}

// lockMoment 让点赞变更和删除动态先锁同一行，避免删除后留下点赞记录。
func lockMoment(tx *gorm.DB, momentID int64) error {
	var moment model.Moment
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Take(&moment, "id = ?", momentID).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrMomentNotFound
	}
	return err
}

// SetMomentLike 设置点赞状态，并在提交前读取该动态的点赞数。
func (m *MySQLRepoImpl) SetMomentLike(ctx context.Context, momentID, userID int64, liked bool) (int64, error) {
	var count int64
	err := m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockMoment(tx, momentID); err != nil {
			return err
		}
		if liked {
			like := model.MomentLike{MomentID: momentID, UserID: userID}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&like).Error; err != nil {
				return err
			}
		} else if err := tx.Where("moment_id = ? AND user_id = ?", momentID, userID).Delete(&model.MomentLike{}).Error; err != nil {
			return err
		}
		return tx.Model(&model.MomentLike{}).Where("moment_id = ?", momentID).Count(&count).Error
	})
	if err != nil {
		return 0, repoError("保存动态点赞状态", err)
	}
	return count, nil
}

// GetMomentLikeStats 在同一条查询里读取点赞总数和当前用户的点赞状态。
func (m *MySQLRepoImpl) GetMomentLikeStats(ctx context.Context, viewerID int64, momentIDs []int64) (map[int64]MomentLikeStats, error) {
	stats := make(map[int64]MomentLikeStats, len(momentIDs))
	if len(momentIDs) == 0 {
		return stats, nil
	}
	var rows []MomentLikeStats
	err := m.db.WithContext(ctx).Model(&model.MomentLike{}).
		Select("moment_id, COUNT(*) AS like_count, MAX(CASE WHEN user_id = ? THEN 1 ELSE 0 END) AS liked_by_me", viewerID).
		Where("moment_id IN ?", momentIDs).Group("moment_id").Scan(&rows).Error
	if err != nil {
		return nil, repoError("查询动态点赞统计", err)
	}
	for _, row := range rows {
		stats[row.MomentID] = row
	}
	return stats, nil
}

func (m *MySQLRepoImpl) ListMomentLikerProfiles(ctx context.Context, momentID int64) ([]model.MomentLiker, error) {
	likers := make([]model.MomentLiker, 0)
	err := m.db.WithContext(ctx).Table("moment_likes AS l").
		Select("l.user_id, u.username, COALESCE(u.avatar_url, '') AS avatar_url").
		Joins("JOIN users u ON u.id = l.user_id").Where("l.moment_id = ?", momentID).
		Order("l.user_id ASC").Scan(&likers).Error
	return likers, repoError("查询动态点赞用户", err)
}
