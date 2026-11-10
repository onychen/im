package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/goim/goim/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type momentOutboxEvent struct {
	MomentID int64 `gorm:"primaryKey;autoIncrement:false"`
	Payload  string
}

func (momentOutboxEvent) TableName() string { return "moment_outbox" }

// CreateMomentWithOutbox 持久化动态和发送事件。事件快照保留发布时间的毫秒精度。
func (m *MySQLRepoImpl) CreateMomentWithOutbox(ctx context.Context, moment *model.Moment) error {
	return m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(moment).Error; err != nil {
			return repoError("插入朋友圈动态", err)
		}
		payload, err := json.Marshal(moment)
		if err != nil {
			return repoError("编码动态分发事件", err)
		}
		event := momentOutboxEvent{MomentID: moment.ID, Payload: string(payload)}
		return repoError("写入动态 Outbox", tx.Create(&event).Error)
	})
}

// RelayNextMomentEvent 每次锁定一条到期事件，其他投递任务可以跳过该行。
// Kafka 确认后删除记录。若删除提交失败，同一事件会再次发送。
func (m *MySQLRepoImpl) RelayNextMomentEvent(ctx context.Context, publish func(context.Context, *model.Moment) error) (bool, error) {
	var event momentOutboxEvent
	var publishErr error
	err := m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Where("available_at <= CURRENT_TIMESTAMP(6)").
			Order("available_at, moment_id").Take(&event).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var moment model.Moment
		publishErr = json.Unmarshal([]byte(event.Payload), &moment)
		if publishErr == nil {
			publishErr = publish(ctx, &moment)
		}
		if publishErr != nil {
			return tx.Model(&event).UpdateColumn("available_at", gorm.Expr("CURRENT_TIMESTAMP(6) + INTERVAL 5 SECOND")).Error
		}
		return tx.Delete(&event).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, repoError("处理动态 Outbox 事务", err)
	}
	if publishErr != nil {
		return true, fmt.Errorf("投递动态事件 %d: %w", event.MomentID, publishErr)
	}
	return event.MomentID != 0, nil
}
