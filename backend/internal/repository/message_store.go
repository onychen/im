package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/goim/goim/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// MessageStore 定义消息与会话在 MySQL 中的读写操作。
// MySQL 是消息的事实来源：messages 存消息本体，conversations 存会话级元数据，
// user_conversations 存每个用户在会话上的个人状态（已读位置、可见下界等）。
//
// 由 *MySQLRepoImpl 实现；独立成接口是为了不扩大 MySQLRepo，避免影响其它模块的测试 mock。
type MessageStore interface {
	// ── 消息 ──

	// PersistMessages 在一个事务里批量写入消息并推进 conversations 的最后一条。
	// 写入用 INSERT IGNORE 保证重复消费幂等：同 (convId, seq) 且同 msgId 视为重复，直接跳过；
	// (convId, seq) 已被其它 msgId 占用，或 (senderId, clientMsgId) 已存在的，作为冲突返回，由调用方转投 DLQ。
	// 撤回通知（msgType=6）会在同一事务内把目标消息置为已撤回。
	PersistMessages(ctx context.Context, msgs []model.Message) (conflicts []model.Message, err error)
	// GetMaxSeq 返回会话已落库的最大 seq，不含预留号段。
	GetMaxSeq(ctx context.Context, convID string) (int64, error)
	// ReserveSeqRange 持久化预留号段 [start, end]。floor 是实际消息/迁移旧计数器的下限。
	ReserveSeqRange(ctx context.Context, convID string, convType int, size, floor int64) (start, end int64, err error)
	// GetMessagesBySeqRange 按 seq 升序返回 [fromSeq, toSeq] 内的消息，最多 limit 条。
	GetMessagesBySeqRange(ctx context.Context, convID string, fromSeq, toSeq int64, limit int) ([]model.Message, error)
	// GetMessagesBefore 按 seq 降序返回可见且未被用户删除的消息，最多 limit 条。
	GetMessagesBefore(ctx context.Context, userID int64, convID string, beforeSeq, minSeq int64, limit int) ([]model.Message, error)
	// GetHistoryState 校验用户会话关系并读取可见下界，群聊同时读取已落库的历史版本。
	GetHistoryState(ctx context.Context, userID int64, convID string, convType int) (*HistoryState, error)
	// GetGroupHistoryWindow 读取指定版本的最近消息，按 seq 降序，不做用户级过滤。
	GetGroupHistoryWindow(ctx context.Context, convID string, version int64, limit int) ([]model.Message, error)
	// GetMessage 返回单条消息；不存在时返回 nil, nil。
	GetMessage(ctx context.Context, convID string, seq int64) (*model.Message, error)
	// SearchMessages 在用户可见的会话中全文检索文本消息；convID 为空表示全部会话。
	SearchMessages(ctx context.Context, userID int64, keyword, convID string, limit int) ([]model.Message, error)

	// ── 会话 ──

	// EnsureConversation 确保 conversations 中存在该会话（已存在则不变）。
	EnsureConversation(ctx context.Context, convID string, convType int) error
	// GetConversations 批量读取会话元数据，返回 convID → Conversation。
	GetConversations(ctx context.Context, convIDs []string) (map[string]model.Conversation, error)
	// EnsureUserConversations 为一批用户建立会话关系（已存在则取消隐藏）。
	EnsureUserConversations(ctx context.Context, convID string, userIDs []int64) error
	// JoinConversation 为新入群成员建立会话关系，min_seq 设为入群时刻的 seq+1、read_seq 设为 min_seq-1，
	// 新成员看不到入群前的历史，也不会把入群前的消息算作未读。已存在的行只取消隐藏，不改可见下界。
	JoinConversation(ctx context.Context, convID string, userIDs []int64, minSeq int64) error
	// DeleteUserConversation 删除用户与会话的关系（退群、被踢、删好友时调用）。
	DeleteUserConversation(ctx context.Context, userID int64, convID string) error
	// ListUserConversations 返回用户的全部会话关系（含已隐藏的，由调用方判断是否展示）。
	ListUserConversations(ctx context.Context, userID int64) ([]model.UserConversation, error)
	// GetUserConversation 返回用户在某个会话上的状态；不存在时返回 nil, nil。
	GetUserConversation(ctx context.Context, userID int64, convID string) (*model.UserConversation, error)
	// UpdateReadSeq 把已读位置推进到 seq，只前进不后退。
	UpdateReadSeq(ctx context.Context, userID int64, convID string, seq int64) error
	// UpdateMinSeq 把可见下界推进到 minSeq（清空聊天记录），同时把已读位置推进到 minSeq-1。
	// hide=true 时同时把会话从列表中隐藏（删除会话）。
	UpdateMinSeq(ctx context.Context, userID int64, convID string, minSeq int64, hide bool) error
	// CountUnread 批量统计区间内实际有效的他人消息，排除撤回和个人删除。
	CountUnread(ctx context.Context, userID int64, ranges []SeqRange) (map[string]int64, error)

	// ── 会话展示资料 ──

	// GetUserBriefs 批量读取用户昵称与头像，用于会话列表。
	GetUserBriefs(ctx context.Context, userIDs []int64) (map[int64]UserBrief, error)
	// GetGroupNames 批量读取群名称，用于会话列表。
	GetGroupNames(ctx context.Context, groupIDs []int64) (map[int64]string, error)

	// ── 单条删除（只对自己生效）──

	AddUserDeletedMessage(ctx context.Context, userID int64, convID string, seq int64) error
	// GetUserDeletedSeqs 返回 [fromSeq, toSeq] 内被该用户删除的 seq 集合。
	GetUserDeletedSeqs(ctx context.Context, userID int64, convID string, fromSeq, toSeq int64) (map[int64]struct{}, error)

	// ── 群 ──

	// ListUserGroupIDs 返回用户加入的全部群 ID。
	ListUserGroupIDs(ctx context.Context, userID int64) ([]int64, error)
}

var _ MessageStore = (*MySQLRepoImpl)(nil)

// SeqRange 是会话内的 seq 闭区间 [From, To]。
type SeqRange struct {
	ConvID string
	From   int64
	To     int64
}

type HistoryState struct {
	MinSeq  int64
	Version int64
}

// UserBrief 是会话列表展示用的用户资料。
type UserBrief struct {
	Name      string // 昵称为空时取用户名
	AvatarURL string
}

// 存储行使用 MySQL 的时间和可空字段类型，避免改变 WebSocket/Kafka 的毫秒协议。
type messageRecord struct {
	ConvID      string `gorm:"primaryKey"`
	Seq         int64  `gorm:"primaryKey;autoIncrement:false"`
	MsgID       int64
	ConvType    int
	SenderID    int64
	MsgType     int
	Content     string
	Status      int
	ClientMsgID *string
	CreatedAt   time.Time `gorm:"autoCreateTime:false"`
}

func (messageRecord) TableName() string { return "messages" }

func messageToRecord(msg model.Message) messageRecord {
	row := messageRecord{ConvID: msg.ConvID, Seq: msg.Seq, MsgID: msg.MsgID, ConvType: msg.ConvType,
		SenderID: msg.SenderID, MsgType: msg.MsgType, Content: msg.Content, Status: msg.Status,
		CreatedAt: time.UnixMilli(msg.Timestamp)}
	// 无 clientMsgId 时必须写 NULL，避免 (sender_id, client_msg_id) 唯一键误去重。
	if msg.ClientMsgID != "" {
		row.ClientMsgID = &msg.ClientMsgID
	}
	return row
}

func (row messageRecord) message() model.Message {
	msg := model.Message{ConvID: row.ConvID, Seq: row.Seq, MsgID: row.MsgID, ConvType: row.ConvType,
		SenderID: row.SenderID, MsgType: row.MsgType, Content: row.Content, Status: row.Status,
		Timestamp: row.CreatedAt.UnixMilli()}
	if row.ClientMsgID != nil {
		msg.ClientMsgID = *row.ClientMsgID
	}
	return msg
}

func findMessages(query *gorm.DB, operation string) ([]model.Message, error) {
	var rows []messageRecord
	if err := query.Find(&rows).Error; err != nil {
		return nil, repoError(operation, err)
	}
	out := make([]model.Message, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.message())
	}
	return out, nil
}

type conversationRecord struct {
	ConvID         string `gorm:"primaryKey"`
	ConvType       int
	MaxSeq         int64
	SeqAllocMax    int64
	HistoryVersion int64 `gorm:"default:0"`
	LastMsgID      int64
	LastMsgBrief   string
	LastSenderID   int64
	LastMsgAt      *time.Time
}

func (conversationRecord) TableName() string { return "conversations" }

type deletedMessageRecord struct {
	UserID int64  `gorm:"primaryKey;autoIncrement:false"`
	ConvID string `gorm:"primaryKey"`
	Seq    int64  `gorm:"primaryKey;autoIncrement:false"`
}

func (deletedMessageRecord) TableName() string { return "user_deleted_messages" }

// ── 消息 ──
func (m *MySQLRepoImpl) PersistMessages(ctx context.Context, msgs []model.Message) ([]model.Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}
	var conflicts []model.Message
	err := m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		rows := make([]messageRecord, len(msgs))
		for i, msg := range msgs {
			rows[i] = messageToRecord(msg)
		}
		result := tx.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&rows)
		if result.Error != nil {
			return repoError("批量写入消息", result.Error)
		}

		accepted := msgs
		if result.RowsAffected < int64(len(msgs)) {
			stored, err := m.lookupMsgIDs(tx, msgs)
			if err != nil {
				return err
			}
			accepted = make([]model.Message, 0, len(msgs))
			for _, msg := range msgs {
				if id, ok := stored[seqKey{msg.ConvID, msg.Seq}]; ok && id == msg.MsgID {
					accepted = append(accepted, msg)
				} else {
					conflicts = append(conflicts, msg)
				}
			}
		}
		// 撤回和通知写入必须处于同一事务。
		for _, msg := range accepted {
			if msg.MsgType != model.MsgTypeRevoked {
				continue
			}
			var notice model.RevokeNotice
			if err := json.Unmarshal([]byte(msg.Content), &notice); err != nil || notice.TargetSeq <= 0 {
				continue
			}
			if err := tx.Model(&messageRecord{}).Where("conv_id = ? AND seq = ? AND msg_id = ?", msg.ConvID, notice.TargetSeq, notice.TargetMsgID).Update("status", model.MsgStatusRevoked).Error; err != nil {
				return repoError("标记消息撤回", err)
			}
		}
		latest := make(map[string]model.Message)
		for _, msg := range accepted {
			if cur, ok := latest[msg.ConvID]; !ok || msg.Seq > cur.Seq {
				latest[msg.ConvID] = msg
			}
		}
		for _, msg := range latest {
			at := time.UnixMilli(msg.Timestamp)
			row := conversationRecord{ConvID: msg.ConvID, ConvType: msg.ConvType, MaxSeq: msg.Seq, LastMsgID: msg.MsgID,
				LastMsgBrief: model.MsgBrief(msg.MsgType, msg.Content), LastSenderID: msg.SenderID, LastMsgAt: &at}
			// MySQL 依次执行赋值；max_seq 最后更新，前面的 IF 才能比较旧值。
			updates := clause.Set{}
			for _, name := range []string{"last_msg_id", "last_msg_brief", "last_sender_id", "last_msg_at"} {
				updates = append(updates, clause.Assignment{Column: clause.Column{Name: name}, Value: gorm.Expr(fmt.Sprintf("IF(VALUES(max_seq) > max_seq, VALUES(%s), %s)", name, name))})
			}
			if msg.ConvType == model.ConvTypeGroup {
				// 低于 max_seq 的补写和撤回也会改变历史，必须推进独立版本。
				row.HistoryVersion = 1
				updates = append(updates, clause.Assignment{Column: clause.Column{Name: "history_version"}, Value: gorm.Expr("history_version + 1")})
			}
			updates = append(updates, clause.Assignment{Column: clause.Column{Name: "max_seq"}, Value: gorm.Expr("GREATEST(max_seq, VALUES(max_seq))")})
			if err := tx.Clauses(clause.OnConflict{DoUpdates: updates}).Create(&row).Error; err != nil {
				return repoError("更新会话最后一条消息", err)
			}
			if msg.ConvType == model.ConvTypePrivate {
				if parsed, err := model.ParseConvID(msg.ConvID); err == nil {
					if err := NewMySQLRepo(tx).EnsureUserConversations(ctx, msg.ConvID, []int64{parsed.UserA, parsed.UserB}); err != nil {
						return err
					}
				}
			}
		}
		type senderConv struct {
			sender int64
			convID string
		}
		senderMax := make(map[senderConv]int64)
		for _, msg := range accepted {
			key := senderConv{msg.SenderID, msg.ConvID}
			if msg.Seq > senderMax[key] {
				senderMax[key] = msg.Seq
			}
		}
		for key, seq := range senderMax {
			if err := NewMySQLRepo(tx).UpdateReadSeq(ctx, key.sender, key.convID, seq); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, repoError("消息落库事务", err)
	}
	return conflicts, nil
}

type seqKey struct {
	convID string
	seq    int64
}

func (m *MySQLRepoImpl) lookupMsgIDs(tx *gorm.DB, msgs []model.Message) (map[seqKey]int64, error) {
	pairs := make([][]any, len(msgs))
	for i, msg := range msgs {
		pairs[i] = []any{msg.ConvID, msg.Seq}
	}
	var rows []messageRecord
	if err := tx.Select("conv_id", "seq", "msg_id").Where("(conv_id, seq) IN ?", pairs).Find(&rows).Error; err != nil {
		return nil, repoError("核对重复消息", err)
	}
	out := make(map[seqKey]int64, len(rows))
	for _, row := range rows {
		out[seqKey{row.ConvID, row.Seq}] = row.MsgID
	}
	return out, nil
}

func (m *MySQLRepoImpl) GetMaxSeq(ctx context.Context, convID string) (int64, error) {
	var seq int64
	err := m.db.WithContext(ctx).Model(&messageRecord{}).Select("COALESCE(MAX(seq), 0)").Where("conv_id = ?", convID).Scan(&seq).Error
	return seq, repoError("查询会话最大 seq", err)
}

func (m *MySQLRepoImpl) GetMessagesBySeqRange(ctx context.Context, convID string, fromSeq, toSeq int64, limit int) ([]model.Message, error) {
	return findMessages(m.db.WithContext(ctx).Where("conv_id = ? AND seq BETWEEN ? AND ?", convID, fromSeq, toSeq).Order("seq ASC").Limit(limit), "按 seq 区间查询消息")
}

func (m *MySQLRepoImpl) GetMessagesBefore(ctx context.Context, userID int64, convID string, beforeSeq, minSeq int64, limit int) ([]model.Message, error) {
	query := m.db.WithContext(ctx).Model(&messageRecord{}).
		Where("conv_id = ? AND seq < ? AND seq >= ?", convID, beforeSeq, minSeq).
		Where("NOT EXISTS (SELECT 1 FROM user_deleted_messages d WHERE d.user_id = ? AND d.conv_id = messages.conv_id AND d.seq = messages.seq)", userID)
	return findMessages(query.Order("seq DESC").Limit(limit), "分页查询历史消息")
}

func (m *MySQLRepoImpl) GetHistoryState(ctx context.Context, userID int64, convID string, convType int) (*HistoryState, error) {
	query := m.db.WithContext(ctx).Table("user_conversations AS uc").Where("uc.user_id = ? AND uc.conv_id = ?", userID, convID)
	if convType == model.ConvTypeGroup {
		query = query.Select("uc.min_seq, COALESCE(c.history_version, 0) AS version").
			Joins("LEFT JOIN conversations c ON c.conv_id = uc.conv_id")
	} else {
		query = query.Select("uc.min_seq")
	}
	return findOne[HistoryState](query, "查询历史消息权限和版本")
}

func (m *MySQLRepoImpl) GetGroupHistoryWindow(ctx context.Context, convID string, version int64, limit int) ([]model.Message, error) {
	// 版本与消息在同一条 SELECT 的快照中读取，避免并发落库时给窗口标错版本。
	query := m.db.WithContext(ctx).Where("conv_id = ?", convID).
		Where("EXISTS (SELECT 1 FROM conversations WHERE conv_id = ? AND history_version = ?)", convID, version).
		Order("seq DESC").Limit(limit)
	return findMessages(query, "查询群历史窗口")
}

// ReserveSeqRange 只在换段时访问数据库；提交结果不确定时调用方放弃该段，不能猜测或复用。
func (m *MySQLRepoImpl) ReserveSeqRange(ctx context.Context, convID string, convType int, size, floor int64) (start, end int64, err error) {
	if size <= 0 || floor < 0 {
		return 0, 0, fmt.Errorf("无效的 seq 号段参数")
	}
	err = m.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 重复行用无操作 UPDATE 获取排他锁，避免 INSERT IGNORE 的共享锁升级死锁。
		row := conversationRecord{ConvID: convID, ConvType: convType}
		if err := tx.Select("ConvID", "ConvType").Clauses(clause.OnConflict{
			DoUpdates: clause.Assignments(map[string]any{"conv_id": convID}),
		}).Create(&row).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("conv_id = ?", convID).Take(&row).Error; err != nil {
			return err
		}
		base := max(row.SeqAllocMax, row.MaxSeq, floor)
		if base > math.MaxInt64-size {
			return fmt.Errorf("会话 seq 已耗尽")
		}
		start, end = base+1, base+size
		return tx.Model(&conversationRecord{}).Where("conv_id = ?", convID).Update("seq_alloc_max", end).Error
	})
	if err != nil {
		return 0, 0, repoError("预留 seq 号段", err)
	}
	return start, end, nil
}

func (m *MySQLRepoImpl) GetMessage(ctx context.Context, convID string, seq int64) (*model.Message, error) {
	row, err := findOne[messageRecord](m.db.WithContext(ctx).Where("conv_id = ? AND seq = ?", convID, seq), "查询消息")
	if err != nil || row == nil {
		return nil, err
	}
	msg := row.message()
	return &msg, nil
}

// SearchMessages 保留 ngram 全文搜索、可见下界和个人删除过滤。
func (m *MySQLRepoImpl) SearchMessages(ctx context.Context, userID int64, keyword, convID string, limit int) ([]model.Message, error) {
	phrase := `"` + strings.NewReplacer(`"`, " ", `\`, " ").Replace(keyword) + `"`
	query := m.db.WithContext(ctx).Table("messages AS m").Select("m.*").
		Joins("JOIN user_conversations uc ON uc.conv_id = m.conv_id AND uc.user_id = ?", userID).
		Where("MATCH(m.content) AGAINST (? IN BOOLEAN MODE)", phrase).
		Where("m.msg_type = ? AND m.status = ? AND m.seq >= uc.min_seq", model.MsgTypeText, model.MsgStatusNormal).
		Where("NOT EXISTS (SELECT 1 FROM user_deleted_messages d WHERE d.user_id = uc.user_id AND d.conv_id = m.conv_id AND d.seq = m.seq)")
	if convID != "" {
		query = query.Where("m.conv_id = ?", convID)
	}
	return findMessages(query.Order("m.created_at DESC").Limit(limit), "搜索消息")
}

// ── 会话 ──
func (m *MySQLRepoImpl) EnsureConversation(ctx context.Context, convID string, convType int) error {
	row := conversationRecord{ConvID: convID, ConvType: convType}
	return repoError("创建会话", m.db.WithContext(ctx).Select("ConvID", "ConvType").Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&row).Error)
}

func (m *MySQLRepoImpl) GetConversations(ctx context.Context, convIDs []string) (map[string]model.Conversation, error) {
	out := make(map[string]model.Conversation, len(convIDs))
	if len(convIDs) == 0 {
		return out, nil
	}
	var rows []conversationRecord
	if err := m.db.WithContext(ctx).Where("conv_id IN ?", convIDs).Find(&rows).Error; err != nil {
		return nil, repoError("批量查询会话", err)
	}
	for _, row := range rows {
		conv := model.Conversation{ConvID: row.ConvID, ConvType: row.ConvType, MaxSeq: row.MaxSeq, LastMsgID: row.LastMsgID, LastMsgBrief: row.LastMsgBrief, LastSenderID: row.LastSenderID}
		if row.LastMsgAt != nil {
			conv.LastMsgAt = row.LastMsgAt.UnixMilli()
		}
		out[row.ConvID] = conv
	}
	return out, nil
}

func (m *MySQLRepoImpl) EnsureUserConversations(ctx context.Context, convID string, userIDs []int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	rows := make([]model.UserConversation, len(userIDs))
	for i, id := range userIDs {
		rows[i] = model.UserConversation{UserID: id, ConvID: convID}
	}
	return repoError("建立用户会话关系", m.db.WithContext(ctx).Clauses(clause.OnConflict{DoUpdates: clause.Assignments(map[string]any{"hidden": false})}).Create(&rows).Error)
}

func (m *MySQLRepoImpl) JoinConversation(ctx context.Context, convID string, userIDs []int64, minSeq int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	if minSeq < 1 {
		minSeq = 1
	}
	rows := make([]model.UserConversation, len(userIDs))
	for i, id := range userIDs {
		rows[i] = model.UserConversation{UserID: id, ConvID: convID, MinSeq: minSeq, ReadSeq: minSeq - 1}
	}
	return repoError("建立入群会话关系", m.db.WithContext(ctx).Clauses(clause.OnConflict{DoUpdates: clause.Assignments(map[string]any{"hidden": false})}).Create(&rows).Error)
}

func (m *MySQLRepoImpl) DeleteUserConversation(ctx context.Context, userID int64, convID string) error {
	return repoError("删除用户会话关系", m.db.WithContext(ctx).Where("user_id = ? AND conv_id = ?", userID, convID).Delete(&model.UserConversation{}).Error)
}

func (m *MySQLRepoImpl) ListUserConversations(ctx context.Context, userID int64) ([]model.UserConversation, error) {
	out := make([]model.UserConversation, 0)
	err := m.db.WithContext(ctx).Where("user_id = ?", userID).Find(&out).Error
	return out, repoError("查询用户会话列表", err)
}

func (m *MySQLRepoImpl) GetUserConversation(ctx context.Context, userID int64, convID string) (*model.UserConversation, error) {
	return findOne[model.UserConversation](m.db.WithContext(ctx).Where("user_id = ? AND conv_id = ?", userID, convID), "查询用户会话")
}

func (m *MySQLRepoImpl) UpdateReadSeq(ctx context.Context, userID int64, convID string, seq int64) error {
	return repoError("更新已读位置", m.db.WithContext(ctx).Model(&model.UserConversation{}).Where("user_id = ? AND conv_id = ?", userID, convID).Update("read_seq", gorm.Expr("GREATEST(read_seq, ?)", seq)).Error)
}

func (m *MySQLRepoImpl) UpdateMinSeq(ctx context.Context, userID int64, convID string, minSeq int64, hide bool) error {
	values := map[string]any{"min_seq": gorm.Expr("GREATEST(min_seq, ?)", minSeq), "read_seq": gorm.Expr("GREATEST(read_seq, ? - 1)", minSeq)}
	if hide {
		values["hidden"] = true
	}
	return repoError("更新可见下界", m.db.WithContext(ctx).Model(&model.UserConversation{}).Where("user_id = ? AND conv_id = ?", userID, convID).Updates(values).Error)
}

func (m *MySQLRepoImpl) CountUnread(ctx context.Context, userID int64, ranges []SeqRange) (map[string]int64, error) {
	out := make(map[string]int64, len(ranges))
	if len(ranges) == 0 {
		return out, nil
	}
	conditions := make([]string, len(ranges))
	args := make([]any, 0, len(ranges)*3)
	for i, r := range ranges {
		conditions[i] = "(conv_id = ? AND seq BETWEEN ? AND ?)"
		args = append(args, r.ConvID, r.From, r.To)
	}
	var rows []struct {
		ConvID string
		Count  int64
	}
	err := m.db.WithContext(ctx).Model(&messageRecord{}).Select("conv_id, COUNT(*) AS count").
		Where("("+strings.Join(conditions, " OR ")+")", args...).
		Where("msg_type <> ? AND status = ? AND sender_id <> ?", model.MsgTypeRevoked, model.MsgStatusNormal, userID).
		Where("NOT EXISTS (SELECT 1 FROM user_deleted_messages d WHERE d.user_id = ? AND d.conv_id = messages.conv_id AND d.seq = messages.seq)", userID).
		Group("conv_id").Scan(&rows).Error
	if err != nil {
		return nil, repoError("统计实际未读消息", err)
	}
	for _, row := range rows {
		out[row.ConvID] = row.Count
	}
	return out, nil
}

// ── 会话展示资料 ──
func (m *MySQLRepoImpl) GetUserBriefs(ctx context.Context, userIDs []int64) (map[int64]UserBrief, error) {
	out := make(map[int64]UserBrief, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	var users []model.User
	err := m.db.WithContext(ctx).Select("id", "username", "nickname", "avatar_url").Where("id IN ?", userIDs).Find(&users).Error
	if err != nil {
		return nil, repoError("批量查询用户资料", err)
	}
	for _, user := range users {
		name := user.Nickname
		if name == "" {
			name = user.Username
		}
		out[user.ID] = UserBrief{Name: name, AvatarURL: user.AvatarURL}
	}
	return out, nil
}

func (m *MySQLRepoImpl) GetGroupNames(ctx context.Context, groupIDs []int64) (map[int64]string, error) {
	out := make(map[int64]string, len(groupIDs))
	if len(groupIDs) == 0 {
		return out, nil
	}
	var groups []model.Group
	if err := m.db.WithContext(ctx).Select("id", "name").Where("id IN ?", groupIDs).Find(&groups).Error; err != nil {
		return nil, repoError("批量查询群名称", err)
	}
	for _, group := range groups {
		out[group.ID] = group.Name
	}
	return out, nil
}

// ── 单条删除 ──
func (m *MySQLRepoImpl) AddUserDeletedMessage(ctx context.Context, userID int64, convID string, seq int64) error {
	row := deletedMessageRecord{UserID: userID, ConvID: convID, Seq: seq}
	return repoError("记录删除消息", m.db.WithContext(ctx).Clauses(clause.Insert{Modifier: "IGNORE"}).Create(&row).Error)
}

func (m *MySQLRepoImpl) GetUserDeletedSeqs(ctx context.Context, userID int64, convID string, fromSeq, toSeq int64) (map[int64]struct{}, error) {
	var seqs []int64
	err := m.db.WithContext(ctx).Model(&deletedMessageRecord{}).Where("user_id = ? AND conv_id = ? AND seq BETWEEN ? AND ?", userID, convID, fromSeq, toSeq).Pluck("seq", &seqs).Error
	if err != nil {
		return nil, repoError("查询已删除消息", err)
	}
	out := make(map[int64]struct{}, len(seqs))
	for _, seq := range seqs {
		out[seq] = struct{}{}
	}
	return out, nil
}

func (m *MySQLRepoImpl) ListUserGroupIDs(ctx context.Context, userID int64) ([]int64, error) {
	groups := make([]int64, 0)
	err := m.db.WithContext(ctx).Model(&model.GroupMember{}).Where("user_id = ?", userID).Pluck("group_id", &groups).Error
	return groups, repoError("查询用户群列表", err)
}
