package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/goim/goim/internal/model"
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
	// GetMaxSeq 返回会话已落库的最大 seq，用于 Redis seq 计数器丢失后的校正。
	GetMaxSeq(ctx context.Context, convID string) (int64, error)
	// GetMessagesBySeqRange 按 seq 升序返回 [fromSeq, toSeq] 内的消息，最多 limit 条。
	GetMessagesBySeqRange(ctx context.Context, convID string, fromSeq, toSeq int64, limit int) ([]model.Message, error)
	// GetMessagesBefore 按 seq 降序返回 seq < beforeSeq 且 seq >= minSeq 的消息，最多 limit 条。
	GetMessagesBefore(ctx context.Context, convID string, beforeSeq, minSeq int64, limit int) ([]model.Message, error)
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
	// CountNonUnread 统计每个区间内"不应计入未读"的消息数：撤回通知、已被撤回的消息、用户自己发的消息。
	// 未读数 = maxSeq - readSeq - 该计数。
	CountNonUnread(ctx context.Context, userID int64, ranges []SeqRange) (map[string]int64, error)

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

// UserBrief 是会话列表展示用的用户资料。
type UserBrief struct {
	Name      string // 昵称为空时取用户名
	AvatarURL string
}

func int64Placeholders(ids []int64) (string, []interface{}) {
	ph := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}
	return strings.Join(ph, ","), args
}

const messageColumns = `conv_id, seq, msg_id, conv_type, sender_id, msg_type, content, status, client_msg_id, created_at`

// scanMessage 把一行 messageColumns 扫描为 model.Message。
func scanMessage(scanner interface{ Scan(...any) error }) (model.Message, error) {
	var (
		msg       model.Message
		clientID  sql.NullString
		createdAt time.Time
	)
	if err := scanner.Scan(&msg.ConvID, &msg.Seq, &msg.MsgID, &msg.ConvType, &msg.SenderID,
		&msg.MsgType, &msg.Content, &msg.Status, &clientID, &createdAt); err != nil {
		return model.Message{}, err
	}
	msg.ClientMsgID = clientID.String
	msg.Timestamp = createdAt.UnixMilli()
	return msg, nil
}

func scanMessages(rows *sql.Rows) ([]model.Message, error) {
	defer rows.Close()
	out := make([]model.Message, 0)
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描消息: %w", err)
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历消息: %w", err)
	}
	return out, nil
}

func nullableString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// ── 消息 ──

func (m *MySQLRepoImpl) PersistMessages(ctx context.Context, msgs []model.Message) ([]model.Message, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("开启消息落库事务: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1. 批量 INSERT IGNORE
	placeholders := make([]string, len(msgs))
	args := make([]interface{}, 0, len(msgs)*10)
	for i, msg := range msgs {
		placeholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
		args = append(args, msg.ConvID, msg.Seq, msg.MsgID, msg.ConvType, msg.SenderID,
			msg.MsgType, msg.Content, msg.Status, nullableString(msg.ClientMsgID), time.UnixMilli(msg.Timestamp))
	}
	insert := fmt.Sprintf(`INSERT IGNORE INTO messages (%s) VALUES %s`, messageColumns, strings.Join(placeholders, ","))
	res, err := tx.ExecContext(ctx, insert, args...)
	if err != nil {
		return nil, fmt.Errorf("批量写入消息: %w", err)
	}

	// 2. 有行被忽略时，逐条核对：同 msgId 是重复消费，不同 msgId 是冲突
	accepted := msgs
	var conflicts []model.Message
	if affected, _ := res.RowsAffected(); affected < int64(len(msgs)) {
		stored, err := m.lookupMsgIDs(ctx, tx, msgs)
		if err != nil {
			return nil, err
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

	// 3. 撤回通知：把目标消息置为已撤回（与通知本身同一事务，且按分区顺序在原消息之后处理）
	for _, msg := range accepted {
		if msg.MsgType != model.MsgTypeRevoked {
			continue
		}
		var notice model.RevokeNotice
		if err := json.Unmarshal([]byte(msg.Content), &notice); err != nil || notice.TargetSeq <= 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE messages SET status = ? WHERE conv_id = ? AND seq = ? AND msg_id = ?`,
			model.MsgStatusRevoked, msg.ConvID, notice.TargetSeq, notice.TargetMsgID); err != nil {
			return nil, fmt.Errorf("标记消息撤回: %w", err)
		}
	}

	// 4. 推进 conversations：每个会话取本批 seq 最大的一条
	latest := make(map[string]model.Message)
	for _, msg := range accepted {
		if cur, ok := latest[msg.ConvID]; !ok || msg.Seq > cur.Seq {
			latest[msg.ConvID] = msg
		}
	}
	for _, msg := range latest {
		// ON DUPLICATE KEY UPDATE 的赋值按书写顺序执行，max_seq 必须放在最后，
		// 前面的 IF 才能拿到更新前的值做比较。
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO conversations (conv_id, conv_type, max_seq, last_msg_id, last_msg_brief, last_sender_id, last_msg_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON DUPLICATE KEY UPDATE
				last_msg_id    = IF(VALUES(max_seq) > max_seq, VALUES(last_msg_id), last_msg_id),
				last_msg_brief = IF(VALUES(max_seq) > max_seq, VALUES(last_msg_brief), last_msg_brief),
				last_sender_id = IF(VALUES(max_seq) > max_seq, VALUES(last_sender_id), last_sender_id),
				last_msg_at    = IF(VALUES(max_seq) > max_seq, VALUES(last_msg_at), last_msg_at),
				max_seq        = GREATEST(max_seq, VALUES(max_seq))`,
			msg.ConvID, msg.ConvType, msg.Seq, msg.MsgID, model.MsgBrief(msg.MsgType, msg.Content),
			msg.SenderID, time.UnixMilli(msg.Timestamp)); err != nil {
			return nil, fmt.Errorf("更新会话最后一条消息: %w", err)
		}
		// 私聊会话关系自愈：确保双方都有 user_conversations 行，并让被隐藏的会话在来新消息时重新出现。
		if msg.ConvType == model.ConvTypePrivate {
			if p, err := model.ParseConvID(msg.ConvID); err == nil {
				if _, err := tx.ExecContext(ctx,
					`INSERT INTO user_conversations (user_id, conv_id) VALUES (?, ?), (?, ?)
					 ON DUPLICATE KEY UPDATE hidden = 0`,
					p.UserA, msg.ConvID, p.UserB, msg.ConvID); err != nil {
					return nil, fmt.Errorf("维护私聊会话关系: %w", err)
				}
			}
		}
	}

	// 5. 发送者自己发的消息视为已读：按 (发送者, 会话) 取本批最大 seq 推进 read_seq
	type senderConv struct {
		sender int64
		convID string
	}
	senderMax := make(map[senderConv]int64)
	for _, msg := range accepted {
		k := senderConv{msg.SenderID, msg.ConvID}
		if msg.Seq > senderMax[k] {
			senderMax[k] = msg.Seq
		}
	}
	for k, seq := range senderMax {
		if _, err := tx.ExecContext(ctx,
			`UPDATE user_conversations SET read_seq = GREATEST(read_seq, ?) WHERE user_id = ? AND conv_id = ?`,
			seq, k.sender, k.convID); err != nil {
			return nil, fmt.Errorf("推进发送者已读位置: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("提交消息落库事务: %w", err)
	}
	return conflicts, nil
}

type seqKey struct {
	convID string
	seq    int64
}

// lookupMsgIDs 查询一批 (convId, seq) 当前落库的 msgId。
func (m *MySQLRepoImpl) lookupMsgIDs(ctx context.Context, tx *sql.Tx, msgs []model.Message) (map[seqKey]int64, error) {
	placeholders := make([]string, len(msgs))
	args := make([]interface{}, 0, len(msgs)*2)
	for i, msg := range msgs {
		placeholders[i] = "(?, ?)"
		args = append(args, msg.ConvID, msg.Seq)
	}
	query := fmt.Sprintf(`SELECT conv_id, seq, msg_id FROM messages WHERE (conv_id, seq) IN (%s)`, strings.Join(placeholders, ","))
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("核对重复消息: %w", err)
	}
	defer rows.Close()
	out := make(map[seqKey]int64, len(msgs))
	for rows.Next() {
		var (
			k  seqKey
			id int64
		)
		if err := rows.Scan(&k.convID, &k.seq, &id); err != nil {
			return nil, fmt.Errorf("扫描重复消息: %w", err)
		}
		out[k] = id
	}
	return out, rows.Err()
}

func (m *MySQLRepoImpl) GetMaxSeq(ctx context.Context, convID string) (int64, error) {
	var seq int64
	if err := m.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM messages WHERE conv_id = ?`, convID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("查询会话最大 seq: %w", err)
	}
	return seq, nil
}

func (m *MySQLRepoImpl) GetMessagesBySeqRange(ctx context.Context, convID string, fromSeq, toSeq int64, limit int) ([]model.Message, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE conv_id = ? AND seq BETWEEN ? AND ?
		 ORDER BY seq ASC LIMIT ?`,
		convID, fromSeq, toSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("按 seq 区间查询消息: %w", err)
	}
	return scanMessages(rows)
}

func (m *MySQLRepoImpl) GetMessagesBefore(ctx context.Context, convID string, beforeSeq, minSeq int64, limit int) ([]model.Message, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT `+messageColumns+` FROM messages
		 WHERE conv_id = ? AND seq < ? AND seq >= ?
		 ORDER BY seq DESC LIMIT ?`,
		convID, beforeSeq, minSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("分页查询历史消息: %w", err)
	}
	return scanMessages(rows)
}

func (m *MySQLRepoImpl) GetMessage(ctx context.Context, convID string, seq int64) (*model.Message, error) {
	row := m.db.QueryRowContext(ctx,
		`SELECT `+messageColumns+` FROM messages WHERE conv_id = ? AND seq = ?`, convID, seq)
	msg, err := scanMessage(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询消息: %w", err)
	}
	return &msg, nil
}

// SearchMessages 使用 ngram 全文索引检索。关键词按短语匹配，避免布尔模式的操作符被用户输入注入。
// 只返回：用户仍在其中的会话、未撤回的文本消息、在可见下界之后且未被用户删除的消息。
func (m *MySQLRepoImpl) SearchMessages(ctx context.Context, userID int64, keyword, convID string, limit int) ([]model.Message, error) {
	phrase := `"` + strings.NewReplacer(`"`, " ", `\`, " ").Replace(keyword) + `"`
	query := `SELECT m.conv_id, m.seq, m.msg_id, m.conv_type, m.sender_id, m.msg_type, m.content, m.status, m.client_msg_id, m.created_at
		FROM messages m
		JOIN user_conversations uc ON uc.conv_id = m.conv_id AND uc.user_id = ?
		WHERE MATCH(m.content) AGAINST (? IN BOOLEAN MODE)
		  AND m.msg_type = ? AND m.status = ? AND m.seq >= uc.min_seq
		  AND NOT EXISTS (
		      SELECT 1 FROM user_deleted_messages d
		      WHERE d.user_id = uc.user_id AND d.conv_id = m.conv_id AND d.seq = m.seq)`
	args := []interface{}{userID, phrase, model.MsgTypeText, model.MsgStatusNormal}
	if convID != "" {
		query += ` AND m.conv_id = ?`
		args = append(args, convID)
	}
	query += ` ORDER BY m.created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("搜索消息: %w", err)
	}
	return scanMessages(rows)
}

// ── 会话 ──

func (m *MySQLRepoImpl) EnsureConversation(ctx context.Context, convID string, convType int) error {
	if _, err := m.db.ExecContext(ctx,
		`INSERT IGNORE INTO conversations (conv_id, conv_type) VALUES (?, ?)`, convID, convType); err != nil {
		return fmt.Errorf("创建会话: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetConversations(ctx context.Context, convIDs []string) (map[string]model.Conversation, error) {
	out := make(map[string]model.Conversation, len(convIDs))
	if len(convIDs) == 0 {
		return out, nil
	}
	placeholders := make([]string, len(convIDs))
	args := make([]interface{}, len(convIDs))
	for i, id := range convIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	query := fmt.Sprintf(`SELECT conv_id, conv_type, max_seq, last_msg_id, last_msg_brief, last_sender_id, last_msg_at
		FROM conversations WHERE conv_id IN (%s)`, strings.Join(placeholders, ","))
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("批量查询会话: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			c      model.Conversation
			lastAt sql.NullTime
		)
		if err := rows.Scan(&c.ConvID, &c.ConvType, &c.MaxSeq, &c.LastMsgID, &c.LastMsgBrief, &c.LastSenderID, &lastAt); err != nil {
			return nil, fmt.Errorf("扫描会话: %w", err)
		}
		if lastAt.Valid {
			c.LastMsgAt = lastAt.Time.UnixMilli()
		}
		out[c.ConvID] = c
	}
	return out, rows.Err()
}

func (m *MySQLRepoImpl) EnsureUserConversations(ctx context.Context, convID string, userIDs []int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	placeholders := make([]string, len(userIDs))
	args := make([]interface{}, 0, len(userIDs)*2)
	for i, uid := range userIDs {
		placeholders[i] = "(?, ?)"
		args = append(args, uid, convID)
	}
	query := fmt.Sprintf(`INSERT INTO user_conversations (user_id, conv_id) VALUES %s
		ON DUPLICATE KEY UPDATE hidden = 0`, strings.Join(placeholders, ","))
	if _, err := m.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("建立用户会话关系: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) JoinConversation(ctx context.Context, convID string, userIDs []int64, minSeq int64) error {
	if len(userIDs) == 0 {
		return nil
	}
	if minSeq < 1 {
		minSeq = 1
	}
	placeholders := make([]string, len(userIDs))
	args := make([]interface{}, 0, len(userIDs)*4)
	for i, uid := range userIDs {
		placeholders[i] = "(?, ?, ?, ?)"
		args = append(args, uid, convID, minSeq, minSeq-1)
	}
	query := fmt.Sprintf(`INSERT INTO user_conversations (user_id, conv_id, min_seq, read_seq) VALUES %s
		ON DUPLICATE KEY UPDATE hidden = 0`, strings.Join(placeholders, ","))
	if _, err := m.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("建立入群会话关系: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) DeleteUserConversation(ctx context.Context, userID int64, convID string) error {
	if _, err := m.db.ExecContext(ctx,
		`DELETE FROM user_conversations WHERE user_id = ? AND conv_id = ?`, userID, convID); err != nil {
		return fmt.Errorf("删除用户会话关系: %w", err)
	}
	return nil
}

const userConvColumns = `user_id, conv_id, read_seq, min_seq, pinned, muted, hidden`

func scanUserConversation(scanner interface{ Scan(...any) error }) (model.UserConversation, error) {
	var uc model.UserConversation
	err := scanner.Scan(&uc.UserID, &uc.ConvID, &uc.ReadSeq, &uc.MinSeq, &uc.Pinned, &uc.Muted, &uc.Hidden)
	return uc, err
}

func (m *MySQLRepoImpl) ListUserConversations(ctx context.Context, userID int64) ([]model.UserConversation, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT `+userConvColumns+` FROM user_conversations WHERE user_id = ?`, userID)
	if err != nil {
		return nil, fmt.Errorf("查询用户会话列表: %w", err)
	}
	defer rows.Close()
	out := make([]model.UserConversation, 0)
	for rows.Next() {
		uc, err := scanUserConversation(rows)
		if err != nil {
			return nil, fmt.Errorf("扫描用户会话: %w", err)
		}
		out = append(out, uc)
	}
	return out, rows.Err()
}

func (m *MySQLRepoImpl) GetUserConversation(ctx context.Context, userID int64, convID string) (*model.UserConversation, error) {
	row := m.db.QueryRowContext(ctx,
		`SELECT `+userConvColumns+` FROM user_conversations WHERE user_id = ? AND conv_id = ?`, userID, convID)
	uc, err := scanUserConversation(row)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("查询用户会话: %w", err)
	}
	return &uc, nil
}

func (m *MySQLRepoImpl) UpdateReadSeq(ctx context.Context, userID int64, convID string, seq int64) error {
	if _, err := m.db.ExecContext(ctx,
		`UPDATE user_conversations SET read_seq = GREATEST(read_seq, ?) WHERE user_id = ? AND conv_id = ?`,
		seq, userID, convID); err != nil {
		return fmt.Errorf("更新已读位置: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) UpdateMinSeq(ctx context.Context, userID int64, convID string, minSeq int64, hide bool) error {
	query := `UPDATE user_conversations
		SET min_seq = GREATEST(min_seq, ?), read_seq = GREATEST(read_seq, ? - 1)`
	if hide {
		query += `, hidden = 1`
	}
	query += ` WHERE user_id = ? AND conv_id = ?`
	if _, err := m.db.ExecContext(ctx, query, minSeq, minSeq, userID, convID); err != nil {
		return fmt.Errorf("更新可见下界: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) CountNonUnread(ctx context.Context, userID int64, ranges []SeqRange) (map[string]int64, error) {
	out := make(map[string]int64, len(ranges))
	if len(ranges) == 0 {
		return out, nil
	}
	conds := make([]string, len(ranges))
	args := make([]interface{}, 0, len(ranges)*3+3)
	for i, r := range ranges {
		conds[i] = "(conv_id = ? AND seq BETWEEN ? AND ?)"
		args = append(args, r.ConvID, r.From, r.To)
	}
	args = append(args, model.MsgTypeRevoked, model.MsgStatusRevoked, userID)
	// 每个条件都是主键 (conv_id, seq) 上的范围扫描
	query := fmt.Sprintf(`SELECT conv_id, COUNT(*) FROM messages
		WHERE (%s) AND (msg_type = ? OR status = ? OR sender_id = ?)
		GROUP BY conv_id`, strings.Join(conds, " OR "))
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("统计不计未读的消息: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			convID string
			n      int64
		)
		if err := rows.Scan(&convID, &n); err != nil {
			return nil, fmt.Errorf("扫描未读校正计数: %w", err)
		}
		out[convID] = n
	}
	return out, rows.Err()
}

// ── 会话展示资料 ──

func (m *MySQLRepoImpl) GetUserBriefs(ctx context.Context, userIDs []int64) (map[int64]UserBrief, error) {
	out := make(map[int64]UserBrief, len(userIDs))
	if len(userIDs) == 0 {
		return out, nil
	}
	ph, args := int64Placeholders(userIDs)
	rows, err := m.db.QueryContext(ctx,
		fmt.Sprintf(`SELECT id, username, nickname, avatar_url FROM users WHERE id IN (%s)`, ph), args...)
	if err != nil {
		return nil, fmt.Errorf("批量查询用户资料: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id                         int64
			username, nickname, avatar sql.NullString
		)
		if err := rows.Scan(&id, &username, &nickname, &avatar); err != nil {
			return nil, fmt.Errorf("扫描用户资料: %w", err)
		}
		name := nickname.String
		if name == "" {
			name = username.String
		}
		out[id] = UserBrief{Name: name, AvatarURL: avatar.String}
	}
	return out, rows.Err()
}

func (m *MySQLRepoImpl) GetGroupNames(ctx context.Context, groupIDs []int64) (map[int64]string, error) {
	out := make(map[int64]string, len(groupIDs))
	if len(groupIDs) == 0 {
		return out, nil
	}
	ph, args := int64Placeholders(groupIDs)
	rows, err := m.db.QueryContext(ctx,
		fmt.Sprintf("SELECT id, name FROM `groups` WHERE id IN (%s)", ph), args...)
	if err != nil {
		return nil, fmt.Errorf("批量查询群名称: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id   int64
			name string
		)
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("扫描群名称: %w", err)
		}
		out[id] = name
	}
	return out, rows.Err()
}

// ── 单条删除 ──

func (m *MySQLRepoImpl) AddUserDeletedMessage(ctx context.Context, userID int64, convID string, seq int64) error {
	if _, err := m.db.ExecContext(ctx,
		`INSERT IGNORE INTO user_deleted_messages (user_id, conv_id, seq) VALUES (?, ?, ?)`,
		userID, convID, seq); err != nil {
		return fmt.Errorf("记录删除消息: %w", err)
	}
	return nil
}

func (m *MySQLRepoImpl) GetUserDeletedSeqs(ctx context.Context, userID int64, convID string, fromSeq, toSeq int64) (map[int64]struct{}, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT seq FROM user_deleted_messages WHERE user_id = ? AND conv_id = ? AND seq BETWEEN ? AND ?`,
		userID, convID, fromSeq, toSeq)
	if err != nil {
		return nil, fmt.Errorf("查询已删除消息: %w", err)
	}
	defer rows.Close()
	out := make(map[int64]struct{})
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("扫描已删除消息: %w", err)
		}
		out[seq] = struct{}{}
	}
	return out, rows.Err()
}

// ── 群 ──

func (m *MySQLRepoImpl) ListUserGroupIDs(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT group_id FROM group_members WHERE user_id = ?`, userID)
	if err != nil {
		return nil, fmt.Errorf("查询用户群列表: %w", err)
	}
	defer rows.Close()
	out := make([]int64, 0)
	for rows.Next() {
		var gid int64
		if err := rows.Scan(&gid); err != nil {
			return nil, fmt.Errorf("扫描群 ID: %w", err)
		}
		out = append(out, gid)
	}
	return out, rows.Err()
}
