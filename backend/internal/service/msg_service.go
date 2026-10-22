package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/idgen"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
	"github.com/goim/goim/internal/repository"
)

// ──────────────────────────────────────────────────────
// 错误
// ──────────────────────────────────────────────────────

// 错误文案保持与 api.errorCodeMap 一致。
const (
	ErrNotFriend          = "不是好友"
	ErrBlocked            = "你已被对方拉黑"
	ErrDuplicate          = "消息正在发送中"
	ErrNotMember          = "不是该群组的成员"
	ErrMuted              = "你已被禁言"
	ErrRevokeFail         = "无法撤回此消息"
	ErrMsgNotRevocable    = "消息无法撤回（未找到或已过时效）"
	ErrMsgRevokeNotOwner  = "仅发送者可撤回消息"
	ErrMsgDeleteFailed    = "删除消息失败"
	ErrConvNotFound       = "会话不存在"
	ErrMsgInvalid         = "消息字段无效"
	ErrMsgSendFailed      = "消息发送失败，请重试"
	ErrMsgInternal        = "服务器内部错误"
)

// 消息链路错误码：WebSocket 的 error 帧与 HTTP 响应共用。
const (
	CodeMsgBadRequest   = 400
	CodeMsgInternal     = 500
	CodeMsgNotFriend    = 4001
	CodeMsgBlocked      = 4002
	CodeMsgDuplicate    = 4003
	CodeMsgNotMember    = 5001
	CodeMsgMuted        = 5002
	CodeMsgNotRevocable = 1401
	CodeMsgNotOwner     = 1402
	CodeMsgDeleteFailed = 1403
	CodeMsgConvNotFound = 1404
)

// MsgError 是消息服务返回给调用方的业务错误，携带客户端错误码。
type MsgError struct {
	Code    int
	Message string
}

func (e *MsgError) Error() string { return e.Message }

func msgErr(code int, message string) *MsgError { return &MsgError{Code: code, Message: message} }

var (
	errMsgInvalid      = msgErr(CodeMsgBadRequest, ErrMsgInvalid)
	errNotFriend       = msgErr(CodeMsgNotFriend, ErrNotFriend)
	errBlocked         = msgErr(CodeMsgBlocked, ErrBlocked)
	errDuplicate       = msgErr(CodeMsgDuplicate, ErrDuplicate)
	errNotMember       = msgErr(CodeMsgNotMember, ErrNotMember)
	errMuted           = msgErr(CodeMsgMuted, ErrMuted)
	errNotRevocable    = msgErr(CodeMsgNotRevocable, ErrMsgNotRevocable)
	errRevokeNotOwner  = msgErr(CodeMsgNotOwner, ErrMsgRevokeNotOwner)
	errConvNotFound    = msgErr(CodeMsgConvNotFound, ErrConvNotFound)
	errMsgSendFailed   = msgErr(CodeMsgInternal, ErrMsgSendFailed)
	errMsgInternal     = msgErr(CodeMsgInternal, ErrMsgInternal)
	errMsgDeleteFailed = msgErr(CodeMsgDeleteFailed, ErrMsgDeleteFailed)
)

// AsMsgError 把任意错误转换为 MsgError；非业务错误统一视为内部错误。
func AsMsgError(err error) *MsgError {
	var me *MsgError
	if errors.As(err, &me) {
		return me
	}
	return errMsgInternal
}

// ──────────────────────────────────────────────────────
// MsgService
// ──────────────────────────────────────────────────────

const (
	revokeWindow      = 2 * time.Minute
	pullMaxBatch      = 100
	historyMaxLimit   = 100
	searchMaxLimit    = 100
	maxClientMsgIDLen = 128
	maxContentBytes   = 1 << 20
	msgOpTimeout      = 5 * time.Second
	convLockShards    = 256
	// unreadExactLimit 以内的未读数会扣除撤回通知、已撤回消息和自己发的消息；
	// 超过时直接用 maxSeq-readSeq（前端展示 99+，精确值意义不大，也避免大范围扫描）。
	unreadExactLimit = 1000
)

// convLocks 按 convId 分片的互斥锁，保证同一会话"分配 seq → 写 Kafka"串行执行，
// 使 seq 顺序与 Kafka 分区内顺序一致：消费者处理到 seq=N 时，所有 seq<N 的消息都已处理过。
// 只在单机内有效；多机部署需要改为在消费侧分配 seq 或使用分布式锁。
type convLocks [convLockShards]chan struct{}

func newConvLocks() *convLocks {
	var l convLocks
	for i := range l {
		l[i] = make(chan struct{}, 1)
	}
	return &l
}

func (l *convLocks) lock(ctx context.Context, convID string) (func(), error) {
	h := fnv.New32a()
	_, _ = h.Write([]byte(convID))
	ch := l[h.Sum32()%convLockShards]
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// MsgService 实现消息链路的全部业务：
//
//	发送：校验 → 关系检查 → 去重 → 会话锁 → 分配 seq → 雪花 msgId → 写 Kafka(acks=all) → 解锁 → serverAck
//	同步：convSyncReq 返回会话列表（maxSeq/readSeq/unread），客户端按 seq 空缺发 pullMsgs 补拉
//	已读：readAck{convId, seq}，只前进
//	撤回：校验后作为一条 msgType=6 的通知消息走正常发送链路
//	投递：deliver 消费者调用 DeliverMessage，更新热缓存后推送给在线成员
type MsgService struct {
	mysql     repository.MySQLRepo
	store     repository.MessageStore
	cache     repository.MessageCache
	relations repository.RelationCache
	mq        repository.MQRepo
	ids       *idgen.Snowflake
	cm        *conn.ConnectionManager
	logger    *zap.Logger
	now       func() time.Time
	locks     *convLocks
}

// NewMsgService 创建 MsgService。
func NewMsgService(
	mysql repository.MySQLRepo,
	store repository.MessageStore,
	cache repository.MessageCache,
	relations repository.RelationCache,
	mq repository.MQRepo,
	ids *idgen.Snowflake,
	cm *conn.ConnectionManager,
	logger *zap.Logger,
) *MsgService {
	return &MsgService{
		mysql:     mysql,
		store:     store,
		cache:     cache,
		relations: relations,
		mq:        mq,
		ids:       ids,
		cm:        cm,
		logger:    logger,
		now:       time.Now,
		locks:     newConvLocks(),
	}
}

// ──────────────────────────────────────────────────────
// WebSocket 入口
// ──────────────────────────────────────────────────────

// HandleSendMessage 处理客户端发送的聊天消息，成功后回 serverAck，失败回 error（带 clientMsgId）。
func (s *MsgService) HandleSendMessage(userID int64, data []byte) {
	var req model.SendMessage
	if err := json.Unmarshal(data, &req); err != nil {
		s.sendError(userID, msgErr(CodeMsgBadRequest, "消息格式无效"), "")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), msgOpTimeout)
	defer cancel()

	ack, err := s.SendMessage(ctx, userID, &req)
	if err != nil {
		s.sendError(userID, err, req.ClientMsgID)
		return
	}
	s.pushToUser(userID, protocol.TypeServerAck, ack)
}

// HandleReadAck 处理已读上报。
func (s *MsgService) HandleReadAck(userID int64, data []byte) {
	var req model.ReadAck
	if err := json.Unmarshal(data, &req); err != nil {
		s.sendError(userID, msgErr(CodeMsgBadRequest, "readAck 格式无效"), "")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), msgOpTimeout)
	defer cancel()
	if err := s.MarkRead(ctx, userID, req.ConvID, req.Seq); err != nil {
		s.sendError(userID, err, "")
	}
}

// HandleConvSyncReq 返回用户的全部会话视图，客户端据此对比本地 seq 决定补拉哪些区间。
func (s *MsgService) HandleConvSyncReq(userID int64, _ []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), msgOpTimeout)
	defer cancel()
	views, err := s.ListConversations(ctx, userID)
	if err != nil {
		s.sendError(userID, err, "")
		return
	}
	s.pushToUser(userID, protocol.TypeConvSync, &model.ConvSync{Conversations: views})
}

// HandlePullMsgs 按 seq 区间补拉消息。
func (s *MsgService) HandlePullMsgs(userID int64, data []byte) {
	var req model.PullMsgsReq
	if err := json.Unmarshal(data, &req); err != nil {
		s.sendError(userID, msgErr(CodeMsgBadRequest, "pullMsgs 格式无效"), "")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), msgOpTimeout)
	defer cancel()
	resp, err := s.PullMessages(ctx, userID, req.ConvID, req.FromSeq, req.ToSeq)
	if err != nil {
		s.sendError(userID, err, "")
		return
	}
	s.pushToUser(userID, protocol.TypePullMsgsResp, resp)
}

// HandleRevokeMsg 处理撤回请求；撤回通知作为普通消息投递，发起方收到 serverAck 即表示撤回已提交。
func (s *MsgService) HandleRevokeMsg(userID int64, data []byte) {
	var req model.RevokeMsgReq
	if err := json.Unmarshal(data, &req); err != nil {
		s.sendError(userID, msgErr(CodeMsgBadRequest, "revokeMsg 格式无效"), "")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), msgOpTimeout)
	defer cancel()
	ack, err := s.RevokeMessage(ctx, userID, req.ConvID, req.Seq)
	if err != nil {
		s.sendError(userID, err, revokeClientMsgID(req.ConvID, req.Seq))
		return
	}
	s.pushToUser(userID, protocol.TypeServerAck, ack)
}

// ──────────────────────────────────────────────────────
// 发送
// ──────────────────────────────────────────────────────

// SendMessage 校验并提交一条用户消息。返回 nil 错误即代表消息已写入 Kafka（提交点）。
func (s *MsgService) SendMessage(ctx context.Context, senderID int64, req *model.SendMessage) (*model.ServerAck, error) {
	if strings.TrimSpace(req.ClientMsgID) == "" || len(req.ClientMsgID) > maxClientMsgIDLen ||
		req.ToID <= 0 || len(req.Content) > maxContentBytes || strings.TrimSpace(req.Content) == "" {
		return nil, errMsgInvalid
	}
	// 系统消息和撤回通知只能由服务端产生
	switch req.MsgType {
	case model.MsgTypeText, model.MsgTypeImage, model.MsgTypeVideo, model.MsgTypeFile:
	default:
		return nil, errMsgInvalid
	}

	convID, err := s.authorizeSend(ctx, senderID, req)
	if err != nil {
		return nil, err
	}
	return s.submit(ctx, &model.Message{
		ConvID:      convID,
		ConvType:    req.ConvType,
		SenderID:    senderID,
		MsgType:     req.MsgType,
		Content:     req.Content,
		ClientMsgID: req.ClientMsgID,
	})
}

// authorizeSend 检查发送权限并返回会话 ID。
// 私聊：双方是好友且对方没有拉黑自己；群聊：是群成员且未被禁言。
func (s *MsgService) authorizeSend(ctx context.Context, senderID int64, req *model.SendMessage) (string, error) {
	switch req.ConvType {
	case model.ConvTypePrivate:
		if req.ToID == senderID {
			return "", errMsgInvalid
		}
		ok, err := s.relations.IsFriend(ctx, senderID, req.ToID)
		if err != nil {
			s.logger.Error("检查好友关系失败", zap.Error(err))
			return "", errMsgInternal
		}
		if !ok {
			return "", errNotFriend
		}
		blocked, err := s.relations.IsBlocked(ctx, req.ToID, senderID)
		if err != nil {
			s.logger.Error("检查黑名单失败", zap.Error(err))
			return "", errMsgInternal
		}
		if blocked {
			return "", errBlocked
		}
		return model.BuildConvID(model.ConvTypePrivate, senderID, req.ToID), nil

	case model.ConvTypeGroup:
		members, err := s.relations.GetGroupMembers(ctx, req.ToID)
		if err != nil {
			s.logger.Error("读取群成员失败", zap.Int64("groupID", req.ToID), zap.Error(err))
			return "", errMsgInternal
		}
		for _, m := range members {
			if m.UserID != senderID {
				continue
			}
			if m.MutedUntil != nil && m.MutedUntil.After(s.now()) {
				return "", errMuted
			}
			return model.BuildConvID(model.ConvTypeGroup, req.ToID, 0), nil
		}
		return "", errNotMember

	default:
		return "", errMsgInvalid
	}
}

// submit 是所有消息（用户消息、撤回通知）共用的提交流程：
// 去重占位 → 会话锁 → 分配 seq → 生成 msgId → 写 Kafka → 解锁 → 记录 serverAck 供重发回放。
// Kafka 写失败时释放去重 key，已分配的 seq 成为空洞（允许）。
// 持锁期间写 Kafka 让同一会话的发送串行化（单会话吞吐受 Kafka 写延迟限制），换取 seq 顺序与分区顺序一致。
func (s *MsgService) submit(ctx context.Context, msg *model.Message) (*model.ServerAck, error) {
	reserved, prev, err := s.cache.ReserveClientMsg(ctx, msg.SenderID, msg.ClientMsgID)
	if err != nil {
		s.logger.Error("占用去重 key 失败", zap.Error(err))
		return nil, errMsgInternal
	}
	if !reserved {
		if prev != nil {
			return prev, nil // 客户端重发：回放之前的 serverAck
		}
		return nil, errDuplicate // 同一消息仍在处理中
	}

	committed := false
	defer func() {
		if committed {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.cache.ReleaseClientMsg(releaseCtx, msg.SenderID, msg.ClientMsgID); err != nil {
			s.logger.Warn("释放去重 key 失败", zap.Error(err))
		}
	}()

	unlock, err := s.locks.lock(ctx, msg.ConvID)
	if err != nil {
		s.logger.Warn("等待会话锁超时", zap.String("convID", msg.ConvID), zap.Error(err))
		return nil, errMsgSendFailed
	}
	seq, err := s.nextSeq(ctx, msg.ConvID)
	if err != nil {
		unlock()
		s.logger.Error("分配 seq 失败", zap.String("convID", msg.ConvID), zap.Error(err))
		return nil, errMsgInternal
	}
	msg.Seq = seq
	msg.MsgID = s.ids.Next()
	msg.Status = model.MsgStatusNormal
	msg.Timestamp = s.now().UnixMilli()

	err = s.mq.PublishMessage(ctx, msg)
	unlock()
	if err != nil {
		s.logger.Error("写入 Kafka 失败，seq 留空洞",
			zap.String("convID", msg.ConvID), zap.Int64("seq", seq), zap.Error(err))
		return nil, errMsgSendFailed
	}
	committed = true

	ack := &model.ServerAck{
		ClientMsgID: msg.ClientMsgID,
		ServerMsgID: msg.MsgID,
		ConvID:      msg.ConvID,
		Seq:         msg.Seq,
		Timestamp:   msg.Timestamp,
	}
	if err := s.cache.CompleteClientMsg(ctx, msg.SenderID, ack); err != nil {
		// 只影响重发回放：key 仍是 pending，5 分钟内重发会收到"正在发送中"
		s.logger.Warn("记录 serverAck 失败", zap.Error(err))
	}
	return ack, nil
}

// nextSeq 分配会话内下一个 seq。计数器丢失时用 max(已落库 seq, 已投递 seq) 校正后再分配。
func (s *MsgService) nextSeq(ctx context.Context, convID string) (int64, error) {
	seq, err := s.cache.NextSeq(ctx, convID)
	if !errors.Is(err, repository.ErrSeqNotInitialized) {
		return seq, err
	}
	base, err := s.seqBase(ctx, convID)
	if err != nil {
		return 0, err
	}
	if err := s.cache.InitSeq(ctx, convID, base); err != nil {
		return 0, err
	}
	return s.cache.NextSeq(ctx, convID)
}

// seqBase 计算 seq 计数器的校正值。conv_meta 由 deliver 消费者维护，可能比 MySQL 更新。
func (s *MsgService) seqBase(ctx context.Context, convID string) (int64, error) {
	base, err := s.store.GetMaxSeq(ctx, convID)
	if err != nil {
		return 0, err
	}
	if metas, err := s.cache.GetConvMetas(ctx, []string{convID}); err == nil {
		if m, ok := metas[convID]; ok && m.Seq > base {
			base = m.Seq
		}
	}
	return base, nil
}

// deliveredSeq 返回会话的已投递水位：max(MySQL 已落库 seq, deliver 消费者维护的 conv_meta.seq)。
// 由于同一会话的 seq 分配与 Kafka 写入在会话锁内串行，水位以下的 seq 要么已被消费者处理，要么是空洞。
func (s *MsgService) deliveredSeq(ctx context.Context, convID string) (int64, error) {
	return s.seqBase(ctx, convID)
}

// CurrentSeq 返回会话当前已分配的最大 seq（包含已分配但还在写 Kafka 的）。
// 用于确定可见下界（清空记录、入群），保证此刻之前分配的消息都不可见。
func (s *MsgService) CurrentSeq(ctx context.Context, convID string) (int64, error) {
	if seq, ok, err := s.cache.CurrentSeq(ctx, convID); err == nil && ok {
		return seq, nil
	}
	return s.seqBase(ctx, convID)
}

// ──────────────────────────────────────────────────────
// 投递（由 deliver 消费者调用）
// ──────────────────────────────────────────────────────

// DeliverMessage 更新热缓存并推送给在线成员（不含发送者，发送者已通过 serverAck 得知结果）。
// 缓存写失败只记日志：读路径会回源 MySQL；只有解析成员失败才返回错误让消费者重试。
func (s *MsgService) DeliverMessage(ctx context.Context, msg *model.Message) error {
	if err := s.cache.CacheMessage(ctx, msg); err != nil {
		s.logger.Warn("写入消息缓存失败", zap.String("convID", msg.ConvID), zap.Int64("seq", msg.Seq), zap.Error(err))
	}
	if msg.MsgType == model.MsgTypeRevoked {
		var notice model.RevokeNotice
		if err := json.Unmarshal([]byte(msg.Content), &notice); err == nil && notice.TargetSeq > 0 {
			if err := s.cache.MarkCachedRevoked(ctx, msg.ConvID, notice.TargetSeq); err != nil {
				s.logger.Warn("更新撤回缓存失败", zap.Error(err))
			}
		}
	}

	recipients, err := s.recipients(ctx, msg)
	if err != nil {
		return err
	}
	encoded, err := protocol.EncodeMsg(protocol.TypeMsg, msg)
	if err != nil {
		return fmt.Errorf("编码消息: %w", err)
	}
	for _, uid := range recipients {
		if uid != msg.SenderID {
			s.pushRaw(uid, encoded, protocol.TypeMsg)
		}
	}
	return nil
}

// recipients 返回会话的全部成员。
func (s *MsgService) recipients(ctx context.Context, msg *model.Message) ([]int64, error) {
	p, err := model.ParseConvID(msg.ConvID)
	if err != nil {
		return nil, err
	}
	if p.ConvType == model.ConvTypePrivate {
		return []int64{p.UserA, p.UserB}, nil
	}
	members, err := s.relations.GetGroupMembers(ctx, p.GroupID)
	if err != nil {
		return nil, fmt.Errorf("读取群成员: %w", err)
	}
	out := make([]int64, 0, len(members))
	for _, m := range members {
		out = append(out, m.UserID)
	}
	return out, nil
}

// ──────────────────────────────────────────────────────
// 会话同步与拉取
// ──────────────────────────────────────────────────────

// ListConversations 返回用户的会话视图列表，按置顶、最后消息时间倒序。
// maxSeq 取 MySQL conversations 与 Redis conv_meta 中较大者：后者由 deliver 消费者维护，通常更新。
func (s *MsgService) ListConversations(ctx context.Context, userID int64) ([]model.ConvView, error) {
	ucs, err := s.store.ListUserConversations(ctx, userID)
	if err != nil {
		s.logger.Error("查询用户会话失败", zap.Int64("userID", userID), zap.Error(err))
		return nil, errMsgInternal
	}
	ids := make([]string, 0, len(ucs))
	for _, uc := range ucs {
		ids = append(ids, uc.ConvID)
	}
	convs, err := s.store.GetConversations(ctx, ids)
	if err != nil {
		s.logger.Error("查询会话元数据失败", zap.Error(err))
		return nil, errMsgInternal
	}
	metas, err := s.cache.GetConvMetas(ctx, ids)
	if err != nil {
		s.logger.Warn("读取会话元数据缓存失败，降级为 MySQL", zap.Error(err))
		metas = map[string]repository.ConvMeta{}
	}

	views := make([]model.ConvView, 0, len(ucs))
	for _, uc := range ucs {
		c := convs[uc.ConvID]
		view := model.ConvView{
			ConvID:       uc.ConvID,
			ConvType:     c.ConvType,
			MaxSeq:       c.MaxSeq,
			MinSeq:       uc.MinSeq,
			LastMsgBrief: c.LastMsgBrief,
			LastMsgTime:  c.LastMsgAt,
			Pinned:       uc.Pinned,
			Muted:        uc.Muted,
		}
		if m, ok := metas[uc.ConvID]; ok && m.Seq >= view.MaxSeq && m.LastMsg != nil {
			view.MaxSeq = m.Seq
			view.LastMsg = m.LastMsg
			view.LastMsgBrief = model.MsgBrief(m.LastMsg.MsgType, m.LastMsg.Content)
			view.LastMsgTime = m.LastMsg.Timestamp
		}
		// 删除会话后没有新消息：不展示
		if uc.Hidden && view.MaxSeq < uc.MinSeq {
			continue
		}
		// 最后一条在可见下界之前（清空过聊天记录）：不展示摘要
		if view.MaxSeq < uc.MinSeq {
			view.LastMsg = nil
			view.LastMsgBrief = ""
		}
		view.ReadSeq = uc.ReadSeq
		if floor := uc.MinSeq - 1; floor > view.ReadSeq {
			view.ReadSeq = floor
		}
		if view.Unread = view.MaxSeq - view.ReadSeq; view.Unread < 0 {
			view.Unread = 0
		}
		views = append(views, view)
	}

	s.correctUnread(ctx, userID, views)
	s.fillTargets(ctx, userID, views)

	sort.SliceStable(views, func(i, j int) bool {
		if views[i].Pinned != views[j].Pinned {
			return views[i].Pinned
		}
		return views[i].LastMsgTime > views[j].LastMsgTime
	})
	return views, nil
}

// correctUnread 从 maxSeq-readSeq 中扣除不应计入未读的消息：撤回通知、已撤回的消息、自己发的消息。
// 只按 MySQL 计算，尚未落库的最新几条（通常是毫秒级窗口）不参与校正；空洞仍会被计入（发送失败才会产生，极少）。
// 查询失败时保留未校正的值。
func (s *MsgService) correctUnread(ctx context.Context, userID int64, views []model.ConvView) {
	ranges := make([]repository.SeqRange, 0, len(views))
	for _, v := range views {
		if v.Unread > 0 && v.Unread <= unreadExactLimit {
			ranges = append(ranges, repository.SeqRange{ConvID: v.ConvID, From: v.ReadSeq + 1, To: v.MaxSeq})
		}
	}
	if len(ranges) == 0 {
		return
	}
	skip, err := s.store.CountNonUnread(ctx, userID, ranges)
	if err != nil {
		s.logger.Warn("校正未读数失败", zap.Error(err))
		return
	}
	for i := range views {
		if n := skip[views[i].ConvID]; n > 0 {
			if views[i].Unread -= n; views[i].Unread < 0 {
				views[i].Unread = 0
			}
		}
	}
}

// fillTargets 批量填充会话对象（私聊对方 / 群）的名称与头像；查询失败时只缺展示字段，不影响同步。
func (s *MsgService) fillTargets(ctx context.Context, userID int64, views []model.ConvView) {
	var userIDs, groupIDs []int64
	for i := range views {
		p, err := model.ParseConvID(views[i].ConvID)
		if err != nil {
			continue
		}
		views[i].ConvType = p.ConvType
		switch p.ConvType {
		case model.ConvTypePrivate:
			views[i].TargetID = p.Peer(userID)
			userIDs = append(userIDs, views[i].TargetID)
		case model.ConvTypeGroup:
			views[i].TargetID = p.GroupID
			groupIDs = append(groupIDs, p.GroupID)
		}
	}
	users, err := s.store.GetUserBriefs(ctx, userIDs)
	if err != nil {
		s.logger.Warn("批量查询用户资料失败", zap.Error(err))
	}
	groups, err := s.store.GetGroupNames(ctx, groupIDs)
	if err != nil {
		s.logger.Warn("批量查询群名称失败", zap.Error(err))
	}
	for i := range views {
		switch views[i].ConvType {
		case model.ConvTypePrivate:
			if u, ok := users[views[i].TargetID]; ok {
				views[i].TargetName = u.Name
				views[i].TargetAvatar = u.AvatarURL
			}
		case model.ConvTypeGroup:
			views[i].TargetName = groups[views[i].TargetID]
		}
	}
}

// requireMember 返回用户在会话上的状态；用户不在会话中时返回 errConvNotFound。
func (s *MsgService) requireMember(ctx context.Context, userID int64, convID string) (*model.UserConversation, error) {
	if _, err := model.ParseConvID(convID); err != nil {
		return nil, errMsgInvalid
	}
	uc, err := s.store.GetUserConversation(ctx, userID, convID)
	if err != nil {
		s.logger.Error("查询用户会话失败", zap.Error(err))
		return nil, errMsgInternal
	}
	if uc == nil {
		return nil, errConvNotFound
	}
	return uc, nil
}

// PullMessages 返回 [fromSeq, toSeq] 内对该用户可见的消息。
// toSeq<=0 表示拉到最新。上界总是截到已投递水位：已分配但还在写 Kafka 的 seq 不能被当作空洞跳过。
// 区间超过 100 条时截断并置 hasMore，客户端从 toSeq+1 继续。
// 优先读 Redis 热窗口，窗口不完整时回源 MySQL 并与缓存合并（缓存里可能有尚未落库的最新消息）。
func (s *MsgService) PullMessages(ctx context.Context, userID int64, convID string, fromSeq, toSeq int64) (*model.PullMsgsResp, error) {
	uc, err := s.requireMember(ctx, userID, convID)
	if err != nil {
		return nil, err
	}
	if fromSeq < uc.MinSeq {
		fromSeq = uc.MinSeq
	}
	if fromSeq < 1 {
		fromSeq = 1
	}
	watermark, err := s.deliveredSeq(ctx, convID)
	if err != nil {
		s.logger.Error("读取会话水位失败", zap.Error(err))
		return nil, errMsgInternal
	}
	if toSeq <= 0 || toSeq > watermark {
		toSeq = watermark
	}
	resp := &model.PullMsgsResp{ConvID: convID, FromSeq: fromSeq, ToSeq: toSeq, Watermark: watermark, Msgs: []model.Message{}}
	if toSeq < fromSeq {
		return resp, nil
	}
	if toSeq-fromSeq+1 > pullMaxBatch {
		toSeq = fromSeq + pullMaxBatch - 1
		resp.ToSeq = toSeq
		resp.HasMore = true
	}

	msgs, err := s.loadRange(ctx, convID, fromSeq, toSeq)
	if err != nil {
		s.logger.Error("拉取消息失败", zap.String("convID", convID), zap.Error(err))
		return nil, errMsgInternal
	}
	resp.Msgs = s.filterDeleted(ctx, userID, convID, msgs, fromSeq, toSeq)
	return resp, nil
}

// loadRange 读取 [fromSeq, toSeq] 内的全部消息（不做用户级过滤），按 seq 升序。
func (s *MsgService) loadRange(ctx context.Context, convID string, fromSeq, toSeq int64) ([]model.Message, error) {
	cached, cacheErr := s.cache.GetCachedRange(ctx, convID, fromSeq, toSeq)
	if cacheErr == nil && int64(len(cached)) == toSeq-fromSeq+1 {
		return cached, nil
	}
	stored, err := s.store.GetMessagesBySeqRange(ctx, convID, fromSeq, toSeq, pullMaxBatch)
	if err != nil {
		return nil, err
	}
	bySeq := make(map[int64]model.Message, len(stored)+len(cached))
	for _, m := range stored {
		bySeq[m.Seq] = m
	}
	for _, m := range cached {
		if _, ok := bySeq[m.Seq]; !ok {
			bySeq[m.Seq] = m
		}
	}
	out := make([]model.Message, 0, len(bySeq))
	for _, m := range bySeq {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// filterDeleted 去掉用户删除过的消息。查询失败时不过滤，保证可用性。
func (s *MsgService) filterDeleted(ctx context.Context, userID int64, convID string, msgs []model.Message, fromSeq, toSeq int64) []model.Message {
	if len(msgs) == 0 {
		return msgs
	}
	deleted, err := s.store.GetUserDeletedSeqs(ctx, userID, convID, fromSeq, toSeq)
	if err != nil {
		s.logger.Warn("查询已删除消息失败", zap.Error(err))
		return msgs
	}
	if len(deleted) == 0 {
		return msgs
	}
	out := msgs[:0]
	for _, m := range msgs {
		if _, ok := deleted[m.Seq]; !ok {
			out = append(out, m)
		}
	}
	return out
}

// GetHistory 返回 seq < beforeSeq 的历史消息（按 seq 升序），用于聊天窗口上滑加载。
// beforeSeq<=0 表示从最新开始。历史分页只读 MySQL；尚未落库的最新消息由 pullMsgs 覆盖。
func (s *MsgService) GetHistory(ctx context.Context, userID int64, convID string, beforeSeq int64, limit int) ([]model.Message, bool, error) {
	uc, err := s.requireMember(ctx, userID, convID)
	if err != nil {
		return nil, false, err
	}
	limit = clampLimit(limit, historyMaxLimit)
	if beforeSeq <= 0 {
		beforeSeq = math.MaxInt64
	}
	msgs, err := s.store.GetMessagesBefore(ctx, convID, beforeSeq, uc.MinSeq, limit)
	if err != nil {
		s.logger.Error("查询历史消息失败", zap.Error(err))
		return nil, false, errMsgInternal
	}
	hasMore := len(msgs) == limit
	// 倒序查出，翻转为升序
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	if len(msgs) > 0 {
		msgs = s.filterDeleted(ctx, userID, convID, msgs, msgs[0].Seq, msgs[len(msgs)-1].Seq)
	}
	return msgs, hasMore, nil
}

// ──────────────────────────────────────────────────────
// 已读、撤回、删除、清空、搜索
// ──────────────────────────────────────────────────────

// MarkRead 把已读位置推进到 seq；seq 超过当前最大值时截断，避免未读数被算成负数。
func (s *MsgService) MarkRead(ctx context.Context, userID int64, convID string, seq int64) error {
	if seq <= 0 {
		return errMsgInvalid
	}
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return err
	}
	if cur, ok, err := s.cache.CurrentSeq(ctx, convID); err == nil && ok && seq > cur {
		seq = cur
	}
	if err := s.store.UpdateReadSeq(ctx, userID, convID, seq); err != nil {
		s.logger.Error("更新已读位置失败", zap.Error(err))
		return errMsgInternal
	}
	return nil
}

// clampLimit：未传或非法时默认 20，超过上限时截到上限。
func clampLimit(limit, max int) int {
	if limit <= 0 {
		return 20
	}
	if limit > max {
		return max
	}
	return limit
}

func revokeClientMsgID(convID string, seq int64) string {
	return fmt.Sprintf("revoke:%s:%d", convID, seq)
}

// RevokeMessage 撤回自己 2 分钟内发送的消息。
// 校验通过后发送一条 msgType=6 的撤回通知，占用新的 seq：
// persist 消费者在同一事务里把原消息置为已撤回，deliver 消费者更新缓存并推送给其它成员。
// 撤回通知和原消息在同一 Kafka 分区内，处理顺序一定在原消息之后。
func (s *MsgService) RevokeMessage(ctx context.Context, userID int64, convID string, seq int64) (*model.ServerAck, error) {
	if seq <= 0 {
		return nil, errMsgInvalid
	}
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return nil, err
	}

	target, err := s.cache.GetCachedMessage(ctx, convID, seq)
	if err != nil || target == nil {
		if target, err = s.store.GetMessage(ctx, convID, seq); err != nil {
			s.logger.Error("查询待撤回消息失败", zap.Error(err))
			return nil, errMsgInternal
		}
	}
	if target == nil || target.Status == model.MsgStatusRevoked ||
		target.MsgType == model.MsgTypeRevoked || target.MsgType == model.MsgTypeSystem {
		return nil, errNotRevocable
	}
	if target.SenderID != userID {
		return nil, errRevokeNotOwner
	}
	if s.now().Sub(time.UnixMilli(target.Timestamp)) > revokeWindow {
		return nil, errNotRevocable
	}

	notice, _ := json.Marshal(model.RevokeNotice{TargetSeq: target.Seq, TargetMsgID: target.MsgID, OperatorID: userID})
	return s.submit(ctx, &model.Message{
		ConvID:      convID,
		ConvType:    target.ConvType,
		SenderID:    userID,
		MsgType:     model.MsgTypeRevoked,
		Content:     string(notice),
		ClientMsgID: revokeClientMsgID(convID, seq),
	})
}

// DeleteMessage 删除一条消息，只对自己生效。
func (s *MsgService) DeleteMessage(ctx context.Context, userID int64, convID string, seq int64) error {
	if seq <= 0 {
		return errMsgInvalid
	}
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return err
	}
	if err := s.store.AddUserDeletedMessage(ctx, userID, convID, seq); err != nil {
		s.logger.Error("删除消息失败", zap.Error(err))
		return errMsgDeleteFailed
	}
	return nil
}

// ClearHistory 清空聊天记录（只对自己生效）：可见下界推进到当前 seq+1。
func (s *MsgService) ClearHistory(ctx context.Context, userID int64, convID string) error {
	return s.advanceMinSeq(ctx, userID, convID, false)
}

// DeleteConversation 从会话列表删除会话并清空记录；对方再发消息时会话重新出现。
func (s *MsgService) DeleteConversation(ctx context.Context, userID int64, convID string) error {
	return s.advanceMinSeq(ctx, userID, convID, true)
}

func (s *MsgService) advanceMinSeq(ctx context.Context, userID int64, convID string, hide bool) error {
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return err
	}
	cur, err := s.CurrentSeq(ctx, convID)
	if err != nil {
		s.logger.Error("读取会话 seq 失败", zap.Error(err))
		return errMsgInternal
	}
	if err := s.store.UpdateMinSeq(ctx, userID, convID, cur+1, hide); err != nil {
		s.logger.Error("更新可见下界失败", zap.Error(err))
		return errMsgInternal
	}
	return nil
}

// SearchMessages 全文检索用户可见的文本消息；convID 为空表示搜索全部会话。
func (s *MsgService) SearchMessages(ctx context.Context, userID int64, keyword, convID string, limit int) ([]model.Message, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return []model.Message{}, nil
	}
	limit = clampLimit(limit, searchMaxLimit)
	if convID != "" {
		if _, err := s.requireMember(ctx, userID, convID); err != nil {
			return nil, err
		}
	}
	msgs, err := s.store.SearchMessages(ctx, userID, keyword, convID, limit)
	if err != nil {
		s.logger.Error("搜索消息失败", zap.Error(err))
		return nil, errMsgInternal
	}
	return msgs, nil
}

// ──────────────────────────────────────────────────────
// 推送辅助
// ──────────────────────────────────────────────────────

// sendError 推送 error 帧；clientMsgID 非空时回填，便于客户端把对应消息标记为失败。
func (s *MsgService) sendError(userID int64, err error, clientMsgID string) {
	me := AsMsgError(err)
	s.pushToUser(userID, protocol.TypeError, &model.WsError{Code: me.Code, Message: me.Message, ClientMsgID: clientMsgID})
}

// pushToUser 编码并推送给在线用户；不在线或缓冲区满时丢弃，用户上线后通过 convSync + pullMsgs 补齐。
func (s *MsgService) pushToUser(userID int64, msgType string, data interface{}) {
	encoded, err := protocol.EncodeMsg(msgType, data)
	if err != nil {
		s.logger.Error("EncodeMsg 编码失败", zap.String("type", msgType), zap.Error(err))
		return
	}
	s.pushRaw(userID, encoded, msgType)
}

func (s *MsgService) pushRaw(userID int64, encoded []byte, msgType string) {
	client, ok := s.cm.Get(userID)
	if !ok {
		return
	}
	select {
	case client.SendCh <- encoded:
	default:
		s.logger.Warn("SendCh 缓冲区已满，丢弃推送（客户端会按 seq 补拉）",
			zap.Int64("userID", userID), zap.String("type", msgType))
	}
}
