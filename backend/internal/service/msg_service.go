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
	"sync"
	"sync/atomic"
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
	ErrNotFriend         = "不是好友"
	ErrBlocked           = "你已被对方拉黑"
	ErrDuplicate         = "消息正在发送中"
	ErrNotMember         = "不是该群组的成员"
	ErrMuted             = "你已被禁言"
	ErrRevokeFail        = "无法撤回此消息"
	ErrMsgNotRevocable   = "消息无法撤回（未找到或已过时效）"
	ErrMsgRevokeNotOwner = "仅发送者可撤回消息"
	ErrMsgDeleteFailed   = "删除消息失败"
	ErrConvNotFound      = "会话不存在"
	ErrMsgInvalid        = "消息字段无效"
	ErrMsgSendFailed     = "消息发送失败，请重试"
	ErrMsgInternal       = "服务器内部错误"
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
	revokeWindow           = 2 * time.Minute
	pullMaxBatch           = 100
	historyMaxLimit        = 100
	groupHistoryWindowSize = 200
	searchMaxLimit         = 100
	maxClientMsgIDLen      = 128
	maxContentBytes        = 1 << 20
	msgOpTimeout           = 5 * time.Second
	convLockShards         = 256
	defaultSeqSegmentSize  = 1000
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
//	投递：msg-persist 提交数据库事务后调用 DeliverMessage，更新热缓存后推送给在线成员
type MsgService struct {
	mysql       repository.MySQLRepo
	store       repository.MessageStore
	cache       repository.MessageCache
	relations   repository.RelationCache
	mq          repository.MQRepo
	ids         *idgen.Snowflake
	cm          *conn.ConnectionManager
	logger      *zap.Logger
	now         func() time.Time
	locks       *convLocks
	segments    sync.Map // convID -> *seqSegment
	segmentSize int64
}

type seqSegment struct {
	last atomic.Int64 // 发送时持锁取号，已读上报可以直接读取实际分配位置
	end  int64        // 发布号段后固定
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
		mysql:       mysql,
		store:       store,
		cache:       cache,
		relations:   relations,
		mq:          mq,
		ids:         ids,
		cm:          cm,
		logger:      logger,
		now:         time.Now,
		locks:       newConvLocks(),
		segmentSize: defaultSeqSegmentSize,
	}
}

// WithSeqSegmentSize 在启动、处理消息之前配置号段大小；<=0 使用默认值。
func (s *MsgService) WithSeqSegmentSize(size int64) *MsgService {
	if size > 0 {
		s.segmentSize = size
	}
	return s
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

// nextSeq 在会话锁内调用。只有换段才访问 MySQL，预留成功后在内存中取号。
func (s *MsgService) nextSeq(ctx context.Context, convID string) (int64, error) {
	var segment *seqSegment
	var last int64
	if value, ok := s.segments.Load(convID); ok {
		segment = value.(*seqSegment)
		last = segment.last.Load()
		if last < segment.end {
			return segment.last.Add(1), nil
		}
	}
	base, err := s.seqBase(ctx, convID)
	if err != nil {
		return 0, err
	}
	if segment == nil {
		// 升级时保留旧计数器作为下限，不再使用它逐条取号。
		legacy, _, err := s.cache.LegacySeq(ctx, convID)
		if err != nil {
			return 0, err
		}
		base = max(base, legacy)
	} else {
		base = max(base, last)
	}
	parsed, err := model.ParseConvID(convID)
	if err != nil {
		return 0, err
	}
	start, end, err := s.store.ReserveSeqRange(ctx, convID, parsed.ConvType, s.segmentSize, base)
	if err != nil {
		return 0, err
	}
	segment = &seqSegment{end: end}
	segment.last.Store(start)
	s.segments.Store(convID, segment)
	return start, nil
}

// seqBase 为申请号段读取旧数据的下限，兼容旧链路已投递但未落库的 seq。
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

// CurrentSeq 读取实际分配或落库的位置，不等待持有会话锁的发送流程。
func (s *MsgService) CurrentSeq(ctx context.Context, convID string) (int64, error) {
	current, err := s.store.GetMaxSeq(ctx, convID)
	if err != nil {
		return 0, err
	}
	if value, ok := s.segments.Load(convID); ok {
		current = max(current, value.(*seqSegment).last.Load())
	}
	return current, nil
}

// WithCurrentSeq 将读取实际位置与边界操作一起串行化（清空/入群期间不发送新消息）。
func (s *MsgService) WithCurrentSeq(ctx context.Context, convID string, apply func(int64) error) error {
	unlock, err := s.locks.lock(ctx, convID)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := s.CurrentSeq(ctx, convID)
	if err != nil {
		return err
	}
	return apply(current)
}

// ──────────────────────────────────────────────────────
// 投递（由 msg-persist 在落库后调用）
// ──────────────────────────────────────────────────────

// DeliverMessage 更新会话缓存并推送给在线成员（不含发送者，发送者已通过 serverAck 得知结果）。
// 缓存写失败只记日志：读路径会回源 MySQL；只有解析成员失败才返回错误让消费者重试。
func (s *MsgService) DeliverMessage(ctx context.Context, msg *model.Message) error {
	if err := s.cache.CacheMessage(ctx, msg); err != nil {
		s.logger.Warn("写入消息缓存失败", zap.String("convID", msg.ConvID), zap.Int64("seq", msg.Seq), zap.Error(err))
	}
	if msg.ConvType != model.ConvTypeGroup && msg.MsgType == model.MsgTypeRevoked {
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

// ListConversations 使用已落库的会话位置和摘要，按置顶、最后消息时间倒序。
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
		// 仅用与已落库最后一条消息一致的缓存补充 LastMsg。
		if m, ok := metas[uc.ConvID]; ok && m.Seq == c.MaxSeq && m.LastMsg != nil && m.LastMsg.MsgID == c.LastMsgID {
			view.LastMsg = m.LastMsg
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
		views = append(views, view)
	}

	if err := s.fillUnread(ctx, userID, views); err != nil {
		s.logger.Error("统计未读消息失败", zap.Error(err))
		return nil, errMsgInternal
	}
	s.fillTargets(ctx, userID, views)

	sort.SliceStable(views, func(i, j int) bool {
		if views[i].Pinned != views[j].Pinned {
			return views[i].Pinned
		}
		return views[i].LastMsgTime > views[j].LastMsgTime
	})
	return views, nil
}

// fillUnread 统计已落库的有效消息数。空洞和未使用号段不计数，不按 seq 差值降级。
func (s *MsgService) fillUnread(ctx context.Context, userID int64, views []model.ConvView) error {
	ranges := make([]repository.SeqRange, 0, len(views))
	for _, v := range views {
		if v.MaxSeq > v.ReadSeq {
			ranges = append(ranges, repository.SeqRange{ConvID: v.ConvID, From: v.ReadSeq + 1, To: v.MaxSeq})
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	counts, err := s.store.CountUnread(ctx, userID, ranges)
	if err != nil {
		return err
	}
	for i := range views {
		views[i].Unread = counts[views[i].ConvID]
	}
	return nil
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
// toSeq<=0 表示拉到最新。上界与消息都取自 MySQL，避免跨过仍在处理的消息。
// 区间超过 100 个 seq 时截断并置 hasMore，客户端从 toSeq+1 继续。
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
	watermark, err := s.store.GetMaxSeq(ctx, convID)
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

	msgs, err := s.store.GetMessagesBySeqRange(ctx, convID, fromSeq, toSeq, pullMaxBatch)
	if err != nil {
		s.logger.Error("拉取消息失败", zap.String("convID", convID), zap.Error(err))
		return nil, errMsgInternal
	}
	resp.Msgs = s.filterDeleted(ctx, userID, convID, msgs, fromSeq, toSeq)
	return resp, nil
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
// beforeSeq<=0 表示从最新开始。历史和增量补拉均以已落库消息为准。
func (s *MsgService) GetHistory(ctx context.Context, userID int64, convID string, beforeSeq int64, limit int) ([]model.Message, bool, error) {
	conv, err := model.ParseConvID(convID)
	if err != nil {
		return nil, false, errMsgInvalid
	}
	state, err := s.store.GetHistoryState(ctx, userID, convID, conv.ConvType)
	if err != nil {
		s.logger.Error("查询历史消息权限和版本失败", zap.Error(err))
		return nil, false, errMsgInternal
	}
	if state == nil {
		return nil, false, errConvNotFound
	}
	limit = clampLimit(limit, historyMaxLimit)
	firstPage := beforeSeq <= 0
	if firstPage {
		beforeSeq = math.MaxInt64
	}
	var msgs []model.Message
	var hit bool
	if conv.ConvType == model.ConvTypeGroup {
		msgs, hit = s.groupHistoryPage(ctx, userID, convID, *state, beforeSeq, limit+1, firstPage)
	}
	if !hit {
		msgs, err = s.store.GetMessagesBefore(ctx, userID, convID, beforeSeq, state.MinSeq, limit+1)
		if err != nil {
			s.logger.Error("查询历史消息失败", zap.Error(err))
			return nil, false, errMsgInternal
		}
	}
	hasMore := len(msgs) > limit
	if hasMore {
		msgs = msgs[:limit]
	}
	// 倒序查出，翻转为升序
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
	return msgs, hasMore, nil
}

func (s *MsgService) groupHistoryPage(ctx context.Context, userID int64, convID string, state repository.HistoryState, beforeSeq int64, limit int, firstPage bool) ([]model.Message, bool) {
	window, err := s.cache.GetGroupHistory(ctx, convID, state.Version)
	if err != nil {
		return nil, false
	}
	if len(window) == 0 && firstPage {
		window, err = s.store.GetGroupHistoryWindow(ctx, convID, state.Version, groupHistoryWindowSize)
		if err != nil || len(window) == 0 {
			return nil, false
		}
		if err := s.cache.SetGroupHistory(ctx, convID, state.Version, window); err != nil {
			s.logger.Debug("回填群历史缓存失败", zap.Error(err))
		}
	}
	if len(window) == 0 {
		return nil, false
	}
	oldest := window[len(window)-1].Seq
	complete := len(window) < groupHistoryWindowSize || oldest <= state.MinSeq
	start := sort.Search(len(window), func(i int) bool { return window[i].Seq < beforeSeq })
	end := sort.Search(len(window), func(i int) bool { return window[i].Seq < state.MinSeq })
	if !complete && end-start < limit {
		return nil, false
	}
	if start >= end {
		return []model.Message{}, true
	}
	candidates := window[start:end]
	deleted, err := s.store.GetUserDeletedSeqs(ctx, userID, convID, candidates[len(candidates)-1].Seq, candidates[0].Seq)
	if err != nil {
		return nil, false
	}
	page := make([]model.Message, 0, min(limit, len(candidates)))
	for _, msg := range candidates {
		if _, hidden := deleted[msg.Seq]; hidden {
			continue
		}
		page = append(page, msg)
		if len(page) == limit {
			break
		}
	}
	// 没有足够的可见消息判断 hasMore，且窗口可能还有更早记录时，整页回源。
	return page, complete || len(page) == limit
}

// ──────────────────────────────────────────────────────
// 已读、撤回、删除、清空、搜索
// ──────────────────────────────────────────────────────

// MarkRead 把已读位置推进到 seq；截到实际位置，禁止客户端将未使用号段提前标为已读。
func (s *MsgService) MarkRead(ctx context.Context, userID int64, convID string, seq int64) error {
	if seq <= 0 {
		return errMsgInvalid
	}
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return err
	}
	current, err := s.CurrentSeq(ctx, convID)
	if err != nil {
		s.logger.Error("读取会话实际位置失败", zap.Error(err))
		return errMsgInternal
	}
	if err := s.store.UpdateReadSeq(ctx, userID, convID, min(seq, current)); err != nil {
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
// 消费者在事务中把原消息置为已撤回，提交后更新缓存并推送给其它成员。
// 撤回通知和原消息在同一 Kafka 分区内，处理顺序一定在原消息之后。
func (s *MsgService) RevokeMessage(ctx context.Context, userID int64, convID string, seq int64) (*model.ServerAck, error) {
	if seq <= 0 {
		return nil, errMsgInvalid
	}
	if _, err := s.requireMember(ctx, userID, convID); err != nil {
		return nil, err
	}

	target, err := s.store.GetMessage(ctx, convID, seq)
	if err != nil {
		s.logger.Error("查询待撤回消息失败", zap.Error(err))
		return nil, errMsgInternal
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
	err := s.WithCurrentSeq(ctx, convID, func(current int64) error {
		return s.store.UpdateMinSeq(ctx, userID, convID, current+1, hide)
	})
	if err != nil {
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
