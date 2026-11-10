package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

type ucKey struct {
	userID int64
	convID string
}

// recordingMessageStore 是 repository.MessageStore 的测试替身：
// 记录会话关系相关调用，并用内存数据支撑 MsgService 的读路径。
type recordingMessageStore struct {
	mu sync.Mutex

	ensuredConvs    map[string]int
	userConvs       map[string][]int64         // convID -> 建立过关系的用户
	joinMinSeq      map[string]map[int64]int64 // convID -> userID -> 入群时设置的 min_seq
	deletedUserConv [][2]interface{}           // {userID, convID}
	userGroupIDs    map[int64][]int64

	ucs                map[ucKey]model.UserConversation
	maxSeq             map[string]int64           // GetMaxSeq 返回值
	messages           map[string][]model.Message // convID -> 已落库消息
	convs              map[string]model.Conversation
	unread             map[string]int64
	unreadErr          error
	allocated          map[string]int64
	reserveCalls       int
	reserveErr         error
	deleted            map[ucKey]map[int64]struct{}
	deletedErr         error
	historyVersions    map[string]int64
	historyWindowReads int
	historyPageReads   int
	briefs             map[int64]repository.UserBrief
	groupNames         map[int64]string
	rangeCalls         []repository.SeqRange
}

func newRecordingMessageStore() *recordingMessageStore {
	return &recordingMessageStore{
		ensuredConvs:    make(map[string]int),
		userConvs:       make(map[string][]int64),
		joinMinSeq:      make(map[string]map[int64]int64),
		userGroupIDs:    make(map[int64][]int64),
		ucs:             make(map[ucKey]model.UserConversation),
		maxSeq:          make(map[string]int64),
		messages:        make(map[string][]model.Message),
		convs:           make(map[string]model.Conversation),
		unread:          make(map[string]int64),
		allocated:       make(map[string]int64),
		deleted:         make(map[ucKey]map[int64]struct{}),
		historyVersions: make(map[string]int64),
		briefs:          make(map[int64]repository.UserBrief),
		groupNames:      make(map[int64]string),
	}
}

func (s *recordingMessageStore) PersistMessages(_ context.Context, _ []model.Message) ([]model.Message, error) {
	return nil, nil
}
func (s *recordingMessageStore) GetMaxSeq(_ context.Context, convID string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxSeq[convID], nil
}
func (s *recordingMessageStore) ReserveSeqRange(_ context.Context, convID string, convType int, size, floor int64) (int64, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reserveCalls++
	if s.reserveErr != nil {
		return 0, 0, s.reserveErr
	}
	s.ensuredConvs[convID] = convType
	base := max(s.allocated[convID], s.maxSeq[convID], floor)
	s.allocated[convID] = base + size
	return base + 1, base + size, nil
}
func (s *recordingMessageStore) GetMessagesBySeqRange(_ context.Context, convID string, fromSeq, toSeq int64, limit int) ([]model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Message, 0)
	for _, m := range s.messages[convID] {
		if m.Seq >= fromSeq && m.Seq <= toSeq {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (s *recordingMessageStore) GetMessagesBefore(_ context.Context, userID int64, convID string, before, floor int64, limit int) ([]model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.historyPageReads++
	msgs := make([]model.Message, 0)
	for _, msg := range s.messages[convID] {
		_, deleted := s.deleted[ucKey{userID, convID}][msg.Seq]
		if msg.Seq < before && msg.Seq >= floor && !deleted {
			msgs = append(msgs, msg)
		}
	}
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq > msgs[j].Seq })
	if len(msgs) > limit {
		msgs = msgs[:limit]
	}
	return msgs, nil
}
func (s *recordingMessageStore) GetHistoryState(_ context.Context, userID int64, convID string, _ int) (*repository.HistoryState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	uc, ok := s.ucs[ucKey{userID, convID}]
	if !ok {
		return nil, nil
	}
	return &repository.HistoryState{MinSeq: uc.MinSeq, Version: s.historyVersions[convID]}, nil
}
func (s *recordingMessageStore) GetGroupHistoryWindow(_ context.Context, convID string, version int64, limit int) ([]model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.historyWindowReads++
	if version != s.historyVersions[convID] {
		return nil, nil
	}
	msgs := append([]model.Message(nil), s.messages[convID]...)
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].Seq > msgs[j].Seq })
	return msgs[:min(len(msgs), limit)], nil
}
func (s *recordingMessageStore) GetMessage(_ context.Context, convID string, seq int64) (*model.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range s.messages[convID] {
		if m.Seq == seq {
			cp := m
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *recordingMessageStore) SearchMessages(_ context.Context, _ int64, _, _ string, _ int) ([]model.Message, error) {
	return nil, nil
}

func (s *recordingMessageStore) EnsureConversation(_ context.Context, convID string, convType int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensuredConvs[convID] = convType
	return nil
}
func (s *recordingMessageStore) GetConversations(_ context.Context, ids []string) (map[string]model.Conversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]model.Conversation, len(ids))
	for _, id := range ids {
		if c, ok := s.convs[id]; ok {
			out[id] = c
		}
	}
	return out, nil
}
func (s *recordingMessageStore) EnsureUserConversations(_ context.Context, convID string, userIDs []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userConvs[convID] = append(s.userConvs[convID], userIDs...)
	return nil
}
func (s *recordingMessageStore) JoinConversation(_ context.Context, convID string, userIDs []int64, minSeq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userConvs[convID] = append(s.userConvs[convID], userIDs...)
	if s.joinMinSeq[convID] == nil {
		s.joinMinSeq[convID] = make(map[int64]int64)
	}
	for _, uid := range userIDs {
		s.joinMinSeq[convID][uid] = minSeq
	}
	return nil
}
func (s *recordingMessageStore) DeleteUserConversation(_ context.Context, userID int64, convID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedUserConv = append(s.deletedUserConv, [2]interface{}{userID, convID})
	return nil
}
func (s *recordingMessageStore) ListUserConversations(_ context.Context, userID int64) ([]model.UserConversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.UserConversation, 0)
	for k, uc := range s.ucs {
		if k.userID == userID {
			out = append(out, uc)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ConvID < out[j].ConvID })
	return out, nil
}
func (s *recordingMessageStore) GetUserConversation(_ context.Context, userID int64, convID string) (*model.UserConversation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if uc, ok := s.ucs[ucKey{userID, convID}]; ok {
		return &uc, nil
	}
	return nil, nil
}
func (s *recordingMessageStore) UpdateReadSeq(_ context.Context, userID int64, convID string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ucKey{userID, convID}
	uc := s.ucs[key]
	uc.ReadSeq = max(uc.ReadSeq, seq)
	s.ucs[key] = uc
	return nil
}
func (s *recordingMessageStore) UpdateMinSeq(_ context.Context, userID int64, convID string, seq int64, hide bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ucKey{userID, convID}
	uc := s.ucs[key]
	uc.MinSeq, uc.ReadSeq = max(uc.MinSeq, seq), max(uc.ReadSeq, seq-1)
	uc.Hidden = uc.Hidden || hide
	s.ucs[key] = uc
	return nil
}
func (s *recordingMessageStore) CountUnread(_ context.Context, _ int64, ranges []repository.SeqRange) (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rangeCalls = append(s.rangeCalls, ranges...)
	if s.unreadErr != nil {
		return nil, s.unreadErr
	}
	out := make(map[string]int64)
	for _, r := range ranges {
		if n, ok := s.unread[r.ConvID]; ok {
			out[r.ConvID] = n
		}
	}
	return out, nil
}
func (s *recordingMessageStore) GetUserBriefs(_ context.Context, ids []int64) (map[int64]repository.UserBrief, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]repository.UserBrief)
	for _, id := range ids {
		if b, ok := s.briefs[id]; ok {
			out[id] = b
		}
	}
	return out, nil
}
func (s *recordingMessageStore) GetGroupNames(_ context.Context, ids []int64) (map[int64]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int64]string)
	for _, id := range ids {
		if n, ok := s.groupNames[id]; ok {
			out[id] = n
		}
	}
	return out, nil
}
func (s *recordingMessageStore) AddUserDeletedMessage(_ context.Context, _ int64, _ string, _ int64) error {
	return nil
}
func (s *recordingMessageStore) GetUserDeletedSeqs(_ context.Context, userID int64, convID string, from, to int64) (map[int64]struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deletedErr != nil {
		return nil, s.deletedErr
	}
	out := make(map[int64]struct{})
	for seq := range s.deleted[ucKey{userID, convID}] {
		if seq >= from && seq <= to {
			out[seq] = struct{}{}
		}
	}
	return out, nil
}
func (s *recordingMessageStore) ListUserGroupIDs(_ context.Context, userID int64) ([]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.userGroupIDs[userID], nil
}

// recordingRelations 是 repository.RelationCache 的测试替身，记录失效调用。
type recordingRelations struct {
	mu sync.Mutex

	friendInvalidations [][2]int64
	blockInvalidations  [][2]int64
	groupInvalidations  []int64
}

func (r *recordingRelations) IsFriend(_ context.Context, _, _ int64) (bool, error) { return true, nil }
func (r *recordingRelations) IsBlocked(_ context.Context, _, _ int64) (bool, error) {
	return false, nil
}
func (r *recordingRelations) GetGroupMembers(_ context.Context, _ int64) ([]model.GroupMember, error) {
	return nil, nil
}
func (r *recordingRelations) InvalidateFriend(_ context.Context, a, b int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.friendInvalidations = append(r.friendInvalidations, [2]int64{a, b})
	return nil
}
func (r *recordingRelations) InvalidateBlock(_ context.Context, u, b int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.blockInvalidations = append(r.blockInvalidations, [2]int64{u, b})
	return nil
}
func (r *recordingRelations) InvalidateGroupMembers(_ context.Context, groupID int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groupInvalidations = append(r.groupInvalidations, groupID)
	return nil
}

// fakeSeqReader 是 ConvSeqReader 的测试替身。
type fakeSeqReader map[string]int64

func (f fakeSeqReader) WithCurrentSeq(_ context.Context, convID string, apply func(int64) error) error {
	return apply(f[convID])
}

// fakeMessageCache 是 repository.MessageCache 的内存实现，语义与 Redis 版本一致：
// 旧 seq 只读用于迁移；去重 key 有 pending（nil）/ 已完成两种状态。
type fakeMessageCache struct {
	mu sync.Mutex

	seqs       map[string]int64
	dedup      map[string]*model.ServerAck
	metas      map[string]repository.ConvMeta
	cached     map[string][]model.Message
	revoked    map[string][]int64
	history    map[string]fakeGroupHistory
	historyErr error
}

type fakeGroupHistory struct {
	version int64
	msgs    []model.Message
}

func newFakeMessageCache() *fakeMessageCache {
	return &fakeMessageCache{
		seqs:    make(map[string]int64),
		dedup:   make(map[string]*model.ServerAck),
		metas:   make(map[string]repository.ConvMeta),
		cached:  make(map[string][]model.Message),
		revoked: make(map[string][]int64),
		history: make(map[string]fakeGroupHistory),
	}
}

func dedupKey(uid int64, cid string) string { return fmt.Sprintf("%d:%s", uid, cid) }

func (c *fakeMessageCache) LegacySeq(_ context.Context, convID string) (int64, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.seqs[convID]
	return v, ok, nil
}
func (c *fakeMessageCache) ReserveClientMsg(_ context.Context, uid int64, cid string) (bool, *model.ServerAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := dedupKey(uid, cid)
	if ack, ok := c.dedup[k]; ok {
		return false, ack, nil
	}
	c.dedup[k] = nil
	return true, nil, nil
}
func (c *fakeMessageCache) CompleteClientMsg(_ context.Context, uid int64, ack *model.ServerAck) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dedup[dedupKey(uid, ack.ClientMsgID)] = ack
	return nil
}
func (c *fakeMessageCache) ReleaseClientMsg(_ context.Context, uid int64, cid string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.dedup, dedupKey(uid, cid))
	return nil
}
func (c *fakeMessageCache) CacheMessage(_ context.Context, msg *model.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached[msg.ConvID] = append(c.cached[msg.ConvID], *msg)
	if m := c.metas[msg.ConvID]; msg.Seq > m.Seq {
		cp := *msg
		c.metas[msg.ConvID] = repository.ConvMeta{Seq: msg.Seq, LastMsg: &cp}
	}
	return nil
}
func (c *fakeMessageCache) MarkCachedRevoked(_ context.Context, convID string, seq int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.revoked[convID] = append(c.revoked[convID], seq)
	return nil
}
func (c *fakeMessageCache) GetCachedRange(_ context.Context, convID string, fromSeq, toSeq int64) ([]model.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]model.Message, 0)
	for _, m := range c.cached[convID] {
		if m.Seq >= fromSeq && m.Seq <= toSeq {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}
func (c *fakeMessageCache) GetCachedMessage(_ context.Context, convID string, seq int64) (*model.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.cached[convID] {
		if m.Seq == seq {
			cp := m
			return &cp, nil
		}
	}
	return nil, nil
}
func (c *fakeMessageCache) GetGroupHistory(_ context.Context, convID string, version int64) ([]model.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.history[convID]
	if entry.version != version {
		return nil, c.historyErr
	}
	return entry.msgs, c.historyErr
}
func (c *fakeMessageCache) SetGroupHistory(_ context.Context, convID string, version int64, msgs []model.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history[convID] = fakeGroupHistory{version: version, msgs: append([]model.Message(nil), msgs...)}
	return nil
}
func (c *fakeMessageCache) GetConvMetas(_ context.Context, ids []string) (map[string]repository.ConvMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]repository.ConvMeta)
	for _, id := range ids {
		if m, ok := c.metas[id]; ok {
			out[id] = m
		}
	}
	return out, nil
}

// fakeMQ 记录发布的消息；failNext>0 时接下来的若干次发布失败。
type fakeMQ struct {
	mu        sync.Mutex
	published []model.Message
	failNext  int
}

var errFakeKafka = errors.New("kafka unavailable")

func (q *fakeMQ) PublishMessage(_ context.Context, msg *model.Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.failNext > 0 {
		q.failNext--
		return errFakeKafka
	}
	q.published = append(q.published, *msg)
	return nil
}
func (q *fakeMQ) PublishMomentPush(_ context.Context, _ *model.Moment) error { return nil }

var (
	_ repository.MessageStore  = (*recordingMessageStore)(nil)
	_ repository.RelationCache = (*recordingRelations)(nil)
	_ repository.MessageCache  = (*fakeMessageCache)(nil)
	_ repository.MQRepo        = (*fakeMQ)(nil)
	_ ConvSeqReader            = fakeSeqReader(nil)
)
