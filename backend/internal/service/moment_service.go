package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// ── 验证/业务错误常量 ──

const (
	ErrMomentContentEmpty   = "动态内容不能为空"
	ErrMomentContentTooLong = "内容不能超过 2000 个字符"
	ErrMomentNotFound       = "动态未找到"
	ErrNotMomentOwner       = "不是该动态的作者"
	ErrNotCommentOwner      = "不是该评论的所有者"
	ErrInvalidVisibility    = "可见性必须为 2（好友）或 3（仅自己）"
	ErrCommentNotFound      = "评论未找到"
)

const maxMomentContentRunes = 2000

type momentRepository interface {
	repository.MySQLRepo
	CreateMomentWithOutbox(ctx context.Context, moment *model.Moment) error
	SetMomentLike(ctx context.Context, momentID, userID int64, liked bool) (int64, error)
	GetMomentLikeStats(ctx context.Context, viewerID int64, momentIDs []int64) (map[int64]repository.MomentLikeStats, error)
	ListMomentLikerProfiles(ctx context.Context, momentID int64) ([]model.MomentLiker, error)
}

// MomentService 处理动态发布、点赞/评论操作以及动态流获取。
type MomentService struct {
	mysqlRepo momentRepository
	redisRepo repository.RedisRepo
	logger    *zap.Logger
}

// NewMomentService 使用所有必需的依赖项创建一个 MomentService。
func NewMomentService(mysqlRepo momentRepository, redisRepo repository.RedisRepo, logger *zap.Logger) *MomentService {
	return &MomentService{
		mysqlRepo: mysqlRepo,
		redisRepo: redisRepo,
		logger:    logger,
	}
}

// PublishMoment 在同一个 MySQL 事务中创建动态和待分发事件，提交后返回动态 ID。
func (s *MomentService) PublishMoment(ctx context.Context, userID int64, content string, mediaUrls *string, visibility int) (int64, error) {
	if strings.TrimSpace(content) == "" {
		return 0, fmt.Errorf(ErrMomentContentEmpty)
	}
	if utf8.RuneCountInString(content) > maxMomentContentRunes {
		return 0, fmt.Errorf(ErrMomentContentTooLong)
	}
	if visibility != 2 && visibility != 3 {
		return 0, fmt.Errorf(ErrInvalidVisibility)
	}

	now := time.Now()
	moment := &model.Moment{
		AuthorID:   userID,
		Content:    content,
		MediaUrls:  mediaUrls,
		Visibility: visibility,
		CreatedAt:  now,
	}

	if err := s.mysqlRepo.CreateMomentWithOutbox(ctx, moment); err != nil {
		return 0, fmt.Errorf("创建动态: %w", err)
	}
	return moment.ID, nil
}

// GetMoment 根据 ID 返回单条动态（含点赞数与"我是否已赞"）。
func (s *MomentService) GetMoment(ctx context.Context, viewerID int64, momentID int64) (*model.Moment, error) {
	moment, err := s.mysqlRepo.GetMomentByID(ctx, momentID)
	if err != nil {
		return nil, fmt.Errorf("获取动态: %w", err)
	}
	if moment == nil {
		return nil, fmt.Errorf(ErrMomentNotFound)
	}
	visible, err := s.canViewMoment(ctx, viewerID, moment)
	if err != nil {
		return nil, fmt.Errorf("检查动态可见性: %w", err)
	}
	if !visible {
		// 对无权查看者返回“未找到”，避免泄露动态是否存在。
		return nil, fmt.Errorf(ErrMomentNotFound)
	}
	batch := []model.Moment{*moment}
	if err := s.enrichLikes(ctx, viewerID, batch); err != nil {
		return nil, err
	}
	s.enrichAuthors(ctx, batch)
	s.enrichComments(ctx, batch)
	return &batch[0], nil
}

// GetUserMoments 返回用户自己的分页动态列表（含点赞数与"我是否已赞"）。
func (s *MomentService) GetUserMoments(ctx context.Context, viewerID int64, userID int64, limit, offset int) ([]model.Moment, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	// offset/limit 作用于“当前查看者可见的动态”，隐藏动态不能占用分页名额。
	const scanBatchSize = 100
	visibleMoments := make([]model.Moment, 0, limit)
	visibleSkipped := 0
	rawOffset := 0
	for len(visibleMoments) < limit {
		batch, err := s.mysqlRepo.GetMomentsByUser(ctx, userID, scanBatchSize, rawOffset)
		if err != nil {
			return nil, fmt.Errorf("按用户获取动态: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		rawOffset += len(batch)

		for i := range batch {
			visible, err := s.canViewMoment(ctx, viewerID, &batch[i])
			if err != nil {
				return nil, fmt.Errorf("检查动态可见性: %w", err)
			}
			if !visible {
				continue
			}
			if visibleSkipped < offset {
				visibleSkipped++
				continue
			}
			visibleMoments = append(visibleMoments, batch[i])
			if len(visibleMoments) == limit {
				break
			}
		}
		if len(batch) < scanBatchSize {
			break
		}
	}

	if err := s.enrichLikes(ctx, viewerID, visibleMoments); err != nil {
		return nil, err
	}
	s.enrichAuthors(ctx, visibleMoments)
	s.enrichComments(ctx, visibleMoments)
	return visibleMoments, nil
}

func (s *MomentService) enrichAuthors(ctx context.Context, moments []model.Moment) {
	profiles := make(map[int64]*model.User)
	for i := range moments {
		profile, cached := profiles[moments[i].AuthorID]
		if !cached {
			var err error
			profile, err = s.mysqlRepo.GetUserByID(ctx, moments[i].AuthorID)
			if err != nil {
				s.logger.Warn("读取动态作者资料失败", zap.Int64("authorID", moments[i].AuthorID), zap.Error(err))
			}
			profiles[moments[i].AuthorID] = profile
		}
		if profile != nil {
			moments[i].AuthorName = profile.Username
			if profile.Nickname != "" {
				moments[i].AuthorName = profile.Nickname
			}
			moments[i].AuthorAvatar = profile.AvatarURL
		}
	}
}

func (s *MomentService) enrichComments(ctx context.Context, moments []model.Moment) {
	for i := range moments {
		comments, err := s.mysqlRepo.GetMomentComments(ctx, moments[i].ID)
		if err != nil {
			s.logger.Warn("读取动态评论失败", zap.Int64("momentID", moments[i].ID), zap.Error(err))
			continue
		}
		moments[i].Comments = comments
	}
}

// canViewMoment 是详情、用户动态列表和 Feed 共用的可见性规则。
func (s *MomentService) canViewMoment(ctx context.Context, viewerID int64, moment *model.Moment) (bool, error) {
	if moment.AuthorID == viewerID {
		return true, nil
	}
	switch moment.Visibility {
	case 1: // 历史“公开”数据按新规则降级为好友可见。
		return s.mysqlRepo.IsFriend(ctx, viewerID, moment.AuthorID)
	case 2:
		return s.mysqlRepo.IsFriend(ctx, viewerID, moment.AuthorID)
	case 3:
		return false, nil
	default:
		return false, nil
	}
}

// LikeMoment 提交点赞后返回该动态的点赞数。重复点赞保持同一条关系。
func (s *MomentService) LikeMoment(ctx context.Context, userID int64, momentID int64) (int64, error) {
	return s.setMomentLike(ctx, userID, momentID, true)
}

// UnlikeMoment 提交取消点赞后返回该动态的点赞数。重复取消保持当前状态。
func (s *MomentService) UnlikeMoment(ctx context.Context, userID int64, momentID int64) (int64, error) {
	return s.setMomentLike(ctx, userID, momentID, false)
}

func (s *MomentService) setMomentLike(ctx context.Context, userID, momentID int64, liked bool) (int64, error) {
	if err := s.checkMomentVisible(ctx, userID, momentID); err != nil {
		return 0, err
	}
	count, err := s.mysqlRepo.SetMomentLike(ctx, momentID, userID, liked)
	if errors.Is(err, repository.ErrMomentNotFound) {
		return 0, fmt.Errorf(ErrMomentNotFound)
	}
	return count, err
}

// GetMomentLikers 返回已提交的点赞关系和用户资料。
func (s *MomentService) GetMomentLikers(ctx context.Context, viewerID, momentID int64) ([]model.MomentLiker, error) {
	if err := s.checkMomentVisible(ctx, viewerID, momentID); err != nil {
		return nil, err
	}
	return s.mysqlRepo.ListMomentLikerProfiles(ctx, momentID)
}

func (s *MomentService) checkMomentVisible(ctx context.Context, viewerID, momentID int64) error {
	moment, err := s.mysqlRepo.GetMomentByID(ctx, momentID)
	if err != nil {
		return fmt.Errorf("获取动态: %w", err)
	}
	if moment == nil {
		return fmt.Errorf(ErrMomentNotFound)
	}
	visible, err := s.canViewMoment(ctx, viewerID, moment)
	if err != nil {
		return fmt.Errorf("检查动态可见性: %w", err)
	}
	if !visible {
		return fmt.Errorf(ErrMomentNotFound)
	}
	return nil
}

// DeleteMoment 仅允许动态作者删除自己的动态。
func (s *MomentService) DeleteMoment(ctx context.Context, userID, momentID int64) error {
	moment, err := s.mysqlRepo.GetMomentByID(ctx, momentID)
	if err != nil {
		return fmt.Errorf("获取动态: %w", err)
	}
	if moment == nil {
		return fmt.Errorf(ErrMomentNotFound)
	}
	if moment.AuthorID != userID {
		return fmt.Errorf(ErrNotMomentOwner)
	}
	if err := s.mysqlRepo.DeleteMoment(ctx, momentID); err != nil {
		if errors.Is(err, repository.ErrMomentNotFound) {
			return fmt.Errorf(ErrMomentNotFound)
		}
		return fmt.Errorf("删除动态: %w", err)
	}
	return nil
}

// enrichLikes 为一批动态填充已提交的点赞数和当前用户状态。
func (s *MomentService) enrichLikes(ctx context.Context, viewerID int64, moments []model.Moment) error {
	if len(moments) == 0 {
		return nil
	}
	ids := make([]int64, len(moments))
	for i := range moments {
		ids[i] = moments[i].ID
	}
	stats, err := s.mysqlRepo.GetMomentLikeStats(ctx, viewerID, ids)
	if err != nil {
		return err
	}
	for i := range moments {
		stat := stats[moments[i].ID]
		moments[i].LikeCount = stat.LikeCount
		moments[i].LikedByMe = stat.LikedByMe
	}
	return nil
}

// CommentMoment 在动态上创建一条评论。
func (s *MomentService) CommentMoment(ctx context.Context, userID int64, momentID int64, content string) (int64, error) {
	if strings.TrimSpace(content) == "" {
		return 0, fmt.Errorf(ErrMomentContentEmpty)
	}
	if utf8.RuneCountInString(content) > maxMomentContentRunes {
		return 0, fmt.Errorf(ErrMomentContentTooLong)
	}

	// 验证动态是否存在
	moment, err := s.mysqlRepo.GetMomentByID(ctx, momentID)
	if err != nil {
		return 0, fmt.Errorf("检查动态: %w", err)
	}
	if moment == nil {
		return 0, fmt.Errorf(ErrMomentNotFound)
	}
	visible, err := s.canViewMoment(ctx, userID, moment)
	if err != nil {
		return 0, fmt.Errorf("检查动态可见性: %w", err)
	}
	if !visible {
		return 0, fmt.Errorf(ErrMomentNotFound)
	}

	comment := &model.MomentComment{
		MomentID:  momentID,
		UserID:    userID,
		Content:   content,
		CreatedAt: time.Now(),
	}

	if err := s.mysqlRepo.CreateMomentComment(ctx, comment); err != nil {
		return 0, fmt.Errorf("创建评论: %w", err)
	}

	return comment.ID, nil
}

// DeleteComment 在删除评论前验证用户是否拥有该评论。
func (s *MomentService) DeleteComment(ctx context.Context, userID int64, commentID int64) error {
	// 获取评论以验证所有权
	comment, err := s.mysqlRepo.GetMomentCommentByID(ctx, commentID)
	if err != nil {
		return fmt.Errorf("获取评论: %w", err)
	}
	if comment == nil {
		return fmt.Errorf(ErrCommentNotFound)
	}
	if comment.UserID != userID {
		return fmt.Errorf(ErrNotCommentOwner)
	}

	if err := s.mysqlRepo.DeleteMomentComment(ctx, commentID); err != nil {
		return fmt.Errorf("删除评论: %w", err)
	}
	return nil
}

// GetFeed 以推拉结合方式获取用户的朋友圈 Feed：
//   - 推：读取用户自己的收件箱 timeline:{userID}（普通好友写扩散来的动态）。
//   - 拉：读取用户自己的寄件箱 + 大V好友的寄件箱 moment_outbox:{bigFriend}。
//
// 各源按 score(时间戳) 降序取一页，Go 内归并、按 momentID 去重，
// 用复合游标 (ts,id) 分页规避重复/漏读与深分页衰减。
// 返回补全后的动态列表与 nextCursor（空串表示无更多）。
func (s *MomentService) GetFeed(ctx context.Context, userID int64, cursor string, limit int) ([]model.Moment, string, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	maxTs, maxID, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", fmt.Errorf("无效的游标: %w", err)
	}

	// ── 1. 确定所有 Feed 源 ──
	// pull=true 读寄件箱（自己 + 大V好友），pull=false 读收件箱（自己）。
	type feedSource struct {
		userID int64
		pull   bool
	}
	sources := []feedSource{
		{userID: userID, pull: true},  // 自己的寄件箱（自见）
		{userID: userID, pull: false}, // 自己的收件箱（普通好友推来的）
	}

	friends, err := s.mysqlRepo.GetFriendList(ctx, userID)
	if err != nil {
		return nil, "", fmt.Errorf("获取好友列表: %w", err)
	}
	if len(friends) > 0 {
		friendIDs := make([]int64, 0, len(friends))
		for _, fs := range friends {
			if fs.FriendID != userID {
				friendIDs = append(friendIDs, fs.FriendID)
			}
		}
		bigFriends, err := s.redisRepo.FilterBigUsers(ctx, friendIDs)
		if err != nil {
			return nil, "", fmt.Errorf("筛选大V好友: %w", err)
		}
		for _, bf := range bigFriends {
			sources = append(sources, feedSource{userID: bf, pull: true}) // 大V好友的寄件箱（拉取）
		}
	}

	// ── 2. 各源取一页并归并 ──
	// 每源多取 1 条用于判定是否还有下一页。
	seen := make(map[int64]struct{})
	merged := make([]model.FeedEntry, 0, limit*2)
	for _, src := range sources {
		var entries []model.FeedEntry
		var err error
		if src.pull {
			entries, err = s.redisRepo.GetOutboxPage(ctx, src.userID, maxTs, maxID, limit+1)
		} else {
			entries, err = s.redisRepo.GetTimelinePage(ctx, src.userID, maxTs, maxID, limit+1)
		}
		if err != nil {
			s.logger.Warn("读取 Feed 源失败",
				zap.Int64("srcUserID", src.userID), zap.Bool("pull", src.pull), zap.Error(err))
			continue // 单源失败不影响其它源
		}
		// 边界过滤（严格早于游标）已在 repo 层完成，这里只需按 momentID 去重跨源合并。
		for _, e := range entries {
			if _, dup := seen[e.MomentID]; dup {
				continue
			}
			seen[e.MomentID] = struct{}{}
			merged = append(merged, e)
		}
	}

	if len(merged) == 0 {
		return []model.Moment{}, "", nil
	}

	// ── 3. 按 (ts desc, id desc) 排序，取 limit 条 ──
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Ts != merged[j].Ts {
			return merged[i].Ts > merged[j].Ts
		}
		return merged[i].MomentID > merged[j].MomentID
	})

	nextCursor := ""
	if len(merged) > limit {
		last := merged[limit-1]
		nextCursor = encodeCursor(last.Ts, last.MomentID)
		merged = merged[:limit]
	}

	// ── 4. 批量补全动态详情（消除 N+1）──
	ids := make([]int64, len(merged))
	for i, e := range merged {
		ids[i] = e.MomentID
	}
	rows, err := s.mysqlRepo.GetMomentsByIDs(ctx, ids)
	if err != nil {
		return nil, "", fmt.Errorf("批量补全动态: %w", err)
	}
	byID := make(map[int64]model.Moment, len(rows))
	for _, m := range rows {
		byID[m.ID] = m
	}

	// 按归并顺序重排，并使用与详情、用户动态列表相同的可见性规则。
	moments := make([]model.Moment, 0, len(merged))
	for _, e := range merged {
		m, ok := byID[e.MomentID]
		if !ok {
			continue // 动态已删除，读时容错跳过
		}
		visible, err := s.canViewMoment(ctx, userID, &m)
		if err != nil {
			return nil, "", fmt.Errorf("检查动态可见性: %w", err)
		}
		if !visible {
			continue
		}
		moments = append(moments, m)
	}

	// 富化点赞数与"我是否已赞"
	if err := s.enrichLikes(ctx, userID, moments); err != nil {
		return nil, "", err
	}
	s.enrichAuthors(ctx, moments)
	s.enrichComments(ctx, moments)

	return moments, nextCursor, nil
}

// ── 游标编解码 ──
// 游标为 base64("{ts}_{id}")，复合键 (时间戳ms, momentID) 保证同毫秒多条也能稳定分界。

// encodeCursor 将 (ts,id) 编码为不透明游标字符串。
func encodeCursor(ts int64, id int64) string {
	raw := strconv.FormatInt(ts, 10) + "_" + strconv.FormatInt(id, 10)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor 解析游标字符串，返回 (maxTs, maxID)。
// 空游标表示首页，返回 (-1, 0)：maxTs<0 由 Redis 层识别为"从最新开始"。
func decodeCursor(cursor string) (int64, int64, error) {
	if cursor == "" {
		return -1, 0, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, 0, fmt.Errorf("游标 base64 解码: %w", err)
	}
	parts := strings.SplitN(string(data), "_", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("游标格式错误")
	}
	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("游标时间戳解析: %w", err)
	}
	id, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("游标ID解析: %w", err)
	}
	return ts, id, nil
}
