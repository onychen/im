package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/service"
)

// MomentHandler 提供朋友圈的 Gin HTTP 处理函数。
type MomentHandler struct {
	momentSvc *service.MomentService
}

// NewMomentHandler 创建一个包装给定 MomentService 的 MomentHandler。
func NewMomentHandler(momentSvc *service.MomentService) *MomentHandler {
	return &MomentHandler{momentSvc: momentSvc}
}

// ── 请求/响应 DTO ──

type publishMomentRequest struct {
	Content    string  `json:"content"`
	MediaUrls  *string `json:"media_urls,omitempty"` // 可空的 JSON 字符串
	Visibility *int    `json:"visibility,omitempty"` // nil=默认好友可见, 2=好友, 3=仅自己
}

type publishMomentResponse struct {
	MomentID int64 `json:"moment_id"`
}

type getMomentResponse struct {
	ID           int64                 `json:"id"`
	AuthorID     int64                 `json:"author_id"`
	Content      string                `json:"content"`
	MediaUrls    *string               `json:"media_urls,omitempty"`
	Visibility   int                   `json:"visibility"`
	CreatedAt    string                `json:"created_at"`  // RFC3339 格式
	LikeCount    int64                 `json:"like_count"`  // 点赞数
	LikedByMe    bool                  `json:"liked_by_me"` // 当前用户是否已赞
	AuthorName   string                `json:"author_name"`
	AuthorAvatar string                `json:"author_avatar"`
	Comments     []model.MomentComment `json:"comments"`
}

type likeMomentResponse struct {
	Ok    bool  `json:"ok"`
	Liked bool  `json:"liked"` // 操作后是否处于已赞状态
	Count int64 `json:"count"` // 最新点赞数
}

type unlikeMomentResponse struct {
	Ok    bool  `json:"ok"`
	Liked bool  `json:"liked"`
	Count int64 `json:"count"`
}

type commentMomentRequest struct {
	Content string `json:"content"`
}

type commentMomentResponse struct {
	CommentID int64 `json:"comment_id"`
}

type deleteCommentResponse struct {
	Ok bool `json:"ok"`
}

type momentLikersResponse struct {
	Items []model.MomentLiker `json:"items"`
}

type deleteMomentResponse struct {
	Ok bool `json:"ok"`
}

type momentFeedResponse struct {
	Moments    []getMomentResponse `json:"moments"`
	NextCursor string              `json:"next_cursor"` // 空串表示无更多
}

// ── 处理函数 ──

// PublishMoment godoc
// @Summary      发布动态
// @Description  发布一条新的朋友圈动态
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  publishMomentRequest  true  "动态内容"
// @Success      201   {object}  ApiResponse{data=publishMomentResponse}  "发布成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Router       /moment [post]
// PublishMoment 处理 POST /api/v1/moment 请求。
func (h *MomentHandler) PublishMoment(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	var req publishMomentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "内容不能为空")
		return
	}

	// 如果未指定，默认好友可见。
	visibility := 2
	if req.Visibility != nil {
		visibility = *req.Visibility
	}

	momentID, err := h.momentSvc.PublishMoment(c.Request.Context(), userID.(int64), req.Content, req.MediaUrls, visibility)
	if err != nil {
		switch err.Error() {
		case service.ErrMomentContentEmpty, service.ErrMomentContentTooLong:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrInvalidVisibility:
			ServiceError(c, http.StatusBadRequest, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	SuccessCreated(c, publishMomentResponse{MomentID: momentID})
}

// GetMoment godoc
// @Summary      获取动态详情
// @Description  根据动态ID获取单条动态的详细信息，包含点赞状态
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Success      200  {object}  ApiResponse{data=getMomentResponse}  "查询成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Failure      404  {object}  ApiResponse  "动态不存在"
// @Router       /moment/{momentID} [get]
// GetMoment 处理 GET /api/v1/moment/:momentID 请求。
func (h *MomentHandler) GetMoment(c *gin.Context) {
	viewerID, _ := c.Get("userID") // 受保护路由，一定存在；用于计算 liked_by_me

	momentIDStr := c.Param("momentID")
	momentID, err := strconv.ParseInt(momentIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}

	moment, err := h.momentSvc.GetMoment(c.Request.Context(), toInt64(viewerID), momentID)
	if err != nil {
		switch err.Error() {
		case service.ErrMomentNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	Success(c, getMomentResponse{
		ID:           moment.ID,
		AuthorID:     moment.AuthorID,
		Content:      moment.Content,
		MediaUrls:    moment.MediaUrls,
		Visibility:   moment.Visibility,
		CreatedAt:    moment.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		LikeCount:    moment.LikeCount,
		LikedByMe:    moment.LikedByMe,
		AuthorName:   moment.AuthorName,
		AuthorAvatar: moment.AuthorAvatar,
		Comments:     moment.Comments,
	})
}

// GetUserMoments godoc
// @Summary      获取用户动态列表
// @Description  获取指定用户发布的朋友圈动态列表，支持分页
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        userID  path   int64  true   "用户ID"
// @Param        limit   query  int    false  "每页数量（默认20）"
// @Param        offset  query  int    false  "偏移量（默认0）"
// @Success      200  {object}  ApiResponse{data=object}  "查询成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Router       /moment/user/{userID} [get]
// GetUserMoments 处理 GET /api/v1/moment/user/:userID 请求。
func (h *MomentHandler) GetUserMoments(c *gin.Context) {
	viewerID, _ := c.Get("userID")

	userIDStr := c.Param("userID")
	userID, err := strconv.ParseInt(userIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的用户ID")
		return
	}

	limit := 20
	offset := 0
	if l, err := strconv.Atoi(c.DefaultQuery("limit", "20")); err == nil && l > 0 {
		limit = l
	}
	if o, err := strconv.Atoi(c.DefaultQuery("offset", "0")); err == nil && o >= 0 {
		offset = o
	}

	moments, err := h.momentSvc.GetUserMoments(c.Request.Context(), toInt64(viewerID), userID, limit, offset)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		return
	}

	response := make([]getMomentResponse, 0, len(moments))
	for _, m := range moments {
		response = append(response, getMomentResponse{
			ID:           m.ID,
			AuthorID:     m.AuthorID,
			Content:      m.Content,
			MediaUrls:    m.MediaUrls,
			Visibility:   m.Visibility,
			CreatedAt:    m.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			LikeCount:    m.LikeCount,
			LikedByMe:    m.LikedByMe,
			AuthorName:   m.AuthorName,
			AuthorAvatar: m.AuthorAvatar,
			Comments:     m.Comments,
		})
	}

	// Service already applies limit/offset; use has-more inference
	total := int64(offset + len(response))
	if len(response) == limit {
		total = int64(offset + limit + 1) // signal has more
	}
	PaginatedSuccess(c, response, total, offset, limit)
}

// LikeMoment godoc
// @Summary      点赞动态
// @Description  对指定动态进行点赞
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Success      200  {object}  ApiResponse{data=likeMomentResponse}  "点赞成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Failure      404  {object}  ApiResponse  "动态不存在"
// @Router       /moment/{momentID}/like [post]
// LikeMoment 处理 POST /api/v1/moment/:momentID/like 请求。
func (h *MomentHandler) LikeMoment(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	momentIDStr := c.Param("momentID")
	momentID, err := strconv.ParseInt(momentIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}

	count, err := h.momentSvc.LikeMoment(c.Request.Context(), userID.(int64), momentID)
	if err != nil {
		switch err.Error() {
		case service.ErrMomentNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	Success(c, likeMomentResponse{Ok: true, Liked: true, Count: count})
}

// UnlikeMoment godoc
// @Summary      取消点赞
// @Description  取消对指定动态的点赞
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Success      200  {object}  ApiResponse{data=unlikeMomentResponse}  "取消点赞成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Router       /moment/{momentID}/like [delete]
// UnlikeMoment 处理 DELETE /api/v1/moment/:momentID/like 请求。
func (h *MomentHandler) UnlikeMoment(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	momentIDStr := c.Param("momentID")
	momentID, err := strconv.ParseInt(momentIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}

	count, err := h.momentSvc.UnlikeMoment(c.Request.Context(), userID.(int64), momentID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		return
	}

	Success(c, unlikeMomentResponse{Ok: true, Liked: false, Count: count})
}

// GetMomentLikers godoc
// @Summary      获取动态点赞用户
// @Description  获取当前用户可见动态的点赞用户资料
// @Tags         朋友圈
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Success      200  {object}  ApiResponse{data=momentLikersResponse}
// @Failure      404  {object}  ApiResponse
// @Router       /moment/{momentID}/likers [get]
func (h *MomentHandler) GetMomentLikers(c *gin.Context) {
	momentID, err := strconv.ParseInt(c.Param("momentID"), 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}
	likers, err := h.momentSvc.GetMomentLikers(c.Request.Context(), c.GetInt64("userID"), momentID)
	if err != nil {
		if err.Error() == service.ErrMomentNotFound {
			ServiceError(c, http.StatusNotFound, err.Error())
		} else {
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}
	Success(c, momentLikersResponse{Items: likers})
}

// DeleteMoment godoc
// @Summary      删除自己的动态
// @Description  仅动态作者可以删除动态及其关联互动
// @Tags         朋友圈
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Success      200  {object}  ApiResponse{data=deleteMomentResponse}
// @Failure      403  {object}  ApiResponse
// @Failure      404  {object}  ApiResponse
// @Router       /moment/{momentID} [delete]
func (h *MomentHandler) DeleteMoment(c *gin.Context) {
	momentID, err := strconv.ParseInt(c.Param("momentID"), 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}
	if err := h.momentSvc.DeleteMoment(c.Request.Context(), c.GetInt64("userID"), momentID); err != nil {
		switch err.Error() {
		case service.ErrNotMomentOwner:
			ServiceError(c, http.StatusForbidden, err.Error())
		case service.ErrMomentNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}
	Success(c, deleteMomentResponse{Ok: true})
}

// CommentMoment godoc
// @Summary      评论动态
// @Description  对指定动态发表评论
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        momentID  path  int64  true  "动态ID"
// @Param        body      body  commentMomentRequest  true  "评论内容"
// @Success      201  {object}  ApiResponse{data=commentMomentResponse}  "评论成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Failure      404  {object}  ApiResponse  "动态不存在"
// @Router       /moment/{momentID}/comment [post]
// CommentMoment 处理 POST /api/v1/moment/:momentID/comment 请求。
func (h *MomentHandler) CommentMoment(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	momentIDStr := c.Param("momentID")
	momentID, err := strconv.ParseInt(momentIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的动态ID")
		return
	}

	var req commentMomentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "内容不能为空")
		return
	}

	commentID, err := h.momentSvc.CommentMoment(c.Request.Context(), userID.(int64), momentID, req.Content)
	if err != nil {
		switch err.Error() {
		case service.ErrMomentContentEmpty, service.ErrMomentContentTooLong:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrMomentNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	SuccessCreated(c, commentMomentResponse{CommentID: commentID})
}

// DeleteComment godoc
// @Summary      删除评论
// @Description  删除自己发表的评论
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        commentID  path  int64  true  "评论ID"
// @Success      200  {object}  ApiResponse{data=deleteCommentResponse}  "删除成功"
// @Failure      400  {object}  ApiResponse  "参数错误"
// @Failure      403  {object}  ApiResponse  "不是自己的评论"
// @Failure      404  {object}  ApiResponse  "评论不存在"
// @Router       /moment/comment/{commentID} [delete]
// DeleteComment 处理 DELETE /api/v1/moment/comment/:commentID 请求。
func (h *MomentHandler) DeleteComment(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	commentIDStr := c.Param("commentID")
	commentID, err := strconv.ParseInt(commentIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "无效的评论ID")
		return
	}

	if err := h.momentSvc.DeleteComment(c.Request.Context(), userID.(int64), commentID); err != nil {
		switch err.Error() {
		case service.ErrNotCommentOwner:
			ServiceError(c, http.StatusForbidden, err.Error())
		case service.ErrCommentNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	Success(c, deleteCommentResponse{Ok: true})
}

// GetFeed godoc
// @Summary      获取朋友圈动态流
// @Description  获取当前用户好友的朋友圈动态流，支持游标分页
// @Tags         朋友圈
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        cursor  query  string  false  "分页游标（首页传空）"
// @Param        limit   query  int     false  "每页数量（默认20）"
// @Success      200  {object}  ApiResponse{data=momentFeedResponse}  "查询成功"
// @Router       /moment/feed [get]
// GetFeed 处理 GET /api/v1/moment/feed 请求。
func (h *MomentHandler) GetFeed(c *gin.Context) {
	userID, exists := c.Get("userID")
	if !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	// 游标分页：首页传空 cursor，后续页传上一页返回的 next_cursor
	cursor := c.DefaultQuery("cursor", "")

	limit := 20
	if l, err := strconv.Atoi(c.DefaultQuery("limit", "20")); err == nil && l > 0 {
		limit = l
	}

	moments, nextCursor, err := h.momentSvc.GetFeed(c.Request.Context(), userID.(int64), cursor, limit)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		return
	}

	response := make([]getMomentResponse, 0, len(moments))
	for _, m := range moments {
		response = append(response, getMomentResponse{
			ID:           m.ID,
			AuthorID:     m.AuthorID,
			Content:      m.Content,
			MediaUrls:    m.MediaUrls,
			Visibility:   m.Visibility,
			CreatedAt:    m.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			LikeCount:    m.LikeCount,
			LikedByMe:    m.LikedByMe,
			AuthorName:   m.AuthorName,
			AuthorAvatar: m.AuthorAvatar,
			Comments:     m.Comments,
		})
	}

	Success(c, momentFeedResponse{Moments: response, NextCursor: nextCursor})
}

// RegisterRoutes 在给定的 Gin 路由组上注册所有朋友圈 HTTP 路由。
func (h *MomentHandler) RegisterRoutes(rg *gin.RouterGroup) {
	moment := rg.Group("/moment")
	moment.POST("", h.PublishMoment)
	moment.GET("/:momentID", h.GetMoment)
	moment.GET("/user/:userID", h.GetUserMoments)
	moment.POST("/:momentID/like", h.LikeMoment)
	moment.DELETE("/:momentID/like", h.UnlikeMoment)
	moment.GET("/:momentID/likers", h.GetMomentLikers)
	moment.DELETE("/:momentID", h.DeleteMoment)
	moment.POST("/:momentID/comment", h.CommentMoment)
	moment.DELETE("/comment/:commentID", h.DeleteComment)
	moment.GET("/feed", h.GetFeed)
}

// toInt64 安全地把 gin 上下文里的 userID（any）转成 int64；缺失/类型不符返回 0。
func toInt64(v interface{}) int64 {
	if id, ok := v.(int64); ok {
		return id
	}
	return 0
}
