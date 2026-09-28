package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/service"
)

// FriendHandler 提供好友相关端点的 Gin HTTP 处理器。
type FriendHandler struct {
	friendSvc *service.FriendService
	rdb       *redis.Client
}

// NewFriendHandler 创建一个 FriendHandler，封装给定的 FriendService。
func NewFriendHandler(friendSvc *service.FriendService, rdb *redis.Client) *FriendHandler {
	return &FriendHandler{friendSvc: friendSvc, rdb: rdb}
}

// ── 请求 / 响应 DTO ──

type sendFriendRequestReq struct {
	ToUserID int64  `json:"to_user_id" binding:"required"`
	Message  string `json:"message"`
}

type sendFriendRequestResp struct {
	RequestID  int64 `json:"request_id"`
	FromUserID int64 `json:"from_user_id"`
	ToUserID   int64 `json:"to_user_id"`
	Status     int   `json:"status"`
}

type acceptFriendRequestReq struct {
	RequestID int64 `json:"request_id" binding:"required"`
}

type acceptFriendRequestResp struct {
	UserID   int64 `json:"user_id"`
	FriendID int64 `json:"friend_id"`
}

type rejectFriendRequestReq struct {
	RequestID int64 `json:"request_id" binding:"required"`
}

type blockUserReq struct {
	BlockedID int64 `json:"blocked_id" binding:"required"`
}

type unblockUserReq struct {
	BlockedID int64 `json:"blocked_id" binding:"required"`
}

// ── 处理器 ──

// SendFriendRequest godoc
// @Summary      发送好友申请
// @Description  向指定用户发送好友申请
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  sendFriendRequestReq  true  "申请信息"
// @Success      201   {object}  ApiResponse{data=sendFriendRequestResp}  "申请发送成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      409   {object}  ApiResponse  "已是好友或重复申请"
// @Router       /friend/request [post]
// SendFriendRequest 处理 POST /friend/request。
func (h *FriendHandler) SendFriendRequest(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req sendFriendRequestReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "to_user_id is required")
		return
	}

	friendReq, err := h.friendSvc.SendFriendRequest(c.Request.Context(), userID, req.ToUserID, req.Message)
	if err != nil {
		switch err.Error() {
		case service.ErrSelfRequest:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrAlreadyFriends:
			ServiceError(c, http.StatusConflict, err.Error())
		case service.ErrFriendBlocked:
			ServiceError(c, http.StatusForbidden, err.Error())
		case service.ErrDuplicateRequest:
			ServiceError(c, http.StatusConflict, err.Error())
		case service.ErrUserNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	SuccessCreated(c, sendFriendRequestResp{
		RequestID:  friendReq.ID,
		FromUserID: friendReq.FromUserID,
		ToUserID:   friendReq.ToUserID,
		Status:     friendReq.Status,
	})
}

// AcceptFriendRequest godoc
// @Summary      接受好友申请
// @Description  接受指定ID的好友申请
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  acceptFriendRequestReq  true  "接受信息"
// @Success      200   {object}  ApiResponse{data=acceptFriendRequestResp}  "接受成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      404   {object}  ApiResponse  "申请不存在"
// @Failure      403   {object}  ApiResponse  "无权操作"
// @Router       /friend/accept [post]
// AcceptFriendRequest handles POST /friend/accept.
func (h *FriendHandler) AcceptFriendRequest(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req acceptFriendRequestReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "request_id is required")
		return
	}

	fs, err := h.friendSvc.AcceptFriendRequest(c.Request.Context(), userID, req.RequestID)
	if err != nil {
		switch err.Error() {
		case service.ErrRequestNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		case service.ErrNotRequestTarget:
			ServiceError(c, http.StatusForbidden, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	Success(c, acceptFriendRequestResp{
		UserID:   fs.UserID,
		FriendID: fs.FriendID,
	})
}

// RejectFriendRequest godoc
// @Summary      拒绝好友申请
// @Description  拒绝指定ID的好友申请
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  rejectFriendRequestReq  true  "拒绝信息"
// @Success      200   {object}  ApiResponse  "拒绝成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      404   {object}  ApiResponse  "申请不存在"
// @Failure      403   {object}  ApiResponse  "无权操作"
// @Router       /friend/reject [post]
// RejectFriendRequest handles POST /friend/reject.
func (h *FriendHandler) RejectFriendRequest(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req rejectFriendRequestReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "request_id is required")
		return
	}

	err := h.friendSvc.RejectFriendRequest(c.Request.Context(), userID, req.RequestID)
	if err != nil {
		switch err.Error() {
		case service.ErrRequestNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		case service.ErrNotRequestTarget:
			ServiceError(c, http.StatusForbidden, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	SuccessMessage(c, "friend request rejected")
}

// GetFriendRequests godoc
// @Summary      获取好友申请列表
// @Description  获取当前用户的待处理好友申请列表
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200   {object}  ApiResponse{data=object}  "获取成功"
// @Failure      500   {object}  ApiResponse  "服务器内部错误"
// @Router       /friend/requests [get]
// GetFriendRequests handles GET /friend/requests.
func (h *FriendHandler) GetFriendRequests(c *gin.Context) {
	userID := c.GetInt64("userID")

	limit := 20
	offset := 0
	if l, err := strconv.Atoi(c.DefaultQuery("limit", "20")); err == nil && l > 0 && l <= 100 {
		limit = l
	}
	if o, err := strconv.Atoi(c.DefaultQuery("offset", "0")); err == nil && o >= 0 {
		offset = o
	}

	requests, err := h.friendSvc.GetFriendRequestList(c.Request.Context(), userID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	total := int64(len(requests))

	// Apply offset/limit slicing
	if offset > len(requests) {
		offset = len(requests)
	}
	end := offset + limit
	if end > len(requests) {
		end = len(requests)
	}
	paged := requests[offset:end]

	PaginatedSuccess(c, paged, total, offset, limit)
}

// GetFriendList godoc
// @Summary      获取好友列表
// @Description  获取当前用户的好友列表
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Success      200   {object}  ApiResponse{data=object}  "获取成功"
// @Failure      500   {object}  ApiResponse  "服务器内部错误"
// @Router       /friend/list [get]
// GetFriendList handles GET /friend/list.
func (h *FriendHandler) GetFriendList(c *gin.Context) {
	userID := c.GetInt64("userID")

	limit := 20
	offset := 0
	if l, err := strconv.Atoi(c.DefaultQuery("limit", "20")); err == nil && l > 0 && l <= 100 {
		limit = l
	}
	if o, err := strconv.Atoi(c.DefaultQuery("offset", "0")); err == nil && o >= 0 {
		offset = o
	}

	friends, err := h.friendSvc.GetFriendList(c.Request.Context(), userID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	total := int64(len(friends))

	// Apply offset/limit slicing
	if offset > len(friends) {
		offset = len(friends)
	}
	end := offset + limit
	if end > len(friends) {
		end = len(friends)
	}
	paged := friends[offset:end]
	for index := range paged {
		friend, err := h.friendSvc.GetUserByID(c.Request.Context(), paged[index].FriendID)
		if err == nil && friend != nil {
			paged[index].Nickname = friend.Username
			paged[index].AvatarURL = friend.AvatarURL
		}
		if h.rdb != nil {
			online, err := h.rdb.Exists(c.Request.Context(), "online:"+strconv.FormatInt(paged[index].FriendID, 10)).Result()
			if err == nil {
				paged[index].Online = online == 1
			}
		}
	}

	PaginatedSuccess(c, paged, total, offset, limit)
}

// DeleteFriend godoc
// @Summary      删除好友
// @Description  删除指定ID的好友关系
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        friendID  path  int64  true  "好友ID"
// @Success      200       {object}  ApiResponse  "删除成功"
// @Failure      400       {object}  ApiResponse  "参数错误"
// @Failure      500       {object}  ApiResponse  "服务器内部错误"
// @Router       /friend/{friendID} [delete]
// DeleteFriend handles DELETE /friend/:friendID.
func (h *FriendHandler) DeleteFriend(c *gin.Context) {
	userID := c.GetInt64("userID")

	friendIDStr := c.Param("friendID")
	friendID, err := strconv.ParseInt(friendIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "invalid friendID")
		return
	}

	err = h.friendSvc.DeleteFriend(c.Request.Context(), userID, friendID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	SuccessMessage(c, "friend deleted")
}

// BlockUser godoc
// @Summary      拉黑用户
// @Description  将指定用户加入黑名单
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  blockUserReq  true  "拉黑信息"
// @Success      200   {object}  ApiResponse  "拉黑成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      409   {object}  ApiResponse  "已拉黑"
// @Router       /friend/block [post]
// BlockUser handles POST /friend/block.
func (h *FriendHandler) BlockUser(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req blockUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "blocked_id is required")
		return
	}

	err := h.friendSvc.BlockUser(c.Request.Context(), userID, req.BlockedID)
	if err != nil {
		switch err.Error() {
		case service.ErrAlreadyBlocked:
			ServiceError(c, http.StatusConflict, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	SuccessMessage(c, "user blocked")
}

// UnblockUser godoc
// @Summary      取消拉黑
// @Description  将指定用户从黑名单中移除
// @Tags         好友
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  unblockUserReq  true  "取消拉黑信息"
// @Success      200   {object}  ApiResponse  "取消拉黑成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Router       /friend/unblock [post]
// UnblockUser handles POST /friend/unblock.
func (h *FriendHandler) UnblockUser(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req unblockUserReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "blocked_id is required")
		return
	}

	err := h.friendSvc.UnblockUser(c.Request.Context(), userID, req.BlockedID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	SuccessMessage(c, "user unblocked")
}

// RegisterRoutes registers all friend HTTP routes on the given Gin router group.
func (h *FriendHandler) RegisterRoutes(rg *gin.RouterGroup) {
	friend := rg.Group("/friend")
	friend.POST("/request", h.SendFriendRequest)
	friend.POST("/accept", h.AcceptFriendRequest)
	friend.POST("/reject", h.RejectFriendRequest)
	friend.GET("/requests", h.GetFriendRequests)
	friend.GET("/list", h.GetFriendList)
	friend.DELETE("/:friendID", h.DeleteFriend)
	friend.POST("/block", h.BlockUser)
	friend.POST("/unblock", h.UnblockUser)
}
