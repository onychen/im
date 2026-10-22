package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/service"
)

// ConvHandler 提供会话相关的 HTTP 接口：会话列表、历史分页、已读、清空与删除。
// 实时链路（convSync / pullMsgs）走 WebSocket，这里是对应的 HTTP 版本，便于首屏加载与分页。
type ConvHandler struct {
	msgSvc *service.MsgService
}

// NewConvHandler 创建 ConvHandler。
func NewConvHandler(msgSvc *service.MsgService) *ConvHandler {
	return &ConvHandler{msgSvc: msgSvc}
}

type markReadReq struct {
	Seq int64 `json:"seq" binding:"required"`
}

// ListConversations godoc
// @Summary      会话列表
// @Description  返回当前用户全部可见会话，包含 maxSeq/readSeq/unread 与最后一条消息摘要
// @Tags         会话
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  ApiResponse  "data.conversations 为会话列表"
// @Router       /conversations [get]
func (h *ConvHandler) ListConversations(c *gin.Context) {
	userID := c.GetInt64("userID")
	views, err := h.msgSvc.ListConversations(c.Request.Context(), userID)
	if err != nil {
		msgServiceError(c, err)
		return
	}
	Success(c, gin.H{"conversations": views})
}

// GetHistory godoc
// @Summary      历史消息分页
// @Description  按 seq 倒序向前翻页；beforeSeq 为 0 表示从最新开始。返回结果按 seq 升序
// @Tags         会话
// @Produce      json
// @Security     BearerAuth
// @Param        convId     path   string  true   "会话ID"
// @Param        beforeSeq  query  int     false  "只返回 seq 小于该值的消息"
// @Param        limit      query  int     false  "每页数量，默认 30，最大 100"
// @Success      200  {object}  ApiResponse  "data.msgs 为消息列表，data.hasMore 表示是否还有更早的消息"
// @Router       /conversations/{convId}/messages [get]
func (h *ConvHandler) GetHistory(c *gin.Context) {
	userID := c.GetInt64("userID")
	convID := c.Param("convId")

	var beforeSeq int64
	if v := c.Query("beforeSeq"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			Error(c, http.StatusBadRequest, CodeInvalidParam, "invalid beforeSeq")
			return
		}
		beforeSeq = n
	}
	limit := 30
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			Error(c, http.StatusBadRequest, CodeInvalidParam, "invalid limit")
			return
		}
		limit = n
	}

	msgs, hasMore, err := h.msgSvc.GetHistory(c.Request.Context(), userID, convID, beforeSeq, limit)
	if err != nil {
		msgServiceError(c, err)
		return
	}
	Success(c, gin.H{"msgs": msgs, "hasMore": hasMore})
}

// MarkRead godoc
// @Summary      标记已读
// @Description  把会话已读位置推进到 seq（只前进，不回退）
// @Tags         会话
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        convId  path  string       true  "会话ID"
// @Param        body    body  markReadReq  true  "已读位置"
// @Success      200  {object}  ApiResponse  "成功"
// @Router       /conversations/{convId}/read [post]
func (h *ConvHandler) MarkRead(c *gin.Context) {
	userID := c.GetInt64("userID")
	var req markReadReq
	if err := c.ShouldBindJSON(&req); err != nil || req.Seq <= 0 {
		Error(c, http.StatusBadRequest, CodeMissingParam, "seq is required")
		return
	}
	if err := h.msgSvc.MarkRead(c.Request.Context(), userID, c.Param("convId"), req.Seq); err != nil {
		msgServiceError(c, err)
		return
	}
	SuccessMessage(c, "ok")
}

// ClearHistory godoc
// @Summary      清空聊天记录
// @Description  仅对自己清空，之后只能看到新消息
// @Tags         会话
// @Produce      json
// @Security     BearerAuth
// @Param        convId  path  string  true  "会话ID"
// @Success      200  {object}  ApiResponse  "成功"
// @Router       /conversations/{convId}/clear [post]
func (h *ConvHandler) ClearHistory(c *gin.Context) {
	userID := c.GetInt64("userID")
	if err := h.msgSvc.ClearHistory(c.Request.Context(), userID, c.Param("convId")); err != nil {
		msgServiceError(c, err)
		return
	}
	SuccessMessage(c, "history cleared")
}

// DeleteConversation godoc
// @Summary      删除会话
// @Description  清空记录并从会话列表隐藏；私聊有新消息时会重新出现
// @Tags         会话
// @Produce      json
// @Security     BearerAuth
// @Param        convId  path  string  true  "会话ID"
// @Success      200  {object}  ApiResponse  "成功"
// @Router       /conversations/{convId} [delete]
func (h *ConvHandler) DeleteConversation(c *gin.Context) {
	userID := c.GetInt64("userID")
	if err := h.msgSvc.DeleteConversation(c.Request.Context(), userID, c.Param("convId")); err != nil {
		msgServiceError(c, err)
		return
	}
	SuccessMessage(c, "conversation deleted")
}

// RegisterRoutes 注册会话路由。
func (h *ConvHandler) RegisterRoutes(rg *gin.RouterGroup) {
	conv := rg.Group("/conversations")
	conv.GET("", h.ListConversations)
	conv.GET("/:convId/messages", h.GetHistory)
	conv.POST("/:convId/read", h.MarkRead)
	conv.POST("/:convId/clear", h.ClearHistory)
	conv.DELETE("/:convId", h.DeleteConversation)
}
