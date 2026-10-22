package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/service"
)

// MsgOpHandler 为消息操作端点（撤回、删除、搜索）提供 Gin HTTP 处理器。
type MsgOpHandler struct {
	msgSvc *service.MsgService
}

// NewMsgOpHandler 创建一个 MsgOpHandler。
func NewMsgOpHandler(msgSvc *service.MsgService) *MsgOpHandler {
	return &MsgOpHandler{msgSvc: msgSvc}
}

// ── 请求 DTO ──

type revokeMsgReq struct {
	ConvID string `json:"convId" binding:"required"`
	Seq    int64  `json:"seq" binding:"required"`
}

type searchMsgReq struct {
	Query  string `form:"q" binding:"required"`
	ConvID string `form:"convId"`
	Limit  int    `form:"limit,default=20"`
}

// msgServiceError 把 MsgService 返回的业务错误映射为 HTTP 响应。
func msgServiceError(c *gin.Context, err error) {
	me := service.AsMsgError(err)
	status := http.StatusInternalServerError
	switch me.Code {
	case service.CodeMsgBadRequest:
		status = http.StatusBadRequest
	case service.CodeMsgNotRevocable, service.CodeMsgDuplicate:
		status = http.StatusBadRequest
	case service.CodeMsgNotOwner, service.CodeMsgNotFriend, service.CodeMsgBlocked,
		service.CodeMsgNotMember, service.CodeMsgMuted:
		status = http.StatusForbidden
	case service.CodeMsgConvNotFound:
		status = http.StatusNotFound
	}
	code := me.Code
	if code == service.CodeMsgInternal {
		code = CodeInternalError
	} else if code == service.CodeMsgBadRequest {
		code = CodeInvalidParam
	}
	Error(c, status, code, me.Message)
}

// ── 处理器 ──

// RevokeMessage godoc
// @Summary      撤回消息
// @Description  在 2 分钟内撤回自己发送的消息。撤回以一条 msgType=6 的通知消息写入会话，由 persist 消费者更新原消息状态
// @Tags         消息操作
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  revokeMsgReq  true  "撤回请求"
// @Success      200   {object}  ApiResponse  "撤回成功，data 为撤回通知的 serverAck"
// @Failure      400   {object}  ApiResponse  "消息不可撤回"
// @Failure      403   {object}  ApiResponse  "非发送者"
// @Router       /msg/revoke [post]
func (h *MsgOpHandler) RevokeMessage(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req revokeMsgReq
	if err := c.ShouldBindJSON(&req); err != nil || req.Seq <= 0 {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convId and seq are required")
		return
	}

	ack, err := h.msgSvc.RevokeMessage(c.Request.Context(), userID, req.ConvID, req.Seq)
	if err != nil {
		msgServiceError(c, err)
		return
	}
	Success(c, ack)
}

// DeleteMessage godoc
// @Summary      删除消息
// @Description  仅对自己删除指定消息（不影响会话中其他成员）
// @Tags         消息操作
// @Produce      json
// @Security     BearerAuth
// @Param        seq     path   int     true  "消息 seq"
// @Param        convId  query  string  true  "会话ID"
// @Success      200     {object}  ApiResponse  "删除成功"
// @Failure      400     {object}  ApiResponse  "参数错误"
// @Router       /msg/{seq} [delete]
func (h *MsgOpHandler) DeleteMessage(c *gin.Context) {
	userID := c.GetInt64("userID")

	seq, err := strconv.ParseInt(c.Param("seq"), 10, 64)
	if err != nil || seq <= 0 {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "invalid seq")
		return
	}
	convID := c.Query("convId")
	if convID == "" {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convId query param is required")
		return
	}

	if err := h.msgSvc.DeleteMessage(c.Request.Context(), userID, convID, seq); err != nil {
		msgServiceError(c, err)
		return
	}
	SuccessMessage(c, "message deleted")
}

// SearchMessages godoc
// @Summary      搜索消息
// @Description  在当前用户可见的文本消息中按关键词搜索，可限定会话
// @Tags         消息操作
// @Produce      json
// @Security     BearerAuth
// @Param        q       query  string  true   "搜索关键词"
// @Param        convId  query  string  false  "限定会话ID"
// @Param        limit   query  int     false  "返回数量限制，默认 20，最大 100"
// @Success      200     {object}  ApiResponse  "搜索结果"
// @Router       /msg/search [get]
func (h *MsgOpHandler) SearchMessages(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req searchMsgReq
	if err := c.ShouldBindQuery(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "search query (q) is required")
		return
	}

	msgs, err := h.msgSvc.SearchMessages(c.Request.Context(), userID, req.Query, req.ConvID, req.Limit)
	if err != nil {
		msgServiceError(c, err)
		return
	}
	Success(c, gin.H{"msgs": msgs})
}

// RegisterRoutes 注册消息操作路由。
func (h *MsgOpHandler) RegisterRoutes(rg *gin.RouterGroup) {
	msg := rg.Group("/msg")
	msg.POST("/revoke", h.RevokeMessage)
	msg.GET("/search", h.SearchMessages)
	msg.DELETE("/:seq", h.DeleteMessage)
}
