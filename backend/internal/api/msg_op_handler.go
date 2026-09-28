package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/service"
)

// MsgOpHandler 为消息操作端点提供 Gin HTTP 处理器。
type MsgOpHandler struct {
	msgOpSvc *service.MsgOpService
}

// NewMsgOpHandler 创建一个 MsgOpHandler，封装给定的 MsgOpService。
func NewMsgOpHandler(msgOpSvc *service.MsgOpService) *MsgOpHandler {
	return &MsgOpHandler{msgOpSvc: msgOpSvc}
}

// ── 请求 / 响应 DTO ──

type revokeMsgReq struct {
	ConvID string `json:"convId" binding:"required"`
	MsgID  int64  `json:"msgId" binding:"required"`
}

type searchMsgReq struct {
	Query  string `form:"q" binding:"required"`
	Limit  int    `form:"limit,default=20"`
	Offset int    `form:"offset,default=0"`
}

// ── 处理器 ──

// RevokeMessage godoc
// @Summary      撤回消息
// @Description  在2分钟内撤回已发送的消息，仅发送者可撤回
// @Tags         消息操作
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  revokeMsgReq  true  "撤回请求"
// @Success      200   {object}  ApiResponse  "撤回成功"
// @Failure      400   {object}  ApiResponse  "消息不可撤回"
// @Failure      403   {object}  ApiResponse  "非发送者"
// @Router       /msg/revoke [post]
func (h *MsgOpHandler) RevokeMessage(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req revokeMsgReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convId and msgId are required")
		return
	}

	err := h.msgOpSvc.RevokeMessage(c.Request.Context(), userID, req.ConvID, req.MsgID)
	if err != nil {
		switch err.Error() {
		case service.ErrMsgNotRevocable:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrMsgRevokeNotOwner:
			ServiceError(c, http.StatusForbidden, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	SuccessMessage(c, "message revoked")
}

// DeleteMessage godoc
// @Summary      删除消息
// @Description  删除指定的消息
// @Tags         消息操作
// @Produce      json
// @Security     BearerAuth
// @Param        msgID   path   string  true  "消息ID"
// @Param        convId  query  string  true  "会话ID"
// @Success      200     {object}  ApiResponse  "删除成功"
// @Failure      400     {object}  ApiResponse  "参数错误"
// @Router       /msg/{msgID} [delete]
func (h *MsgOpHandler) DeleteMessage(c *gin.Context) {
	userID := c.GetInt64("userID")

	msgIDStr := c.Param("msgID")
	msgID, err := strconv.ParseInt(msgIDStr, 10, 64)
	if err != nil {
		Error(c, http.StatusBadRequest, CodeInvalidParam, "invalid msgID")
		return
	}

	convID := c.Query("convId")
	if convID == "" {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convId query param is required")
		return
	}

	err = h.msgOpSvc.DeleteMessage(c.Request.Context(), userID, convID, msgID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, err.Error())
		return
	}

	SuccessMessage(c, "message deleted")
}

// SearchMessages godoc
// @Summary      搜索消息
// @Description  根据关键词搜索用户的消息记录
// @Tags         消息操作
// @Produce      json
// @Security     BearerAuth
// @Param        q       query  string  true   "搜索关键词"
// @Param        limit   query  int     false  "返回数量限制"
// @Param        offset  query  int     false  "分页偏移量"
// @Success      200     {object}  ApiResponse  "搜索结果"
// @Router       /msg/search [get]
func (h *MsgOpHandler) SearchMessages(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req searchMsgReq
	if err := c.ShouldBindQuery(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "search query (q) is required")
		return
	}

	msgs, err := h.msgOpSvc.SearchMessages(c.Request.Context(), userID, req.Query, req.Limit, req.Offset)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	// Service already applies limit/offset; use has-more inference
	total := int64(req.Offset + len(msgs))
	if len(msgs) == req.Limit {
		total = int64(req.Offset + req.Limit + 1) // signal has more
	}
	PaginatedSuccess(c, msgs, total, req.Offset, req.Limit)
}

// RegisterRoutes registers all message operation HTTP routes on the given Gin router group.
func (h *MsgOpHandler) RegisterRoutes(rg *gin.RouterGroup) {
	msg := rg.Group("/msg")
	msg.POST("/revoke", h.RevokeMessage)
	msg.DELETE("/:msgID", h.DeleteMessage)
	msg.GET("/search", h.SearchMessages)
}
