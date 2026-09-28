package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/service"
)

// SettingsHandler 提供用户设置端点的 Gin HTTP 处理函数。
type SettingsHandler struct {
	settingsSvc *service.SettingsService
}

// NewSettingsHandler 创建一个包装给定 SettingsService 的 SettingsHandler。
func NewSettingsHandler(settingsSvc *service.SettingsService) *SettingsHandler {
	return &SettingsHandler{settingsSvc: settingsSvc}
}

// ── 请求 / 响应 DTO ──

type updateSettingsReq struct {
	NotificationEnabled bool   `json:"notification_enabled"`
	MsgPreviewEnabled   bool   `json:"msg_preview_enabled"`
	MuteList            string `json:"mute_list"`
}

type muteConvReq struct {
	ConvID string `json:"convId" binding:"required"`
}

// ── 处理函数 ──

// GetSettings godoc
// @Summary      获取用户设置
// @Description  获取当前用户的通知偏好和免打扰设置
// @Tags         设置
// @Produce      json
// @Security     BearerAuth
// @Success      200   {object}  ApiResponse{data=object}  "获取成功"
// @Router       /settings [get]
func (h *SettingsHandler) GetSettings(c *gin.Context) {
	userID := c.GetInt64("userID")

	settings, err := h.settingsSvc.GetSettings(c.Request.Context(), userID)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		return
	}

	Success(c, settings)
}

// UpdateSettings godoc
// @Summary      更新用户设置
// @Description  更新当前用户的通知偏好和免打扰设置
// @Tags         设置
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  updateSettingsReq  true  "设置参数"
// @Success      200   {object}  ApiResponse  "更新成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Router       /settings [put]
func (h *SettingsHandler) UpdateSettings(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req updateSettingsReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "无效的请求体")
		return
	}

	settings := &model.UserSettings{
		NotificationEnabled: req.NotificationEnabled,
		MsgPreviewEnabled:   req.MsgPreviewEnabled,
		MuteList:            req.MuteList,
	}

	err := h.settingsSvc.UpdateSettings(c.Request.Context(), userID, settings)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		return
	}

	SuccessMessage(c, "设置已更新")
}

// AddMuteConv godoc
// @Summary      添加免打扰会话
// @Description  将指定会话添加到免打扰列表
// @Tags         设置
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  muteConvReq  true  "会话ID"
// @Success      200   {object}  ApiResponse  "添加成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      409   {object}  ApiResponse  "会话已存在"
// @Router       /settings/mute [post]
func (h *SettingsHandler) AddMuteConv(c *gin.Context) {
	userID := c.GetInt64("userID")

	var req muteConvReq
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convId 为必填项")
		return
	}

	err := h.settingsSvc.AddMuteConv(c.Request.Context(), userID, req.ConvID)
	if err != nil {
		switch err.Error() {
		case service.ErrMuteConvExists:
			ServiceError(c, http.StatusConflict, err.Error())
		case service.ErrInvalidMuteConv:
			ServiceError(c, http.StatusBadRequest, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	SuccessMessage(c, "会话已静音")
}

// RemoveMuteConv godoc
// @Summary      移除免打扰会话
// @Description  将指定会话从免打扰列表中移除
// @Tags         设置
// @Produce      json
// @Security     BearerAuth
// @Param        convID  path  string  true  "会话ID"
// @Success      200     {object}  ApiResponse  "移除成功"
// @Failure      400     {object}  ApiResponse  "参数错误"
// @Failure      404     {object}  ApiResponse  "会话未找到"
// @Router       /settings/mute/{convID} [delete]
func (h *SettingsHandler) RemoveMuteConv(c *gin.Context) {
	userID := c.GetInt64("userID")

	convID := c.Param("convID")
	if convID == "" {
		Error(c, http.StatusBadRequest, CodeMissingParam, "convID 为必填项")
		return
	}

	err := h.settingsSvc.RemoveMuteConv(c.Request.Context(), userID, convID)
	if err != nil {
		switch err.Error() {
		case service.ErrMuteConvNotFound:
			ServiceError(c, http.StatusNotFound, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "内部错误")
		}
		return
	}

	SuccessMessage(c, "会话已取消静音")
}

// RegisterRoutes 在给定的 Gin 路由组上注册所有用户设置 HTTP 路由。
func (h *SettingsHandler) RegisterRoutes(rg *gin.RouterGroup) {
	settings := rg.Group("/settings")
	settings.GET("", h.GetSettings)
	settings.PUT("", h.UpdateSettings)
	settings.POST("/mute", h.AddMuteConv)
	settings.DELETE("/mute/:convID", h.RemoveMuteConv)
}
