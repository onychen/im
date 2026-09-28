package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// AvatarHandler 提供头像端点的 Gin HTTP 处理函数。
type AvatarHandler struct{}

// NewAvatarHandler 创建一个 AvatarHandler。
func NewAvatarHandler() *AvatarHandler {
	return &AvatarHandler{}
}

// GetAvatar godoc
// @Summary      获取用户头像
// @Description  返回用户头像图片。如果用户未设置头像，自动生成包含用户名首字母的 SVG 占位头像
// @Tags         用户
// @Produce      image/svg+xml
// @Param        userID  path  int64  true  "用户ID"
// @Param        name    query string false "用户名（用于生成默认头像首字母）"
// @Success      200  {string}  string  "头像图片（SVG或重定向到图片URL）"
// @Router       /avatar/{userID} [get]
func (h *AvatarHandler) GetAvatar(c *gin.Context) {
	// 这是公开端点，无需认证
	// userIDStr := c.Param("userID")
	// userID, _ := strconv.ParseInt(userIDStr, 10, 64)

	// 获取用户名用于生成首字母（从查询参数，前端传入）
	name := c.DefaultQuery("name", "?")

	// 生成首字母（取第一个字符）
	initial := strings.ToUpper(name[:1])
	if initial == "" {
		initial = "?"
	}

	// 根据首字母生成稳定的颜色
	color := initialColor(initial)

	// 生成 SVG 占位头像
	svg := fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" viewBox="0 0 200 200">
  <rect width="200" height="200" fill="%s"/>
  <text x="100" y="135" font-family="Arial, sans-serif" font-size="100" font-weight="bold" fill="#fff" text-anchor="middle">%s</text>
</svg>`, color, initial)

	c.Header("Content-Type", "image/svg+xml")
	c.Header("Cache-Control", "public, max-age=86400")
	c.String(http.StatusOK, svg)
}

// RegisterRoutes 在公开路由组上注册头像路由。
func (h *AvatarHandler) RegisterRoutes(rg *gin.RouterGroup) {
	rg.GET("/avatar/:userID", h.GetAvatar)
}

// initialColor 根据首字母返回稳定的颜色（Material Design 调色板）。
func initialColor(initial string) string {
	colors := map[string]string{
		"A": "#F44336", "B": "#E91E63", "C": "#9C27B0", "D": "#673AB7",
		"E": "#3F51B5", "F": "#2196F3", "G": "#03A9F4", "H": "#00BCD4",
		"I": "#009688", "J": "#4CAF50", "K": "#8BC34A", "L": "#CDDC39",
		"M": "#FF9800", "N": "#FF5722", "O": "#795548", "P": "#607D8B",
		"Q": "#E53935", "R": "#D81B60", "S": "#8E24AA", "T": "#5E35B1",
		"U": "#3949AB", "V": "#1E88E5", "W": "#039BE5", "X": "#00ACC1",
		"Y": "#00897B", "Z": "#43A047",
	}
	if c, ok := colors[initial]; ok {
		return c
	}
	return "#9E9E9E"
}
