package api

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goim/goim/internal/repository"
)

// UploadHandler 提供文件上传端点的 Gin HTTP 处理函数。
type UploadHandler struct {
	uploadDir   string
	maxSizeMB   int
	allowedExts map[string]bool
	userRepo    repository.MySQLRepo
}

// NewUploadHandler 创建一个 UploadHandler。
// uploadDir 是文件存储根目录，maxSizeMB 是单文件大小上限，allowedExts 是允许的扩展名列表。
func NewUploadHandler(uploadDir string, maxSizeMB int, allowedExts []string, repos ...repository.MySQLRepo) *UploadHandler {
	extMap := make(map[string]bool, len(allowedExts))
	for _, ext := range allowedExts {
		extMap[strings.ToLower(ext)] = true
	}
	var userRepo repository.MySQLRepo
	if len(repos) > 0 {
		userRepo = repos[0]
	}
	return &UploadHandler{
		uploadDir:   uploadDir,
		maxSizeMB:   maxSizeMB,
		allowedExts: extMap,
		userRepo:    userRepo,
	}
}

// ── 请求/响应 DTO ──

type uploadAvatarResponse struct {
	URL      string `json:"url"`
	FilePath string `json:"file_path"`
	Size     int64  `json:"size"`
}

// ── 处理函数 ──

// UploadAvatar godoc
// @Summary      上传头像
// @Description  上传用户头像图片，支持 jpg/png/gif/webp 格式
// @Tags         文件
// @Accept       mpfd
// @Produce      json
// @Security     BearerAuth
// @Param        file  formData  file  true  "头像图片文件"
// @Success      200   {object}  ApiResponse{data=uploadAvatarResponse}  "上传成功"
// @Failure      400   {object}  ApiResponse  "文件格式不支持或文件过大"
// @Router       /upload/avatar [post]
func (h *UploadHandler) UploadAvatar(c *gin.Context) {
	if _, exists := c.Get("userID"); !exists {
		Error(c, http.StatusUnauthorized, CodeUnauthorized, "未授权")
		return
	}

	// 限制请求体大小
	maxBytes := int64(h.maxSizeMB) * 1024 * 1024
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes+1024) // +1KB 余量

	file, header, err := c.Request.FormFile("file")
	if err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			Error(c, http.StatusBadRequest, CodeInvalidParam, fmt.Sprintf("文件大小超过上限（%dMB）", h.maxSizeMB))
			return
		}
		Error(c, http.StatusBadRequest, CodeMissingParam, "请选择要上传的文件")
		return
	}
	defer file.Close()

	// 校验扩展名
	ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(header.Filename), "."))
	if !h.allowedExts[ext] {
		Error(c, http.StatusBadRequest, CodeInvalidParam,
			fmt.Sprintf("不支持的文件格式 .%s，仅支持 %s", ext, h.allowedExtsList()))
		return
	}

	// 确保存储目录存在
	avatarDir := filepath.Join(h.uploadDir, "avatars")
	if err := os.MkdirAll(avatarDir, 0755); err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "创建上传目录失败")
		return
	}

	// 生成唯一文件名：{userID}_{timestamp}.{ext}
	uid := c.GetInt64("userID")
	filename := fmt.Sprintf("%d_%d.%s", uid, time.Now().UnixNano(), ext)
	savePath := filepath.Join(avatarDir, filename)

	// 保存文件
	dst, err := os.Create(savePath)
	if err != nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "保存文件失败")
		return
	}
	defer dst.Close()

	written, err := io.Copy(dst, file)
	if err != nil {
		os.Remove(savePath)
		Error(c, http.StatusInternalServerError, CodeInternalError, "写入文件失败")
		return
	}

	// 返回可访问的相对路径
	url := "/uploads/avatars/" + filename
	if h.userRepo != nil {
		user, err := h.userRepo.GetUserByID(c.Request.Context(), uid)
		if err != nil || user == nil {
			_ = os.Remove(savePath)
			Error(c, http.StatusInternalServerError, CodeInternalError, "保存头像资料失败")
			return
		}
		user.AvatarURL = url
		if err := h.userRepo.UpdateUser(c.Request.Context(), user); err != nil {
			_ = os.Remove(savePath)
			Error(c, http.StatusInternalServerError, CodeInternalError, "保存头像资料失败")
			return
		}
	}

	Success(c, uploadAvatarResponse{
		URL:      url,
		FilePath: savePath,
		Size:     written,
	})
}

// RegisterRoutes 在给定的 Gin 路由组上注册文件上传路由。
func (h *UploadHandler) RegisterRoutes(rg *gin.RouterGroup) {
	upload := rg.Group("/upload")
	upload.POST("/avatar", h.UploadAvatar)
}

// allowedExtsList 返回允许的扩展名列表字符串（用于错误消息）。
func (h *UploadHandler) allowedExtsList() string {
	exts := make([]string, 0, len(h.allowedExts))
	for ext := range h.allowedExts {
		exts = append(exts, "."+ext)
	}
	return strings.Join(exts, "、")
}

// 使用说明：需要在 main.go 的 setupRouter 中注册 uploadHandler。
// 并在 setupRouter 中添加静态文件路由以对外提供上传目录的访问：
//   r.Static("/uploads", cfg.Server.UploadDir)
