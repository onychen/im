package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/goim/goim/internal/service"
)

// AuthHandler 提供认证端点的 Gin HTTP 处理器。
type AuthHandler struct {
	authSvc *service.AuthService
}

// NewAuthHandler 创建一个 AuthHandler，封装给定的 AuthService。
func NewAuthHandler(authSvc *service.AuthService) *AuthHandler {
	return &AuthHandler{authSvc: authSvc}
}

// ── Request / response DTOs ──

type registerRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type registerResponse struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type loginRequest struct {
	Username string `json:"username" binding:"required"`
	Password string `json:"password" binding:"required"`
}

type loginResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	AvatarURL    string `json:"avatar_url"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

type refreshResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

type updateUsernameRequest struct {
	Username string `json:"username" binding:"required"`
}

type updatePasswordRequest struct {
	CurrentPassword string `json:"current_password" binding:"required"`
	NewPassword     string `json:"new_password" binding:"required"`
}

// ── Handlers ──

// Register godoc
// @Summary      用户注册
// @Description  使用用户名和密码注册新用户
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body  registerRequest  true  "注册信息"
// @Success      201   {object}  ApiResponse{data=registerResponse}  "注册成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      409   {object}  ApiResponse  "用户名已存在"
// @Router       /auth/register [post]
// Register handles POST /api/v1/auth/register.
func (h *AuthHandler) Register(c *gin.Context) {
	var req registerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "username and password are required")
		return
	}

	userID, username, err := h.authSvc.Register(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		switch err.Error() {
		case service.ErrUsernameTooShort, service.ErrPasswordTooShort:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrUsernameTaken:
			ServiceError(c, http.StatusConflict, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	SuccessCreated(c, registerResponse{UserID: userID, Username: username})
}

// Login godoc
// @Summary      用户登录
// @Description  使用用户名和密码登录，返回访问令牌和刷新令牌
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body  loginRequest  true  "登录信息"
// @Success      200   {object}  ApiResponse{data=loginResponse}  "登录成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      401   {object}  ApiResponse  "用户名或密码错误"
// @Router       /auth/login [post]
// Login handles POST /api/v1/auth/login.
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "username and password are required")
		return
	}

	accessToken, refreshToken, expiresIn, err := h.authSvc.Login(c.Request.Context(), req.Username, req.Password)
	if err != nil {
		switch err.Error() {
		case service.ErrUserNotFound:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		case service.ErrWrongPassword:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}
	user, err := h.authSvc.GetUserByUsername(c.Request.Context(), req.Username)
	if err != nil || user == nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}

	Success(c, loginResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    expiresIn,
		AvatarURL:    user.AvatarURL,
	})
}

// Refresh godoc
// @Summary      刷新令牌
// @Description  使用刷新令牌获取新的访问令牌
// @Tags         认证
// @Accept       json
// @Produce      json
// @Param        body  body  refreshRequest  true  "刷新令牌"
// @Success      200   {object}  ApiResponse{data=refreshResponse}  "刷新成功"
// @Failure      400   {object}  ApiResponse  "参数错误"
// @Failure      401   {object}  ApiResponse  "令牌无效或已过期"
// @Router       /auth/refresh [post]
// Refresh handles POST /api/v1/auth/refresh.
func (h *AuthHandler) Refresh(c *gin.Context) {
	var req refreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "refresh_token is required")
		return
	}

	accessToken, expiresIn, err := h.authSvc.Refresh(c.Request.Context(), req.RefreshToken)
	if err != nil {
		switch err.Error() {
		case service.ErrInvalidToken:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		case service.ErrUserNotFound:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	Success(c, refreshResponse{
		AccessToken: accessToken,
		ExpiresIn:   expiresIn,
	})
}

// UpdateUsername godoc
// @Summary      修改用户名
// @Description  修改当前登录用户的用户名。用户名全局唯一，成功后返回包含新用户名的令牌。
// @Tags         账户
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  updateUsernameRequest  true  "新用户名"
// @Success      200   {object}  ApiResponse{data=loginResponse}
// @Failure      400   {object}  ApiResponse
// @Failure      401   {object}  ApiResponse
// @Failure      409   {object}  ApiResponse
// @Router       /account/username [put]
func (h *AuthHandler) UpdateUsername(c *gin.Context) {
	var req updateUsernameRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "username is required")
		return
	}

	accessToken, refreshToken, expiresIn, err := h.authSvc.UpdateUsername(c.Request.Context(), c.GetInt64("userID"), req.Username)
	if err != nil {
		switch err.Error() {
		case service.ErrUsernameTooShort:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrUsernameTaken:
			ServiceError(c, http.StatusConflict, err.Error())
		case service.ErrUserNotFound:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}

	user, err := h.authSvc.GetUserByID(c.Request.Context(), c.GetInt64("userID"))
	if err != nil || user == nil {
		Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		return
	}
	Success(c, loginResponse{AccessToken: accessToken, RefreshToken: refreshToken, ExpiresIn: expiresIn, AvatarURL: user.AvatarURL})
}

// UpdatePassword godoc
// @Summary      修改密码
// @Description  校验当前密码后更新为新密码。
// @Tags         账户
// @Accept       json
// @Produce      json
// @Security     BearerAuth
// @Param        body  body  updatePasswordRequest  true  "密码信息"
// @Success      200   {object}  ApiResponse
// @Failure      400   {object}  ApiResponse
// @Failure      401   {object}  ApiResponse
// @Router       /account/password [put]
func (h *AuthHandler) UpdatePassword(c *gin.Context) {
	var req updatePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Error(c, http.StatusBadRequest, CodeMissingParam, "current_password and new_password are required")
		return
	}

	err := h.authSvc.UpdatePassword(c.Request.Context(), c.GetInt64("userID"), req.CurrentPassword, req.NewPassword)
	if err != nil {
		switch err.Error() {
		case service.ErrPasswordTooShort:
			ServiceError(c, http.StatusBadRequest, err.Error())
		case service.ErrWrongPassword:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		case service.ErrUserNotFound:
			ServiceError(c, http.StatusUnauthorized, err.Error())
		default:
			Error(c, http.StatusInternalServerError, CodeInternalError, "internal error")
		}
		return
	}
	SuccessMessage(c, "密码修改成功")
}

// RegisterRoutes registers all auth HTTP routes on the given Gin engine.
func (h *AuthHandler) RegisterRoutes(rg *gin.RouterGroup) {
	auth := rg.Group("/auth")
	auth.POST("/register", h.Register)
	auth.POST("/login", h.Login)
	auth.POST("/refresh", h.Refresh)
}

// RegisterAccountRoutes 注册需要登录态的账户资料修改路由。
func (h *AuthHandler) RegisterAccountRoutes(rg *gin.RouterGroup) {
	account := rg.Group("/account")
	account.PUT("/username", h.UpdateUsername)
	account.PUT("/password", h.UpdatePassword)
}
