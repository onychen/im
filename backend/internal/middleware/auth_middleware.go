package middleware

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
)

// Claims 保存 GoIM 令牌的 JWT 负载。
type Claims struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
	jwt.RegisteredClaims
}

type authErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const codeUnauthorized = 1003

func abortUnauthorized(c *gin.Context, message string) {
	c.AbortWithStatusJSON(http.StatusUnauthorized, authErrorResponse{
		Code:    codeUnauthorized,
		Message: message,
	})
}

// GenerateAccessToken 使用给定的过期时间创建签名的 JWT 访问令牌。
// expireHours 是令牌的有效期，以小时为单位（通常为 2）。
func GenerateAccessToken(userID int64, username, secret string, expireHours int) (string, error) {
	claims := &Claims{
		UserID:   userID,
		Username: username,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(expireHours) * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// GenerateRefreshToken 使用给定的过期时间创建签名的 JWT 刷新令牌。
// expireDays 是令牌的有效期，以天为单位（通常为 7）。
// 刷新令牌不携带用户名 — 仅嵌入 userID。
func GenerateRefreshToken(userID int64, secret string, expireDays int) (string, error) {
	claims := &Claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(expireDays) * 24 * time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(secret))
}

// ParseToken 使用给定的密钥解析并验证 JWT 令牌字符串。
// 成功时返回解析后的令牌和 claims，失败时返回错误。
// JWTAuthMiddleware 和 ServeWebSocket 都使用此辅助函数，以避免
// 重复 JWT 解析逻辑。
func ParseToken(tokenStr, secret string) (*jwt.Token, *Claims, error) {
	claims := &Claims{}
	parsedToken, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})
	if err != nil {
		return nil, nil, err
	}
	if !parsedToken.Valid {
		return nil, nil, fmt.Errorf("令牌无效")
	}
	return parsedToken, claims, nil
}

// JWTAuthMiddleware 返回一个验证 JWT 令牌的 Gin 中间件。
// 它从以下位置接受令牌：
//   - Authorization 请求头 (Bearer <token>)
//   - 查询参数 "token"（用于 WebSocket 升级请求）
//
// 成功时，它在 Gin 上下文中设置 "userID" (int64) 和 "username" (string)。
// 失败时，它以 401 Unauthorized 中止请求。
func JWTAuthMiddleware(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			// 同时检查查询参数以支持 WebSocket 连接
			token := c.Query("token")
			if token == "" {
				abortUnauthorized(c, "缺少令牌")
				return
			}
			authHeader = "Bearer " + token
		}

		tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
		_, claims, err := ParseToken(tokenStr, secret)
		if err != nil {
			abortUnauthorized(c, "无效令牌")
			return
		}

		c.Set("userID", claims.UserID)
		c.Set("username", claims.Username)
		c.Next()
	}
}
