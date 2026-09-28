package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// CORS 返回一个 Gin 中间件，处理跨域请求。
// 开发环境允许所有来源；生产环境应限制 allowedOrigins。
func CORS(allowedOrigins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		// 判断来源是否在允许列表中（空列表 = 允许所有）
		allowOrigin := "*"
		if len(allowedOrigins) > 0 {
			allowed := false
			for _, o := range allowedOrigins {
				if o == origin || o == "*" {
					allowed = true
					break
				}
			}
			if allowed {
				allowOrigin = origin
			} else {
				// 不在允许列表中，不设置 CORS 头，继续处理
				c.Next()
				return
			}
		}

		c.Header("Access-Control-Allow-Origin", allowOrigin)
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Content-Type")
		c.Header("Access-Control-Allow-Credentials", "true")

		// 预检请求直接返回 204
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}
