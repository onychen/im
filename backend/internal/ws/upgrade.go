package ws

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/middleware"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

// upgrader 将 HTTP 连接升级为 WebSocket。
// CheckOrigin 允许所有来源，用于开发环境。
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// ServeWebSocket 返回一个 Gin 处理函数，该处理函数执行以下操作：
//  1. 从查询参数 (?token=...) 验证 JWT 令牌
//  2. 将 HTTP 连接升级为 WebSocket
//  3. 创建 ClientConnection
//  4. 踢掉同一用户的现有连接（单设备策略）
//  5. 更新 Redis 在线状态（online:{userID} TTL=60s, conn:{userID} TTL=60s）
//  6. 启动 ReadPump 和 WritePump 协程
func ServeWebSocket(jwtSecret string, rdb *redis.Client, cm *conn.ConnectionManager, msgHandler func(*conn.ClientConnection, []byte)) gin.HandlerFunc {
	return func(c *gin.Context) {
		// JWT 认证 — WebSocket 的令牌通过查询参数传递
		token := c.Query("token")
		if token == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "缺少令牌"})
			return
		}

		_, claims, err := middleware.ParseToken(token, jwtSecret)
		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "无效的令牌"})
			return
		}

		// 升级为 WebSocket
		wsConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Printf("WebSocket 升级错误: %v", err)
			return
		}

		// 创建 ClientConnection
		client := conn.NewClientConnection(claims.UserID, wsConn)

		// 踢掉该用户的旧连接（单设备策略）
		cm.KickOld(claims.UserID, client)

		// 更新 Redis 在线状态（nil 保护 — Redis 在测试环境中为可选项）
		if rdb != nil {
			ctx := context.Background()
			onlineKey := fmt.Sprintf("online:%d", claims.UserID)
			connKey := fmt.Sprintf("conn:%d", claims.UserID)
			if err := rdb.Set(ctx, onlineKey, "1", 60*time.Second).Err(); err != nil {
				log.Printf("Redis 设置在线键错误: %v", err)
			}
			if err := rdb.Set(ctx, connKey, fmt.Sprintf("ws:%d", time.Now().UnixNano()), 60*time.Second).Err(); err != nil {
				log.Printf("Redis 设置连接键错误: %v", err)
			}
		}

		log.Printf("用户 %d (%s) 通过 WebSocket 连接", claims.UserID, claims.Username)

		// 定时续期在线键：在线状态由 WebSocket 存活决定，而不是仅以握手时刻决定。
		if rdb != nil {
			go func(userID int64, done <-chan struct{}) {
				ticker := time.NewTicker(20 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						ctx := context.Background()
						if err := rdb.Del(ctx, fmt.Sprintf("online:%d", userID), fmt.Sprintf("conn:%d", userID)).Err(); err != nil {
							log.Printf("Redis 清理在线键错误: %v", err)
						}
						return
					case <-ticker.C:
						ctx := context.Background()
						if err := rdb.Expire(ctx, fmt.Sprintf("online:%d", userID), 60*time.Second).Err(); err != nil {
							log.Printf("Redis 刷新在线键错误: %v", err)
						}
						if err := rdb.Expire(ctx, fmt.Sprintf("conn:%d", userID), 60*time.Second).Err(); err != nil {
							log.Printf("Redis 刷新连接键错误: %v", err)
						}
					}
				}
			}(claims.UserID, client.CloseCh)
		}

		// 启动读写协程
		go client.WritePump()
		go client.ReadPump(msgHandler)
	}
}
