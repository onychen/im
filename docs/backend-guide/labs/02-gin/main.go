package main

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type SendMessage struct {
	MsgID   string `json:"msgId" binding:"required,max=128"`
	ToID    int64  `json:"toId" binding:"required,gt=0"`
	Content string `json:"content" binding:"required,max=1000"`
}

func main() {
	router := gin.New()
	router.Use(gin.Logger(), gin.Recovery())
	router.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })
	router.POST("/api/v1/messages", func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		var req SendMessage
		if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Content) == "" {
			c.JSON(400, gin.H{"code": 1001, "message": "消息字段无效"})
			return
		}
		c.JSON(200, gin.H{"code": 0, "message": "ok", "data": req})
	})
	log.Println("实验 02：http://localhost:18100；仅验证消息，尚未发送或保存")
	log.Fatal((&http.Server{Addr: "127.0.0.1:18100", Handler: router, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
