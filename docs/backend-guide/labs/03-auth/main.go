package main

import (
	"crypto/rand"
	"encoding/hex"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

type user struct {
	ID   int64
	Hash []byte
}
type credentials struct {
	Username string `json:"username" binding:"required,min=3,max=50"`
	Password string `json:"password" binding:"required,min=6,max=72"`
}
type claims struct {
	UserID int64 `json:"user_id"`
	jwt.RegisteredClaims
}

func main() {
	var mu sync.Mutex
	users := map[string]user{}
	var nextID int64
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		log.Fatal(err)
	}
	r := gin.New()
	r.Use(gin.Logger(), gin.Recovery())
	r.POST("/api/v1/auth/register", func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		var req credentials
		if c.ShouldBindJSON(&req) != nil || len(req.Password) > 72 {
			c.JSON(400, gin.H{"message": "字段无效"})
			return
		}
		hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 10)
		if err != nil {
			c.Status(500)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if _, ok := users[req.Username]; ok {
			c.JSON(409, gin.H{"message": "用户名已存在"})
			return
		}
		nextID++
		users[req.Username] = user{nextID, hash}
		c.JSON(201, gin.H{"code": 0, "data": gin.H{"user_id": nextID}})
	})
	r.POST("/api/v1/auth/login", func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
		var req credentials
		if c.ShouldBindJSON(&req) != nil {
			c.Status(400)
			return
		}
		mu.Lock()
		u, ok := users[req.Username]
		mu.Unlock()
		if !ok || bcrypt.CompareHashAndPassword(u.Hash, []byte(req.Password)) != nil {
			c.JSON(401, gin.H{"message": "凭据错误"})
			return
		}
		now := time.Now()
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{u.ID, jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(now.Add(2 * time.Hour)), IssuedAt: jwt.NewNumericDate(now)}}).SignedString(secret)
		if err != nil {
			c.Status(500)
			return
		}
		c.JSON(200, gin.H{"code": 0, "data": gin.H{"access_token": token}})
	})
	r.GET("/api/v1/me", func(c *gin.Context) {
		parsed := &claims{}
		token, err := jwt.ParseWithClaims(strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer "), parsed, func(t *jwt.Token) (interface{}, error) { return secret, nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
		if err != nil || !token.Valid || parsed.UserID <= 0 {
			c.Status(401)
			return
		}
		c.JSON(200, gin.H{"code": 0, "data": gin.H{"user_id": parsed.UserID}})
	})
	log.Println("实验 03：http://localhost:18100；用户和密钥只保存在内存，重启失效；启动标识：" + hex.EncodeToString(secret[:2]))
	log.Fatal((&http.Server{Addr: "127.0.0.1:18100", Handler: r, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}
