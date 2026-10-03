package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server struct {
		Port                  int   `yaml:"port"`
		ReadHeaderTimeoutMS   int   `yaml:"read_header_timeout_ms"`
		RequestBodyLimitBytes int64 `yaml:"request_body_limit_bytes"`
	} `yaml:"server"`
	JWT struct {
		Secret           string `yaml:"secret"`
		AccessExpMinutes int    `yaml:"access_exp_minutes"`
	} `yaml:"jwt"`
	CORS struct {
		AllowedOrigins []string `yaml:"allowed_origins"`
	} `yaml:"cors"`
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	decoder := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(raw))))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return cfg, err
	}
	if cfg.Server.Port < 1 || cfg.Server.Port > 65535 || cfg.Server.ReadHeaderTimeoutMS <= 0 || cfg.Server.RequestBodyLimitBytes < 128 {
		return cfg, fmt.Errorf("server 参数无效：检查端口、超时和请求体上限")
	}
	if len(cfg.JWT.Secret) < 32 || cfg.JWT.AccessExpMinutes <= 0 {
		return cfg, fmt.Errorf("请设置至少 32 字节的 NANOIM_JWT_SECRET 和正数有效期")
	}
	if len(cfg.CORS.AllowedOrigins) == 0 {
		return cfg, fmt.Errorf("请显式配置允许的浏览器 Origin")
	}
	return cfg, nil
}

type Envelope struct {
	Code      int    `json:"code"`
	Message   string `json:"message"`
	Data      any    `json:"data,omitempty"`
	RequestID string `json:"request_id"`
}

func respond(c *gin.Context, status, code int, message string, data any) {
	c.JSON(status, Envelope{code, message, data, c.GetString("requestID")})
}

func requestTrace() gin.HandlerFunc {
	return func(c *gin.Context) {
		var bytes [12]byte
		if _, err := rand.Read(bytes[:]); err != nil {
			c.AbortWithStatus(500)
			return
		}
		id := hex.EncodeToString(bytes[:])
		c.Set("requestID", id)
		c.Header("X-Request-ID", id)
		started := time.Now()
		c.Next()
		log.Printf("request_id=%s method=%s path=%s status=%d elapsed=%s", id, c.Request.Method, c.Request.URL.Path, c.Writer.Status(), time.Since(started))
	}
}

func cors(origins []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		origin := c.GetHeader("Origin")
		c.Header("Vary", "Origin")
		if origin != "" {
			allowed := false
			for _, candidate := range origins {
				if candidate == origin {
					allowed = true
					break
				}
			}
			if !allowed {
				respond(c, 403, 1004, "浏览器来源不在允许列表", nil)
				c.Abort()
				return
			}
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, OPTIONS")
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
			c.Header("Access-Control-Expose-Headers", "X-Request-ID")
		}
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

type Claims struct {
	UserID int64 `json:"user_id"`
	jwt.RegisteredClaims
}

func authenticate(secret string) gin.HandlerFunc {
	return func(c *gin.Context) {
		authorization := c.GetHeader("Authorization")
		if !strings.HasPrefix(authorization, "Bearer ") {
			respond(c, 401, 1003, "缺少访问令牌", nil)
			c.Abort()
			return
		}
		claims := &Claims{}
		token, err := jwt.ParseWithClaims(strings.TrimPrefix(authorization, "Bearer "), claims,
			func(token *jwt.Token) (any, error) { return []byte(secret), nil },
			jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired())
		if err != nil || !token.Valid || claims.UserID != 1 {
			respond(c, 401, 1003, "令牌无效或已过期", nil)
			c.Abort()
			return
		}
		c.Set("userID", claims.UserID)
		c.Next()
	}
}

type Profile struct {
	UserID              int64  `json:"user_id"`
	Nickname            string `json:"nickname"`
	NotificationEnabled bool   `json:"notification_enabled"`
}

type ProfileStore interface {
	Get(userID int64) Profile
	Save(profile Profile)
}

type MemoryStore struct {
	mu      sync.RWMutex
	profile Profile
}

func (s *MemoryStore) Get(userID int64) Profile { s.mu.RLock(); defer s.mu.RUnlock(); return s.profile }
func (s *MemoryStore) Save(profile Profile)     { s.mu.Lock(); defer s.mu.Unlock(); s.profile = profile }

type ProfileService struct{ store ProfileStore }

func (s *ProfileService) Update(userID int64, nickname string, enabled bool) (Profile, error) {
	nickname = strings.TrimSpace(nickname)
	if n := utf8.RuneCountInString(nickname); n < 2 || n > 20 {
		return Profile{}, fmt.Errorf("昵称应为 2–20 个字符")
	}
	profile := Profile{UserID: userID, Nickname: nickname, NotificationEnabled: enabled}
	s.store.Save(profile)
	return profile, nil
}

type updateProfileRequest struct {
	Nickname            string `json:"nickname" binding:"required"`
	NotificationEnabled *bool  `json:"notification_enabled" binding:"required"`
}

type ProfileHandler struct{ service *ProfileService }

func (h *ProfileHandler) Get(c *gin.Context) {
	respond(c, 200, 0, "ok", h.service.store.Get(c.GetInt64("userID")))
}
func (h *ProfileHandler) Update(c *gin.Context) {
	var request updateProfileRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond(c, 413, 1001, "请求体超过上限", nil)
		} else {
			respond(c, 400, 1001, "需要 nickname 和 notification_enabled", nil)
		}
		return
	}
	profile, err := h.service.Update(c.GetInt64("userID"), request.Nickname, *request.NotificationEnabled)
	if err != nil {
		respond(c, 400, 1001, err.Error(), nil)
		return
	}
	respond(c, 200, 0, "ok", profile)
}

func login(cfg Config, passwordHash []byte) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request struct {
			Username string `json:"username" binding:"required"`
			Password string `json:"password" binding:"required"`
		}
		if err := c.ShouldBindJSON(&request); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				respond(c, 413, 1001, "请求体超过上限", nil)
			} else {
				respond(c, 400, 1001, "登录字段无效", nil)
			}
			return
		}
		if request.Username != "lesson" || bcrypt.CompareHashAndPassword(passwordHash, []byte(request.Password)) != nil {
			respond(c, 401, 1003, "用户名或密码错误", nil)
			return
		}
		claims := Claims{UserID: 1, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Duration(cfg.JWT.AccessExpMinutes) * time.Minute))}}
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(cfg.JWT.Secret))
		if err != nil {
			respond(c, 500, 5000, "签发失败", nil)
			return
		}
		respond(c, 200, 0, "ok", gin.H{"access_token": token, "expires_in": cfg.JWT.AccessExpMinutes * 60})
	}
}

func main() {
	path := flag.String("c", "05-api/config.example.yaml", "configuration file")
	flag.Parse()
	cfg, err := loadConfig(*path)
	if err != nil {
		log.Fatal(err)
	}
	// A fixed development account isolates the API exercise from registration/storage.
	hash, err := bcrypt.GenerateFromPassword([]byte("lesson123"), 10)
	if err != nil {
		log.Fatal(err)
	}
	store := &MemoryStore{profile: Profile{UserID: 1, Nickname: "学习者", NotificationEnabled: true}}
	handler := &ProfileHandler{service: &ProfileService{store: store}}
	r := gin.New()
	r.Use(gin.Recovery(), requestTrace(), cors(cfg.CORS.AllowedOrigins))
	r.Use(func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, cfg.Server.RequestBodyLimitBytes)
		c.Next()
	})
	r.GET("/health", func(c *gin.Context) { respond(c, 200, 0, "ok", gin.H{"service": "nanoim-api"}) })
	public := r.Group("/api/v1")
	public.POST("/auth/login", login(cfg, hash))
	protected := r.Group("/api/v1", authenticate(cfg.JWT.Secret))
	protected.GET("/profile", handler.Get)
	protected.PUT("/profile", handler.Update)
	r.NoRoute(func(c *gin.Context) { respond(c, 404, 1005, "接口不存在", nil) })
	log.Printf("实验 05：http://localhost:%d；开发账户 lesson / lesson123；仅保存内存，不含注册、刷新和数据库", cfg.Server.Port)
	server := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", cfg.Server.Port), Handler: r, ReadHeaderTimeout: time.Duration(cfg.Server.ReadHeaderTimeoutMS) * time.Millisecond}
	log.Fatal(server.ListenAndServe())
}
