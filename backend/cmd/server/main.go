package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"go.uber.org/zap"

	_ "github.com/goim/goim/docs"
	"github.com/goim/goim/internal/api"
	"github.com/goim/goim/internal/config"
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/consumer"
	"github.com/goim/goim/internal/idgen"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/middleware"
	redislua "github.com/goim/goim/internal/redis"
	"github.com/goim/goim/internal/repository"
	"github.com/goim/goim/internal/service"
	"github.com/goim/goim/internal/ws"
)

func main() {
	configPath := flag.String("c", "configs/config.local.yaml", "configuration file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run keeps the dependency graph explicit: Kafka (acks=all) is the commit
// point for messages, MySQL is the source of truth written by the persist
// consumer, and Redis only holds caches, seq counters and dedup keys.
func run(configPath string) error {
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("create logger: %w", err)
	}
	defer logger.Sync()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := infra.NewMySQLPool(&cfg.MySQL)
	if err != nil {
		return fmt.Errorf("create MySQL pool: %w", err)
	}
	defer db.Close()
	checkCtx, checkCancel := context.WithTimeout(ctx, 5*time.Second)
	err = db.PingContext(checkCtx)
	checkCancel()
	if err != nil {
		return fmt.Errorf("connect MySQL: %w", err)
	}

	rdb, err := infra.NewRedisClient(&cfg.Redis)
	if err != nil {
		return fmt.Errorf("create Redis client: %w", err)
	}
	defer rdb.Close()
	checkCtx, checkCancel = context.WithTimeout(ctx, 5*time.Second)
	err = rdb.Ping(checkCtx).Err()
	checkCancel()
	if err != nil {
		return fmt.Errorf("connect Redis: %w", err)
	}

	kafkaClient, err := infra.ConnectKafkaWithRetry(ctx, &cfg.Kafka, time.Second, 10*time.Second,
		func(err error, delay time.Duration) {
			logger.Warn("waiting for Kafka", zap.Error(err), zap.Duration("retryIn", delay))
		})
	if err != nil {
		return fmt.Errorf("connect Kafka: %w", err)
	}
	kafkaReady := kafkaClient.StartHealthMonitor(ctx, 10*time.Second,
		func(ready bool, err error) {
			logger.Warn("Kafka readiness changed", zap.Bool("ready", ready), zap.Error(err))
		})
	if err := redislua.LoadLuaScripts(rdb, ctx); err != nil {
		return fmt.Errorf("load Redis Lua scripts: %w", err)
	}

	mysqlRepo := repository.NewMySQLRepo(db)
	redisRepo := repository.NewRedisRepo(rdb)
	msgCache := repository.NewMessageCache(rdb)
	relations := repository.NewRelationCache(rdb, mysqlRepo)
	mqWriter := kafkaClient.NewWriter()
	defer mqWriter.Close()
	mqRepo := repository.NewMQRepo(mqWriter)
	cm := conn.NewConnectionManager()
	ids, err := idgen.NewSnowflake(cfg.Message.NodeID)
	if err != nil {
		return fmt.Errorf("create snowflake: %w", err)
	}

	// mysqlRepo 同时实现 MySQLRepo（用户/好友/群组）与 MessageStore（消息/会话）。
	msgSvc := service.NewMsgService(mysqlRepo, mysqlRepo, msgCache, relations, mqRepo, ids, cm, logger)
	authSvc := service.NewAuthService(mysqlRepo, cfg.JWT.Secret, cfg.JWT.AccessExpHours, cfg.JWT.RefreshExpDays)
	friendSvc := service.NewFriendService(mysqlRepo, redisRepo, logger).WithMessaging(mysqlRepo, relations)
	groupSvc := service.NewGroupService(mysqlRepo, redisRepo, logger).WithMessaging(mysqlRepo, relations, msgSvc)
	momentSvc := service.NewMomentService(mysqlRepo, redisRepo, mqRepo, logger, time.Duration(cfg.Moment.LikeCacheTTLHours)*time.Hour)
	settingsSvc := service.NewSettingsService(mysqlRepo, logger)
	dispatcher := ws.NewMessageDispatcher(msgSvc, friendSvc)

	if err := os.MkdirAll(cfg.Server.UploadDir, 0755); err != nil {
		return fmt.Errorf("create upload directory: %w", err)
	}
	router := setupRouter(cfg, rdb, cm, dispatcher, kafkaReady,
		api.NewAuthHandler(authSvc), api.NewFriendHandler(friendSvc, rdb),
		api.NewGroupHandler(groupSvc, cm), api.NewMomentHandler(momentSvc),
		api.NewMsgOpHandler(msgSvc), api.NewConvHandler(msgSvc), api.NewSettingsHandler(settingsSvc),
		api.NewUploadHandler(cfg.Server.UploadDir, cfg.File.MaxSizeMB, cfg.File.AllowedExts, mysqlRepo),
		api.NewAvatarHandler())

	// msg_topic 上两个独立的消费者组：persist 负责落库，deliver 负责缓存与在线推送。
	msgDLQ := consumer.DeadLetterPolicy{
		MaxAttempts: consumer.DefaultDeadLetterAttempts,
		Writer:      mqWriter,
		Topic:       infra.TopicMsgDLQ,
	}
	persistConsumer := consumer.NewMsgPersistConsumer(
		kafkaClient.NewReader(infra.TopicMsg, infra.GroupMsgPersist),
		mysqlRepo, logger, cfg.Message.PersistBatchSize, cfg.Message.PersistFlushMs,
	).WithDeadLetter(msgDLQ)
	if err := persistConsumer.Start(ctx); err != nil {
		return fmt.Errorf("start message persist consumer: %w", err)
	}
	deliverConsumer := consumer.NewMsgDeliverConsumer(
		kafkaClient.NewReader(infra.TopicMsg, infra.GroupMsgDeliver),
		msgSvc, logger,
	).WithDeadLetter(msgDLQ)
	if err := deliverConsumer.Start(ctx); err != nil {
		return fmt.Errorf("start message deliver consumer: %w", err)
	}
	momentConsumer := consumer.NewMomentFeedConsumer(kafkaClient.NewReader(infra.TopicMomentPush, infra.GroupMomentFeed), mysqlRepo, redisRepo, logger, cfg.Moment.BigUserFriendThreshold, cfg.Moment.TimelineMaxLen)
	if err := momentConsumer.Start(ctx); err != nil {
		return fmt.Errorf("start moment feed consumer: %w", err)
	}
	likeConsumer := consumer.NewLikePersistConsumer(kafkaClient.NewReader(infra.TopicLikePersist, infra.GroupLikePersist), mysqlRepo, logger, cfg.Moment.LikePersistBatchSize, cfg.Moment.LikePersistFlushMs)
	if err := likeConsumer.Start(ctx); err != nil {
		return fmt.Errorf("start like persistence consumer: %w", err)
	}
	infra.StartCleanupTask(rdb, logger, time.Hour)
	// Cancel workers before the deferred Kafka writers and database clients close.
	defer cancel()

	if cfg.Server.PprofPort > 0 {
		pprofServer := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", cfg.Server.PprofPort), Handler: http.DefaultServeMux}
		defer pprofServer.Shutdown(context.Background())
		go func() {
			if err := pprofServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("pprof server stopped", zap.Error(err))
			}
		}()
	}

	server := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Server.Port), Handler: router}
	serverErr := make(chan error, 1)
	go func() { serverErr <- server.ListenAndServe() }()
	logger.Info("GoIM server started", zap.Int("port", cfg.Server.Port))
	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	return server.Shutdown(shutdownCtx)
}

func setupRouter(cfg *config.Config, rdb *redis.Client, cm *conn.ConnectionManager, dispatcher *ws.MessageDispatcher, kafkaReady func() bool,
	authHandler *api.AuthHandler, friendHandler *api.FriendHandler, groupHandler *api.GroupHandler,
	momentHandler *api.MomentHandler, msgOpHandler *api.MsgOpHandler, convHandler *api.ConvHandler, settingsHandler *api.SettingsHandler,
	uploadHandler *api.UploadHandler, avatarHandler *api.AvatarHandler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery(), middleware.CORS(nil))
	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "goim"}) })
	r.GET("/ready", func(c *gin.Context) {
		if !kafkaReady() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready"})
	})
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	public := r.Group("/api/v1")
	authHandler.RegisterRoutes(public)
	avatarHandler.RegisterRoutes(public)
	protected := r.Group("/api/v1", middleware.JWTAuthMiddleware(cfg.JWT.Secret))
	friendHandler.RegisterRoutes(protected)
	groupHandler.RegisterRoutes(protected)
	momentHandler.RegisterRoutes(protected)
	msgOpHandler.RegisterRoutes(protected)
	convHandler.RegisterRoutes(protected)
	settingsHandler.RegisterRoutes(protected)
	uploadHandler.RegisterRoutes(protected)
	r.GET(cfg.Server.WsPath, ws.ServeWebSocket(cfg.JWT.Secret, rdb, cm, dispatcher.Callback()))
	r.Static("/uploads", cfg.Server.UploadDir)

	frontendDir := findFrontendDir()
	if frontendDir != "" {
		r.Static("/assets", filepath.Join(frontendDir, "assets"))
	}
	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path
		if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/ws") || frontendDir == "" {
			c.JSON(http.StatusNotFound, gin.H{"code": 404, "message": "not found"})
			return
		}
		c.File(filepath.Join(frontendDir, "index.html"))
	})
	return r
}

func findFrontendDir() string {
	for _, dir := range []string{"frontend/dist", "../frontend/dist"} {
		if info, err := os.Stat(filepath.Join(dir, "index.html")); err == nil && !info.IsDir() {
			return dir
		}
	}
	return ""
}
