package infra

import (
	"github.com/goim/goim/internal/config"
	"github.com/redis/go-redis/v9"
)

// NewRedisClient 创建一个已配置的 Redis 客户端。
func NewRedisClient(cfg *config.RedisConfig) (*redis.Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	return rdb, nil
}
