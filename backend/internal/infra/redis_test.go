package infra

import (
	"context"
	"testing"

	"github.com/goim/goim/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestRedisConnection(t *testing.T) {
	cfg, err := config.LoadConfig("../../configs/config.test.yaml")
	assert.NoError(t, err)

	rdb, err := NewRedisClient(&cfg.Redis)
	assert.NoError(t, err)
	assert.NotNil(t, rdb)

	err = rdb.Ping(context.Background()).Err()
	assert.NoError(t, err)

	rdb.Close()
}
