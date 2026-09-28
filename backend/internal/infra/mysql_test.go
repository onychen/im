package infra

import (
	"testing"

	"github.com/goim/goim/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestMySQLConnection(t *testing.T) {
	cfg, err := config.LoadConfig("../../configs/config.test.yaml")
	assert.NoError(t, err)

	db, err := NewMySQLPool(&cfg.MySQL)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	err = db.Ping()
	assert.NoError(t, err)

	db.Close()
}
