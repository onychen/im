package infra

import (
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/goim/goim/internal/config"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
)

func TestMySQLPoolConfiguration(t *testing.T) {
	cfg := &config.MySQLConfig{Host: "127.0.0.1", Port: 1, User: "test", Password: "p@ss:/?#", DBName: "goim_test"}
	db, err := NewMySQLPool(cfg)
	require.NoError(t, err, "初始化不应在启动检查之前连接数据库")
	pool, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })
	require.Equal(t, 100, pool.Stats().MaxOpenConnections)
	dialect, ok := db.Dialector.(*mysql.Dialector)
	require.True(t, ok)
	dsn, err := mysqldriver.ParseDSN(dialect.DSN)
	require.NoError(t, err)
	require.Equal(t, cfg.Password, dsn.Passwd)
	require.Equal(t, cfg.DBName, dsn.DBName)
	require.True(t, dsn.ParseTime)
	require.Contains(t, dsn.FormatDSN(), "charset=utf8mb4")
}

func TestMySQLConnection(t *testing.T) {
	cfg, err := config.LoadConfig("../../configs/config.test.yaml")
	require.NoError(t, err)

	db, err := NewMySQLPool(&cfg.MySQL)
	require.NoError(t, err)
	require.NotNil(t, db)

	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.Equal(t, 100, sqlDB.Stats().MaxOpenConnections)
	require.NoError(t, sqlDB.Ping())
}
