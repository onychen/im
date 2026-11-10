package infra

import (
	"fmt"
	"net"
	"strconv"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/goim/goim/internal/config"
)

// NewMySQLPool 创建 GORM MySQL 客户端。表结构继续由 scripts/migrations 管理。
func NewMySQLPool(cfg *config.MySQLConfig) (*gorm.DB, error) {
	dsn := mysqldriver.NewConfig()
	dsn.User, dsn.Passwd = cfg.User, cfg.Password
	dsn.Net = "tcp"
	dsn.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
	dsn.DBName = cfg.DBName
	dsn.ParseTime = true
	dsn.Params = map[string]string{"charset": "utf8mb4"}
	db, err := gorm.Open(mysql.New(mysql.Config{
		DSN: dsn.FormatDSN(), SkipInitializeWithVersion: true,
	}), &gorm.Config{
		// 启动入口用带超时的 PingContext 检查连通性。
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取 MySQL 连接池: %w", err)
	}
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(20)
	sqlDB.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}
