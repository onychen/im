package infra

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/goim/goim/internal/config"
)

// NewMySQLPool 创建并配置一个 MySQL 连接池。
func NewMySQLPool(cfg *config.MySQLConfig) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.DBName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(5 * time.Minute)

	return db, nil
}
