// benchmark/register_users.go — 批量注册压测用户，输出 tokens.csv
//
// 用法：
//   go run benchmark/register_users.go -count=10000 -url=http://localhost:8080
//
// 输出 benchmark/tokens.csv（userID,token,username），供 k6 压测脚本读取。

//go:build benchmark_register

package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	count   = flag.Int("count", 10000, "要注册的用户数")
	baseURL = flag.String("url", "http://localhost:8080", "GoIM 服务端地址")
	outFile = flag.String("out", "", "输出 CSV 路径（默认 benchmark/tokens.csv）")
	workers = flag.Int("workers", 50, "并发注册 goroutine 数")
)

type registerReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type registerResp struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

type tokenRow struct {
	UserID   int64
	Token    string
	Username string
}

func main() {
	flag.Parse()

	output := *outFile
	if output == "" {
		output = filepath.Join("tokens.csv")
	}

	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		log.Fatalf("创建输出目录失败: %v", err)
	}

	log.Printf("开始注册 %d 个用户，%d workers，目标: %s", *count, *workers, *baseURL)

	var (
		wg      sync.WaitGroup
		tasks   = make(chan int, *count)
		results = make(chan tokenRow, *count)
		done    int32
	)

	// 生产者：投放任务编号
	go func() {
		for i := 0; i < *count; i++ {
			tasks <- i
		}
		close(tasks)
	}()

	// 消费者：注册用户
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for i := range tasks {
				row, err := registerAndLogin(client, i)
				if err != nil {
					log.Printf("用户 %d: %v", i, err)
					continue
				}
				results <- *row
				n := atomic.AddInt32(&done, 1)
				if n%1000 == 0 {
					log.Printf("进度: %d/%d", n, *count)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// 写入 CSV
	f, err := os.Create(output)
	if err != nil {
		log.Fatalf("创建输出文件失败: %v", err)
	}
	defer f.Close()

	// CSV header
	fmt.Fprintln(f, "userID,token,username")

	var total int32
	for row := range results {
		fmt.Fprintf(f, "%d,%s,%s\n", row.UserID, row.Token, row.Username)
		total++
	}

	log.Printf("完成！共注册 %d 个用户，输出到 %s", total, output)
}

func registerAndLogin(client *http.Client, idx int) (*tokenRow, error) {
	username := fmt.Sprintf("bench_u%d", idx)
	password := "bench123"

	// 1. 注册
	regBody, _ := json.Marshal(registerReq{Username: username, Password: password})
	resp, err := client.Post(*baseURL+"/api/v1/auth/register", "application/json", bytes.NewReader(regBody))
	if err != nil {
		return nil, fmt.Errorf("注册失败: %w", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// 409 用户名已存在则继续（幂等）
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
		return nil, fmt.Errorf("注册返回 %d: %s", resp.StatusCode, string(body))
	}

	var regResp registerResp
	if err := json.Unmarshal(body, &regResp); err != nil {
		// 409 冲突时可能不返回 userID，我们直接 login
	}

	// 2. 登录获取 token
	loginBody, _ := json.Marshal(loginReq{Username: username, Password: password})
	resp, err = client.Post(*baseURL+"/api/v1/auth/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		return nil, fmt.Errorf("登录失败: %w", err)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("登录返回 %d: %s", resp.StatusCode, string(body))
	}

	var lr loginResp
	if err := json.Unmarshal(body, &lr); err != nil {
		return nil, fmt.Errorf("解析登录响应失败: %w", err)
	}

	return &tokenRow{
		UserID:   regResp.UserID,
		Token:    lr.AccessToken,
		Username: username,
	}, nil
}
