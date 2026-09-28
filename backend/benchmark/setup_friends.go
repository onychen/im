// benchmark/setup_friends.go — 批量配对加好友，输出 pair.csv 供 msg_bench.go 使用
//
// 用法：
//   go run benchmark/setup_friends.go -url=http://localhost:8080 -pairs=1000
//
// 将 bench 用户两两配对：p0↔p1, p2↔p3, ...，互相加为好友。
// 输出 benchmark/pairs.csv（sender_token, receiver_id）。

//go:build benchmark_friends

package main

import (
	"bytes"
	"context"
	"encoding/csv"
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

	goredis "github.com/redis/go-redis/v9"

	"github.com/goim/goim/internal/middleware"
)

var (
	baseURL   = flag.String("url", "http://localhost:8080", "GoIM 服务端地址")
	pairs     = flag.Int("pairs", 1000, "好友对数")
	workers   = flag.Int("workers", 50, "并发 worker 数")
	outFile   = flag.String("out", "pairs.csv", "输出文件路径")
	redisAddr = flag.String("redis", "localhost:16379", "Redis 地址")
)

type pair struct {
	SenderToken  string
	SenderID     int64
	ReceiverID   int64
	ReceiverName string
}

func main() {
	flag.Parse()

	output := *outFile
	if !filepath.IsAbs(output) {
		output = filepath.Join("benchmark", output)
	}
	os.MkdirAll(filepath.Dir(output), 0755)

	log.Printf("开始配对 %d 对好友，%d workers", *pairs, *workers)
	log.Printf("输出: %s", output)

	var (
		wg      sync.WaitGroup
		tasks   = make(chan int, *pairs)
		results = make(chan pair, *pairs)
		done    int32
	)

	go func() {
		for i := 0; i < *pairs; i++ {
			tasks <- i
		}
		close(tasks)
	}()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for i := range tasks {
				p, err := makeFriends(client, i)
				if err != nil {
					log.Printf("配对 %d 失败: %v", i, err)
					continue
				}
				results <- *p
				n := atomic.AddInt32(&done, 1)
				if n%500 == 0 {
					log.Printf("进度: %d/%d", n, *pairs)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	f, _ := os.Create(output)
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"sender_token", "sender_id", "receiver_id", "receiver_name"})

	var total int
	for r := range results {
		w.Write([]string{
			r.SenderToken,
			fmt.Sprintf("%d", r.SenderID),
			fmt.Sprintf("%d", r.ReceiverID),
			r.ReceiverName,
		})
		total++
	}
	w.Flush()
	log.Printf("完成！共 %d 对好友，输出到 %s", total, output)
}

func makeFriends(client *http.Client, idx int) (*pair, error) {
	userA := fmt.Sprintf("bench_u%d", idx*2)
	userB := fmt.Sprintf("bench_u%d", idx*2+1)
	password := "bench123"

	// 1. 登录 A
	tokenA, userAID, err := login(client, userA, password)
	if err != nil {
		return nil, fmt.Errorf("登录A(%s): %w", userA, err)
	}

	// 2. 登录 B，获取 B 的 userID
	_, userBID, err := login(client, userB, password)
	if err != nil {
		return nil, fmt.Errorf("登录B(%s): %w", userB, err)
	}

	// 3. A 向 B 发好友申请（如果已经是好友则跳过）
	reqID, err := sendFriendRequest(client, tokenA, userBID)
	if err != nil && !isAlreadyFriend(err) {
		return nil, fmt.Errorf("A→B 好友申请: %w", err)
	}

	// 4. B 接受好友申请（仅当申请不是409时）
	alreadyFriends := err != nil && isAlreadyFriend(err)
	if !alreadyFriends {
		tokenB, _, err := login(client, userB, password)
		if err != nil {
			return nil, fmt.Errorf("登录B token: %w", err)
		}
		if err := acceptFriendRequest(client, tokenB, reqID); err != nil && !isAlreadyFriend(err) {
			return nil, fmt.Errorf("B 接受申请: %w", err)
		}
	}

	// 6. 预热 Redis 好友缓存（Lua 消息校验依赖此 key）
	if err := warmFriendCache(userAID, userBID); err != nil {
		return nil, fmt.Errorf("预热好友缓存: %w", err)
	}

	return &pair{
		SenderToken:  tokenA,
		SenderID:     userAID,
		ReceiverID:   userBID,
		ReceiverName: userB,
	}, nil
}

func login(client *http.Client, username, password string) (string, int64, error) {
	body, _ := json.Marshal(map[string]string{"username": username, "password": password})
	resp, err := client.Post(*baseURL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", 0, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}

	var r struct {
		AccessToken string `json:"access_token"`
	}
	json.Unmarshal(b, &r)

	// 解析 token 拿 userID
	_, claims, err := middleware.ParseToken(r.AccessToken, "test-secret")
	if err != nil {
		return "", 0, fmt.Errorf("解析token: %w", err)
	}

	return r.AccessToken, claims.UserID, nil
}

func sendFriendRequest(client *http.Client, token string, toUserID int64) (int64, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"to_user_id": toUserID,
		"message":    "bench",
	})
	req, _ := http.NewRequest("POST", *baseURL+"/api/v1/friend/request", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 {
		return 0, fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}

	var r struct {
		RequestID int64 `json:"request_id"`
	}
	json.Unmarshal(b, &r)
	return r.RequestID, nil
}

// isAlreadyFriend returns true if the error indicates the user is already a friend.
func isAlreadyFriend(err error) bool {
	return err != nil && (contains(err.Error(), "409") || contains(err.Error(), "已经是好友"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func acceptFriendRequest(client *http.Client, token string, requestID int64) error {
	body, _ := json.Marshal(map[string]interface{}{
		"request_id": requestID,
	})
	req, _ := http.NewRequest("POST", *baseURL+"/api/v1/friend/accept", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status=%d body=%s", resp.StatusCode, string(b))
	}
	return nil
}

// warmFriendCache 在 Redis 中写入双向好友缓存，消息 Lua 脚本依赖此 key。
func warmFriendCache(uidA, uidB int64) error {
	rdb := goredis.NewClient(&goredis.Options{Addr: *redisAddr, DB: 1})
	defer rdb.Close()

	ctx := context.Background()
	pipe := rdb.Pipeline()
	pipe.Set(ctx, fmt.Sprintf("friend:%d:%d", uidA, uidB), "1", 0)
	pipe.Set(ctx, fmt.Sprintf("friend:%d:%d", uidB, uidA), "1", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	return nil
}
