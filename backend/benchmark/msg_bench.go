// benchmark/msg_bench.go — WebSocket 消息写入 QPS 压测
//
// 用法：
//   go run benchmark/msg_bench.go -conns=100 -duration=30s
//   go run benchmark/msg_bench.go -conns=500 -duration=30s
//   go run benchmark/msg_bench.go -conns=1000 -duration=30s
//
// 每个 goroutine 持一个 WS 连接，发→收serverAck→立刻发下一条，持续到 duration 结束。

//go:build benchmark_messages

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

var (
	wsURL    = flag.String("url", "ws://localhost:8080/ws", "WebSocket 端点")
	conns    = flag.Int("conns", 100, "并发连接数")
	duration = flag.Duration("duration", 30*time.Second, "压测时长")
	pairFile = flag.String("pairs", "benchmark/pairs.csv", "好友对文件")
)

type wsMsg struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type connConfig struct {
	Token      string
	SenderID   int64
	ReceiverID int64
}

func main() {
	flag.Parse()

	configs := loadPairs(*pairFile)
	if len(configs) == 0 {
		log.Fatal("没有好友对，请先运行 setup_friends.go")
	}
	if *conns > len(configs) {
		*conns = len(configs)
	}
	configs = configs[:*conns]

	log.Printf("消息压测: %d 连接, %v 时长", *conns, *duration)

	var (
		totalSent   int64
		totalAcked  int64
		totalFailed int64
		latencies   []int64
		latMu       sync.Mutex
		stopCh      = make(chan struct{})
		wg          sync.WaitGroup
	)

	// 启动所有连接
	for i := 0; i < *conns; i++ {
		wg.Add(1)
		go runSender(configs[i], stopCh, &wg, &totalSent, &totalAcked, &totalFailed, &latencies, &latMu)
	}

	// 进度显示
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		lastSent := int64(0)
		for {
			select {
			case <-ticker.C:
				sent := atomic.LoadInt64(&totalSent)
				acked := atomic.LoadInt64(&totalAcked)
				failed := atomic.LoadInt64(&totalFailed)
				log.Printf("sent=%d ack=%d fail=%d qps=%d", sent, acked, failed, sent-lastSent)
				lastSent = sent
			case <-stopCh:
				return
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case <-time.After(*duration):
	case <-sigCh:
		log.Println("中断信号，正在停止...")
	}

	close(stopCh)
	wg.Wait()

	// 统计
	sent := atomic.LoadInt64(&totalSent)
	acked := atomic.LoadInt64(&totalAcked)
	failed := atomic.LoadInt64(&totalFailed)
	elapsed := *duration

	latMu.Lock()
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	avg := int64(0)
	if len(latencies) > 0 {
		var sum int64
		for _, l := range latencies {
			sum += l
		}
		avg = sum / int64(len(latencies))
	}
	latMu.Unlock()

	fmt.Println()
	fmt.Println("═══════════════════════════════════════")
	fmt.Println("  消息写入 QPS 压测结果")
	fmt.Println("═══════════════════════════════════════")
	fmt.Printf("  并发连接:       %d\n", *conns)
	fmt.Printf("  压测时长:       %v\n", elapsed)
	fmt.Printf("  总发送:         %d\n", sent)
	fmt.Printf("  成功(ack):      %d\n", acked)
	if sent > 0 {
		fmt.Printf("  成功率:         %.2f%%\n", float64(acked)/float64(sent)*100)
	}
	fmt.Printf("  失败(超时5s):   %d\n", failed)
	fmt.Printf("  QPS (发送):     %.0f msg/s\n", float64(sent)/elapsed.Seconds())
	fmt.Printf("  QPS (成功):     %.0f msg/s\n", float64(acked)/elapsed.Seconds())
	fmt.Printf("  ─────────────────────────────────\n")
	fmt.Printf("  延迟 P50:       %.2f ms\n", float64(p50)/1000)
	fmt.Printf("  延迟 P95:       %.2f ms\n", float64(p95)/1000)
	fmt.Printf("  延迟 P99:       %.2f ms\n", float64(p99)/1000)
	fmt.Printf("  延迟 AVG:       %.2f ms\n", float64(avg)/1000)
	fmt.Println("═══════════════════════════════════════")
}

func runSender(cfg connConfig, stopCh chan struct{}, wg *sync.WaitGroup,
	sent, acked, failed *int64, latencies *[]int64, latMu *sync.Mutex) {

	defer wg.Done()

	u, _ := url.Parse(*wsURL)
	q := u.Query()
	q.Set("token", cfg.Token)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Printf("sender %d: WS连接失败: %v", cfg.SenderID, err)
		return
	}
	defer conn.Close()

	// 读 goroutine：接收 serverAck
	ackCh := make(chan struct{}, 8)
	errCh := make(chan error, 1)
	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			var msg wsMsg
			if err := json.Unmarshal(raw, &msg); err != nil {
				continue
			}
			if msg.Type == "serverAck" {
				select {
				case ackCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	// 连续发送：发→收ack→立刻发下一条
	seq := 0
	for {
		select {
		case <-stopCh:
			return
		case <-errCh:
			return
		default:
		}

		seq++
		msgID := fmt.Sprintf("b_%d_%d", cfg.SenderID, seq)
		t0 := time.Now()

		payloadRaw := fmt.Sprintf(
			`{"msgId":"%s","convType":1,"toId":%d,"msgType":1,"content":"m","timestamp":%d}`,
			msgID, cfg.ReceiverID, t0.UnixMilli(),
		)
		envelope, _ := json.Marshal(wsMsg{Type: "msg", Data: json.RawMessage(payloadRaw)})

		if err := conn.WriteMessage(websocket.TextMessage, envelope); err != nil {
			atomic.AddInt64(failed, 1)
			return
		}
		atomic.AddInt64(sent, 1)

		// 等 ack（5s 超时）
		select {
		case <-ackCh:
			elapsed := time.Since(t0).Microseconds()
			latMu.Lock()
			*latencies = append(*latencies, elapsed)
			latMu.Unlock()
			atomic.AddInt64(acked, 1)
		case <-time.After(5 * time.Second):
			atomic.AddInt64(failed, 1)
		case <-stopCh:
			return
		case <-errCh:
			return
		}
	}
}

func loadPairs(path string) []connConfig {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("读取 %s 失败: %v", path, err)
	}
	lines := bytesSplit(data, '\n')
	if len(lines) < 2 {
		return nil
	}
	configs := make([]connConfig, 0, len(lines)-1)
	for _, line := range lines[1:] {
		if len(line) == 0 {
			continue
		}
		parts := bytesSplit(line, ',')
		if len(parts) < 4 {
			continue
		}
		senderID := parseInt(string(parts[1]))
		receiverID := parseInt(string(parts[2]))
		configs = append(configs, connConfig{
			Token:      string(parts[0]),
			SenderID:   senderID,
			ReceiverID: receiverID,
		})
	}
	return configs
}

func percentile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func bytesSplit(data []byte, sep byte) [][]byte {
	var result [][]byte
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == sep {
			result = append(result, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		result = append(result, data[start:])
	}
	return result
}

func parseInt(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
