// benchmark/msg_debug.go — 单连接单消息诊断
// go run benchmark/msg_debug.go

//go:build benchmark_debug

package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

type wsMsg struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func main() {
	f, _ := os.Open("benchmark/pairs.csv")
	defer f.Close()
	r := csv.NewReader(f)
	r.Read() // header
	row, _ := r.Read()
	token := row[0]
	receiverID, _ := strconv.ParseInt(row[2], 10, 64)

	log.Printf("Token valid, ReceiverID=%d", receiverID)

	u, _ := url.Parse("ws://localhost:8080/ws")
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatalf("WS连接失败: %v", err)
	}
	defer conn.Close()
	log.Println("✅ WS 已连接")

	done := make(chan struct{})
	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Read 错误: %v", err)
				close(done)
				return
			}
			var msg wsMsg
			json.Unmarshal(raw, &msg)
			log.Printf("← type=%s data=%s", msg.Type, string(msg.Data))
		}
	}()

	time.Sleep(200 * time.Millisecond)

	// 发送消息 — toId 必须是数字
	payloadRaw := fmt.Sprintf(
		`{"msgId":"dbg001","convType":1,"toId":%d,"msgType":1,"content":"hello","timestamp":%d}`,
		receiverID, time.Now().UnixMilli(),
	)
	envelope, _ := json.Marshal(wsMsg{Type: "msg", Data: json.RawMessage(payloadRaw)})

	log.Printf("→ %s", string(envelope))
	conn.WriteMessage(websocket.TextMessage, envelope)
	log.Println("→ 已发送，等待响应...")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		log.Println("超时 5s")
	}
}
