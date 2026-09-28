//go:build e2e

// Package e2e 包含 GoIM 服务器的端到端集成测试。
// 这些测试覆盖完整技术栈：HTTP API、WebSocket、MQ 消费者、
// Redis、MySQL 和 Kafka。测试要求 Docker 服务处于运行状态
// （按 configs/config.test.yaml 中的配置）。
//
// 运行命令：go test ./tests/... -v -tags e2e -timeout 120s
package e2e

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// ── 响应 DTO ──

type authRegisterResp struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type authLoginResp struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int64  `json:"expires_in"`
	AvatarURL    string `json:"avatar_url"`
}

type authRefreshResp struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
}

type friendRequestResp struct {
	RequestID  int64 `json:"request_id"`
	FromUserID int64 `json:"from_user_id"`
	ToUserID   int64 `json:"to_user_id"`
	Status     int   `json:"status"`
}

type acceptFriendResp struct {
	UserID   int64 `json:"user_id"`
	FriendID int64 `json:"friend_id"`
}

type createGroupResp struct {
	GroupID int64 `json:"group_id"`
}

type publishMomentResp struct {
	MomentID int64 `json:"moment_id"`
}

type wsEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type serverAckData struct {
	ClientMsgID string `json:"clientMsgId"`
	ServerMsgID int64  `json:"serverMsgId"`
	GroupSeq    int64  `json:"groupSeq,omitempty"`
	Timestamp   int64  `json:"timestamp"`
}

type sendMsgData struct {
	ClientMsgID string `json:"msgId"`
	ConvType    int    `json:"convType"`
	ToID        int64  `json:"toId"`
	MsgType     int    `json:"msgType"`
	Content     string `json:"content"`
	Timestamp   int64  `json:"timestamp"`
}

type apiResponse struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func decodeSuccessResponse(t *testing.T, body []byte, dest interface{}) {
	t.Helper()

	var envelope apiResponse
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("parse API response: %v", err)
	}
	if envelope.Code != 0 {
		t.Fatalf("API returned code=%d message=%s", envelope.Code, envelope.Message)
	}
	if dest != nil {
		if err := json.Unmarshal(envelope.Data, dest); err != nil {
			t.Fatalf("parse API response data: %v", err)
		}
	}
}

// ── HTTP 辅助函数 ──

// doRequest 执行一个 HTTP 请求并返回响应体及状态码。
func doRequest(t *testing.T, method, baseURL, path string, body interface{}, token string) (statusCode int, responseBody []byte) {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("序列化请求体: %v", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, baseURL+path, bodyReader)
	if err != nil {
		t.Fatalf("创建请求: %v", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("执行请求: %v", err)
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	responseBody, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("读取响应体: %v", err)
	}

	return statusCode, responseBody
}

// doAuthedRequest 执行一个带认证的 HTTP 请求。
func doAuthedRequest(t *testing.T, method, baseURL, path string, body interface{}, token string) (int, []byte) {
	t.Helper()
	return doRequest(t, method, baseURL, path, body, token)
}

// registerUser 注册一个新用户并返回 userID。
func registerUser(t *testing.T, baseURL, username, password string) int64 {
	t.Helper()
	status, body := doRequest(t, http.MethodPost, baseURL, "/api/v1/auth/register",
		map[string]string{"username": username, "password": password}, "")
	if status != http.StatusCreated {
		t.Fatalf("注册用户: status=%d body=%s", status, body)
	}
	var resp authRegisterResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("反序列化注册响应: %v", err)
	}
	decodeSuccessResponse(t, body, &resp)
	return resp.UserID
}

// loginUser 登录一个用户并返回访问令牌。
func loginUser(t *testing.T, baseURL, username, password string) string {
	t.Helper()
	status, body := doRequest(t, http.MethodPost, baseURL, "/api/v1/auth/login",
		map[string]string{"username": username, "password": password}, "")
	if status != http.StatusOK {
		t.Fatalf("登录用户: status=%d body=%s", status, body)
	}
	var resp authLoginResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("反序列化登录响应: %v", err)
	}
	decodeSuccessResponse(t, body, &resp)
	return resp.AccessToken
}

// loginUserFull 登录一个用户并返回完整的登录响应（访问令牌 + 刷新令牌）。
func loginUserFull(t *testing.T, baseURL, username, password string) authLoginResp {
	t.Helper()
	status, body := doRequest(t, http.MethodPost, baseURL, "/api/v1/auth/login",
		map[string]string{"username": username, "password": password}, "")
	if status != http.StatusOK {
		t.Fatalf("登录用户: status=%d body=%s", status, body)
	}
	var resp authLoginResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("反序列化登录响应: %v", err)
	}
	decodeSuccessResponse(t, body, &resp)
	return resp
}

// refreshToken 使用刷新令牌来刷新访问令牌。
func refreshToken(t *testing.T, baseURL, refreshTokenStr string) authRefreshResp {
	t.Helper()
	status, body := doRequest(t, http.MethodPost, baseURL, "/api/v1/auth/refresh",
		map[string]string{"refresh_token": refreshTokenStr}, "")
	if status != http.StatusOK {
		t.Fatalf("刷新令牌: status=%d body=%s", status, body)
	}
	var resp authRefreshResp
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatalf("反序列化刷新响应: %v", err)
	}
	decodeSuccessResponse(t, body, &resp)
	return resp
}

// ── WebSocket 辅助函数 ──

// connectWS 使用给定的 JWT 令牌建立 WebSocket 连接。
func connectWS(t *testing.T, baseURL, token string) *websocket.Conn {
	t.Helper()

	// 将 http:// 转换为 ws://
	wsURL := strings.Replace(baseURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)

	u, err := url.Parse(wsURL + "/ws")
	if err != nil {
		t.Fatalf("解析 ws url: %v", err)
	}
	q := u.Query()
	q.Set("token", token)
	u.RawQuery = q.Encode()

	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		t.Fatalf("ws 拨号: %v", err)
	}
	return conn
}

// sendWSMessage 发送一个带类型的 WebSocket 消息信封。
func sendWSMessage(t *testing.T, conn *websocket.Conn, msgType string, data interface{}) {
	t.Helper()
	dataBytes, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("序列化 ws 数据: %v", err)
	}
	envelope := wsEnvelope{Type: msgType, Data: dataBytes}
	msg, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("序列化 ws 信封: %v", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
		t.Fatalf("写入 ws 消息: %v", err)
	}
}

// readWSMessage 带超时地读取一个 WebSocket 消息信封。
func readWSMessage(t *testing.T, conn *websocket.Conn, timeout time.Duration) wsEnvelope {
	t.Helper()
	msgChan := make(chan wsEnvelope, 1)
	errChan := make(chan error, 1)

	go func() {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			errChan <- err
			return
		}
		var envelope wsEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			errChan <- err
			return
		}
		msgChan <- envelope
	}()

	select {
	case msg := <-msgChan:
		return msg
	case err := <-errChan:
		t.Fatalf("读取 ws 消息: %v", err)
		return wsEnvelope{}
	case <-time.After(timeout):
		t.Fatalf("读取 ws 消息: 超时 %v", timeout)
		return wsEnvelope{}
	}
}

// readWSMessageType 读取 WS 消息并按预期类型过滤。
// 只有在收到预期类型的消息时才返回，跳过
// 其他类型（例如 pong、kick）。
func readWSMessageType(t *testing.T, conn *websocket.Conn, expectedType string, timeout time.Duration) wsEnvelope {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msg := readWSMessage(t, conn, time.Until(deadline))
		if msg.Type == expectedType {
			return msg
		}
		// 跳过非目标消息（pong、presence 等）
	}
	t.Fatalf("读取 ws 消息类型 %s: 超时 %v", expectedType, timeout)
	return wsEnvelope{}
}

// closeWS 优雅地关闭一个 WebSocket 连接。
func closeWS(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	if err := conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
		// 连接可能已经关闭
		return
	}
	conn.Close()
}
