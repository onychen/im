package conn

import (
	"time"

	"github.com/gorilla/websocket"
)

// WebSocket 连接常量
const (
	maxMessageSize = 4096             // 单条消息的最大字节数
	writeWait      = 10 * time.Second // 写入超时时间
	pongWait       = 60 * time.Second // 等待客户端pong响应的超时时间
	pingPeriod     = 30 * time.Second // 发送ping的间隔（必须小于 pongWait）
)

// ClientConnection 表示单个用户的 WebSocket 连接。
type ClientConnection struct {
	UserID   int64
	Conn     *websocket.Conn
	SendCh   chan []byte   // 用于发送消息的缓冲通道（容量 256）
	KickCh   chan []byte   // 单设备登录时的控制消息；写出后立即关闭连接
	CloseCh  chan struct{} // 当连接需要关闭时发送信号
	LastPing time.Time     // 记录最后一次收到的 ping/pong 用于健康监测
}

// NewClientConnection 创建一个新的 ClientConnection，封装 websocket.Conn。
func NewClientConnection(userID int64, conn *websocket.Conn) *ClientConnection {
	return &ClientConnection{
		UserID:   userID,
		Conn:     conn,
		SendCh:   make(chan []byte, 256),
		KickCh:   make(chan []byte, 1),
		CloseCh:  make(chan struct{}),
		LastPing: time.Now(),
	}
}

// ReadPump 在一个 goroutine 中运行，从 WebSocket 连接读取消息。
// 它将每条消息转发给 msgHandler 回调，并处理 pong/超时。
// 当连接关闭或发生读取错误时退出。
func (c *ClientConnection) ReadPump(msgHandler func(*ClientConnection, []byte)) {
	defer func() {
		c.Conn.Close()
		select {
		case <-c.CloseCh:
		default:
			close(c.CloseCh)
		}
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	c.Conn.SetPongHandler(func(appData string) error {
		c.LastPing = time.Now()
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})
	c.Conn.SetPingHandler(func(appData string) error {
		c.LastPing = time.Now()
		c.Conn.SetReadDeadline(time.Now().Add(pongWait))
		// 使用 WriteControl 发送 pong 响应（与默认处理程序相同）
		err := c.Conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
		if err == websocket.ErrCloseSent {
			return nil
		}
		return err
	})

	for {
		_, message, err := c.Conn.ReadMessage()
		if err != nil {
			break
		}
		msgHandler(c, message)
	}
}

// WritePump 在一个 goroutine 中运行，将 SendCh 中的消息写入 WebSocket 连接。
// 它定期发送 ping 消息，并处理 CloseCh 以进行优雅关闭。
// 当收到 KickCh 控制消息时，会先将该消息写出再关闭连接；这保证了
// 单设备登录的 kick 通知不会与关闭信号竞争。CloseCh、SendCh 关闭或写入错误时退出。
func (c *ClientConnection) WritePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case kickMsg := <-c.KickCh:
			// kick 是有顺序语义的控制消息：必须先通知客户端，再断开连接。
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.TextMessage, kickMsg); err != nil {
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		case msg, ok := <-c.SendCh:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// SendCh 已关闭 — 发送关闭帧并退出
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			c.Conn.WriteMessage(websocket.TextMessage, msg)
		case <-ticker.C:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.CloseCh:
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}
	}
}

// Close 关闭底层的 WebSocket 连接。
func (c *ClientConnection) Close() {
	c.Conn.Close()
}
