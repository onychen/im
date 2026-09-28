package conn

import (
	"encoding/json"
	"sync"
)

// ConnectionManager 管理活动的 WebSocket 连接，以 userID 为键。
// 使用 sync.Map 保证并发安全访问。
type ConnectionManager struct {
	connections sync.Map // userID (int64) -> *ClientConnection
}

// NewConnectionManager 创建一个新的 ConnectionManager。
func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{}
}

// Register 将客户端连接存储到指定的 userID 下。
func (cm *ConnectionManager) Register(userID int64, client *ClientConnection) {
	cm.connections.Store(userID, client)
}

// Get 根据 userID 获取客户端连接。
// 如果找到则返回连接和 true，否则返回 nil 和 false。
func (cm *ConnectionManager) Get(userID int64) (*ClientConnection, bool) {
	val, ok := cm.connections.Load(userID)
	if !ok {
		return nil, false
	}
	return val.(*ClientConnection), true
}

// Delete 根据 userID 删除客户端连接。
func (cm *ConnectionManager) Delete(userID int64) {
	cm.connections.Delete(userID)
}

// KickOld 踢掉指定 userID 的现有连接并注册新连接。
// 向旧连接的 KickCh 发送踢出 JSON 消息 {"type":"kick","reason":"new_login"}。
// WritePump 会保证先写出该消息、再关闭连接，随后注册新客户端。
// 如果没有现有连接，则直接注册新连接。
func (cm *ConnectionManager) KickOld(userID int64, newClient *ClientConnection) {
	old, ok := cm.Get(userID)
	if ok {
		// 向旧连接发送踢出消息
		kickMsg, _ := json.Marshal(map[string]string{"type": "kick", "reason": "new_login"})
		select {
		case old.KickCh <- kickMsg:
		default: // 已有踢出请求等待处理，无需重复发送
		}
		cm.Delete(userID)
	}
	cm.Register(userID, newClient)
}
