package ws

import (
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/service"
)

// MessageDispatcher 根据 WsMessage 信封中的消息类型字段，将 WebSocket 消息路由到对应的服务处理器。
type MessageDispatcher struct {
	MsgSvc    *service.MsgService
	FriendSvc *service.FriendService
}

// NewMessageDispatcher 创建一个调度器，持有所有服务处理器的引用。
// 尚未实现的服务可以为 nil —— 调度器将跳过这些消息类型。
func NewMessageDispatcher(msgSvc *service.MsgService, friendSvc *service.FriendService) *MessageDispatcher {
	return &MessageDispatcher{
		MsgSvc:    msgSvc,
		FriendSvc: friendSvc,
	}
}

// HandleMessage 是传递给 ReadPump 的回调函数。
// 它在连接的读 goroutine 中同步执行，因此同一连接上的发送请求天然按到达顺序处理，
// 分配到的 seq 顺序与客户端发送顺序一致。
func (d *MessageDispatcher) HandleMessage(c *conn.ClientConnection, rawMsg []byte) {
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		return
	}

	switch msg.Type {
	case TypeMsg:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleSendMessage(c.UserID, msg.Data)
		}
	case TypeReadAck:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleReadAck(c.UserID, msg.Data)
		}
	case TypeConvSyncReq:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleConvSyncReq(c.UserID, msg.Data)
		}
	case TypePullMsgs:
		if d.MsgSvc != nil {
			d.MsgSvc.HandlePullMsgs(c.UserID, msg.Data)
		}
	case TypeRevokeMsg:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleRevokeMsg(c.UserID, msg.Data)
		}
	case TypeFriendApply:
		if d.FriendSvc != nil {
			d.FriendSvc.HandleFriendApply(c.UserID, msg.Data)
		}
	case TypePing:
		// 应用层心跳：协议层 ping/pong 由 ReadPump 处理，这里无需响应
	default:
	}
}

// Callback 返回一个与 ReadPump 的 msgHandler 回调签名匹配的函数。
func (d *MessageDispatcher) Callback() func(*conn.ClientConnection, []byte) {
	return d.HandleMessage
}
