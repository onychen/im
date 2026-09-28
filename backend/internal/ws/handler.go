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
// 它将原始 JSON 字节解码为 WsMessage 信封，并根据 msg.Type 将消息路由到对应的服务处理器。
func (d *MessageDispatcher) HandleMessage(c *conn.ClientConnection, rawMsg []byte) {
	msg, err := DecodeMsg(rawMsg)
	if err != nil {
		// 无效的消息格式，跳过
		return
	}

	switch msg.Type {
	case TypeMsg:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleSendMessage(c.UserID, msg.Data)
		}
	case TypeDeliverAck:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleDeliverAck(c.UserID, msg.Data)
		}
	case TypeReadAck:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleReadAck(c.UserID, msg.Data)
		}
	case TypeSyncReq:
		if d.MsgSvc != nil {
			d.MsgSvc.HandleSyncReq(c.UserID, msg.Data)
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
		// Ping 由 ReadPump 中的 PingHandler 处理；此处跳过
	default:
		// 未知的消息类型，跳过
	}
}

// Callback 返回一个与 ReadPump 的 msgHandler 回调签名匹配的函数。
// 这样便于传递给 ServeWebSocket 和 ReadPump。
func (d *MessageDispatcher) Callback() func(*conn.ClientConnection, []byte) {
	return d.HandleMessage
}
