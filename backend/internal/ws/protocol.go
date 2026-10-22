package ws

// 协议类型和编解码函数已移至 internal/protocol 以打破 ws 和 service 之间的导入循环。
// 本文件在 ws 包内重新导出它们以保持向后兼容。

import (
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/protocol"
)

// 在 ws 包内重新导出 Type 常量。
const (
	TypeMsg            = protocol.TypeMsg
	TypeServerAck      = protocol.TypeServerAck
	TypeReadAck        = protocol.TypeReadAck
	TypeConvSyncReq    = protocol.TypeConvSyncReq
	TypeConvSync       = protocol.TypeConvSync
	TypePullMsgs       = protocol.TypePullMsgs
	TypePullMsgsResp   = protocol.TypePullMsgsResp
	TypeRevokeMsg      = protocol.TypeRevokeMsg
	TypeKick           = protocol.TypeKick
	TypeFriendApply    = protocol.TypeFriendApply
	TypeFriendAccepted = protocol.TypeFriendAccepted
	TypePresence       = protocol.TypePresence
	TypeError          = protocol.TypeError
	TypePing           = protocol.TypePing
	TypePong           = protocol.TypePong
)

// EncodeMsg 委托给 protocol.EncodeMsg。
func EncodeMsg(msgType string, data interface{}) ([]byte, error) {
	return protocol.EncodeMsg(msgType, data)
}

// DecodeMsg 委托给 protocol.DecodeMsg。
func DecodeMsg(raw []byte) (*model.WsMessage, error) {
	return protocol.DecodeMsg(raw)
}
