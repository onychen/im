package protocol

import (
	"encoding/json"

	"github.com/goim/goim/internal/model"
)

// 类型常量 — WebSocket 信封中所有可能的 "type" 值。
const (
	TypeMsg            = "msg"
	TypeServerAck      = "serverAck"
	TypeDeliverAck     = "deliverAck"
	TypeReadAck        = "readAck"
	TypeSyncReq        = "syncReq"
	TypeSyncBatch      = "syncBatch"
	TypeConvSync       = "convSync"
	TypeRevokeMsg      = "revokeMsg"
	TypeMsgRevoked     = "msgRevoked"
	TypeKick           = "kick"
	TypeFriendApply    = "friendApply"
	TypeFriendAccepted = "friendAccepted"
	TypePresence       = "presence"
	TypeError          = "error"
	TypePing           = "ping"
	TypePong           = "pong"
)

// EncodeMsg 构建 WsMessage 信封并将其序列化为 JSON 字节。
// msgType 是 Type* 常量之一；data 是要嵌入到 "data" 中的有效载荷。
func EncodeMsg(msgType string, data interface{}) ([]byte, error) {
	dataBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	envelope := model.WsMessage{Type: msgType, Data: dataBytes}
	return json.Marshal(envelope)
}

// DecodeMsg 将原始 JSON 字节解析为 WsMessage 信封。
// 调用方随后可以检查 msg.Type 并进一步反序列化 msg.Data。
func DecodeMsg(raw []byte) (*model.WsMessage, error) {
	var msg model.WsMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
