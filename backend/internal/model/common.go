package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// 消息类型常量
const (
	MsgTypeText    = 1
	MsgTypeImage   = 2
	MsgTypeVideo   = 3
	MsgTypeFile    = 4
	MsgTypeSystem  = 5
	MsgTypeRevoked = 6 // 撤回通知：content 为 RevokeNotice JSON

	ConvTypePrivate = 1
	ConvTypeGroup   = 2

	RoleMember = 0
	RoleAdmin  = 1
	RoleOwner  = 2
)

// BuildConvID 生成会话 ID：
//
//	private: p_{较小ID}_{较大ID}
//	group:   g_{群组ID}
func BuildConvID(convType int, id1, id2 int64) string {
	if convType == ConvTypeGroup {
		return fmt.Sprintf("g_%d", id1)
	}
	if id1 > id2 {
		id1, id2 = id2, id1
	}
	return fmt.Sprintf("p_%d_%d", id1, id2)
}

// ParsedConvID 是 ParseConvID 的结果。
// 私聊时 UserA < UserB；群聊时 GroupID 有效。
type ParsedConvID struct {
	ConvType int
	UserA    int64
	UserB    int64
	GroupID  int64
}

// ParseConvID 解析 BuildConvID 生成的会话 ID。
func ParseConvID(convID string) (ParsedConvID, error) {
	switch {
	case strings.HasPrefix(convID, "g_"):
		gid, err := strconv.ParseInt(convID[2:], 10, 64)
		if err != nil || gid <= 0 {
			return ParsedConvID{}, fmt.Errorf("无效的群会话 ID: %q", convID)
		}
		return ParsedConvID{ConvType: ConvTypeGroup, GroupID: gid}, nil
	case strings.HasPrefix(convID, "p_"):
		parts := strings.Split(convID[2:], "_")
		if len(parts) != 2 {
			return ParsedConvID{}, fmt.Errorf("无效的私聊会话 ID: %q", convID)
		}
		a, errA := strconv.ParseInt(parts[0], 10, 64)
		b, errB := strconv.ParseInt(parts[1], 10, 64)
		if errA != nil || errB != nil || a <= 0 || b <= 0 || a >= b {
			return ParsedConvID{}, fmt.Errorf("无效的私聊会话 ID: %q", convID)
		}
		return ParsedConvID{ConvType: ConvTypePrivate, UserA: a, UserB: b}, nil
	default:
		return ParsedConvID{}, fmt.Errorf("无效的会话 ID: %q", convID)
	}
}

// Peer 返回私聊会话中 userID 的对方；userID 不在会话中时返回 0。
func (p ParsedConvID) Peer(userID int64) int64 {
	if p.ConvType != ConvTypePrivate {
		return 0
	}
	switch userID {
	case p.UserA:
		return p.UserB
	case p.UserB:
		return p.UserA
	}
	return 0
}

// MsgBrief 生成会话列表中展示的最后一条消息摘要。
func MsgBrief(msgType int, content string) string {
	switch msgType {
	case MsgTypeImage:
		return "[图片]"
	case MsgTypeVideo:
		return "[视频]"
	case MsgTypeFile:
		return "[文件]"
	case MsgTypeRevoked:
		return "[消息已撤回]"
	}
	r := []rune(content)
	if len(r) > 64 {
		return string(r[:64])
	}
	return content
}

// WsMessage WebSocket 通用消息封装
type WsMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// WsError 通过 WebSocket 发送的错误负载
type WsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	// ClientMsgID 在发送失败时回填，便于客户端把对应消息标记为失败。
	ClientMsgID string `json:"clientMsgId,omitempty"`
}
