package model

// ──────────────────────────────────────────────────────
// 存储模型
// ──────────────────────────────────────────────────────

// 消息状态
const (
	MsgStatusNormal  = 0
	MsgStatusRevoked = 1
)

// Message 是私聊、群聊统一的消息模型，对应 messages 表，也是 msg_topic 中的事件体。
// (ConvID, Seq) 唯一确定一条消息；Seq 在会话内单调递增，允许因发送失败留下空洞。
type Message struct {
	ConvID      string `json:"convId"`
	Seq         int64  `json:"seq"`
	MsgID       int64  `json:"msgId,string"` // 雪花 ID，全局唯一；超过 JS 安全整数，JSON 中以字符串传输
	ConvType    int    `json:"convType"`
	SenderID    int64  `json:"fromId"`
	MsgType     int    `json:"msgType"`
	Content     string `json:"content"`
	Status      int    `json:"status"` // 0=正常, 1=已撤回
	ClientMsgID string `json:"clientMsgId,omitempty"`
	Timestamp   int64  `json:"timestamp"` // 服务端时间，毫秒
}

// Conversation 对应 conversations 表：会话级元数据，由 persist 消费者维护。
type Conversation struct {
	ConvID       string `json:"convId"`
	ConvType     int    `json:"convType"`
	MaxSeq       int64  `json:"maxSeq"`
	LastMsgID    int64  `json:"lastMsgId,string"`
	LastMsgBrief string `json:"lastMsgBrief"`
	LastSenderID int64  `json:"lastSenderId"`
	LastMsgAt    int64  `json:"lastMsgAt"` // 毫秒
}

// UserConversation 对应 user_conversations 表：用户在某个会话上的个人状态。
type UserConversation struct {
	UserID  int64  `json:"userId"`
	ConvID  string `json:"convId"`
	ReadSeq int64  `json:"readSeq"`
	MinSeq  int64  `json:"minSeq"` // 小于该值的消息对该用户不可见（清空聊天记录）
	Pinned  bool   `json:"pinned"`
	Muted   bool   `json:"muted"`
	Hidden  bool   `json:"hidden"`
}

// RevokeNotice 是撤回通知消息（MsgTypeRevoked）的 content。
type RevokeNotice struct {
	TargetSeq   int64 `json:"targetSeq"`
	TargetMsgID int64 `json:"targetMsgId,string"`
	OperatorID  int64 `json:"operatorId"`
}

// ──────────────────────────────────────────────────────
// WebSocket 协议负载
// ──────────────────────────────────────────────────────

// SendMessage — 客户端通过 WebSocket 发送的聊天消息
type SendMessage struct {
	ClientMsgID string `json:"msgId"`    // 客户端生成的ID，用于去重
	ConvType    int    `json:"convType"` // 1=私聊, 2=群聊
	ToID        int64  `json:"toId"`     // 接收者ID(私聊) 或 群组ID(群聊)
	MsgType     int    `json:"msgType"`  // 1=文字, 2=图片, 3=视频, 等
	Content     string `json:"content"`
}

// ServerAck — 消息写入 Kafka 成功后返回给发送者的回执
type ServerAck struct {
	ClientMsgID string `json:"clientMsgId"`
	ServerMsgID int64  `json:"serverMsgId,string"`
	ConvID      string `json:"convId"`
	Seq         int64  `json:"seq"`
	Timestamp   int64  `json:"timestamp"`
}

// ReadAck — 用户把会话已读到 seq（只允许前进）
type ReadAck struct {
	ConvID string `json:"convId"`
	Seq    int64  `json:"seq"`
}

// ConvView — convSync 中返回的单个会话视图
type ConvView struct {
	ConvID       string   `json:"convId"`
	ConvType     int      `json:"convType"`
	TargetID     int64    `json:"targetId"`
	TargetName   string   `json:"targetName"`
	TargetAvatar string   `json:"targetAvatar"`
	MaxSeq       int64    `json:"maxSeq"`
	ReadSeq      int64    `json:"readSeq"`
	MinSeq       int64    `json:"minSeq"`
	Unread       int64    `json:"unread"`
	LastMsg      *Message `json:"lastMsg,omitempty"`
	LastMsgBrief string   `json:"lastMsgBrief"`
	LastMsgTime  int64    `json:"lastMsgTime"`
	Pinned       bool     `json:"pinned"`
	Muted        bool     `json:"muted"`
}

// ConvSync — 对 convSyncReq 的响应：用户全部会话的最新状态
type ConvSync struct {
	Conversations []ConvView `json:"conversations"`
}

// PullMsgsReq — 按 seq 区间补拉消息，区间为闭区间 [FromSeq, ToSeq]
type PullMsgsReq struct {
	ConvID  string `json:"convId"`
	FromSeq int64  `json:"fromSeq"`
	ToSeq   int64  `json:"toSeq"`
}

// PullMsgsResp — pullMsgs 的响应；HasMore 表示区间被截断，客户端应从 ToSeq+1 继续拉。
// Watermark 是会话的已投递水位：seq <= Watermark 的消息都已被消费者处理，区间内缺失的 seq 是空洞，不会再出现。
// ToSeq 不会超过 Watermark，客户端可以安全地把本地同步位置推进到 ToSeq。
type PullMsgsResp struct {
	ConvID    string    `json:"convId"`
	FromSeq   int64     `json:"fromSeq"`
	ToSeq     int64     `json:"toSeq"`
	Watermark int64     `json:"watermark"`
	Msgs      []Message `json:"msgs"`
	HasMore   bool      `json:"hasMore"`
}

// RevokeMsgReq — 客户端请求撤回一条消息
type RevokeMsgReq struct {
	ConvID string `json:"convId"`
	Seq    int64  `json:"seq"`
}
