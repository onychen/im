// ============================================================================
// GoIM WebSocket 协议 TypeScript 类型定义
// ============================================================================
// 会话模型：每个会话（私聊 p_{小}_{大} / 群聊 g_{gid}）维护单调递增的 seq。
// 客户端以 convId + seq 唯一定位消息：
//   - 上线/重连发送 convSyncReq，服务端返回全部会话的 maxSeq/readSeq/unread；
//   - 本地 seq 落后时发送 pullMsgs 按区间补拉；
//   - 已读上报 readAck { convId, seq }；
//   - 撤回以 msgType=6 的通知消息下发，content 为 RevokeNotice JSON。
// ============================================================================

// ──────────────────────────────────────────────────────
// 消息类型枚举
// ──────────────────────────────────────────────────────

/** 消息内容类型 */
export enum MsgType {
  Text    = 1,  // 纯文本消息
  Image   = 2,  // 图片消息
  Video   = 3,  // 视频消息
  System  = 5,  // 系统通知
  Revoke  = 6,  // 撤回通知（content 为 RevokeNotice JSON）
}

/** 消息状态 */
export enum MsgStatus {
  Normal  = 0,
  Revoked = 1,
}

/** 会话类型 */
export enum ConvType {
  Private = 1,  // 私聊 (convId 格式: p_{较小ID}_{较大ID})
  Group   = 2,  // 群聊 (convId 格式: g_{群组ID})
}

/** 群成员角色 */
export enum GroupRole {
  Member = 0,  // 普通成员
  Admin  = 1,  // 管理员
  Owner  = 2,  // 群主
}

/** 朋友圈可见性 */
export enum MomentVisibility {
  Friends = 2,  // 仅好友可见
  Private = 3,  // 仅自己可见
}

/** WS 消息 type 字段 — 客户端→服务端 */
export const ClientMsgType = {
  Msg:         "msg",
  ReadAck:     "readAck",
  ConvSyncReq: "convSyncReq",
  PullMsgs:    "pullMsgs",
  RevokeMsg:   "revokeMsg",
} as const;

/** WS 消息 type 字段 — 服务端→客户端 */
export const ServerMsgType = {
  Msg:          "msg",
  ServerAck:    "serverAck",
  ConvSync:     "convSync",
  PullMsgsResp: "pullMsgsResp",
  Kick:         "kick",
  Error:        "error",
} as const;

/** 撤回请求在 serverAck / error 中使用的 clientMsgId 前缀：revoke:{convId}:{seq} */
export const RevokeClientMsgPrefix = "revoke:";

// ──────────────────────────────────────────────────────
// 数据载荷类型
// ──────────────────────────────────────────────────────

/** 会话内的一条消息（推送 / 补拉 / 历史共用） */
export interface WsMessage {
  convId:       string;
  seq:          number;   // 会话内单调递增序号
  msgId:        string;   // 服务端雪花 ID，全局唯一；JSON 中为字符串，避免超过 JS 安全整数丢精度
  convType:     ConvType;
  fromId:       number;
  msgType:      MsgType;
  content:      string;
  status:       MsgStatus;
  clientMsgId?: string;
  timestamp:    number;   // 服务端时间，Unix 毫秒
}

/** 撤回通知内容（msgType=6 消息的 content 解析结果） */
export interface RevokeNotice {
  targetSeq:   number;
  targetMsgId: string;
  operatorId:  number;
}

/** 客户端发送的聊天消息 */
export interface SendMessage {
  msgId:     string;    // 客户端生成的唯一ID（clientMsgId），用于幂等
  convType:  ConvType;
  toId:      number;    // 接收者ID(私聊) 或 群组ID(群聊)
  msgType:   MsgType;
  content:   string;
  timestamp?: number;   // 仅客户端本地展示用，服务端以自身时间为准
}

/** 服务端回执：消息已写入 Kafka，seq 已分配 */
export interface ServerAck {
  clientMsgId: string;
  serverMsgId: string;  // 雪花 ID 字符串
  convId:      string;
  seq:         number;
  timestamp:   number;
}

/** 已读上报：把已读位置推进到 seq（只前进不回退） */
export interface ReadAck {
  convId: string;
  seq:    number;
}

/** 会话视图 */
export interface ConvView {
  convId:       string;
  convType:     ConvType;
  targetId:     number;
  targetName:   string;
  targetAvatar: string;
  maxSeq:       number;
  readSeq:      number;
  minSeq:       number;
  unread:       number;
  lastMsg?:     WsMessage;
  lastMsgBrief: string;
  lastMsgTime:  number;
  pinned:       boolean;
  muted:        boolean;
}

/** 会话同步 */
export interface ConvSync {
  conversations: ConvView[];
}

/** 按 seq 区间补拉；toSeq<=0 表示拉到最新 */
export interface PullMsgsReq {
  convId:  string;
  fromSeq: number;
  toSeq:   number;
}

/**
 * 补拉结果；hasMore=true 时从 toSeq+1 继续。
 * watermark 为服务端已投递水位：toSeq 不会超过它，水位以下仍缺失的 seq 是空洞（Kafka 写失败），无需再拉。
 */
export interface PullMsgsResp {
  convId:    string;
  fromSeq:   number;
  toSeq:     number;
  watermark: number;
  msgs:      WsMessage[];
  hasMore:   boolean;
}

/** 撤回消息请求 */
export interface RevokeMsgReq {
  convId: string;
  seq:    number;
}

/** 被踢出通知 */
export interface KickNotification {
  reason: "new_login";
}

export interface GroupRemovedNotification {
  groupId: number;
  reason: "removed";
}

export interface GroupAddedNotification {
  groupId: number;
  name: string;
}

/** 错误通知；发送/撤回失败时带 clientMsgId */
export interface WsError {
  code:         number;
  message:      string;
  clientMsgId?: string;
}

// ──────────────────────────────────────────────────────
// 联合类型：客户端 → 服务端
// ──────────────────────────────────────────────────────

export type ClientWsMessage =
  | { type: "msg";         data: SendMessage }
  | { type: "readAck";     data: ReadAck }
  | { type: "convSyncReq"; data?: Record<string, never> }
  | { type: "pullMsgs";    data: PullMsgsReq }
  | { type: "revokeMsg";   data: RevokeMsgReq };

// ──────────────────────────────────────────────────────
// 联合类型：服务端 → 客户端
// ──────────────────────────────────────────────────────

export type ServerWsMessage =
  | { type: "msg";          data: WsMessage }
  | { type: "serverAck";    data: ServerAck }
  | { type: "convSync";     data: ConvSync }
  | { type: "pullMsgsResp"; data: PullMsgsResp }
  // `kick` is emitted by the connection manager without a `data` envelope.
  | { type: "kick";         reason: KickNotification["reason"] }
  | { type: "groupRemoved"; data: GroupRemovedNotification }
  | { type: "groupAdded";   data: GroupAddedNotification }
  | { type: "error";        data: WsError };

// ──────────────────────────────────────────────────────
// 辅助函数
// ──────────────────────────────────────────────────────

/** 构建私聊会话ID: p_{较小ID}_{较大ID} */
export function buildPrivateConvId(userIdA: number, userIdB: number): string {
  const [smaller, larger] = userIdA < userIdB ? [userIdA, userIdB] : [userIdB, userIdA];
  return `p_${smaller}_${larger}`;
}

/** 构建群聊会话ID: g_{群组ID} */
export function buildGroupConvId(groupId: number): string {
  return `g_${groupId}`;
}

/** 从会话ID解析类型；私聊返回较小的用户ID，需要对端ID请用 privatePeerId */
export function parseConvId(convId: string): { type: ConvType; id: number } | null {
  if (convId.startsWith("p_")) {
    return { type: ConvType.Private, id: Number(convId.split("_")[1]) };
  }
  if (convId.startsWith("g_")) {
    return { type: ConvType.Group, id: Number(convId.slice(2)) };
  }
  return null;
}

/** 私聊会话中对端用户ID */
export function privatePeerId(convId: string, selfId: number): number {
  const [, a, b] = convId.split("_");
  const ua = Number(a);
  const ub = Number(b);
  return ua === selfId ? ub : ua;
}

/** 解析撤回通知内容，失败返回 null */
export function parseRevokeNotice(content: string): RevokeNotice | null {
  try {
    const parsed = JSON.parse(content) as Partial<RevokeNotice>;
    if (typeof parsed.targetSeq === "number" && parsed.targetSeq > 0) {
      return { targetSeq: parsed.targetSeq, targetMsgId: String(parsed.targetMsgId ?? ""), operatorId: parsed.operatorId ?? 0 };
    }
  } catch { /* 非 JSON 视为无效通知 */ }
  return null;
}

/** 解析撤回请求的 clientMsgId（revoke:{convId}:{seq}） */
export function parseRevokeClientMsgId(clientMsgId: string): { convId: string; seq: number } | null {
  if (!clientMsgId.startsWith(RevokeClientMsgPrefix)) return null;
  const rest = clientMsgId.slice(RevokeClientMsgPrefix.length);
  const idx = rest.lastIndexOf(":");
  if (idx <= 0) return null;
  const seq = Number(rest.slice(idx + 1));
  if (!Number.isFinite(seq) || seq <= 0) return null;
  return { convId: rest.slice(0, idx), seq };
}

// ──────────────────────────────────────────────────────
// REST API 错误码
// ──────────────────────────────────────────────────────

/** API 响应错误码对照表 */
export enum ApiErrorCode {
  Success = 0,

  // 通用
  InternalError  = 1000,
  MissingParam   = 1001,
  InvalidParam   = 1002,
  Unauthorized   = 1003,

  // 认证 1100+
  UsernameTooShort = 1101,
  PasswordTooShort = 1102,
  UsernameTaken    = 1103,
  UserNotFound     = 1104,
  WrongPassword    = 1105,
  InvalidToken     = 1106,

  // 好友 1200+
  SelfRequest      = 1201,
  AlreadyFriends   = 1202,
  FriendBlocked    = 1203,
  DuplicateRequest = 1204,
  RequestNotFound  = 1205,
  NotRequestTarget = 1206,
  AlreadyBlocked   = 1207,

  // 群组 1300+
  NotOwnerOrAdmin    = 1301,
  GroupNotFound      = 1302,
  AlreadyMember      = 1303,
  GroupFull          = 1304,
  CannotRemoveOwner  = 1305,
  CannotLeaveAsOwner = 1306,
  InvalidRole        = 1307,

  // 消息操作 1400+
  MsgNotRevocable   = 1401,
  MsgRevokeNotOwner = 1402,
  MsgDeleteFailed   = 1403,
  ConvNotFound      = 1404,

  // 朋友圈 1500+
  MomentContentEmpty = 1501,
  MomentNotFound     = 1502,
  NotCommentOwner    = 1503,
  InvalidVisibility  = 1504,
  CommentNotFound    = 1505,

  // 设置 1700+
  SettingsNotFound = 1701,
  MuteConvExists   = 1702,
  MuteConvNotFound = 1703,

  // 发送校验
  MsgNotFriend  = 4001,
  MsgBlocked    = 4002,
  MsgDuplicate  = 4003,
  MsgNotMember  = 5001,
  MsgMuted      = 5002,
}

/** REST API 标准响应信封 */
export interface ApiResponse<T = unknown> {
  code:    number;
  message: string;
  data?:   T;
}
