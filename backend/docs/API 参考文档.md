# GoIM API 参考文档

## 基础 URL

```
http://localhost:8080/api/v1
```

## 认证

所有需要保护的接口都需要通过 `Authorization` 头携带 JWT 访问令牌：

```
Authorization: Bearer <access_token>
```

WebSocket 连接通过查询参数进行认证：

```
GET /ws?token=<access_token>
```

令牌通过 `/auth/login` 或 `/auth/refresh` 获取。

---

## 健康检查

### GET /health

无需认证。

**响应：**
```json
{
  "status": "ok",
  "service": "goim"
}
```

---

## 认证（公开 — 无需 JWT）

### POST /auth/register

**请求：**
```json
{
  "username": "alice",
  "password": "pass1234"
}
```

**响应 (201)：**
```json
{
  "user_id": 1,
  "username": "alice"
}
```

**错误：**
- `400`：用户名/密码过短（每个至少 3 个字符）
- `409`：用户名已被占用

### POST /auth/login

**请求：**
```json
{
  "username": "alice",
  "password": "pass1234"
}
```

**响应 (200)：**
```json
{
  "access_token": "eyJhbG...",
  "refresh_token": "eyJhbG...",
  "expires_in": 7200
}
```

**错误：**
- `401`：用户未找到或密码错误

### POST /auth/refresh

**请求：**
```json
{
  "refresh_token": "eyJhbG..."
}
```

**响应 (200)：**
```json
{
  "access_token": "eyJhbG...",
  "expires_in": 7200
}
```

**错误：**
- `401`：刷新令牌无效或已过期

---

## 好友（需要 JWT）

### POST /friend/request

发送好友申请。

**请求：**
```json
{
  "to_user_id": 2,
  "message": "Let's be friends!"
}
```

**响应 (201)：**
```json
{
  "request_id": 1,
  "from_user_id": 1,
  "to_user_id": 2,
  "status": 0
}
```

**错误：**
- `400`：不能向自己发送申请
- `403`：对方已将你拉黑
- `409`：已经是好友或存在重复申请

### POST /friend/accept

**请求：**
```json
{
  "request_id": 1
}
```

**响应 (200)：**
```json
{
  "user_id": 2,
  "friend_id": 1
}
```

**错误：**
- `403`：不是该申请的接收方
- `404`：申请未找到

### POST /friend/reject

**请求：**
```json
{
  "request_id": 1
}
```

**响应 (200)：**
```json
{
  "message": "friend request rejected"
}
```

### GET /friend/requests

**响应 (200)：**
```json
{
  "requests": [
    {
      "id": 1,
      "from_user_id": 3,
      "to_user_id": 1,
      "message": "Hi!",
      "status": 0,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### GET /friend/list

**响应 (200)：**
```json
{
  "friends": [
    {
      "id": 1,
      "user_id": 1,
      "friend_id": 2,
      "created_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### DELETE /friend/:friendID

删除好友关系。

**响应 (200)：**
```json
{
  "message": "friend deleted"
}
```

### POST /friend/block

**请求：**
```json
{
  "blocked_id": 5
}
```

**响应 (200)：**
```json
{
  "message": "user blocked"
}
```

**错误：**
- `409`：已经被拉黑

### POST /friend/unblock

**请求：**
```json
{
  "blocked_id": 5
}
```

**响应 (200)：**
```json
{
  "message": "user unblocked"
}
```

---

## 群组（需要 JWT）

### POST /group

创建群组。创建者成为群主 (role=2)。

**请求：**
```json
{
  "name": "My Group",
  "notice": "Welcome!"
}
```

**响应 (201)：**
```json
{
  "group_id": 1
}
```

### PUT /group/:groupID

更新群组名称/公告。仅群主或管理员可以更新。

**请求：**
```json
{
  "name": "Updated Name",
  "notice": "Updated notice"
}
```

**响应 (200)：**
```json
{
  "message": "group updated"
}
```

**错误：**
- `403`：不是群主或管理员
- `404`：群组未找到

### GET /group/:groupID

**响应 (200)：**
```json
{
  "id": 1,
  "name": "My Group",
  "notice": "Welcome!",
  "owner_id": 1,
  "max_members": 500,
  "created_at": "2024-01-01T00:00:00Z"
}
```

### POST /group/:groupID/member

添加成员。仅群主或管理员可以添加。

**请求：**
```json
{
  "member_id": 3
}
```

**响应 (200)：**
```json
{
  "message": "member added"
}
```

**错误：**
- `403`：不是群主/管理员
- `404`：群组未找到
- `409`：已经是成员或群组已满（最多 500 人）

### DELETE /group/:groupID/member/:memberID

移除/踢出成员。群主不能被移除。

**响应 (200)：**
```json
{
  "message": "member removed"
}
```

**错误：**
- `403`：不是群主/管理员或试图移除群主
- `404`：群组未找到

### GET /group/:groupID/members

**响应 (200)：**
```json
{
  "members": [
    {
      "group_id": 1,
      "user_id": 1,
      "role": 2,
      "joined_at": "2024-01-01T00:00:00Z"
    }
  ]
}
```

### PUT /group/:groupID/member/:memberID/role

更新成员角色。仅群主可以更改角色。

**请求：**
```json
{
  "role": 1
}
```

角色：0=普通成员，1=管理员，2=群主

**响应 (200)：**
```json
{
  "message": "member role updated"
}
```

### POST /group/:groupID/leave

退出群组。群主不能退出（必须先转让群主）。

**响应 (200)：**
```json
{
  "message": "left group"
}
```

**错误：**
- `403`：群主不能退出
- `404`：群组未找到

---

## 朋友圈（需要 JWT）

### POST /moment

发布一条朋友圈。

**请求：**
```json
{
  "content": "Great day today!",
  "media_urls": "https://img.example.com/1.jpg",
  "visibility": 1
}
```

可见性：1=公开，2=仅好友，3=私密

**响应 (201)：**
```json
{
  "moment_id": 1
}
```

### GET /moment/:momentID

**响应 (200)：**
```json
{
  "id": 1,
  "author_id": 1,
  "content": "Great day today!",
  "media_urls": "https://img.example.com/1.jpg",
  "visibility": 1,
  "created_at": "2024-01-01T00:00:00Z"
}
```

### GET /moment/user/:userID?limit=20&offset=0

获取指定用户的朋友圈。

**响应 (200)：**
```json
{
  "moments": [...]
}
```

### POST /moment/:momentID/like

**响应 (200)：**
```json
{
  "ok": true
}
```

**错误：**
- `404`：朋友圈未找到
- `409`：已经点赞

### DELETE /moment/:momentID/like

**响应 (200)：**
```json
{
  "ok": true
}
```

### POST /moment/:momentID/comment

**请求：**
```json
{
  "content": "Nice post!"
}
```

**响应 (201)：**
```json
{
  "comment_id": 1
}
```

### DELETE /moment/comment/:commentID

仅评论作者可以删除。

**响应 (200)：**
```json
{
  "ok": true
}
```

**错误：**
- `403`：不是评论作者
- `404`：评论未找到

### GET /moment/feed?last_sync_time=0&limit=20

获取用户的朋友圈动态流（来自好友的时间线）。

**响应 (200)：**
```json
{
  "moments": [...]
}
```

---

## 消息操作（需要 JWT）

### POST /msg/revoke

撤回一条消息（发送后 2 分钟内，仅发送者可撤回）。

**请求：**
```json
{
  "convId": "p_1_2",
  "msgId": 100
}
```

**响应 (200)：**
```json
{
  "message": "message revoked"
}
```

**错误：**
- `400`：消息不可撤回（超时或已被撤回）
- `403`：不是消息发送者

### DELETE /msg/:msgID?convId=p_1_2

删除一条消息（仅本地删除，对方仍能看到）。

**响应 (200)：**
```json
{
  "message": "message deleted"
}
```

### GET /msg/search?q=hello&limit=20&offset=0

搜索私聊消息。

**响应 (200)：**
```json
{
  "messages": [...]
}
```

---

## 设置（需要 JWT）

### GET /settings

**响应 (200)：**
```json
{
  "user_id": 1,
  "notification_enabled": true,
  "msg_preview_enabled": true,
  "mute_list": "",
  "created_at": "2024-01-01T00:00:00Z"
}
```

### PUT /settings

**请求：**
```json
{
  "notification_enabled": true,
  "msg_preview_enabled": false,
  "mute_list": ""
}
```

**响应 (200)：**
```json
{
  "message": "settings updated"
}
```

### POST /settings/mute

免打扰某个会话。

**请求：**
```json
{
  "convId": "p_1_2"
}
```

**响应 (200)：**
```json
{
  "message": "conversation muted"
}
```

**错误：**
- `409`：已经免打扰

### DELETE /settings/mute/:convID

**响应 (200)：**
```json
{
  "message": "conversation unmuted"
}
```

**错误：**
- `404`：未处于免打扰状态

---

## WebSocket 协议

连接到 `GET /ws?token=<access_token>`。

所有消息使用 JSON 信封格式：

```json
{
  "type": "<message_type>",
  "data": { ... }
}
```

### 客户端 → 服务端消息

| 类型 | 用途 | 数据字段 |
|------|------|----------|
| `msg` | 发送聊天消息 | `msgId`（客户端 ID）、`convType`（1=私聊，2=群聊）、`toId`、`msgType`（1=文本）、`content`、`timestamp` |
| `deliverAck` | 确认送达 | `serverMsgId` |
| `readAck` | 标记会话已读 | `convId` |
| `syncReq` | 请求离线同步 | `lastSyncTime`、`batchSize` |
| `revokeMsg` | 撤回一条消息 | `convId`、`serverMsgId` |
| `friendApply` | 好友申请（通过 WS） | （占位） |
| `ping` | 心跳 | — |

### 服务端 → 客户端消息

| 类型 | 用途 | 数据字段 |
|------|------|----------|
| `serverAck` | 消息已确认 | `clientMsgId`、`serverMsgId`、`groupSeq`（群聊）、`timestamp` |
| `msg` | 收到的聊天消息 | `msgId`、`convId`、`convType`、`fromId`、`toId`、`msgType`、`content`、`readStatus`（私聊）、`groupSeq`（群聊）、`timestamp` |
| `syncBatch` | 离线同步批次 | `msgs[]`、`hasMore`、`syncTime` |
| `convSync` | 会话同步 | `conversations[]`、`unreadMap` |
| `msgRevoked` | 消息撤回通知 | `convId`、`serverMsgId`、`operatorId` |
| `kick` | 连接被踢出 | `reason: "new_login"` |
| `friendAccepted` | 好友申请被接受 | — |
| `presence` | 在线状态变更 | — |
| `error` | 错误通知 | `code`、`message` |
| `pong` | 心跳响应 | — |

### 消息类型

| 常量 | 值 | 描述 |
|------|-----|------|
| MsgTypeText | 1 | 纯文本消息 |
| MsgTypeImage | 2 | 图片消息 |
| MsgTypeVideo | 3 | 视频消息 |
| MsgTypeSystem | 5 | 系统通知 |
| MsgTypeRevoked | 6 | 撤回占位符 |

### 会话类型

| 常量 | 值 | ID 格式 |
|------|-----|---------|
| ConvTypePrivate | 1 | `p_{较小ID}_{较大ID}` |
| ConvTypeGroup | 2 | `g_{群组ID}` |

### 单设备策略

当用户通过 WebSocket 连接时，同一用户 ID 的任何现有连接都会被踢出。旧连接会收到：

```json
{
  "type": "kick",
  "reason": "new_login"
}
```

然后旧连接的 WebSocket 会被关闭。
