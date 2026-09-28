# GoIM Postman 接口测试清单

## 使用说明

- 基础地址：`http://localhost:18080`
- API 前缀：`http://localhost:18080/api/v1`。当前本地服务使用 `configs/config.local.example.yaml`，监听端口为 `18080`；本文所有示例均可直接复制。
- 除公开接口外，统一在请求头添加：`Authorization: Bearer <访问令牌>`。
- 所有 JSON 请求使用 `Content-Type: application/json`。
- 标准成功响应为 `{ "code": 0, "message": "ok", "data": ... }`；每次通过后将 `- [ ]` 改为 `- [x]`，并填写“实测结果”。
- 建议准备三个测试用户 A/B/C，分别记录它们的用户 ID 和访问令牌。推荐用户名使用唯一值，例如 `postman_a_20260711`、`postman_b_20260711`、`postman_c_20260711`；密码统一使用 `pass1234`。

## 测试状态

| 标记 | 含义 |
| --- | --- |
| `- [ ]` | 未测试 |
| `- [x]` | 通过 |
| `- [!]` | 失败，需在《问题反馈记录》登记 |
| `- [-]` | 阻塞或不适用 |

## 第二轮 Postman 测试数据

> 第二轮开始时间：2026-07-11 19:24（Asia/Shanghai）。不记录访问令牌或刷新令牌。

| 角色 | 用户 ID | 用户名 | 当前关系 |
| --- | ---: | --- | --- |
| A2 | 152 | `postman2_a_20260711_192448` | 已与 B2、C2 成为好友；群 13 群主 |
| B2 | 153 | `postman2_b_20260711_192448` | 已与 A2、C2 成为好友；群 13 管理员 |
| C2 | 154 | `postman2_c_20260711_192448` | 已与 A2、B2 成为好友；群 13 普通成员 |

| 第二轮资源 | ID | 说明 |
| --- | ---: | --- |
| 好友申请 | 46 | 覆盖接受、拒绝、拒绝后重开并复用 ID |
| 测试群 | 13 | A2 群主、B2 管理员、C2 普通成员 |
| 公开动态 | 23 | A2 发布 |
| 好友动态 | 24 | A2 发布 |
| 私密动态 | 25 | A2 发布 |

> 密码统一为 `pass1234`；旧 A（119）的过期 Access Token 仅用于第二轮 `401` 回归。

## 第一轮 Postman 测试数据（历史）

> 不记录访问令牌或刷新令牌。

| 角色 | 用户 ID | 用户名 | 已验证关系 |
| --- | ---: | --- | --- |
| A | 119 | `postman_a_20260711_1335` | 已与 B、C 成为好友 |
| B | 120 | `postman_b_20260711_1335` | 已与 A、C 成为好友 |
| C | 121 | `postman_c_20260711_1335` | 已与 A、B 成为好友 |

| 好友申请 ID | 发起方 | 接收方 | 结果 |
| ---: | --- | --- | --- |
| 31 | A（119） | B（120） | 已接受 |
| 32 | A（119） | C（121） | 已接受 |
| 33 | B（120） | C（121） | 已接受 |

| 测试群组 ID | 名称 | 群主 | 当前成员 |
| ---: | --- | --- | --- |
| 10 | `Postman 三人测试群` | A（119） | A（119）、B（120）、C（121） |

## 0. 服务与公开资源

- [x] `GET /health`：预期 `200`，响应含 `status: "ok"`、`service: "goim"`。
  - 实测结果：2026-07-11，`200`，通过。
- [x] `GET /swagger/index.html`：预期 `200`，Swagger UI 可打开。
- [x] `GET /api/v1/avatar/{{userIdA}}?name=Alice`：预期 `200`、`Content-Type: image/svg+xml` 或返回已上传头像。
- [x] `GET /uploads/{{fileName}}`：在头像上传成功后访问，预期 `200` 且文件内容一致。

## 1. 认证

- [x] `POST /api/v1/auth/register`：注册用户 A，预期 `201`，保存 `data.user_id` 为 `userIdA`。

  ```json
  { "username": "{{usernameA}}", "password": "pass1234" }
  ```

- [x] `POST /api/v1/auth/register`：注册用户 B，预期 `201`，保存 `data.user_id` 为 `userIdB`。
- [x] `POST /api/v1/auth/register`：注册用户 C，预期 `201`，保存 `data.user_id` 为 `userIdC`。
  - 实测结果：2026-07-11，A=`119`、B=`120`、C=`121`，均返回 `201`。
- [x] `POST /api/v1/auth/register`：重复注册 A，预期 `409`、业务错误码 `1103`。
- [x] `POST /api/v1/auth/login`：登录 A，预期 `200`，保存 `data.access_token` / `data.refresh_token`。

  ```json
  { "username": "{{usernameA}}", "password": "pass1234" }
  ```

- [x] A/B/C 正确密码登录：2026-07-11，均返回 `200`，访问令牌有效期为 `7200` 秒。
- [x] `POST /api/v1/auth/login`：错误密码，预期 `401`、业务错误码 `1105`。
- [x] `POST /api/v1/auth/refresh`：使用 `refreshTokenA`，预期 `200`，更新 `accessTokenA`。

  ```json
  { "refresh_token": "{{refreshTokenA}}" }
  ```

- [x] 受保护接口不带 Token：例如 `GET /api/v1/friend/list`，预期 `401`。
- [x] 受保护接口携带伪造 Token：预期 `401`。

## 2. 好友关系（A、B、C）

- [x] `POST /api/v1/friend/request`：A 向 B 发申请，预期 `201`，保存 `data.request_id` 为 `friendRequestId`。

  ```json
  { "to_user_id": {{userIdB}}, "message": "Postman 测试好友申请" }
  ```

  - 实测结果：2026-07-11，A（119）→B（120）创建成功，`request_id=31`。

- [x] A 向自己申请好友：预期 `400`、业务错误码 `1201`。
- [x] 重复发申请：预期 `409`、业务错误码 `1204`。
- [x] `GET /api/v1/friend/requests?offset=0&limit=20`：用 B 的 Token，预期列表包含该申请。
  - 实测结果：2026-07-11，B 查询到 `id=31` 的待处理申请，分页 `total=1`、`has_more=false`。
- [x] `POST /api/v1/friend/accept`：用 B 的 Token，预期 `200`，双方成为好友。

  ```json
  { "request_id": {{friendRequestId}} }
  ```

  - 实测结果：2026-07-11，B 接受 `request_id=31` 成功，返回 A=`119`、B=`120`。

- [x] `GET /api/v1/friend/list?offset=0&limit=20`：A、B 均执行，预期彼此出现在列表中。
  - 实测结果：2026-07-11，A 的列表包含 B（`friend_id=120`、昵称正确）；B 侧待后续交叉验证。
- [x] `POST /api/v1/friend/block`：A 拉黑 B，预期 `200`。

  ```json
  { "blocked_id": {{userIdB}} }
  ```

- [x] 重复拉黑：预期 `409`、业务错误码 `1207`。
- [x] `POST /api/v1/friend/unblock`：A 取消拉黑 B，预期 `200`。
- [x] `DELETE /api/v1/friend/{{userIdB}}`：A 删除 B，预期 `200`；随后好友列表不再包含 B。
- [x] 拒绝分支：创建一条新申请后，B 请求 `POST /api/v1/friend/reject`，预期 `200`。

  ```json
  { "request_id": {{friendRequestIdToReject}} }
  ```

  - BUG-008 修复后 E2E：申请 `44` 被拒绝后，同方向再次申请返回 `201`、复用 `request_id=44` 并恢复为 `status=0`；随后再次重复申请正确返回 `409`，不再出现 `500`。

- [x] 为群聊和朋友圈测试重新建立 A↔B、A↔C、B↔C 三组好友关系；三人均应互在好友列表中。

## 3. 群组（A 为群主，B 为管理员，C 为普通成员）

- [x] `POST /api/v1/group`：A 创建群，预期 `201`，保存 `data.group_id` 为 `groupId`。

  ```json
  { "name": "Postman 测试群", "notice": "初始公告" }
  ```

  - 实测结果：2026-07-11，A 创建成功，`group_id=10`。

- [x] `GET /api/v1/group/{{groupId}}`：预期 `200`，群信息正确。
  - 实测结果：2026-07-11，`group_id=10` 的名称、公告、群主 A（119）及容量 500 均正确。
- [x] `PUT /api/v1/group/{{groupId}}`：A 更新名称/公告，预期 `200`。

  ```json
  { "name": "Postman 测试群（已更新）", "notice": "更新后的公告" }
  ```

- [x] `POST /api/v1/group/{{groupId}}/member`：A 添加 B，预期 `200`。

  ```json
  { "member_id": {{userIdB}} }
  ```

  - 实测结果：2026-07-11，B（120）已加入群 10。

- [x] 重复添加 B：预期 `409`、业务错误码 `1303`。
- [x] A 添加 C，预期 `200`。

  ```json
  { "member_id": {{userIdC}} }
  ```

  - 实测结果：2026-07-11，C（121）已加入群 10。

- [x] `GET /api/v1/group/{{groupId}}/members?offset=0&limit=20`：预期含 A、B、C。
  - 实测结果：2026-07-11，群 10 共 3 名成员：A（119）角色 2/群主，B（120）与 C（121）角色 0/普通成员。
- [x] `PUT /api/v1/group/{{groupId}}/member/{{userIdB}}/role`：A 将 B 设为管理员，预期 `200`。

  ```json
  { "role": 1 }
  ```

  - 实测结果：2026-07-11，B（120）角色更新成功，服务返回 `member role updated`。

- [x] 用 B 的 Token 更新群信息：管理员应为 `200`；用 C 的 Token 更新同一信息，预期 `403`。
  - 实测结果：2026-07-11，B（管理员）更新公告成功；C（普通成员）返回 `403`、错误码 `1301`。
- [x] `POST /api/v1/group/{{groupId}}/leave`：C 退群，预期 `200`。
  - 实测结果：2026-07-11，C（121）退群成功，服务返回 `left group`。
- [x] A 作为群主退群：预期并实测 HTTP `403`、业务码 `1306`；群主需先转让群主身份或解散群组。
- [x] 重新添加 C 后，`DELETE /api/v1/group/{{groupId}}/member/{{userIdC}}`：预期 `200`。

## 4. 朋友圈

- [x] 先确保 A、B 已互为好友（若第 2 节已删除好友，请重新建立关系）。
- [x] `POST /api/v1/moment`：A 发布好友可见动态，预期 `201`，保存 `data.moment_id` 为 `momentId`。

  ```json
  { "content": "Postman 朋友圈测试", "visibility": 2 }
  ```

- [x] `GET /api/v1/moment/{{momentId}}`：A 请求，预期 `200`，内容正确。
- [x] `GET /api/v1/moment/user/{{userIdA}}?limit=20&offset=0`：预期列表包含该动态。
- [x] `GET /api/v1/moment/feed?limit=20`：用 B 的 Token，等待 1–2 秒后请求；预期 Feed 含该动态。
- [x] `POST /api/v1/moment/{{momentId}}/like`：B 点赞，预期 `200`，`data.liked=true` 且计数增加。
- [x] 重复点赞：预期 `200`，计数不重复增加（幂等）。
- [x] `DELETE /api/v1/moment/{{momentId}}/like`：B 取消赞，预期 `200`，`data.liked=false`。
- [x] `POST /api/v1/moment/{{momentId}}/comment`：B 评论，预期 `201`，保存 `data.comment_id` 为 `commentId`。

  ```json
  { "content": "Postman 评论测试" }
  ```

- [x] `DELETE /api/v1/moment/comment/{{commentId}}`：B 删除自己的评论，预期 `200`。
- [x] A 删除他人的评论：使用有效 Token 时实测 HTTP `403`、业务码 `1503`；此前的 `401` 是 Access Token 过期后被认证中间件拦截。
- [x] 私密动态：A 发布 `visibility: 3`，B 查询 Feed 不应看到该动态。
- [x] 分页：连续发布多条动态，使用响应中的 `next_cursor` 请求下一页，验证无重复、无遗漏。

## 5. 设置与文件

- [x] `GET /api/v1/settings`：A 请求，预期 `200`，首次返回默认设置。
- [x] `PUT /api/v1/settings`：更新通知和预览开关，预期 `200`。

  ```json
  {
    "notification_enabled": true,
    "msg_preview_enabled": false,
    "mute_list": "[]"
  }
  ```

- [x] `POST /api/v1/settings/mute`：添加会话免打扰，预期 `200`。

  ```json
  { "convId": "p_{{userIdA}}_{{userIdB}}" }
  ```

- [x] 重复免打扰：预期 `409`、业务错误码 `1702`。
- [x] `DELETE /api/v1/settings/mute/p_{{userIdA}}_{{userIdB}}`：预期 `200`。
- [x] `POST /api/v1/upload/avatar`：Body 选 `form-data`，键名 `file`，类型选 File，上传 jpg/png 文件；预期 `200` 并保存 `data.url`。
- [x] 上传非白名单格式或超出大小限制文件：实测 `.md` 返回 `400`、业务错误码 `1002`。

## 6. WebSocket 消息与消息操作

> Postman 选择 **New → WebSocket**，使用：`ws://localhost:18080/ws?token=访问令牌本体`。另开一个连接供 B 使用。WebSocket 请求不使用 HTTP Authorization 头；URL 中不得保留 `<` 或 `>`。

- [x] A、B、C 已互为好友；分别建立 A/B/C WebSocket 连接，连接成功。
- [x] 私聊：A 发送以下消息，预期 A 收到 `serverAck`，B 收到 `msg`；保存 `serverMsgId` 为 `privateMsgId`。

  ```json
  {
    "type": "msg",
    "data": {
      "msgId": "pm-{{$timestamp}}",
      "convType": 1,
      "toId": {{userIdB}},
      "msgType": 1,
      "content": "Postman 私聊测试",
      "timestamp": {{$timestamp}}
    }
  }
  ```

  `timestamp` 必须是 Unix **毫秒**；Postman 可用 Pre-request Script：`pm.variables.set("timestamp", Date.now())`。

  - 实测结果：2026-07-11，A 收到 `serverMsgId=17` 的回执；B 收到一条内容正确的 `msgId=17` 私聊消息，未观察到重复。

- [x] B 向服务端发送送达确认：

  ```json
  { "type": "deliverAck", "data": { "serverMsgId": {{privateMsgId}} } }
  ```

- [x] B 发送已读确认：

  ```json
  { "type": "readAck", "data": { "convId": "p_{{userIdA}}_{{userIdB}}" } }
  ```

- [x] B 重连后发送同步请求，预期收到 `syncBatch` 和 `convSync`。

  - 实测结果：2026-07-11，B 重连后发送 `syncReq(lastSyncTime=0,batchSize=50)`，收到包含历史消息的 `syncBatch` 和会话摘要/未读信息的 `convSync`。

  ```json
  { "type": "syncReq", "data": { "lastSyncTime": 0, "batchSize": 50 } }
  ```

  后续页必须同时回传上一批响应中的复合游标：

  ```json
  {
    "type": "syncReq",
    "data": {
      "lastSyncTime": {{syncTime}},
      "lastSyncMsgId": {{syncMsgId}},
      "batchSize": 50
    }
  }
  ```

- [x] A 发送群消息：先确保 A、B、C 均为群成员，`convType: 2`、`toId: {{groupId}}`；预期 `serverAck` 包含 `groupSeq`，B 和 C 均收到同一 `msg`。
  - 首次实测异常：`msgId=1` 与既有 MySQL 主键冲突，触发重复推送，详见 BUG-004。
  - 清理并对齐测试环境计数器后复测：2026-07-11，A 收到 `serverMsgId=16`、`groupSeq=2`；B/C 各仅收到一条相同的 `msgId=16` 消息，验证通过。
- [x] C 退出群后再次发送群消息：预期 C 不再收到该消息；C 主动发送群消息时收到 `error`，业务错误码 `5001`。
  - 实测结果：2026-07-11，C 退群后发送群消息返回 `{"type":"error","data":{"code":5001,"message":"不是该群组的成员"}}`；A/B 未收到新消息。
- [x] 消息去重：用相同 `msgId` 重发，预期 A 收到 `error`，业务错误码为私聊 `4003` 或群聊 `5003`。
- [x] 非好友私聊：预期 `error`，业务错误码 `4001`。
- [x] 非成员群聊：预期 `error`，业务错误码 `5001`。
- [x] WebSocket 撤回：A 在两分钟内发送，预期双方收到 `msgRevoked`。

  ```json
  {
    "type": "revokeMsg",
    "data": { "convId": "p_{{userIdA}}_{{userIdB}}", "serverMsgId": {{privateMsgId}} }
  }
  ```

- [x] 单设备登录：使用同一 A Token 建立第二个 WebSocket；第一个连接预期先收到：

  ```json
  { "type": "kick", "reason": "new_login" }
  ```

  随后断开。
- [x] HTTP 搜索：待消息异步落库后请求 `GET /api/v1/msg/search?q=Postman&limit=20&offset=0`，预期包含对应私聊消息。
  - 实测结果：2026-07-11，A 按“私聊消息”搜索，返回 `msgId=17`、内容为“A 发给 B 的私聊消息”。
- [x] HTTP 撤回：重新发送一条未撤回私聊消息后，A 请求 `POST /api/v1/msg/revoke`，预期 `200`。

  ```json
  { "convId": "p_{{userIdA}}_{{userIdB}}", "msgId": {{privateMsgId}} }
  ```

  - 首次实测：消息 `msgId=18` 返回 `1401`；排查确认客户端提交的消息时间戳距撤回请求已超过两分钟，符合撤回窗口限制。
  - 复测：2026-07-11，使用当前毫秒时间戳发送 `msgId=19` 后立即撤回，返回 `200`、`message=message revoked`。

- [x] HTTP 删除：发送另一条消息后，A 请求 `DELETE /api/v1/msg/{{privateMsgId}}?convId=p_{{userIdA}}_{{userIdB}}`，预期 `200`。
  - 修复后 E2E：收到 `serverAck` 后不等待，立即撤回和 HTTP 删除均返回 `200`；不再依赖约 300ms 的异步写入延迟。
  - 其他边界复测：去重返回 `4003`；非好友返回 `4001`；单设备收到 `kick/new_login`；撤回双方收到 `msgRevoked`。

## 7. 第二轮新增回归与边界测试

> 本节是 2026-07-11 修复 BUG-001～008 后新增的长期回归项。正式开始第二轮全量测试时，再将前述 73 项统一重置为待测；当前保留第一轮结果作为历史证据。

### 7.1 认证边界

- [x] 注册用户名少于 3 个字符：预期 `400`、业务码 `1101`。
- [x] 注册密码少于 6 个字符：预期 `400`、业务码 `1102`。
- [x] 登录不存在的用户名：预期 `401`、业务码 `1104`。
- [x] 使用伪造或已过期的 Refresh Token：预期 `401`、业务码 `1106`。
- [x] 使用已过期的 Access Token 请求受保护接口：预期 `401`；确认不是业务权限错误 `403`。

### 7.2 好友关系边界

- [x] A 拉黑 B 后先删除好友关系但保留黑名单，再验证 A→B 与 B→A 申请均返回 `403/1203`；取消拉黑后恢复正常。
- [x] BUG-005 回归：接受或拒绝申请后查询 `GET /friend/requests`，响应只包含 `status=0`，不包含 `status=1/2`。
- [x] 使用非接收者 Token 接受或拒绝申请：预期 `403`、业务码 `1206`。
- [x] 接受或拒绝不存在的 `request_id`：预期 `404`、业务码 `1205`。
- [x] BUG-008 回归：申请被拒绝后同方向再次申请预期 `201`、复用原 `request_id` 并恢复 `status=0`；紧接着重复申请预期 `409/1204`。
- [x] 黑名单与好友关系独立：拉黑本身不自动删除好友；显式删除好友后，取消拉黑也不会自动恢复好友关系。

### 7.3 群组权限与参数

- [x] 空群名创建、非法字符串群 ID：预期 `400`；查询不存在群 ID：预期 `404/1302`。
- [x] 普通成员 C 更新群信息、添加成员、移除成员、修改角色：均预期 `403/1301`。
- [x] 管理员 B 更新群信息、添加普通成员、移除普通成员：均预期成功；管理员不得修改群主角色。
- [x] 管理员 B 尝试移除群主 A：预期 `403/1305`；B 修改任何成员角色预期 `403/1301`；群主 A 修改自己的角色预期 `403/1305`。
- [x] A 将成员角色设置为 `-1`、`2` 等非法值：预期 `400`、业务码 `1307`。
- [x] BUG-009 回归：A 将管理员 B 的角色设置回合法值 `0`，返回 `200`；缺少 `role` 仍返回 `400/1001`。
  - 修复后实测：群 13 中 B2 成功 `role 1→0`，随后恢复为 `role=1`；真实 E2E 与 `internal` 全量测试通过。
- [x] 群成员分页：使用较小 `limit` 连续请求，验证 `total/offset/has_more` 与成员无重复、无遗漏。

### 7.4 朋友圈权限与分页

- [x] 发布空内容动态：预期 `400/1501`；使用 `visibility=0/4`：预期 `400/1504`。
  - BUG-010/012 修复后：空字符串或缺少 `content` 均返回 `400/1501`；显式 `visibility=0/4` 均返回 `400/1504`；缺失可见性默认公开 `1`。
- [x] 查询、点赞或评论不存在的动态：预期 `404/1502`，不得产生点赞或评论记录。
- [x] 可见性矩阵：公开动态所有登录用户可见；好友动态仅好友可见；私密动态仅作者可见。
  - BUG-011 修复后：A2 可见动态 23/24/25；B2 可见 23/24；非好友 119 仅可见 23，无权详情返回 `404/1502`。
- [x] 在动态详情、用户动态列表、Feed 三个入口分别验证可见性，结果必须一致，不能通过详情接口绕过隐私。
  - 修复后真实 E2E 与第二轮现场矩阵均通过，三个入口复用同一可见性规则。
- [x] 发表评论内容为空：预期 `400/1501`；删除不存在的评论：预期 `404/1505`。
  - BUG-012 修复后：空字符串或缺少 `content` 均返回 `400/1501`；malformed JSON 保持 `400/1001`。
- [x] 未点赞时直接取消点赞、连续取消两次：接口保持幂等，点赞数不得变成负数。

### 7.5 设置与上传边界

- [x] 删除不存在的免打扰会话：预期 `404`、业务码 `1703`。
- [x] 设置更新使用非法 JSON，或免打扰请求缺少 `convId`：预期 `400`，原设置不得被修改。
- [x] 上传请求不包含 `file`：预期 `400`、业务码 `1001`。
- [x] 单独验证超过 `max_size_mb` 的白名单格式文件：预期 `400/1002`；不能只用非法扩展名代替超限测试。

### 7.6 WebSocket 与消息操作回归

- [x] WebSocket 缺少、伪造、已过期 Token：握手均预期 `401`；有效新 Token 可正常重连。
- [x] 发送非法 JSON、缺少 `type/data`、未知消息类型：连接不崩溃，客户端收到明确 `error` 或请求被安全忽略。
- [x] BUG-003 回归：私聊接收方对同一 `serverMsgId` 只收到一次 `msg`，Redis 收件箱也只有一条。
- [x] BUG-004 回归：连续快速发送多条私聊与群聊消息，`serverMsgId` 全部唯一、递增且不超过 JavaScript 安全整数。
- [x] BUG-007 回归：收到 `serverAck` 后不加任何等待，立即执行 WebSocket 撤回、HTTP 撤回和 HTTP 删除，均应成功。
- [x] 非发送者撤回消息：WebSocket/HTTP 均应拒绝；超过两分钟撤回预期 `400/1401`，原消息保持不变。
- [x] `syncReq` 分页：设置较小 `batchSize`，使用响应的 `syncTime + syncMsgId` 复合游标继续同步，验证 `hasMore`、顺序及无重复遗漏。
  - BUG-013 修复后：B2 现场分页第一页 `[1783770205809001,1783770205730001]`，第二页 `[1783770205749001,1783770205762001]`，无重叠；同毫秒 3 条消息的真实 E2E 按 2+1 分页无遗漏。
- [x] 消息搜索权限与分页：A 只能搜索到自己参与的私聊；使用 `offset/limit` 翻页时无重复、无越权结果。

## 测试批次记录

| 日期 | 测试人 | 服务版本/提交 | 通过 | 失败 | 阻塞 | 备注 |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 2026-07-11 |  | 本地开发环境 | 1 | 0 | 0 | 健康检查已通过 |
| 2026-07-11 | Codex 自动直测 | `localhost:18080` | 17 | 0 | 0 | 认证刷新、设置、头像上传与访问、朋友圈主链路均通过。 |
| 2026-07-11 | Codex 第二轮全量回归 | BUG-001～008 修复后本地服务 | 103 | 6 | 0 | 共 109 项（测试中新增 BUG-009 项）；新增 BUG-009～013。`internal` 全量与完整 E2E 均通过。 |
| 2026-07-11 | Codex 修复后回归 | BUG-001～013 修复后本地服务 | 109 | 0 | 0 | BUG-009～013 均已关闭；复合游标现场验证、`internal` 全量与完整 E2E 均通过。 |

