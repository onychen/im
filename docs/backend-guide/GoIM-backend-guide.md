# GoIM 后端从零到一

呈现参考：https://pi-from-scratch.vercel.app/#chapter2

源码基线：1fc79b0adb1fb6b6dc2604e1cf58ef1f7ce27b8d；包含 2026-10-03 当前工作区修改，文件快照哈希见 source-manifest.json。

五个可运行实验在独立 labs 模块。后续实现作业、真实项目源码与状态模拟均有明确标签。

<a id="chapter1"></a>

## 01 · 先定义：什么才算发出一条消息？

一切从 Alice 给 Bob 发出“你好”开始。先说清楚承诺，再决定代码长什么样。

## 你正在造什么
GoIM 是一个 Web 即时通讯系统。浏览器负责展示，后端负责身份、权限、保存和分发。默认发送路径先写 Redis，再由 Relay 转交 Kafka。Kafka 确认后，实时推送和 MySQL 归档分别进行：

```text
浏览器 → 服务 → Redis（读模型 + Stream）→ Relay → Kafka 确认
                                                    ├─ Relay → 发送回执 / 接收者
                                                    └─ 消费者 → MySQL 异步归档
```

接收者不必等 MySQL 落库才能收到消息；归档也可能先于实时展示完成。

假设你已能写函数、循环和条件语句；Go 不熟也可以，从下一章的可运行实验开始。全课阅读与基础实验约 26.5 小时，可分成约 14 次、每次 2 小时的学习。依赖下载、排障和扩展作业另留时间。进度按每章的可观察结果判断。

## 把需求变成可检验的约束
- Alice 必须先登录；消息里的发送者由服务端身份决定。
- 私聊的双方必须是好友，Bob 拉黑 Alice 后拒绝发送。
- 同一个客户端消息重复发送，不能生成两条独立消息。
- Bob 离线时，重连以后仍应能补回消息。
- 用户看见“发送成功”时，系统必须能解释成功到哪一步。
- 群消息有会话内顺序；已读游标不能倒退。

这是开发的第一份产物：验收契约。它会影响数据库主键、协议字段、重试策略和测试，不只是需求文档里的文字。

## 成功其实有四层
1. 请求通过校验。
2. 消息已写入实时读模型和待归档事件。
3. Kafka 已接受归档事件。
4. MySQL 已完成归档；接收端是否展示又是另一件事。

当前默认首次发送链路在第 3 层之后发送 serverAck，但 MySQL 归档仍异步进行。重复请求目前在私聊和群聊都返回重复错误，并未统一实现原 ACK 重发。不要把任何 ACK 自动理解成“接收者看见了”或“硬盘永不丢失”。后面会实测这些差别。

## 用一个纵向切片推进
每次只让一条用户行为穿过所有必要层。先做 /health，再做输入校验、注册登录、数据库，再做通信。第一轮能运行、能拒绝错误输入、能验证结果，比一开始铺齐所有目录更有价值。

五个实验在独立 labs 模块运行，各章注明工作目录；完整 GoIM 用于项目对照与联调。源码栏展示当前工作区快照，学习顺序按理解安排。

## 这一章的设计记录
写一页自己的约束清单：哪些信息可以最终一致？哪些动作必须即时拒绝？哪些“成功”必须等持久化？到第 12 章时回头改它，你会看到架构选择来自承诺，而不是技术名字。

### 动手任务
画出 Alice 发消息、Bob 离线、Bob 重连这三个场景；为每个场景写输入、权限、状态变化和成功条件。

### 验收标准
- 能区分“接收”“队列确认”“归档”“展示”四种状态。
- 有一份至少包含正常、重复、离线、无权限四类场景的验收清单。

### 自检
如果 serverAck 已返回而 MySQL 暂时不可用，是否一定是系统错误？

取决于 ACK 契约。当前默认首次发送路径在 Kafka 确认后 ACK，MySQL 可稍后归档。真正要检查的是事件有没有保留、重试是否能恢复，以及客户端是否把“队列已确认”误写成“已归档”。

### 对照文件
- backend/internal/model/message.go
- backend/internal/protocol/protocol.go
- backend/internal/relay/message_relay.go

<a id="chapter2"></a>

## 02 · 从空目录启动第一个服务

20 多行代码，先亲眼看见一次 HTTP 请求走完。

## 动手：从真正的空目录开始
页面顶部可以下载实验包。也可以自己创建 nanoim 文件夹，将右侧实验 01 的完整代码保存为 01-http/main.go。实验用 18100 端口，项目用 18080，避免互相覆盖。以下命令在 PowerShell 中执行。

```powershell
New-Item -ItemType Directory nanoim
Set-Location nanoim
go mod init example.com/nanoim
New-Item -ItemType Directory 01-http
# 将右侧 main.go 完整代码保存到 01-http/main.go
go run ./01-http
```

如果使用下载包，解压后进入含 go.mod 的 labs 目录即可，不需要重复 go mod init。另开一个终端发请求：

```powershell
Invoke-RestMethod http://localhost:18100/health
```

预期得到 status=ok、service=nanoim。把 GET 改成 POST，再请求 /unknown：分别观察 405 和 404。浏览器能打开页面，只说明一条路径可达；错误路径也要符合协议。

## 把每一行想明白
package main 代表可执行程序；main 是入口。http.ServeMux 按方法和路径路由。Handler 从 Request 读输入，用 ResponseWriter 写输出。Header 必须在写响应体以前设置，JSON 不是随手拼字符串。

net/http 会并发处理请求。即使你只写了一个 Handler，也不能假设所有请求串行。这就是后面 map 需要锁、SQL 需要唯一约束的根源。

## Go 的第一组工具
go.mod 记录模块路径、Go 版本和依赖；go.sum 校验依赖内容。go fmt 统一排版，go test 编译并运行测试，go build 生成可执行文件。错误是返回值：必须判断 err，决定是返回、重试还是终止，不能只打印后继续。

GoIM 的模块是 github.com/goim/goim，go.mod 要求 Go 1.24.0；本次检查环境为 Go 1.25.1。教程采用项目锁定的依赖版本，不把“升级到最新”混入开发步骤。

## 从实验迁移到项目
真正的入口在 cmd/server/main.go。main 解析 -c 参数，然后 run 装配配置、日志、存储、业务服务、消费者和 HTTP 服务。你现在只需要识别：它是一个启动和组装入口，业务规则应放到 service。

## 扩展作业
给实验增加 GET /version，返回版本和启动时间。连续请求三次，启动时间应相同。先思考变量是在 main 中初始化，还是在 Handler 中初始化，这个差别就是状态生命周期。

### 动手任务
自己敲出实验 01 并启动，增加 /version；记录 GET、POST、不存在路径的状态码。

### 验收标准
- /health 返回正确 JSON 和 Content-Type。
- 能解释监听端口、路由、Handler、模块的区别。
- 服务重启后启动时间改变，同次启动的三次请求时间相同。

### 自检
为什么把启动时间放进 Handler 会得到不同结果？

Handler 每次请求都会执行。在 main 里计算一次并由闭包读取，是进程状态；在 Handler 里计算，是请求状态。以后数据库池属于进程状态，当前用户属于请求状态。

### 对照文件
- labs/01-http/main.go
- labs/go.mod
- backend/go.mod
- backend/cmd/server/main.go

<a id="chapter3"></a>

## 03 · Gin：让请求有明确的入口和出口

把“收到 JSON”变成“验证过的命令”，写第一份接口契约。

## 动手：加入项目的 HTTP 框架
在同一个 labs 模块内新增 02-gin/main.go，复制侧栏实验 02 的完整文件。停止实验 01，然后运行：

```powershell
go get github.com/gin-gonic/gin@v1.9.1
go run ./02-gin
```

下载的实验包已经声明依赖，首次执行 go mod tidy 后运行即可。

```powershell
$body = @{msgId='lesson-001'; toId=2; content='你好'} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:18100/api/v1/messages -ContentType 'application/json' -Body $body
```

预期 code=0，data 原样返回验证过的字段。把 toId 改成 -1，或 content 改成空白：预期 HTTP 400。这个实验尚未保存或分发消息。

## Handler 负责翻译
网络输入是不可信的字节。请求结构体叫 DTO，用来描述接口接收的字段。ShouldBindJSON 将 JSON 解码到 DTO，binding 做字段校验；Handler（请求处理函数）再把结果翻译成 HTTP 状态和响应体。业务方法使用 context.Context 传递取消与超时，分层方式见下一章。

只设置 binding:"required" 还不够：数字有范围，字符串有长度，空白内容可能要拒绝。实验限制整个请求体 4096 字节，字段长度约束和整体体积约束分开处理。

## 两套错误信息各有读者
HTTP 401 让调用方知道需要认证；业务码让前端知道具体原因；日志给维护者说明底层错误。当前项目的统一响应是 code、message、data，不是所有接口都使用同一个 HTTP 状态。

学习时先记录契约，再看实际路由和 DTO。不要凭 Swagger 注释推断字段。GoIM 注册返回 user_id，登录返回 access_token；WebSocket 请求却使用 camelCase。跨端契约必须按实际类型核对。

## 中间件是请求经过的关卡
中间件是 Handler 之前的公共处理：Logger 记录请求，Recovery 捕获 panic，鉴权提取身份。c.Next 继续处理，Abort 停止后续 Handler。鉴权与执行顺序在[注册与登录](#chapter6)中练习。

## 第一份接口说明
自己写出路径、方法、请求字段、返回字段、状态码、鉴权要求，以及三个错误样例。以后新增接口沿用这份清单；[贯通实战](#chapter19)会用它串起业务、配置和浏览器。

### 动手任务
实现实验 02；再添加 msgType 字段，只接受 1、2、3，拒绝未知类型和超大请求体。

### 验收标准
- 正常请求返回 200；非法 toId 和空白内容返回 400。
- 能把传输错误、业务错误、内部故障分别说明。
- 新字段的请求、约束和失败返回已写进接口说明。

### 自检
为什么通过 JSON 解析还不代表请求可以执行？

解析只证明结构能够解码。发送给负数用户、空白内容、未授权群组在语法上都是合法 JSON。类型校验、业务约束、资源权限是不同关卡。

### 对照文件
- labs/02-gin/main.go
- backend/internal/api/response.go
- backend/internal/api/auth_handler.go

<a id="chapter4"></a>

## 04 · 分层与依赖：给业务留出可替换的边界

把上一章的一大块 Handler 拆成能单独理解、测试的三个部分。

## 先重构一次你自己的代码
保留实验 02 的行为，把消息字段规则提取为 ValidateSend(req) error；Handler 只负责绑定、调用、响应。再定义一个只包含 Save(ctx, message) error 的小接口，先用内存实现。这个接口是为一个实际调用点产生的，暂时不创建几十个空接口。

```go
type MessageStore interface {
    Save(ctx context.Context, message Message) error
}
type MessageService struct { store MessageStore }
```

上面是教学接口，不是 GoIM 当前同名类型，需自行补上 Message 结构体。把实现依赖传入构造函数，避免在 Save 内临时打开数据库。

## 对照真实项目的责任
- api：把 HTTP 请求和响应翻译成 Go 值。
- service：规定注册、加好友、发消息、点赞允许怎么做。
- repository：封装 SQL、Redis 命令和 Kafka 投递。
- infra：构造连接池和客户端，做健康监控。
- conn / ws：维护连接生命周期与协议分发。
- relay / consumer：把待办转成队列事件，再归档。

cmd/server 是知道这些实现的装配点。HTTP 与 WebSocket 可以调用同一个业务服务，因为规则不应该由每种传输重复实现。

## 这里值得学，也有可改进之处
GoIM 的 MySQLRepo 接口较大，测试 mock 需要实现许多方法。这是一个阅读成本与替换成本的信号。AtomicMessageWriter、StreamStore 等较小接口展示了另一种边界：使用者只知道它需要的能力。

练习把“注册需要的用户存储”定义成 CreateUser、GetUserByUsername 的小接口。不要立即重写整个项目；先让一个新模块用小接口，验证测试是否更清楚。好接口把复杂实现藏起来，仍把失败语义告诉调用方。

## Go 接口的直觉
Go 的接口由方法集合满足，不需要显式 implements。返回具体实现、接收小接口常有助于保持简单，但不是机械规则。context.Context 表达取消和期限，应沿调用链传递。

## 不要误把目录当成架构
你能把同一段业务复制进三个文件夹，却仍然没有边界。问自己：换掉 MySQL 实现时，哪些业务代码要改？用 mock 时是否只需实现两三个方法？答案比文件夹名称更能说明设计质量。

### 动手任务
在自己的实验中拆出 ValidateSend 和 MessageStore；写一个内存实现，把验证规则从 Gin Handler 移出去。

### 验收标准
- HTTP 行为和重构前一致。
- 不启动 HTTP 就能调用业务校验。
- 能在 main 中替换 store，而 Handler 不用修改。

### 自检
为什么业务方法接收 context.Context，而不直接接收 *gin.Context？

业务需要取消和超时，不一定需要路由、Header 或响应写入。传最小必要能力能让同一个服务被 HTTP、WebSocket、消费者和测试复用。

### 对照文件
- backend/internal/service/auth_service.go
- backend/internal/repository/mysql_repo.go
- backend/cmd/server/main.go

<a id="chapter5"></a>

## 05 · MySQL：先让数据重启后还存在

从一张 users 表理解查询、索引、事务，以及并发下谁来守住规则。

## 开发顺序：先写查询场景，再设计表
用户需要按用户名登录、按 ID 查资料；用户名必须唯一。好友关系需要按当前用户列出，并检查两个人是否互为好友。群消息需要按 group_id 和 group_seq 取一页。每个索引都应能回答“服务哪条查询”。

打开 users 的迁移文件，找主键、用户名唯一约束和密码哈希列。GoIM 使用 database/sql 和 SQL 驱动，没有 GORM。读 SQL、理解扫描和事务是这套项目的必修课。

## 动手：把内存用户换成数据库
在自己的练习模块中接入项目锁定的 MySQL 驱动，使用教学专用数据库 nanoim_lab。不要在练习里改 GoIM 正在使用的数据。先执行一张 users 表迁移，再实现 CreateUser 和 GetUserByUsername。

```powershell
go get github.com/go-sql-driver/mysql@v1.10.0
```

```sql
CREATE DATABASE IF NOT EXISTS nanoim_lab CHARACTER SET utf8mb4;
USE nanoim_lab;
CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  username VARCHAR(50) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  UNIQUE KEY uk_username (username)
);
```

SQL 是独立实验表；不要复制到项目迁移目录。连接池、PingContext、ExecContext、QueryRowContext 的完整用法就在侧栏的 infra/mysql.go 与 mysql_repo.go，按用户相关方法实现自己的小仓储。查询参数使用 ?，不要拼接输入。

## sql.Open 不等于连通
sql.Open 构造数据库句柄，PingContext 才能验证连接。连接池不是一个连接，池大小必须与数据库容量配合；无限放大池会把排队搬到数据库。请求结束后不要关闭全局池，rows 和事务等短期资源则需要按生命周期释放。

## 并发会推翻“先查再插”
两个注册请求都查到用户名不存在，然后都插入。只有数据库唯一约束能在并发下守住唯一性；预查询用于友好提示，插入错误仍必须映射。当前注册分支对插入错误的表达仍有改进空间，不能把已有代码都当作最佳实践。

## 事务的真正用途
接受好友申请会改变申请状态并建立双向关系。若只写一边就崩溃，会出现 Alice 认为是好友而 Bob 不认为是好友。一个 MySQL 事务可保护库内这些修改；它不会自动保护 Redis 和 Kafka。[Lua 与 Stream](#chapter11)会进一步说明跨存储移交。

## 索引要通过执行计划解释
对侧栏 idx_group_seq 写 EXPLAIN 查询。复合索引字段顺序取决于过滤与排序，不是“出现在哪就加在哪”。取消息使用稳定游标，深分页 OFFSET 的扫描成本会增长。

## 迁移也有生命周期
Docker MySQL 的初始化目录只在空数据目录初始化时自动执行。老卷不会因为重启自动应用新 SQL。现有迁移含 ALTER TABLE，重复执行可能失败；应记录已应用版本、逐条确认，而不是反复运行全部文件，更不要为更新迁移删除数据卷。

### 动手任务
将实验用户存储改为 MySQL；实现唯一约束错误映射；对同名注册发起并发请求，并验证重启后用户仍可查到。

### 验收标准
- 重启服务不丢失用户。
- 并发注册同一用户名只有一条记录。
- 能解释事务边界和至少一条 EXPLAIN 结果。

### 自检
用了事务，为什么 MySQL 和 Redis 仍可能不一致？

MySQL 事务只控制该库的参与操作。提交后写 Redis 失败，或者写 Redis 后 MySQL 回滚，都会产生跨系统差异。需要明确主事实来源，并设计重建、补偿或同存储 outbox。

### 对照文件
- backend/scripts/migrations/001_create_users.sql
- backend/scripts/migrations/002_create_friendships.sql
- backend/scripts/migrations/004_create_messages.sql
- backend/internal/infra/mysql.go
- backend/internal/repository/mysql_repo.go

<a id="chapter6"></a>

## 06 · 注册与登录：身份不能由客户端自报

亲手完成 bcrypt 和 JWT，再辨认登录、鉴权与授权的不同。

## 动手：先跑通一个完整身份实验
新增 03-auth/main.go，复制右侧完整实验文件，停掉前一个实验进程。下载包在 labs 目录执行 go mod tidy 后已有全部依赖。

```powershell
go run ./03-auth
```

```powershell
$base = 'http://localhost:18100/api/v1'
$body = @{username='alice'; password='lesson123'} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/auth/register" -ContentType 'application/json' -Body $body
$login = Invoke-RestMethod -Method Post -Uri "$base/auth/login" -ContentType 'application/json' -Body $body
$headers = @{Authorization="Bearer $($login.data.access_token)"}
Invoke-RestMethod -Uri "$base/me" -Headers $headers
```

应得到 user_id=1。去掉 Header 或改坏 token，预期 401；再次注册同名用户，预期 409。实验存储仍是内存，随机密钥每次启动变化；上一章的 MySQL 替换是你的实现作业，实验 03 是可运行参考答案。

## 为什么存 bcrypt
密码哈希是单向验证，不需要取回明文。bcrypt 的 salt 让相同密码生成不同哈希，cost 让暴力猜测更昂贵，也增加正常登录的 CPU 成本。不要对客户端密码简单做 MD5。项目 bcrypt cost=10；bcrypt 还有输入字节上限，字段长度策略要考虑中文字符与字节差异。

## JWT 带来什么，没带来什么
签名让服务验证声明未被伪造，payload 通常可被解码，不用于保存秘密。验签、算法限制、exp、issuer/audience 与 token 用途都是需要明确的策略。实验限制 HS256 且要求过期字段；侧栏是项目当前实现，两者可对照。

JWT 让请求携带身份，不等于授权。Alice 的有效 token 不允许删除 Bob 的评论，也不允许给任意群发消息。资源的所有者与成员关系应在业务层检查。

## access 和 refresh 的边界要分开
项目签发访问令牌和刷新令牌，但当前 Claims 没有用途字段，ParseToken 也没有做 access/refresh 类型隔离。不要将“有两个接口”理解成已经实现刷新令牌轮换、撤销和退出登录失效。

改进作业：加 token_type、限制算法，访问入口只接受 access，刷新入口只接受 refresh；为旧 token 兼容作出显式迁移策略。密钥轮换、密码修改后的会话失效要有服务端状态设计。

## 回到 GoIM 做一次验证
实验 03 通过后，先按下一章启动 GoIM，再用项目端口 18080 调用 auth/register 与 auth/login。记录返回的真实用户 ID，用 access_token 调用 /api/v1/friend/list，观察中间件如何填入 userID。

## 追踪一次中间件执行
Gin 按注册顺序执行中间件。c.Next 前后分别适合记录开始与完成；拒绝请求要 Abort 加 return：前者阻止后续 Handler，后者退出当前 Go 函数。

项目 setupRouter 使用 gin.New，显式加入 Recovery 与 CORS；受保护路由组再加入 JWT。这里没有注册 gin.Logger，启动日志也不等于 HTTP 访问日志。下一章先启动 GoIM；随后在[贯通实战](#chapter19)观察浏览器请求、预检和请求 ID。


### 动手任务
完成实验 03 和 MySQL 版本；列出 token 过期、算法不匹配、刷新 token 访问业务接口的测试。

### 验收标准
- 正确密码能登录，错误密码被拒绝，未携带 token 的受保护路径返回 401。
- 只存密码哈希，日志不记录完整 token。
- 能指出当前项目尚缺的用途隔离与撤销能力。
- 能从 setupRouter 找到公共路由、JWT 路由组与中间件执行顺序。

### 自检
JWT 已验签，客户端请求体里的 fromId 能直接相信吗？

不能。验签建立的是请求身份，fromId 仍是客户端输入。消息发送者应从已验证身份取得，授权再判断这个身份能否操作目标资源。

### 对照文件
- labs/03-auth/main.go
- backend/internal/service/auth_service.go
- backend/internal/middleware/auth_middleware.go
- backend/internal/middleware/auth_middleware_test.go
- backend/cmd/server/main.go

<a id="chapter7"></a>

## 07 · 把完整 GoIM 启起来，读懂装配过程

现在把你的最小服务与真实项目接起来：看配置、连接池和后台任务如何一起工作。

## 先区分两个工作目录
此前实验在 nanoim/labs；从现在起默认在 GoIM 的 im 目录。项目已经存在，不需要再次 go mod init。后端命令在 im/backend 执行；docker compose 在 im 执行。已有本地配置就沿用，不覆盖你的改动。

```powershell
# 工作目录：im；只在目标文件不存在时复制模板
if (!(Test-Path .env)) { Copy-Item .env.example .env }
if (!(Test-Path backend/configs/config.local.yaml)) {
  Copy-Item backend/configs/config.local.example.yaml backend/configs/config.local.yaml
}
docker compose up -d mysql redis kafka
docker compose ps
```

等三个依赖健康后，在另一个终端：

```powershell
Set-Location backend
go mod download
go run ./cmd/server -c configs/config.local.yaml
```

```powershell
Invoke-RestMethod http://localhost:18080/health
Invoke-RestMethod http://localhost:18080/ready
```

第一次启动空 MySQL 卷会应用迁移，已有卷需确认迁移版本。若缺少依赖校验项，再运行 go mod tidy 并检查 go.mod/go.sum 变化；不要把它当作每次启动的必要步骤。

## 端口与网络地址不是一回事
本地 MySQL 13306、Redis 16379、Kafka 19092、后端 18080、pprof 16060。容器内部使用 mysql:3306、redis:6379、kafka:9092；容器里的 localhost 指自己。Kafka advertised listeners 必须能被相应客户端访问，初次连上 bootstrap 还不代表拿到的 broker 地址可达。

## 顺着 run 看依赖图
读取 YAML → 创建日志 → 建立取消上下文 → MySQL 和 Redis Ping → Kafka 重试连接 → 注册 Lua → 构造 repository → 构造 service → 路由 → relay/consumer → HTTP 服务。配置未包含 atomic_pipeline 时，LoadConfig 默认开启新链路；显式 false 才走旧链路。

构造与启动分开，能让“配置无效”和“依赖暂时不可达”使用不同处理方式。Kafka 启动会退避等待；Ctrl+C 触发取消，不必等到下一次请求。

## health、ready 各说了多少
当前 /health 只报告进程路由存活；/ready 读取 Kafka 周期健康状态，不检查所有 MySQL、Redis 或消费者积压。它们都是有用的信号，但不可当作完整依赖可用性保证。

## 没启动时怎么定位
先查端口和 Docker 状态，再看所选配置，再读对应依赖错误，最后才怀疑业务。连接拒绝不是“SQL 写错了”；Kafka metadata 地址错误不是“用户密码错了”。保留首次失败日志，并记录你的诊断顺序。

## 配置从文件到运行对象
-c 选择 YAML → os.ExpandEnv 替换占位变量 → 解码 Config → 设置默认值 → run 将值传给连接池和业务服务。只有 YAML 写了 ${变量名}，环境变量才参与替换；运行时改文件需要重启。

```yaml
jwt:
  secret: "${GOIM_JWT_SECRET}"
  access_exp_hours: 2
  refresh_exp_days: 7
```

```powershell
# 先把所用 YAML 的 secret 改成上面的占位写法
$env:GOIM_JWT_SECRET = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'))
go run ./cmd/server -c configs/config.local.yaml
```

模板值不等于代码默认值。当前 GoIM 不严格拒绝未知字段，也没有完整必需项校验；后面的实验 05 会展示启动前校验。参数的单位和读取点在本章末尾速查。

## 先确认程序在哪运行
| 运行者 | MySQL / Redis / Kafka | 浏览器连接 |
| --- | --- | --- |
| Windows 宿主机后端 | localhost:13306 / localhost:16379 / localhost:19092 | localhost:18080 |
| Compose 网络内后端 | mysql:3306 / redis:6379 / kafka:9092 | 使用对外发布地址 |
| Vite 前端 | 不直连数据库 | API 18080/api/v1；WS 18080/ws；页面 5173 |

Compose 的 .env 用于 Compose 插值；后端不主动加载该文件，只扩展本进程环境变量。前端 VITE_ 值会交给浏览器，只能放公开地址。

## 只改一个参数，验证生效
将 server.port 从 18080 改为一个空闲端口（如 18082），重启后比较新旧 /health 地址，再恢复。保存配置路径、运行目录与响应结果。前端地址将在[贯通实战](#chapter19)一起验证。

## 附录：配置参数速查
首次阅读可跳过。遇到具体配置问题时，展开对应参数与源码读取点。

<details><summary>配置参数、默认值与当前限制</summary>

### HTTP、身份与上传参数逐项对照
下表中的“示例”来自本地模板；它不是自动默认值，缺省行为须看代码。单位、读取点和失败症状一起记，比背 YAML 名字更有用。

| 参数 | 本地示例/单位 | 实际读取点与影响 |
| --- | --- | --- |
| server.port | 18080，TCP 端口 | run 构造 HTTP Server；端口占用则监听失败 |
| server.pprof_port | 16060，端口 | 大于 0 时启动 127.0.0.1 上的性能端口；不是外部健康接口 |
| server.ws_path | /ws，路径 | setupRouter 注册 WS；前端 wsUrl 必须一起对齐 |
| server.upload_dir | ./uploads，目录 | 创建上传目录、构造 UploadHandler、静态 /uploads 读取同一项；相对后端工作目录 |
| jwt.secret | 开发 secret，字符串 | AuthService 与 JWT/WS 验签共用；改密钥后旧 token 通常失效 |
| jwt.access_exp_hours | 2，小时 | 访问 token 有效期；不是分钟或秒 |
| jwt.refresh_exp_days | 7，天 | 刷新 token 有效期；不自动实现撤销或轮换 |
| file.max_size_mb | 50，MB | UploadHandler 的文件大小约束 |
| file.allowed_exts | jpg、png、gif、mp4 | UploadHandler 接收的扩展名集合；仍需检查输入内容 |
| file.upload_dir | ./uploads，目录 | Config 中有这个字段，但当前启动上传与静态目录读 server.upload_dir；单改此项不会搬迁上传目录 |

参数改动实验：把 server.port 改为 18082，旧地址应连接失败、新 /health 应成功；前端 API、WS、静态文件地址也要一起改。只改 ws_path 而不改前端，会让 HTTP 能登录而 WS 升级 404。判断联通要逐个通道检查。

### MySQL、Redis 与 Kafka 参数逐项对照
| 参数 | 本地示例/缺省处理 | 运行影响与观察 |
| --- | --- | --- |
| mysql.host、mysql.port | localhost、13306 | 组成 TCP DSN；端口须对应宿主机映射 |
| mysql.user、mysql.password | goim、开发密码 | 数据库身份；拒绝访问与连接拒绝是不同错误 |
| mysql.db_name | goim | 目标库；迁移必须落在同一库 |
| redis.addr | localhost:16379 | Redis 客户端连接地址 |
| redis.password | 空字符串 | Redis AUTH；是否需要取决于服务配置 |
| redis.db | 0 | 逻辑 DB；写错 DB 可能能连接却看不到预期 key |
| kafka.brokers | localhost:19092 | bootstrap 地址；也支持逗号分隔字符串，metadata 地址仍要可达 |
| kafka.client_id | 模板 goim-local；空则 goim | 客户端日志与配额识别 |
| kafka.group_prefix | 模板 goim-local-；空则 goim- | 消费者组命名；改前缀会进入新的消费位置语义 |
| kafka.partitions | 未提供或非正数则 3 | 自动创建 Topic 时使用；改 YAML 不等于改变已有 Topic |
| kafka.replication_factor | 模板 1；非正数则 1 | 自动创建 Topic 的副本数；不能超过可用 broker 数 |
| kafka.start_offset | 空则 earliest | 首次没有已提交 offset 的起点；已有组不会因改此值直接重新消费 |
| kafka.commit_interval_ms | 0/未设置映射 1000ms；负数映射同步提交 | 按间隔提交有重放窗口；同步提交要承担等待 |

mysql.go 的最大连接数 100、空闲数 20、连接生命周期 5 分钟目前写死在代码；Redis 池沿用所锁定客户端的默认设置，不能发明 redis.pool_size 来期待自动生效。sql.DB 本身管理连接池；扩大池之前观察 DB.Stats 的 InUse、WaitCount、WaitDuration 及数据库承载能力。[Go 连接池说明](https://go.dev/doc/database/manage-connections)。

### 消息与 Feed 参数逐项对照
| 参数 | 默认/单位 | 实际行为 |
| --- | --- | --- |
| message.atomic_pipeline | 未发现该配置键时 true；显式 false 走旧链路 | 决定 MsgService、Relay 与消息消费者的装配，不是仅改变一条 Lua 调用 |
| moment.big_user_friend_threshold | 非正数则 500，好友数 | 好友数大于阈值采用拉取合并；不代表群聊成员上限 |
| moment.timeline_max_len | 非正数则 1000，条 | Feed 收件箱/寄件箱保留长度 |
| moment.like_persist_batch_size | 非正数则 200，事件条数 | 点赞归档攒批阈值 |
| moment.like_persist_flush_ms | 非正数则 500，毫秒 | 最大攒批等待；降低会增加写入频率 |
| moment.like_cache_ttl_hours | 非正数则 168，小时 | 点赞缓存 TTL；到期后的预热也需要验证 |

当前 atomic_pipeline 默认判断使用原始配置文本中的键字符串，值得改进为显式可选值与统一校验。

### 哪些中间件参数当前还不能在 YAML 改
当前没有已接通的 cors.allowed_origins、HTTP 请求超时、最大 JSON 请求体、rate_limit、request_id 等配置字段。CORS 由 setupRouter 的 CORS(nil) 调用决定；WS CheckOrigin 写在 upgrade.go；MySQL 池数字、Kafka Writer 的确认策略等也有代码内的配置。配置表只承诺真实读取点。

新增参数必须接通 Config、加载校验、启动装配和使用者。实验 05 的配置独立于 GoIM，不能直接混用。
</details>


### 动手任务
启动依赖与 GoIM；记录启动顺序、两个健康接口和真实端口；用错误 Redis 端口的独立配置复现启动失败后恢复。

### 验收标准
- /health 和 /ready 返回预期状态。
- 注册、登录与 friend/list 能完成一次真实调用。
- 能准确解释容器 localhost 和宿主机地址差别。
- 能说出 YAML、环境变量、默认值、启动装配的实际优先关系。
- 已实测一个参数变化；区分模板值、缺省值与目前写死的参数。

### 自检
为什么 Kafka 暂时不可用时程序还活着，而 /ready 可能不就绪？

存活和对外服务准备好是不同信号。客户端可重连、后台任务可等待，进程不一定应立即退出。当前 readiness 仅跟踪 Kafka，因此还要配合其他依赖和业务指标。

### 对照文件
- backend/configs/config.local.example.yaml
- backend/internal/config/config.go
- backend/cmd/server/main.go
- docker-compose.yaml
- backend/internal/infra/mysql.go
- backend/internal/infra/redis.go
- backend/internal/infra/kafka.go
- backend/internal/middleware/cors_middleware.go
- backend/internal/ws/upgrade.go
- frontend/src/config/env.ts

<a id="chapter19"></a>

## 08 · 贯通实战：配置、中间件与 API 到浏览器

先跑通独立的 HTTP 与浏览器实验，再用 GoIM 设置接口验证同一条调用路径。

## 先拿到一个完整切片
下载实验包，解压后进入 labs。实验 05 的 main.go、config.example.yaml、client.html 分别是后端、配置和前端。开发账户 lesson / lesson123，用户 ID 1；数据只在内存中，重启还原。/profile 是教学接口，GoIM 迁移部分使用真实 /settings。

## 先写出本次 API 的契约
| 方法与完整路径 | 输入与身份 | 成功 | 失败 |
| --- | --- | --- | --- |
| POST /api/v1/auth/login | username=lesson、password=lesson123 | 200；code=0；data.access_token、expires_in（秒） | 缺字段 400，错误密码 401，超大请求体 413 |
| GET /api/v1/profile | Bearer token；无请求体 | 200；data.user_id、nickname、notification_enabled | 未认证/过期/错误算法 401 |
| PUT /api/v1/profile | Bearer；nickname 与 notification_enabled 必须同时提供 | 200；返回更新后的完整资料 | 缺字段/空白或超长昵称 400；过大请求体 413 |

统一响应有 code、message、可选 data、request_id；HTTP 状态与 JSON 业务码都要检查。nickname 去首尾空白后为 2–20 个 Unicode 字符。notification_enabled=false 是合法更新，缺省该字段则 400。user_id 只取鉴权身份，不能靠请求体切换用户。

PUT 应明确完整替换的字段；PATCH 才按提供的字段局部更新。实验 DTO 使用 *bool：nil 是没传，指向 false 是明确关闭。项目 /settings 使用普通 bool，省略字段会得到 false，所以迁移时传完整字段。

## 配置读取、校验与启动
在 labs 打开终端 A：

```powershell
$env:NANOIM_JWT_SECRET = ([guid]::NewGuid().ToString('N') + [guid]::NewGuid().ToString('N'))
go run ./05-api -c ./05-api/config.example.yaml
```

默认监听 127.0.0.1:18101。端口必须 1–65535，read_header_timeout_ms 为正数，request_body_limit_bytes 至少 128，JWT secret 至少 32 字节，access_exp_minutes 为正数，allowed_origins 不为空。未知 YAML 字段也拒绝，失败发生在监听之前；这些属于实验实现，不是 GoIM 现有校验能力。

头部超时不等于业务执行超时。secret 只留在后端配置。

## 从中间件追到业务
main 组装 Recovery → requestTrace → CORS → 请求体上限 → 公共登录 / 受保护 JWT 路由 → Handler → Service → Store。Handler 翻译 JSON 与状态码，Service 检查昵称，Store 用锁保护内存。

requestTrace 在 c.Next 前生成请求 ID，处理结束后记录状态；响应 X-Request-ID 与日志一致。OPTIONS 由 CORS 返回 204，不进入 JWT。authenticate 限制 HS256、要求 exp，只使用验签身份。逐个找到这些函数，解释每层接收什么、返回什么。

## 先用命令行排除浏览器变量
另开终端 B，以下都只打你本机实验服务：

```powershell
$base = 'http://localhost:18101/api/v1'
$body = @{username='lesson'; password='lesson123'} | ConvertTo-Json
$login = Invoke-RestMethod -Method Post -Uri "$base/auth/login" -ContentType 'application/json' -Body $body
$headers = @{Authorization="Bearer $($login.data.access_token)"}
Invoke-RestMethod "$base/profile" -Headers $headers
$update = @{nickname='后端学习者'; notification_enabled=$false} | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "$base/profile" -Headers $headers -ContentType 'application/json' -Body $update
Invoke-RestMethod "$base/profile" -Headers $headers
```

预期读取→更新→再次读取一致，通知确实为 false。命令行成功尚未验证浏览器跨源限制；接下来做真实网页调用。

## 浏览器调用与 CORS
保持终端 A 的后端运行，在 labs 终端 C 启动前端静态页面：

```powershell
python -m http.server 18102 --bind 127.0.0.1 --directory ./05-api
```

浏览器打开 http://localhost:18102/client.html，按登录 → 获取资料 → 更新资料 → 再次获取走一遍。打开 Network 保留日志，查看 Request URL、Method、Origin、Authorization、响应 HTTP 状态、JSON 与 X-Request-ID。token 只留在页面内存，登录结果隐藏其正文。

注意使用 localhost，不是 127.0.0.1：这两个 Origin 不相同，配置只允许 http://localhost:18102。不要直接 file:// 打开 HTML，否则 Origin 语义不同；也不要从远端教程网站直接调用这个本地实验。终端中的静态服务器让来源固定、可检查。

Origin 由协议、主机、端口组成。JSON 或 Authorization 常触发 OPTIONS 预检；预检通常不带业务 token。CORS 决定浏览器能否读取跨源响应，不代替登录和资源授权。命令行请求不受浏览器的同样限制。

## 把五种失败拆开，逐个得到证据
1. 清除页面 token，再获取资料：正式请求应 401，错误体有请求 ID。
2. 昵称只留空白，或省略通知字段：400；关闭通知为 false 则 200。验证不会误把 false 当成缺省。
3. 命令行提交约 5000 字符昵称：在解析阶段超过默认 4096 字节请求体上限，应 413，而不是只报昵称长度。
4. 将后端端口改成 18103 并重启：旧地址无法连接；页面后端地址改为新端口后恢复。这是运行配置生效的证据。
5. 使用未允许的 Origin 打开页面：浏览器拿不到可读响应；查看预检或实际响应的跨源头。CORS 问题与 JWT 401、接口 404 分开记录。

把 JWT 有效期改成 1 分钟并重新登录，等待过期后验证 401。实验无 refresh；项目的 refresh 流程在下一节对照。恢复端口、白名单与有效期后结束实验。

## 将同一条路径迁移到 GoIM 前端
GoIM 的 frontend 使用 React/Vite、GoIMApiClient（fetch），配置在 src/config/env.ts。开发时默认 API=http://localhost:18080/api/v1、WS=ws://localhost:18080/ws、静态资源=http://localhost:18080；vite.config.ts 当前只设置 5173，没有配置开发代理。

先按第 7 章启动 GoIM 与依赖，再在 im/frontend 的终端运行（项目锁定依赖存在 package-lock.json）：

```powershell
npm ci
$env:VITE_API_BASE_URL = 'http://localhost:18080/api/v1'
$env:VITE_WS_URL = 'ws://localhost:18080/ws'
$env:VITE_STATIC_BASE_URL = 'http://localhost:18080'
npm run dev
```

这些值可写进你自己的 .env.local；修改后重启 Vite。生产构建时 VITE_ 值被写入客户端；生产默认使用页面同源。若有自定义构建变量覆盖默认值，也需重新构建，不会因服务器环境变量变化自动修改已经构建的 JS。

## 用项目已有设置接口完成一次闭环
先用独立练习用户名注册 GoIM，登录得到真实 access_token，再 GET /api/v1/settings。更新时按项目契约传完整三个字段，并把 mute_list 保持为有效 JSON 数组字符串；这是业务字段字符串，不是外层 JSON 数组。

```powershell
$base = 'http://localhost:18080/api/v1'
$demoUser = 'guide_' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$credentials = @{username=$demoUser; password='lesson123'} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$base/auth/register" -ContentType 'application/json' -Body $credentials
$login = Invoke-RestMethod -Method Post -Uri "$base/auth/login" -ContentType 'application/json' -Body $credentials
# 这里使用的是刚刚从 GoIM 返回的令牌
$headers = @{Authorization="Bearer $($login.data.access_token)"}
Invoke-RestMethod "$base/settings" -Headers $headers
$body = @{notification_enabled=$false; msg_preview_enabled=$true; mute_list='[]'} | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "$base/settings" -Headers $headers -ContentType 'application/json' -Body $body
Invoke-RestMethod "$base/settings" -Headers $headers
```

前端 createSettingsApi 的 get/update 对应这两个接口；UpdateSettings 返回成功消息，通用客户端可能返回 undefined data，不能强行读取不存在的属性。浏览器与命令行对照同一用户的读取结果，再按你的练习需求恢复设置。

setupRouter 将 /auth 放到公共 /api/v1 组，将 /settings 放到 JWT 组。RegisterRoutes 的相对路径加上组前缀才是完整 URL；实现 Handler 后仍要在启动入口挂载。

注册成功 201，登录成功 200；响应读取 data.user_id 与 data.access_token。GET /settings 返回 data，PUT 只返回成功消息。/health 是裸 JSON，不能送进要求 code=0 的业务客户端。DTO、frontend/goim-api-types.ts 和 Swagger 要对照真实响应核验，类型声明不检查网络字节。

## 进阶对照：客户端与跨源边界
完成上述闭环后，再按需查看刷新、WebSocket 和现有 CORS 的限制。

<details><summary>项目客户端、刷新和 WS</summary>

### 客户端如何附带 token、处理错误与刷新
GoIMApiClient 用 baseUrl+相对路径拼接 URL，getAccessToken 将 Bearer 放入 Header；默认 15 秒超时。判断 response.ok、JSON Content-Type 与 code=0 后才返回 body.data；网络/超时、HTTP 状态和业务码组成 ApiError。上传使用 FormData，不手写 multipart 的 Content-Type boundary。

lib/api.ts 区分公共 authApi 与受保护 apiClient。401 时 refreshAccessToken 使用共享 Promise 合并并发刷新；成功后原请求最多重试一次，刷新失败则清除会话。这避免无限刷新循环；仍要考虑非幂等写入能否安全重试，不该为任意 5xx 盲目重发。

联调时检查真实 JWT 过期状态、缓存中的登录状态和 previewMode。前端预览数据能显示页面，不能证明后台 API 已连接；一条 Network 请求及真实返回才能提供调用证据。

### HTTP 通了，还要单独验证 WS 与静态资源
HTTP API、WS 和头像 URL 使用三个不同配置。实时客户端从 env.wsUrl 构造带 token 的握手 URL；浏览器原生 WebSocket 无普通 fetch 的 Header 接口，当前项目用查询 token，因此日志与代理应避免记录完整查询串。HTTPS 页面应使用 WSS，不能仍指向 ws:// 明文地址。

在 Network 的 WS 类别看 101 升级与后续帧；检查 /uploads 的头像是否从正确后端读取。反向代理需要转发升级头并设置适当连接超时，HTTP CORS 配置不能替代 WS Origin 策略。Vite 代理可以作为开发方案，但要真实添加配置并同时处理 /api、/ws 与上传路径，不能假设仓库已替你配好。

### GoIM 与实验的 CORS 差别
GoIM CORS(nil) 返回 Allow-Origin: * 和 Allow-Credentials: true；当前前端使用 Bearer，未设置 credentials:'include'。若改成跨源 Cookie 会话，需要精确 Origin 和相应凭据策略。GoIM 白名单分支对不允许的 Origin 不加跨源头，但仍执行后续处理；实验 05 则直接拒绝该来源。浏览器看不到响应，不代表后端没有执行。

WebSocket 的 CheckOrigin 独立于 HTTP CORS；GoIM 当前允许所有来源，生产需要单独限定。[浏览器 CORS 规则](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS)。
</details>

## 保存一次联通验收
记录登录 → GET → PUT → GET 一致，以及无 token 401、缺字段 400、超限 413、预检 204 和请求 ID。再记录 GoIM 设置闭环。独立实验与真实项目分别保存结果。

连接拒绝查地址和进程；404 查前缀与路由；浏览器 network error 查 Origin 和预检；401 查 token；400 查 DTO；500 查请求 ID 对应日志。定位后重放同一最小请求。

### 动手任务
跑通实验 05 的命令行与浏览器闭环，完成五种失败实验；再在独立 GoIM 用户上验证设置 API 与实际前端调用。

### 验收标准
- 实验 GET→PUT→GET 状态一致，false 被正确保存，缺省通知字段被拒绝。
- 记录预检、401、400、413 和参数变更生效的证据。
- 响应 X-Request-ID 可在后端日志找到；秘密与 token 不出现在记录里。
- 能追踪真实 GoIM 的 env→fetch 客户端→路由→中间件→Handler→service，并区分实验通过与真实依赖联通。

### 自检
命令行 PUT 成功，浏览器却只显示“网络失败”，应该先改 Handler 吗？

先比较 URL、协议与 Origin，再看 OPTIONS 和正式请求的跨源头、状态及请求 ID。浏览器可能不能读取响应，但后端可能已经执行，尤其现有 CORS 白名单分支仍继续处理。确定失败边界以后再修对应层，不要先重复提交有副作用的请求。

### 对照文件
- labs/05-api/main.go
- labs/05-api/config.example.yaml
- labs/05-api/client.html
- backend/internal/api/settings_handler.go
- backend/internal/api/auth_handler.go
- frontend/src/config/env.ts
- frontend/src/api/client.ts
- frontend/src/lib/api.ts
- frontend/src/api/settings.ts
- frontend/vite.config.ts
- frontend/goim-api-types.ts
- frontend/src/realtime/socket.ts
- backend/cmd/server/main.go

<a id="chapter8"></a>

## 09 · 好友与群组：写业务规则，别只写 CRUD

联系人、角色和状态转换，才是聊天系统里真正的权限模型。

## 从状态变化理解关系
好友申请不是一条任意可更新的记录。发送人创建 pending，只有接收人能接受或拒绝；接受后创建双向关系。拉黑影响能否发送，删除好友影响缓存和后续行为。把“谁可以让什么从什么变成什么”写出来，比只罗列表字段更接近业务。

群组同样有成员、管理员、群主。踢成员、改角色、转群主不能只判断“已登录”。服务端必须判断操作者角色、目标角色、群归属以及操作后的不变量。

## 动手：通过公开 API 建立好友
在 18080 端口注册两个不同的新用户，记录真实 user_id 和各自 access_token。以下变量来自前面登录结果；aliceID/bobID 不能凭空填 1、2。

```powershell
$api = 'http://localhost:18080/api/v1'
$a = @{Authorization="Bearer $aliceToken"}
$b = @{Authorization="Bearer $bobToken"}
$payload = @{to_user_id=$bobID; message='一起做实验'} | ConvertTo-Json
$request = Invoke-RestMethod -Method Post -Uri "$api/friend/request" -Headers $a -ContentType 'application/json' -Body $payload
$accept = @{request_id=$request.data.request_id} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "$api/friend/accept" -Headers $b -ContentType 'application/json' -Body $accept
Invoke-RestMethod -Uri "$api/friend/list" -Headers $a
```

保存返回值。再让 Alice 自己接受这条申请，预期拒绝；再接受已处理申请，预期不能再次创造关系。注册用户名请加时间后缀，避免已有数据影响实验。

## 数据库事实与 Redis 投影
好友关系持久化在 MySQL，发送热路径在 Redis Lua 检查 friend 键、blacklist 集合。关系变更需要更新或失效缓存。缓存冷启动不是业务上的“从未是好友”，但实现如果无法恢复就会误拒绝。当前群消息路径有成员缓存修复，私聊缓存恢复的对称性值得检查。

这说明缓存常常是业务读模型：key 的缺失含义、过期策略、重建来源、失败策略要写进设计。权限缓存失效时继续放行和全部拒绝都是产品与风险取舍，不能无意识选择。

## 实现作业：状态化权限
在自己的最小服务实现 FriendRequest 和 Accept 两个业务方法。用事务写申请状态和双向关系，再更新 Redis。只完成一个纵向切片，再拓展拒绝、删除、拉黑和群组管理。

## 并发边界
两次接受申请、同时转让群主、转让期间旧群主踢人，都需要用数据库条件更新、事务锁或其他明确机制保护。用户身份来源可信只是第一关，资源状态并发变化仍然存在。

### 动手任务
建立双向好友后，设计三条越权请求；在自己的服务实现接受申请的事务切片，再做群主转让的状态图。

### 验收标准
- 能从双方 friend/list 看见对方。
- 自发自收、越权接受、重复接受有明确结果。
- 能说明 MySQL 成功而 Redis 更新失败如何发现与修复。

### 自检
Redis 没有某人的群成员缓存，可以直接认定他不是成员吗？

要区分“缓存冷”与“确实不是”。项目新链路遇到 NotMember 可尝试从 MySQL 载入成员后重试，但仍须再次校验，不能因为缓存缺失就跳过权限。

### 对照文件
- backend/internal/service/friend_service.go
- backend/internal/service/group_service.go
- backend/internal/api/friend_handler.go
- backend/internal/api/group_handler.go
- backend/scripts/migrations/003_create_groups.sql

<a id="chapter9"></a>

## 10 · WebSocket：让服务主动说话

从一个回声连接，走到鉴权、单设备登录和读写协程。

## 动手：先看见一条长连接
停止之前的实验，运行实验 04，打开它自带的页面：

```powershell
go run ./04-ws
# 浏览器访问 http://localhost:18100
```

输入“你好 GoIM”并发送，页面出现 type=msg 的 JSON。刷新浏览器，连接会重新建立。这个实验是本机、未鉴权、没有数据库的回声服务，每条消息只回给自己，目的是把传输层单独看清楚。

## HTTP 与 WebSocket 的分工
注册、资料和列表仍适合 HTTP。聊天和在线通知需要服务主动推送，WebSocket 先通过 HTTP 握手，再持续传输帧。连接不是会话历史，断了以后要重新鉴权、恢复和同步。

## 接入项目鉴权与协议
真实 GoIM 的 /ws?token=... 在升级前调用 ParseToken，UserID 来自已验签 Claims。当前开发 CheckOrigin 接受所有来源，生产需要按站点配置允许来源。查询 token 可能出现在代理访问日志，部署时要处理脱敏；它是项目现状，不是所有系统的推荐方式。

## 一个读者，一个数据写者
Gorilla 要求同一连接最多一个并发读者和一个并发写者。项目 ReadPump 读取并交给分发器；WritePump 是数据写入拥有者，业务通过 SendCh 投递。WriteControl 的并发使用有单独规则，不能简单说“所有写方法都能并发”。

通道容量 256 是有限缓冲。慢客户端会挤满它；代码采用非阻塞投递时可能只记录并跳过实时推送，恢复要依赖同步。调大缓冲不能解决长期速度不匹配，反而增加内存与延迟。

## 生命周期是这章最难的地方
读限制当前为 4096 字节；服务层内容上限更大，实际传输仍先受 ReadPump 限制。ping 每 30 秒、pong 超时 60 秒，Redis 在线键 60 秒并每 20 秒续期。应用层 type=ping 与 WebSocket 控制帧 ping 是两个概念。

单设备策略向旧连接投递 kick，然后关闭。旧连接的清理不能误删新连接的在线标记；当前按 userID 清理键的实现有竞态审查空间。要用连接 generation/ownership 比较后删除，不能只看到“有踢人功能”就认定并发正确。

## 实现作业
把回声实验分成 reader 和 writer，两者用容量有限的队列连接；再加入实验 03 的 token 验证。测试 10 个并发发送、断开重连、慢接收者和同账号再次登录。不要同时让 Handler、consumer 和心跳 goroutine 都写数据帧。

### 动手任务
完成回声实验；加入 JWT 验证和一读一写结构；在 GoIM 同账号开两条连接观察 kick。

### 验收标准
- 能看到真实帧并说明握手与数据帧的区别。
- 无 token 的项目握手被拒绝。
- 能解释关闭、超时、慢客户端与旧连接清理的责任。

### 自检
业务 goroutine 直接调用 connection.WriteMessage 会带来什么问题？

多个业务与心跳可能同时写，违反连接并发约束，也使发送顺序和关闭竞态难管理。把数据写入集中到一个 writer，其他代码只排队，明确满队列处理策略。

### 对照文件
- labs/04-ws/main.go
- backend/internal/ws/upgrade.go
- backend/internal/ws/handler.go
- backend/internal/conn/client.go
- backend/internal/conn/manager.go

<a id="chapter10"></a>

## 11 · Redis：从缓存走向消息读模型

收件箱、会话列表和已读状态，要按访问方式选择数据结构。

## 为什么 SQL 能保存，还需要 Redis
高频操作包括：检查好友、获取在线状态、消息去重、写收件箱、更新会话摘要和未读数。逐条跨网络往返会增加延迟，而且“先查再写”存在并发窗口。Redis 提供低延迟数据结构，但同时带来恢复、容量和持久化设计。

当前默认消息热路径把 Redis 当作可同步的消息读模型，MySQL 是异步归档。因此清空 Redis 不是无害的“清缓存”：尚未移交 Kafka 的消息和同步状态可能仍在其中。

## 每种结构都对应一个问题
- String：在线 TTL、去重标记、群最新序号。
- Set：成员集合、黑名单、点赞用户，回答“是否属于”。
- Hash：按消息 ID 取正文、按会话 ID 取摘要、按群取已读位置。
- ZSet：按时间排消息与会话；score 是排序值，不负责保存所有字段。
- Stream：需要处理和确认的归档事件，有消费者组与 pending。

## order + data 为什么比整段 JSON 排序清楚
inbox_order:{uid} 存顺序，inbox_data:{uid} 按 ID 存数据；conv_order 与 conv_meta 也是同样思路。摘要变了，更新 Hash 即可。把会话 JSON 当 ZSet member 时，文本变化会产生另一个 member，容易保留重复会话。

项目迁移期间同时写旧 inbox/outbox 和新 order/data，便于兼容回滚。双写增加容量与维护成本，应有退出计划。保存范围由两种清理共同决定：

- 新链路 Lua 在发送时将消息盒裁剪到最多 2000 条。
- 后台清理在启动时及每小时执行：收件箱最多 1000 条，群发件箱最多 500 条，并清理超过 3 天的记录；新 order/data 同步清理。

当前 syncReq 从 Redis 读取，没有自动从 MySQL 补齐被清理历史的路径。离线补同步受上述范围限制，不能把“已归档”理解为“重连一定能补回全部历史”。

## 动手：独立 key 上练一次
在 Docker 运行后，使用明显的 lesson: 前缀，避免改项目业务键。

```powershell
docker compose exec redis redis-cli SET lesson:online:alice 1 EX 60
docker compose exec redis redis-cli TTL lesson:online:alice
docker compose exec redis redis-cli ZADD lesson:inbox_order 1000 1 1001 2
docker compose exec redis redis-cli HSET lesson:inbox_data 1 hello 2 world
docker compose exec redis redis-cli ZRANGE lesson:inbox_order 0 -1 WITHSCORES
docker compose exec redis redis-cli HGET lesson:inbox_data 2
```

预期 TTL 递减、ZSet 给出顺序、Hash 返回 world。清理仅用 DEL lesson:... 具体实验键。不要使用 FLUSHDB、KEYS * 或删开发数据卷来“恢复环境”。

## Pipeline 不是事务
Pipeline 把命令一起发送，减少网络往返，不提供失败自动回滚。MULTI/EXEC、Lua 和数据库事务各有语义差异。下一章让权限检查、去重和消息读模型写入在同一个 Lua 执行期间完成。

## 还要回答容量与持久化
TTL 是生命周期，不是可靠性保证；AOF/RDB 是恢复策略，不是无限耐久承诺。开发 compose 没有显式开启 appendonly，教程不能宣称已配置 AOF 每秒刷盘。淘汰策略、故障恢复、内存上限、备份都需要部署时验证。

### 动手任务
用独立实验键实现 order/data 读取；列出 GoIM 中哪些 Redis 数据可重建，哪些丢失会影响尚未确认的消息。

### 验收标准
- 能按顺序取 ID 后读取正文。
- 能说明 TTL、裁剪和持久化分别解决什么问题。
- 能区分 Pipeline 减少往返与 Lua 原子执行。

### 自检
给在线 key 设置 60 秒 TTL 后，为什么还需要续期？

用户的连接可能持续几个小时。TTL 不续期会把仍在线的人变成离线；只续期也不够，断连后应按连接所有权清理，异常退出则由 TTL 兜底。

### 对照文件
- backend/internal/repository/redis_repo.go
- backend/internal/repository/message_pipeline.go
- backend/internal/redis/lua_private_msg.go
- backend/internal/redis/lua_message_write.go
- backend/internal/infra/cleanup.go

<a id="chapter11"></a>

## 12 · Lua + Stream：把“一半成功”变成可恢复待办

这一章是默认消息链路的核心：保存实时状态的同时，留下待归档事件。

## 先看一种坏顺序
业务先写 Redis 收件箱，再发 Kafka；如果第二步失败，消息已经可见却没有归档事件。反过来先发 Kafka，Redis 失败，归档可能完成而客户端暂时看不到。两个存储并不会因为放进一个 Go 函数就共享事务。

## 同一存储里同时写状态和待办
GoIM 新链路把权限检查、去重、生成 ID、写收件箱/发件箱、摘要与未读、XADD 待归档事件放进一个 Redis Lua 调用。这借用了 outbox 思路：可见状态的修改和“以后必须做的事”在同一存储里留下记录，再由 relay 转发。

```text
校验好友/群成员 → 检查重复 → 生成服务端 ID
    → 写 order/data/摘要/未读 → XADD message_persist_stream
```

它避免普通多次客户端往返之间的并发插入，但可靠性仍取决于脚本写入前置校验、数据类型正确和 Redis 持久化配置。

## Lua 原子执行，不代表错误回滚
脚本运行期间其他命令不能插进来；脚本中途报错，已经完成的写入不会自动撤销。要在写入前检查可能出错的类型、参数和不变量，控制脚本长度。当前脚本中的多 key 数据类型错误会是一个值得验证的失败场景。

Redis Cluster 还要求参与脚本的键遵循分片约束；当前方案使用多个动态键，不能直接宣称可无改动迁移 Redis Cluster。更大规模可能需要按会话分片、不同数据布局或变更事务边界。

## 两个 ID 回答两种问题
请求的 msgId 解码到 SendMessage.ClientMsgID，表示同一个客户端逻辑发送；服务生成 serverMsgId 表示后端消息。当前 Redis 去重键按发送者与客户端 ID，TTL=300 秒。协议代码字段名与 Go 字段名不一致，复制 JSON 时尤其容易错。

服务端 ID 由 Redis TIME 毫秒 * 1000 + 毫秒内序号生成，序号超过 999 拒绝。它不是 Snowflake，也不是跨任意 Redis 实例通用的分布式 ID；时间回拨、吞吐上限、分片方案都是要考虑的边界。

## 动手：重复同一逻辑消息
先让 Alice/Bob 成为好友并登录 WS。使用第 14 章的真实浏览器实验台，连续发送两次相同 msgId，观察 ACK/错误和数据库记录。再换一个 msgId 重发同样内容：内容相同并不等于逻辑消息相同。

```powershell
# 工作目录：im/backend；Redis 已运行，集成测试使用 DB 1
$env:GOIM_TEST_REDIS_ADDR = 'localhost:16379'
go test ./internal/redis -run 'TestAtomic' -v -count=1
```

若显示 SKIP，说明未执行真实 Redis 验证。Lua 返回中保留了旧消息 ID 等信息，但当前 service 对私聊、群聊的重复结果都发送重复错误；它并没有把旧结果重新组成 ACK。你可以将“重试返回原逻辑结果”作为改进作业，同时明确首次待办尚未移交 Kafka 时的成功语义。

## 幂等的边界
幂等必须定义“哪一种重复、保留多久、冲突内容怎么办”。5 分钟 Redis 去重不是永久保障。迁移 011 给 client_msg_id 加的是普通索引，不是唯一索引；归档幂等主要围绕服务端主键 ID 和内容一致性检查。客户端跨窗口重试仍需产品和存储层契约。

### 动手任务
追踪 Lua 的返回码到 service 的错误或 ACK；重复同一客户端 ID，再用新 ID 发相同内容，记录差别。

### 验收标准
- 重复请求不在当前去重窗口生成新 ID。
- 能指出 XADD 与消息读模型写入的共同边界。
- 能说明 Lua 无回滚、去重 TTL 和多 key 分片限制。

### 自检
Lua 成功后进程崩溃，为什么还能继续归档？

待办已进入 Redis Stream，不依赖该 goroutine 的内存。relay 重启可接管 pending 或读取新事件。但如果 Redis 自身丢失未持久化数据，这个保证也会失效。

### 对照文件
- backend/internal/redis/lua_message_write.go
- backend/internal/redis/lua_message_write_test.go
- backend/internal/repository/message_pipeline.go
- backend/internal/service/msg_service.go

<a id="chapter12"></a>

## 13 · Kafka 与 Relay：可靠转发需要移交顺序

为什么必须先确认队列，再移除待办？为什么重复几乎不可避免？

## 一个 relay，只做一件移交工作
relay 从 Stream 消费者组取事件，向 Kafka message_persist 投递，得到确认后 ACK/删除 Stream 记录，再推 serverAck 和实时消息。它不归档 MySQL，归档消费者是下一阶段。

```text
Redis Stream → Relay → Kafka 确认
                         ↓
                  ACK/删除 Stream → WebSocket 推送
```

## 三个崩溃点决定顺序
投递前崩溃：Stream 记录仍在；投递后确认前崩溃：不知道 Kafka 是否收到，应允许重试；确认后删除前崩溃：可能重复投递。让归档消费者幂等，就能容忍后两种重复。

若先删除待办再投递，中间崩溃就永久丢失。宁可产生可识别的重复，也不要制造无记录的丢失窗口。不要因此说已经达成端到端 exactly-once，这里仍是可重复投递的链路。

## pending 与接管
XREADGROUP 的 > 读取新事件；已领取未确认的事件在 pending 中。当前 relay 每轮先 XAUTOCLAIM 接管闲置 10 秒以上的 pending，再读新事件，最多一批 32 条。读到了不代表处理完了，XACK 也不会替你实现业务写入。

失败事件超过 30 分钟移到 relay_failed_events，持续重放，达到 1000 条记录告警。这个待办仍在 Redis，和 Stream 使用相同恢复条件。重放当前只负责归档投递，不重走所有实时推送，客户端补同步仍然重要。

## Kafka 的几个核心词
Topic 是事件类别；Partition 是有序日志单元；Key 参与决定分区；消费者组让多个实例分担分区；Offset 是处理位置。相同 Key 的事件会路由到同分区，但失败隔离、接管和 retry 回流仍可能改变应用观察顺序；群消息顺序仍要由 groupSeq 解释。

GoIM Writer 用 Hash、RequireAll、同步写、5ms BatchTimeout。acks=all 表示按当前 ISR 条件确认，不等于每条数据立即 fsync，更不保证单副本开发集群在机器毁坏后还能恢复。副本、min.insync.replicas 和故障域要配合设计。

## 这里必须纠正文档直觉
当前 Reader 默认 CommitInterval=1 秒。业务调用 CommitMessages 先登记，后台按间隔提交；commit_interval_ms 设置负数会映射到客户端同步提交模式。进程崩溃可能重放未提交区间。README 的“同步提交”描述不能覆盖实际代码，教程以实现为准。

## 动手：观察待办
在自己的实验数据上短暂停止 Kafka，然后发送一条消息。查看 Stream XLEN、XPENDING 与 relay 日志；恢复 Kafka 后查看积压是否回落。只在隔离开发环境做故障实验，按下一章的顺序恢复，不删除数据。

```powershell
# 工作目录：im
docker compose exec redis redis-cli XLEN message_persist_stream
docker compose exec redis redis-cli XPENDING message_persist_stream goim-message-relay
```

没有消费者组时 XPENDING 会报 NOGROUP，先确认应用是否已经启动新链路。正常时 relay 很快删掉已确认事件，Stream 可能为空，空并不代表没有发送过。

### 动手任务
用演示器走一遍 Kafka 不可用与确认后崩溃；在真实开发环境验证 pending 增长和恢复后下降。

### 验收标准
- 能解释每一个确认动作确认了哪一步。
- Kafka 未确认前，待办仍可找到。
- 能解释为何同一事件可能投递两次，以及消费端如何承接。

### 自检
Kafka 确认后、Stream 删除前崩溃，重新投递是否是 bug？

它是这条可靠移交链路必须容忍的结果。只有最终业务出现重复记录才说明幂等边界失败；无条件追求不重复投递，反而容易制造丢失窗口。

### 对照文件
- backend/internal/relay/message_relay.go
- backend/internal/repository/message_pipeline.go
- backend/internal/repository/mq_repo.go
- backend/internal/infra/kafka.go
- backend/internal/consumer/kafka_runner.go

<a id="chapter13"></a>

## 14 · 完整私聊：把客户端、回执和归档接起来

第一次沿完整链路发一条真实消息，逐个验证每个承诺。

## 本章的全链路验收
准备互为好友的 Alice 和 Bob，各自的 ID 与 access_token 来自第 9 章。下面浏览器控制台实验应在 GoIM 本地页面使用，或打开[真实 WebSocket 实验台](ws-client.html)；某些网页的 CSP 会阻止连接本机 WebSocket。在线 HTTPS 教程可能不能连接本机 ws://，可从本地预览打开实验台，或下载页面在本机使用。

```javascript
const ws = new WebSocket('ws://localhost:18080/ws?token=' + encodeURIComponent('粘贴自己的 access_token'));
ws.onmessage = event => console.log(JSON.parse(event.data));
ws.onopen = () => console.log('已连接，可以发送');
// 等待已连接后执行；toId 用 Bob 的真实 ID。
ws.send(JSON.stringify({type:'msg', data:{
  msgId:'alice-lesson-001', convType:1, toId:2,
  msgType:1, content:'你好 Bob', timestamp:Date.now()
}}));
```

上面的 2 是占位示例，务必替换。实验台允许直接输入目标 ID，并在连接 readyState 为 OPEN 时发送。用两个不同用户、不同窗口连接；同一个用户的新连接会踢掉旧连接。

## 预期观察顺序
Alice 收到 serverAck，包含 clientMsgId、serverMsgId、timestamp；Bob 收到 msg；MySQL private_messages 稍后出现该 ID。查看服务日志与数据，不要只看 UI 中是否出现聊天气泡。

```sql
SELECT id, client_msg_id, sender_id, receiver_id, content
FROM private_messages
WHERE sender_id = 你的AliceID AND receiver_id = 你的BobID
ORDER BY id DESC LIMIT 10;
```

SQL 中中文占位必须替换为整数。在 MySQL 客户端执行，并记录 ACK 的 serverMsgId 对应哪条记录。

## 三种回执的真实能力
serverAck 是服务处理/队列确认后的发送回执；deliverAck 表示客户端报告收到，但当前 HandleDeliverAck 主要记录日志，没有完整持久化送达状态与转发；readAck 会更新会话已读状态。不要把协议定义了一个名字等同于完整实现。

## 归档消费者要容忍重放
MessagePersistConsumer 解析不可变事件，按会话类型插入 MySQL。主键重复要比较内容：相同事件重放可以视为成功，同 ID 不同内容应报冲突。项目仓储集成测试专门验证这件事。吞掉全部 duplicate 错误会把冲突伪装成成功。

## 不可用时怎样活下来
归档失败先写 message_persist_retry，header 带下次时间和次数；5 秒后回流主 Topic，最多 5 次重试后进入 message_persist_dlq。转投成功才允许提交原 offset，失败则原地退避。不合法事件可丢弃并记录；它们和临时 MySQL 故障不同。

retry Topic 的等待由消费者承担，Kafka 本身不是自带按条目定时触发的延迟队列。重试、DLQ 需要查看和人工重放工具；当前没有完整管理界面，死信不会自动凭空修好。

## 动手故障实验
先确认 Kafka/Redis 正常，再停 MySQL，发送一条新 msgId，观察仍可能收到 ACK。恢复 MySQL 后查询归档；若已进入 DLQ，记录事件和修复策略，不宣称恢复依赖后它一定自动回放。此实验只用于开发环境。

```powershell
# 工作目录：im；仅在你的隔离开发实例
docker compose stop mysql
# 从实验台发送一条消息；观察日志与 retry/DLQ
docker compose start mysql
```

如果应用还没有启动，MySQL 启动 Ping 会失败；本实验是在已经运行的进程上制造暂时故障。

### 动手任务
完成双用户私聊；为一个 serverMsgId 同时保存 Alice ACK、Bob 消息、Redis 记录与 MySQL 归档证据。

### 验收标准
- senderId 来自身份，无法伪造。
- serverAck 与归档记录能按服务端 ID 关联。
- 知道 deliverAck 当前的实现限度，以及 retry/DLQ 的恢复条件。

### 自检
为什么数据库已归档，Bob 仍可能没有实时收到？

Bob 可能离线、发送缓冲满、网络断开，或者 relay 在删除事件后尚未推送就崩溃。归档与展示是不同状态；需要补同步和客户端去重，而不是把数据库成功当作送达证明。

### 对照文件
- backend/internal/service/msg_service.go
- backend/internal/model/message.go
- backend/internal/relay/message_relay.go
- backend/internal/consumer/message_persist_consumer.go
- backend/internal/repository/mysql_repo.go
- backend/internal/consumer/kafka_runner.go

<a id="chapter14"></a>

## 15 · 离线同步、游标和群聊水位线

当重连、同毫秒消息和并发已读一起发生，时间戳已经不够用了。

## 网络断开是常态
WebSocket 只是实时通道，收不到就补同步。当前 SyncReq 提供 lastSyncTime、lastSyncMsgId、batchSize；SyncBatch 提供下一页游标和 hasMore。客户端需要合并、去重、处理分批，不能只把实时帧 append 到列表。

```json
{"type":"syncReq","data":{"lastSyncTime":0,"lastSyncMsgId":0,"batchSize":50}}
```

先让 Bob 断开，Alice 发送三条不同 ID 的消息；Bob 重连后发 syncReq。预期 syncBatch 中有待同步消息，并有 convSync 会话状态。查询数据库归档不能代替观察当前同步路径。

## 为什么时间戳游标会漏消息
若 A、B 同为 timestamp=1000，第一页只返回 A；下一页只查 timestamp>1000 就漏 B。复合游标 (timestamp,msgId) 把同毫秒消息明确排序：时间更大，或时间相同且 ID 更大。

```text
timestamp > cursorTime
OR (timestamp = cursorTime AND msgId > cursorID)
```

游标记录“最后成功处理到的位置”，应在客户端消息持久化/展示策略之后更新，不能仅在接收到包时草率推进。服务端 ID 使用 int64，前端需确认精度；当前毫秒*1000 的 ID 暂在 JS 安全整数内，但系统演进时字符串 ID 往往更稳。

## 群聊避免每个成员累加未读
当前群读模型写一份 outbox；群最新序号 group_seq 单调推进，每个成员在 group_read_pos 保存已读位置。未读通常按 max(0, latestSeq-readPos) 计算。读到 40 后收到 41，未读为 1；迟到的 readAck=35 不应把游标拉回。

```json
{"type":"readAck","data":{"convId":"g_你的群ID","lastReadGroupSeq":40}}
```

加入群时游标要按产品选择初始化，否则新成员可能看到大量历史未读。撤回、过滤不可见消息、退群再入群都会让“序号差”与“可见未读条数”产生差异，规则必须明确。

## 存储省了，扇出还在
群消息只保存一份正文，但当前 Lua 仍遍历成员更新会话摘要，并把收件成员放入事件；relay 仍逐个尝试实时推送。水位线减少逐成员未读累加，并没有把所有群发送成本降成 O(1)。大群规模会放大 Lua 阻塞与事件大小。

## 已读的并发语义
当前 SetGroupReadPos 直接 HSET，尚未原子地取 max，也未限制显式上报的游标到最新 seq。迟到 ACK 可能让位置后退，过大值可能掩盖未读。这是需要亲自实现的改进任务，页面演示器展示的是目标规则，不能当作当前实现。

改进时先检查成员资格，再在同一 Lua 操作中读取 old 与 latest，写 max(old, min(reported, latest))；不要在 Go 中先读后写，因为并发 ACK 仍会互相覆盖。若客户端没带 lastReadGroupSeq 或传 0，当前会降级为“标到最新”，可能把尚未展示的新消息一起标读。新客户端应发送实际展示到的序号。

```powershell
# 工作目录：im/backend
go test ./internal/service -run 'TestSyncReq_CompositeCursorSameTimestamp|TestReadAck_Group' -v -count=1
go test ./internal/repository -run 'TestGetUnreadMapWatermark' -v -count=1
```

第二条需要 Redis，检查 SKIP。用页面水位线演示器复现迟到 readAck，再设计测试验证自己的实现。

### 动手任务
完成离线三条消息补同步；按两页构造同毫秒消息；测试群最新 42、已读 40、迟到 ACK 35 的状态。

### 验收标准
- 同毫秒分页无遗漏、重连合并无重复。
- 迟到已读游标不会后退。
- 能说出群正文共享、摘要扇出、实时扇出的不同成本。

### 自检
群最新序号 42，已读 40，再收到已读 35，未读应是多少？

仍为 2。已读位置取 max(old,new)，同时不能超过群最新序号；若直接覆盖为 35，会凭空增加未读。

### 对照文件
- backend/internal/service/msg_service.go
- backend/internal/repository/redis_repo.go
- backend/internal/redis/lua_inbox_mark_read.go
- backend/internal/model/message.go
- backend/internal/repository/redis_repo_integration_test.go

<a id="chapter15"></a>

## 16 · 朋友圈与点赞：把消息经验迁移到另一种业务

同样的 MySQL、Redis、Kafka，换了访问模式就需要不同策略。

## 写扩散与读扩散
普通作者发布动态后，消费者把动态 ID 写进好友时间线，阅读便宜，但发布成本随好友数增长。好友数超过阈值的大作者主要写自己的 outbox，好友读 feed 时拉取合并：发布便宜，阅读与合并更贵。

项目默认 big_user_friend_threshold=500、timeline_max_len=1000。这个阈值是当前实现选择，不是普遍标准；应该根据好友分布、活跃读写比、延迟和资源测量调整。

## 先建事实，再扩散投影
动态本体存 MySQL，Kafka moment_push 执行时间线扩散。读侧必须重新处理权限，例如可见范围、好友变化、删除。Feed 有一条 ID 并不意味着当前用户永远有权限查看本体。

这里的状态写入与 Kafka 投递不自动获得消息链路 Lua+Stream 的同存储保证。发布后投递失败、拉黑后旧投影存在、删除后延迟事件回流，都应设计补偿或重建。这是架构经验的迁移，不能看到用了相同工具就复制同一承诺。

## 点赞是集合语义
一个用户对一条动态只能有一个点赞状态，计数随状态切换。简单 INCR 会让重试重复加一；Lua 同时操作用户集合和计数，能保持单个 Redis 执行边界内的状态一致。

点赞缓存默认 TTL=168 小时，消费者默认攒到 200 条或 500ms flush。攒批减少 SQL 往返，也增加可见与归档之间的时间窗口。当前 LikePersistConsumer 使用 FetchMessage，整批落库成功后才 CommitMessages；不是一进内存批就提交。按 (momentID,userID) 折叠最后动作，再批量写库；重放仍要求业务幂等。这个“先落库再确认”的顺序值得复用，消费提交默认仍可能按间隔进行。

## 冷缓存预热是一项业务协议
缓存 key 不存在时，MySQL 可作为点赞事实来源预热；但预热与并发点赞要防止覆盖新状态。缓存里有旧 count、集合为空、warm marker 存在等组合都应该测试。

## 实现作业：从拉取开始
先实现发布动态和查询自己动态，使用 MySQL 事务与稳定分页。再加好友时间线的写扩散，最后加大作者读扩散。通过同一套业务场景对比代码和成本，比一上来同时写三种路径更容易理解。

```powershell
# 工作目录：im/backend
go test ./internal/service -run 'TestGetFeed|TestLikeMoment|TestUnlikeMoment' -v -count=1
go test ./internal/consumer -run 'TestMoment|TestLike' -v -count=1
```

正则可能匹配多个相关用例，以实际输出的测试名为准。只跑 mock 通过，不代表真实 Redis 预热或 Kafka 扩散已验证。

## 你应该带走的经验
读多写少偏向预计算，写多或高扇出偏向读时合并，但权限、删除与恢复都可能改变最优选择。Feed 是读模型，不能把投影里的条目当成唯一事实。

### 动手任务
实现最小动态发布与查询，再写一张写扩散/读扩散成本对比；模拟重复点赞与取消点赞，核对集合和计数。

### 验收标准
- 能解释普通用户和高好友数作者的不同路径。
- 同一用户重复点赞不会重复增加计数。
- 能指出异步落库、预热和权限变化的失败窗口。

### 自检
把所有动态都写入每个好友的时间线，会有什么代价？

发布一次要写好友数那么多份投影，大作者会产生大量写放大和删除清理成本。读扩散把部分成本移到查询，实际选择取决于活跃度、好友分布与延迟目标。

### 对照文件
- backend/internal/service/moment_service.go
- backend/internal/consumer/moment_feed_consumer.go
- backend/internal/consumer/like_persist_consumer.go
- backend/internal/redis/lua_moment_like.go
- backend/internal/config/config.go

<a id="chapter16"></a>

## 17 · 上传、撤回与设置：横向功能也有边界

通过三种小功能，练习资源授权、幂等和生命周期，而不只是增加接口。

## 上传：文件也有不可信输入
项目允许 jpg、png、gif、mp4，默认大小 50MB，提供 /uploads 静态访问。实际上传 Handler 需要同时检查大小、扩展名与内容特征，生成服务端文件名，控制路径、权限和磁盘容量。仅检查客户端文件后缀不够。

前端接收到上传成功后再发送媒体消息；文件保存成功但消息未发送会产生孤儿资源，消息引用存在但文件删除会产生断链。上传目录需要持久卷与备份，多实例部署时本地磁盘不是天然共享存储。

## 从授权到文件生命周期
哪些图片可公开访问？私聊文件链接被转发后还能访问吗？默认静态目录没有自动继承聊天权限。若产品需要私有附件，需要鉴权下载、对象存储签名链接或代理策略，不能只依靠“上传接口要登录”。

实现作业先做 1MB 文本文件实验，写入随机文件名；拒绝伪后缀、超大文件与路径穿越。然后再按项目实际媒体类型扩展。不要为了测试修改真实上传目录的数据。

## 撤回与删除不是同一个动作
删除通常只影响当前用户可见状态；撤回会影响会话其他成员并需要通知。操作人、所属会话、消息所有者、时间窗口都要验证。撤回成功后的归档或延迟消息又出现，需要 tombstone/撤回事实在读侧合并，不能只发一条临时通知。

查看 MsgOpService 与 Lua 的责任分配：哪些事实在 MySQL、哪些读模型在 Redis、哪些通知走 WS。为每个动作写“部分失败后用户看到什么”。当前代码的边界就是你设计改进作业的起点。

## 设置：小业务也要可验证
用户设置、免打扰和屏蔽会话需要当前用户身份和正确归属。默认值与未设置的区别要明确；重复加入免打扰是否报错或保持成功，应该是接口契约，而不是数据库偶然报错。

## 一套可迁移的开发步骤
需求与权限 → 数据与生命周期 → 接口契约 → 最小纵向实现 → 正常与错误测试 → 跨端联调 → 故障恢复 → 部署检查。上传、撤回和设置都会走这套流程，只是数据和失败模式不同。

```powershell
# 工作目录：im/backend
go test ./internal/service -run 'TestRevoke|TestDeleteMessage|TestSettings|TestMute' -v -count=1
```

正则匹配不到某些名称时，查看源码中的真实测试名再选择；没有执行用例不是验证成功。

### 动手任务
任选上传、撤回或免打扰做一条完整切片，写权限规则、重复调用行为、清理策略和部分失败的恢复方案。

### 验收标准
- 越权操作被拒绝。
- 重复动作有定义且可测试的结果。
- 能解释资源公开访问与操作接口鉴权的区别。

### 自检
上传接口已经要求登录，为什么附件仍可能对任何人可见？

上传和下载是两条入口。静态目录公开提供文件时，持有地址的人可以直接访问；私有附件必须在下载路径或签名链接层面控制访问。

### 对照文件
- backend/internal/api/upload_handler.go
- backend/internal/service/msg_op_service.go
- backend/internal/redis/lua_revoke_msg.go
- backend/internal/service/settings_service.go
- backend/internal/model/settings.go

<a id="chapter17"></a>

## 18 · 测试与排障：先确定失败发生在哪一层

从一个错误现象，找到可重复的证据，再验证修复。

## 三类测试提供不同证据
单元测试验证业务分支与依赖调用，不需要整个环境。集成测试验证 SQL、Lua、Stream 和真实客户端。端到端测试验证注册、好友、WS 发送和落库能串起来。层层通过才能说明完整链路，不要拿 mock 的成功当成 Kafka 可用。

## 先跑不依赖外部服务的切片
```powershell
# 工作目录：im/backend
go test ./internal/service ./internal/middleware ./internal/conn ./internal/ws ./internal/consumer -count=1
```

本次制作时这组测试已通过。relay 包的单元测试也通过，但其中真实 Redis/Kafka 用例在依赖不可用时可能 SKIP，报告要分开写。

## 再开真实依赖
```powershell
# 先在 im 启动依赖，再到 im/backend
$env:GOIM_TEST_REDIS_ADDR = 'localhost:16379'
$env:GOIM_TEST_KAFKA_BROKERS = 'localhost:19092'
$env:GOIM_TEST_CONFIG = (Resolve-Path configs/config.local.yaml).Path
go test ./internal/redis ./internal/repository ./internal/relay -v -count=1
go test ./... -count=1
```

config.local 仅用于你的隔离练习实例；MySQL 集成测试会写入并清理测试 ID，不要指向生产。Redis 集成测试通常用 DB 1。当前 infra 的 MySQL/Redis 连接测试缺依赖时会 FAIL，有些 repository/relay 测试则 SKIP；测试结果要看具体状态，不能一概说全通过。

## 完整端到端
```powershell
$env:GOIM_CONFIG = (Resolve-Path configs/config.local.yaml).Path
go test ./tests/... -tags e2e -v -timeout 120s -count=1
```

先阅读 tests/e2e_test.go 的 TestMain：它负责创建路由和依赖，不等于直接访问你另一个终端里跑的 server。当前它用 NewMsgService 与旧 private/group 消费者，覆盖的是旧消息链路；设置配置里的 atomic_pipeline 不会自动切换这套测试装配。因此它通过也不能证明新 Lua+Stream+Relay 链路完整通过。

新链路需另做进程级 smoke 检查，或改造 E2E 的装配入口，让它和 cmd/server 使用一致的可选依赖。真实 Redis/Kafka 的 relay 集成测试只覆盖其中一个切片。测试名称是覆盖线索，实际装配才决定验证了什么。

## 一个正确的 bug 调查循环
1. 写出预期与实际，保留用户、msgId、serverMsgId、时间与环境。
2. 缩小到最短复现：单条消息、单个群、单次重连。
3. 沿边界检查：握手 → 分发 → service → Lua → Stream → relay → Kafka → MySQL。
4. 形成一个假设，并用能推翻它的实验检查。
5. 修复后验证原复现，再跑涉及的回归。

不要同时改三层后看到“好了”，你会失去解释原因的能力。遇到重复，先看是否同客户端 ID、同服务端 ID、同 Kafka offset；这些代表不同种类的重复。

## 关键测试清单
权限失败无写入；并发重复只有一个逻辑结果；同毫秒分页无遗漏；旧 readAck 不回退；Kafka 未确认不能删 Stream；归档重复内容相同可接受、不同要报冲突；重启可恢复 pending；转投失败不能提前提交原 offset。

## 验证能力的边界
本次环境 Docker daemon 未运行，完整 MySQL/Redis/Kafka 联调没有执行。教程会保留这一事实，所有依赖实验都给出复现步骤。不要因页面演示器显示绿色就把它视为真实后端检查，它只展示状态模型。

### 动手任务
挑选一个故障写最短复现和一个可被推翻的假设；跑相关单元、集成、E2E，并把 PASS、FAIL、SKIP 分开记录。

### 验收标准
- 有明确复现与边界证据，不凭猜测改代码。
- 至少验证一个失败窗口或并发场景。
- 测试报告区分 mock、真实依赖和未执行。

### 自检
测试命令返回成功，但是有 SKIP，能说真实依赖已经验证了吗？

不能。成功仅表示执行的测试通过。依赖不可用而跳过的用例没有提供连接、Lua 或落库证据，应单列未验证项。

### 对照文件
- backend/internal/service/msg_service_test.go
- backend/internal/relay/message_relay_test.go
- backend/internal/repository/mysql_message_integration_test.go
- backend/internal/redis/lua_message_write_test.go
- backend/tests/e2e_test.go
- backend/configs/config.test.yaml

<a id="chapter18"></a>

## 19 · 性能、部署与最终交付：给系统真实的承诺

把能跑的后端交付成能测量、能恢复、能解释边界的系统。

## 性能问题先变成一个目标
并发连接数、发送吞吐、p50/p95/p99 延迟、错误率、消费 lag、Stream pending、失败待办长度、CPU/内存和连接池等待各表示不同问题。仅报告“连接成功一万个”不能说明消息吞吐或可靠性。

实验先选一个小目标：100 个连接、15 秒发送，完整记录硬件、消息大小、好友/群规模、版本和运行模式。项目历史压测数据不是你机器的承诺，教程不把旧结果当作本次实测。

## 使用现成工具之前先准备数据
```powershell
# 工作目录：im/backend；仅隔离开发实例
go run -tags benchmark_register ./benchmark -count=20 -url=http://localhost:18080
go run -tags benchmark_friends ./benchmark -pairs=10 -url=http://localhost:18080
go run -tags benchmark_messages ./benchmark -url=ws://localhost:18080/ws -conns=10 -duration=15s
```

先阅读 benchmark 的参数和生成数据。tokens.csv、pairs.csv 含短期凭证或实验关系，应本地保留并避免提交。从小规模递增，不把压测直接指向线上公开服务。

## 找瓶颈，不先调大数字
pprof 默认绑定 127.0.0.1:16060。观察 CPU profile、堆、goroutine 数，结合消息 RTT 与消费 lag。若 Lua 遍历大群耗时，增加 HTTP goroutine 没用；若数据库锁等待，增大连接池可能更糟；若队列积压，先区分生产快、消费慢还是重试风暴。

异步让发送延迟降低，不会消灭工作。积压是延迟的另一种表现，还意味着更大的恢复窗口和内存/磁盘需求。

## 部署是运行模型的延续
Dockerfile 构建前端与 Go 二进制；生产 compose 管理依赖、端口与持久化。上线前核对 secret、数据库权限、上传卷、Redis 恢复策略、Kafka 副本、TLS/WSS、CORS/Origin、代理超时和备份恢复。

开发单节点 Kafka replication_factor=1，开发 Redis 未显式开启 AOF，项目进程内 ConnectionManager 只知道本机连接。多实例需要用户到节点路由、跨节点推送或连接网关；一个 relay 拿到事件时，接收者可能在另一个实例。当前不能直接宣称支持任意水平扩展。

## 停机也必须有顺序
当前 run 用 signal.NotifyContext 取消任务，HTTP Shutdown 等待常规请求退出，最后关闭客户端。HTTP Shutdown 不会自动管理已经升级的 WS 连接；后台 worker 也需要明确等待，防止取消后仍在操作已经关闭的 writer。生产设计要处理连接关闭、消费退出和超时上限。

## 最终综合任务
从独立目录实现一个小后端：注册登录、好友授权、双用户消息、离线补同步、幂等归档。逐步加 Redis 读模型和同存储待办，再接 Kafka；扩展到群水位线。你可以参考已完成实验，但最后必须讲得出每个边界，而不只是复制完整仓库。

交付五份证据：接口契约、状态/数据模型、一次完整消息追踪、故障恢复记录、可重复启动说明。再写一页取舍：何时保留同步 MySQL 简单方案？什么时候消息量和延迟目标值得引入 Redis/Stream/Kafka 的复杂度？

## 一次完整的验收
新用户注册并登录 → 互为好友 → 在线发送 → Bob 离线 → Alice 重复发送 → Bob 重连补同步 → 群成员已读 → MySQL 短暂停止并恢复 → 观察重试/DLQ → 正常关闭与重启。每一段保存可观察结果，说明没有覆盖的故障模式。

技术栈不是成绩单。理解了何时需要它、它给出的承诺和新增的失败模式，你才真正能把这些经验应用到订单、通知、Feed 和协作系统。

### 动手任务
完成最小后端综合作业和五份交付证据；用小规模压测解释一个真实瓶颈，写清单机可靠性和多实例边界。

### 验收标准
- 别人能按说明在空环境启动你的服务。
- 一次消息有从请求到归档/同步的完整证据。
- 能解释同步方案与当前异步方案的复杂度取舍。
- 故障、性能与部署报告明确未验证项。

### 自检
什么情况下不该一上来就用 Kafka 和 Redis Stream？

业务量小、同步数据库已经满足延迟与吞吐目标、没有独立异步消费者需求时，先把事实、事务和接口做正确通常更划算。引入队列要同时承担幂等、积压、重试、运维和恢复，不应只为了技术栈齐全。

### 对照文件
- backend/benchmark/README.md
- backend/benchmark/msg_bench.go
- backend/internal/infra/cleanup.go
- backend/cmd/server/main.go
- deploy/docker-compose.prod.yaml
- Dockerfile

## 官方阅读入口

- Go 模块：https://go.dev/doc/tutorial/create-module
- Gorilla WebSocket：https://pkg.go.dev/github.com/gorilla/websocket
- Redis Lua：https://redis.io/docs/latest/develop/programmability/eval-intro/
- Kafka 3.7 设计：https://kafka.apache.org/37/design/design/
- MySQL 8.4 事务：https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-model.html
