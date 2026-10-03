/* Recommended teaching order, selected from immutable source snapshots.
 * Each tuple is [chapter file index, declaration needle (* = complete file), task, rationale].
 * Stable chapter IDs deliberately differ from displayed chapter numbers. */
window.DEVELOPMENT_PLANS={
  1:[
    [0,'type SendMessage','定义发送请求','先约定客户端提供哪些字段，区分客户端标识与服务端消息编号。'],
    [0,'type ServerAck','定义服务端回执','先决定成功的含义，后面的可靠链路才能围绕这个承诺实现。'],
    [0,'type MessagePersistEvent','定义持久化事件','把跨进程传递的数据约定下来，再实现发送方和消费者。'],
    [1,'func EncodeMsg','实现协议编码','在统一数据契约上增加传输封装，而不是在各处拼接 JSON。'],
    [2,') pushAfterConfirm(','对照确认后的推送','最后看回执触发的位置：先确认移交成功，再通知客户端。此处依赖后续章节实现。']
  ],
  2:[
    [1,'*','建立 Go 模块','先声明模块与依赖，才有可复现的构建入口；这里复用整个实验包的 go.mod。'],
    [0,'@http-setup','创建 HTTP 服务骨架','先有可接入请求的 ServeMux；不要一开始就引入数据库和消息队列。'],
    [0,'@http-health','写第一个健康检查','先完成一个从请求到 JSON 响应的最小闭环。'],
    [0,'@http-listen','配置超时并监听','路由写好后再接入 Server，明确地址、端口和请求头超时。']
  ],
  3:[
    [0,'type SendMessage','先写请求 DTO','先确定必填字段、长度与数值范围，让调用者知道接口接受什么。'],
    [0,'@gin-setup','建立 Gin 与中间件','用显式的 Logger、Recovery 建立观察和异常处理边界。'],
    [0,'@gin-health','先接健康路由','先确认服务入口可用，再增加业务路由，缩小排错范围。'],
    [0,'@gin-message','写绑定、校验和响应','在 HTTP 边界限制请求体并校验 JSON；这个实验只回显，不负责发送消息。'],
    [0,'@gin-listen','把处理链接到监听入口','完成一个可以运行的切片之后，再抽取通用模块。'],
    [1,'type ApiResponse','对照项目响应契约','项目统一返回 code、message、data，避免每个 Handler 自创返回格式。'],
    [1,'func ServiceError','对照错误转换','业务错误在 HTTP 边界转换，业务层不用依赖 Gin。']
  ],
  4:[
    [1,'type MySQLRepo interface','先写持久化接口','业务只声明自己需要的数据操作，具体存储实现可以替换。'],
    [0,'type AuthService','声明业务依赖','依赖通过字段显式保存，不在业务方法里临时创建连接。'],
    [0,'func NewAuthService','写注入入口','把仓库、密钥和有效期作为构造参数，让测试能够提供替身。'],
    [0,') Register(','实现注册规则','先处理账号与密码规则，再调用仓库；调用者不用理解 SQL。'],
    [2,'func run','最后对照启动装配','依赖准备好后才组装 Service 与 Handler；本章展示装配片段，完整启动见启动章。']
  ],
  5:[
    [0,'*','先写用户表迁移','先确定字段与唯一约束，让重复用户名的规则也由数据库保证。'],
    [3,'func NewMySQLPool','配置连接池','用明确的连接、超时与池大小连接数据库，先确认基础设施边界。'],
    [4,'type MySQLRepoImpl','声明仓库实现','仓库持有连接池，业务层继续面向接口。'],
    [4,'func NewMySQLRepo','接入连接池','用构造函数把连接交给仓库，避免业务方法自行连接。'],
    [4,') CreateUser(','实现参数化写入','用占位符写入用户并返回生成的 ID；这里是一条 INSERT，没有显式事务。'],
    [1,'*','增加好友关系表','一个功能切片跑通后再扩展关系数据与约束。'],
    [2,'*','增加消息表与索引','索引从查询条件反推，并为后续重复消费留出幂等约束。']
  ],
  6:[
    [0,'type user','定义账号数据','先确定保存的数据，实验的内存存储只用于隔离学习。'],
    [0,'type credentials','定义登录输入','账号密码从请求体进入，身份不能由客户端自由声明。'],
    [0,'type claims','定义令牌载荷','令牌携带身份与有效期，后续接口据此恢复用户身份。'],
    [0,'@auth-setup','准备内存存储与密钥','先初始化共享存储、锁和签名密钥，再创建 Gin 路由。'],
    [0,'@auth-register','实现注册与密码哈希','先提供创建账号的入口，不存储明文密码，也不允许同名账号覆盖。'],
    [0,'@auth-login','实现登录与令牌签发','注册能生成账号后，再校验密码并签发带有效期的访问令牌。'],
    [0,'@auth-me','实现受保护身份接口','先验签并验证有效期，再使用令牌里的用户身份。'],
    [0,'@auth-listen','接通实验监听','三个入口都准备好后监听，再按注册→登录→带令牌访问顺序验收。'],
    [2,'func ParseToken','对照项目令牌验证','从完整实验迁移到项目模块，明确验签与载荷检查边界。'],
    [2,'func JWTAuthMiddleware','接入鉴权中间件','校验通过后才把可信身份放入上下文，失败要终止处理链。'],
    [3,'func TestJWTAuthMiddleware_MissingToken','补写缺失令牌测试','除了成功路径，还要证明没有令牌的请求不能进入业务处理。']
  ],
  7:[
    [1,'type Config struct','定义配置入口','先给参数确定结构与归属；详细子结构保留在完整源码中。'],
    [0,'*','写本机配置样例','用具体值解释端口、依赖地址与超时；密钥通过环境变量提供。'],
    [1,'func LoadConfig','实现加载与默认值','展开环境变量、解析 YAML 并填充默认值；当前没有完整的必填项与参数范围校验。'],
    [3,'*','准备外部依赖','先准备 MySQL、Redis、Kafka；本播放展示配置，不会启动容器。'],
    [4,'func NewMySQLPool','连接 MySQL','先检查基础设施连接，业务对象随后才能拿到有效仓库。'],
    [5,'func NewRedisClient','连接 Redis','每项依赖分别初始化，连接失败有明确的定位点。'],
    [6,'func NewKafkaClient','创建 Kafka 客户端','完成配置到依赖客户端的转换，再装配可靠消息链路。'],
    [2,'func setupRouter','装配路由与中间件','中间件必须实际挂到路由上；只有定义函数还不算接通。'],
    [2,'func run','最后组装生命周期','入口负责创建、启动与清理依赖，业务层负责业务规则。']
  ],
  8:[
    [4,'*','定义群与成员约束','先把群成员角色落实到存储模型，再写权限规则。'],
    [0,') createPrivateConversation(','实现私聊会话创建','好友业务需要会话能力，先准备这个依赖。'],
    [0,') AcceptFriendRequest(','对照同意好友申请流程','检查请求存在与接收者身份，再更新状态、建立双向关系；当前没有拒绝已处理申请的状态检查。'],
    [1,') isOwnerOrAdmin(','写群权限判断','把权限判断集中起来，再用于各个群管理操作。'],
    [2,') SendFriendRequest(','接好友 HTTP 入口','业务规则在 Service 中确定后，Handler 只负责输入与响应。'],
    [3,') RegisterRoutes(','注册群路由','最后把实现挂到 URL，检查鉴权链是否覆盖这些入口。']
  ],
  9:[
    [0,'type envelope','先定义 WebSocket 消息封装','先约定消息类型和载荷，让客户端与服务端使用同一契约。'],
    [0,'@ws-setup','创建本机实验入口','先建立 Gin 路由，回声实验暂不接鉴权与消息存储。'],
    [0,'@ws-page','接入同源浏览器页面','实验页面从同一服务访问 /ws，便于隔离跨源与网络问题。'],
    [0,'@ws-echo','实现升级与读写循环','依次写升级、读取限制、解码校验、写入超时与回声响应。'],
    [0,'@ws-listen','接通监听','读写循环准备好后启动服务，形成独立可运行的传输实验。'],
    [3,'type ClientConnection','定义连接对象','迁移到项目时，明确连接持有哪些通道与生命周期状态。'],
    [3,'func NewClientConnection','初始化连接与通道','先创建发送、踢出与关闭通道，再启动读写协程；结构体声明不会自动初始化通道。'],
    [3,') ReadPump(','实现读取与退出通知','读取消息交给分发回调，设置心跳期限；退出时关闭连接并通知 CloseCh，供写协程和在线状态任务退出。'],
    [3,') WritePump(','实现唯一写协程','先满足并发写约束，业务发送只能向发送队列投递。'],
    [3,') Close(','提供关闭入口','这里只关闭底层连接；关闭通知来自 ReadPump 的退出处理，不能把它当成全部生命周期清理。'],
    [4,') KickOld(','实现单设备连接管理','连接对象稳定后，管理同一用户旧连接的替换。'],
    [2,') HandleMessage(','接入消息分发','把原始消息解码并分派到业务，而不是在升级函数里堆业务。'],
    [1,'func ServeWebSocket','最后接升级入口','鉴权、连接管理和分发都准备好后再连接 HTTP 与 WebSocket。']
  ],
  10:[
    [0,'type RedisRepo interface','先定义读模型接口','从需要的写入、读取与裁剪行为反推存储接口。'],
    [1,'type PersistStreamEntry','定义可恢复事件','明确可靠链路传递的数据，后续 Stream 与 Relay 使用同一契约。'],
    [0,') WriteInbox(','实现收件箱写入','先把消息内容和索引的关系落实，再增加读取。'],
    [0,') ReadInbox(','实现游标读取','读取顺序与写入索引必须一致，不能只按时间猜测顺序。'],
    [0,') TrimInbox(','增加数量裁剪','先确认读写正确，再控制数据规模。'],
    [4,'func CleanupExpiredData','增加过期清理','后台清理属于生命周期任务，不应阻塞每条业务请求。']
  ],
  11:[
    [2,'type PersistStreamEntry','先约定持久化待办','先明确原子写入后必须留下什么恢复凭据。'],
    [0,'const luaPrivateMsgWrite','写原子私聊脚本','在一次脚本中处理去重、编号、读模型与待办，避免半成功。'],
    [2,') WritePrivateMessageAtomic(','封装脚本调用','把脚本参数与返回值转换藏在仓库边界，业务无需理解 Redis 细节。'],
    [3,') handlePrivateMsg(','接入私聊业务','先执行原子存储，再由可靠链路完成后续移交。'],
    [1,'func TestAtomicPrivateMessageWrite','验证重复与原子性','写完成后在真实 Redis 上验证，播放只展示测试代码，不报告执行结果。']
  ],
  12:[
    [1,'type StreamStore interface','定义待办存储接口','先声明读取与确认行为，Relay 才能不依赖具体 Redis 客户端。'],
    [1,'type PersistPublisher interface','定义发布接口','发布失败与成功必须能被上层明确观察。'],
    [3,') NewWriter(','配置可靠发布器','先准备具体发布实现，注意确认机制和批次配置。'],
    [2,') PublishPersistEvent(','封装事件发布','把事件编码并按会话选择分区 Key，再调用同步 Writer；返回成功后 Relay 才能继续确认。'],
    [0,'type MessageRelay','定义 Relay 依赖','通过接口注入存储与发布器，后续可以用替身验证调用顺序。'],
    [0,'func NewMessageRelay','组装 Relay','生成消费组成员标识并注入依赖；完整装配已在启动章对照。'],
    [0,') moveToBacklog(','实现失败待办转存','先保存失败待办，再摘除 Stream 条目；中途崩溃可能重复，但不留下无处恢复的窗口。'],
    [0,') replayBacklog(','实现失败待办重放','重新发布成功才移除待办，失败留待下轮；当前这条重放路径不补发 WebSocket 回执或推送。'],
    [0,') handle(','连接正常与异常移交','正常路径先发布确认，再摘除 Stream 并推送；新失败留在 pending，超龄失败转存待办，无法解码的事件会丢弃。'],
    [0,') run(','补上循环与恢复','单条处理正确之后，再接读取、重试与积压恢复。'],
    [0,') Start(','连接启动入口','最后启动后台循环，让上下文负责退出控制。']
  ],
  13:[
    [1,'type SendMessage','约定消息输入','先锁定输入与幂等标识，服务端不能依赖客户端提供可信身份。'],
    [1,'type MessagePersistEvent','约定归档事件','发送端与持久化消费者必须使用一致的数据结构。'],
    [1,'type ServerAck','约定成功回执','用回执表达消息已经被可靠接收，而不是声称已经送达。'],
    [0,') HandleSendMessage(','接入消息业务入口','把输入路由到私聊或群聊流程，保持协议边界清晰。'],
    [2,') pushAfterConfirm(','在移交后发送回执','此时才具备发送成功回执的条件，离线用户由后续同步恢复。'],
    [4,') InsertPrivateMessage(','实现幂等归档','消费者可能重复收到事件，数据库写入必须容忍相同消息重放。'],
    [3,') retryOrDeadLetter(','实现重试与死信移交','落库失败时转投 retry，达到上限转投 DLQ；转投成功后才允许提交原消息进度，转投失败继续原地重试。'],
    [3,') handleRetry(','实现延迟回流','等到重试时间再写回主 Topic，写回成功后才允许提交 retry 的消费进度。'],
    [3,') handle(','连接事件解析与归档','正常事件落库成功或失败事件已安全转投时返回成功；格式错误的事件记录后丢弃，不把消费成功等同于落库成功。'],
    [5,'func commitWithRetry','实现消费进度登记','调用 CommitMessages 并重试调用错误；默认配置下真正的 offset 提交由后台定期完成，崩溃后可能重复消费。'],
    [5,'func handleWithRetry','处理原地重试与丢弃','明确哪些返回值可以推进消费，哪些错误必须退避重试；本消费者使用自己的 retry/DLQ 策略。'],
    [5,'func runReader','接通拉取、处理与提交','先 FetchMessage，再完成处理或安全移交，最后登记消费进度；退出时关闭 Reader。'],
    [3,') Start(','启动主与重试消费者','为两个 Topic 分别启动消费循环；与 Relay 和仓库一起在启动入口装配。']
  ],
  14:[
    [3,'type SyncReq','定义同步请求','先定义游标与批大小，接口才有明确的分页语义。'],
    [3,'type SyncBatch','定义同步批次','返回数据与下一页位置一起约定，客户端才知道如何继续。'],
    [1,') readIndexedMessages(','实现稳定排序读取','先做好复合游标读取，避免同毫秒消息被遗漏。'],
    [1,') SetGroupReadPos(','保存群已读位置','当前直接 HSET 覆盖游标，没有保证只前进；乱序确认可能回退，实现单调水位线还需要原子取最大值。'],
    [1,') fillGroupUnread(','计算群未读','把最新群序号与个人阅读位置结合，注意缺失状态的处理。'],
    [0,') HandleSyncReq(','接上同步业务入口','底层读取与游标准备好后再处理同步协议。'],
    [4,'func TestGetUnreadMapWatermark','补水位线集成测试','实际运行时验证成员状态与序号变化，不能只看页面动画。']
  ],
  15:[
    [4,'type MomentConfig','先约定批次与超时配置','把消费者的批次和刷新周期变成显式参数。'],
    [0,') PublishMoment(','实现发布规则','先校验业务内容和可见范围，再进入异步分发。'],
    [1,') process(','实现动态分发','发布成功之后再建立各用户的读取入口。'],
    [3,'const luaMomentLike','写原子点赞脚本','去重与计数要在同一原子边界内完成。'],
    [0,') ensureLikesLoaded(','准备点赞缓存预热','先从 MySQL 恢复已有点赞集合，再进行原子点赞；冷缓存不能直接把老用户判为新增点赞。'],
    [0,') publishLikeEvent(','封装点赞事件发布','Redis 已生效后发布持久化事件；当前发布失败只记错误，没有消息链路的 Stream 补偿，不能承诺事件必达。'],
    [0,') LikeMoment(','接通点赞业务流程','检查动态与可见性，预热缓存，再原子点赞；只有状态变化时才发布事件，重复点赞只返回当前计数。'],
    [2,') flush(','实现批量落库','解析事件并按用户和动态折叠为最后动作，再批量写入或删除；这里不提交消费进度。'],
    [2,') commit(','登记批次消费进度','整批处理完成后调用 CommitMessages；提交失败会记录日志，重复消费依靠幂等写入处理。'],
    [2,') flushWithRetry(','连接落库、重试与提交','正常路径整批落库成功才登记进度；当前启动装配未启用此消费者的 DLQ，落库失败持续退避重试。'],
    [2,') runBatcher(','实现数量与时间触发','攒够批大小或等到刷新周期就处理批次；退出时用短超时尽力处理剩余事件。'],
    [2,') runFetcher(','接入事件拉取','从 Kafka 拉取后送入批次通道；取消上下文时结束拉取并关闭通道，触发攒批协程处理剩余事件。'],
    [2,') Start(','启动拉取与攒批','通过有界通道连接拉取协程与攒批协程；批次处理阻塞会形成背压。']
  ],
  16:[
    [4,'type UserSettings','先定义设置模型','先确定默认值与字段语义，特别是 false 是否是有效输入。'],
    [3,') UpdateSettings(','实现设置业务','业务层处理规则与持久化，HTTP 层只转换输入。'],
    [2,'const luaRevokeMsg','实现撤回原子操作','先准备读模型的原子变更能力。'],
    [1,') RevokeMessage(','写撤回权限与时间规则','准备好底层能力，再验证消息归属与撤回窗口。'],
    [0,'func NewUploadHandler','配置上传边界','通过参数明确目录、大小和后缀，避免处理器暗藏配置。'],
    [0,') UploadAvatar(','写上传限制与保存','限制请求体大小并检查扩展名，再保存文件；当前没有验证真实图片内容，扩展名不能证明文件格式。'],
    [0,') RegisterRoutes(','最后注册上传路由','只有挂到受保护路由组，处理器才真正成为可调用接口。']
  ],
  17:[
    [0,'func TestPrivateMsgSend_NotFriend','先写失败情形','先把业务规则转成可观察的断言；该片段依赖文件内的测试替身。'],
    [1,'type relayStoreStub','声明存储替身','保存已确认条目和待办结果；具体方法见完整文件，当前没有记录调用顺序。'],
    [1,'type relayPublisherStub','写发布替身','让测试可以控制发布结果，覆盖难以稳定复现的异常。'],
    [1,'func TestRelayPublishesThenAcknowledgesAndPushes','检查成功移交结果','检查发布事件、Stream 确认和两端推送结果；此测试没有顺序断言，不能证明发布、确认、推送的先后关系。'],
    [1,'func TestRelayKeepsFreshEventPendingOnPublishFailure','检查发布失败保留待办','发布失败时不能摘除新事件；与成功结果测试一起覆盖正常和失败分支。'],
    [5,'*','准备集成测试配置','单元测试后，再明确真实数据库与队列的测试环境。'],
    [2,'func TestPrivateMessageInsert','增加 MySQL 幂等测试','用真实数据库验证唯一约束，替身无法证明 SQL 的语义。'],
    [3,'func TestAtomicGroupMessageWrite','增加 Redis 原子性测试','把脚本放到真实 Redis 中验证。'],
    [4,'func TestE2E_PrivateMessaging','最后验证端到端链路','在各层边界确认后，再运行跨服务测试定位联通问题。']
  ],
  18:[
    [0,'*','先约定性能验收方法','先确定负载、指标与环境，避免用一个漂亮数字代替结论。'],
    [1,'func runSender','实现负载发送','根据验收方法构造并发与持续时间，不把页面模拟当成压测。'],
    [2,'func CleanupExpiredData','检查长期运行清理','交付之前关注容量增长，短时间运行无法暴露这些问题。'],
    [3,'func run','对照优雅退出','检查服务停止时如何取消任务并释放连接，而不只考虑启动。'],
    [5,'*','编写镜像构建','本机链路验证后，准备可重复的运行产物。'],
    [4,'*','连接生产服务与依赖','最后写部署装配，密钥与环境配置在真实环境中提供。']
  ],
  19:[
    [0,'type Profile struct','先写资源模型','先约定 /profile 返回什么，API 设计从资源与字段语义开始。'],
    [0,'type updateProfileRequest','写更新请求 DTO','用 *bool 区分缺失与 false，保证关闭通知也是有效请求。'],
    [0,'type Envelope','约定统一响应','客户端与后端先共同约定 code、message、data、request_id。'],
    [0,'func respond','封装响应输出','后续业务处理器统一使用这个出口。'],
    [0,'type ProfileStore','声明存储接口','先确定业务需要 Get 与 Save，存储实现再跟进。'],
    [0,'type MemoryStore','声明内存仓库','先用小范围内存实现打通闭环，数据库接入属于后续迭代。'],
    [0,') Get(userID','写并发安全读取','并发 HTTP 请求会共享仓库，读取也需要保护。'],
    [0,') Save(profile','写并发安全保存','把互斥控制放在仓库内部，调用者不承担加锁职责。'],
    [0,'type ProfileService','注入业务依赖','业务层面向存储接口，便于替换与测试。'],
    [0,') Update(userID','实现昵称与保存规则','规则放在 Service，避免重复写进不同 Handler。'],
    [0,'type ProfileHandler','注入 Handler 依赖','HTTP 层拿到业务能力之后才开始实现接口。'],
    [0,') Get(c','实现 GET /profile','身份来自鉴权上下文；客户端不能自报 user_id。'],
    [0,') Update(c','实现 PUT /profile','绑定 JSON、转换错误、调用 Service，再统一输出响应。'],
    [0,'type Config','定义配置参数','根据已经明确的运行需求，定义端口、超时、请求体上限、JWT 与 Origin。'],
    [1,'*','切换文件，写 YAML 样例','示例参数与 Config 字段一致；密钥用环境变量引用。'],
    [0,'func loadConfig','切回 Go，写加载与校验','未知字段与无效参数在启动时失败，避免带着错误配置处理请求。'],
    [0,'func requestTrace','写请求追踪中间件','先建立请求 ID，后续错误响应与日志才能关联到同一次请求。'],
    [0,'func cors','写跨源中间件','先处理允许的 Origin 和 OPTIONS，浏览器才有机会发送实际请求。'],
    [0,'type Claims','定义 JWT 身份载荷','明确身份和有效期，登录签发与鉴权读取使用相同契约。'],
    [0,'func authenticate','写鉴权中间件','限制签名算法并要求有效期，校验成功才把 userID 放入上下文。'],
    [0,'func login','实现登录和令牌签发','开发账号只用于本实验；响应令牌后，客户端才能调用受保护接口。'],
    [0,'@api-setup','先组装配置、仓库和 Handler','对象依赖准备好后，再创建路由；每个注入关系都能在这里找到。'],
    [0,'@api-middleware','按顺序挂载中间件','Recovery → 追踪 → CORS → 请求体限制；鉴权只放到受保护路由组。'],
    [0,'@api-routes','注册公共与受保护 API','把 /health、登录、GET/PUT 明确接到 Handler，这一步才算接口可访问。'],
    [0,'@api-listen','接入监听与超时','路由组装完后才监听；到这里 Go 文件已完整重建。'],
    [2,'*','新建浏览器客户端并接请求','最后连接 API 地址、Authorization、JSON 与错误显示，按登录→读取→修改→再读取验证闭环。']
  ]
};

window.buildDevelopmentLesson=(chapter,sources,earlierFiles=[])=>{
  const spec=window.DEVELOPMENT_PLANS[chapter.id];
  const events=[],created=new Set(),inserted=new Map();
  const errors=[];
  function emit(file,start,end,title,why){
    const lines=sources[file].code.split('\n');
    const text=lines.slice(start,end).join('\n')+(end<lines.length?'\n':'');
    const key=file+':'+start+':'+end;
    if(inserted.has(key))return;
    inserted.set(key,true);events.push({kind:'type',file,start,end,text,title,why});
  }
  for(const [index,needle,title,why] of spec??[]){
    const file=chapter.files[index];
    if(!sources[file]){errors.push('Missing file '+file);continue;}
    const lines=sources[file].code.split('\n');
    const starts=file.endsWith('.go')?lines.flatMap((l,i)=>/^(func |type |const |var )/.test(l)?[i]:[]):[];
    if(!created.has(file)){
      created.add(file);
      events.push({kind:'create',file,title:(earlierFiles.includes(file)?'打开已有文件，重演本章片段':'新建演示文件')+' · '+file.split('/').pop(),why:earlierFiles.includes(file)?'这份文件在前面的章节已出现。这里从文件头重演本章要补写的片段，其他代码仍可回到阅读模式查看。':'先创建文件，再写代码；路径体现这一段代码所属的模块。'});
      if(starts.length)emit(file,0,starts[0],'写包声明与 import','先说明模块归属与用到的依赖；本章中途的代码尚未写完，不能直接编译。');
    }
    if(needle==='*'){emit(file,0,lines.length,title,why);continue;}
    // For the small executable labs, split main into actual development tasks.
    if(needle.startsWith('@')){
      const ranges={
        '@http-setup':['func main','\tmux.HandleFunc'],
        '@http-health':['\tmux.HandleFunc','\tserver :='],
        '@http-listen':['\tserver :=',null],
        '@gin-setup':['func main','\trouter.GET'],
        '@gin-health':['\trouter.GET','\trouter.POST'],
        '@gin-message':['\trouter.POST','\tlog.Println'],
        '@gin-listen':['\tlog.Println',null],
        '@api-setup':['func main','\tr := gin.New'],
        '@api-middleware':['\tr := gin.New','\tr.GET'],
        '@api-routes':['\tr.GET','\tlog.Printf'],
        '@api-listen':['\tlog.Printf',null],
        '@auth-setup':['func main','\tr.POST("/api/v1/auth/register"'],
        '@auth-register':['\tr.POST("/api/v1/auth/register"','\tr.POST("/api/v1/auth/login"'],
        '@auth-login':['\tr.POST("/api/v1/auth/login"','\tr.GET("/api/v1/me"'],
        '@auth-me':['\tr.GET("/api/v1/me"','\tlog.Println'],
        '@auth-listen':['\tlog.Println',null],
        '@ws-setup':['func main','\tr.GET("/"'],
        '@ws-page':['\tr.GET("/"','\tupgrader :='],
        '@ws-echo':['\tupgrader :=','\tlog.Println'],
        '@ws-listen':['\tlog.Println',null]
      };
      const bounds=ranges[needle];
      const start=bounds?lines.findIndex(l=>l.startsWith(bounds[0])):-1;
      const end=bounds?.[1]?lines.findIndex(l=>l.startsWith(bounds[1])):lines.length;
      if(start<0||end<=start){errors.push(file+' '+needle);continue;}
      emit(file,start,end,title,why);continue;
    }
    const at=lines.findIndex(l=>l.includes(needle));
    const start=[...starts].reverse().find(n=>n<=at);
    const end=starts.find(n=>n>at)??lines.length;
    if(at<0||start===undefined){errors.push(file+' '+needle);continue;}
    emit(file,start,end,title,why);
  }
  const lab=[2,3,6,9,19].includes(chapter.id);
  events.push({kind:'check',title:'停下来，亲自验证',why:lab?'演示不会执行命令。回到正文，按本章实验命令启动服务，检查成功与失败请求；播放完成不等于实验通过。':'当前是本章关键片段的开发顺序。回到正文完成装配与验收，在实际项目中运行测试；页面不会连接后端或自动标记学习完成。'});
  return {events,errors,scope:lab?'实验入口按步骤重建 · 项目对照仅展示关键片段':'本章关键片段 · 依赖与其他代码参见完整源码'};
};
