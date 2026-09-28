<div align="center">

# GoIM

一个面向 Web 的即时通讯应用，支持私聊、群聊、好友关系、朋友圈动态、文件上传与实时 WebSocket 通信。

[在线访问](https://im.vectorcontrol.tech/) · [快速开始](#快速开始) · [开发文档](#重要文档)

![Go](https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat-square&logo=go&logoColor=00ADD8)
![React](https://img.shields.io/badge/React-19-61DAFB?style=flat-square&logo=react&logoColor=61DAFB)
![TypeScript](https://img.shields.io/badge/TypeScript-5.7-3178C6?style=flat-square&logo=typescript&logoColor=3178C6)
![Vite](https://img.shields.io/badge/Vite-8-646CFF?style=flat-square&logo=vite&logoColor=646CFF)
![MySQL](https://img.shields.io/badge/MySQL-8.4-4479A1?style=flat-square&logo=mysql&logoColor=4479A1)
![Redis](https://img.shields.io/badge/Redis-7.2-DC382D?style=flat-square&logo=redis&logoColor=DC382D)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=2496ED)
![CI](https://img.shields.io/github/actions/workflow/status/Johnny-Cheung/IM/ghcr-publish.yml?branch=main&style=flat-square&label=CI)

</div>

## 项目功能

GoIM 是一个完整的 Web 即时通讯系统。前端提供面向用户的聊天与社交界面，后端同时提供 REST API 与 WebSocket 长连接；MySQL、Redis、Kafka 分别负责数据持久化、热点缓存和异步消息处理。

- **账户与身份认证**：用户注册、登录、JWT 访问令牌与刷新令牌、个人资料和设置管理。
- **好友关系**：好友申请、接受/拒绝、删除好友、拉黑用户与在线状态查询。
- **私聊与群聊**：WebSocket 实时收发消息、离线消息、已读状态、消息撤回与会话管理。
- **群组管理**：创建群组、邀请/移除成员、角色管理、群主转让与群成员列表。
- **朋友圈**：发布图文动态、时间线、点赞、评论与互动通知。
- **媒体与文件**：头像及图片、GIF、视频上传，后端提供静态资源访问。
- **工程能力**：单元/集成测试、基准压测工具、Swagger 接口文档、Docker Compose 本地依赖和 GHCR 镜像发布。

## 技术栈

| 层级 | 技术 | 职责 |
| --- | --- | --- |
| 前端 | React 19 · TypeScript · Vite · Tailwind CSS | SPA 页面、组件、样式与构建 |
| 前端状态 | Zustand · TanStack Query · React Router | 本地状态、服务端缓存与路由 |
| 后端 | Go 1.24+ · Gin | REST API、鉴权、中间件与业务服务 |
| 实时通信 | Gorilla WebSocket | 私聊、群聊与在线实时推送 |
| 数据存储 | MySQL 8.4 | 用户、关系、消息、群组、动态等持久化数据 |
| 缓存 | Redis 7.2 · Lua | 会话、未读/已读、时间线及高频原子操作 |
| 异步消息 | Kafka 3.7（KRaft）· segmentio/kafka-go | 消息持久化、动态分发、点赞异步落库 |
| 交付 | Docker Compose · GitHub Actions · GHCR | 本地依赖、CI/CD 与镜像发布 |

## 项目结构

```text
IM/
├── frontend/                         # React/Vite 前端
│   ├── src/
│   │   ├── api/                      # REST API 客户端
│   │   ├── components/               # 通用组件、布局与实时连接组件
│   │   ├── features/                 # 好友、群组、朋友圈等功能模块
│   │   ├── pages/                    # 登录、聊天、联系人、动态、设置页面
│   │   ├── realtime/                 # WebSocket 客户端与通知处理
│   │   ├── stores/                   # Zustand 状态管理
│   │   └── test/                     # Vitest 测试
│   └── docs/                         # 前端协议、接口与联调文档
├── backend/                          # Go 后端
│   ├── cmd/server/                   # 服务启动入口
│   ├── internal/
│   │   ├── api/                      # HTTP Handler
│   │   ├── service/                  # 业务服务层
│   │   ├── repository/               # MySQL、Redis、Kafka 仓储层
│   │   ├── ws/                       # WebSocket 处理与协议
│   │   ├── consumer/                 # Kafka 消费者（消费者组 + 手动提交 offset）
│   │   ├── redis/                    # Redis Lua 脚本
│   │   └── middleware/               # JWT、CORS 等中间件
│   ├── scripts/migrations/           # MySQL 初始化与迁移脚本
│   ├── configs/                      # 本地、测试、容器配置
│   ├── benchmark/                    # 压力测试工具
│   └── docs/                         # 后端、架构、API 与部署文档
├── deploy/                           # 生产部署编排与变量模板
├── docs/                             # 跨端联调与后续优化文档
├── docker-compose.yaml               # 本地 MySQL、Redis、Kafka、Kafka UI
└── Dockerfile                        # 前后端单镜像构建
```

## 重要文档

<details>
<summary><code>docs/</code> · 跨端协作与规划</summary>

- [前后端联调实施计划](docs/前后端联调实施计划.md)
- [前后端联调问题记录](docs/前后端联调问题记录.md)
- [后续的优化更新计划](docs/后续的优化更新计划.md)

</details>

<details>
<summary><code>backend/docs/</code> · 后端、接口与部署</summary>

- [API 参考文档](backend/docs/API%20参考文档.md)
- [产品功能需求](backend/docs/产品功能需求.md)
- [产品技术设计](backend/docs/产品技术设计.md)
- [项目系统架构](backend/docs/项目系统架构.md)
- [项目部署指南](backend/docs/项目部署指南.md)
- [接口测试清单](backend/docs/接口测试清单.md)
- [压力测试总结](backend/docs/压力测试总结.md)

</details>

<details>
<summary><code>frontend/docs/</code> · 前端协议与联调</summary>

- [WebSocket 协议](frontend/docs/WebSocket协议.md)
- [前后端联调指南](frontend/docs/前后端联调指南.md)
- [前端接口契约](frontend/docs/前端接口契约.md)
- [前端开发实施流程](frontend/docs/前端开发实施流程.md)
- [前端界面效果说明](frontend/docs/前端界面效果说明.md)

</details>

## 快速开始

```powershell
# 1. 复制本地配置模板
Copy-Item .env.example .env
Copy-Item backend/configs/config.local.example.yaml backend/configs/config.local.yaml
Copy-Item frontend/.env.example frontend/.env.local

# 2. 启动 MySQL、Redis、Kafka
docker compose up -d

# 3. 启动后端（新终端，首次需补全依赖校验）
Set-Location backend
go mod tidy
go run ./cmd/server -c configs/config.local.yaml

# 4. 启动前端（另一个新终端）
Set-Location frontend
npm ci
npm run dev
```

前端默认地址为 `http://localhost:5173`，后端健康检查为 `http://localhost:18080/health`。Kafka 对宿主机暴露 `localhost:19092`，Kafka UI 为 `http://localhost:18081`。

## 消息队列说明（Kafka）

本项目由原 RabbitMQ 版本迁移而来，Topic 与原队列一一对应，由服务启动时统一创建（Broker 关闭了自动建 Topic）：

| Topic | 消费者组 | 分区 Key | 说明 |
| --- | --- | --- | --- |
| `message_persist` | `goim-message-persist` | 会话 ID | 私聊/群聊消息异步落库 |
| `message_persist_retry` | `goim-message-persist-retry` | 会话 ID | 落库失败的延迟重试（5s 间隔，最多 5 次） |
| `message_persist_dlq` | - | 会话 ID | 超过重试次数的死信 |
| `private_msg_persist` | `goim-private-msg` | 会话 ID | 私聊持久化 |
| `group_msg_fanout` | `goim-group-msg` | 群会话 ID | 群消息扇出 |
| `moment_push` | `goim-moment-feed` | 作者 ID | 动态推送到好友时间线 |
| `like_persist` | `goim-like-persist` | 动态 ID | 点赞批量落库 |
| `comment_persist` | - | - | 预留（与原版一致，暂无消费者） |

与 RabbitMQ 版本的语义差异：生产端 `RequiredAcks=all` 同步写入替代 publisher confirm；消费端同步提交 offset 替代 ack；处理失败在分区内退避重试（200ms 到 10s）替代 nack requeue；Kafka 客户端自动重连，服务不再因 MQ 断连主动退出，`/ready` 基于周期 Ping 判断。
