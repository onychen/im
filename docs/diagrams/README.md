# GoIM 图表索引

本目录存放项目所有交互式架构图与时序图产物，均由 archify 生成并通过 `showcase` 档规格校验。

每个图表独占一个同名文件夹，文件夹内同时包含 HTML 产物、JSON 规格源和 visual-check 验收证据。

## 时序图

| 图表 | 文件夹 | 说明 |
| --- | --- | --- |
| 群聊广播与成员扇出 | [group-message-path/](./group-message-path/) | Lua 原子扇出 + Redis Stream 中继 + 幂等落库，共享发件箱拉模型、水位线未读、ACK 晚于 Kafka 确认、离线 syncReq 补齐 |
| WebSocket 连接建立与在线状态 | [goim-ws-connection/](./goim-ws-connection/) | 握手鉴权、单设备踢出、在线键 TTL 与 20 秒心跳续期 |
| 朋友圈动态扇出 | [moment-fanout/](./moment-fanout/) | 发布落库、私密动态终止、大V降级跳过写扩散、时间线裁剪与游标分页 |
| 消息收发链路（旧版） | [goim-message-path/](./goim-message-path/) | 私聊推模型旧链路，仅在 `atomic_pipeline=false` 时生效，已被 message-flow 取代 |
| 消息收发链路 | [goim-message-flow/](./goim-message-flow/) | Lua 原子写入 → Redis Stream → relay 中继 → Kafka 幂等落库，含失败重试与 DLQ |

## 架构图

| 图表 | 文件夹 | 说明 |
| --- | --- | --- |
| 项目架构 | [project-architecture/](./project-architecture/) | 分层与模块职责总览，源码证据已锚定到仓库修订版本 |
| 目录结构 | [goim-structure/](./goim-structure/) | 仓库目录与代码组织 |

## 文件夹内容约定

以 `group-message-path/` 为例，每个文件夹内通常包含：

- `goim-group-message-path.sequence.html`：交互式图表产物，可直接用浏览器打开。
- `goim-group-message-path.sequence.json`：规格源文件，修改后用 archify 重新 deliver 即可更新 HTML。
- `goim-group-message-path.sequence.visual-check.*`：浏览器渲染验收证据，包括 1440x900 与 2048x1320 的明暗截图、联系表与回执 JSON。

## 重新生成

在仓库根目录执行，路径按实际图表文件夹与名称替换：

```bash
node .agents/skills/archify/bin/archify.mjs validate sequence docs/diagrams/group-message-path/goim-group-message-path.sequence.json --quality showcase --json
node .agents/skills/archify/bin/archify.mjs deliver sequence docs/diagrams/group-message-path/goim-group-message-path.sequence.json docs/diagrams/group-message-path/goim-group-message-path.sequence.html --quality showcase --json
node .agents/skills/archify/bin/archify.mjs visual-check docs/diagrams/group-message-path/goim-group-message-path.sequence.html --json
```

架构图类型将上述命令中的 `sequence` 替换为 `architecture`。`project-architecture` 声明了源码证据，校验与生成时需额外传 `--repo-root .`。

## 验收状态

- message-flow、group-message-path、message-path、goim-structure、project-architecture：已通过 `showcase` 校验与四视口（1440x900 / 1600x1000 / 1920x1080 / 2048x1320 明暗）visual-check。
- ws-connection、moment-fanout：链路内容正确，但在 1440x900 视口存在纵向溢出（大图需滚动查看），2048x1320 下正常，属遗留待优化项。
