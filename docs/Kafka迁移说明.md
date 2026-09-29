# RabbitMQ → Kafka 迁移说明

本仓库由 GoIM（RabbitMQ 版）迁移而来，业务接口 `repository.MQRepo` 保持不变，替换范围仅限基础设施层、消费者层、启动流程和部署配置。

## 1. 依赖

| 原 | 现 |
| --- | --- |
| `github.com/rabbitmq/amqp091-go v1.12.0` | `github.com/segmentio/kafka-go v0.4.47` |

`go.sum` 只删除了 amqp 的校验行，尚未写入 kafka-go 及其间接依赖（`klauspost/compress`、`pierrec/lz4/v4` 等）。首次构建前在 `backend/` 下执行：

```bash
go mod tidy
```

## 2. 概念映射

| RabbitMQ | Kafka 实现 |
| --- | --- |
| Queue（durable） | Topic，由 `infra.KafkaClient.EnsureTopics` 启动时创建 |
| Publisher confirm | `Writer{RequiredAcks: RequireAll, Async: false}`，`WriteMessages` 返回 nil 即确认 |
| 独立 confirm channel | relay 使用独立的 `persistWriter`，与动态、点赞的 Writer 隔离 |
| Consumer + 手动 ack | 消费者组 Reader，处理成功后 `CommitMessages` 登记，按 `commit_interval_ms`（默认 1s）异步批量提交 |
| nack requeue | 分区内指数退避重试（200ms 到 10s），默认 10 次后转投 `<topic>_dlq` |
| reject（不重入队） | handler 返回 `drop(err)`，记录日志后提交 offset |
| TTL 延迟队列 + DLX | `message_persist_retry` Topic，header 带 `x-retry-at`、`x-retry-count`，到期后回流主 Topic |
| 死信队列 | `message_persist_dlq`，重试超过 5 次写入 |
| prefetch / 多消费者 | 分区数（默认 3），同组实例自动分摊分区 |
| NotifyClose 退出进程 | 删除。kafka-go 自动重连，`/ready` 由 `StartHealthMonitor` 周期 Ping 决定 |

## 3. 有序性

分区 Key：私聊和群聊用会话 ID，动态用作者 ID，点赞用动态 ID。同一 Key 落到同一分区，保证同一会话的落库顺序与发送顺序一致。

## 4. 语义差异（需要关注）

1. 分区内阻塞：某条消息持续失败时，会短暂阻塞同分区后续消息。`message_persist` 通过 retry Topic 规避；其余消费者重试 10 次（约 40s）后转投 `private_msg_persist_dlq` / `group_msg_fanout_dlq` / `moment_push_dlq` / `like_persist_dlq`，header 带原 topic/partition/offset 与最后错误，可人工回放。
2. 至少一次：offset 异步提交，进程崩溃或 rebalance 时最多重复消费约一个提交间隔的消息，消费端需要幂等。原有的 `serverMsgId` 幂等落库保持不变。
3. 点赞批量：`like_persist` 拆成 fetch 和 batch 两个 goroutine，整批写库成功后一次性提交该批 offset。
4. 服务不再因 MQ 断连退出，watchdog 仍然作为兜底保留。

## 5. 配置

```yaml
kafka:
  brokers: ["localhost:19092"]   # 也支持 "a:9092,b:9092"
  client_id: goim
  group_prefix: goim-
  partitions: 3
  replication_factor: 1
  start_offset: earliest         # earliest | latest
  commit_interval_ms: 1000       # offset 异步提交间隔；负数表示每条同步提交
```

本地 compose：Kafka `localhost:19092`（容器内 `kafka:9092`），Kafka UI `http://localhost:18081`。

## 6. 待验证清单（需要 Docker + Go 环境）

- [ ] `go mod tidy && go build ./...`
- [ ] `go vet ./...`
- [ ] `go test ./...`（单元测试不依赖 Kafka）
- [ ] `docker compose up -d` 后跑 `go test ./internal/relay/... ./tests/...`，集成测试读取 `GOIM_TEST_KAFKA_BROKERS`，默认 `localhost:19092`，连不上时自动 Skip
- [ ] 手动停掉 MySQL，确认 `message_persist_retry` 有消息、恢复后回流落库，超过 5 次进入 `message_persist_dlq`
- [ ] 停掉 Kafka，确认 `/ready` 变为不可用、服务不退出，恢复后自动重连
