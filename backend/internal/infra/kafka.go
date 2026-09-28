package infra

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/goim/goim/internal/config"
)

// ── Topic 名称 ──
// 与原 RabbitMQ 队列一一对应，业务代码只需要替换投递目标，语义保持不变。
const (
	TopicMessagePersist      = "message_persist"
	TopicMessagePersistRetry = "message_persist_retry" // 替代原 message_persist_delay（TTL + 死信回流）
	TopicMessagePersistDLQ   = "message_persist_dlq"
	TopicPrivateMsgPersist   = "private_msg_persist"
	TopicGroupMsgFanout      = "group_msg_fanout"
	TopicMomentPush          = "moment_push"
	TopicLikePersist         = "like_persist"
	TopicCommentPersist      = "comment_persist"
)

// TopicNames 列出 IM 系统所需的所有 Topic。
var TopicNames = []string{
	TopicMessagePersist,
	TopicMessagePersistRetry,
	TopicMessagePersistDLQ,
	TopicPrivateMsgPersist,
	TopicGroupMsgFanout,
	TopicMomentPush,
	TopicLikePersist,
	TopicCommentPersist,
}

// ── 消费者组名称（会拼接 config.Kafka.GroupPrefix）──
const (
	GroupMessagePersist      = "message-persist"
	GroupMessagePersistRetry = "message-persist-retry"
	GroupPrivateMsg          = "private-msg"
	GroupGroupMsg            = "group-msg"
	GroupMomentFeed          = "moment-feed"
	GroupLikePersist         = "like-persist"
)

// KafkaClient 封装 broker 地址与 Dialer，负责创建 Writer / Reader 以及运维类操作。
// kafka-go 的 Writer/Reader 自带断线重连，因此不再需要像 RabbitMQ 那样
// 监听连接关闭并主动退出进程。
type KafkaClient struct {
	cfg    config.KafkaConfig
	dialer *kafka.Dialer
}

// NewKafkaClient 校验配置并创建客户端，不会发起网络连接。
func NewKafkaClient(cfg *config.KafkaConfig) (*KafkaClient, error) {
	if cfg == nil || len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers 未配置")
	}
	return &KafkaClient{
		cfg: *cfg,
		dialer: &kafka.Dialer{
			ClientID:  cfg.ClientID,
			Timeout:   10 * time.Second,
			DualStack: true,
		},
	}, nil
}

// Config 返回客户端使用的配置副本。
func (c *KafkaClient) Config() config.KafkaConfig { return c.cfg }

// dialAny 依次尝试连接每个 broker，返回第一个成功的连接。
func (c *KafkaClient) dialAny(ctx context.Context) (*kafka.Conn, error) {
	var lastErr error
	for _, broker := range c.cfg.Brokers {
		conn, err := c.dialer.DialContext(ctx, "tcp", broker)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("连接 Kafka broker 失败: %w", lastErr)
}

// Ping 检查至少一个 broker 可达，并能拿到集群元数据。
func (c *KafkaClient) Ping(ctx context.Context) error {
	conn, err := c.dialAny(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.Brokers(); err != nil {
		return fmt.Errorf("读取 Kafka 集群元数据失败: %w", err)
	}
	return nil
}

// EnsureTopics 在 controller 上创建所有 Topic；已存在的 Topic 会被忽略。
// 生产环境建议关闭 broker 端 auto.create.topics.enable，由这里统一声明分区数与副本数。
func (c *KafkaClient) EnsureTopics(ctx context.Context) error {
	conn, err := c.dialAny(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("获取 Kafka controller 失败: %w", err)
	}
	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	cc, err := c.dialer.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("连接 Kafka controller %s 失败: %w", controllerAddr, err)
	}
	defer cc.Close()

	topics := make([]kafka.TopicConfig, 0, len(TopicNames))
	for _, name := range TopicNames {
		topics = append(topics, kafka.TopicConfig{
			Topic:             name,
			NumPartitions:     c.cfg.Partitions,
			ReplicationFactor: c.cfg.ReplicationFactor,
		})
	}
	if err := cc.CreateTopics(topics...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("创建 Kafka topics 失败: %w", err)
	}
	return nil
}

// ConnectKafkaWithRetry 持续探测 Kafka 并声明 Topic，直到成功或 ctx 被取消。
// waitFn 会在每次失败后收到本次错误和下一次等待时长，可用于记录日志。
func ConnectKafkaWithRetry(
	ctx context.Context,
	cfg *config.KafkaConfig,
	initialDelay time.Duration,
	maxDelay time.Duration,
	waitFn func(error, time.Duration),
) (*KafkaClient, error) {
	client, err := NewKafkaClient(cfg)
	if err != nil {
		return nil, err
	}
	if initialDelay <= 0 {
		initialDelay = time.Second
	}
	if maxDelay < initialDelay {
		maxDelay = initialDelay
	}

	delay := initialDelay
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = client.Ping(attemptCtx)
		if err == nil {
			err = client.EnsureTopics(attemptCtx)
		}
		cancel()
		if err == nil {
			return client, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("等待 Kafka 连接时终止: %w", ctx.Err())
		}
		if waitFn != nil {
			waitFn(err, delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, fmt.Errorf("等待 Kafka 连接时终止: %w", ctx.Err())
		case <-timer.C:
		}

		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

// NewWriter 创建一个同步、强确认的 Writer：
//   - RequiredAcks=RequireAll 等价于 RabbitMQ publisher confirm，WriteMessages 返回 nil
//     即代表 ISR 全部落盘，可作为 relay 的可靠性边界。
//   - Topic 留空，由每条 Message 自行指定，便于一个 Writer 服务多个 Topic。
//   - Balancer 使用 Hash，按 Key 路由分区，保证同一会话/同一动态的事件有序。
//   - BatchTimeout 必须调小，kafka-go 默认 1s，同步写会被白白拖慢。
func (c *KafkaClient) NewWriter() *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(c.cfg.Brokers...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireAll,
		Async:                  false,
		MaxAttempts:            3,
		BatchSize:              100,
		BatchTimeout:           5 * time.Millisecond,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: false,
		Transport: &kafka.Transport{
			ClientID:    c.cfg.ClientID,
			DialTimeout: 10 * time.Second,
		},
	}
}

// NewReader 为指定 Topic 创建消费者组 Reader。
// CommitInterval=0 表示 CommitMessages 同步提交，业务处理成功后才前移 offset，
// 等价于 RabbitMQ 的手动 ack。
func (c *KafkaClient) NewReader(topic, group string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:           c.cfg.Brokers,
		GroupID:           c.cfg.GroupPrefix + group,
		Topic:             topic,
		Dialer:            c.dialer,
		MinBytes:          1,
		MaxBytes:          10 << 20,
		MaxWait:           500 * time.Millisecond,
		CommitInterval:    0,
		StartOffset:       c.startOffset(),
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		ReadBackoffMin:    100 * time.Millisecond,
		ReadBackoffMax:    time.Second,
	})
}

func (c *KafkaClient) startOffset() int64 {
	if c.cfg.StartOffset == "latest" {
		return kafka.LastOffset
	}
	return kafka.FirstOffset
}

// StartHealthMonitor 周期性 Ping broker，返回一个并发安全的就绪查询函数，供 /ready 使用。
// onChange 在就绪状态发生变化时回调（可为 nil）。
func (c *KafkaClient) StartHealthMonitor(ctx context.Context, interval time.Duration, onChange func(ready bool, err error)) func() bool {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	var ready atomic.Bool
	ready.Store(true)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := c.Ping(pingCtx)
			cancel()
			now := err == nil
			if prev := ready.Swap(now); prev != now && onChange != nil {
				onChange(now, err)
			}
		}
	}()

	return ready.Load
}
