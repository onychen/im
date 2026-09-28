package relay

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/goim/goim/internal/config"
	"github.com/goim/goim/internal/conn"
	"github.com/goim/goim/internal/infra"
	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

func TestMessageRelayRedisStreamToKafka(t *testing.T) {
	redisAddr := os.Getenv("GOIM_TEST_REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:16379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: redisAddr, DB: 1})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis unavailable: %v", err)
	}
	defer rdb.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A test-local Stream namespace is not currently configurable, so reset the
	// dedicated persistence Stream before and after this isolated integration test.
	require.NoError(t, rdb.Del(ctx, "message_persist_stream", "message_persist_confirmed").Err())
	defer rdb.Del(context.Background(), "message_persist_stream", "message_persist_confirmed")

	brokers := os.Getenv("GOIM_TEST_KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:19092"
	}
	kafkaCfg := &config.KafkaConfig{
		Brokers:           strings.Split(brokers, ","),
		ClientID:          "goim-relay-test",
		GroupPrefix:       "goim-test-" + time.Now().Format("150405.000") + "-",
		Partitions:        1,
		ReplicationFactor: 1,
		StartOffset:       "latest",
	}
	client, err := infra.NewKafkaClient(kafkaCfg)
	require.NoError(t, err)
	pingCtx, pingCancel := context.WithTimeout(ctx, 3*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx); err != nil {
		t.Skipf("Kafka unavailable: %v", err)
	}
	require.NoError(t, client.EnsureTopics(ctx))

	// 独立消费者组 + latest，确保只读到本测试产生的事件。
	reader := client.NewReader(infra.TopicMessagePersist, "relay-integration")
	defer reader.Close()
	writer := client.NewWriter()
	defer writer.Close()

	redisRepo := repository.NewRedisRepo(rdb)
	relay := NewMessageRelay(redisRepo, repository.NewMQRepo(writer), conn.NewConnectionManager(), zap.NewNop())
	require.NoError(t, relay.Start(ctx))
	clientID := "relay-integration-test"
	cleanupKeys := []string{"friend:92001:92002", "friend:92002:92001", "msg_dedup:92001:" + clientID, "inbox_order:92001", "inbox_order:92002", "inbox_data:92001", "inbox_data:92002", "conv_order:92001", "conv_order:92002", "conv_meta:92001", "conv_meta:92002", "unread:92002"}
	rdb.Del(ctx, cleanupKeys...)
	defer rdb.Del(context.Background(), cleanupKeys...)
	require.NoError(t, rdb.Set(ctx, "friend:92001:92002", "1", 0).Err())
	require.NoError(t, rdb.Set(ctx, "friend:92002:92001", "1", 0).Err())

	// 让 reader 先加入组并定位到 latest，再产生消息。
	time.Sleep(3 * time.Second)

	result, err := redisRepo.WritePrivateMessageAtomic(ctx, 92001, &model.SendMessage{ClientMsgID: clientID, ConvType: model.ConvTypePrivate, ToID: 92002, MsgType: model.MsgTypeText, Content: "relay"})
	require.NoError(t, err)
	require.Equal(t, 0, result.ErrCode)

	fetchCtx, fetchCancel := context.WithTimeout(ctx, 15*time.Second)
	defer fetchCancel()
	for {
		msg, err := reader.FetchMessage(fetchCtx)
		if err != nil {
			t.Fatalf("timed out waiting for relayed Kafka event: %v", err)
		}
		var event model.MessagePersistEvent
		require.NoError(t, json.Unmarshal(msg.Value, &event))
		require.NoError(t, reader.CommitMessages(ctx, msg))
		if event.ServerMsgID == result.MsgID {
			break
		}
	}
	require.Eventually(t, func() bool { return rdb.XLen(ctx, "message_persist_stream").Val() == 0 }, 5*time.Second, 50*time.Millisecond)
}

