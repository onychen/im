package infra

import (
	"context"
	"testing"
	"time"

	"github.com/goim/goim/internal/config"
	"github.com/stretchr/testify/require"
)

func TestConnectKafkaWithRetryStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempted := make(chan struct{}, 1)
	result := make(chan error, 1)

	go func() {
		_, err := ConnectKafkaWithRetry(ctx, &config.KafkaConfig{
			Brokers:           []string{"127.0.0.1:1"},
			Partitions:        1,
			ReplicationFactor: 1,
		}, time.Hour, time.Hour, func(error, time.Duration) {
			select {
			case attempted <- struct{}{}:
			default:
			}
		})
		result <- err
	}()

	select {
	case <-attempted:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("Kafka 连接尝试未按时发生")
	}

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("取消 context 后重试循环没有退出")
	}
}

func TestNewKafkaClientRequiresBrokers(t *testing.T) {
	_, err := NewKafkaClient(&config.KafkaConfig{})
	require.Error(t, err)
}
