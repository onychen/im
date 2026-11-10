package consumer

import (
	"context"
	"time"

	"github.com/goim/goim/internal/model"
	"go.uber.org/zap"
)

type momentOutboxStore interface {
	RelayNextMomentEvent(context.Context, func(context.Context, *model.Moment) error) (bool, error)
}

// StartMomentOutboxRelay 启动投递任务，返回在任务退出后关闭的通道。
func StartMomentOutboxRelay(ctx context.Context, store momentOutboxStore, publish func(context.Context, *model.Moment) error, logger *zap.Logger) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			found, err := store.RelayNextMomentEvent(attemptCtx, publish)
			cancel()
			if err != nil && ctx.Err() == nil {
				logger.Warn("动态 Outbox 投递失败，将重试", zap.Error(err))
			}
			if (err != nil || !found) && !sleepCtx(ctx, 250*time.Millisecond) {
				return
			}
		}
	}()
	return done
}
