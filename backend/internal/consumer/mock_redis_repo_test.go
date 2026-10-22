package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/goim/goim/internal/model"
	"github.com/goim/goim/internal/repository"
)

// mockRedisRepo 是 consumer 包测试用的 repository.RedisRepo 实现，
// 只记录朋友圈消费者会用到的寄件箱、收件箱扇出与大V标记。
type mockRedisRepo struct {
	mu           sync.Mutex
	momentOutbox map[int64][]int64 // authorID -> momentIDs
	fanoutInbox  map[int64][]int64 // userID -> momentIDs
	bigUsers     map[int64]bool

	addOutboxErr error
}

var _ repository.RedisRepo = (*mockRedisRepo)(nil)

func newMockRedisRepo() *mockRedisRepo {
	return &mockRedisRepo{
		momentOutbox: make(map[int64][]int64),
		fanoutInbox:  make(map[int64][]int64),
		bigUsers:     make(map[int64]bool),
	}
}

func (m *mockRedisRepo) TrimTimelineByTime(context.Context, int64, int64) error { return nil }

func (m *mockRedisRepo) PublishMomentFeed(context.Context, int64, int64, int64) error { return nil }

func (m *mockRedisRepo) GetMomentFeed(context.Context, int64, int64, int) ([]int64, error) {
	return nil, nil
}

func (m *mockRedisRepo) FanoutMomentFeed(_ context.Context, friendIDs []int64, momentID int64, _ int64, _ int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fid := range friendIDs {
		m.fanoutInbox[fid] = append(m.fanoutInbox[fid], momentID)
	}
	return nil
}

func (m *mockRedisRepo) AddToOutbox(_ context.Context, authorID int64, momentID int64, _ int64, _ int) error {
	if m.addOutboxErr != nil {
		return m.addOutboxErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.momentOutbox[authorID] = append(m.momentOutbox[authorID], momentID)
	return nil
}

func (m *mockRedisRepo) MarkBigUser(_ context.Context, userID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bigUsers[userID] = true
	return nil
}

func (m *mockRedisRepo) FilterBigUsers(_ context.Context, userIDs []int64) ([]int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int64, 0, len(userIDs))
	for _, id := range userIDs {
		if m.bigUsers[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *mockRedisRepo) GetTimelinePage(context.Context, int64, int64, int64, int) ([]model.FeedEntry, error) {
	return nil, nil
}

func (m *mockRedisRepo) GetOutboxPage(context.Context, int64, int64, int64, int) ([]model.FeedEntry, error) {
	return nil, nil
}

func (m *mockRedisRepo) LikeMomentAtomic(context.Context, int64, int64) (bool, int64, error) {
	return false, 0, nil
}

func (m *mockRedisRepo) UnlikeMomentAtomic(context.Context, int64, int64) (bool, int64, error) {
	return false, 0, nil
}

func (m *mockRedisRepo) EnsureMomentLikesLoaded(context.Context, int64, func(context.Context) ([]int64, error), time.Duration) error {
	return nil
}

func (m *mockRedisRepo) GetMomentLikeStats(context.Context, int64, []int64) (map[int64]int64, map[int64]bool, error) {
	return map[int64]int64{}, map[int64]bool{}, nil
}

func (m *mockRedisRepo) GetMomentLikerIDs(context.Context, int64) ([]int64, error) { return nil, nil }

func (m *mockRedisRepo) DeleteMomentLikes(context.Context, int64) error { return nil }
