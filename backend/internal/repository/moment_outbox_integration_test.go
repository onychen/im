//go:build integration

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/goim/goim/internal/model"
	"github.com/stretchr/testify/require"
)

func TestMomentOutboxRollsBackMomentOnEnqueueFailure(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx := context.Background()
	require.NoError(t, db.Create(&momentOutboxEvent{MomentID: 100, Payload: "{}"}).Error)

	moment := &model.Moment{ID: 100, AuthorID: 1, Content: "hello", Visibility: 2}
	require.ErrorContains(t, repo.CreateMomentWithOutbox(ctx, moment), "写入动态 Outbox")
	stored, err := repo.GetMomentByID(ctx, moment.ID)
	require.NoError(t, err)
	require.Nil(t, stored)
}

func TestMomentOutboxRetriesAndClearsAfterAck(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx := context.Background()
	moment := &model.Moment{AuthorID: 1, Content: "hello", Visibility: 2, CreatedAt: time.UnixMilli(1234567890123)}
	require.NoError(t, repo.CreateMomentWithOutbox(ctx, moment))

	unavailable := errors.New("Kafka unavailable")
	found, err := repo.RelayNextMomentEvent(ctx, func(_ context.Context, event *model.Moment) error {
		require.Equal(t, moment.ID, event.ID)
		return unavailable
	})
	require.ErrorIs(t, err, unavailable)
	require.True(t, found)

	publish := func(_ context.Context, event *model.Moment) error {
		require.Equal(t, moment.ID, event.ID)
		require.Equal(t, moment.CreatedAt.UnixMilli(), event.CreatedAt.UnixMilli())
		return nil
	}
	found, err = repo.RelayNextMomentEvent(ctx, publish)
	require.NoError(t, err)
	require.False(t, found)

	require.NoError(t, db.Exec("UPDATE moment_outbox SET available_at = CURRENT_TIMESTAMP(6) WHERE moment_id = ?", moment.ID).Error)
	found, err = repo.RelayNextMomentEvent(ctx, func(ctx context.Context, event *model.Moment) error {
		// A competing worker skips this row while the first one awaits confirmation.
		competingCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		claimed, err := repo.RelayNextMomentEvent(competingCtx, publish)
		require.NoError(t, err)
		require.False(t, claimed)
		return publish(ctx, event)
	})
	require.NoError(t, err)
	require.True(t, found)
	found, err = repo.RelayNextMomentEvent(ctx, publish)
	require.NoError(t, err)
	require.False(t, found)
}
