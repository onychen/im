//go:build integration

package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/goim/goim/internal/model"
	"github.com/stretchr/testify/require"
)

func TestMomentLikesConcurrentWrites(t *testing.T) {
	repo, _ := newGORMTestRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	moment := &model.Moment{AuthorID: 1, Content: "likes", Visibility: 2}
	require.NoError(t, repo.CreateMoment(ctx, moment))
	other := &model.Moment{AuthorID: 2, Content: "other", Visibility: 2}
	require.NoError(t, repo.CreateMoment(ctx, other))
	_, err := repo.SetMomentLike(ctx, other.ID, 9, true)
	require.NoError(t, err)

	const likers = 8
	var wg sync.WaitGroup
	errs := make(chan error, likers*2)
	for i := int64(1); i <= likers; i++ {
		for range 2 {
			wg.Add(1)
			go func(userID int64) {
				defer wg.Done()
				_, err := repo.SetMomentLike(ctx, moment.ID, userID, true)
				errs <- err
			}(i)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	stats, err := repo.GetMomentLikeStats(ctx, 1, []int64{moment.ID, other.ID})
	require.NoError(t, err)
	require.Equal(t, MomentLikeStats{MomentID: moment.ID, LikeCount: likers, LikedByMe: true}, stats[moment.ID])
	require.Equal(t, MomentLikeStats{MomentID: other.ID, LikeCount: 1}, stats[other.ID])
	for range 2 {
		count, err := repo.SetMomentLike(ctx, moment.ID, 1, false)
		require.NoError(t, err)
		require.EqualValues(t, likers-1, count)
	}
}

func TestMomentLikeConcurrentWithDelete(t *testing.T) {
	repo, db := newGORMTestRepo(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 10 {
		moment := &model.Moment{AuthorID: 1, Content: "delete", Visibility: 2}
		require.NoError(t, repo.CreateMoment(ctx, moment))
		liked := make(chan error, 1)
		go func() {
			_, err := repo.SetMomentLike(ctx, moment.ID, 2, true)
			liked <- err
		}()
		deleteErr := repo.DeleteMoment(ctx, moment.ID)
		likeErr := <-liked
		require.NoError(t, deleteErr)
		if likeErr != nil {
			require.ErrorIs(t, likeErr, ErrMomentNotFound)
		}
	}
	var count int64
	require.NoError(t, db.Model(&model.MomentLike{}).Count(&count).Error)
	require.Zero(t, count)
}
