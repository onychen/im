package model

import "time"

type Moment struct {
	ID         int64     `json:"id"`
	AuthorID   int64     `json:"author_id"`
	Content    string    `json:"content"`
	MediaUrls  *string   `json:"media_urls,omitempty"` // 以JSON字符串形式存储在数据库中；可为空
	Visibility int       `json:"visibility"`           // 2=好友可见，3=仅自己可见（1 为历史值，读取时按好友可见处理）
	CreatedAt  time.Time `json:"created_at"`

	// 以下展示字段在查询时填充。
	LikeCount    int64           `json:"like_count" gorm:"-"`  // 点赞数
	LikedByMe    bool            `json:"liked_by_me" gorm:"-"` // 当前查看者是否已赞
	AuthorName   string          `json:"author_name" gorm:"-"`
	AuthorAvatar string          `json:"author_avatar" gorm:"-"`
	Comments     []MomentComment `json:"comments" gorm:"-"`
}

type MomentLike struct {
	ID        int64     `json:"id"`
	MomentID  int64     `json:"moment_id"`
	UserID    int64     `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

// MomentLiker 是动态点赞列表中可展示的用户资料。
type MomentLiker struct {
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	AvatarURL string `json:"avatar_url"`
}

type MomentComment struct {
	ID        int64     `json:"id"`
	MomentID  int64     `json:"moment_id"`
	UserID    int64     `json:"user_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
	Username  string    `json:"username" gorm:"->;-:migration"`
	AvatarURL string    `json:"avatar_url" gorm:"->;-:migration"`
}

// FeedEntry 是 Feed ZSet（收件箱/寄件箱）中的一条记录：
// 动态 ID 及其发布时间戳（毫秒），用于推拉合并时的排序与游标分页。
type FeedEntry struct {
	MomentID int64
	Ts       int64 // 发布时间戳（毫秒），即 ZSet 的 score
}
