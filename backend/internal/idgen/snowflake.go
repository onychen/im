// Package idgen 提供单机雪花 ID 生成器。
//
// 布局（共 63 位）：41 位毫秒时间戳 | 10 位节点号 | 12 位毫秒内序号。
// 当前项目只部署单机，节点号由配置 message.node_id 给出，默认 0。
package idgen

import (
	"fmt"
	"sync"
	"time"
)

const (
	nodeBits     = 10
	sequenceBits = 12
	maxNode      = -1 ^ (-1 << nodeBits)
	maxSequence  = -1 ^ (-1 << sequenceBits)
	timeShift    = nodeBits + sequenceBits
	nodeShift    = sequenceBits
)

// epoch 为 2024-01-01 00:00:00 UTC，41 位时间戳可用约 69 年。
var epoch = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()

// Snowflake 是并发安全的 ID 生成器。
type Snowflake struct {
	mu       sync.Mutex
	node     int64
	lastMs   int64
	sequence int64
	now      func() int64
}

// NewSnowflake 创建节点号为 node 的生成器。
func NewSnowflake(node int64) (*Snowflake, error) {
	if node < 0 || node > maxNode {
		return nil, fmt.Errorf("雪花节点号必须在 [0, %d] 内: %d", maxNode, node)
	}
	return &Snowflake{node: node, now: func() int64 { return time.Now().UnixMilli() }}, nil
}

// Next 返回下一个 ID。时钟回拨时等待追平上次时间，保证单调递增。
func (s *Snowflake) Next() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	ms := s.now()
	if ms < s.lastMs {
		ms = s.lastMs
	}
	if ms == s.lastMs {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			for ms <= s.lastMs {
				time.Sleep(100 * time.Microsecond)
				ms = s.now()
			}
		}
	} else {
		s.sequence = 0
	}
	s.lastMs = ms
	return (ms-epoch)<<timeShift | s.node<<nodeShift | s.sequence
}
