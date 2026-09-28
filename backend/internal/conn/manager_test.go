package conn

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionManager_RegisterGetDelete(t *testing.T) {
	cm := NewConnectionManager()

	// 注册
	client := &ClientConnection{UserID: 1}
	cm.Register(1, client)

	// 获取
	got, ok := cm.Get(1)
	assert.True(t, ok)
	assert.Equal(t, client, got)

	// 删除
	cm.Delete(1)
	_, ok = cm.Get(1)
	assert.False(t, ok)
}

func TestConnectionManager_KickOld(t *testing.T) {
	cm := NewConnectionManager()

	oldClient := &ClientConnection{UserID: 1, SendCh: make(chan []byte, 256), KickCh: make(chan []byte, 1), CloseCh: make(chan struct{})}
	cm.Register(1, oldClient)

	newClient := &ClientConnection{UserID: 1, SendCh: make(chan []byte, 256), KickCh: make(chan []byte, 1), CloseCh: make(chan struct{})}
	cm.KickOld(1, newClient)

	// 旧连接应该收到踢出消息
	select {
	case msg := <-oldClient.KickCh:
		assert.Contains(t, string(msg), "kick")
		assert.Contains(t, string(msg), "new_login")
	default:
		t.Fatal("旧客户端应收到踢出消息")
	}

	// 新连接应该已被注册
	got, ok := cm.Get(1)
	assert.True(t, ok)
	assert.Equal(t, newClient, got)
}

func TestConnectionManager_KickOld_NoExisting(t *testing.T) {
	cm := NewConnectionManager()

	// 没有已存在的连接 — KickOld 应直接注册新连接
	newClient := &ClientConnection{UserID: 1, SendCh: make(chan []byte, 256), KickCh: make(chan []byte, 1), CloseCh: make(chan struct{})}
	cm.KickOld(1, newClient)

	got, ok := cm.Get(1)
	assert.True(t, ok)
	assert.Equal(t, newClient, got)
}
