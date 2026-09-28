package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis 连接到本地 Redis 实例用于集成测试。
// 默认使用 docker-compose 暴露的 16379，可通过 GOIM_TEST_REDIS_ADDR 覆盖。
func setupTestRedis(t *testing.T) *goredis.Client {
	addr := os.Getenv("GOIM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:16379"
	}
	rdb := goredis.NewClient(&goredis.Options{Addr: addr, DB: 1})
	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		t.Skipf("Redis 在 localhost:6379 不可用: %v", err)
	}
	return rdb
}

// cleanupKeys 删除测试期间创建的所有键，避免跨测试干扰。
func cleanupKeys(t *testing.T, rdb *goredis.Client, ctx context.Context, keys ...string) {
	for _, key := range keys {
		rdb.Del(ctx, key)
	}
}

// ========== 私聊消息检查 ==========

func TestPrivateMsgCheck(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：创建双向好友关系，接收者在线，无黑名单
	rdb.Set(ctx, "friend:1:2", "1", 0)
	rdb.Set(ctx, "friend:2:1", "1", 0)
	rdb.Set(ctx, "online:2", "1", 0)
	// 模拟旧全局计数器被重置；新 ID 生成不应依赖它。
	rdb.Set(ctx, "msg_id_global", "1", 0)

	defer cleanupKeys(t, rdb, ctx, "friend:1:2", "friend:2:1", "online:2", "msg_id_global")

	result, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-001")
	require.NoError(t, err)

	assert.Equal(t, PMErrOK, result.ErrCode, "有效好友关系应成功")
	assert.Greater(t, result.MsgID, int64(0), "msgID 应被分配")
	assert.Greater(t, result.MsgID, int64(1_000_000_000_000), "msgID 应包含毫秒时间前缀")
	assert.Less(t, result.MsgID, int64(9_007_199_254_740_991), "msgID 必须保持 JavaScript 安全整数")
	assert.True(t, result.IsOnline, "接收者应在线")
	assert.True(t, result.IsFriend, "应为好友")
	assert.False(t, result.IsBlocked, "不应被拉黑")

	second, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-002-unique")
	require.NoError(t, err)
	assert.NotEqual(t, result.MsgID, second.MsgID, "连续分配的消息 ID 必须唯一")

	// 清理去重键
	rdb.Del(ctx, "msg_dedup:1:client-msg-001")
	rdb.Del(ctx, "msg_dedup:1:client-msg-002-unique")
}

func TestPrivateMsgCheckNotFriend(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 未创建好友关系键

	defer cleanupKeys(t, rdb, ctx, "msg_id_global")

	result, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-002")
	require.NoError(t, err)

	assert.Equal(t, PMErrNotFriend, result.ErrCode, "非好友应返回 not_friend 错误")
	assert.Equal(t, int64(0), result.MsgID, "失败时 msgID 应为 0")
}

func TestPrivateMsgCheckBlocked(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：好友关系存在，但接收者将发送者加入黑名单
	rdb.Set(ctx, "friend:1:2", "1", 0)
	rdb.Set(ctx, "friend:2:1", "1", 0)
	rdb.SAdd(ctx, "blacklist:2", "1")

	defer cleanupKeys(t, rdb, ctx, "friend:1:2", "friend:2:1", "blacklist:2", "msg_id_global")

	result, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-003")
	require.NoError(t, err)

	assert.Equal(t, PMErrBlocked, result.ErrCode, "被拉黑应返回 blocked 错误")
	assert.True(t, result.IsBlocked, "应被拉黑")
}

func TestPrivateMsgCheckDuplicate(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：好友关系存在，无黑名单
	rdb.Set(ctx, "friend:1:2", "1", 0)
	rdb.Set(ctx, "friend:2:1", "1", 0)
	// 模拟之前已有一条相同 clientMsgID 的消息（去重键已存在）
	rdb.Set(ctx, "msg_dedup:1:client-msg-dup", "1", 0)

	defer cleanupKeys(t, rdb, ctx, "friend:1:2", "friend:2:1", "msg_dedup:1:client-msg-dup", "msg_id_global")

	result, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-dup")
	require.NoError(t, err)

	assert.Equal(t, PMErrDuplicate, result.ErrCode, "重复消息应返回 duplicate 错误")
}

func TestPrivateMsgCheckOffline(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：好友关系存在，接收者离线
	rdb.Set(ctx, "friend:1:2", "1", 0)
	rdb.Set(ctx, "friend:2:1", "1", 0)
	// 无 online:2 键 → 接收者离线

	defer cleanupKeys(t, rdb, ctx, "friend:1:2", "friend:2:1", "msg_id_global")

	result, err := ExecPrivateMsgCheck(rdb, ctx, 1, 2, "client-msg-offline")
	require.NoError(t, err)

	assert.Equal(t, PMErrOK, result.ErrCode, "应成功")
	assert.False(t, result.IsOnline, "接收者应离线")

	// 清理去重键
	rdb.Del(ctx, "msg_dedup:1:client-msg-offline")
}

// ========== 群聊消息检查 ==========

func TestGroupMsgCheck(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：发送者是群成员，未被禁言
	rdb.SAdd(ctx, "group_members:5", "1")
	// memberInfo 中 muted=false
	memberInfo, _ := json.Marshal(map[string]interface{}{"role": "member", "muted": false})
	rdb.HSet(ctx, "group_member_info:5", "1", memberInfo)

	defer cleanupKeys(t, rdb, ctx, "group_members:5", "group_member_info:5", "group_seq:5", "msg_id_global")

	result, err := ExecGroupMsgCheck(rdb, ctx, 5, 1, "client-grp-001")
	require.NoError(t, err)

	assert.Equal(t, GMErrOK, result.ErrCode, "应成功")
	assert.Greater(t, result.MsgID, int64(0), "msgID 应被分配")
	assert.Greater(t, result.MsgID, int64(1_000_000_000_000), "msgID 应包含毫秒时间前缀")
	assert.Less(t, result.MsgID, int64(9_007_199_254_740_991), "msgID 必须保持 JavaScript 安全整数")
	assert.Greater(t, result.GroupSeq, int64(0), "groupSeq 应被分配")
	assert.True(t, result.IsMember, "应为群成员")
	assert.False(t, result.IsMuted, "不应被禁言")

	// 清理去重键
	rdb.Del(ctx, "msg_dedup:1:client-grp-001")
}

func TestGroupMsgCheckNotMember(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 发送者不在 group_members 中
	defer cleanupKeys(t, rdb, ctx, "msg_id_global")

	result, err := ExecGroupMsgCheck(rdb, ctx, 5, 99, "client-grp-notmember")
	require.NoError(t, err)

	assert.Equal(t, GMErrNotMember, result.ErrCode, "非群成员应返回 not_member 错误")
	assert.Equal(t, int64(0), result.MsgID, "失败时 msgID 应为 0")
}

func TestGroupMsgCheckMuted(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：发送者是成员但被禁言
	rdb.SAdd(ctx, "group_members:5", "1")
	memberInfo, _ := json.Marshal(map[string]interface{}{"role": "member", "muted": true})
	rdb.HSet(ctx, "group_member_info:5", "1", memberInfo)

	defer cleanupKeys(t, rdb, ctx, "group_members:5", "group_member_info:5", "msg_id_global")

	result, err := ExecGroupMsgCheck(rdb, ctx, 5, 1, "client-grp-muted")
	require.NoError(t, err)

	assert.Equal(t, GMErrMuted, result.ErrCode, "被禁言应返回 muted 错误")
	assert.True(t, result.IsMuted, "应被禁言")
}

func TestGroupMsgCheckMutedNoInfo(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	// 准备：发送者是成员，无 memberInfo 哈希条目（未禁言）
	rdb.SAdd(ctx, "group_members:5", "1")
	// 无 group_member_info 条目 → 未禁言

	defer cleanupKeys(t, rdb, ctx, "group_members:5", "group_seq:5", "msg_id_global")

	result, err := ExecGroupMsgCheck(rdb, ctx, 5, 1, "client-grp-noinfo")
	require.NoError(t, err)

	assert.Equal(t, GMErrOK, result.ErrCode, "无禁言信息时应成功")
	assert.False(t, result.IsMuted, "无信息时不应被禁言")

	// 清理去重键
	rdb.Del(ctx, "msg_dedup:1:client-grp-noinfo")
}

// ========== 收件箱标记已读 ==========

func TestInboxMarkRead(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	userID := int64(100)
	convID := "p_100_200"
	inboxKey := fmt.Sprintf("inbox:%d", userID)
	unreadKey := fmt.Sprintf("unread:%d", userID)

	// 准备：向收件箱添加 3 条消息，2 条未读 (readStatus=0) 属于目标会话，1 条已读
	ts := time.Now().UnixMilli()
	msg1 := map[string]interface{}{
		"msgId":      1,
		"convId":     convID,
		"convType":   1,
		"fromId":     200,
		"toId":       100,
		"msgType":    1,
		"content":    "hello",
		"readStatus": 0, // 未读
		"timestamp":  ts,
	}
	msg2 := map[string]interface{}{
		"msgId":      2,
		"convId":     convID,
		"convType":   1,
		"fromId":     200,
		"toId":       100,
		"msgType":    1,
		"content":    "world",
		"readStatus": 0, // 未读
		"timestamp":  ts + 1,
	}
	msg3 := map[string]interface{}{
		"msgId":      3,
		"convId":     "p_100_300", // 不同会话
		"convType":   1,
		"fromId":     300,
		"toId":       100,
		"msgType":    1,
		"content":    "other conv",
		"readStatus": 0, // 未读但属于不同 convID — 不应被标记
		"timestamp":  ts + 2,
	}

	msg1JSON, _ := json.Marshal(msg1)
	msg2JSON, _ := json.Marshal(msg2)
	msg3JSON, _ := json.Marshal(msg3)

	rdb.ZAdd(ctx, inboxKey, goredis.Z{Score: float64(ts), Member: string(msg1JSON)})
	rdb.ZAdd(ctx, inboxKey, goredis.Z{Score: float64(ts + 1), Member: string(msg2JSON)})
	rdb.ZAdd(ctx, inboxKey, goredis.Z{Score: float64(ts + 2), Member: string(msg3JSON)})
	rdb.HSet(ctx, unreadKey, convID, 2) // p_100_200 有 2 条未读

	defer cleanupKeys(t, rdb, ctx, inboxKey, unreadKey)

	count, err := ExecInboxMarkRead(rdb, ctx, userID, convID)
	require.NoError(t, err)

	assert.Equal(t, int64(2), count, "应将 2 条消息标记为已读")

	// 验证未读计数器已重置
	unreadVal, _ := rdb.HGet(ctx, unreadKey, convID).Int64()
	assert.Equal(t, int64(0), unreadVal, "未读计数器应为 0")

	// 验证不同会话的消息仍为未读
	// （可以通过扫描收件箱中 p_100_300 里 readStatus=0 的消息来检查）
	remaining, _ := rdb.ZRange(ctx, inboxKey, 0, -1).Result()
	foundUnreadOther := false
	for _, m := range remaining {
		var parsed map[string]interface{}
		json.Unmarshal([]byte(m), &parsed)
		if parsed["convId"] == "p_100_300" {
			rs, _ := strconv.Atoi(fmt.Sprintf("%v", parsed["readStatus"]))
			if rs == 0 {
				foundUnreadOther = true
			}
		}
	}
	assert.True(t, foundUnreadOther, "其他会话的消息应保持未读")
}

func TestInboxMarkReadNoUnread(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	userID := int64(100)
	convID := "p_100_200"
	inboxKey := fmt.Sprintf("inbox:%d", userID)

	// 准备：添加一条已读消息
	ts := time.Now().UnixMilli()
	msg := map[string]interface{}{
		"msgId":      1,
		"convId":     convID,
		"convType":   1,
		"fromId":     200,
		"toId":       100,
		"msgType":    1,
		"content":    "already read",
		"readStatus": 1, // 已读
		"timestamp":  ts,
	}
	msgJSON, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, inboxKey, goredis.Z{Score: float64(ts), Member: string(msgJSON)})

	defer cleanupKeys(t, rdb, ctx, inboxKey)

	count, err := ExecInboxMarkRead(rdb, ctx, userID, convID)
	require.NoError(t, err)

	assert.Equal(t, int64(0), count, "所有消息已读时应标记 0 条")
}

// ========== 撤回消息 ==========

func TestRevokeMsgPrivate(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	userID := int64(100) // 原消息发送者
	receiverID := int64(200)
	convID := "p_100_200"
	msgID := int64(12345)
	senderInboxKey := fmt.Sprintf("inbox:%d", userID)
	receiverInboxKey := fmt.Sprintf("inbox:%d", receiverID)
	rdb.Del(ctx, senderInboxKey, receiverInboxKey)

	// 准备：向收件箱添加一条最近发送的消息
	ts := time.Now().UnixMilli() - 30000 // 30 秒前（在 2 分钟窗口内）
	msg := map[string]interface{}{
		"msgId":      msgID,
		"convId":     convID,
		"convType":   1,
		"fromId":     100,
		"toId":       200,
		"msgType":    1,
		"content":    "hello",
		"readStatus": 0,
		"timestamp":  ts,
	}
	msgJSON, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, senderInboxKey, goredis.Z{Score: float64(ts), Member: string(msgJSON)})
	rdb.ZAdd(ctx, receiverInboxKey, goredis.Z{Score: float64(ts), Member: string(msgJSON)})

	// 撤回消息 JSON (msgType=6)
	revokeMsg := map[string]interface{}{
		"msgId":      msgID,
		"convId":     convID,
		"convType":   1,
		"fromId":     100,
		"toId":       200,
		"msgType":    6, // 撤回类型
		"content":    "消息已撤回",
		"readStatus": 0,
		"timestamp":  ts,
	}
	revokeJSON, _ := json.Marshal(revokeMsg)

	defer cleanupKeys(t, rdb, ctx, senderInboxKey, receiverInboxKey)

	ok, err := ExecRevokeMsg(rdb, ctx, userID, convID, msgID, string(revokeJSON), time.Now().UnixMilli())
	require.NoError(t, err)

	assert.True(t, ok, "2 分钟内撤回应成功")

	// 双方刷新时都从各自收件箱读取，因此两个副本都必须被替换。
	for _, inboxKey := range []string{senderInboxKey, receiverInboxKey} {
		remaining, _ := rdb.ZRange(ctx, inboxKey, 0, -1).Result()
		if assert.Len(t, remaining, 1, "收件箱应恰好包含 1 条消息") {
			var parsed map[string]interface{}
			json.Unmarshal([]byte(remaining[0]), &parsed)
			mt, _ := strconv.Atoi(fmt.Sprintf("%v", parsed["msgType"]))
			assert.Equal(t, 6, mt, "消息类型应为撤回 (6)")
			assert.Equal(t, ts, int64(parsed["timestamp"].(float64)), "撤回不得改变同步游标时间戳")
		}
	}
}

func TestRevokeMsgTooLate(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	userID := int64(200)
	convID := "p_100_200"
	msgID := int64(12346)
	inboxKey := fmt.Sprintf("inbox:%d", userID)

	// 准备：添加一条发送时间超过 2 分钟的消息
	ts := time.Now().UnixMilli() - 180000 // 3 分钟前
	msg := map[string]interface{}{
		"msgId":      msgID,
		"convId":     convID,
		"convType":   1,
		"fromId":     100,
		"toId":       200,
		"msgType":    1,
		"content":    "hello",
		"readStatus": 0,
		"timestamp":  ts,
	}
	msgJSON, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, inboxKey, goredis.Z{Score: float64(ts), Member: string(msgJSON)})

	defer cleanupKeys(t, rdb, ctx, inboxKey)

	ok, err := ExecRevokeMsg(rdb, ctx, userID, convID, msgID, "\"revoked\"", time.Now().UnixMilli())
	require.NoError(t, err)

	assert.False(t, ok, "消息超过 2 分钟时撤回应失败")
}

func TestRevokeMsgNotFound(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	userID := int64(200)
	convID := "p_100_200"
	msgID := int64(99999) // 不存在的消息
	inboxKey := fmt.Sprintf("inbox:%d", userID)

	// 收件箱为空

	defer cleanupKeys(t, rdb, ctx, inboxKey)

	ok, err := ExecRevokeMsg(rdb, ctx, userID, convID, msgID, "\"revoked\"", time.Now().UnixMilli())
	require.NoError(t, err)

	assert.False(t, ok, "消息不存在时撤回应失败")
}

func TestRevokeMsgGroup(t *testing.T) {
	rdb := setupTestRedis(t)
	ctx := context.Background()

	convID := "g_5"
	groupID := "5"
	msgID := int64(12347)
	outboxKey := fmt.Sprintf("outbox:%s", groupID)
	rdb.Del(ctx, outboxKey)

	// 对于群聊撤回，userID 仅作为参数，但 ZSet 键为 outbox:{groupID}
	ts := time.Now().UnixMilli() - 10000 // 10 秒前
	msg := map[string]interface{}{
		"msgId":     msgID,
		"groupId":   5,
		"convId":    convID,
		"convType":  2,
		"fromId":    100,
		"msgType":   1,
		"content":   "hello group",
		"timestamp": ts,
		"groupSeq":  42,
	}
	msgJSON, _ := json.Marshal(msg)
	rdb.ZAdd(ctx, outboxKey, goredis.Z{Score: float64(ts), Member: string(msgJSON)})

	revokeMsg := map[string]interface{}{
		"msgId":     msgID,
		"groupId":   5,
		"convId":    convID,
		"convType":  2,
		"fromId":    100,
		"msgType":   6,
		"content":   "消息已撤回",
		"timestamp": ts,
		"groupSeq":  42,
	}
	revokeJSON, _ := json.Marshal(revokeMsg)

	defer cleanupKeys(t, rdb, ctx, outboxKey)

	// 仅原消息发送者可以撤回群消息。
	ok, err := ExecRevokeMsg(rdb, ctx, 100, convID, msgID, string(revokeJSON), time.Now().UnixMilli())
	require.NoError(t, err)

	assert.True(t, ok, "群聊消息在 2 分钟内撤回应成功")

	// 验证撤回后的消息在发件箱中
	remaining, _ := rdb.ZRange(ctx, outboxKey, 0, -1).Result()
	assert.Equal(t, 1, len(remaining), "发件箱应恰好包含 1 条消息")
	var parsed map[string]interface{}
	json.Unmarshal([]byte(remaining[0]), &parsed)
	mt, _ := strconv.Atoi(fmt.Sprintf("%v", parsed["msgType"]))
	assert.Equal(t, 6, mt, "消息类型应为撤回 (6)")
}
