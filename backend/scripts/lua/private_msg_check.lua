-- private_msg_check.lua
-- 原子性检查：好友关系 + 在线状态 + 黑名单 + 去重 + 消息ID分配
-- KEYS[1] = 发送者ID
-- KEYS[2] = 接收者ID
-- KEYS[3] = 客户端消息ID
-- 返回：{错误码, 消息ID, 是否在线, 是否好友, 是否被拉黑}
--   错误码：0=正常, 1=非好友, 2=被拉黑, 3=重复消息
--   消息ID：分配的全局消息ID（错误时为0）
--   是否在线：接收者在线状态（1=在线, 0=离线）
--   是否好友：好友关系状态（1=是好友, 0=非好友）
--   是否被拉黑：黑名单状态（1=被拉黑, 0=未被拉黑）

local senderID = KEYS[1]
local receiverID = KEYS[2]
local clientMsgID = KEYS[3]

-- 1. 好友关系检查（双向）
local friend1 = redis.call('EXISTS', 'friend:' .. senderID .. ':' .. receiverID)
local friend2 = redis.call('EXISTS', 'friend:' .. receiverID .. ':' .. senderID)
if friend1 == 0 or friend2 == 0 then
    return {1, 0, 0, 0, 0}
end

-- 2. 黑名单检查（接收者是否拉黑了发送者？）
local isBlocked = redis.call('SISMEMBER', 'blacklist:' .. receiverID, senderID)
if isBlocked == 1 then
    return {2, 0, 0, 1, 1}
end

-- 3. 消息去重
local dedupKey = 'msg_dedup:' .. senderID .. ':' .. clientMsgID
local dedup = redis.call('SET', dedupKey, '1', 'EX', 300, 'NX')
if dedup == false then
    return {3, 0, 0, 0, 0}
end

-- 4. 在线状态检查
local isOnline = redis.call('EXISTS', 'online:' .. receiverID)

-- 5. 基于 Redis 服务器时间分配全局消息ID
local redisTime = redis.call('TIME')
local milliseconds = redisTime[1] * 1000 + math.floor(redisTime[2] / 1000)
local sequenceKey = 'msg_id_seq:' .. milliseconds
local sequence = redis.call('INCR', sequenceKey)
if sequence == 1 then
    redis.call('EXPIRE', sequenceKey, 2)
end
if sequence > 999 then
    return redis.error_reply('message ID sequence overflow')
end
local msgID = milliseconds * 1000 + sequence

return {0, msgID, isOnline, 1, 0}
