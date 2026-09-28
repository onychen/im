-- group_msg_check.lua
-- 原子性检查：群成员资格 + 禁言状态 + 去重 + 消息ID + 群序列号
-- KEYS[1] = 群ID
-- KEYS[2] = 发送者ID
-- KEYS[3] = 客户端消息ID
-- 返回：{错误码, 消息ID, 群序列号, 是否成员, 是否禁言}
--   错误码：0=正常, 1=非成员, 2=被禁言, 3=重复消息
--   消息ID：分配的全局消息ID（错误时为0）
--   群序列号：分配的群序列号（错误时为0）
--   是否成员：成员状态（1=是成员, 0=非成员）
--   是否禁言：禁言状态（1=被禁言, 0=未禁言）

local groupID = KEYS[1]
local senderID = KEYS[2]
local clientMsgID = KEYS[3]

-- 1. 成员资格检查
local isMember = redis.call('SISMEMBER', 'group_members:' .. groupID, senderID)
if isMember == 0 then
    return {1, 0, 0, 0, 0}
end

-- 2. 禁言状态检查
local memberInfo = redis.call('HGET', 'group_member_info:' .. groupID, senderID)
if memberInfo then
    local info = cjson.decode(memberInfo)
    if info.muted then
        return {2, 0, 0, 1, 1}
    end
end

-- 3. 消息去重
local dedupKey = 'msg_dedup:' .. senderID .. ':' .. clientMsgID
local dedup = redis.call('SET', dedupKey, '1', 'EX', 300, 'NX')
if dedup == false then
    return {3, 0, 0, 0, 0}
end

-- 4. 基于 Redis 服务器时间分配全局消息ID
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

-- 5. 分配群序列号
local groupSeq = redis.call('INCR', 'group_seq:' .. groupID)

return {0, msgID, groupSeq, 1, 0}
