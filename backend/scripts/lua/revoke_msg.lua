-- revoke_msg.lua
-- 在2分钟内撤回消息：通过 msgID 查找，检查时间戳，替换为撤回版本
-- KEYS[1] = 用户ID（执行撤回操作的用户，用于定位收件箱/发件箱）
-- KEYS[2] = 会话ID（会话ID，例如 p_100_200 或 g_5）
-- KEYS[3] = 消息ID（要撤回的消息ID）
-- ARGV[1] = 撤回消息JSON（替换消息，msgType=6）
-- ARGV[2] = 当前时间戳（毫秒，用于2分钟检查）
-- 返回：1（成功）或 0（失败 - 未找到或超时）

local userID = KEYS[1]
local convID = KEYS[2]
local msgID = tonumber(KEYS[3])
local revokeMsgJSON = ARGV[1]
local nowTimestamp = tonumber(ARGV[2])

-- 判断是私聊还是群聊会话
local isGroup = false
if string.sub(convID, 1, 2) == 'g_' then
    isGroup = true
end

local zsetKey
if isGroup then
    -- 群聊：outbox:{群ID}
    local groupID = string.sub(convID, 3)
    zsetKey = 'outbox:' .. groupID
else
    -- 私聊：inbox:{用户ID}
    zsetKey = 'inbox:' .. userID
end

-- 扫描 ZSet 按 msgID 查找消息
local msgs = redis.call('ZRANGE', zsetKey, 0, -1)
for i, msg in ipairs(msgs) do
    local decoded = cjson.decode(msg)
    if decoded.msgId == msgID then
        -- 权限检查：仅原始发送者可以撤回
        if decoded.fromId ~= tonumber(userID) then
            return 0 -- 未授权：请求者不是发送者
        end

        -- 检查2分钟时间窗口
        if nowTimestamp - decoded.timestamp > 120000 then
            return 0 -- 已超时，无法撤回
        end

        -- ZREM 删除原始消息
        redis.call('ZREM', zsetKey, msg)

        -- ZADD 添加撤回替换消息（相同分数以保持位置）
        redis.call('ZADD', zsetKey, decoded.timestamp, revokeMsgJSON)

        return 1 -- 成功
    end
end

return 0 -- 未找到消息
