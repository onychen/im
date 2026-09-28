-- inbox_mark_read.lua
-- 将指定会话中所有未读消息标记为已读
-- KEYS[1] = 用户ID
-- KEYS[2] = 会话ID
-- 逻辑：扫描 inbox:{用户ID} ZSet，查找匹配 会话ID 且 readStatus=0 的消息，
--   对每条：ZREM 旧条目 + ZADD 新条目（readStatus 改为 1），
--   然后将 unread:{用户ID} 中对应 会话ID 字段清零
-- 返回：标记为已读的消息数量

local userID = KEYS[1]
local convID = KEYS[2]
local inboxKey = 'inbox:' .. userID
local unreadKey = 'unread:' .. userID

local msgs = redis.call('ZRANGE', inboxKey, 0, -1)
local modified = 0

for i, msg in ipairs(msgs) do
    local decoded = cjson.decode(msg)
    -- 仅修改匹配目标会话且未读的消息
    if decoded.convId == convID and decoded.readStatus == 0 then
        decoded.readStatus = 1
        local newMsg = cjson.encode(decoded)
        -- ZREM 删除旧条目，然后 ZADD 使用相同分数但新的 JSON 值
        redis.call('ZREM', inboxKey, msg)
        redis.call('ZADD', inboxKey, decoded.timestamp, newMsg)
        modified = modified + 1
    end
end

-- 将此会话的未读计数更新为 0
if modified > 0 then
    redis.call('HSET', unreadKey, convID, 0)
end

return modified
