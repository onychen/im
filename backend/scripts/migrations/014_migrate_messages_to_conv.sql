-- 014: 把旧的 private_messages / group_messages 迁到 messages，并初始化会话表。
-- 依赖 MySQL 8（窗口函数）。可重复执行：全部使用 INSERT IGNORE / ON DUPLICATE KEY。
-- 执行前先停服，避免新旧两套写入交叉。旧表保留，确认无误后由 015 手工删除。

-- 1. 私聊：按会话、按 (created_at, id) 补连续 seq
INSERT IGNORE INTO messages (conv_id, seq, msg_id, conv_type, sender_id, msg_type, content, status, client_msg_id, created_at)
SELECT t.conv_id,
       ROW_NUMBER() OVER (PARTITION BY t.conv_id ORDER BY t.created_at, t.id) AS seq,
       t.id, 1, t.sender_id, t.msg_type, t.content, 0, t.client_msg_id, t.created_at
FROM (
    SELECT CONCAT('p_', LEAST(sender_id, receiver_id), '_', GREATEST(sender_id, receiver_id)) AS conv_id,
           id, sender_id, msg_type, content, client_msg_id, created_at
    FROM private_messages
) t;

-- 2. 群聊：沿用原 group_seq
INSERT IGNORE INTO messages (conv_id, seq, msg_id, conv_type, sender_id, msg_type, content, status, client_msg_id, created_at)
SELECT CONCAT('g_', group_id), group_seq, id, 2, sender_id, msg_type, content, 0, client_msg_id, created_at
FROM group_messages;

-- 3. 撤回状态
UPDATE messages m
JOIN msg_revoked r ON r.msg_id = m.msg_id
SET m.status = 1;

-- 4. 会话元数据：取每个会话 seq 最大的一条
INSERT INTO conversations (conv_id, conv_type, max_seq, last_msg_id, last_msg_brief, last_sender_id, last_msg_at)
SELECT m.conv_id, m.conv_type, m.seq, m.msg_id,
       CASE
           WHEN m.status = 1 THEN '[消息已撤回]'
           WHEN m.msg_type = 2 THEN '[图片]'
           WHEN m.msg_type = 3 THEN '[视频]'
           WHEN m.msg_type = 4 THEN '[文件]'
           ELSE LEFT(m.content, 64)
       END,
       m.sender_id, m.created_at
FROM messages m
JOIN (SELECT conv_id, MAX(seq) AS max_seq FROM messages GROUP BY conv_id) x
  ON x.conv_id = m.conv_id AND x.max_seq = m.seq
ON DUPLICATE KEY UPDATE
    max_seq        = VALUES(max_seq),
    last_msg_id    = VALUES(last_msg_id),
    last_msg_brief = VALUES(last_msg_brief),
    last_sender_id = VALUES(last_sender_id),
    last_msg_at    = VALUES(last_msg_at);

-- 5. 还没有消息的好友会话、群会话
INSERT IGNORE INTO conversations (conv_id, conv_type)
SELECT DISTINCT CONCAT('p_', LEAST(user_id, friend_id), '_', GREATEST(user_id, friend_id)), 1
FROM friendships;

INSERT IGNORE INTO conversations (conv_id, conv_type)
SELECT CONCAT('g_', id), 2 FROM `groups`;

-- 6. 用户会话：好友双方、有过私聊的双方、群成员
INSERT IGNORE INTO user_conversations (user_id, conv_id)
SELECT user_id, CONCAT('p_', LEAST(user_id, friend_id), '_', GREATEST(user_id, friend_id))
FROM friendships;

INSERT IGNORE INTO user_conversations (user_id, conv_id)
SELECT sender_id, CONCAT('p_', LEAST(sender_id, receiver_id), '_', GREATEST(sender_id, receiver_id))
FROM private_messages
UNION
SELECT receiver_id, CONCAT('p_', LEAST(sender_id, receiver_id), '_', GREATEST(sender_id, receiver_id))
FROM private_messages;

INSERT IGNORE INTO user_conversations (user_id, conv_id)
SELECT user_id, CONCAT('g_', group_id) FROM group_members;

-- 7. 旧的未读状态在 Redis 中，无法可靠迁移：迁移时把已有消息全部视为已读。
UPDATE user_conversations uc
JOIN conversations c ON c.conv_id = uc.conv_id
SET uc.read_seq = c.max_seq
WHERE uc.read_seq < c.max_seq;

-- 8. Redis 中的 seq:{convId} 不需要手工初始化：
--    首次分配时发现 key 不存在，会用 MAX(messages.seq) 校正后再 INCR。
--    旧的 inbox:* / outbox:* / conv_list:* / unread:* / group_read_pos:* / group_seq:* /
--    user_groups:* / msg_stream 等 key 可在切换后用 SCAN + DEL 清理。
