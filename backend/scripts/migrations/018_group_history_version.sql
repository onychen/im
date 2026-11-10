ALTER TABLE conversations
    ADD COLUMN history_version BIGINT NOT NULL DEFAULT 0 COMMENT '群历史缓存版本，随消息落库事务更新';
