-- 016: 会话 seq 改为 MySQL 持久化号段 + 进程内取号。
-- 升级前暂停旧发送入口并排空 Kafka 消费，禁止新旧分配器同时运行。
-- 保留旧 Redis seq:{convId}，新进程首次申请号段会读取它作为下限。
ALTER TABLE conversations
    ADD COLUMN seq_alloc_max BIGINT NOT NULL DEFAULT 0 COMMENT '已预留的 seq 号段上限';

UPDATE conversations c
LEFT JOIN (SELECT conv_id, MAX(seq) AS max_seq FROM messages GROUP BY conv_id) m
    ON m.conv_id = c.conv_id
SET c.seq_alloc_max = GREATEST(c.seq_alloc_max, c.max_seq, COALESCE(m.max_seq, 0));
