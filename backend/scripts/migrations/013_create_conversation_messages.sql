-- 013: 统一消息表与会话表（私聊、群聊合并，按会话 seq 组织）
-- messages 是消息的事实来源；Redis 只做缓存。

CREATE TABLE IF NOT EXISTS messages (
    conv_id        VARCHAR(64)  NOT NULL COMMENT 'p_{小uid}_{大uid} 或 g_{gid}',
    seq            BIGINT       NOT NULL COMMENT '会话内序号，单调递增，允许空洞',
    msg_id         BIGINT       NOT NULL COMMENT '雪花 ID',
    conv_type      TINYINT      NOT NULL COMMENT '1=私聊 2=群聊',
    sender_id      BIGINT       NOT NULL,
    msg_type       TINYINT      NOT NULL DEFAULT 1,
    content        TEXT         NOT NULL,
    status         TINYINT      NOT NULL DEFAULT 0 COMMENT '0=正常 1=已撤回',
    client_msg_id  VARCHAR(128) NULL,
    created_at     DATETIME(3)  NOT NULL,
    PRIMARY KEY (conv_id, seq),
    UNIQUE KEY uk_msg_id (msg_id),
    UNIQUE KEY uk_client (sender_id, client_msg_id),
    FULLTEXT KEY ft_content (content) WITH PARSER ngram
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS conversations (
    conv_id         VARCHAR(64)  NOT NULL PRIMARY KEY,
    conv_type       TINYINT      NOT NULL,
    max_seq         BIGINT       NOT NULL DEFAULT 0 COMMENT '已落库的最大 seq',
    last_msg_id     BIGINT       NOT NULL DEFAULT 0,
    last_msg_brief  VARCHAR(256) NOT NULL DEFAULT '',
    last_sender_id  BIGINT       NOT NULL DEFAULT 0,
    last_msg_at     DATETIME(3)  NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_conversations (
    user_id     BIGINT      NOT NULL,
    conv_id     VARCHAR(64) NOT NULL,
    read_seq    BIGINT      NOT NULL DEFAULT 0 COMMENT '已读到的 seq，只前进',
    min_seq     BIGINT      NOT NULL DEFAULT 0 COMMENT '小于该值的消息对该用户不可见',
    pinned      TINYINT(1)  NOT NULL DEFAULT 0,
    muted       TINYINT(1)  NOT NULL DEFAULT 0,
    hidden      TINYINT(1)  NOT NULL DEFAULT 0,
    updated_at  DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, conv_id),
    INDEX idx_conv (conv_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS user_deleted_messages (
    user_id  BIGINT      NOT NULL,
    conv_id  VARCHAR(64) NOT NULL,
    seq      BIGINT      NOT NULL,
    PRIMARY KEY (user_id, conv_id, seq)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
