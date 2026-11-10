CREATE TABLE IF NOT EXISTS moment_outbox (
    moment_id    BIGINT PRIMARY KEY,
    payload      JSON NOT NULL,
    available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_moment_outbox_due (available_at, moment_id)
) ENGINE=InnoDB;
