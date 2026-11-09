-- Additive, repeatable migration. No archive rows are removed or fetched from Telegram.
SET @add_source = IF(
    EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema=DATABASE()
            AND table_name='messages' AND column_name='ingest_source'),
    'DO 0',
    'ALTER TABLE messages ADD COLUMN ingest_source VARCHAR(32) NOT NULL DEFAULT ''legacy'''
);
PREPARE source_migration FROM @add_source;
EXECUTE source_migration;
DEALLOCATE PREPARE source_migration;

CREATE TABLE IF NOT EXISTS user_history (
    snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    username VARCHAR(64), first_name VARCHAR(255), last_name VARCHAR(255), bio TEXT,
    bio_checked_at DATETIME(6), source VARCHAR(32) NOT NULL,
    observed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_user_history (user_id, snapshot_id)
) ENGINE=InnoDB;

INSERT INTO user_history (user_id,username,first_name,last_name,bio,bio_checked_at,source)
SELECT u.user_id,u.username,u.first_name,u.last_name,u.bio,u.bio_checked_at,'legacy_baseline'
FROM users u WHERE NOT EXISTS (SELECT 1 FROM user_history h WHERE h.user_id=u.user_id);

CREATE TABLE IF NOT EXISTS message_metrics (
    chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
    views BIGINT UNSIGNED, forwards BIGINT UNSIGNED, reactions JSON,
    observed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (chat_id,message_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS message_metric_history (
    snapshot_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
    views BIGINT UNSIGNED, forwards BIGINT UNSIGNED, reactions JSON,
    source VARCHAR(32) NOT NULL,
    observed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_metric_history (chat_id,message_id,snapshot_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS collector_runs (
    run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    pid INT NOT NULL, started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    heartbeat_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), stopped_at DATETIME(6),
    received_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    saved_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    message_errors BIGINT UNSIGNED NOT NULL DEFAULT 0,
    error_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    metadata_events BIGINT UNSIGNED NOT NULL DEFAULT 0,
    last_message_at DATETIME(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS collector_errors (
    error_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    run_id BIGINT UNSIGNED, stage VARCHAR(32) NOT NULL, error_type VARCHAR(128) NOT NULL,
    chat_id BIGINT, message_id BIGINT, user_id BIGINT,
    occurred_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_errors_run (run_id,error_id), INDEX ix_errors_chat (chat_id,error_id)
) ENGINE=InnoDB;
