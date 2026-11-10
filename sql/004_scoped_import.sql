-- Separate checkpoints: previous unscoped jobs and archived messages remain intact.
CREATE TABLE IF NOT EXISTS scoped_channel_imports (
    scope_key CHAR(64) NOT NULL, account_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
    target_username VARCHAR(64) NOT NULL, since_at DATETIME(6) NOT NULL,
    title VARCHAR(255), upper_message_id BIGINT NOT NULL,
    next_max_id BIGINT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'pending',
    processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    inserted BIGINT UNSIGNED NOT NULL DEFAULT 0,
    error_type VARCHAR(128), details_error VARCHAR(128),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY(scope_key,account_id,chat_id),
    UNIQUE KEY ix_scope_target(scope_key,account_id,target_username)
) ENGINE=InnoDB;
