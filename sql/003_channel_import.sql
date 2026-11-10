-- Additive, repeatable. No existing archive data is deleted.
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema=DATABASE() AND table_name='messages' AND column_name='source_account_id'),
    'DO 0', 'ALTER TABLE messages ADD COLUMN source_account_id BIGINT NULL');
PREPARE migration FROM @ddl; EXECUTE migration; DEALLOCATE PREPARE migration;
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema=DATABASE() AND table_name='messages' AND column_name='author_signature'),
    'DO 0', 'ALTER TABLE messages ADD COLUMN author_signature VARCHAR(255) NULL');
PREPARE migration FROM @ddl; EXECUTE migration; DEALLOCATE PREPARE migration;

CREATE TABLE IF NOT EXISTS channel_imports (
    account_id BIGINT NOT NULL, chat_id BIGINT NOT NULL,
    title VARCHAR(255), upper_message_id BIGINT NOT NULL,
    next_max_id BIGINT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'pending',
    processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    inserted BIGINT UNSIGNED NOT NULL DEFAULT 0,
    error_type VARCHAR(128), details_error VARCHAR(128),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY(account_id,chat_id)
) ENGINE=InnoDB;

-- Account-specific file IDs and observations also survive an archive-row collision.
CREATE TABLE IF NOT EXISTS message_sources (
    account_id BIGINT NOT NULL, chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
    media JSON, outgoing BOOLEAN NOT NULL, author_signature VARCHAR(255),
    observed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY(account_id,chat_id,message_id),
    INDEX ix_message_sources_message(chat_id,message_id)
) ENGINE=InnoDB;
