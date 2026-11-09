-- MySQL 8. Run once as a schema administrator; runtime accounts cannot run DDL.
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT NOT NULL PRIMARY KEY,
    username VARCHAR(64), first_name VARCHAR(255), last_name VARCHAR(255),
    bio TEXT, is_bot BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    bio_checked_at DATETIME(6),
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_users_username (username)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chats (
    chat_id BIGINT NOT NULL PRIMARY KEY,
    type VARCHAR(32) NOT NULL, title VARCHAR(255), username VARCHAR(64),
    description TEXT, members_count INT, is_forum BOOLEAN NOT NULL DEFAULT FALSE,
    linked_chat_id BIGINT, updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    details_checked_at DATETIME(6), INDEX ix_chats_username (username)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS messages (
    archive_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
    chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
    sender_user_id BIGINT, sender_chat_id BIGINT, recipient_user_id BIGINT,
    sent_at DATETIME(6) NOT NULL, edited_at DATETIME(6),
    reply_to_chat_id BIGINT, reply_to_message_id BIGINT, topic_id BIGINT,
    text LONGTEXT, text_hash CHAR(64) NOT NULL,
    media_type VARCHAR(32), media JSON, entities JSON, forward_info JSON,
    media_group_id VARCHAR(64), service_type VARCHAR(64),
    outgoing BOOLEAN NOT NULL DEFAULT FALSE,
    deleted_at DATETIME(6), collected_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (chat_id, message_id),
    INDEX ix_messages_date (sent_at DESC, chat_id DESC, message_id DESC),
    INDEX ix_messages_chat_date (chat_id, sent_at DESC, message_id DESC),
    INDEX ix_messages_sender_date (sender_user_id, sent_at DESC, chat_id DESC, message_id DESC),
    INDEX ix_messages_chat_sender_date (chat_id, sender_user_id, sent_at DESC, message_id DESC),
    INDEX ix_messages_sender_chat (sender_chat_id, sent_at DESC),
    INDEX ix_messages_reply (reply_to_chat_id, reply_to_message_id),
    INDEX ix_messages_topic (chat_id, topic_id, message_id),
    FULLTEXT INDEX ft_messages_text (text)
) ENGINE=InnoDB;

-- Replies and senders may be unavailable: no foreign key requiring a complete archive.
CREATE TABLE IF NOT EXISTS message_revisions (
    revision_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    chat_id BIGINT NOT NULL, message_id BIGINT NOT NULL,
    text LONGTEXT, text_hash CHAR(64) NOT NULL, edited_at DATETIME(6),
    captured_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX ix_revisions_message (chat_id, message_id, revision_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS chat_sync (
    chat_id BIGINT NOT NULL PRIMARY KEY,
    oldest_message_id BIGINT, latest_message_id BIGINT NOT NULL DEFAULT 0,
    history_complete BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS message_embeddings (
    archive_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    model VARCHAR(191) NOT NULL, text_hash CHAR(64) NOT NULL,
    vector MEDIUMBLOB NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS unresolved_deletions (
    message_id BIGINT NOT NULL PRIMARY KEY,
    observed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
