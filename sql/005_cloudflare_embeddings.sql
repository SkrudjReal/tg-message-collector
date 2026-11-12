-- Separate vectors preserve the local embedding generation for rollback.
CREATE TABLE IF NOT EXISTS cloudflare_embeddings (
    archive_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    model VARCHAR(191) NOT NULL, text_hash CHAR(64) NOT NULL,
    vector MEDIUMBLOB NOT NULL,
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    CONSTRAINT cf_vector_dimension CHECK (OCTET_LENGTH(vector)=4096)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS embedding_jobs (
    provider VARCHAR(16) NOT NULL PRIMARY KEY,
    upper_archive_id BIGINT UNSIGNED NOT NULL,
    last_archive_id BIGINT UNSIGNED NOT NULL DEFAULT 0,
    processed BIGINT UNSIGNED NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL DEFAULT 'running',
    updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;
