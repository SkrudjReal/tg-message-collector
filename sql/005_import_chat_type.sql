-- Default preserves all previous channel-only scopes.
SET @ddl = IF(EXISTS(SELECT 1 FROM information_schema.columns
    WHERE table_schema=DATABASE() AND table_name='scoped_channel_imports' AND column_name='chat_type'),
    'DO 0', 'ALTER TABLE scoped_channel_imports ADD COLUMN chat_type VARCHAR(32) NOT NULL DEFAULT ''channel''');
PREPARE migration FROM @ddl; EXECUTE migration; DEALLOCATE PREPARE migration;
