CREATE TABLE page_sync_state (
    page_id TEXT PRIMARY KEY NOT NULL,
    content_hash TEXT,
    notion_last_edited_ms INTEGER,
    synced_at_ms INTEGER NOT NULL,
    tombstoned_at_ms INTEGER
) STRICT;

CREATE TABLE crawl_checkpoints (
    checkpoint_key TEXT PRIMARY KEY NOT NULL,
    checkpoint_value TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE webhook_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    received_at_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE index_versions (
    index_name TEXT PRIMARY KEY NOT NULL,
    version TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
) STRICT;
