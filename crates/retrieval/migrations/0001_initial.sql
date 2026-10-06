CREATE TABLE page_sync_state (
    page_id TEXT PRIMARY KEY NOT NULL CHECK (length(trim(page_id)) > 0),
    content_hash TEXT,
    last_edited_time TEXT,
    tombstoned INTEGER NOT NULL CHECK (tombstoned IN (0, 1)),
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
    CHECK (
        (tombstoned = 1 AND content_hash IS NULL)
        OR
        (
            tombstoned = 0
            AND content_hash IS NOT NULL
            AND length(trim(content_hash)) > 0
        )
    )
);

CREATE TABLE crawl_checkpoints (
    checkpoint_key TEXT PRIMARY KEY NOT NULL CHECK (length(trim(checkpoint_key)) > 0),
    cursor TEXT,
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE webhook_events (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(trim(event_id)) > 0),
    received_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE index_versions (
    index_name TEXT PRIMARY KEY NOT NULL CHECK (length(trim(index_name)) > 0),
    version TEXT NOT NULL CHECK (length(trim(version)) > 0),
    updated_at_unix INTEGER NOT NULL DEFAULT (unixepoch())
);
