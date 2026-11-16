-- Disposable: dropping this table only forces the next full walk.
-- No title or content columns; ids, timestamps and outcomes only.
CREATE TABLE IF NOT EXISTS page_inventory (
    page_id TEXT PRIMARY KEY NOT NULL,
    parent_kind TEXT NOT NULL CHECK (parent_kind IN ('root', 'page', 'data_source')),
    parent_id TEXT,
    database_id TEXT,
    -- The page whose Markdown tagged the database; set only for rows.
    database_parent_id TEXT,
    root_id TEXT NOT NULL,
    last_edited_time TEXT NOT NULL,
    in_trash INTEGER NOT NULL CHECK (in_trash IN (0, 1)),
    walk_run TEXT NOT NULL,
    indexed_revision TEXT,
    last_fetched_at INTEGER,
    outcome TEXT CHECK (outcome IN ('indexed', 'skipped', 'failed')),
    outcome_detail TEXT
);
