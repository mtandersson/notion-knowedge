-- Disposable: the last sync's completeness and drift counts, one row.
CREATE TABLE IF NOT EXISTS inventory_walk (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    run_id TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('full', 'incremental')),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    held INTEGER NOT NULL,
    added INTEGER NOT NULL,
    changed INTEGER NOT NULL,
    moved INTEGER NOT NULL,
    removed INTEGER NOT NULL,
    trashed INTEGER NOT NULL,
    unplaced INTEGER NOT NULL,
    finished_at INTEGER NOT NULL
);
