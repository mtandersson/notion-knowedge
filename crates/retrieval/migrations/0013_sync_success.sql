-- Completion evidence only; never infer success from page timestamps or phase alone.
CREATE TABLE reconciliation_success (
 run_id TEXT PRIMARY KEY REFERENCES reconciliation_runs(run_id),
 table_name TEXT NOT NULL, workspace TEXT NOT NULL, scope TEXT NOT NULL,
 generation TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>=0),
 completed_at_seconds INTEGER NOT NULL CHECK(completed_at_seconds>=0)
);
