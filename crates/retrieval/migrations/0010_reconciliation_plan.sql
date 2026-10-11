-- Additive, atomic migration: existing journal/sync/receipt state is untouched.
CREATE TABLE reconciliation_plans (
 run_id TEXT PRIMARY KEY REFERENCES reconciliation_runs(run_id),
 version INTEGER NOT NULL CHECK(version=1),
 document TEXT NOT NULL
);
CREATE TABLE reconciliation_plan_progress (
 run_id TEXT NOT NULL REFERENCES reconciliation_plans(run_id),
 page_id TEXT NOT NULL,
 operation_id TEXT NOT NULL UNIQUE,
 status INTEGER NOT NULL DEFAULT 0 CHECK(status IN(0,1,2)),
 failure TEXT,
 prepared TEXT,
 outcome INTEGER CHECK(outcome IN(0,1,2)),
 PRIMARY KEY(run_id,page_id),
 CHECK((status=1 AND outcome IS NOT NULL AND failure IS NULL) OR
       (status=2 AND outcome IS NULL AND failure IS NOT NULL) OR
       (status=0 AND outcome IS NULL AND failure IS NULL))
);
CREATE TABLE reconciliation_plan_run_failures (
 run_id TEXT NOT NULL REFERENCES reconciliation_plans(run_id),
 failure_id TEXT NOT NULL,
 class TEXT NOT NULL,
 PRIMARY KEY(run_id,failure_id)
);
