-- Scoped durable scheduling; existing inventory and receipts remain untouched.
ALTER TABLE reconciliation_runs ADD COLUMN abandoned INTEGER NOT NULL DEFAULT 0 CHECK(abandoned IN(0,1));
DROP INDEX reconciliation_one_active;
CREATE UNIQUE INDEX reconciliation_one_active ON reconciliation_runs ((1)) WHERE phase<>2 AND abandoned=0;
CREATE TABLE reconciliation_schedules (
 schedule_id TEXT PRIMARY KEY, table_name TEXT NOT NULL, workspace TEXT NOT NULL,
 scope TEXT NOT NULL, generation TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>=0),
 interval_seconds INTEGER NOT NULL CHECK(interval_seconds BETWEEN 60 AND 2592000),
 max_attempts INTEGER NOT NULL CHECK(max_attempts BETWEEN 1 AND 16),
 backoff_seconds INTEGER NOT NULL CHECK(backoff_seconds BETWEEN 1 AND 86400),
 max_backoff_seconds INTEGER NOT NULL CHECK(max_backoff_seconds BETWEEN backoff_seconds AND 604800),
 next_deadline INTEGER NOT NULL CHECK(next_deadline>=0),
 active_run TEXT UNIQUE REFERENCES reconciliation_runs(run_id),
 status INTEGER NOT NULL DEFAULT 0 CHECK(status IN(0,1,2,3)),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0 AND attempts<=max_attempts),
 cycle INTEGER NOT NULL DEFAULT 0 CHECK(cycle>=0),
 UNIQUE(table_name,workspace,scope,generation,epoch)
);
CREATE TABLE reconciliation_schedule_events (
 sequence INTEGER PRIMARY KEY, schedule_id TEXT NOT NULL REFERENCES reconciliation_schedules(schedule_id),
 cycle INTEGER NOT NULL, run_id TEXT, at_seconds INTEGER NOT NULL CHECK(at_seconds>=0),
 kind INTEGER NOT NULL CHECK(kind BETWEEN 0 AND 7), failure INTEGER CHECK(failure IN(0,1,2,3)),
 attempt INTEGER NOT NULL, deadline INTEGER NOT NULL, event_id TEXT,
 UNIQUE(schedule_id,cycle,event_id)
);
CREATE TABLE reconciliation_failure_history (
 sequence INTEGER PRIMARY KEY, run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id),
 page_id TEXT, kind INTEGER NOT NULL, class TEXT NOT NULL
);
INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class)
 SELECT run_id,page_id,0,CAST(failure AS TEXT) FROM reconciliation_inventory WHERE status=2;
INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class)
 SELECT run_id,NULL,1,CAST(failure AS TEXT) FROM reconciliation_runs WHERE failure IS NOT NULL;
INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class)
 SELECT run_id,page_id,2,failure FROM reconciliation_plan_progress WHERE status=2;
INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class)
 SELECT run_id,NULL,3,class FROM reconciliation_plan_run_failures;
CREATE TRIGGER reconciliation_inventory_failure_history AFTER UPDATE OF status,failure ON reconciliation_inventory
 WHEN NEW.status=2 AND (OLD.status<>2 OR OLD.failure IS NOT NEW.failure)
 BEGIN INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class) VALUES(NEW.run_id,NEW.page_id,0,CAST(NEW.failure AS TEXT)); END;
CREATE TRIGGER reconciliation_run_failure_history AFTER UPDATE OF failure ON reconciliation_runs
 WHEN NEW.failure IS NOT NULL AND OLD.failure IS NOT NEW.failure
 BEGIN INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class) VALUES(NEW.run_id,NULL,1,CAST(NEW.failure AS TEXT)); END;
CREATE TRIGGER reconciliation_page_failure_history AFTER UPDATE OF status,failure ON reconciliation_plan_progress
 WHEN NEW.status=2 AND (OLD.status<>2 OR OLD.failure IS NOT NEW.failure)
 BEGIN INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class) VALUES(NEW.run_id,NEW.page_id,2,NEW.failure); END;
CREATE TRIGGER reconciliation_plan_failure_history AFTER INSERT ON reconciliation_plan_run_failures
 BEGIN INSERT INTO reconciliation_failure_history(run_id,page_id,kind,class) VALUES(NEW.run_id,NULL,3,NEW.class); END;
