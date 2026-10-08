CREATE TABLE reconciliation_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), fence INTEGER NOT NULL, expires_at INTEGER NOT NULL);
INSERT INTO reconciliation_lease VALUES (1,0,0);
CREATE TABLE reconciliation_runs (run_id TEXT PRIMARY KEY, scope TEXT NOT NULL, fence INTEGER NOT NULL, phase INTEGER NOT NULL CHECK(phase IN (0,1,2)), checkpoint TEXT, next_deadline INTEGER, failure INTEGER CHECK(failure IN(0,1,2,3)));
CREATE UNIQUE INDEX reconciliation_one_active ON reconciliation_runs ((1)) WHERE phase <> 2;
CREATE TABLE reconciliation_inventory (run_id TEXT NOT NULL REFERENCES reconciliation_runs(run_id), page_id TEXT NOT NULL, revision TEXT NOT NULL, action INTEGER NOT NULL CHECK(action IN(0,1,2)), status INTEGER NOT NULL CHECK(status IN(0,1,2)), failure INTEGER CHECK(failure IN(0,1,2,3)), PRIMARY KEY(run_id,page_id));
