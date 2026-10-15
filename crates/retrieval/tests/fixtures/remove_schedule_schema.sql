-- Restore a historical fixture before exercising the older additive migrations.
ALTER TABLE graph_edges DROP COLUMN display_name;
DROP TRIGGER reconciliation_inventory_failure_history;
DROP TRIGGER reconciliation_run_failure_history;
DROP TRIGGER reconciliation_page_failure_history;
DROP TRIGGER reconciliation_plan_failure_history;
DROP TABLE reconciliation_success;
DROP TABLE reconciliation_schedule_events;
DROP TABLE reconciliation_schedules;
DROP TABLE reconciliation_failure_history;
DROP INDEX reconciliation_one_active;
ALTER TABLE reconciliation_runs DROP COLUMN abandoned;
CREATE UNIQUE INDEX reconciliation_one_active ON reconciliation_runs ((1)) WHERE phase<>2;
DELETE FROM schema_migrations WHERE version>=12;
