-- The durable reconciliation plan is never written: syncs apply through the
-- page walk. Dropping the tables also drops their failure-history triggers;
-- failure-history rows already copied out of them are retained.
DROP TABLE IF EXISTS reconciliation_retry_proposals;
DROP TABLE IF EXISTS reconciliation_plan_run_failures;
DROP TABLE IF EXISTS reconciliation_plan_progress;
DROP TABLE IF EXISTS reconciliation_plans;
