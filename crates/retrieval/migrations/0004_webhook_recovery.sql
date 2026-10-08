-- Existing running claims consume one attempt; legacy failed rows remain inert.
ALTER TABLE webhook_inbox ADD COLUMN cycle_attempts INTEGER NOT NULL DEFAULT 0 CHECK(cycle_attempts >= 0);
ALTER TABLE webhook_inbox ADD COLUMN lifetime_attempts INTEGER NOT NULL DEFAULT 0 CHECK(lifetime_attempts >= cycle_attempts);
ALTER TABLE webhook_inbox ADD COLUMN retry_at INTEGER CHECK(retry_at >= 0);
ALTER TABLE webhook_inbox ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5 CHECK(max_attempts BETWEEN 1 AND 100);
ALTER TABLE webhook_inbox ADD COLUMN base_seconds INTEGER NOT NULL DEFAULT 5 CHECK(base_seconds BETWEEN 1 AND 86400);
ALTER TABLE webhook_inbox ADD COLUMN max_seconds INTEGER NOT NULL DEFAULT 300 CHECK(max_seconds BETWEEN base_seconds AND 86400);
UPDATE webhook_inbox SET cycle_attempts=CASE WHEN generation>0 THEN 1 ELSE 0 END,lifetime_attempts=generation;

ALTER TABLE webhook_inbox ADD COLUMN last_failure INTEGER CHECK(last_failure BETWEEN 0 AND 3);
UPDATE webhook_inbox SET last_failure=failure;
