-- Keep legacy identity-only webhook_events intact: they contain no recoverable hints.
CREATE TABLE webhook_inbox (
 workspace_id TEXT NOT NULL,
 subscription_id TEXT NOT NULL,
 event_id TEXT NOT NULL,
 integration_id TEXT NOT NULL,
 event_timestamp TEXT NOT NULL,
 event_type TEXT NOT NULL,
 entity_id TEXT NOT NULL,
 entity_type TEXT NOT NULL,
 attempt_number INTEGER NOT NULL CHECK(attempt_number > 0),
 state INTEGER NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 3),
 generation INTEGER NOT NULL DEFAULT 0 CHECK(generation >= 0),
 lease_until INTEGER,
 failure INTEGER CHECK(failure BETWEEN 0 AND 3),
 received_at_unix INTEGER NOT NULL DEFAULT (unixepoch()),
 PRIMARY KEY(workspace_id, subscription_id, event_id),
 CHECK ((state = 1 AND lease_until IS NOT NULL) OR (state != 1 AND lease_until IS NULL)),
 CHECK ((state = 3 AND failure IS NOT NULL) OR (state != 3 AND failure IS NULL))
);
CREATE INDEX webhook_inbox_work ON webhook_inbox(state, lease_until, received_at_unix);
