-- Durable pending membership and immutable claimed snapshots. Event hints remain
-- in the inbox, preserving recovery counters and operator visibility.
CREATE TABLE webhook_page_work (
 workspace_id TEXT NOT NULL,
 subscription_id TEXT NOT NULL,
 page_id TEXT NOT NULL,
 generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 lease_until_ms INTEGER CHECK(lease_until_ms>=0),
 first_ms INTEGER CHECK(first_ms>=0),
 due_ms INTEGER CHECK(due_ms>=first_ms),
 quiet_ms INTEGER NOT NULL CHECK(quiet_ms BETWEEN 1 AND 60000),
 max_delay_ms INTEGER NOT NULL CHECK(max_delay_ms BETWEEN quiet_ms AND 300000),
 PRIMARY KEY(workspace_id,subscription_id,page_id)
);
CREATE TABLE webhook_page_members (
 workspace_id TEXT NOT NULL,
 subscription_id TEXT NOT NULL,
 event_id TEXT NOT NULL,
 page_id TEXT NOT NULL,
 batch INTEGER NOT NULL DEFAULT 0 CHECK(batch>=0),
 PRIMARY KEY(workspace_id,subscription_id,event_id),
 FOREIGN KEY(workspace_id,subscription_id,event_id)
 REFERENCES webhook_inbox(workspace_id,subscription_id,event_id),
 FOREIGN KEY(workspace_id,subscription_id,page_id)
 REFERENCES webhook_page_work(workspace_id,subscription_id,page_id)
);
CREATE INDEX webhook_page_members_batch ON webhook_page_members(workspace_id,subscription_id,page_id,batch);
