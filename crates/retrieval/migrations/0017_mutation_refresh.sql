-- Trusted local mutation refresh work is separate from authenticated webhook
-- receipts. A mutation digest admits one page once; restart never repeats writes.
CREATE TABLE mutation_refresh (
 workspace_id TEXT NOT NULL,
 mutation_digest TEXT NOT NULL,
 page_id TEXT NOT NULL,
 state INTEGER NOT NULL DEFAULT 0 CHECK(state BETWEEN 0 AND 3),
 generation INTEGER NOT NULL DEFAULT 0 CHECK(generation>=0),
 attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 3),
 lease_until INTEGER,
 PRIMARY KEY(workspace_id,mutation_digest)
);
CREATE INDEX mutation_refresh_pending ON mutation_refresh(workspace_id,state,mutation_digest);
