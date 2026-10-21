-- Source effects and derived refresh have different lifetimes. Expiry of a
-- source owner quarantines its accepted intent; it never proves quiescence.
CREATE TABLE file_refresh_intents (
 workspace_id TEXT NOT NULL,
 mutation_digest TEXT NOT NULL,
 owner BLOB NOT NULL CHECK(length(owner)=32),
 lease_until INTEGER NOT NULL,
 deadline INTEGER NOT NULL,
 phase TEXT NOT NULL CHECK(phase IN ('held','released','discarded')),
 PRIMARY KEY(workspace_id,mutation_digest),
 FOREIGN KEY(workspace_id,mutation_digest) REFERENCES mutation_refresh(workspace_id,mutation_digest)
);
