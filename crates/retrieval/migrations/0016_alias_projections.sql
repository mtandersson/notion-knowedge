-- Derived alias completeness and immutable per-operation proposal identity.
CREATE TABLE page_alias_projections (
 page_id TEXT PRIMARY KEY NOT NULL,
 generation TEXT NOT NULL,
 version INTEGER NOT NULL,
 effect TEXT NOT NULL
);
CREATE TABLE alias_operation_proposals (
 operation_id TEXT PRIMARY KEY NOT NULL REFERENCES index_commit_receipts(operation_id),
 effect TEXT NOT NULL
);
