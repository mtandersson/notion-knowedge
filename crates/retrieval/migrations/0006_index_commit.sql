CREATE TABLE index_commit_state_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), identity TEXT NOT NULL CHECK(length(identity)=32));
INSERT INTO index_commit_state_identity VALUES(1,lower(hex(randomblob(16))));
CREATE TABLE index_commit_binding (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 index_device TEXT NOT NULL, index_inode TEXT NOT NULL,
 state_device TEXT NOT NULL, state_inode TEXT NOT NULL,
 table_name TEXT NOT NULL, workspace TEXT NOT NULL,
 scope TEXT NOT NULL, generation TEXT NOT NULL, epoch INTEGER NOT NULL CHECK(epoch>=0)
);
CREATE TABLE index_commit_generations (
 generation TEXT PRIMARY KEY NOT NULL, scope TEXT NOT NULL, epoch INTEGER UNIQUE NOT NULL CHECK(epoch>=0)
);
CREATE TABLE index_commit_receipts (
 operation_id TEXT PRIMARY KEY NOT NULL,
 generation TEXT NOT NULL REFERENCES index_commit_generations(generation),
 page_id TEXT NOT NULL, revision TEXT NOT NULL,
 action INTEGER NOT NULL CHECK(action IN(0,1,2)),
 content_hash TEXT, last_edited_time TEXT,
 status INTEGER NOT NULL CHECK(status IN(0,1,2,3)),
 failure INTEGER CHECK(failure IN(0,1,2,3)),
 attempts INTEGER NOT NULL CHECK(attempts>0),
 CHECK((action=1 AND content_hash IS NULL) OR (action<>1 AND content_hash IS NOT NULL)),
 CHECK((status=2 AND failure IS NOT NULL) OR (status=3 AND failure=2) OR (status IN(0,1) AND failure IS NULL))
);
