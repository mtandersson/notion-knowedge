CREATE TABLE page_metadata_identity (page_id TEXT PRIMARY KEY NOT NULL, generation TEXT NOT NULL REFERENCES index_commit_generations(generation), identity TEXT NOT NULL CHECK(length(identity)=64));
