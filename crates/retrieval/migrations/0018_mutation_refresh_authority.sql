-- Existing trusted rows retain their explicit integration authority. OAuth
-- admission never upgrades or reinterprets them, including on digest collision.
ALTER TABLE mutation_refresh ADD COLUMN authority TEXT NOT NULL DEFAULT 'integration';
CREATE INDEX mutation_refresh_authority_pending ON mutation_refresh(workspace_id,authority,state,mutation_digest);
