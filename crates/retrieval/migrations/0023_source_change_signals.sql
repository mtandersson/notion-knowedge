-- A separate ordering token avoids relying on Notion's edit timestamp
-- granularity and survives queue pruning. These tables contain no page text.
CREATE TABLE source_signal_clock (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    sequence INTEGER NOT NULL CHECK(sequence >= 0)
);
INSERT INTO source_signal_clock VALUES (1, 0);

CREATE TABLE source_change_signals (
    workspace_id TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence > 0),
    subtree_sequence INTEGER NOT NULL CHECK(subtree_sequence >= 0),
    PRIMARY KEY (workspace_id, entity_type, entity_id)
);

CREATE TABLE source_inventory_watermarks (
    workspace_id TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK(sequence >= 0),
    PRIMARY KEY (workspace_id, scope_id)
);

-- The Lance row is visible to fresh reads only once this pointer is durable.
CREATE TABLE source_document_publications (
    workspace_id TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    page_id TEXT NOT NULL,
    publication TEXT NOT NULL,
    PRIMARY KEY (workspace_id, scope_id, page_id)
);

CREATE TRIGGER source_signal_webhook AFTER INSERT ON webhook_inbox
WHEN NEW.entity_type IN ('page', 'database', 'data_source')
BEGIN
    UPDATE source_signal_clock SET sequence = sequence + 1 WHERE singleton = 1;
    INSERT INTO source_change_signals
        SELECT NEW.workspace_id, NEW.entity_type, NEW.entity_id, sequence,
               CASE WHEN NEW.entity_type <> 'page' OR
                         NEW.event_type IN ('page.deleted', 'page.undeleted', 'page.moved')
                    THEN sequence ELSE 0 END
        FROM source_signal_clock WHERE singleton = 1
        ON CONFLICT(workspace_id, entity_type, entity_id) DO UPDATE SET
            sequence = excluded.sequence,
            subtree_sequence = MAX(subtree_sequence, excluded.subtree_sequence);
END;

CREATE TRIGGER source_signal_mutation AFTER INSERT ON mutation_refresh
BEGIN
    UPDATE source_signal_clock SET sequence = sequence + 1 WHERE singleton = 1;
    INSERT INTO source_change_signals
        SELECT NEW.workspace_id, 'page', NEW.page_id, sequence, 0
        FROM source_signal_clock WHERE singleton = 1
        ON CONFLICT(workspace_id, entity_type, entity_id) DO UPDATE SET
            sequence = excluded.sequence;
END;
