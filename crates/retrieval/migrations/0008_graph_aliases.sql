-- Rebuildable, namespace-limited canonical Notion page aliases (#64).
-- Alias keys are normalized by the core adapter before persisting. The UNIQUE
-- key deliberately retains provenance; a title and property can share an alias.
CREATE TABLE graph_aliases (
    workspace_id TEXT NOT NULL CHECK (length(trim(workspace_id)) > 0),
    root_page_id TEXT NOT NULL CHECK (length(trim(root_page_id)) > 0),
    page_id TEXT NOT NULL CHECK (length(trim(page_id)) > 0),
    alias_key TEXT NOT NULL CHECK (length(trim(alias_key)) > 0),
    display_name TEXT NOT NULL CHECK (length(trim(display_name)) > 0),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    PRIMARY KEY (workspace_id, root_page_id, page_id, alias_key, provenance)
);
CREATE INDEX graph_aliases_by_name
    ON graph_aliases(workspace_id, root_page_id, alias_key, page_id);
CREATE INDEX graph_aliases_by_page
    ON graph_aliases(workspace_id, root_page_id, page_id);
