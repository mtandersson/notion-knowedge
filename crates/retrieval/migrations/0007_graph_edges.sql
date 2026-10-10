-- Derived page graph, independent of vectors and full-text search.
-- Exactly one target representation must be present. Unresolved references
-- are never considered page identities, and incoming queries only use page IDs.
CREATE TABLE graph_edges (
    source_page_id TEXT NOT NULL CHECK (length(trim(source_page_id)) > 0),
    target_page_id TEXT CHECK (target_page_id IS NULL OR length(trim(target_page_id)) > 0),
    unresolved_reference TEXT CHECK (unresolved_reference IS NULL OR length(trim(unresolved_reference)) > 0),
    relation_type TEXT NOT NULL CHECK (length(trim(relation_type)) > 0),
    provenance TEXT NOT NULL CHECK (length(trim(provenance)) > 0),
    CHECK ((target_page_id IS NOT NULL) != (unresolved_reference IS NOT NULL))
);

-- Separate partial unique indexes are necessary: a nullable composite UNIQUE
-- would otherwise permit duplicate unresolved or resolved relationships.
CREATE UNIQUE INDEX graph_edges_resolved_unique
    ON graph_edges(source_page_id, target_page_id, relation_type, provenance)
    WHERE target_page_id IS NOT NULL;
CREATE UNIQUE INDEX graph_edges_unresolved_unique
    ON graph_edges(source_page_id, unresolved_reference, relation_type, provenance)
    WHERE unresolved_reference IS NOT NULL;
CREATE INDEX graph_edges_by_source ON graph_edges(source_page_id);
CREATE INDEX graph_edges_by_target ON graph_edges(target_page_id)
    WHERE target_page_id IS NOT NULL;
