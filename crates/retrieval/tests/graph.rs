use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    sync_state::SyncStateError,
};
use notion_knowledge_retrieval::sync_state::{LATEST_SCHEMA_VERSION, SqliteSyncStateStore};
use rusqlite::{Connection, params};

static NEXT_DB: AtomicU64 = AtomicU64::new(1);

fn page(source: &str, target: &str, kind: &str, provenance: &str) -> GraphEdge {
    GraphEdge::new(
        source.into(),
        GraphTarget::Page {
            page_id: target.into(),
        },
        kind.into(),
        provenance.into(),
    )
    .unwrap()
}

fn unresolved(source: &str, reference: &str, kind: &str, provenance: &str) -> GraphEdge {
    GraphEdge::new(
        source.into(),
        GraphTarget::Unresolved {
            reference: reference.into(),
        },
        kind.into(),
        provenance.into(),
    )
    .unwrap()
}

struct TestDb(PathBuf);

impl TestDb {
    fn new() -> Self {
        let id = NEXT_DB.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!(
                "notion-knowledge-graph-{}-{id}",
                std::process::id()
            ))
            .join("state.sqlite3");
        Self(path)
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if let Some(dir) = self.0.parent() {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

#[test]
fn graph_edges_roundtrip_across_restart_and_query_both_directions() {
    let db = TestDb::new();
    let a_to_b = page("a-page", "b-page", "relation", "property:participants");
    let a_to_unknown = unresolved("a-page", "missing-page", "link", "block:111");
    let c_to_b = page("c-page", "b-page", "link", "block:222");

    let store = SqliteSyncStateStore::open(&db.0).unwrap();
    assert_eq!(store.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
    store
        .replace_page_edges("a-page", &[a_to_b.clone(), a_to_unknown.clone()])
        .unwrap();
    store
        .replace_page_edges("c-page", std::slice::from_ref(&c_to_b))
        .unwrap();
    drop(store);

    let reopened = SqliteSyncStateStore::open(&db.0).unwrap();
    assert_eq!(
        reopened.edges_from("a-page").unwrap(),
        vec![a_to_unknown, a_to_b.clone()]
    );
    assert_eq!(reopened.edges_to("b-page").unwrap(), vec![a_to_b, c_to_b]);
    assert!(reopened.edges_to("missing-page").unwrap().is_empty());
}

#[test]
fn replace_is_atomic_scoped_and_can_remove_stale_edges() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let old = page("source", "old", "relation", "property:p");
    let other = page("other", "old", "relation", "property:p");
    store
        .replace_page_edges("source", &[old.clone(), old.clone()])
        .unwrap();
    store.replace_page_edges("other", std::slice::from_ref(&other)).unwrap();
    assert_eq!(store.edges_from("source").unwrap(), vec![old]);

    let updated = page("source", "new", "relation", "property:p");
    store
        .replace_page_edges("source", std::slice::from_ref(&updated))
        .unwrap();
    assert_eq!(store.edges_from("source").unwrap(), vec![updated.clone()]);
    assert_eq!(store.edges_to("old").unwrap(), vec![other]);
    assert_eq!(store.edges_to("new").unwrap(), vec![updated]);

    store.replace_page_edges("source", &[]).unwrap();
    assert!(store.edges_from("source").unwrap().is_empty());
    assert!(store.edges_to("new").unwrap().is_empty());
}

#[test]
fn invalid_input_cannot_erase_existing_graph() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let original = page("owner", "target", "relation", "property:id");
    store
        .replace_page_edges("owner", std::slice::from_ref(&original))
        .unwrap();

    assert_eq!(
        store.replace_page_edges("owner", &[page("intruder", "target", "link", "block:b")]),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(
        store.replace_page_edges(" ", &[]),
        Err(SyncStateError::InvalidInput)
    );
    assert!(store.edges_from("owner\n").is_err());
    assert!(store.edges_to("").is_err());
    assert!(
        GraphEdge::new(
            "owner".into(),
            GraphTarget::Unresolved {
                reference: " ".into()
            },
            "link".into(),
            "block:a".into(),
        )
        .is_err()
    );
    assert!(
        GraphEdge::new(
            "owner".into(),
            GraphTarget::Page {
                page_id: "target".into()
            },
            "".into(),
            "block:a".into(),
        )
        .is_err()
    );

    assert_eq!(store.edges_from("owner").unwrap(), vec![original]);
}

#[test]
fn versioned_schema_enforces_target_invariant_and_has_lookup_indexes() {
    let db = TestDb::new();
    let store = SqliteSyncStateStore::open(&db.0).unwrap();
    assert_eq!(store.schema_version().unwrap(), 7);
    drop(store);

    let connection = Connection::open(&db.0).unwrap();
    let migrated: String = connection
        .query_row(
            "SELECT name FROM schema_migrations WHERE version=7",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(migrated, "knowledge_graph_edges");

    for (target, unresolved) in [(None, None), (Some("page"), Some("unknown"))] {
        assert!(
            connection
                .execute(
                    "INSERT INTO graph_edges (
                    source_page_id, target_page_id, unresolved_reference,
                    relation_type, provenance
                 ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params!["source", target, unresolved, "link", "block:1"],
                )
                .is_err()
        );
    }
    let mut statement = connection
        .prepare("PRAGMA index_list(graph_edges)")
        .unwrap();
    let indexes: Vec<String> = statement
        .query_map([], |row| row.get(1))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    for expected in [
        "graph_edges_by_source",
        "graph_edges_by_target",
        "graph_edges_resolved_unique",
        "graph_edges_unresolved_unique",
    ] {
        assert!(
            indexes.iter().any(|index| index == expected),
            "missing {expected}"
        );
    }
}
