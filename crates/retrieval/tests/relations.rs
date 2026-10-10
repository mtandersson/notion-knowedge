use std::collections::{BTreeMap, BTreeSet};

use notion_knowledge_core::{
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    indexed::{IndexedMetadata, PropertyValue, SourceMetadata},
    relations::relation_edges,
    sync_state::SyncStateError,
};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;

fn metadata(properties: BTreeMap<String, PropertyValue>) -> IndexedMetadata {
    IndexedMetadata {
        page_id: "source".into(),
        block_id: None,
        url: "https://notion.so/source".into(),
        title: "Test".into(),
        heading_path: vec![],
        last_edited_time: "2026-10-10T17:00:00Z".into(),
        source: SourceMetadata {
            workspace_id: "trusted-workspace".into(),
            root_page_id: "trusted-root".into(),
            database_id: None,
            data_source_id: None,
        },
        properties,
    }
}

fn pages() -> BTreeSet<String> {
    ["source", "inside-a", "inside-b"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

fn relations(targets: &[&str]) -> BTreeMap<String, PropertyValue> {
    BTreeMap::from([
        (
            "project_relation_id".into(),
            PropertyValue::PageIds(targets.iter().map(|s| (*s).to_owned()).collect()),
        ),
        (
            "unrelated_people".into(),
            PropertyValue::PersonIds(vec!["person-secret".into()]),
        ),
    ])
}

#[test]
fn resolved_unresolved_multirelation_and_property_provenance() {
    let mut props = relations(&["inside-b", "outside", "inside-a", "inside-a"]);
    props.insert(
        "team_relation".into(),
        PropertyValue::PageIds(vec!["inside-a".into()]),
    );
    props.insert(
        "title".into(),
        PropertyValue::Text("inside-b is mentioned".into()),
    );
    let result = relation_edges(&metadata(props), &pages()).unwrap();
    assert_eq!(result.len(), 4);
    assert!(result.iter().any(|edge| {
        edge.relation_type() == "relation:project_relation_id"
            && edge.provenance() == "property:project_relation_id"
            && edge.target()
                == &GraphTarget::Page {
                    page_id: "inside-a".into(),
                }
    }));
    assert!(result.iter().any(|edge| {
        edge.relation_type() == "relation:team_relation"
            && edge.provenance() == "property:team_relation"
            && edge.target()
                == &GraphTarget::Page {
                    page_id: "inside-a".into(),
                }
    }));
    assert!(result.iter().any(|edge| {
        edge.target()
            == &GraphTarget::Unresolved {
                reference: "outside".into(),
            }
    }));
    assert!(!result.iter().any(|edge| edge.relation_type().contains("people")));
}

#[test]
fn idempotent_replacement_preserves_links_and_removes_stale_relations() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let link = GraphEdge::new(
        "source".into(),
        GraphTarget::Page {
            page_id: "inside-b".into(),
        },
        "link".into(),
        "block:one".into(),
    )
    .unwrap();
    store
        .replace_page_edges("source", std::slice::from_ref(&link))
        .unwrap();
    let initial = relation_edges(&metadata(relations(&["inside-a", "outside"])), &pages())
        .unwrap();
    store.replace_page_relation_edges("source", &initial).unwrap();
    store.replace_page_relation_edges("source", &initial).unwrap();
    assert_eq!(store.edges_from("source").unwrap().len(), 3);
    assert_eq!(store.edges_to("inside-a").unwrap().len(), 1);
    assert!(store.edges_to("outside").unwrap().is_empty());

    let updated = relation_edges(&metadata(relations(&["inside-b"])), &pages()).unwrap();
    store.replace_page_relation_edges("source", &updated).unwrap();
    assert_eq!(store.edges_from("source").unwrap().len(), 2);
    assert!(store.edges_to("inside-a").unwrap().is_empty());
    assert_eq!(store.edges_to("inside-b").unwrap().len(), 2);

    store.replace_page_relation_edges("source", &[]).unwrap();
    assert_eq!(store.edges_from("source").unwrap(), vec![link]);
    assert!(store.edges_to("outside").unwrap().is_empty());
}

#[test]
fn invalid_or_cross_family_batch_does_not_delete_existing_edges() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let initial = relation_edges(&metadata(relations(&["inside-a"])), &pages()).unwrap();
    store.replace_page_relation_edges("source", &initial).unwrap();

    let untrusted_link = GraphEdge::new(
        "source".into(),
        GraphTarget::Page {
            page_id: "inside-b".into(),
        },
        "link".into(),
        "block:one".into(),
    )
    .unwrap();
    assert_eq!(
        store.replace_page_relation_edges("source", &[untrusted_link]),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(
        store.replace_page_relation_edges("foreign", &initial),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(store.edges_from("source").unwrap(), initial);
}

#[test]
fn missing_authorized_source_and_invalid_targets_fail_before_write() {
    let mut allowed = pages();
    allowed.remove("source");
    assert_eq!(
        relation_edges(&metadata(relations(&["inside-a"])), &allowed),
        Err(SyncStateError::InvalidInput)
    );
    let invalid = metadata(relations(&["inside-a", "bad\npage"]));
    assert_eq!(
        relation_edges(&invalid, &pages()),
        Err(SyncStateError::InvalidInput)
    );
}

#[test]
fn empty_or_removed_relation_property_emits_empty_replacement() {
    let empty = metadata(BTreeMap::new());
    assert!(relation_edges(&empty, &pages()).unwrap().is_empty());
    let no_targets = metadata(relations(&[]));
    assert!(relation_edges(&no_targets, &pages()).unwrap().is_empty());
}
