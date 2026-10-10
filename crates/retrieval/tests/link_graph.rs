use std::collections::{BTreeMap, BTreeSet};

use notion_knowledge_core::{
    backend::{Page, PageContent, PageId},
    discovery::{DiscoveredPage, DiscoveryReport},
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    indexed::{IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
    sync_state::SyncStateError,
};
use notion_knowledge_notion::{documents::DocumentSnapshot, link_graph::page_link_edges};
use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;

const SOURCE: &str = "11111111-1111-4111-8111-111111111111";
const INSIDE: &str = "22222222-2222-4222-8222-222222222222";
const OUTSIDE: &str = "33333333-3333-4333-8333-333333333333";
const BLOCK: &str = "44444444-4444-4444-8444-444444444444";
const ROOT: &str = "55555555-5555-4555-8555-555555555555";

fn content(markdown: &str) -> PageContent {
    PageContent {
        page: Page {
            id: PageId(SOURCE.to_owned()),
            url: format!("https://notion.so/{SOURCE}"),
            title: "Source".into(),
            last_edited_time: "2026-10-10T17:00:00Z".into(),
            archived: false,
            properties: BTreeMap::new(),
        },
        markdown: markdown.to_owned(),
    }
}

fn discovery() -> BTreeSet<String> {
    [SOURCE, INSIDE].into_iter().map(str::to_owned).collect()
}

fn snapshot(markdown: &str) -> DocumentSnapshot {
    let origin = content(markdown);
    DocumentSnapshot {
        discovery: DiscoveryReport {
            roots: vec![ROOT.to_owned()],
            pages: [SOURCE, INSIDE]
                .into_iter()
                .map(|id| DiscoveredPage {
                    id: id.into(),
                    url: format!("https://notion.so/{id}"),
                    title: "Page".into(),
                })
                .collect(),
            skipped: vec![],
        },
        documents: vec![IndexedDocument {
            schema_version: SchemaVersion::V1,
            metadata: IndexedMetadata {
                page_id: SOURCE.into(),
                block_id: None,
                url: origin.page.url.clone(),
                title: origin.page.title.clone(),
                heading_path: vec![],
                last_edited_time: origin.page.last_edited_time.clone(),
                source: SourceMetadata {
                    workspace_id: "trusted-workspace".into(),
                    root_page_id: ROOT.into(),
                    database_id: None,
                    data_source_id: None,
                },
                properties: BTreeMap::new(),
            },
            text: origin.markdown,
            content_hash: "source-hash".into(),
            links: vec![],
        }],
        chunks: vec![],
    }
}

#[test]
fn explicit_links_mentions_and_blocks_deduplicate_canonically() {
    let markdown = format!(
        "[one](https://notion.so/{INSIDE})\n[repeated](https://notion.so/{INSIDE})\n<mention-page url=\"https://notion.so/{INSIDE}\"/>\n[block](https://notion.so/{INSIDE}#{BLOCK})\n<page url=\"https://notion.so/{INSIDE}\"/>\n"
    );
    let edges = page_link_edges(&content(&markdown), &discovery()).unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().any(|edge| {
        edge.relation_type() == "link:page"
            && edge.target()
                == &GraphTarget::Page {
                    page_id: INSIDE.into(),
                }
            && edge.provenance() == "markdown:0"
    }));
    assert!(edges.iter().any(|edge| {
        edge.relation_type() == "link:block"
            && edge.target()
                == &GraphTarget::Page {
                    page_id: INSIDE.into(),
                }
            && edge.provenance().starts_with("markdown:")
    }));
}

#[test]
fn excluded_targets_are_unresolved_and_external_urls_never_become_nodes() {
    let markdown = format!(
        "[outside](https://notion.so/{OUTSIDE})\n[evil](https://notion.so.evil.test/{INSIDE})\n[external](https://example.com/{INSIDE})\n[relative](/other)\n[app](https://app.notion.com/{INSIDE})"
    );
    let edges = page_link_edges(&content(&markdown), &discovery()).unwrap();
    assert_eq!(edges.len(), 2);
    assert!(edges.iter().any(|edge| {
        edge.target()
            == &GraphTarget::Unresolved {
                reference: OUTSIDE.into(),
            }
    }));
    assert!(edges.iter().any(|edge| {
        edge.target()
            == &GraphTarget::Page {
                page_id: INSIDE.into(),
            }
    }));
    assert!(!edges.iter().any(|edge| edge.provenance().contains("evil")));
}

#[test]
fn invalid_internal_references_remain_diagnosable_without_raw_url_content() {
    let markdown = "[bad](https://notion.so/not-a-uuid?secret=private-token)\n<mention-page url=\"broken-reference\"/>\n<mention-user url=\"person-secret\"/>\n[external](https://example.org/broken)";
    let edges = page_link_edges(&content(markdown), &discovery()).unwrap();
    assert_eq!(edges.len(), 2);
    for edge in edges {
        assert_eq!(edge.relation_type(), "link:invalid");
        let GraphTarget::Unresolved { reference } = edge.target() else {
            panic!("malformed page must never resolve");
        };
        assert!(reference.starts_with("invalid-page-link:"));
        assert!(!reference.contains("private-token"));
        assert!(!reference.contains("broken-reference"));
        assert!(edge.provenance().starts_with("markdown:"));
    }
}

#[test]
fn fenced_code_and_escaped_tags_do_not_create_graph_edges() {
    let markdown = format!(
        "`[literal](https://notion.so/{INSIDE})`\n```html\n<mention-page url=\"https://notion.so/{INSIDE}\"/>\n```\n\\<mention-page url=\"https://notion.so/{INSIDE}\"/>"
    );
    assert!(
        page_link_edges(&content(&markdown), &discovery())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn reindex_is_idempotent_and_removes_only_old_link_family() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let relation = GraphEdge::new(
        SOURCE.into(),
        GraphTarget::Page {
            page_id: INSIDE.into(),
        },
        "relation:stable-id".into(),
        "property:stable-id".into(),
    )
    .unwrap();
    store
        .replace_page_relation_edges(SOURCE, &[relation.clone()])
        .unwrap();

    let initial = snapshot(&format!(
        "[inside](https://notion.so/{INSIDE})\n[outside](https://notion.so/{OUTSIDE})"
    ));
    initial.persist_link_edges(&store).unwrap();
    initial.persist_link_edges(&store).unwrap();
    assert_eq!(store.edges_from(SOURCE).unwrap().len(), 3);
    assert_eq!(store.edges_to(INSIDE).unwrap().len(), 2);
    assert!(store.edges_to(OUTSIDE).unwrap().is_empty());

    let update = snapshot(&format!("[block](https://notion.so/{INSIDE}#{BLOCK})"));
    update.persist_link_edges(&store).unwrap();
    assert_eq!(store.edges_from(SOURCE).unwrap().len(), 2);
    assert_eq!(store.edges_to(INSIDE).unwrap().len(), 2);

    snapshot("").persist_link_edges(&store).unwrap();
    assert_eq!(store.edges_from(SOURCE).unwrap(), vec![relation]);
}

#[test]
fn malformed_input_is_rejected_before_graph_replacement() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let initial = snapshot(&format!("[page](https://notion.so/{INSIDE})"));
    initial.persist_link_edges(&store).unwrap();
    let existing = store.edges_from(SOURCE).unwrap();
    let mut invalid = initial;
    invalid.documents[0].metadata.source.root_page_id = "wrong-root".into();
    assert_eq!(
        invalid.persist_link_edges(&store),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(store.edges_from(SOURCE).unwrap(), existing);
    let relation = GraphEdge::new(
        SOURCE.into(),
        GraphTarget::Page {
            page_id: INSIDE.into(),
        },
        "relation:stable-id".into(),
        "property:stable-id".into(),
    )
    .unwrap();
    assert_eq!(
        store.replace_page_link_edges(SOURCE, &[relation]),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(store.edges_from(SOURCE).unwrap(), existing);
    assert_eq!(
        store.replace_page_link_edges("different-source", &existing),
        Err(SyncStateError::InvalidInput)
    );
}

#[test]
fn a_discovery_without_the_source_page_cannot_authorize_edges() {
    let mut allowed = discovery();
    allowed.remove(SOURCE);
    assert_eq!(
        page_link_edges(
            &content(&format!("[page](https://notion.so/{INSIDE})")),
            &allowed
        ),
        Err(SyncStateError::InvalidInput)
    );
}
