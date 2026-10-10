use std::{collections::BTreeMap, fs, sync::atomic::{AtomicU64, Ordering}};
use notion_knowledge_core::{
    aliases::{AliasSources, PageAlias, PageAliasStore, normalized},
    indexed::{IndexedMetadata, PropertyValue, SourceMetadata},
    sync_state::SyncStateError,
};
use notion_knowledge_retrieval::sync_state::{LATEST_SCHEMA_VERSION, SqliteSyncStateStore};

static NEXT: AtomicU64 = AtomicU64::new(1);
const WS: &str = "approved-workspace";
const ROOT: &str = "approved-root";

fn document(page: &str, title: &str, properties: BTreeMap<String, PropertyValue>) -> IndexedMetadata {
    IndexedMetadata {
        page_id: page.to_owned(),
        block_id: None,
        title: title.to_owned(),
        url: format!("https://notion.so/{page}"),
        heading_path: vec![],
        last_edited_time: "2026-10-10T16:00:00Z".to_owned(),
        source: SourceMetadata {
            workspace_id: WS.to_owned(),
            root_page_id: ROOT.to_owned(),
            database_id: None,
            data_source_id: None,
        },
        properties,
    }
}

fn indexed(store: &SqliteSyncStateStore, doc: &IndexedMetadata, sources: &AliasSources) {
    store.replace_indexed_aliases(doc, sources).unwrap();
}

#[test]
fn configured_title_and_properties_support_swedish_and_english_aliases() {
    let mut props = BTreeMap::new();
    props.insert("alternative_names".to_owned(), PropertyValue::Strings(vec![
        "Östra sjön".to_owned(),
        "Eastern Lake".to_owned(),
        " ÖSTRA   SJÖN ".to_owned(),
    ]));
    props.insert("unlisted_private".to_owned(), PropertyValue::Text("Do not index".into()));
    props.insert("relations".to_owned(), PropertyValue::PageIds(vec!["another-page".into()]));
    let sources = AliasSources::new(true, vec!["alternative_names".into(), "relations".into()]).unwrap();
    let doc = document("canonical-lake", "Västra Sjön", props);
    let aliases = sources.extract(&doc).unwrap();
    assert!(aliases.iter().any(|entry| entry.key() == "östra sjön"));
    assert!(aliases.iter().any(|entry| entry.key() == "eastern lake"));
    assert!(aliases.iter().any(|entry| entry.key() == "västra sjön"));
    assert!(!aliases.iter().any(|entry| entry.key() == "do not index"));
    assert!(!aliases.iter().any(|entry| entry.key() == "another-page"));
    assert_eq!(normalized("  VÄSTRA \t Sjön  ").unwrap(), "västra sjön");
    assert!(aliases.iter().any(|entry| entry.provenance() == "property:alternative_names"));

    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    indexed(&store, &doc, &sources);
    for query in ["ÖSTRA SJÖN", "eastern lake", "Västra Sjön"] {
        assert_eq!(store.lookup_alias(WS, ROOT, query).unwrap(), vec!["canonical-lake"]);
    }
    assert!(store.lookup_alias(WS, ROOT, "Do not index").unwrap().is_empty());
}

#[test]
fn ambiguous_alias_lookup_returns_distinct_sorted_page_ids_not_one_guess() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let sources = AliasSources::new(true, vec![]).unwrap();
    indexed(&store, &document("page-z", "Åhus", BTreeMap::new()), &sources);
    indexed(&store, &document("page-a", "ÅHUS", BTreeMap::new()), &sources);
    assert_eq!(store.lookup_alias(WS, ROOT, "åhus").unwrap(), vec!["page-a", "page-z"]);

    store.replace_page_aliases("other-workspace", ROOT, "foreign", &[PageAlias::new("Åhus", "title").unwrap()]).unwrap();
    store.replace_page_aliases(WS, "other-root", "other", &[PageAlias::new("Åhus", "title").unwrap()]).unwrap();
    assert_eq!(store.lookup_alias(WS, ROOT, "åhus").unwrap(), vec!["page-a", "page-z"]);
}

#[test]
fn replacements_tombstones_and_invalid_inputs_are_atomic_and_scoped() {
    let store = SqliteSyncStateStore::open_in_memory().unwrap();
    let sources = AliasSources::new(true, vec![]).unwrap();
    indexed(&store, &document("p1", "Original", BTreeMap::new()), &sources);
    indexed(&store, &document("p2", "Original", BTreeMap::new()), &sources);
    assert_eq!(store.lookup_alias(WS, ROOT, "original").unwrap(), vec!["p1", "p2"]);

    indexed(&store, &document("p1", "Ny titel", BTreeMap::new()), &sources);
    assert_eq!(store.lookup_alias(WS, ROOT, "original").unwrap(), vec!["p2"]);
    assert_eq!(store.lookup_alias(WS, ROOT, "ny titel").unwrap(), vec!["p1"]);
    assert_eq!(
        store.replace_page_aliases(WS, ROOT, "", &[]),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(
        PageAlias::new("No\nunsafe", "title"),
        Err(SyncStateError::InvalidInput)
    );
    assert_eq!(store.lookup_alias(WS, ROOT, "ny titel").unwrap(), vec!["p1"]);
    store.replace_page_aliases(WS, ROOT, "p1", &[]).unwrap();
    assert!(store.lookup_alias(WS, ROOT, "ny titel").unwrap().is_empty());
}

#[test]
fn source_controls_cannot_expand_to_unknown_names_or_nontext_properties() {
    for invalid in ["", "  ", "foo\nbar"] {
        assert!(AliasSources::new(false, vec![invalid.to_owned()]).is_err());
    }
    assert!(AliasSources::new(false, vec![]).is_err());
    assert!(AliasSources::new(true, (0..33).map(|i| format!("p{i}")).collect()).is_err());
    let sources = AliasSources::new(false, vec!["aliases".into()]).unwrap();
    let mut props = BTreeMap::new();
    props.insert("aliases".into(), PropertyValue::Number(123.0));
    assert!(sources.extract(&document("p", "Title", props)).unwrap().is_empty());
    let sources = AliasSources::new(true, vec![]).unwrap();
    assert!(sources.extract(&document("p", "bad\nname", BTreeMap::new())).is_err());
    assert_eq!(normalized(" \t "), Err(SyncStateError::InvalidInput));
    assert_eq!(normalized(&"x".repeat(257)), Err(SyncStateError::InvalidInput));
}

#[test]
fn migrated_alias_store_persists_across_restart() {
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("nk-alias-test-{}-{id}", std::process::id()));
    let db = dir.join("data.sqlite3");
    let sources = AliasSources::new(true, vec![]).unwrap();
    {
        let store = SqliteSyncStateStore::open(&db).unwrap();
        assert_eq!(store.schema_version().unwrap(), LATEST_SCHEMA_VERSION);
        indexed(&store, &document("persistent-id", "Höst", BTreeMap::new()), &sources);
    }
    {
        let reopened = SqliteSyncStateStore::open(&db).unwrap();
        assert_eq!(reopened.lookup_alias(WS, ROOT, "HÖST").unwrap(), vec!["persistent-id"]);
    }
    fs::remove_dir_all(dir).unwrap();
}
