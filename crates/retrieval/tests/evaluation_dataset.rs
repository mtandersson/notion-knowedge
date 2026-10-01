//! Dataset integrity specifications; retrieval quality is measured by the later harness.
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

fn dataset() -> Value {
    serde_json::from_str(include_str!(
        "../../../eval/retrieval/personal-knowledge-v1.json"
    ))
    .expect("versioned dataset must be valid JSON")
}

fn string(value: &Value) -> &str {
    value.as_str().expect("expected string field")
}

#[test]
fn ground_truth_resolves_to_unique_fixture_sources_and_valid_links() {
    let data = dataset();
    assert_eq!(data["schema_version"], 1);
    assert_eq!(data["dataset_id"], "personal-knowledge-fixtures-v1");
    let pages = data["pages"].as_array().unwrap();
    let mut page_ids = BTreeSet::new();
    let mut chunks = BTreeMap::new();
    for page in pages {
        let page_id = string(&page["id"]);
        assert!(page_id.starts_with("fixture:page:"));
        assert!(page_ids.insert(page_id), "duplicate page {page_id}");
        assert!(!string(&page["title"]).trim().is_empty());
        assert!(["sv", "en"].contains(&string(&page["language"])));
        assert!(["active", "complete", "archived"].contains(&string(&page["status"])));
        let date = string(&page["updated_at"]);
        assert!(date <= string(&data["as_of"]));
        assert_eq!(date.len(), 10);
        assert!(!page["chunks"].as_array().unwrap().is_empty());
        for chunk in page["chunks"].as_array().unwrap() {
            let id = string(&chunk["id"]);
            assert!(id.starts_with("fixture:chunk:"));
            assert!(chunks.insert(id, page_id).is_none(), "duplicate chunk {id}");
            assert!(!string(&chunk["text"]).trim().is_empty());
        }
    }
    for page in pages {
        let mut links = BTreeSet::new();
        for target in page["links"].as_array().unwrap() {
            let id = string(target);
            assert!(page_ids.contains(id), "dangling link {id}");
            assert_ne!(id, string(&page["id"]));
            assert!(links.insert(id));
        }
    }
    let mut query_ids = BTreeSet::new();
    for query in data["queries"].as_array().unwrap() {
        assert!(query_ids.insert(string(&query["id"])));
        assert!(!string(&query["text"]).trim().is_empty());
        assert!(!string(&query["intent"]).trim().is_empty());
        let mut relevant = BTreeSet::new();
        for judgment in query["relevant"].as_array().unwrap() {
            let id = string(&judgment["source_id"]);
            assert!(chunks.contains_key(id), "dangling judgment {id}");
            assert!(relevant.insert(id), "duplicate judgment {id}");
            assert!((1..=3).contains(&judgment["grade"].as_u64().unwrap()));
            assert!(!string(&judgment["rationale"]).trim().is_empty());
        }
        assert!(!relevant.is_empty());
        let mut excluded = BTreeSet::new();
        for id in query["excluded_source_ids"].as_array().unwrap() {
            let id = string(id);
            assert!(chunks.contains_key(id));
            assert!(excluded.insert(id));
            assert!(!relevant.contains(id), "conflicting judgment {id}");
        }
        assert!(
            !excluded.is_empty(),
            "each query needs a plausible distractor"
        );
    }
}

#[test]
fn both_languages_cover_every_query_pattern_with_equivalent_ground_truth() {
    let data = dataset();
    let queries = data["queries"].as_array().unwrap();
    for category in ["semantic", "exact_name", "date_status", "cross_link"] {
        for language in ["sv", "en"] {
            let count = queries
                .iter()
                .filter(|q| q["language"] == language && q["category"] == category)
                .count();
            assert!(count >= 3, "insufficient {language}/{category} coverage");
        }
    }
    for query in queries.iter().filter(|q| q["language"] == "en") {
        let base = string(&query["id"]).strip_suffix("-en").unwrap();
        let counterpart = queries
            .iter()
            .find(|q| q["id"] == format!("{base}-sv"))
            .unwrap();
        assert_eq!(query["relevant"], counterpart["relevant"]);
        assert_eq!(
            query["excluded_source_ids"],
            counterpart["excluded_source_ids"]
        );
        assert_eq!(query["category"], counterpart["category"]);
    }
}

#[test]
fn linked_questions_resolve_targets_and_current_records_exclude_archive_distractors() {
    let data = dataset();
    let pages = data["pages"].as_array().unwrap();
    let queries = data["queries"].as_array().unwrap();
    for (query, referring_page, expected_target) in [
        ("link-energy-en", "project-lighthouse", "energy-decision"),
        ("link-garden-en", "odling", "odling-review"),
        ("link-study-en", "study", "reading"),
    ] {
        let page_id = format!("fixture:page:{referring_page}");
        let target_id = format!("fixture:page:{expected_target}");
        let page = pages.iter().find(|p| p["id"] == page_id).unwrap();
        assert!(
            page["links"]
                .as_array()
                .unwrap()
                .iter()
                .any(|id| id == &target_id)
        );
        let query = queries.iter().find(|q| q["id"] == query).unwrap();
        let target = pages.iter().find(|p| p["id"] == target_id).unwrap();
        assert!(query["relevant"].as_array().unwrap().iter().any(|j| {
            j["grade"] == 3
                && target["chunks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|c| c["id"] == j["source_id"])
        }));
    }
    let active = queries
        .iter()
        .find(|q| q["id"] == "status-active-en")
        .unwrap();
    assert_eq!(active["relevant"].as_array().unwrap().len(), 6);
    for judgment in active["relevant"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|j| j["grade"] == 3)
    {
        let page = pages
            .iter()
            .find(|p| {
                p["chunks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|c| c["id"] == judgment["source_id"])
            })
            .unwrap();
        assert_eq!(page["status"], "active");
    }
    let bike = queries.iter().find(|q| q["id"] == "date-bike-en").unwrap();
    assert_eq!(
        bike["relevant"][0]["source_id"],
        "fixture:chunk:bike:service"
    );
    assert_eq!(
        bike["excluded_source_ids"][0],
        "fixture:chunk:bike-old:service"
    );
}
