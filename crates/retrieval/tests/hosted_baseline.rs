//! Offline integrity checks, never a hosted-access or retrieval-score CI gate.
use serde_json::Value;
use std::collections::BTreeMap;

fn baseline() -> Value {
    serde_json::from_str(include_str!(
        "../../../eval/retrieval/hosted-notion-2026-10-01.json"
    ))
    .unwrap()
}

#[test]
fn baseline_preserves_every_fixture_query_and_its_information_need() {
    let dataset: Value = serde_json::from_str(include_str!(
        "../../../eval/retrieval/personal-knowledge-v1.json"
    ))
    .unwrap();
    let record = baseline();
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["dataset_id"], dataset["dataset_id"]);
    let queries = dataset["queries"].as_array().unwrap();
    let rows = record["queries"].as_array().unwrap();
    assert_eq!(rows.len(), queries.len());
    let mut by_id = BTreeMap::new();
    for row in rows {
        assert!(
            by_id
                .insert(row["query_id"].as_str().unwrap(), row)
                .is_none()
        );
    }
    for query in queries {
        let row = by_id[query["id"].as_str().unwrap()];
        assert_eq!(row["query_text"], query["text"]);
        assert_eq!(row["language"], query["language"]);
        assert_eq!(row["category"], query["category"]);
    }
}

#[test]
fn unavailable_observations_carry_no_invented_rankings_or_quality_scores() {
    let record = baseline();
    assert_eq!(record["evidence"]["search_calls_made"], 0);
    assert_eq!(record["comparison"]["comparable_queries"], 0);
    assert_eq!(record["comparison"]["ci_requires_hosted_access"], false);
    assert!(record["comparison"]["metrics"].is_null());
    for row in record["queries"].as_array().unwrap() {
        assert_eq!(row["status"], "unavailable");
        assert_eq!(row["reason"], "access_discovery_tool_unavailable");
        assert_eq!(row["executed"], false);
        assert!(row["ranked_page_ids"].is_null());
        assert!(row["metrics"].is_null());
    }
}

#[test]
fn observation_distinguishes_missing_discovery_from_a_plan_denial() {
    let record = baseline();
    assert_eq!(record["observed_at"], "2026-10-01T21:40:13Z");
    assert_eq!(record["evidence"]["search_exposed"], true);
    assert_eq!(record["evidence"]["get_tool_access_exposed"], false);
    assert_eq!(record["evidence"]["ai_search_exposed"], false);
    assert_eq!(record["evidence"]["corpus_verification"], "not_performed");
    let context = &record["tool_context"];
    assert_eq!(context["plan"], "unknown");
    assert_eq!(context["ai_search_access"], "unknown");
    assert!(context["tool_version"].is_null());
    for note in [
        &context["version_note"],
        &record["evidence"]["method"],
        &record["evidence"]["description_requires"],
        &record["evidence"]["corpus_note"],
    ] {
        assert!(!note.as_str().unwrap().trim().is_empty());
    }
}
