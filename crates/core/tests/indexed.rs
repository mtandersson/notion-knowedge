use notion_knowledge_core::indexed::{IndexedChunk, IndexedDocument, SchemaVersion};
use serde_json::json;

fn document_json() -> serde_json::Value {
    json!({
        "schema_version": "1",
        "metadata": {
            "page_id": "page-1", "block_id": null,
            "url": "https://notion.so/page-1", "title": "Kunskap 日本語",
            "heading_path": [], "last_edited_time": "2026-10-01T12:34:56.123Z",
            "source": {"workspace_id": "workspace-1", "root_page_id": "root-1",
                       "database_id": null, "data_source_id": null},
            "properties": {
                "empty": {"type": "null"}, "text": {"type": "text", "value": "Åäö"},
                "number": {"type": "number", "value": 1.5},
                "checkbox": {"type": "boolean", "value": false},
                "tags": {"type": "strings", "value": ["Rust", "MCP"]},
                "date": {"type": "date", "value": {"start": "2026-10-01", "end": null}},
                "relations": {"type": "page_ids", "value": ["related-page"]},
                "people": {"type": "person_ids", "value": ["person-1"]}
            }
        },
        "text": "# Kunskap\n\nText med [länk](https://example.org).\n",
        "content_hash": "hash-supplied-by-producer",
        "links": [{"type": "external", "url": "https://example.org"},
                  {"type": "page", "page_id": "related-page"},
                  {"type": "block", "page_id": "page-1", "block_id": "block-1"}]
    })
}

#[test]
fn document_wire_contract_preserves_normalized_content_and_typed_properties() {
    let wire = document_json();
    let document: IndexedDocument = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(document.schema_version, SchemaVersion::V1);
    assert_eq!(serde_json::to_value(&document).unwrap(), wire);
    let decoded: IndexedDocument =
        serde_json::from_str(&serde_json::to_string(&document).unwrap()).unwrap();
    assert_eq!(decoded, document);
}

#[test]
fn chunk_round_trip_preserves_independent_provenance_and_heading_order() {
    let mut wire = document_json();
    wire["chunk_id"] = json!("stable-chunk-1");
    wire["metadata"]["block_id"] = json!("block-1");
    wire["metadata"]["heading_path"] = json!(["Guide", "Installation"]);
    wire["metadata"]["source"]["database_id"] = json!("database-1");
    wire["metadata"]["source"]["data_source_id"] = json!("source-1");
    wire["metadata"]["properties"]["date"]["value"]["end"] = json!("2026-10-02");
    wire["text"] = json!("");
    wire["links"] = json!([]);
    let chunk: IndexedChunk = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(&chunk).unwrap(), wire);
    let decoded: IndexedChunk =
        serde_json::from_str(&serde_json::to_string(&chunk).unwrap()).unwrap();
    assert_eq!(decoded, chunk);
}

#[test]
fn readers_reject_missing_or_unsupported_schema_versions() {
    for version in [json!("2"), json!(null), json!(1)] {
        let mut wire = document_json();
        wire["schema_version"] = version;
        assert!(serde_json::from_value::<IndexedDocument>(wire.clone()).is_err());
        wire["chunk_id"] = json!("chunk-1");
        assert!(serde_json::from_value::<IndexedChunk>(wire).is_err());
    }
    let mut wire = document_json();
    wire.as_object_mut().unwrap().remove("schema_version");
    assert!(serde_json::from_value::<IndexedDocument>(wire).is_err());
}
