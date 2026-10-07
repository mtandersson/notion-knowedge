use serde_json::{Value, json};

pub fn assert_search_catalog(tools: &Value) {
    let tools = tools.as_array().expect("tools catalog");
    assert_eq!(tools.len(), 2);
    let tool = tools
        .iter()
        .find(|tool| tool["name"] == "knowledge_search")
        .expect("knowledge_search tool");
    let get = tools
        .iter()
        .find(|tool| tool["name"] == "knowledge_get")
        .expect("knowledge_get tool");
    assert_eq!(get["inputSchema"]["required"], json!(["refs", "max_chars"]));
    assert_eq!(get["annotations"]["readOnlyHint"], true);
    assert_eq!(get["annotations"]["openWorldHint"], true);
    assert_eq!(
        get["inputSchema"]["properties"]["freshness"]["enum"],
        json!(["indexed", "fresh"])
    );
    assert_eq!(
        get["inputSchema"]["properties"]["freshness"]["default"],
        "indexed"
    );
    let source = &get["outputSchema"]["properties"]["sources"]["items"];
    assert_eq!(
        source["properties"]["content_scope"]["enum"],
        json!(["indexed", "page"])
    );
    assert!(
        source["properties"]["provenance"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("indexed_last_edited_time"))
    );
    assert_eq!(
        source["properties"]["provenance"]["properties"]["refreshed_last_edited_time"]["type"],
        "string"
    );
    assert_eq!(
        source["properties"]["provenance"]["properties"]["index_stale"]["type"],
        "boolean"
    );
    assert!(tool["description"].as_str().unwrap().contains("semantic"));
    assert_eq!(tool["annotations"]["readOnlyHint"], true);
    let input = &tool["inputSchema"];
    assert_eq!(input["required"], json!(["query", "limit", "mode"]));
    assert_eq!(
        input["properties"]["mode"]["enum"],
        json!(["semantic", "lexical", "hybrid"])
    );
    assert_eq!(input["properties"]["limit"]["maximum"], 100);
    assert!(input["properties"]["filters"]["properties"]["page_ids"].is_object());
    assert!(input["properties"]["filters"]["properties"]["root_page_ids"].is_object());
    let metadata = &input["properties"]["filters"]["properties"]["metadata"];
    assert_eq!(metadata["additionalProperties"], false);
    assert_eq!(
        metadata["properties"]["page_kind"]["enum"],
        json!(["standalone", "database"])
    );
    assert_eq!(
        metadata["properties"]["edited"]["properties"]["from"]["format"],
        "date-time"
    );
    assert_eq!(
        metadata["properties"]["properties"]["items"]["oneOf"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let result = &tool["outputSchema"]["properties"]["results"]["items"];
    assert_eq!(result["properties"]["score"]["type"], "number");
    assert_eq!(
        result["properties"]["source"]["required"],
        json!([
            "page_id",
            "chunk_id",
            "url",
            "title",
            "heading_path",
            "last_edited_time"
        ])
    );
    assert!(result["properties"]["source"]["properties"]["block_id"].is_object());
    assert_eq!(
        result["required"],
        json!(["source", "text", "score", "matched_paths"])
    );
    assert_eq!(
        result["properties"]["source"]["properties"]["last_edited_time"]["type"],
        "string"
    );
    assert_eq!(
        result["properties"]["matched_paths"]["items"]["enum"],
        json!(["semantic", "lexical"])
    );
}
