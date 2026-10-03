use serde_json::{Value, json};

pub fn assert_search_catalog(tools: &Value) {
    let tools = tools.as_array().expect("tools catalog");
    assert_eq!(tools.len(), 1);
    let tool = &tools[0];
    assert_eq!(tool["name"], "knowledge_search");
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
    let result = &tool["outputSchema"]["properties"]["results"]["items"];
    assert_eq!(result["properties"]["score"]["type"], "number");
    assert_eq!(
        result["properties"]["source"]["required"],
        json!(["page_id", "chunk_id", "url", "title", "heading_path"])
    );
    assert!(result["properties"]["source"]["properties"]["block_id"].is_object());
}
