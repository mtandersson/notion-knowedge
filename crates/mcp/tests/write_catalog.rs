//! #76: MCP catalog contracts for proposed write workflows, never implicit writes.
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServerHandler;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn serialized(tool: rmcp::model::Tool) -> Value {
    serde_json::to_value(tool).expect("tool schema is serializable")
}

#[test]
fn writable_discovery_preserves_read_and_unavailable_upload_contracts() {
    let server = KnowledgeServer::default().with_read_only(false);
    let tools = server.tool_catalog();
    assert_eq!(
        tools.len(),
        3,
        "unimplemented writes must not be advertised"
    );
    for tool in tools {
        let wire = serialized(tool);
        if wire["name"] == "knowledge_upload_file" {
            assert_eq!(wire["annotations"]["readOnlyHint"], false);
            continue;
        }
        assert!(["knowledge_search", "knowledge_get"].contains(&wire["name"].as_str().unwrap()));
        assert_eq!(wire["annotations"]["readOnlyHint"], true);
        assert_eq!(wire["annotations"]["destructiveHint"], false);
    }
    for name in [
        "knowledge_create_page",
        "knowledge_append",
        "knowledge_update_section",
        "knowledge_archive_page",
    ] {
        assert!(
            server.get_tool(name).is_none(),
            "{name} must not be callable by default"
        );
    }
}

#[test]
fn explicit_design_preview_discovery_separates_narrow_writes_and_destructive_archiving() {
    let server = KnowledgeServer::default()
        .with_read_only(false)
        .with_destructive_writes_enabled(true)
        .with_write_design_preview();
    let tools = server.tool_catalog();
    assert_eq!(tools.len(), 7);
    let names: BTreeSet<String> = tools
        .iter()
        .map(|tool| {
            serialized(tool.clone())["name"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(
        names,
        [
            "knowledge_search",
            "knowledge_get",
            "knowledge_upload_file",
            "knowledge_create_page",
            "knowledge_append",
            "knowledge_update_section",
            "knowledge_archive_page",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    );
    for name in [
        "knowledge_create_page",
        "knowledge_append",
        "knowledge_update_section",
        "knowledge_archive_page",
    ] {
        let wire = serialized(server.get_tool(name).expect("preview tool"));
        let input = &wire["inputSchema"];
        assert_eq!(wire["annotations"]["readOnlyHint"], false);
        assert_eq!(
            wire["annotations"]["destructiveHint"],
            matches!(name, "knowledge_archive_page" | "knowledge_update_section")
        );
        assert_eq!(wire["annotations"]["openWorldHint"], true);
        assert_eq!(input["additionalProperties"], false);
        let required = input["required"].as_array().unwrap();
        for common in ["root_page_id", "idempotency_key"] {
            assert!(required.contains(&json!(common)), "{name} missing {common}");
            assert_eq!(input["properties"][common]["minLength"], 1);
        }
        assert!(
            wire["description"]
                .as_str()
                .unwrap()
                .contains("PREVIEW ONLY")
        );
        assert_eq!(
            wire["outputSchema"]["required"],
            json!(["page_id", "url", "last_edited_time", "verified"])
        );
        assert_eq!(
            wire["outputSchema"]["properties"]["verified"]["const"],
            true
        );
    }
    let create = serialized(server.get_tool("knowledge_create_page").unwrap());
    for key in ["parent_page_id", "title", "markdown"] {
        assert!(
            create["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!(key))
        );
    }
    assert!(create["inputSchema"]["properties"].get("page_id").is_none());

    let append = serialized(server.get_tool("knowledge_append").unwrap());
    let update = serialized(server.get_tool("knowledge_update_section").unwrap());
    for tool in [&append, &update] {
        for required in ["page_id", "expected_last_edited_time", "markdown"] {
            assert!(
                tool["inputSchema"]["required"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(required))
            );
        }
        assert_eq!(
            tool["inputSchema"]["properties"].get("confirmation_id").is_some(),
            tool["inputSchema"]["properties"].get("section_anchor").is_some()
        );
    }
    assert!(
        append["inputSchema"]["properties"]
            .get("section_anchor")
            .is_none()
    );
    assert!(
        update["inputSchema"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("section_anchor"))
    );
    let archive = serialized(server.get_tool("knowledge_archive_page").unwrap());
    for key in ["page_id", "expected_last_edited_time", "confirmation_id"] {
        assert!(
            archive["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!(key))
        );
    }
    assert!(
        archive["inputSchema"]["properties"]
            .get("markdown")
            .is_none()
    );
    assert!(server.get_tool("knowledge_update_page").is_none());
    assert!(server.get_tool("arbitrary_notion_block").is_none());
}

#[test]
fn read_only_catalog_hides_all_mutation_schemas_even_with_preview_enabled() {
    let server = KnowledgeServer::default().with_write_design_preview();
    let names: Vec<_> = server
        .tool_catalog()
        .into_iter()
        .map(|tool| tool.name.into_owned())
        .collect();
    assert_eq!(names, ["knowledge_search", "knowledge_get"]);
    for name in [
        "knowledge_upload_file",
        "knowledge_create_page",
        "knowledge_append",
        "knowledge_update_section",
        "knowledge_archive_page",
    ] {
        assert!(server.get_tool(name).is_none());
    }
}

#[test]
fn default_destructive_gate_hides_preview_schemas_without_disabling_safe_writes() {
    let server = KnowledgeServer::default()
        .with_read_only(false)
        .with_write_design_preview();
    assert_eq!(server.tool_catalog().len(), 5);
    for name in ["knowledge_update_section", "knowledge_archive_page"] {
        assert!(server.get_tool(name).is_none());
    }
    for name in ["knowledge_create_page", "knowledge_append"] {
        assert!(server.get_tool(name).is_some());
    }
}
