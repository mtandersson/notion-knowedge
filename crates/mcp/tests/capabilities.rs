use notion_knowledge_mcp::capabilities::{CapabilityPolicy, PolicyListError};
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServerHandler;

fn policy(allow: Option<&str>, block: Option<&str>) -> CapabilityPolicy {
    CapabilityPolicy::from_lists(allow, block).unwrap()
}

#[test]
fn default_keeps_only_existing_operations_not_arbitrary_future_names() {
    let config = policy(None, None);
    for name in [
        "knowledge_search",
        "knowledge_get",
        "knowledge_upload_file",
        "knowledge_create_page",
        "knowledge_append",
        "knowledge_update_section",
        "knowledge_archive_page",
    ] {
        assert!(config.permits(name), "{name}");
    }
    for name in ["unknown", "knowledge_delete_everything", "admin"] {
        assert!(!config.permits(name), "{name}");
    }
}

#[test]
fn exact_and_category_rules_have_deterministic_block_precedence() {
    let config = policy(Some("read,knowledge_append"), Some("knowledge_get"));
    assert!(config.permits("knowledge_search"));
    assert!(!config.permits("knowledge_get"));
    assert!(config.permits("knowledge_append"));
    assert!(!config.permits("knowledge_upload_file"));

    let config = policy(Some("knowledge_search,knowledge_append"), Some("read,write"));
    assert!(!config.permits("knowledge_search"));
    assert!(!config.permits("knowledge_append"));
    assert!(!config.permits("knowledge_get"));
    assert!(!config.permits("knowledge_update_section"));
    assert!(!config.permits("knowledge_create_page"));

    let config = policy(Some("admin"), None);
    assert!(!config.permits("knowledge_search"));
    assert!(!config.permits("future_admin_tool"));
}

#[test]
fn empty_allowlist_disables_everything_but_absent_blocklist_denies_nothing() {
    let empty = policy(Some(""), None);
    assert!(!empty.permits("knowledge_get"));
    assert!(!empty.permits("knowledge_upload_file"));
    let blacklist = policy(None, Some("knowledge_upload_file"));
    assert!(blacklist.permits("knowledge_get"));
    assert!(!blacklist.permits("knowledge_upload_file"));
}

#[test]
fn malformed_and_unknown_entries_never_silently_enable_operations() {
    for value in [
        ",", "read,", ",write", "read,,write", "knowledge_get,not_real",
        "knowledge_delete_everything", "*", "READ", "knowledge get",
        "🚫", "read\nwrite",
    ] {
        assert_eq!(
            CapabilityPolicy::from_lists(Some(value), None).unwrap_err(),
            PolicyListError::Allowlist
        );
        assert_eq!(
            CapabilityPolicy::from_lists(None, Some(value)).unwrap_err(),
            PolicyListError::Blocklist
        );
    }
}

#[test]
fn catalog_and_get_tool_match_for_read_write_and_destructive_previews() {
    let server = KnowledgeServer::default()
        .with_read_only(false)
        .with_destructive_writes_enabled(true)
        .with_write_design_preview()
        .with_capability_policy(policy(
            Some("read,knowledge_append,knowledge_archive_page"),
            Some("knowledge_get"),
        ));
    let names: Vec<_> = server
        .tool_catalog()
        .iter()
        .map(|tool| tool.name.to_string())
        .collect();
    assert_eq!(names, [
        "knowledge_search",
        "knowledge_append",
        "knowledge_archive_page",
    ]);
    for operation in [
        "knowledge_get", "knowledge_upload_file", "knowledge_create_page",
        "knowledge_update_section", "unknown",
    ] {
        assert!(server.get_tool(operation).is_none(), "{operation}");
    }
    for name in &names {
        assert!(server.get_tool(name).is_some(), "{name}");
    }
    let readonly = server.with_read_only(true);
    assert_eq!(readonly.tool_catalog().len(), 1);
    assert_eq!(readonly.tool_catalog()[0].name.as_ref(), "knowledge_search");
    assert!(readonly.get_tool("knowledge_archive_page").is_none());
}

#[test]
fn category_allowlist_never_bypasses_existing_destructive_mode() {
    let server = KnowledgeServer::default()
        .with_read_only(false)
        .with_write_design_preview()
        .with_capability_policy(policy(Some("write"), None));
    assert!(server.get_tool("knowledge_append").is_some());
    assert!(server.get_tool("knowledge_archive_page").is_none());
    assert!(server.get_tool("knowledge_update_section").is_none());
}
