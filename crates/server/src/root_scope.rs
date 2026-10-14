//! Single trusted root-scope composition path for both MCP transports.
//! The absence of an authoritative scope never falls back to index metadata.

use std::sync::Arc;

use notion_knowledge_mcp::KnowledgeServer;

use crate::config::{Config, NotionAuth};

pub fn attach_root_scope(
    handler: KnowledgeServer,
    config: &Config,
) -> Result<KnowledgeServer, &'static str> {
    let handler = crate::sync_status::attach(handler, config)
        .with_max_upload_files(config.max_upload_files)?;
    let Some(scope) = &config.notion_scope else {
        // The MCP handler itself refuses content-bearing calls without the gate.
        return Ok(handler);
    };
    let NotionAuth::Integration(token) = &config.notion_auth else {
        return Err("authoritative root scope needs integration credentials");
    };
    let source = notion_knowledge_notion::NotionClient::integration(token.expose_secret())
        .map_err(|_| "Notion lifecycle backend unavailable")?;
    handler.and_authoritative_scope(Arc::new(source), scope.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::ServerHandler;
    #[test]
    fn installed_schema_keeps_startup_count_after_configuration_source_changes() {
        let mut source = std::collections::HashMap::from([("NK_MAX_UPLOAD_FILES", "2")]);
        let config =
            Config::from_lookup(|key| source.get(key).map(std::ffi::OsString::from)).unwrap();
        let handler =
            attach_root_scope(KnowledgeServer::default().with_read_only(false), &config).unwrap();
        source.insert("NK_MAX_UPLOAD_FILES", "16");
        let new_config =
            Config::from_lookup(|key| source.get(key).map(std::ffi::OsString::from)).unwrap();
        assert_eq!(new_config.max_upload_files, 16);
        assert_eq!(config.max_upload_files, 2);
        let tool =
            serde_json::to_value(handler.get_tool("knowledge_upload_file").unwrap()).unwrap();
        assert_eq!(tool["inputSchema"]["properties"]["files"]["maxItems"], 2);
        let standalone = serde_json::to_value(
            KnowledgeServer::default()
                .with_read_only(false)
                .get_tool("knowledge_upload_file")
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            standalone["inputSchema"]["properties"]["files"]["maxItems"],
            4
        );
    }
}
