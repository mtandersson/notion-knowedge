//! Single trusted root-scope composition path for both MCP transports.
//! The absence of an authoritative scope never falls back to index metadata.

use std::sync::Arc;

use notion_knowledge_mcp::KnowledgeServer;

use crate::config::{Config, NotionAuth};

pub fn attach_root_scope(
    handler: KnowledgeServer,
    config: &Config,
) -> Result<KnowledgeServer, &'static str> {
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
