//! Transport-independent MCP application surface.
//!
//! Semantic MCP tools map onto application services here. stdio and
//! Streamable HTTP transport wiring belongs in the server crate.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "mcp";

use rmcp::{
    ServerHandler,
    model::{Implementation, ServerCapabilities, ServerConfig},
};

/// Shared application handler for all MCP transports.
///
/// The semantic contract is shared by stdio and Streamable HTTP.
#[derive(Debug, Clone, Copy, Default)]
pub struct KnowledgeServer;

pub mod search;

impl ServerHandler for KnowledgeServer {
    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::ListToolsResult, rmcp::ErrorData> {
        Ok(rmcp::model::ListToolsResult {
            tools: vec![search::tool()],
            ..Default::default()
        })
    }

    fn get_tool(&self, name: &str) -> Option<rmcp::model::Tool> {
        (name == "knowledge_search").then(search::tool)
    }

    async fn call_tool(
        &self,
        request: rmcp::model::CallToolRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::CallToolResponse, rmcp::ErrorData> {
        if request.name != "knowledge_search" {
            return Err(rmcp::ErrorData::invalid_params("unknown tool", None));
        }
        let input: search::SearchRequest = serde_json::from_value(serde_json::Value::Object(
            request.arguments.unwrap_or_default(),
        ))
        .map_err(|_| rmcp::ErrorData::invalid_params("invalid knowledge_search arguments", None))?;
        input
            .validate()
            .map_err(|message| rmcp::ErrorData::invalid_params(message, None))?;
        Ok(rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text("retrieval_unavailable: no retrieval index adapter is configured; no search was performed")]).into())
    }
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new(
                notion_knowledge_core::SERVER_NAME,
                notion_knowledge_core::VERSION,
            ),
        )
    }
}
