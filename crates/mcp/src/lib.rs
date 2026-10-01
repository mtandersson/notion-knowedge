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
/// Tool discovery currently returns the SDK's empty catalog. Semantic tools
/// will be registered here as their application services become available.
#[derive(Debug, Clone, Copy, Default)]
pub struct KnowledgeServer;

impl ServerHandler for KnowledgeServer {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build()).with_server_info(
            Implementation::new("notion-knowledge", env!("CARGO_PKG_VERSION")),
        )
    }
}
