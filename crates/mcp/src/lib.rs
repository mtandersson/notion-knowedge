//! Transport-independent MCP application surface.
//!
//! Semantic MCP tools map onto application services here. stdio and
//! Streamable HTTP transport wiring belongs in the server crate.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "mcp";
