//! Domain types, application services, and ports.
//!
//! This crate must remain independent of MCP transports, Notion API types,
//! LanceDB, SQLite, and HTTP implementation details.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "core";

/// Server identity shared by protocol and operational diagnostics.
pub const SERVER_NAME: &str = "notion-knowledge";
/// All workspace packages inherit the release version from the root manifest.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod health;

/// Temporary hosted cache probe: exercise a newly compiled public function.
pub fn cache_probe_component() -> &'static str {
    COMPONENT
}

#[cfg(test)]
mod cache_probe_tests {
    #[test]
    fn source_refresh_executes_new_code() {
        assert_eq!(super::cache_probe_component(), "core");
    }
}
