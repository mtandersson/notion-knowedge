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

/// Whether a byte slice is empty, used by the matched CI source-change probe.
pub fn ci_probe_is_empty(input: &[u8]) -> bool {
    input.is_empty()
}

#[cfg(test)]
mod ci_probe_tests {
    #[test]
    fn distinguishes_empty_and_nonempty_input() {
        assert!(super::ci_probe_is_empty(&[]));
        assert!(!super::ci_probe_is_empty(b"notion"));
    }
}
