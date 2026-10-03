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

/// Canonical, provider-independent indexed content contracts.
pub mod indexed;

/// Authoritative page read/write ports implemented by the Notion adapter.
pub mod backend;

/// Read-only discovery under explicit configured roots.
pub mod discovery;
