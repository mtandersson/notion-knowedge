//! Local retrieval and operational-state adapters.
//!
//! Embedded LanceDB and SQLite implementations belong here and implement
//! interfaces owned by `notion-knowledge-core`. Their concrete types must not
//! leak into the application layer.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "retrieval";
