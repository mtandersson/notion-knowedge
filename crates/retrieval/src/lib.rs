//! Local retrieval and operational-state adapters.
//!
//! Embedded LanceDB and SQLite implementations belong here and implement
//! interfaces owned by `notion-knowledge-core`. Their concrete types must not
//! leak into the application layer.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "retrieval";

#[cfg(feature = "local-lancedb")]
pub mod chunks;

#[cfg(feature = "local-qwen")]
pub mod qwen;

/// SQLite-backed durable operational state used by crawlers and index orchestration.
pub mod sync_state;

pub mod reconciliation;
