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

/// SQLite-backed derived page relationship edges.
pub mod graph;

/// Scoped, durable lookup for canonical Notion page aliases.
pub mod aliases;

pub mod reconciliation;

pub mod webhook;

pub mod webhook_debounce;

/// Owned cross-process index commit coordination (local Unix filesystems).
#[cfg(unix)]
pub mod commit;

/// Dedicated durable, privacy-limited agent mutation audit store.
pub mod audit;

/// Separate persistent once-only write ledger (never part of disposable indexes).
pub mod idempotency;

/// Real authoritative selected-page refresh through guarded SQLite/LanceDB.
#[cfg(all(unix, feature = "local-lancedb"))]
pub mod refresh;
