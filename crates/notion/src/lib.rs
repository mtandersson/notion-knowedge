//! Notion backend adapter.
//!
//! This crate will implement the authoritative Notion read/write ports defined
//! by `notion-knowledge-core`. Notion SDK/REST/OAuth types stay inside this
//! adapter boundary.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "notion";
