//! Notion backend adapter.
//!
//! This crate implements (as concrete adapters are added) the authoritative Notion read/write ports defined
//! by `notion-knowledge-core`. Notion SDK/REST/OAuth types stay inside this
//! adapter boundary.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "notion";

pub mod client;
pub use client::{IntegrationIdentity, NotionClient};

pub mod writes;
pub use writes::CreatedPage;

pub mod pages;

pub mod crawl;

pub mod content;
