//! Notion backend adapter.
//!
//! This crate implements (as concrete adapters are added) the authoritative Notion read/write ports defined
//! by `notion-knowledge-core`. Notion SDK/REST/OAuth types stay inside this
//! adapter boundary.

/// Stable component identifier used by the bootstrap composition smoke check.
pub const COMPONENT: &str = "notion";

mod backend;

pub mod client;
pub use client::{IntegrationIdentity, NotionClient};

pub mod writes;
pub use writes::CreatedPage;

pub mod pages;

pub mod crawl;

pub mod content;

pub mod links;

/// Scoped graph projection from parsed Notion page links and mentions.
pub mod link_graph;

pub mod replacement;

mod transport;
pub use transport::RequestMetrics;

pub mod documents;

pub mod lifecycle;

/// Input checks for future opt-in Notion File Upload workflows.
pub mod file_validation;
pub use file_validation::{FileValidationError, FileValidationPolicy, ValidatedFile};
pub mod file_download;

/// Authenticated, server-only single-part file upload. Attachment requires later scoped flow.
pub mod file_upload;
pub use file_upload::{UploadStatus, UploadedFile};

/// Operator-scoped attachment of verified native Notion image blocks.
pub mod image_attachment;
pub use image_attachment::AttachedImage;

/// One-page, read-only canonical refresh preparation under live physical scope.
pub mod prepare;
