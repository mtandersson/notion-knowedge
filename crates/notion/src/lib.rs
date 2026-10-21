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

/// Server-owned scoped Notion page creation with canonical authoritative readback.
pub mod scoped_create;
pub use scoped_create::{ScopedPageCreateInput, ScopedPageCreated};

pub mod pages;

pub mod crawl;

pub mod content;

pub mod links;

/// Scoped graph projection from parsed Notion page links and mentions.
pub mod link_graph;

pub mod replacement;

mod transport;
pub use transport::{RequestAuthority, RequestMetrics};

pub mod documents;

pub mod lifecycle;

/// Input checks for future opt-in Notion File Upload workflows.
pub mod file_validation;
pub use file_validation::{FileValidationError, FileValidationPolicy, ValidatedFile};
pub mod file_download;

/// Authenticated, server-only single-part file upload. Attachment requires later scoped flow.
pub mod file_upload;
pub use file_upload::{UploadStatus, UploadedFile};

/// Opt-in multipart upload for operator-approved larger files.
pub mod file_multipart;
pub use file_multipart::MultipartPolicy;

/// Operator-scoped attachment of verified native Notion image blocks.
pub mod image_attachment;
pub use image_attachment::AttachedImage;

/// Server-only, root-scoped native Notion document file attachment.
pub mod file_attachment;
pub use file_attachment::AttachedFile;

/// Bounded sequential native attachment with per-item results.
pub mod batch_attachment;
pub use batch_attachment::{BatchAttachmentOutcome, BatchAttachmentStatus};

/// Fail-closed attachment audit facts, explicitly excluding signed source URLs.
pub mod file_ingest_audit;

// Only the validated adapter flows may invoke the shared bounded read-back.
mod attachment_readback;

/// One-page, read-only canonical refresh preparation under live physical scope.
pub mod prepare;

/// Restricted section PATCH adapter; no MCP exposure.
mod section_update;

/// Adapter for the core audited single-file ingestion application.
pub mod file_ingest;

/// Standalone authoritative relation proposals; production activation is separate.
pub mod relation_proposal;

#[cfg(test)]
mod test_clock;

#[cfg(test)]
mod file_authority_tests;
