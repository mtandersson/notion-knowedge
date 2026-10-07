//! Versioned cache records shared by discovery, chunking and retrieval.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Supported persisted contract. Unknown versions fail deserialization so a
/// reader cannot silently interpret incompatible data as the current schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaVersion {
    #[serde(rename = "1")]
    V1,
}

/// Provenance of a page; IDs refer to Notion objects, never credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceMetadata {
    pub workspace_id: String,
    pub root_page_id: String,
    pub database_id: Option<String>,
    pub data_source_id: Option<String>,
}

/// Typed normalized values keyed by stable Notion property ID, not display name.
/// Formula and rollup outputs use the corresponding resolved value variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PropertyValue {
    Null,
    Text(String),
    Number(f64),
    Boolean(bool),
    Strings(Vec<String>),
    Date { start: String, end: Option<String> },
    PageIds(Vec<String>),
    PersonIds(Vec<String>),
}

/// Extracted source relationships. Extraction policy belongs to the adapter.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LinkTarget {
    External { url: String },
    Page { page_id: String },
    Block { page_id: String, block_id: String },
}

/// Metadata needed to cite and refresh either a page or a chunk independently.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedMetadata {
    pub page_id: String,
    pub block_id: Option<String>,
    pub url: String,
    pub title: String,
    /// Ordered outermost heading first; empty means page-level content.
    pub heading_path: Vec<String>,
    /// Original Notion RFC 3339 edit timestamp, preserved without conversion.
    pub last_edited_time: String,
    pub source: SourceMetadata,
    pub properties: BTreeMap<String, PropertyValue>,
}

/// Normalized page content, independent of storage and embedding providers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedDocument {
    pub schema_version: SchemaVersion,
    pub metadata: IndexedMetadata,
    /// Normalized Markdown/plain text, never raw Notion API JSON.
    pub text: String,
    /// Hash of normalized content; algorithm/payload are owned by the hasher.
    pub content_hash: String,
    pub links: Vec<LinkTarget>,
}

/// Independently serializable retrieval unit derived from one indexed page.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexedChunk {
    pub schema_version: SchemaVersion,
    /// Stable identifier supplied by the chunk-ID generator.
    pub chunk_id: String,
    pub metadata: IndexedMetadata,
    pub text: String,
    /// Hash of this chunk's normalized content, not the entire page.
    pub content_hash: String,
    pub links: Vec<LinkTarget>,
}
