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

/// Sanitized diagnostic projection for untrusted error strings.
pub mod redaction;

/// Payload-free structured operations and task-scoped correlation IDs.
pub mod logging;

/// Canonical, provider-independent indexed content contracts.
pub mod indexed;

/// Authoritative page read/write ports implemented by the Notion adapter.
pub mod backend;

/// Read-only discovery under explicit configured roots.
pub mod discovery;

/// Semantic Markdown chunk drafts, before hashing and ID assignment.
pub mod chunking;

/// Versioned content fingerprints and cross-run chunk identities.
pub mod fingerprint;

/// Embedding execution port and persisted vector-space identity.
pub mod embedding;

/// Semantic retrieval execution independent of MCP and storage types.
pub mod search;

/// Stable source expansion independent of MCP and storage types.
pub mod source;

/// Durable operational sync-state contracts. Concrete databases belong in adapters.
pub mod sync_state;

/// Provider-independent graph edge contracts.
pub mod graph;

/// Safe graph-edge derivation from normalized Notion relation properties.
pub mod relations;

/// Canonical page-name aliases derived only from configured indexed metadata.
pub mod aliases;

/// Configurable reciprocal-rank fusion of independent retrieval paths.
pub mod hybrid;

pub mod search_filters;

pub mod reconciliation;

pub mod webhook;

pub mod lifecycle;

/// Restricted privacy-aware agent mutation audit contracts.
pub mod audit;

/// Fresh, fail-closed optimistic preconditions for future safe write workflows.
pub mod revision;

/// Durable mutation claims and verified once-only receipts.
pub mod idempotency;

/// Fresh physical-ancestry authorization gate shared by future MCP tools.
pub mod root_scope;

/// Opt-in, scope-checked, durable once-only Notion append application workflow.
pub mod append;

/// Server-trusted confirmation gate for destructive operations (#82).
pub mod destructive;
