//! Configuration owned by the server composition root.

pub mod config;
pub mod diagnostics;
pub mod file_recovery_command;
pub mod http;
pub mod oauth_writes;
pub mod shutdown;
pub mod state_migration;

/// Installed SDK session and SSE response lifetime budgets.
pub mod session_limits;

pub mod crawl_args;

pub mod webhook;

pub mod webhook_recovery;

/// HTTPS OAuth discovery; never enables MCP authorization by itself.
pub mod oauth_discovery;

/// Approved-grant-only OAuth authorization code and PKCE engine (#120).
pub mod oauth_code;

/// Notion OAuth consent redirect and independent single-use CSRF state (#121).
pub mod notion_oauth_redirect;

/// Notion OAuth callback and confidential code exchange; still not an exposed login.
pub mod notion_oauth_callback;

/// Encrypted versioned Notion grant store with immutable owner checks (#123).
pub mod notion_grant_store;

/// Server-only serialized Notion refresh and sealed credential rotation (#124).
pub mod notion_oauth_refresh;

/// Trusted Notion scope composition shared by HTTP and stdio.
pub mod root_scope;

/// Single shared HTTP-process Notion OAuth credential composition (#299).
pub mod notion_oauth_runtime;

pub mod webhook_worker;

pub mod index_command;
pub mod index_runtime;
pub mod reconciliation_command;

pub mod scheduled_reconciliation;

pub mod sync_status;

pub mod file_ingestion;

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod retrieval_runtime;

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod graph_runtime;

/// Browser entry point for staged Notion authorization.
pub mod oauth_authorize;

/// Installed OAuth MCP resource and exact-token session enforcement.
mod oauth_mcp;

mod oauth_content;

mod notion_grant_lease;

/// Grant-bound durable refresh building blocks; shipping installation is separate.
#[cfg(all(unix, feature = "webhook-worker"))]
pub mod grant_refresh;

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod file_refresh;
