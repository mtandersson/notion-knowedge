//! Configuration owned by the server composition root.

pub mod config;
pub mod diagnostics;
pub mod http;

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

/// Single shared HTTP-process Notion OAuth credential composition (#299).
pub mod notion_oauth_runtime;
