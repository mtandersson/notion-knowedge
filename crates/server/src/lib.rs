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
