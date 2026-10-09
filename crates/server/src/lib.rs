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
