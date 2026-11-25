//! Configuration owned by the server composition root.

pub mod allocator;
pub mod config;
pub mod diagnostics;
pub mod http;
#[cfg(all(unix, feature = "webhook-worker"))]
pub mod integration_writes;
pub mod lexical_expansion;
pub mod shutdown;
pub mod state_migration;
pub mod write_stores;

/// Installed SDK session and SSE response lifetime budgets.
pub mod session_limits;

pub mod crawl_args;

pub mod webhook;

pub mod webhook_recovery;

/// Trusted Notion scope composition shared by HTTP and stdio.
pub mod root_scope;
mod saved_view;

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
mod index_readiness;

#[cfg(all(unix, feature = "webhook-worker"))]
pub mod graph_runtime;

mod inventory_membership;
