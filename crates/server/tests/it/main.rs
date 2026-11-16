//! One integration test binary for the crate: each module was a separate
//! `tests/*.rs` binary. `allocator_bounds` stays its own binary because its
//! allocator bound is process-wide.
mod admission;
mod bearer_fallback;
mod common;
mod http;
mod index_dispatch;
mod oauth_discovery;
mod oauth_forget;
mod reconciliation_command;
mod reconciliation_dispatch;
mod session_bounds;
mod shutdown;
mod startup;
mod stdio;
mod sync_status;
mod webhook_recovery;
mod webhook_worker;
