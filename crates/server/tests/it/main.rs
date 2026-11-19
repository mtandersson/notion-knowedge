//! One integration test binary for the crate: each module was a separate
//! `tests/*.rs` binary. `allocator_bounds` stays its own binary because its
//! allocator bound is process-wide.
mod admission;
mod bearer_fallback;
mod common;
mod http;
mod index_dispatch;
// Shared by `shutdown` and `webhook_worker`, so declared once here.
#[cfg(unix)]
#[path = "support/listener_shutdown.rs"]
mod listener_shutdown;
mod reconciliation_command;
mod reconciliation_dispatch;
mod session_bounds;
mod shutdown;
mod startup;
mod stdio;
mod sync_status;
// Content reads need the shared sync state, which only this build can configure.
#[cfg(all(unix, feature = "webhook-worker"))]
mod untrusted_http;
mod webhook_recovery;
mod webhook_worker;
