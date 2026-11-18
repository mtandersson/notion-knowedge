//! One integration test binary for the crate: each module was a separate
//! `tests/*.rs` binary that linked LanceDB on its own.
mod aliases;
mod authoritative_refresh;
mod ci_retrieval_quality;
mod commit_memory;
mod evaluation_dataset;
mod file_refresh_intents;
mod grant_refresh_recovery;
mod graph;
mod guarded_chunks;
mod hosted_baseline;
mod idempotency;
mod index_commits;
mod link_graph;
mod mutation_refresh;
mod reconciliation;
mod relations;
mod schedule;
mod sync_state;
mod webhook_debounce;
mod webhook_inbox;
mod zip_compat;
