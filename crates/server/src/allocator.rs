//! Process allocator bounds (#656).
//!
//! glibc gives each contending thread its own malloc arena (up to eight per
//! core). Model activations and Lance buffers freed on Tokio worker and
//! blocking threads stay cached in those arenas, so a long sync's resident
//! memory grew with pages processed while live heap stayed flat. A small fixed
//! arena count lets freed memory be reused instead of retained per thread.

/// Arena count used unless the operator sets `MALLOC_ARENA_MAX`.
pub const DEFAULT_ARENA_MAX: i32 = 2;

/// Call first in `main`, before any runtime threads exist. Returns the
/// applied limit, or `None` when the operator's environment governs it or
/// the platform allocator is not glibc.
pub fn bound_allocator_arenas() -> Option<i32> {
    if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        return None;
    }
    apply(DEFAULT_ARENA_MAX)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn apply(limit: i32) -> Option<i32> {
    // SAFETY: mallopt only adjusts allocator tunables; it takes no pointers.
    (unsafe { libc::mallopt(libc::M_ARENA_MAX, limit) } == 1).then_some(limit)
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn apply(_: i32) -> Option<i32> {
    None
}
