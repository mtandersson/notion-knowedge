//! Test-only pause point inside an owned page commit, after the index
//! directory guard and SQLite fence are held and before any effect. Tests use
//! it to cancel, expire or shut down work at that boundary. Registered per
//! index directory, so parallel tests do not see each other's pauses.
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type CommitPause = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

static PAUSES: Mutex<BTreeMap<PathBuf, CommitPause>> = Mutex::new(BTreeMap::new());

/// Run `pause` inside every owned page commit on `index` until cleared.
fn key(index: &Path) -> PathBuf {
    std::fs::canonicalize(index).unwrap_or_else(|_| index.to_path_buf())
}

pub fn set_commit_pause(index: &Path, pause: CommitPause) {
    PAUSES.lock().unwrap().insert(key(index), pause);
}

pub fn clear_commit_pause(index: &Path) {
    PAUSES.lock().unwrap().remove(&key(index));
}

pub(crate) async fn commit_pause(index: &Path) {
    let pause = PAUSES.lock().unwrap().get(&key(index)).cloned();
    if let Some(pause) = pause {
        pause().await;
    }
}
