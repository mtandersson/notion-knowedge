use super::*;
use notion_knowledge_core::reconciliation::ReconciliationJournal;

impl IndexCommitCoordinator {
    /// Startup-only reset for this single-process service. Preserve the state
    /// database and all upstream-effect records; discard only known cache
    /// artifacts. No old generation or runtime is needed to reconstruct it.
    pub fn reset_cache(index: &Path, state: &Path, now: i64) -> Result<(), CommitError> {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(index)
            .map_err(io)?;
        let index = fs::canonicalize(index).map_err(io)?;
        if state.starts_with(&index) {
            return Err(CommitError::InvalidInput);
        }
        let directory = File::open(&index).map_err(io)?;
        directory.try_lock().map_err(|_| CommitError::Conflict)?;
        let store = SqliteSyncStateStore::open(state).map_err(|_| CommitError::Unavailable)?;
        if fs::canonicalize(state).map_err(io)?.starts_with(&index) {
            return Err(CommitError::InvalidInput);
        }
        let lease = store
            .acquire_lease(now, 330)
            .map_err(|_| CommitError::Conflict)?;
        let result = (|| {
            // Commit the pending-sync flag before removing any rows/files, so a
            // stopped rebuild is retried rather than admitted as an empty cache.
            store
                .reset_retrieval_cache(&lease, now)
                .map_err(|_| CommitError::Unavailable)?;
            let table = index.join("chunks.lance");
            if table.symlink_metadata().is_ok() {
                if table.symlink_metadata().map_err(io)?.is_dir() {
                    fs::remove_dir_all(&table).map_err(io)?;
                } else {
                    fs::remove_file(&table).map_err(io)?;
                }
            }
            for path in [
                index.join(ANCHOR),
                index.join(".nk-lexical-binding-v1.json"),
                transition::journal_path(state)?,
            ] {
                match fs::remove_file(path) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io(e)),
                }
            }
            Ok(())
        })();
        let _ = store.release_lease(&lease, now);
        result
    }
}
