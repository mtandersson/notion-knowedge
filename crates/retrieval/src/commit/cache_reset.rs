use super::*;
use notion_knowledge_core::reconciliation::ReconciliationJournal;

// Recognize the fixed legacy artifact only to discard and rebuild an older
// installation. No legacy journal contents or transition phases are admitted.
fn legacy_journal_path(state: &Path) -> Result<PathBuf, CommitError> {
    let mut name = state
        .file_name()
        .ok_or(CommitError::InvalidInput)?
        .to_os_string();
    name.push(".lexical-transition-v1.json");
    Ok(state.with_file_name(name))
}

impl IndexCommitCoordinator {
    /// An older installation with an unfinished handoff must rebuild its cache.
    pub fn has_legacy_cache_journal(state: &Path) -> Result<bool, CommitError> {
        legacy_journal_path(state)?.try_exists().map_err(io)
    }

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
        let metadata = fs::symlink_metadata(index).map_err(io)?;
        let own = fs::metadata("/proc/self").map_err(io)?.uid();
        if !metadata.is_dir() || metadata.uid() != own || metadata.mode() & 0o022 != 0 {
            return Err(CommitError::InvalidInput);
        }
        let index = fs::canonicalize(index).map_err(io)?;
        if state.starts_with(&index) {
            return Err(CommitError::InvalidInput);
        }
        let directory = File::open(&index).map_err(io)?;
        directory.try_lock().map_err(|_| CommitError::Conflict)?;
        if Identity::read(&index)? != Identity::file(&directory)? {
            return Err(CommitError::BindingMismatch);
        }
        // Refuse SQLite aliases before opening it: opening may migrate state,
        // and separate journal names cannot safely coordinate a hardlinked DB.
        match fs::symlink_metadata(state) {
            Ok(metadata) => {
                if !metadata.is_file()
                    || metadata.nlink() != 1
                    || metadata.uid() != own
                    || metadata.mode() & 0o022 != 0
                {
                    return Err(CommitError::InvalidInput);
                }
                if fs::canonicalize(state).map_err(io)?.starts_with(&index) {
                    return Err(CommitError::InvalidInput);
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let parent = state.parent().ok_or(CommitError::InvalidInput)?;
                fs::DirBuilder::new()
                    .recursive(true)
                    .mode(0o700)
                    .create(parent)
                    .map_err(io)?;
                if fs::canonicalize(parent).map_err(io)?.starts_with(&index) {
                    return Err(CommitError::InvalidInput);
                }
            }
            Err(error) => return Err(io(error)),
        }
        for path in [
            index.as_path(),
            state.parent().ok_or(CommitError::InvalidInput)?,
        ] {
            for ancestor in path.ancestors() {
                let metadata = fs::symlink_metadata(ancestor).map_err(io)?;
                if !metadata.is_dir()
                    || (metadata.uid() != own && metadata.uid() != 0)
                    || (metadata.mode() & 0o022 != 0 && metadata.mode() & 0o1000 == 0)
                {
                    return Err(CommitError::InvalidInput);
                }
            }
        }
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
                legacy_journal_path(state)?,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_handoff_blocks_existing_and_reopened_readers_until_reset() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("index");
        let state = root.path().join("state.sqlite");
        fs::create_dir(&index).unwrap();
        let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v1").unwrap();
        let binding = CommitBinding::new("chunks", "workspace", &scope, "generation", 1).unwrap();
        let coordinator =
            IndexCommitCoordinator::initialize(&index, &state, binding.clone()).unwrap();
        #[cfg(feature = "local-lancedb")]
        assert!(coordinator.search_guard().is_ok());
        fs::write(legacy_journal_path(&state).unwrap(), b"obsolete").unwrap();
        #[cfg(feature = "local-lancedb")]
        assert!(matches!(
            coordinator.search_guard(),
            Err(CommitError::Conflict)
        ));
        assert!(matches!(
            IndexCommitCoordinator::open_read_only(&index, &state, binding.clone()),
            Err(CommitError::Conflict)
        ));
        assert!(matches!(
            IndexCommitCoordinator::open_existing(&index, &state, binding),
            Err(CommitError::Conflict)
        ));
        drop(coordinator);
        IndexCommitCoordinator::reset_cache(&index, &state, 1).unwrap();
        assert!(!IndexCommitCoordinator::has_legacy_cache_journal(&state).unwrap());
    }

    #[test]
    fn legacy_handoff_artifact_is_discarded_without_resuming_it() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("index");
        let state = root.path().join("state.sqlite");
        let journal = root.path().join("state.sqlite.lexical-transition-v1.json");
        assert!(!IndexCommitCoordinator::has_legacy_cache_journal(&state).unwrap());
        fs::write(&journal, b"unreadable obsolete handoff contents").unwrap();
        assert!(IndexCommitCoordinator::has_legacy_cache_journal(&state).unwrap());
        IndexCommitCoordinator::reset_cache(&index, &state, 1).unwrap();
        assert!(!journal.exists());
        assert!(
            SqliteSyncStateStore::open(&state)
                .unwrap()
                .cache_rebuild_pending()
                .unwrap()
        );
    }

    #[test]
    fn refused_state_aliases_preserve_operational_and_derived_rows() {
        let root = tempfile::tempdir().unwrap();
        let index = root.path().join("index");
        let state = root.path().join("state.sqlite");
        let store = SqliteSyncStateStore::open(&state).unwrap();
        store.lock_connection().unwrap().execute_batch(
            "CREATE TABLE external_write_ledger(id TEXT); INSERT INTO external_write_ledger VALUES('done');
             INSERT INTO page_sync_state VALUES('page','hash',NULL,0,0);"
        ).unwrap();
        drop(store);
        let before = fs::read(&state).unwrap();
        for hardlink in [false, true] {
            let alias = root.path().join("alias.sqlite");
            if hardlink {
                fs::hard_link(&state, &alias).unwrap();
            } else {
                std::os::unix::fs::symlink(&state, &alias).unwrap();
            }
            assert!(IndexCommitCoordinator::reset_cache(&index, &alias, 1).is_err());
            assert_eq!(fs::read(&state).unwrap(), before);
            fs::remove_file(alias).unwrap();
        }
    }
}
