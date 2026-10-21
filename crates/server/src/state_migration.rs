//! Explicit offline operational-state upgrade. No provider or index effects.
use crate::config::Config;
use std::path::{Path, PathBuf};

pub fn run(config: &Config, args: &[String]) -> Result<(), &'static str> {
    let [command, flag, backup] = args else {
        return Err("expected --migrate-operational-state --backup PATH");
    };
    if command != "--migrate-operational-state" || flag != "--backup" {
        return Err("expected --migrate-operational-state --backup PATH");
    }
    let state = config
        .webhook_state_file
        .as_deref()
        .ok_or("configured operational state required")?;
    let backup = Path::new(backup);
    crate::file_ingestion::validate_audit_path(state)
        .map_err(|_| "private operational state required")?;
    crate::file_ingestion::validate_audit_path(backup)
        .map_err(|_| "private new backup path required")?;
    let candidate = |p: &Path| -> Result<PathBuf, &'static str> {
        Ok(
            std::fs::canonicalize(p.parent().ok_or("invalid storage path")?)
                .map_err(|_| "storage parent unavailable")?
                .join(p.file_name().ok_or("invalid storage path")?),
        )
    };
    let selected = [candidate(state)?, candidate(backup)?];
    if selected[0] == selected[1] {
        return Err("state and backup must be distinct");
    }
    let mut protected = Vec::new();
    if let Some(grant) = &config.notion_grant_store {
        protected.extend([grant.key_file.as_path(), grant.state_file.as_path()]);
    }
    if let Some(stores) = &config.oauth_write_stores {
        protected.extend([stores.ledger.as_path(), stores.audit.as_path()]);
    }
    if let Some(files) = &config.file_ingestion {
        protected.push(files.audit_file.as_path());
    }
    for path in protected {
        let protected = candidate(path)?;
        if selected.contains(&protected) {
            return Err("state storage must be distinct from other durable stores");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if std::fs::metadata(path)
                .ok()
                .zip(std::fs::metadata(state).ok())
                .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino())
            {
                return Err("state storage must be distinct from other durable stores");
            }
        }
    }
    if let Some(retrieval) = &config.retrieval {
        let index =
            std::fs::canonicalize(&retrieval.index).map_err(|_| "existing index required")?;
        if selected.iter().any(|path| path.starts_with(&index)) {
            return Err("state and backup must be outside disposable index");
        }
    }
    notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::migrate_existing_with_backup(
        state, backup,
    )
    .map_err(|_| "offline migration failed; preserve the original and any created backup")
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn setup() -> (tempfile::TempDir, Config) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let mut config = Config::from_lookup(|_| None).unwrap();
        config.webhook_state_file = Some(dir.path().join("state.sqlite"));
        (dir, config)
    }
    fn args(backup: &Path) -> Vec<String> {
        vec![
            "--migrate-operational-state".into(),
            "--backup".into(),
            backup.to_str().unwrap().into(),
        ]
    }
    #[test]
    fn configured_offline_command_creates_backup_and_never_initializes_missing_state() {
        let (dir, config) = setup();
        let backup = dir.path().join("backup.sqlite");
        assert!(run(&config, &args(&backup)).is_err());
        assert!(!backup.exists());
        assert!(!config.webhook_state_file.as_ref().unwrap().exists());
        drop(
            notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(
                config.webhook_state_file.as_ref().unwrap(),
            )
            .unwrap(),
        );
        run(&config, &args(&backup)).unwrap();
        assert!(backup.is_file());
        assert!(run(&config, &args(&backup)).is_err());
    }
    #[test]
    fn argument_mixing_and_aliasing_are_rejected_before_storage_changes() {
        let (dir, config) = setup();
        let state = config.webhook_state_file.as_ref().unwrap();
        drop(notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(state).unwrap());
        let before = std::fs::read(state).unwrap();
        assert!(run(&config, &args(state)).is_err());
        let mut mixed = args(&dir.path().join("backup"));
        mixed.push("--http".into());
        assert!(run(&config, &mixed).is_err());
        assert_eq!(std::fs::read(state).unwrap(), before);
        assert!(!dir.path().join("backup").exists());
    }
}
