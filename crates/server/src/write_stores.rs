//! Operator-configured mutation ledger and audit store paths for integration writes.
use crate::config::{ConfigError, invalid};
use std::{ffi::OsString, path::PathBuf};

#[derive(Debug)]
pub struct Settings {
    pub ledger: PathBuf,
    pub audit: PathBuf,
}
impl Settings {
    pub(crate) fn parse_keys(
        lookup: &mut impl FnMut(&str) -> Option<OsString>,
        enabled: bool,
        activation: &'static str,
        ledger_key: &'static str,
        audit_key: &'static str,
    ) -> Result<Option<Self>, ConfigError> {
        let ledger = lookup(ledger_key);
        let audit = lookup(audit_key);
        if !enabled {
            if ledger.is_some() || audit.is_some() {
                return Err(invalid(
                    activation,
                    "write stores require explicit write activation",
                ));
            }
            return Ok(None);
        }
        if !cfg!(all(unix, feature = "webhook-worker")) {
            return Err(invalid(
                activation,
                "requires Unix and the webhook-worker feature",
            ));
        }
        let path = |value: Option<OsString>, setting| {
            let path = PathBuf::from(value.ok_or_else(|| invalid(setting, "required for writes"))?);
            crate::file_ingestion::validate_audit_path(&path).map_err(|_| {
                invalid(
                    setting,
                    "requires an absolute private owner-controlled durable file path",
                )
            })?;
            Ok::<_, ConfigError>(path)
        };
        let settings = Self {
            ledger: path(ledger, ledger_key)?,
            audit: path(audit, audit_key)?,
        };
        if settings.ledger == settings.audit {
            return Err(invalid(
                audit_key,
                "must be separate from the mutation ledger",
            ));
        }
        Ok(Some(settings))
    }
}

#[cfg(unix)]
pub fn validate_stores(
    config: &crate::config::Config,
    stores: &Settings,
) -> Result<(), &'static str> {
    use std::os::unix::fs::MetadataExt;
    let mut paths = vec![stores.ledger.as_path(), stores.audit.as_path()];
    if let Some(files) = &config.file_ingestion {
        paths.push(files.audit_file.as_path());
    }
    if let Some(state) = config.webhook_state_file.as_deref() {
        paths.push(state);
    }
    if let Some(grant) = &config.notion_grant_store {
        for write in &paths {
            for secret in [&grant.key_file, &grant.state_file] {
                if *write == secret
                    || std::fs::metadata(write)
                        .ok()
                        .zip(std::fs::metadata(secret).ok())
                        .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino())
                {
                    return Err("mutation stores must be separate from credential storage");
                }
            }
        }
    }
    for path in &paths {
        crate::file_ingestion::validate_audit_path(path)
            .map_err(|_| "mutation storage must be private and operator-owned")?;
    }
    for (i, a) in paths.iter().enumerate() {
        for b in &paths[i + 1..] {
            if a == b
                || std::fs::metadata(a)
                    .ok()
                    .zip(std::fs::metadata(b).ok())
                    .is_some_and(|(a, b)| a.dev() == b.dev() && a.ino() == b.ino())
            {
                return Err("mutation stores must be distinct");
            }
        }
        if let Some(settings) = &config.retrieval {
            let index = std::fs::canonicalize(&settings.index)
                .map_err(|_| "existing retrieval index unavailable")?;
            let parent = std::fs::canonicalize(a.parent().ok_or("invalid write store path")?)
                .map_err(|_| "write store parent unavailable")?;
            let target = parent.join(a.file_name().ok_or("invalid write store path")?);
            if target.starts_with(index) {
                return Err("mutation stores must be outside disposable index");
            }
        }
    }
    Ok(())
}

#[cfg(all(test, unix, feature = "webhook-worker"))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    fn private() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        dir
    }
    fn parse(
        lookup: &mut impl FnMut(&str) -> Option<std::ffi::OsString>,
        enabled: bool,
    ) -> Result<Option<Settings>, ConfigError> {
        Settings::parse_keys(
            lookup,
            enabled,
            "NK_INTEGRATION_WRITES",
            "NK_INTEGRATION_WRITE_LEDGER_FILE",
            "NK_INTEGRATION_WRITE_AUDIT_FILE",
        )
    }
    fn validate(config: &crate::config::Config) -> Result<(), &'static str> {
        validate_stores(config, config.integration_write_stores.as_ref().unwrap())
    }
    #[test]
    fn write_stores_require_explicit_activation_and_complete_private_paths() {
        let dir = private();
        let ledger = dir.path().join("ledger.sqlite");
        let audit = dir.path().join("audit.sqlite");
        let lookup = |key: &str| match key {
            "NK_INTEGRATION_WRITE_LEDGER_FILE" => Some(ledger.clone().into_os_string()),
            "NK_INTEGRATION_WRITE_AUDIT_FILE" => Some(audit.clone().into_os_string()),
            _ => None,
        };
        assert!(parse(&mut |_| None, false).unwrap().is_none());
        assert!(parse(&mut lookup.clone(), false).is_err());
        assert!(parse(&mut lookup.clone(), true).is_ok());
        for missing in [
            "NK_INTEGRATION_WRITE_LEDGER_FILE",
            "NK_INTEGRATION_WRITE_AUDIT_FILE",
        ] {
            assert!(
                parse(
                    &mut |key| if key == missing { None } else { lookup(key) },
                    true
                )
                .is_err()
            );
        }
        assert!(parse(&mut |_| Some(ledger.clone().into_os_string()), true).is_err());
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(parse(&mut lookup.clone(), true).is_err());
    }
    #[test]
    fn initialization_rejects_absent_state_aliases_and_canonical_index_containment() {
        use std::os::unix::fs::symlink;
        let dir = private();
        let index = dir.path().join("index");
        std::fs::create_dir(&index).unwrap();
        std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = dir.path().join("index-alias");
        symlink(&index, &alias).unwrap();
        let ledger = index.join("not-created.sqlite");
        let mut config = crate::config::Config::from_lookup(|_| None).unwrap();
        config.integration_write_stores = Some(Settings {
            ledger: ledger.clone(),
            audit: dir.path().join("audit"),
        });
        config.webhook_state_file = Some(ledger.clone());
        assert!(validate(&config).is_err());
        assert!(!ledger.exists());
        config.webhook_state_file = Some(dir.path().join("state"));
        config.retrieval = Some(crate::webhook_worker::WorkerSettings {
            projection_mode: Default::default(),
            lexical: None,
            index: alias,
            assets: dir.path().join("assets"),
            poll_ms: 100,
            timeout_seconds: 20,
            inventory_timeout_seconds: 900,
            alias_sources: notion_knowledge_core::aliases::AliasSources::new(true, vec![]).unwrap(),
        });
        assert_eq!(
            validate(&config),
            Err("mutation stores must be outside disposable index")
        );
        assert!(!ledger.exists());
        config.retrieval = None;
        config.notion_grant_store = Some(crate::config::GrantStoreSettings {
            key_file: dir.path().join("key"),
            state_file: ledger.clone(),
        });
        assert!(validate(&config).is_err());
        assert!(!ledger.exists());
    }

    #[test]
    fn durable_write_files_cannot_alias_each_other_state_or_credentials() {
        let dir = private();
        let ledger = dir.path().join("ledger.sqlite");
        let audit = dir.path().join("audit.sqlite");
        let state = dir.path().join("state.sqlite");
        let mut config = crate::config::Config::from_lookup(|_| None).unwrap();
        config.integration_write_stores = Some(Settings {
            ledger: ledger.clone(),
            audit: audit.clone(),
        });
        config.webhook_state_file = Some(state.clone());
        assert!(validate(&config).is_ok());
        std::fs::write(&ledger, b"fixture").unwrap();
        std::fs::set_permissions(&ledger, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::hard_link(&ledger, &audit).unwrap();
        assert!(validate(&config).is_err());
        std::fs::remove_file(&audit).unwrap();
        std::fs::hard_link(&ledger, &state).unwrap();
        assert!(validate(&config).is_err());
        std::fs::remove_file(&state).unwrap();
        config.notion_grant_store = Some(crate::config::GrantStoreSettings {
            key_file: ledger.clone(),
            state_file: dir.path().join("grant"),
        });
        assert!(validate(&config).is_err());
    }
}

#[cfg(not(unix))]
pub fn validate_stores(_: &crate::config::Config, _: &Settings) -> Result<(), &'static str> {
    Err("Mutation stores require Unix")
}
