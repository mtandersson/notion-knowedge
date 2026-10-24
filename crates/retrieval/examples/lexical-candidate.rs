//! Trusted offline operator entrypoint. No credentials, network or activation.
#[cfg(unix)]
fn main() {
    if run().is_err() {
        eprintln!(
            "offline lexical preparation failed; retain artifacts and check ownership/compatibility"
        );
        std::process::exit(2);
    }
}
#[cfg(not(unix))]
fn main() {
    eprintln!("offline lexical preparation requires Unix filesystem ownership");
    std::process::exit(2);
}

#[cfg(unix)]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    index_directory: std::path::PathBuf,
    state_file: std::path::PathBuf,
    table: String,
    workspace: String,
    roots: Vec<String>,
    exclusions: Vec<String>,
    policy: String,
    scope_generation: String,
    generation: String,
    epoch: u64,
    next_generation: String,
    next_epoch: u64,
    embedding: notion_knowledge_core::embedding::EmbeddingMetadata,
    asset: std::path::PathBuf,
    asset_sha256: String,
    candidate: std::path::PathBuf,
    backup: std::path::PathBuf,
    activation: Option<ActivationConfig>,
}
#[cfg(unix)]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ActivationConfig {
    archive: std::path::PathBuf,
    backup: std::path::PathBuf,
    quarantine_generation: String,
}

#[cfg(unix)]
struct Arguments {
    action: std::ffi::OsString,
    configuration: std::path::PathBuf,
    history: Option<std::path::PathBuf>,
}

#[cfg(unix)]
fn arguments(
    mut args: impl Iterator<Item = std::ffi::OsString>,
) -> Result<Arguments, Box<dyn std::error::Error>> {
    let action = args.next().ok_or(
        "expected prepare|verify|prepare-activation|abort-prepared and trusted configuration path",
    )?;
    let path = args.next().ok_or("expected configuration path")?;
    if action == "prepare-activation" || action == "abort-prepared" {
        if args.next().as_deref() != Some(std::ffi::OsStr::new("--all-source-processes-stopped")) {
            return Err(
                "transition preparation/recovery requires explicit source-process shutdown acknowledgement"
                    .into(),
            );
        }
    } else if action != "prepare" && action != "verify" {
        return Err("invalid action".into());
    }
    let history = match args.next() {
        Some(flag) if action == "abort-prepared" && flag == "--history" => Some(
            std::path::PathBuf::from(args.next().ok_or("missing explicit history path")?),
        ),
        None => None,
        _ => return Err("unexpected arguments".into()),
    };
    if args.next().is_some() {
        return Err("unexpected arguments".into());
    }
    Ok(Arguments {
        action,
        configuration: path.into(),
        history,
    })
}
#[cfg(unix)]
fn open_configuration(
    path: &std::path::Path,
    private: bool,
) -> Result<std::fs::File, Box<dyn std::error::Error>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err("configuration must be regular".into());
    }
    if private
        && (metadata.nlink() != 1
            || metadata.uid() != std::fs::metadata("/proc/self")?.uid()
            || metadata.mode() & 0o077 != 0)
    {
        return Err(
            "activation configuration must be private, singly linked and owned by the operator"
                .into(),
        );
    }
    Ok(file)
}

#[cfg(unix)]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use notion_knowledge_core::reconciliation::ReconciliationScope;
    use notion_knowledge_retrieval::{
        chunks::{CandidateLimits, OfflineLexicalSource, TransitionCandidateInputs},
        commit::transition::{
            adoption::AdoptionLimits,
            manifest::ManifestLimits,
            preparation::{ActivationPaths, prepare_activation},
        },
        commit::{CommitBinding, IndexCommitCoordinator},
        lexical_dictionary::Dictionary,
    };
    use std::io::Read;
    let Arguments {
        action,
        configuration: path,
        history,
    } = arguments(std::env::args_os().skip(1))?;
    let file = open_configuration(
        std::path::Path::new(&path),
        action == "prepare-activation" || action == "abort-prepared",
    )?;
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("configuration exceeds bound".into());
    }
    let config: Config = serde_json::from_slice(&bytes)?;
    let scope = ReconciliationScope::new(
        config.roots,
        config.exclusions,
        &config.policy,
        &config.scope_generation,
    )
    .map_err(|_| "invalid scope")?;
    let current = CommitBinding::new(
        &config.table,
        &config.workspace,
        &scope,
        &config.generation,
        config.epoch,
    )?;
    let next = CommitBinding::new(
        &config.table,
        &config.workspace,
        &scope,
        &config.next_generation,
        config.next_epoch,
    )?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;
    if action == "abort-prepared" {
        use notion_knowledge_retrieval::commit::transition::{
            TransitionOwnership, TransitionRecord,
        };
        let active = TransitionRecord::read(&config.state_file)?;
        let record = if let Some(history) = history {
            let retained = TransitionRecord::read_aborted(&config.state_file, &history)?;
            if active.as_ref().is_some_and(|record| record != &retained) {
                return Err("history does not match the active transition".into());
            }
            retained
        } else {
            active.ok_or("no active transition; supply explicit aborted history for retry")?
        };
        let activation = config
            .activation
            .as_ref()
            .ok_or("missing activation configuration")?;
        if record.source.binding != serde_json::to_string(&current)?
            || record.target.binding != serde_json::to_string(&next)?
            || record.source.path != config.index_directory
            || record.candidate_path != config.candidate
            || record.archive_path != activation.archive
            || record.backup_path != activation.backup
            || record.quarantine_generation != activation.quarantine_generation
        {
            return Err("configuration does not match the retained transition".into());
        }
        let owner = TransitionOwnership::acquire(&config.state_file, &record)?;
        let history = owner.abort_prepared(now)?;
        println!(
            "{}",
            serde_json::json!({"history":history,"source_generation":current.generation()})
        );
        return Ok(());
    }
    let coordinator = IndexCommitCoordinator::open_existing(
        &config.index_directory,
        &config.state_file,
        current.clone(),
    )?;
    let source = OfflineLexicalSource::bind(
        &config.index_directory,
        &config.table,
        config.embedding.clone(),
        coordinator.clone(),
    )?;
    let dictionary = Dictionary::load(&config.asset, &config.asset_sha256)
        .map_err(|_| "incompatible verified asset")?;
    if action == "prepare-activation" {
        let activation = config
            .activation
            .ok_or("missing activation configuration")?;
        let record = prepare_activation(
            &coordinator,
            now,
            TransitionCandidateInputs {
                source: &current,
                target: &next,
                embedding: &config.embedding,
                dictionary: &dictionary,
                preparation_backup: &config.backup,
                limits: CandidateLimits::default(),
            },
            ActivationPaths {
                candidate: &config.candidate,
                archive: &activation.archive,
                backup: &activation.backup,
                quarantine_generation: &activation.quarantine_generation,
            },
            AdoptionLimits::default(),
            ManifestLimits::default(),
            2 * 1024 * 1024 * 1024,
        )?;
        println!("{}", serde_json::to_string(&record)?);
        return Ok(());
    }
    let result = if action == "prepare" {
        source.prepare(
            &config.candidate,
            &config.backup,
            &next,
            &dictionary,
            now,
            CandidateLimits::default(),
        )?
    } else {
        source.verify(
            &config.candidate,
            &config.backup,
            &next,
            &dictionary,
            now,
            CandidateLimits::default(),
        )?
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    fn parse(args: &[&str]) -> bool {
        arguments(args.iter().map(std::ffi::OsString::from)).is_ok()
    }
    #[test]
    fn activation_preparation_requires_exact_shutdown_acknowledgement() {
        assert!(!parse(&["abort-prepared", "config.json"]));
        assert!(parse(&[
            "abort-prepared",
            "config.json",
            "--all-source-processes-stopped"
        ]));
        assert!(parse(&[
            "abort-prepared",
            "config.json",
            "--all-source-processes-stopped",
            "--history",
            "retained.history"
        ]));
        assert!(!parse(&[
            "abort-prepared",
            "config.json",
            "--all-source-processes-stopped",
            "--history"
        ]));
        assert!(!parse(&[
            "prepare-activation",
            "config.json",
            "--all-source-processes-stopped",
            "--history",
            "retained.history"
        ]));
        assert!(!parse(&["prepare-activation", "config.json"]));
        assert!(!parse(&["prepare-activation", "config.json", "--yes"]));
        assert!(parse(&[
            "prepare-activation",
            "config.json",
            "--all-source-processes-stopped"
        ]));
        assert!(!parse(&[
            "prepare-activation",
            "config.json",
            "--all-source-processes-stopped",
            "extra"
        ]));
        assert!(!parse(&[
            "activate",
            "config.json",
            "--all-source-processes-stopped"
        ]));
    }
    #[test]
    fn candidate_only_commands_keep_existing_argument_contract() {
        assert!(parse(&["prepare", "config.json"]));
        assert!(parse(&["verify", "config.json"]));
        assert!(!parse(&[
            "verify",
            "config.json",
            "--all-source-processes-stopped"
        ]));
        assert!(!parse(&["prepare"]));
    }

    #[test]
    fn activation_configuration_refuses_shared_or_linked_files() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        std::fs::write(&path, b"{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(open_configuration(&path, true).is_ok());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(open_configuration(&path, true).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let link = directory.path().join("linked.json");
        std::fs::hard_link(&path, &link).unwrap();
        assert!(open_configuration(&path, true).is_err());
        std::fs::remove_file(&link).unwrap();
        symlink(&path, &link).unwrap();
        assert!(open_configuration(&link, true).is_err());
        assert!(open_configuration(directory.path(), true).is_err());
    }
}
