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
}
#[cfg(unix)]
fn run() -> Result<(), Box<dyn std::error::Error>> {
    use notion_knowledge_core::reconciliation::ReconciliationScope;
    use notion_knowledge_retrieval::{
        chunks::{CandidateLimits, OfflineLexicalSource},
        commit::{CommitBinding, IndexCommitCoordinator},
        lexical_dictionary::Dictionary,
    };
    use std::{io::Read, os::unix::fs::OpenOptionsExt};
    let mut args = std::env::args_os().skip(1);
    let action = args
        .next()
        .ok_or("expected prepare|verify and trusted configuration path")?;
    let path = args.next().ok_or("expected configuration path")?;
    if args.next().is_some() || (action != "prepare" && action != "verify") {
        return Err("invalid arguments".into());
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)?;
    if !file.metadata()?.is_file() {
        return Err("configuration must be regular".into());
    }
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
    let coordinator =
        IndexCommitCoordinator::open(&config.index_directory, &config.state_file, current)?;
    let source = OfflineLexicalSource::bind(
        &config.index_directory,
        &config.table,
        config.embedding,
        coordinator,
    )?;
    let dictionary = Dictionary::load(&config.asset, &config.asset_sha256)
        .map_err(|_| "incompatible verified asset")?;
    let now = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
    )?;
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
