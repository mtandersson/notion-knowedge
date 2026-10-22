//! Explicit operator recovery of accepted file intents; never source replay.
use crate::config::Config;

#[derive(Debug, PartialEq, Eq)]
enum Action {
    List,
    Status,
    Release,
}
#[derive(Debug, PartialEq, Eq)]
struct Command {
    action: Action,
    page: String,
    digest: String,
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
impl Command {
    fn parse(args: &[String]) -> Result<Self, &'static str> {
        if args == ["--file-recovery", "list"] {
            return Ok(Self {
                action: Action::List,
                page: String::new(),
                digest: String::new(),
            });
        }
        let [
            command,
            action,
            page_flag,
            page,
            digest_flag,
            digest,
            rest @ ..,
        ] = args
        else {
            return Err("invalid file recovery arguments");
        };
        if command != "--file-recovery"
            || page_flag != "--page"
            || digest_flag != "--digest"
            || !uuid(page)
            || digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid file recovery arguments");
        }
        let action = match (action.as_str(), rest) {
            ("status", []) => Action::Status,
            ("release", [stopped, quiescent])
                if stopped == "--all-source-owners-stopped"
                    && quiescent == "--provider-quiescent" =>
            {
                Action::Release
            }
            _ => {
                return Err(
                    "release requires explicit stopped-owner and provider-quiescence confirmations",
                );
            }
        };
        Ok(Self {
            action,
            page: page.clone(),
            digest: digest.clone(),
        })
    }
}

pub async fn run(config: &Config, args: &[String]) -> Result<serde_json::Value, &'static str> {
    let command = Command::parse(args)?;
    #[cfg(not(all(unix, feature = "webhook-worker")))]
    {
        let _ = (config, command);
        Err("file recovery requires Unix and the webhook-worker feature")
    }
    #[cfg(all(unix, feature = "webhook-worker"))]
    execute(config, command, || {
        crate::notion_oauth_runtime::NotionOAuthRuntime::from_config(config)
            .map_err(|_| "exclusive original grant unavailable; stop credential users")?
            .ok_or("OAuth original grant configuration required")
    })
    .await
}

/// Local-provider seam: parser, configuration validation, state and operations
/// remain identical to the shipped operator command.
#[cfg(all(test, unix, feature = "webhook-worker"))]
pub(crate) async fn run_fixture(
    config: &Config,
    args: &[String],
    runtime: crate::notion_oauth_runtime::NotionOAuthRuntime,
) -> Result<serde_json::Value, &'static str> {
    execute(config, Command::parse(args)?, || Ok(runtime)).await
}

#[cfg(all(unix, feature = "webhook-worker"))]
async fn execute(
    config: &Config,
    command: Command,
    runtime: impl FnOnce() -> Result<crate::notion_oauth_runtime::NotionOAuthRuntime, &'static str>,
) -> Result<serde_json::Value, &'static str> {
    use notion_knowledge_core::write_refresh::RefreshKey;
    use notion_knowledge_retrieval::sync_state::SqliteSyncStateStore;
    if !config.oauth_writes || config.file_ingestion.is_none() {
        return Err("explicit OAuth file ingestion configuration required");
    }
    crate::oauth_writes::validate_distinct(config)
        .map_err(|_| "distinct protected operational storage required")?;
    config
        .validate_grant_store()
        .map_err(|_| "approved original grant unavailable")?;
    let scope = config
        .notion_scope
        .as_ref()
        .ok_or("configured physical scope required")?;
    let path = config
        .webhook_state_file
        .as_deref()
        .ok_or("existing operational state required")?;
    let state = SqliteSyncStateStore::open_existing(path)
        .map_err(|_| "current operational state unavailable")?;
    let runtime = runtime()?;
    let key = RefreshKey {
        workspace_id: scope.workspace_id.clone(),
        page_id: command.page,
        mutation_digest: command.digest,
    };
    match command.action {
        Action::List => crate::file_refresh::operator_list(&state, &runtime, scope)
            .await
            .map_err(|_| "original grant or scope unavailable"),
        Action::Status => crate::file_refresh::operator_status(&state, &runtime, scope, &key)
            .await
            .map_err(|_| "original-grant file intent status unavailable"),
        Action::Release => {
            crate::file_refresh::reconcile_operator_quiescent(
                &state,
                &runtime,
                scope,
                &key,
                crate::file_refresh::SourceQuiescence::OperatorConfirmed,
            )
            .await
            .map_err(|_| "file intent release denied; retain recovery evidence")?;
            Ok(
                serde_json::json!({"source_replayed":false,"recovery":"released","index_status":"queued"}),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const PAGE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    fn args(action: &str) -> Vec<String> {
        [
            "--file-recovery",
            action,
            "--page",
            PAGE,
            "--digest",
            &"b".repeat(64),
        ]
        .into_iter()
        .map(str::to_owned)
        .collect()
    }
    #[test]
    fn status_accepts_only_exact_receipt_identifiers() {
        let command = Command::parse(&args("status")).unwrap();
        assert_eq!(command.action, Action::Status);
        assert_eq!(command.page, PAGE);
        for bad in [
            "../state",
            "https://example.test/?credential=secret",
            "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
        ] {
            let mut input = args("status");
            input[3] = bad.into();
            assert!(Command::parse(&input).is_err());
        }
        let mut input = args("status");
        input[5] = "B".repeat(64);
        assert!(Command::parse(&input).is_err());
        let mut input = args("status");
        input.push("--http".into());
        assert!(Command::parse(&input).is_err());
    }
    #[test]
    fn release_requires_both_explicit_operator_confirmations() {
        let mut input = args("release");
        assert!(Command::parse(&input).is_err());
        input.push("--all-source-owners-stopped".into());
        assert!(Command::parse(&input).is_err());
        input.push("--provider-quiescent".into());
        assert_eq!(Command::parse(&input).unwrap().action, Action::Release);
        input.push("--provider-quiescent".into());
        assert!(Command::parse(&input).is_err());
        assert!(Command::parse(&args("retry-upload")).is_err());
    }
}
