//! File opt-in must preserve the installed OAuth storage and authority boundary.
use super::*;
use notion_knowledge_core::{backend::PageId, lifecycle::LifecycleScope};
use std::collections::BTreeMap;

fn parse(values: &BTreeMap<String, String>) -> Result<Config, crate::config::ConfigError> {
    Config::from_lookup(|key| values.get(key).map(Into::into))
}

#[tokio::test]
async fn file_opt_in_requires_oauth_writes_even_with_an_integration_token() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let scope = LifecycleScope {
        workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        generation: 1,
        roots: vec![PageId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into())],
        exclusions: Default::default(),
    };
    let seed = content_index::seed(
        h._dir.path(),
        &scope,
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
    )
    .await;
    let mut values = installed_writes::config_values(&h, &seed);
    assert!(parse(&values).unwrap().file_ingestion.is_none());
    values.insert("NK_FILE_INGESTION".into(), "true".into());
    values.insert("NK_FILE_SOURCE_HOSTS".into(), "files.example.com".into());
    values.insert(
        "NK_FILE_AUDIT_FILE".into(),
        h._dir
            .path()
            .join("file-audit.sqlite")
            .to_str()
            .unwrap()
            .into(),
    );
    assert!(parse(&values).unwrap().file_ingestion.is_some());
    values.insert("NK_READ_ONLY".into(), "true".into());
    assert!(parse(&values).is_err());
    values.insert("NK_READ_ONLY".into(), "false".into());
    values.insert("NK_OAUTH_WRITES".into(), "false".into());
    values.insert(
        "NOTION_TOKEN".into(),
        "integration-must-not-authorize-oauth-files".into(),
    );
    assert!(parse(&values).is_err());
}

#[tokio::test]
async fn file_audit_cannot_alias_credentials_operational_state_or_disposable_index() {
    let h = Harness::new(grant("workspace-a", "user-a"), StatusCode::OK).await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let scope = LifecycleScope {
        workspace_id: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa".into(),
        generation: 1,
        roots: vec![PageId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb".into())],
        exclusions: Default::default(),
    };
    let seed = content_index::seed(
        h._dir.path(),
        &scope,
        "cccccccc-cccc-cccc-cccc-cccccccccccc",
    )
    .await;
    let mut values = installed_writes::config_values(&h, &seed);
    values.insert("NK_FILE_INGESTION".into(), "true".into());
    values.insert("NK_FILE_SOURCE_HOSTS".into(), "files.example.com".into());
    let audit = h._dir.path().join("file-audit.sqlite");
    values.insert("NK_FILE_AUDIT_FILE".into(), audit.to_str().unwrap().into());
    assert!(crate::oauth_writes::validate_distinct(&parse(&values).unwrap()).is_ok());
    for key in [
        "NK_NOTION_GRANT_KEY_FILE",
        "NK_NOTION_GRANT_STATE_FILE",
        "NK_OAUTH_WRITE_LEDGER_FILE",
        "NK_OAUTH_WRITE_AUDIT_FILE",
        "NK_WEBHOOK_STATE_FILE",
    ] {
        let target = values[key].clone();
        values.insert("NK_FILE_AUDIT_FILE".into(), target);
        assert!(
            parse(&values).map_or(true, |config| crate::oauth_writes::validate_distinct(
                &config
            )
            .is_err()),
            "alias of {key}"
        );
    }
    values.insert(
        "NK_FILE_AUDIT_FILE".into(),
        seed.index
            .join("file-audit.sqlite")
            .to_str()
            .unwrap()
            .into(),
    );
    assert!(parse(&values).map_or(true, |config| {
        crate::oauth_writes::validate_distinct(&config).is_err()
    }));
    values.insert("NK_FILE_AUDIT_FILE".into(), audit.to_str().unwrap().into());
    std::fs::write(&audit, b"private audit").unwrap();
    std::fs::set_permissions(&audit, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        parse(&values).is_err(),
        "file audit must not be world-readable"
    );
    std::fs::remove_file(&audit).unwrap();
    std::os::unix::fs::symlink(h._dir.path().join("key"), &audit).unwrap();
    assert!(
        parse(&values).is_err(),
        "file audit must not follow a credential symlink"
    );
    std::fs::remove_file(&audit).unwrap();
    std::fs::hard_link(h._dir.path().join("key"), &audit).unwrap();
    values.insert("NK_FILE_AUDIT_FILE".into(), audit.to_str().unwrap().into());
    assert!(parse(&values).map_or(true, |config| {
        crate::oauth_writes::validate_distinct(&config).is_err()
    }));
}
