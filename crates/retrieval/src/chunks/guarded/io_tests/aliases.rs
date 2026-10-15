//! Alias effects through the real owned vector/SQLite commit boundary.
use super::*;
use crate::commit::AliasProjection;
use notion_knowledge_core::{
    aliases::{AliasSources, PageAlias, PageAliasStore},
    graph::{GraphEdge, GraphEdgeStore, GraphTarget},
    indexed::PropertyValue,
};

fn sources() -> AliasSources {
    AliasSources::new(true, vec!["names".into()]).unwrap()
}
fn projection(f: &Fixture, title: &str) -> AliasProjection {
    let mut metadata = chunk("one", "same", title).metadata;
    metadata.properties.insert(
        "names".into(),
        PropertyValue::Strings(vec!["Öresund".into(), "Oresund".into(), "ÖRESUND".into()]),
    );
    AliasProjection::from_indexed(f.coordinator.binding(), &metadata, &sources()).unwrap()
}
async fn commit_with(
    f: &Fixture,
    coordinator: Arc<IndexCommitCoordinator>,
    id: &str,
    hash: &str,
    aliases: Option<AliasProjection>,
    retry: bool,
) -> Result<CommitOutcome, CommitError> {
    let table =
        GuardedChunkTable::bind(&f.index, "chunks", f.embedding.clone(), coordinator).unwrap();
    let tombstone = hash == "deleted";
    let mut op = operation(id, hash, tombstone);
    if retry {
        op = PageOperation::retryable_lifecycle(
            id,
            "revision-v1",
            op.action(),
            op.checkpoint().clone(),
        )
        .unwrap();
    }
    if let Some(aliases) = aliases {
        op = op.with_alias_projection(aliases).unwrap();
    }
    // Relations and aliases must succeed together in the final owner transaction.
    op = op
        .with_relation_projection(relation_projection(
            f,
            (!tombstone).then_some(hash),
            tombstone,
        ))
        .unwrap();
    let rows = if tombstone {
        vec![]
    } else {
        vec![chunk("one", hash, hash)]
    };
    let prepared = table
        .prepare_page(&CountingProvider::new(f.embedding.clone()), op, &rows)
        .await
        .unwrap();
    table
        .commit_page(prepared, f.lease.clone(), clock(), || async { Ok(()) })
        .await
}
async fn commit(
    f: &Fixture,
    id: &str,
    hash: &str,
    aliases: AliasProjection,
) -> Result<CommitOutcome, CommitError> {
    commit_with(f, f.coordinator.clone(), id, hash, Some(aliases), false).await
}
fn lookup(f: &Fixture, root: &str, name: &str) -> Vec<String> {
    f.coordinator
        .state()
        .lookup_alias("workspace", root, name)
        .unwrap()
}

#[tokio::test]
async fn canonical_alias_effects_compare_exact_rows_configuration_zero_and_tombstones() {
    let f = Fixture::new();
    f.create().await;
    // Another namespace and the link family are outside the replacement.
    f.coordinator
        .state()
        .replace_page_aliases(
            "workspace",
            "other-root",
            "page",
            &[PageAlias::new("Keep", "title").unwrap()],
        )
        .unwrap();
    let link = GraphEdge::new(
        "page".into(),
        GraphTarget::Page {
            page_id: "target".into(),
        },
        "link:page".into(),
        "markdown:0".into(),
    )
    .unwrap();
    f.coordinator
        .state()
        .replace_page_link_edges("page", std::slice::from_ref(&link))
        .unwrap();
    let initial = projection(&f, "Svenska");
    assert!(!f.coordinator.alias_projection_complete(&initial).unwrap());
    assert_eq!(
        commit(&f, "aliases-first", "same", initial.clone()).await,
        Ok(CommitOutcome::Applied)
    );
    assert!(f.coordinator.alias_projection_complete(&initial).unwrap());
    assert_eq!(lookup(&f, "root", "öresund"), vec!["page"]);
    assert_eq!(lookup(&f, "root", "Oresund"), vec!["page"]);
    let history = f.coordinator.receipt("aliases-first").unwrap();
    assert_eq!(
        commit(&f, "aliases-first", "same", initial.clone()).await,
        Ok(CommitOutcome::AlreadyApplied)
    );
    // Same vector/source hash, changed alias-only input.
    let renamed = projection(&f, "English");
    assert_eq!(
        commit(&f, "aliases-renamed", "same", renamed.clone()).await,
        Ok(CommitOutcome::Applied)
    );
    assert!(lookup(&f, "root", "Svenska").is_empty());
    assert_eq!(lookup(&f, "root", "English"), vec!["page"]);
    assert!(!f.coordinator.alias_projection_complete(&initial).unwrap());
    // Same aliases, changed explicit configuration must require projection.
    let metadata = chunk("one", "same", "English").metadata;
    let first = AliasProjection::from_indexed(
        f.coordinator.binding(),
        &metadata,
        &AliasSources::new(true, vec![]).unwrap(),
    )
    .unwrap();
    commit(&f, "title-only", "same", first.clone())
        .await
        .unwrap();
    let changed = AliasProjection::from_indexed(
        f.coordinator.binding(),
        &metadata,
        &AliasSources::new(true, vec!["absent".into()]).unwrap(),
    )
    .unwrap();
    assert_eq!(first.aliases(), changed.aliases());
    assert!(!f.coordinator.alias_projection_complete(&changed).unwrap());
    assert_eq!(
        commit(&f, "title-only", "same", changed.clone()).await,
        Err(CommitError::ProjectionRepairRequired)
    );
    let raw = rusqlite::Connection::open(f.root.join("state.sqlite")).unwrap();
    raw.execute(
        "UPDATE graph_aliases SET display_name='tampered' WHERE root_page_id='root'",
        [],
    )
    .unwrap();
    assert!(!f.coordinator.alias_projection_complete(&first).unwrap());
    assert_eq!(
        commit(&f, "title-only", "same", first).await,
        Err(CommitError::ProjectionRepairRequired)
    );
    commit(&f, "repair-display", "same", changed.clone())
        .await
        .unwrap();
    raw.execute(
        "UPDATE graph_aliases SET alias_key='wrong' WHERE root_page_id='root'",
        [],
    )
    .unwrap();
    assert!(!f.coordinator.alias_projection_complete(&changed).unwrap());
    commit(&f, "repair-key", "same", changed.clone())
        .await
        .unwrap();
    raw.execute("DELETE FROM page_alias_projections", [])
        .unwrap();
    assert!(!f.coordinator.alias_projection_complete(&changed).unwrap());
    commit(&f, "repair-marker", "same", changed).await.unwrap();
    let empty = AliasProjection::from_indexed(
        f.coordinator.binding(),
        &metadata,
        &AliasSources::new(false, vec!["absent".into()]).unwrap(),
    )
    .unwrap();
    commit(&f, "aliases-zero", "same", empty.clone())
        .await
        .unwrap();
    assert!(f.coordinator.alias_projection_complete(&empty).unwrap());
    assert!(lookup(&f, "root", "English").is_empty());
    let tombstone =
        AliasProjection::tombstone(f.coordinator.binding(), "root", "page", &sources()).unwrap();
    assert!(!f.coordinator.alias_projection_complete(&tombstone).unwrap());
    commit(&f, "aliases-delete", "deleted", tombstone.clone())
        .await
        .unwrap();
    assert!(f.coordinator.alias_projection_complete(&tombstone).unwrap());
    assert!(!f.coordinator.alias_projection_complete(&empty).unwrap());
    assert_eq!(lookup(&f, "other-root", "Keep"), vec!["page"]);
    assert_eq!(
        f.coordinator.state().edges_from("page").unwrap(),
        vec![link]
    );
    assert_eq!(f.coordinator.receipt("aliases-first").unwrap(), history);
    raw.execute("UPDATE page_alias_projections SET version=999", [])
        .unwrap();
    assert!(!f.coordinator.alias_projection_complete(&tombstone).unwrap());
}

#[tokio::test]
async fn alias_and_ack_failures_roll_back_sqlite_and_replay_real_vector_effects_after_restart() {
    let f = Fixture::new();
    f.create().await;
    let old = projection(&f, "Old");
    commit(&f, "alias-seed", "old", old.clone()).await.unwrap();
    let old_checkpoint = f.coordinator.state().page_state("page").unwrap();
    let old_relation = relation_projection(&f, Some("old"), false);
    let next = projection(&f, "New");
    let raw = rusqlite::Connection::open(f.root.join("state.sqlite")).unwrap();
    for (id, trigger) in [
        (
            "alias-insert-failure",
            "CREATE TRIGGER injected BEFORE INSERT ON graph_aliases BEGIN SELECT RAISE(ABORT,'fixture'); END;",
        ),
        (
            "alias-marker-failure",
            "CREATE TRIGGER injected BEFORE INSERT ON page_alias_projections BEGIN SELECT RAISE(ABORT,'fixture'); END;",
        ),
        (
            "alias-ack-failure",
            "CREATE TRIGGER injected BEFORE UPDATE OF status ON index_commit_receipts WHEN NEW.status=1 BEGIN SELECT RAISE(ABORT,'fixture'); END;",
        ),
    ] {
        raw.execute_batch(trigger).unwrap();
        assert_eq!(
            commit(&f, id, "new", next.clone()).await,
            Err(CommitError::Unavailable)
        );
        assert_eq!(
            f.guarded().page_chunks("page").await.unwrap()[0].text,
            "new"
        );
        assert_eq!(
            f.coordinator.state().page_state("page").unwrap(),
            old_checkpoint
        );
        assert!(f.coordinator.alias_projection_complete(&old).unwrap());
        assert!(
            f.coordinator
                .relation_projection_complete(&old_relation)
                .unwrap()
        );
        assert!(!f.coordinator.alias_projection_complete(&next).unwrap());
        assert_eq!(
            f.coordinator.receipt(id).unwrap().unwrap().status,
            ReceiptStatus::Pending
        );
        assert!(f.guarded().search_snapshot().await.is_err());
        raw.execute_batch("DROP TRIGGER injected").unwrap();
        let reopened = IndexCommitCoordinator::open_existing(
            &f.index,
            f.root.join("state.sqlite"),
            f.coordinator.binding().clone(),
        )
        .unwrap();
        assert_eq!(
            commit_with(&f, reopened.clone(), id, "new", Some(next.clone()), false).await,
            Ok(CommitOutcome::Applied)
        );
        assert!(reopened.alias_projection_complete(&next).unwrap());
        commit(&f, &format!("restore-{id}"), "old", old.clone())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn alias_repairs_keep_immutable_history_and_unfinished_identity_when_source_reverts() {
    let f = Fixture::new();
    f.create().await;
    let old = projection(&f, "Old");
    commit(&f, "immutable-alias", "old", old.clone())
        .await
        .unwrap();
    let history = f.coordinator.receipt("immutable-alias").unwrap();
    let next = projection(&f, "New");
    assert_eq!(
        commit(&f, "immutable-alias", "old", next.clone()).await,
        Err(CommitError::ProjectionRepairRequired)
    );
    let repair = f
        .coordinator
        .alias_repair_id("immutable-alias", "page")
        .unwrap();
    assert_ne!(repair, "immutable-alias");
    let raw = rusqlite::Connection::open(f.root.join("state.sqlite")).unwrap();
    raw.execute_batch("CREATE TRIGGER injected BEFORE INSERT ON graph_aliases BEGIN SELECT RAISE(ABORT,'fixture'); END;").unwrap();
    assert_eq!(
        commit_with(&f, f.coordinator.clone(), &repair, "new", Some(next), true).await,
        Err(CommitError::Unavailable)
    );
    raw.execute_batch("DROP TRIGGER injected").unwrap();
    assert_eq!(
        commit_with(
            &f,
            f.coordinator.clone(),
            &repair,
            "new",
            Some(old.clone()),
            false
        )
        .await,
        Err(CommitError::Conflict)
    );
    let reopened = IndexCommitCoordinator::open_existing(
        &f.index,
        f.root.join("state.sqlite"),
        f.coordinator.binding().clone(),
    )
    .unwrap();
    assert_eq!(
        reopened.alias_repair_id("immutable-alias", "page").unwrap(),
        repair
    );
    assert_eq!(
        reopened
            .unfinished_alias_repair_id("immutable-alias", "page")
            .unwrap(),
        Some(repair.clone())
    );
    // Old SQLite aliases still match, but new Lance effects remain unfinished.
    assert!(reopened.alias_projection_complete(&old).unwrap());
    assert_eq!(
        commit(&f, "immutable-alias", "old", old.clone()).await,
        Err(CommitError::ProjectionRepairRequired)
    );
    assert_eq!(
        commit_with(
            &f,
            reopened.clone(),
            &repair,
            "old",
            Some(old.clone()),
            true
        )
        .await,
        Ok(CommitOutcome::Applied)
    );
    assert_eq!(
        f.guarded().page_chunks("page").await.unwrap()[0].text,
        "old"
    );
    assert_eq!(
        reopened
            .unfinished_alias_repair_id("immutable-alias", "page")
            .unwrap(),
        None
    );
    assert_ne!(
        reopened.alias_repair_id("immutable-alias", "page").unwrap(),
        repair
    );
    assert_eq!(reopened.receipt("immutable-alias").unwrap(), history);
    // Omitting an alias proposal cannot bypass an immutable Applied effect.
    assert_eq!(
        commit_with(&f, reopened.clone(), "immutable-alias", "old", None, false).await,
        Err(CommitError::ProjectionRepairRequired)
    );
    assert_eq!(
        reopened.alias_repair_id("immutable-alias", "different-page"),
        Err(CommitError::Conflict)
    );
    // A failed repair is also retained and retryable.
    let second = reopened.alias_repair_id("immutable-alias", "page").unwrap();
    let prepared = f
        .guarded()
        .prepare_page(
            &CountingProvider::new(f.embedding.clone()),
            operation(&second, "old", false)
                .with_alias_projection(old)
                .unwrap(),
            &[chunk("one", "old", "old")],
        )
        .await
        .unwrap();
    assert_eq!(
        f.guarded()
            .commit_page(prepared, f.lease.clone(), clock(), || async {
                Err(FailureClass::Unavailable)
            })
            .await,
        Err(CommitError::Operation(FailureClass::Unavailable))
    );
    assert_eq!(
        reopened.alias_repair_id("immutable-alias", "page").unwrap(),
        second
    );
    assert_eq!(
        reopened.receipt(&second).unwrap().unwrap().status,
        ReceiptStatus::Failed
    );
}

#[tokio::test]
async fn alias_proposals_require_current_namespace_page_generation_and_final_lease_authority() {
    let f = Fixture::new();
    f.create().await;
    let valid = projection(&f, "Bound");
    let mut metadata = chunk("one", "same", "Bound").metadata;
    metadata.source.workspace_id = "other-workspace".into();
    assert_eq!(
        AliasProjection::from_indexed(f.coordinator.binding(), &metadata, &sources()),
        Err(CommitError::BindingMismatch)
    );
    metadata.source.workspace_id = "workspace".into();
    metadata.source.root_page_id = "other-root".into();
    assert_eq!(
        AliasProjection::from_indexed(f.coordinator.binding(), &metadata, &sources()),
        Err(CommitError::BindingMismatch)
    );
    metadata.source.root_page_id = "root".into();
    metadata.page_id = "other-page".into();
    let other =
        AliasProjection::from_indexed(f.coordinator.binding(), &metadata, &sources()).unwrap();
    assert_eq!(
        operation("wrong-page", "same", false).with_alias_projection(other),
        Err(CommitError::InvalidInput)
    );
    metadata.page_id = "page".into();
    metadata.title = "\nunsafe".into();
    assert_eq!(
        AliasProjection::from_indexed(f.coordinator.binding(), &metadata, &sources()),
        Err(CommitError::InvalidInput)
    );
    assert!(
        f.coordinator
            .receipt("extraction-failure")
            .unwrap()
            .is_none()
    );
    commit(&f, "alias-authority", "same", valid.clone())
        .await
        .unwrap();
    let reader = IndexCommitCoordinator::open_read_only(
        &f.index,
        f.root.join("state.sqlite"),
        f.coordinator.binding().clone(),
    )
    .unwrap();
    let before = fs::read(f.root.join("state.sqlite")).unwrap();
    assert!(reader.alias_projection_complete(&valid).unwrap());
    assert_eq!(fs::read(f.root.join("state.sqlite")).unwrap(), before);
    let prepared = f
        .guarded()
        .prepare_page(
            &CountingProvider::new(f.embedding.clone()),
            operation("alias-expired", "new", false)
                .with_alias_projection(projection(&f, "New"))
                .unwrap(),
            &[chunk("one", "new", "New")],
        )
        .await
        .unwrap();
    let time = Arc::new(std::sync::atomic::AtomicI64::new(10));
    let now = time.clone();
    assert_eq!(
        f.guarded()
            .commit_page(
                prepared,
                f.lease.clone(),
                Arc::new(move || now.load(Ordering::SeqCst)),
                move || async move {
                    time.store(110, Ordering::SeqCst);
                    Ok(())
                }
            )
            .await,
        Err(CommitError::Operation(FailureClass::Conflict))
    );
    assert!(f.coordinator.alias_projection_complete(&valid).unwrap());
    // Supersede unfinished operation explicitly before changing generation.
    let scope = ReconciliationScope::new(vec!["root".into()], vec![], "policy", "v2").unwrap();
    let binding = CommitBinding::new("chunks", "workspace", &scope, "v2", 2).unwrap();
    let next = f
        .coordinator
        .supersede_scope(binding, &f.lease, 10, &["alias-expired".into()])
        .unwrap();
    assert_eq!(
        next.alias_projection_complete(&valid),
        Err(CommitError::BindingMismatch)
    );
    assert_eq!(
        next.alias_repair_id("alias-authority", "page"),
        Err(CommitError::Conflict)
    );
    let table =
        GuardedChunkTable::bind(&f.index, "chunks", f.embedding.clone(), next.clone()).unwrap();
    let prepared = table
        .prepare_page(
            &CountingProvider::new(f.embedding.clone()),
            operation("cross-generation", "same", false)
                .with_alias_projection(valid)
                .unwrap(),
            &[chunk("one", "same", "same")],
        )
        .await
        .unwrap();
    assert_eq!(
        table
            .commit_page(prepared, f.lease.clone(), clock(), || async {
                panic!("invalid generation must fail before source/vector effects")
            })
            .await,
        Err(CommitError::BindingMismatch)
    );
    assert!(next.receipt("cross-generation").unwrap().is_none());
}

#[tokio::test]
async fn alias_commit_owns_cancellation_and_retains_source_authority_through_acknowledgment() {
    struct Authority {
        coordinator: Arc<IndexCommitCoordinator>,
        observed: Arc<std::sync::Mutex<Option<ReceiptStatus>>>,
    }
    impl Drop for Authority {
        fn drop(&mut self) {
            *self.observed.lock().unwrap() = self
                .coordinator
                .receipt("alias-owned")
                .unwrap()
                .map(|r| r.status);
        }
    }
    let f = Fixture::new();
    f.create().await;
    let aliases = projection(&f, "Owned");
    let observed = Arc::new(std::sync::Mutex::new(None));
    let authority = Authority {
        coordinator: f.coordinator.clone(),
        observed: observed.clone(),
    };
    let prepared = f
        .guarded()
        .prepare_page(
            &CountingProvider::new(f.embedding.clone()),
            operation("alias-owned", "same", false)
                .with_alias_projection(aliases.clone())
                .unwrap(),
            &[chunk("one", "same", "same")],
        )
        .await
        .unwrap();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let observer = f.guarded().commit_page_with_authority(
        prepared,
        f.lease.clone(),
        clock(),
        move || async move {
            entered_tx.send(()).unwrap();
            release_rx.await.unwrap();
            Ok(authority)
        },
    );
    entered_rx.await.unwrap();
    drop(observer);
    assert_eq!(
        f.coordinator
            .receipt("alias-owned")
            .unwrap()
            .unwrap()
            .status,
        ReceiptStatus::Pending
    );
    release_tx.send(()).unwrap();
    f.coordinator.drain_owned().await.unwrap();
    assert_eq!(*observed.lock().unwrap(), Some(ReceiptStatus::Applied));
    assert!(f.coordinator.alias_projection_complete(&aliases).unwrap());
    // A later legacy write cannot attest an earlier alias projection is current.
    commit_with(
        &f,
        f.coordinator.clone(),
        "alias-legacy",
        "same",
        None,
        false,
    )
    .await
    .unwrap();
    assert!(!f.coordinator.alias_projection_complete(&aliases).unwrap());
    assert_eq!(lookup(&f, "root", "Owned"), vec!["page"]);
    // Adding aliases to an immutable legacy receipt needs a distinct operation.
    assert_eq!(
        commit(&f, "alias-legacy", "same", aliases).await,
        Err(CommitError::ProjectionRepairRequired)
    );
}
