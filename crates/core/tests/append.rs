use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use notion_knowledge_core::{
    append::{AppendError, AppendRequest, append_once},
    backend::{
        AppendPageContent, BackendError, BackendErrorKind, BackendFuture, CreatePage, NotionRead,
        NotionWrite, Page, PageContent, PageId, ReplacePageContent,
    },
    discovery::ExclusionRules,
    idempotency::{
        IdempotencyError, MutationClaim, Reservation, VerifiedReceipt, WriteIdempotencyStore,
    },
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    root_scope::RootScopeGate,
};

const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "33333333-3333-3333-3333-333333333333";
const KEY: &str = "test-append-operation-key-1234";
const REVISION: &str = "2026-10-10T12:00:00Z";
const AFTER: &str = "2026-10-10T12:00:01Z";

fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}

fn error() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unavailable,
        operation: "fixture",
        retry_after: None,
        committed_page_id: None,
    }
}

#[derive(Clone, Copy)]
enum Mode {
    Normal,
    TimeoutAfterCommit,
    RewriteOriginal,
    ReadbackUnavailable,
}

struct Fixture {
    markdown: Mutex<String>,
    writes: AtomicUsize,
    reads: AtomicUsize,
    mode: Mode,
    deny_scope: AtomicBool,
    deny_revalidation: AtomicBool,
}
impl Fixture {
    fn new(mode: Mode) -> Arc<Self> {
        Arc::new(Self {
            markdown: Mutex::new("# Existing\n\noriginal\n".into()),
            writes: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            mode,
            deny_scope: AtomicBool::new(false),
            deny_revalidation: AtomicBool::new(false),
        })
    }

    fn page(&self) -> Page {
        Page {
            id: PageId(PAGE.into()),
            url: format!("https://www.notion.so/{PAGE}"),
            title: "Untouched title".into(),
            last_edited_time: (if self.writes.load(Ordering::SeqCst) == 0 {
                REVISION
            } else {
                AFTER
            })
            .into(),
            archived: false,
            properties: Default::default(),
        }
    }
}

impl NotionRead for Fixture {
    fn fetch_page<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(async move { Ok(self.page()) })
    }

    fn read_content<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.writes.load(Ordering::SeqCst) > 0
                && matches!(self.mode, Mode::ReadbackUnavailable)
            {
                return Err(error());
            }
            Ok(PageContent {
                page: self.page(),
                markdown: self.markdown.lock().unwrap().clone(),
            })
        })
    }
}

impl NotionWrite for Fixture {
    fn append_content(&self, request: AppendPageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            assert_eq!(request.page_id.0, PAGE);
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut markdown = self.markdown.lock().unwrap();
            if matches!(self.mode, Mode::RewriteOriginal) {
                *markdown = "# REPLACED ORIGINAL\n".into();
            }
            markdown.push_str(&request.markdown);
            if matches!(self.mode, Mode::TimeoutAfterCommit) {
                return Err(error());
            }
            Ok(self.page())
        })
    }

    fn replace_content(&self, _: ReplacePageContent) -> BackendFuture<'_, Page> {
        Box::pin(async { Err(error()) })
    }

    fn create_page(&self, _: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async { Err(error()) })
    }
}

impl PageLifecycle for Fixture {
    fn lifecycle<'a>(
        &'a self,
        _: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            if self.deny_scope.load(Ordering::SeqCst) {
                return Err(error());
            }
            Ok(LifecycleEvidence {
                scope: scope.clone(),
                status: LifecycleStatus::Allowed {
                    roots: vec![ROOT.into()],
                },
                ancestry: vec![
                    LifecycleNode {
                        kind: LifecycleKind::Page,
                        id: PAGE.into(),
                        revision: REVISION.into(),
                        inactive: false,
                        parent: PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: ROOT.into(),
                        },
                    },
                    LifecycleNode {
                        kind: LifecycleKind::Page,
                        id: ROOT.into(),
                        revision: REVISION.into(),
                        inactive: false,
                        parent: PhysicalParent::Workspace,
                    },
                ],
            })
        })
    }

    fn revalidate_lifecycle<'a>(
        &'a self,
        _: &'a LifecycleEvidence,
        _: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.deny_revalidation.load(Ordering::SeqCst) {
                Err(error())
            } else {
                Ok(())
            }
        })
    }
}

type LedgerRecord = (String, u8, Option<VerifiedReceipt>);

#[derive(Default)]
struct Ledger {
    state: Mutex<HashMap<String, LedgerRecord>>,
    starts: AtomicUsize,
}
impl WriteIdempotencyStore for Ledger {
    fn begin(&self, claim: &MutationClaim) -> Result<Reservation, IdempotencyError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let mut state = self.state.lock().unwrap();
        let Some((digest, status, receipt)) = state.get(claim.key_digest()) else {
            state.insert(
                claim.key_digest().into(),
                (claim.request_digest().into(), 0, None),
            );
            return Ok(Reservation::ExecuteOnce);
        };
        if digest != claim.request_digest() {
            return Err(IdempotencyError::KeyConflict);
        }
        Ok(if *status == 2 {
            Reservation::Replay(receipt.clone().unwrap())
        } else {
            Reservation::Reconcile
        })
    }

    fn mark_uncertain(&self, claim: &MutationClaim) -> Result<(), IdempotencyError> {
        let mut state = self.state.lock().unwrap();
        let entry = state.get_mut(claim.key_digest()).unwrap();
        entry.1 = 1;
        Ok(())
    }

    fn record_verified(
        &self,
        claim: &MutationClaim,
        receipt: &VerifiedReceipt,
    ) -> Result<(), IdempotencyError> {
        let mut state = self.state.lock().unwrap();
        let entry = state.get_mut(claim.key_digest()).unwrap();
        entry.1 = 2;
        entry.2 = Some(receipt.clone());
        Ok(())
    }
}

fn request() -> AppendRequest {
    AppendRequest {
        page_id: PageId(PAGE.into()),
        markdown: "\n## New entry\n".into(),
        expected_last_edited_time: REVISION.into(),
        expected_markdown_sha256: None,
        idempotency_key: KEY.into(),
    }
}

fn gate(fixture: Arc<Fixture>) -> RootScopeGate {
    RootScopeGate::new(fixture, scope()).unwrap()
}

#[tokio::test]
async fn single_append_preserves_exact_original_and_replays_only_verified_receipt() {
    let source = Fixture::new(Mode::Normal);
    let ledger = Ledger::default();
    let result = append_once(
        source.as_ref(),
        source.as_ref(),
        &gate(source.clone()),
        &ledger,
        request(),
    )
    .await
    .unwrap();

    assert!(!result.replayed);
    assert_eq!(result.receipt.page_id, PAGE);
    assert_eq!(result.receipt.last_edited_time, AFTER);
    assert_eq!(
        *source.markdown.lock().unwrap(),
        "# Existing\n\noriginal\n\n## New entry\n"
    );
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);

    let replay = append_once(
        source.as_ref(),
        source.as_ref(),
        &gate(source.clone()),
        &ledger,
        request(),
    )
    .await
    .unwrap();
    assert_eq!(replay.receipt, result.receipt);
    assert!(replay.replayed);
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);

    let mut different = request();
    different.markdown = "another append".into();
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            different
        )
        .await,
        Err(AppendError::IdempotencyConflict)
    );
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn invalid_and_out_of_scope_calls_cannot_reserve_or_write() {
    let source = Fixture::new(Mode::Normal);
    let ledger = Ledger::default();
    let mut empty = request();
    empty.markdown = " ".into();
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            empty
        )
        .await,
        Err(AppendError::InvalidInput)
    );

    source.deny_scope.store(true, Ordering::SeqCst);
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            request()
        )
        .await,
        Err(AppendError::NotAuthorized)
    );
    assert_eq!(ledger.starts.load(Ordering::SeqCst), 0);
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stale_revision_denies_without_mutating_and_does_not_replay() {
    let source = Fixture::new(Mode::Normal);
    let ledger = Ledger::default();
    let mut wrong = request();
    wrong.expected_last_edited_time = "2026-10-08T09:00:00Z".into();
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            wrong.clone()
        )
        .await,
        Err(AppendError::RevisionConflict)
    );
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            wrong
        )
        .await,
        Err(AppendError::OutcomeUnknown)
    );
}

#[tokio::test]
async fn timeout_after_commit_does_not_send_second_patch() {
    let source = Fixture::new(Mode::TimeoutAfterCommit);
    let ledger = Ledger::default();
    for _ in 0..2 {
        assert_eq!(
            append_once(
                source.as_ref(),
                source.as_ref(),
                &gate(source.clone()),
                &ledger,
                request()
            )
            .await,
            Err(AppendError::OutcomeUnknown)
        );
    }
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    assert!(source.markdown.lock().unwrap().contains("## New entry"));
}

#[tokio::test]
async fn readback_preserves_original_or_keeps_key_unknown() {
    for mode in [Mode::RewriteOriginal, Mode::ReadbackUnavailable] {
        let source = Fixture::new(mode);
        let ledger = Ledger::default();
        assert_eq!(
            append_once(
                source.as_ref(),
                source.as_ref(),
                &gate(source.clone()),
                &ledger,
                request()
            )
            .await,
            Err(AppendError::OutcomeUnknown)
        );
        assert_eq!(
            append_once(
                source.as_ref(),
                source.as_ref(),
                &gate(source.clone()),
                &ledger,
                request()
            )
            .await,
            Err(AppendError::OutcomeUnknown)
        );
        assert_eq!(source.writes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn scope_change_before_write_fails_without_patch() {
    let source = Fixture::new(Mode::Normal);
    source.deny_revalidation.store(true, Ordering::SeqCst);
    let ledger = Ledger::default();
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            request()
        )
        .await,
        Err(AppendError::NotAuthorized)
    );
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn hash_precondition_must_match_exact_fresh_markdown() {
    let source = Fixture::new(Mode::Normal);
    let ledger = Ledger::default();
    let mut invalid = request();
    invalid.expected_markdown_sha256 = Some("e".repeat(64));
    assert_eq!(
        append_once(
            source.as_ref(),
            source.as_ref(),
            &gate(source.clone()),
            &ledger,
            invalid
        )
        .await,
        Err(AppendError::RevisionConflict)
    );
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
}
