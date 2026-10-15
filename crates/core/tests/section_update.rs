use notion_knowledge_core::{
    backend::{
        BackendError, BackendErrorKind, BackendFuture, NotionRead, NotionSectionWrite, Page,
        PageContent, PageId, UpdateSectionContent,
    },
    discovery::ExclusionRules,
    idempotency::{
        IdempotencyError, MutationClaim, Reservation, VerifiedReceipt, WriteIdempotencyStore,
    },
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    revision::markdown_sha256,
    root_scope::RootScopeGate,
    section_update::{SectionUpdateError, SectionUpdateRequest, update_section_once},
    sections::SectionAnchor,
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "33333333-3333-3333-3333-333333333333";
const OLD: &str = "# Title\n## Target\nold body\n## Sibling\nsafe\n";
const EDITED: &str = "# Title\n## Target\nnew body\n## Sibling\nsafe\n";
const REV: &str = "2026-10-11T12:00:00Z";
const NEW_REV: &str = "2026-10-11T12:00:01Z";
const KEY: &str = "section-test-idempotency-key-001";

fn err() -> BackendError {
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
    MismatchedReadback,
    ConcurrentEdit,
    BadReceipt,
}
struct Fixture {
    markdown: Mutex<String>,
    writes: AtomicUsize,
    reads: AtomicUsize,
    deny: AtomicBool,
    mode: Mode,
}
impl Fixture {
    fn new(mode: Mode, markdown: &str) -> Arc<Self> {
        Arc::new(Self {
            markdown: Mutex::new(markdown.into()),
            writes: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            deny: AtomicBool::new(false),
            mode,
        })
    }
    fn page(&self) -> Page {
        Page {
            relation_names: Default::default(),
            id: PageId(PAGE.into()),
            url: format!("https://www.notion.so/{PAGE}"),
            title: "Title".into(),
            last_edited_time: if self.writes.load(Ordering::SeqCst) > 0 {
                NEW_REV.into()
            } else {
                REV.into()
            },
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
            let nth = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
            let mut markdown = self.markdown.lock().unwrap().clone();
            if matches!(self.mode, Mode::ConcurrentEdit) && nth >= 3 {
                markdown.push_str("concurrent edit\n");
            }
            Ok(PageContent {
                page: self.page(),
                markdown,
            })
        })
    }
}
impl NotionSectionWrite for Fixture {
    fn update_section(&self, request: UpdateSectionContent) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            assert_eq!(request.page_id.0, PAGE);
            assert!(!request.old_str.is_empty());
            assert_eq!(request.old_str, "old body\n");
            self.writes.fetch_add(1, Ordering::SeqCst);
            let mut markdown = self.markdown.lock().unwrap();
            assert_eq!(markdown.matches(&request.old_str).count(), 1);
            *markdown = markdown.replacen(&request.old_str, &request.new_str, 1);
            if matches!(self.mode, Mode::MismatchedReadback) {
                *markdown = markdown.replace("## Sibling\nsafe\n", "## Sibling\naltered\n");
            }
            if matches!(self.mode, Mode::TimeoutAfterCommit | Mode::BadReceipt) {
                return Err(err());
            }
            Ok(())
        })
    }
}
impl PageLifecycle for Fixture {
    fn lifecycle<'a>(
        &'a self,
        _: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            if self.deny.load(Ordering::SeqCst) {
                return Err(err());
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
                        revision: self.page().last_edited_time,
                        inactive: false,
                        parent: PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: ROOT.into(),
                        },
                    },
                    LifecycleNode {
                        kind: LifecycleKind::Page,
                        id: ROOT.into(),
                        revision: REV.into(),
                        inactive: false,
                        parent: PhysicalParent::Workspace,
                    },
                ],
            })
        })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        _: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.deny.load(Ordering::SeqCst)
                || evidence
                    .ancestry
                    .first()
                    .is_none_or(|node| node.revision != self.page().last_edited_time)
            {
                Err(err())
            } else {
                Ok(())
            }
        })
    }
}
fn gate(fixture: Arc<Fixture>) -> RootScopeGate {
    RootScopeGate::new(
        fixture,
        LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        },
    )
    .unwrap()
}

type LedgerRecord = (String, u8, Option<VerifiedReceipt>);

#[derive(Default)]
struct Ledger {
    data: Mutex<HashMap<String, LedgerRecord>>,
    starts: AtomicUsize,
}
impl WriteIdempotencyStore for Ledger {
    fn begin(&self, claim: &MutationClaim) -> Result<Reservation, IdempotencyError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        let mut data = self.data.lock().unwrap();
        if let Some((digest, status, receipt)) = data.get(claim.key_digest()) {
            if digest != claim.request_digest() {
                return Err(IdempotencyError::KeyConflict);
            }
            return Ok(if *status == 2 {
                Reservation::Replay(receipt.clone().unwrap())
            } else {
                Reservation::Reconcile
            });
        }
        data.insert(
            claim.key_digest().into(),
            (claim.request_digest().into(), 0, None),
        );
        Ok(Reservation::ExecuteOnce)
    }
    fn mark_uncertain(&self, claim: &MutationClaim) -> Result<(), IdempotencyError> {
        self.data
            .lock()
            .unwrap()
            .get_mut(claim.key_digest())
            .unwrap()
            .1 = 1;
        Ok(())
    }
    fn record_verified(
        &self,
        claim: &MutationClaim,
        receipt: &VerifiedReceipt,
    ) -> Result<(), IdempotencyError> {
        let mut data = self.data.lock().unwrap();
        let entry = data.get_mut(claim.key_digest()).unwrap();
        entry.1 = 2;
        entry.2 = Some(receipt.clone());
        Ok(())
    }
}
fn request(markdown: &str) -> SectionUpdateRequest {
    SectionUpdateRequest {
        page_id: PageId(PAGE.into()),
        anchor: SectionAnchor {
            level: 2,
            heading: "Target".into(),
        },
        replacement: "new body\n".into(),
        expected_last_edited_time: Some(REV.into()),
        expected_markdown_sha256: Some(markdown_sha256(markdown)),
        idempotency_key: KEY.into(),
    }
}
async fn run(
    source: &Arc<Fixture>,
    ledger: &Ledger,
    req: SectionUpdateRequest,
) -> Result<notion_knowledge_core::section_update::SectionUpdateResult, SectionUpdateError> {
    update_section_once(
        source.as_ref(),
        source.as_ref(),
        &gate(source.clone()),
        ledger,
        req,
    )
    .await
}
#[tokio::test]
async fn one_targeted_patch_preserves_siblings_and_replay_never_writes() {
    let src = Fixture::new(Mode::Normal, OLD);
    let ledger = Ledger::default();
    let result = run(&src, &ledger, request(OLD)).await.unwrap();
    assert!(!result.replayed);
    assert_eq!(result.receipt.page_id, PAGE);
    assert_eq!(&*src.markdown.lock().unwrap(), EDITED);
    assert_eq!(src.writes.load(Ordering::SeqCst), 1);
    let replay = run(&src, &ledger, request(OLD)).await.unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.receipt, result.receipt);
    assert_eq!(src.writes.load(Ordering::SeqCst), 1);
    let mut different = request(OLD);
    different.replacement = "changed again\n".into();
    assert_eq!(
        run(&src, &ledger, different).await,
        Err(SectionUpdateError::IdempotencyConflict)
    );
}

#[tokio::test]
async fn all_invalid_selections_fail_before_any_patch() {
    for (markdown, error) in [
        (
            "# Title\n## Wrong\nold body\n",
            SectionUpdateError::SelectionRejected,
        ),
        (
            "## Target\nold body\n## Target\nold body\n",
            SectionUpdateError::SelectionRejected,
        ),
        (
            "## Target\nold body\n## Sibling\nold body\n",
            SectionUpdateError::SelectionRejected,
        ),
        (
            "## Target\n## Sibling\nsafe\n",
            SectionUpdateError::SelectionRejected,
        ),
        (
            "## Target\n<unknown id=\"1\"/>\n## Sibling\nsafe\n",
            SectionUpdateError::SelectionRejected,
        ),
    ] {
        let src = Fixture::new(Mode::Normal, markdown);
        let ledger = Ledger::default();
        assert_eq!(run(&src, &ledger, request(markdown)).await, Err(error));
        assert_eq!(src.writes.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn stale_revision_scope_denial_and_concurrent_edit_do_not_patch() {
    let src = Fixture::new(Mode::Normal, OLD);
    let ledger = Ledger::default();
    src.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        run(&src, &ledger, request(OLD)).await,
        Err(SectionUpdateError::NotAuthorized)
    );
    assert_eq!(ledger.starts.load(Ordering::SeqCst), 0);
    src.deny.store(false, Ordering::SeqCst);
    let mut stale = request(OLD);
    stale.expected_markdown_sha256 = Some("0".repeat(64));
    assert_eq!(
        run(&src, &Ledger::default(), stale).await,
        Err(SectionUpdateError::RevisionConflict(
            notion_knowledge_core::revision::RevisionMetadata {
                page_id: PageId(PAGE.into()),
                last_edited_time: REV.into(),
            },
        ))
    );
    assert_eq!(src.writes.load(Ordering::SeqCst), 0);
    let concurrent = Fixture::new(Mode::ConcurrentEdit, OLD);
    assert_eq!(
        run(&concurrent, &Ledger::default(), request(OLD)).await,
        Err(SectionUpdateError::RevisionConflict(
            notion_knowledge_core::revision::RevisionMetadata {
                page_id: PageId(PAGE.into()),
                last_edited_time: REV.into(),
            },
        ))
    );
    assert_eq!(concurrent.writes.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unknown_outcomes_are_not_retried_even_when_patch_committed() {
    for mode in [
        Mode::TimeoutAfterCommit,
        Mode::MismatchedReadback,
        Mode::BadReceipt,
    ] {
        let src = Fixture::new(mode, OLD);
        let ledger = Ledger::default();
        for _ in 0..2 {
            assert_eq!(
                run(&src, &ledger, request(OLD)).await,
                Err(SectionUpdateError::OutcomeUnknown)
            );
        }
        assert_eq!(src.writes.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn replacement_that_creates_sibling_heading_is_rejected() {
    let src = Fixture::new(Mode::Normal, OLD);
    let mut req = request(OLD);
    req.replacement = "new body\n## Escaped sibling\n".into();
    assert_eq!(
        run(&src, &Ledger::default(), req).await,
        Err(SectionUpdateError::SelectionRejected)
    );
    assert_eq!(src.writes.load(Ordering::SeqCst), 0);
    let mut markup = request(OLD);
    markup.replacement = "<page url=\"https://notion.so/child\"/>\n".into();
    assert_eq!(
        run(&src, &Ledger::default(), markup).await,
        Err(SectionUpdateError::SelectionRejected)
    );
}

#[tokio::test]
async fn hash_only_stale_conflict_reports_only_current_metadata_then_explicit_retry() {
    let source = Fixture::new(Mode::Normal, OLD);
    let ledger = Ledger::default();
    let mut stale = request(OLD);
    stale.expected_last_edited_time = None;
    stale.expected_markdown_sha256 = Some("f".repeat(64));
    let error = run(&source, &ledger, stale.clone()).await.unwrap_err();
    let SectionUpdateError::RevisionConflict(current) = error else {
        panic!("stale content must yield conflict metadata");
    };
    assert_eq!(current.page_id.0, PAGE);
    assert_eq!(current.last_edited_time, REV);
    assert!(!format!("{current:?}").contains("old body"));
    assert_eq!(source.writes.load(Ordering::SeqCst), 0);
    assert_eq!(
        run(&source, &ledger, stale).await,
        Err(SectionUpdateError::OutcomeUnknown)
    );
    let fresh = source.read_content(&PageId(PAGE.into())).await.unwrap();
    let mut retry = request(&fresh.markdown);
    retry.idempotency_key = "fresh-user-retry-section-0002".into();
    retry.expected_last_edited_time = None;
    assert!(!run(&source, &ledger, retry).await.unwrap().replayed);
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn time_only_precondition_and_missing_revision_behavior() {
    let source = Fixture::new(Mode::Normal, OLD);
    let ledger = Ledger::default();
    let mut missing = request(OLD);
    missing.expected_last_edited_time = None;
    missing.expected_markdown_sha256 = None;
    assert_eq!(
        run(&source, &ledger, missing).await,
        Err(SectionUpdateError::InvalidInput)
    );
    assert_eq!(ledger.starts.load(Ordering::SeqCst), 0);
    let mut time_only = request(OLD);
    time_only.expected_markdown_sha256 = None;
    assert!(!run(&source, &ledger, time_only).await.unwrap().replayed);
    assert_eq!(source.writes.load(Ordering::SeqCst), 1);
}
