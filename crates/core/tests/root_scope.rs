use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::{ExclusionRules, SourceType},
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    root_scope::{RootScopeGate, ScopeError},
};

const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const OTHER: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 3,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}

fn page_node(id: &str, parent: PhysicalParent) -> LifecycleNode {
    LifecycleNode {
        kind: LifecycleKind::Page,
        id: id.into(),
        revision: "2026-10-10T11:00:00+00:00".into(),
        inactive: false,
        parent,
    }
}

fn valid_evidence(policy: &LifecycleScope) -> LifecycleEvidence {
    LifecycleEvidence {
        scope: policy.clone(),
        status: LifecycleStatus::Allowed {
            roots: vec![ROOT.into()],
        },
        ancestry: vec![
            page_node(
                PAGE,
                PhysicalParent::Object {
                    kind: LifecycleKind::Page,
                    id: ROOT.into(),
                },
            ),
            page_node(ROOT, PhysicalParent::Workspace),
        ],
    }
}

fn denied_backend() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unavailable,
        operation: "fixture.lifecycle",
        retry_after: None,
        committed_page_id: None,
    }
}

struct FakeLifecycle {
    evidence: Mutex<Option<LifecycleEvidence>>,
    allow_revalidation: AtomicBool,
    inspections: AtomicUsize,
    rechecks: AtomicUsize,
}

impl FakeLifecycle {
    fn new(evidence: LifecycleEvidence) -> Self {
        Self {
            evidence: Mutex::new(Some(evidence)),
            allow_revalidation: AtomicBool::new(true),
            inspections: AtomicUsize::new(0),
            rechecks: AtomicUsize::new(0),
        }
    }
}

impl PageLifecycle for FakeLifecycle {
    fn lifecycle<'a>(
        &'a self,
        _page: &'a PageId,
        _scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            self.evidence
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(denied_backend)
        })
    }

    fn revalidate_lifecycle<'a>(
        &'a self,
        _evidence: &'a LifecycleEvidence,
        _current_scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            self.rechecks.fetch_add(1, Ordering::SeqCst);
            if self.allow_revalidation.load(Ordering::SeqCst) {
                Ok(())
            } else {
                Err(denied_backend())
            }
        })
    }
}

#[tokio::test]
async fn direct_page_id_is_allowed_only_when_ancestry_reaches_approved_root() {
    let current = scope();
    let backend = Arc::new(FakeLifecycle::new(valid_evidence(&current)));
    let gate = RootScopeGate::new(backend.clone(), current.clone()).unwrap();

    let permit = gate.authorize(&PageId(PAGE.into())).await.unwrap();
    gate.revalidate(&permit, &current).await.unwrap();
    assert_eq!(backend.inspections.load(Ordering::SeqCst), 1);
    assert_eq!(backend.rechecks.load(Ordering::SeqCst), 1);

    // Authorizing another operation must fetch authoritative ancestry again.
    gate.authorize(&PageId(PAGE.into())).await.unwrap();
    assert_eq!(backend.inspections.load(Ordering::SeqCst), 2);
    assert_eq!(
        gate.authorize(&PageId("not a UUID".into())).await.err(),
        Some(ScopeError::Denied)
    );
    assert_eq!(backend.inspections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn foreign_page_cannot_spoof_the_root_with_claimed_status_or_references() {
    let policy = scope();
    let mut evidence = valid_evidence(&policy);
    evidence.ancestry[1] = page_node(OTHER, PhysicalParent::Workspace);
    evidence.ancestry[0].parent = PhysicalParent::Object {
        kind: LifecycleKind::Page,
        id: OTHER.into(),
    };
    // A compromised indexed view claims it belongs to ROOT; physical chain does not.
    let gate = RootScopeGate::new(Arc::new(FakeLifecycle::new(evidence)), policy).unwrap();
    assert_eq!(
        gate.authorize(&PageId(PAGE.into())).await.err(),
        Some(ScopeError::Denied)
    );
}

#[tokio::test]
async fn metadata_ambiguity_and_exclusions_fail_closed() {
    let base = scope();
    let cases = {
        let mut list = Vec::new();
        let mut wrong_selected = valid_evidence(&base);
        wrong_selected.ancestry[0].id = OTHER.into();
        list.push(wrong_selected);

        let mut missing_parent = valid_evidence(&base);
        missing_parent.ancestry.remove(1);
        list.push(missing_parent);

        let mut reversed_parent = valid_evidence(&base);
        reversed_parent.ancestry[1].parent = PhysicalParent::Object {
            kind: LifecycleKind::Page,
            id: PAGE.into(),
        };
        list.push(reversed_parent);

        let mut invalid_status = valid_evidence(&base);
        invalid_status.status = LifecycleStatus::OutsideScope;
        list.push(invalid_status);

        let mut duplicate_roots = valid_evidence(&base);
        duplicate_roots.status = LifecycleStatus::Allowed {
            roots: vec![ROOT.into(), ROOT.into()],
        };
        list.push(duplicate_roots);

        let mut archived = valid_evidence(&base);
        archived.ancestry[0].inactive = true;
        list.push(archived);

        let mut stale_scope = valid_evidence(&base);
        stale_scope.scope.generation += 1;
        list.push(stale_scope);

        list
    };
    for evidence in cases {
        let gate =
            RootScopeGate::new(Arc::new(FakeLifecycle::new(evidence)), base.clone()).unwrap();
        assert!(gate.authorize(&PageId(PAGE.into())).await.is_err());
    }

    for excluded in [PAGE, ROOT] {
        let mut policy = scope();
        policy.exclusions.page_ids.insert(excluded.into());
        let gate = RootScopeGate::new(
            Arc::new(FakeLifecycle::new(valid_evidence(&policy))),
            policy,
        )
        .unwrap();
        assert_eq!(
            gate.authorize(&PageId(PAGE.into())).await.err(),
            Some(ScopeError::Denied)
        );
    }

    let mut policy = scope();
    policy.exclusions.descendants_of.insert(ROOT.into());
    let gate = RootScopeGate::new(
        Arc::new(FakeLifecycle::new(valid_evidence(&policy))),
        policy,
    )
    .unwrap();
    assert_eq!(
        gate.authorize(&PageId(PAGE.into())).await.err(),
        Some(ScopeError::Denied)
    );

    let mut policy = scope();
    policy.exclusions.source_types.insert(SourceType::Page);
    let gate = RootScopeGate::new(
        Arc::new(FakeLifecycle::new(valid_evidence(&policy))),
        policy,
    )
    .unwrap();
    assert_eq!(
        gate.authorize(&PageId(PAGE.into())).await.err(),
        Some(ScopeError::Denied)
    );
}

#[tokio::test]
async fn provider_failures_and_revalidation_changes_deny_without_leaking_metadata() {
    let policy = scope();
    let backend = Arc::new(FakeLifecycle::new(valid_evidence(&policy)));
    let gate = RootScopeGate::new(backend.clone(), policy.clone()).unwrap();

    *backend.evidence.lock().unwrap() = None;
    assert_eq!(
        gate.authorize(&PageId(PAGE.into())).await.err(),
        Some(ScopeError::Unverifiable)
    );
    *backend.evidence.lock().unwrap() = Some(valid_evidence(&policy));
    let permit = gate.authorize(&PageId(PAGE.into())).await.unwrap();

    let mut moved_policy = policy.clone();
    moved_policy.generation += 1;
    assert_eq!(
        gate.revalidate(&permit, &moved_policy).await,
        Err(ScopeError::Changed)
    );
    assert_eq!(backend.rechecks.load(Ordering::SeqCst), 0);
    backend.allow_revalidation.store(false, Ordering::SeqCst);
    assert_eq!(
        gate.revalidate(&permit, &policy).await,
        Err(ScopeError::Changed)
    );
    assert_eq!(backend.rechecks.load(Ordering::SeqCst), 1);

    let diagnostic = ScopeError::Unverifiable.to_string();
    assert!(!diagnostic.contains(PAGE));
    assert!(!diagnostic.contains(ROOT));
}

#[test]
fn policy_rejects_unconfigured_ambiguous_or_noncanonical_roots() {
    let fixture = Arc::new(FakeLifecycle::new(valid_evidence(&scope())));
    let mut cases = Vec::new();
    let mut empty = scope();
    empty.roots.clear();
    cases.push(empty);
    let mut duplicate = scope();
    duplicate.roots.push(PageId(ROOT.into()));
    cases.push(duplicate);
    let mut invalid = scope();
    invalid.roots = vec![PageId("https://www.notion.so/other".into())];
    cases.push(invalid);
    let mut upper = scope();
    upper.workspace_id = WORKSPACE.to_uppercase();
    cases.push(upper);
    for policy in cases {
        assert!(matches!(
            RootScopeGate::new(fixture.clone(), policy),
            Err(ScopeError::InvalidConfiguration)
        ));
    }
}
