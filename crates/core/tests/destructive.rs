use std::sync::atomic::{AtomicUsize, Ordering};

use notion_knowledge_core::destructive::{
    ConfirmationDecision, ConfirmationError, ConfirmationFuture, ConfirmationProvider,
    ConfirmationRequest, DestructivePolicy, WriteOperation,
};

struct Provider {
    decision: ConfirmationDecision,
    calls: AtomicUsize,
}

impl Provider {
    fn new(decision: ConfirmationDecision) -> Self {
        Self {
            decision,
            calls: AtomicUsize::new(0),
        }
    }
}

impl ConfirmationProvider for Provider {
    fn decide<'a>(&'a self, _: &'a ConfirmationRequest) -> ConfirmationFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.decision })
    }
}

fn request(operation: WriteOperation) -> ConfirmationRequest {
    ConfirmationRequest::new(
        operation,
        "workspace-approved",
        "page-approved",
        &"a".repeat(64),
        "idempotency-key-0123456789",
    )
    .unwrap()
}

#[test]
fn all_write_operations_are_classified_conservatively() {
    for (name, operation, destructive) in [
        ("knowledge_create_page", WriteOperation::CreatePage, false),
        ("knowledge_append", WriteOperation::Append, false),
        (
            "knowledge_update_section",
            WriteOperation::UpdateSection,
            true,
        ),
        ("knowledge_archive_page", WriteOperation::ArchivePage, true),
    ] {
        assert_eq!(WriteOperation::from_tool_name(name), Some(operation));
        assert_eq!(operation.is_destructive(), destructive);
    }
    assert!(WriteOperation::ReplaceContent.is_destructive());
    assert!(WriteOperation::DeletePage.is_destructive());
    assert_eq!(
        WriteOperation::from_tool_name("knowledge_delete_everything"),
        None
    );
}

#[tokio::test]
async fn default_policy_never_asks_for_confirmation_or_grants_access() {
    let provider = Provider::new(ConfirmationDecision::Approved);
    assert!(!DestructivePolicy::default().enabled());
    assert!(matches!(
        DestructivePolicy::default()
            .require_confirmation(&request(WriteOperation::ArchivePage), Some(&provider))
            .await,
        Err(ConfirmationError::Disabled)
    ));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unavailable_declined_and_missing_provider_fail_closed_without_retry() {
    let policy = DestructivePolicy::new(true);
    let req = request(WriteOperation::ArchivePage);
    assert!(matches!(
        policy.require_confirmation(&req, None).await,
        Err(ConfirmationError::Unavailable)
    ));
    for (decision, expected) in [
        (ConfirmationDecision::Declined, ConfirmationError::Declined),
        (
            ConfirmationDecision::Unavailable,
            ConfirmationError::Unavailable,
        ),
    ] {
        let provider = Provider::new(decision);
        assert!(matches!(
            policy.require_confirmation(&req, Some(&provider)).await,
            Err(error) if error == expected
        ));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn trusted_approval_is_bound_to_operation_scope_target_and_payload() {
    let policy = DestructivePolicy::new(true);
    let provider = Provider::new(ConfirmationDecision::Approved);
    let original = request(WriteOperation::ReplaceContent);
    let permit = policy
        .require_confirmation(&original, Some(&provider))
        .await
        .unwrap();
    let changed = ConfirmationRequest::new(
        original.operation(),
        original.trusted_scope(),
        "another-page",
        original.request_sha256(),
        original.idempotency_key(),
    )
    .unwrap();
    assert_eq!(permit.consume(&changed), Err(ConfirmationError::Mismatch));
    let permit = policy
        .require_confirmation(&original, Some(&provider))
        .await
        .unwrap();
    assert_eq!(permit.consume(&original), Ok(()));
    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
}

#[test]
fn malformed_claims_and_nondestructive_confirmation_requests_are_rejected() {
    assert_eq!(
        ConfirmationRequest::new(
            WriteOperation::Append,
            "trusted",
            "page",
            &"a".repeat(64),
            "idempotency-key-0123456789"
        )
        .unwrap_err(),
        ConfirmationError::InvalidRequest
    );
    for (scope, target, digest, key) in [
        ("", "page", "a".repeat(64), "idempotency-key-0123456789"),
        (
            "workspace",
            "../page",
            "a".repeat(64),
            "idempotency-key-0123456789",
        ),
        (
            "workspace",
            "page",
            "invalid".to_owned(),
            "idempotency-key-0123456789",
        ),
        ("workspace", "page", "a".repeat(64), "short"),
    ] {
        assert_eq!(
            ConfirmationRequest::new(WriteOperation::ArchivePage, scope, target, &digest, key)
                .unwrap_err(),
            ConfirmationError::InvalidRequest
        );
    }
    let req = request(WriteOperation::DeletePage);
    assert!(!format!("{req:?}").contains("workspace-approved"));
}
#[test]
fn confirmation_scope_changes_when_exclusion_policy_changes() {
    use notion_knowledge_core::{
        backend::PageId, destructive::confirmation_scope, lifecycle::LifecycleScope,
    };
    let original = LifecycleScope {
        workspace_id: "workspace".into(),
        generation: 1,
        roots: vec![PageId("root".into())],
        exclusions: Default::default(),
    };
    let fingerprint = confirmation_scope(&original).unwrap();
    let mut changed = original.clone();
    changed.exclusions.page_ids.insert("excluded-page".into());
    assert_ne!(fingerprint, confirmation_scope(&changed).unwrap());
    changed = original.clone();
    changed
        .exclusions
        .descendants_of
        .insert("retained-root".into());
    assert_ne!(fingerprint, confirmation_scope(&changed).unwrap());
    changed = original.clone();
    changed
        .exclusions
        .source_types
        .insert(notion_knowledge_core::discovery::SourceType::Database);
    assert_ne!(fingerprint, confirmation_scope(&changed).unwrap());
}

#[tokio::test]
async fn approved_section_permit_cannot_transfer_to_another_effect_or_idempotency_intent() {
    use notion_knowledge_core::destructive::SectionConfirmationPreview;
    let original = request(WriteOperation::UpdateSection)
        .with_section_preview(SectionConfirmationPreview {
            section_anchor: "## Target".into(),
            original_body: "Original".into(),
            replacement_body: "Replacement".into(),
        })
        .unwrap();
    let provider = Provider::new(ConfirmationDecision::Approved);
    let policy = DestructivePolicy::new(true);
    for changed in [
        ConfirmationRequest::new(
            original.operation(),
            "other-scope",
            original.target_id(),
            original.request_sha256(),
            original.idempotency_key(),
        )
        .unwrap(),
        ConfirmationRequest::new(
            original.operation(),
            original.trusted_scope(),
            "other-page",
            original.request_sha256(),
            original.idempotency_key(),
        )
        .unwrap(),
        ConfirmationRequest::new(
            original.operation(),
            original.trusted_scope(),
            original.target_id(),
            &"b".repeat(64),
            original.idempotency_key(),
        )
        .unwrap(),
        ConfirmationRequest::new(
            original.operation(),
            original.trusted_scope(),
            original.target_id(),
            original.request_sha256(),
            "another-idempotency-intent",
        )
        .unwrap(),
        original
            .clone()
            .with_section_preview(SectionConfirmationPreview {
                section_anchor: "## Target".into(),
                original_body: "Original".into(),
                replacement_body: "Changed payload".into(),
            })
            .unwrap(),
    ] {
        let permit = policy
            .require_confirmation(&original, Some(&provider))
            .await
            .unwrap();
        assert_eq!(permit.consume(&changed), Err(ConfirmationError::Mismatch));
    }
}
#[test]
fn readable_preview_preserves_existing_section_size_bounds() {
    use notion_knowledge_core::destructive::SectionConfirmationPreview;
    let preview = SectionConfirmationPreview {
        section_anchor: "## Target".into(),
        original_body: "a".repeat(1024 * 1024),
        replacement_body: "b".repeat(200_000),
    };
    assert!(
        request(WriteOperation::UpdateSection)
            .with_section_preview(preview.clone())
            .is_ok()
    );
    let mut oversized = preview.clone();
    oversized.replacement_body.push('b');
    assert_eq!(
        request(WriteOperation::UpdateSection)
            .with_section_preview(oversized)
            .unwrap_err(),
        ConfirmationError::InvalidRequest
    );
    let mut oversized = preview;
    oversized.original_body.push('a');
    assert_eq!(
        request(WriteOperation::UpdateSection)
            .with_section_preview(oversized)
            .unwrap_err(),
        ConfirmationError::InvalidRequest
    );
}
