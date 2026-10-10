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
