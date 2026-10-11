//! Explicit, payload-free file-ingest outcome audit adapter.
//!
//! The source URL, temporary path, filename, content, MIME parameters, and
//! upstream error message cannot be represented by this closed schema.
use crate::{file_attachment::AttachedFile, image_attachment::AttachedImage};
use notion_knowledge_core::{
    audit::{AuditActor, AuditError, AuditEvent, AuditOutcome, AuditTool, FileKind, SafeFileFacts},
    backend::PageId,
};

/// A verified, already committed native attachment. An upload receipt alone
/// cannot authorize a success event: only the post-append readback may do so.
pub enum VerifiedAttachment<'a> {
    Image(&'a AttachedImage),
    Document(&'a AttachedFile),
}

pub fn successful_attachment_event(
    attachment: VerifiedAttachment<'_>,
    uploaded_size_bytes: usize,
    occurred_at_unix: i64,
    actor: AuditActor,
    correlation_id: String,
) -> Result<AuditEvent, AuditError> {
    let (target, kind, verified) = match attachment {
        VerifiedAttachment::Image(value) => (&value.page_id, FileKind::Image, value.verified),
        VerifiedAttachment::Document(value) => (&value.page_id, FileKind::Other, value.verified),
    };
    if !verified || uploaded_size_bytes == 0 {
        return Err(AuditError::InvalidInput);
    }
    attachment_event_success(
        target,
        kind,
        uploaded_size_bytes,
        occurred_at_unix,
        actor,
        correlation_id,
    )
}

/// Record denied/failed/ambiguous outcomes without ever accepting a success
/// claim based merely on a sent upload. The caller must record the resulting
/// event in its configured durable AuditStore before claiming durable audit.
#[allow(clippy::too_many_arguments)]
pub fn attachment_event(
    target: &PageId,
    kind: FileKind,
    uploaded_size_bytes: usize,
    outcome: AuditOutcome,
    occurred_at_unix: i64,
    actor: AuditActor,
    correlation_id: String,
) -> Result<AuditEvent, AuditError> {
    if outcome == AuditOutcome::Succeeded || uploaded_size_bytes == 0 {
        return Err(AuditError::InvalidInput);
    }
    AuditEvent::new(
        occurred_at_unix,
        actor,
        AuditTool::FileAttach,
        Some(target.0.clone()),
        outcome,
        correlation_id,
        Some(SafeFileFacts::new(kind, uploaded_size_bytes as u64)),
    )
}

/// Only called by the verified success path, after the authoritative GET.
fn attachment_event_success(
    target: &PageId,
    kind: FileKind,
    bytes: usize,
    occurred_at_unix: i64,
    actor: AuditActor,
    correlation_id: String,
) -> Result<AuditEvent, AuditError> {
    AuditEvent::new(
        occurred_at_unix,
        actor,
        AuditTool::FileAttach,
        Some(target.0.clone()),
        AuditOutcome::Succeeded,
        correlation_id,
        Some(SafeFileFacts::new(kind, bytes as u64)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    const PAGE: &str = "11111111-1111-1111-1111-111111111111";

    #[test]
    fn verified_image_audit_contains_only_safe_fields() {
        let image = AttachedImage {
            page_id: PageId(PAGE.into()),
            block_id: "22222222-2222-2222-2222-222222222222".into(),
            upload_id: "33333333-3333-3333-3333-333333333333".into(),
            verified: true,
        };
        let record = successful_attachment_event(
            VerifiedAttachment::Image(&image),
            1234,
            1_800_000_000,
            AuditActor::ApprovedUser,
            "request_0123456789abcdef".into(),
        )
        .unwrap();
        assert_eq!(record.target_page_id(), Some(PAGE));
        assert_eq!(record.outcome(), AuditOutcome::Succeeded);
        let file = record.file().unwrap();
        assert_eq!(file.size_bytes(), 1234);
        assert_eq!(file.kind(), FileKind::Image);
        let debug = format!("{record:?}");
        assert!(!debug.contains(&image.upload_id));
        assert!(!debug.contains("token="));
    }

    #[test]
    fn unverified_attachment_must_never_create_success_audit() {
        let file = AttachedFile {
            page_id: PageId(PAGE.into()),
            block_id: "22222222-2222-2222-2222-222222222222".into(),
            upload_id: "33333333-3333-3333-3333-333333333333".into(),
            verified: false,
        };
        assert_eq!(
            successful_attachment_event(
                VerifiedAttachment::Document(&file),
                100,
                1_800_000_000,
                AuditActor::Server,
                "request_0123456789abcdef".into()
            ),
            Err(AuditError::InvalidInput)
        );
        assert_eq!(
            attachment_event(
                &file.page_id,
                FileKind::Other,
                100,
                AuditOutcome::Succeeded,
                1_800_000_000,
                AuditActor::Server,
                "request_0123456789abcdef".into()
            ),
            Err(AuditError::InvalidInput)
        );
    }

    #[test]
    fn failed_or_indeterminate_outcomes_redact_and_validate_inputs() {
        let page = PageId(PAGE.into());
        let record = attachment_event(
            &page,
            FileKind::Other,
            100,
            AuditOutcome::Indeterminate,
            1_800_000_000,
            AuditActor::Server,
            "request_0123456789abcdef".into(),
        )
        .unwrap();
        assert_eq!(record.outcome(), AuditOutcome::Indeterminate);
        assert_eq!(record.file().unwrap().kind(), FileKind::Other);
        assert!(
            attachment_event(
                &page,
                FileKind::Other,
                100,
                AuditOutcome::Failed,
                1_800_000_000,
                AuditActor::Server,
                "https://secret.example/?token=x".into()
            )
            .is_err()
        );
    }
}
