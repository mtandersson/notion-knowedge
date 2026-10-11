//! Sequential, bounded native attachment of already-uploaded files.
//!
//! A batch is not a transaction: earlier verified blocks remain attached after
//! later failures. No ambiguous write is ever replayed by this layer.
use crate::{NotionClient, file_upload::UploadedFile};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    lifecycle::LifecycleScope,
};

pub const HARD_MAX_ATTACHMENTS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchAttachmentStatus {
    Verified { block_id: String },
    Failed { kind: BackendErrorKind },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchAttachmentOutcome {
    pub index: usize,
    /// Notion upload receipt ID, not a temporary URL or file name.
    pub upload_id: String,
    pub status: BatchAttachmentStatus,
}

fn invalid_batch() -> BackendError {
    BackendError {
        operation: "notion.file.batch",
        kind: BackendErrorKind::InvalidInput,
        retry_after: None,
        committed_page_id: None,
    }
}

fn validate_batch_count(count: usize, maximum: usize) -> Result<(), BackendError> {
    if count == 0 || maximum == 0 || maximum > HARD_MAX_ATTACHMENTS || count > maximum {
        return Err(invalid_batch());
    }
    Ok(())
}

fn outcome(index: usize, upload_id: &str, result: Result<String, BackendError>) -> BatchAttachmentOutcome {
    BatchAttachmentOutcome {
        index,
        upload_id: upload_id.into(),
        status: match result {
            Ok(block_id) => BatchAttachmentStatus::Verified { block_id },
            Err(error) => BatchAttachmentStatus::Failed { kind: error.kind },
        },
    }
}

impl NotionClient {
    /// Attach valid upload receipts in request order. The operator controls
    /// `maximum` and `scope`, never the ChatGPT file arguments. A per-file
    /// failure is recorded but does not prevent the next file from being
    /// attempted; the caller must reconcile any failed/ambiguous upload before
    /// manually retrying that item. No atomic rollback is promised.
    pub async fn attach_uploaded_batch(
        &self,
        target: &PageId,
        uploads: &[UploadedFile],
        captions: &[Option<String>],
        scope: &LifecycleScope,
        maximum: usize,
    ) -> Result<Vec<BatchAttachmentOutcome>, BackendError> {
        validate_batch_count(uploads.len(), maximum)?;
        if uploads.len() != captions.len() {
            return Err(invalid_batch());
        }
        let mut outcomes = Vec::with_capacity(uploads.len());
        for (index, (upload, text)) in uploads.iter().zip(captions).enumerate() {
            let result = if matches!(
                upload.mime_type.as_str(), "image/png" | "image/jpeg" | "image/gif" | "image/webp"
            ) {
                self.attach_uploaded_image(target, upload, text.as_deref(), scope)
                    .await.map(|value| value.block_id)
            } else {
                self.attach_uploaded_file(target, upload, text.as_deref(), scope)
                    .await.map(|value| value.block_id)
            };
            outcomes.push(outcome(index, &upload.id, result));
        }
        Ok(outcomes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(kind: BackendErrorKind) -> BackendError {
        BackendError {
            operation: "notion.file.attach",
            kind,
            retry_after: None,
            committed_page_id: None,
        }
    }

    #[test]
    fn rejects_invalid_counts_and_configuration_before_any_effects() {
        for (count, maximum) in [(0,1),(1,0),(2,1),(1,17),(17,16)] {
            assert_eq!(validate_batch_count(count, maximum).unwrap_err().kind, BackendErrorKind::InvalidInput);
        }
        assert!(validate_batch_count(4,4).is_ok());
    }

    #[test]
    fn reports_individual_partial_failures_in_original_order_without_urls() {
        let statuses = [
            outcome(0, "first", Ok("block1".into())),
            outcome(1, "second", Err(failure(BackendErrorKind::Conflict))),
            outcome(2, "third", Ok("block3".into())),
        ];
        assert_eq!(statuses.iter().map(|r| r.index).collect::<Vec<_>>(),vec![0,1,2]);
        assert_eq!(statuses[1].status,BatchAttachmentStatus::Failed { kind: BackendErrorKind::Conflict });
        assert_eq!(statuses[2].status,BatchAttachmentStatus::Verified { block_id:"block3".into() });
        assert!(!format!("{statuses:?}").contains("https://"));
    }
}
