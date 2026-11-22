//! Concrete secure downloader/Notion integration for the core ingestion service.
use crate::file_multipart::MultipartPolicy;
use crate::{
    NotionClient, UploadedFile,
    file_download::{DownloadError, DownloadPolicy, DownloadedFile},
    file_validation::{FileValidationError, FileValidationPolicy},
};
use notion_knowledge_core::{
    audit::{FileKind, SafeFileFacts},
    backend::PageId,
    file_ingest::{
        FileEffectFailure, FileIngestAdapter, FileInputFailure, FileReferences, IngestFailure,
    },
    lifecycle::LifecycleScope,
};

/// Policies and integration are trusted composition inputs, never tool arguments.
pub struct NotionFileIngestAdapter {
    client: NotionClient,
    download: DownloadPolicy,
    validation: FileValidationPolicy,
    scope: LifecycleScope,
    multipart: Option<MultipartPolicy>,
}
impl NotionFileIngestAdapter {
    pub fn new(
        client: NotionClient,
        download: DownloadPolicy,
        validation: FileValidationPolicy,
        scope: LifecycleScope,
    ) -> Self {
        Self {
            client,
            download,
            validation,
            scope,
            multipart: None,
        }
    }
    /// Explicit trusted paid-plan opt-in. Single-part covers through
    /// the validated threshold, without a gap below multipart selection.
    pub fn with_multipart(mut self, policy: MultipartPolicy) -> Self {
        self.validation = FileValidationPolicy::for_single_part(policy.threshold_bytes())
            .expect("validated multipart threshold");
        self.multipart = Some(policy);
        self
    }
}
impl FileIngestAdapter for NotionFileIngestAdapter {
    fn configured_scope(&self) -> &LifecycleScope {
        &self.scope
    }
    type LocalFile = DownloadedFile;
    type Upload = UploadedFile;
    async fn download(&self, url: &str) -> Result<DownloadedFile, FileInputFailure> {
        self.download.download(url).await.map_err(|error| {
            let (failure, reason) = match error {
                DownloadError::InvalidUrl => (IngestFailure::InvalidInput, "invalid_source_url"),
                DownloadError::HostDenied => (IngestFailure::InvalidInput, "source_host_denied"),
                DownloadError::DomainBlocked => {
                    (IngestFailure::InvalidInput, "source_domain_blocked")
                }
                DownloadError::AddressDenied => {
                    (IngestFailure::InvalidInput, "source_address_denied")
                }
                DownloadError::TooLarge => (IngestFailure::InvalidInput, "file_too_large"),
                DownloadError::EmptyFile => (IngestFailure::InvalidInput, "empty_file"),
                DownloadError::PartialResponse => {
                    (IngestFailure::InvalidInput, "source_partial_response")
                }
                DownloadError::UnsupportedEncoding => {
                    (IngestFailure::InvalidInput, "source_unsupported_encoding")
                }
                DownloadError::UnexpectedStatus => {
                    (IngestFailure::InvalidInput, "source_unexpected_status")
                }
                DownloadError::RedirectDenied => {
                    (IngestFailure::InvalidInput, "source_redirect_denied")
                }
                DownloadError::SourceExpired => (IngestFailure::SourceExpired, "source_expired"),
                DownloadError::SourceUnavailable => {
                    (IngestFailure::DownloadUnavailable, "source_unavailable")
                }
                DownloadError::Timeout => (IngestFailure::DownloadUnavailable, "source_timeout"),
                DownloadError::StorageUnavailable => (
                    IngestFailure::DownloadUnavailable,
                    "temporary_storage_unavailable",
                ),
                DownloadError::InvalidPolicy => (
                    IngestFailure::DownloadUnavailable,
                    "invalid_download_policy",
                ),
            };
            FileInputFailure { failure, reason }
        })
    }
    async fn validate(
        &self,
        file: &DownloadedFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> Result<SafeFileFacts, FileInputFailure> {
        let valid = self.validated(file, name, mime).await.map_err(|error| {
            let reason = match error {
                FileValidationError::InvalidLimit => "invalid_validation_limit",
                FileValidationError::EmptyFile => "empty_file",
                FileValidationError::FileTooLarge => "file_too_large",
                FileValidationError::InvalidFilename => "invalid_filename",
                FileValidationError::UnsupportedExtension => "unsupported_extension",
                FileValidationError::UnsupportedMime => "unsupported_mime",
                FileValidationError::MimeExtensionMismatch => "mime_extension_mismatch",
                FileValidationError::ContentTypeMismatch => "content_type_mismatch",
            };
            FileInputFailure {
                failure: IngestFailure::InvalidInput,
                reason,
            }
        })?;
        Ok(SafeFileFacts::new(
            if valid.mime_type.starts_with("image/") {
                FileKind::Image
            } else {
                FileKind::Other
            },
            valid.size_bytes as u64,
        ))
    }
    async fn upload(
        &self,
        file: &DownloadedFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> Result<UploadedFile, FileEffectFailure> {
        let mut references = FileReferences {
            target: None,
            upload_id: None,
            block_id: None,
        };
        self.upload_tracked(file, name, mime, &mut references).await
    }
    async fn upload_tracked(
        &self,
        file: &DownloadedFile,
        name: Option<&str>,
        mime: Option<&str>,
        references: &mut FileReferences,
    ) -> Result<UploadedFile, FileEffectFailure> {
        let valid = self
            .validated(file, name, mime)
            .await
            .map_err(|_| FileEffectFailure {
                operation: "notion.file_upload.validate",
                kind: notion_knowledge_core::backend::BackendErrorKind::InvalidInput,
                upload_id: None,
                block_id: None,
                effect_possible: false,
            })?;
        if let Some(policy) = self.multipart
            && file.size_bytes() > policy.threshold_bytes()
        {
            self.client
                .upload_multipart_tracked(
                    file,
                    &valid.filename,
                    valid.mime_type,
                    policy,
                    &mut references.upload_id,
                )
                .await
        } else {
            self.client
                .upload_single_part_tracked(
                    file,
                    &valid.filename,
                    valid.mime_type,
                    self.validation,
                    &mut references.upload_id,
                )
                .await
        }
    }
    fn upload_id<'a>(&self, upload: &'a UploadedFile) -> &'a str {
        &upload.id
    }
    async fn attach(
        &self,
        target: &PageId,
        upload: &UploadedFile,
        caption: Option<&str>,
        kind: FileKind,
    ) -> Result<String, FileEffectFailure> {
        let mut references = FileReferences {
            target: Some(target.clone()),
            upload_id: Some(upload.id.clone()),
            block_id: None,
        };
        self.attach_tracked(target, upload, caption, kind, &mut references)
            .await
    }
    async fn attach_tracked(
        &self,
        target: &PageId,
        upload: &UploadedFile,
        caption: Option<&str>,
        kind: FileKind,
        references: &mut FileReferences,
    ) -> Result<String, FileEffectFailure> {
        match kind {
            FileKind::Image => self
                .client
                .attach_uploaded_image_tracked(target, upload, caption, &mut references.block_id)
                .await
                .map(|value| value.block_id),
            FileKind::Other => self
                .client
                .attach_uploaded_file_tracked(target, upload, caption, &mut references.block_id)
                .await
                .map(|value| value.block_id),
        }
    }
}

impl NotionFileIngestAdapter {
    async fn validated(
        &self,
        file: &DownloadedFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> Result<crate::ValidatedFile, FileValidationError> {
        crate::file_validation::validate_downloaded(
            file,
            self.multipart
                .map_or(self.validation.max_bytes(), MultipartPolicy::maximum_bytes),
            name,
            mime,
        )
        .await
    }
}

#[cfg(test)]
mod tests;
