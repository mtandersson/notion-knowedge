//! Concrete secure downloader/Notion integration for the core ingestion service.
use crate::{
    NotionClient, UploadedFile,
    file_download::{DownloadError, DownloadPolicy, DownloadedFile},
    file_validation::FileValidationPolicy,
};
use notion_knowledge_core::{
    audit::{FileKind, SafeFileFacts},
    backend::PageId,
    file_ingest::{FileEffectFailure, FileIngestAdapter, IngestFailure},
    lifecycle::LifecycleScope,
};
use tokio::io::AsyncReadExt;

/// Policies and integration are trusted composition inputs, never tool arguments.
pub struct NotionFileIngestAdapter {
    client: NotionClient,
    download: DownloadPolicy,
    validation: FileValidationPolicy,
    scope: LifecycleScope,
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
        }
    }
}
impl FileIngestAdapter for NotionFileIngestAdapter {
    fn configured_scope(&self) -> &LifecycleScope {
        &self.scope
    }
    type LocalFile = DownloadedFile;
    type Upload = UploadedFile;
    async fn download(&self, url: &str) -> Result<DownloadedFile, IngestFailure> {
        self.download
            .download(url)
            .await
            .map_err(|error| match error {
                DownloadError::SourceExpired => IngestFailure::SourceExpired,
                DownloadError::InvalidUrl
                | DownloadError::AddressDenied
                | DownloadError::TooLarge
                | DownloadError::EmptyFile
                | DownloadError::SourceRejected
                | DownloadError::RedirectDenied => IngestFailure::InvalidInput,
                _ => IngestFailure::DownloadUnavailable,
            })
    }
    async fn validate(
        &self,
        file: &DownloadedFile,
        name: Option<&str>,
        mime: Option<&str>,
    ) -> Result<SafeFileFacts, IngestFailure> {
        let valid = self.validated(file, name, mime).await?;
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
        self.client
            .upload_single_part_detailed(file, &valid.filename, valid.mime_type, self.validation)
            .await
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
        match kind {
            FileKind::Image => self
                .client
                .attach_uploaded_image_detailed(target, upload, caption, &self.scope)
                .await
                .map(|value| value.block_id),
            FileKind::Other => self
                .client
                .attach_uploaded_file_detailed(target, upload, caption, &self.scope)
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
    ) -> Result<crate::ValidatedFile, IngestFailure> {
        let mut bytes = Vec::new();
        tokio::fs::File::from_std(
            file.reopen()
                .map_err(|_| IngestFailure::DownloadUnavailable)?,
        )
        .take((self.validation.max_bytes() + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| IngestFailure::DownloadUnavailable)?;
        if bytes.len() != file.size_bytes() {
            return Err(IngestFailure::InvalidInput);
        }
        // Optional hints never override observed signature/UTF-8 validation.
        let inferred = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
            "image/png"
        } else if bytes.starts_with(b"\xff\xd8\xff") {
            "image/jpeg"
        } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
            "image/gif"
        } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
            "image/webp"
        } else if bytes.starts_with(b"%PDF-") {
            "application/pdf"
        } else {
            "text/plain"
        };
        let inferred = if inferred == "text/plain" {
            match name
                .and_then(|value| value.rsplit('.').next())
                .map(str::to_ascii_lowercase)
                .as_deref()
            {
                Some("md" | "markdown") => "text/markdown",
                Some("csv") => "text/csv",
                _ => inferred,
            }
        } else {
            inferred
        };
        let mime = mime.unwrap_or(inferred);
        let fallback = match mime
            .trim()
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "image/png" => "uploaded.png",
            "image/jpeg" => "uploaded.jpg",
            "image/gif" => "uploaded.gif",
            "image/webp" => "uploaded.webp",
            "application/pdf" => "uploaded.pdf",
            "text/markdown" => "uploaded.md",
            "text/csv" => "uploaded.csv",
            _ => "uploaded.txt",
        };
        self.validation
            .validate(name.unwrap_or(fallback), mime, &bytes)
            .map_err(|_| IngestFailure::InvalidInput)
    }
}

#[cfg(test)]
mod tests;
