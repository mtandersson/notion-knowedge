//! Server-only Notion single-part upload primitive.
//!
//! This does not activate the ChatGPT upload tool or attach content to pages.
//! The caller must hold a request-scoped, bounded temporary download; every
//! upload is revalidated before any network effect and is never replayed.
use crate::{
    NotionClient, file_download::DownloadedFile, file_validation::FileValidationPolicy, pages,
    writes::MARKDOWN_VERSION,
};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use reqwest::{
    Method,
    header::AUTHORIZATION,
    multipart::{Form, Part},
};
use serde_json::{Value, json};
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;

const CREATE: &str = "notion.file_upload.create";
const SEND: &str = "notion.file_upload.send";
const MAX_RECEIPT_BYTES: usize = 64 * 1024;

fn error(operation: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation,
        retry_after: None,
        committed_page_id: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UploadStatus {
    Uploaded,
}

/// A verified upstream upload, not evidence of a block/page attachment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadedFile {
    pub id: String,
    pub status: UploadStatus,
    pub filename: String,
    pub mime_type: String,
    pub size_bytes: usize,
}

fn upload_id(
    value: &Value,
    expected_status: &str,
    operation: &'static str,
) -> Result<String, BackendError> {
    if value["object"] != "file_upload"
        || value["status"] != expected_status
        || value["in_trash"] == true
    {
        return Err(error(operation, BackendErrorKind::Internal));
    }
    value["id"]
        .as_str()
        .and_then(pages::uuid)
        .ok_or_else(|| error(operation, BackendErrorKind::Internal))
}

fn classify(status: u16) -> BackendErrorKind {
    match status {
        400 | 413 | 415 => BackendErrorKind::InvalidInput,
        401 => BackendErrorKind::Unauthenticated,
        403 => BackendErrorKind::PermissionDenied,
        404 => BackendErrorKind::NotFound,
        409 => BackendErrorKind::Conflict,
        429 | 529 => BackendErrorKind::RateLimited,
        500..=599 => BackendErrorKind::Unavailable,
        _ => BackendErrorKind::Internal,
    }
}

impl NotionClient {
    /// Create and send a Notion single-part file using this client's credential.
    ///
    /// The downloaded temporary file must remain owned by the active request.
    /// A separate file handle streams the upload after a bounded signature/MIME
    /// check. Failed/ambiguous sends are never retried or attached.
    pub async fn upload_single_part(
        &self,
        file: &DownloadedFile,
        filename: &str,
        declared_mime: &str,
        policy: FileValidationPolicy,
    ) -> Result<UploadedFile, BackendError> {
        if file.size_bytes() == 0 || file.size_bytes() > policy.max_bytes() {
            return Err(error(CREATE, BackendErrorKind::InvalidInput));
        }
        let reader = tokio::fs::File::from_std(
            file.reopen()
                .map_err(|_| error(CREATE, BackendErrorKind::Unavailable))?,
        );
        let mut bytes = Vec::new();
        reader
            .take((policy.max_bytes() + 1) as u64)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| error(CREATE, BackendErrorKind::Unavailable))?;
        if bytes.len() != file.size_bytes() {
            return Err(error(CREATE, BackendErrorKind::InvalidInput));
        }
        let valid = policy
            .validate(filename, declared_mime, &bytes)
            .map_err(|_| error(CREATE, BackendErrorKind::InvalidInput))?;
        drop(bytes);

        let created = self.write(
            Method::POST,
            "/file_uploads",
            json!({"mode": "single_part", "filename": valid.filename, "content_type": valid.mime_type}),
            CREATE,
        ).await?;
        let id = upload_id(&created, "pending", CREATE)?;

        // Reopen independently: a previous validator or concurrent reader must
        // not share the transfer's file offset. The body is streamed, not buffered.
        let stream = tokio::fs::File::from_std(
            file.reopen()
                .map_err(|_| error(SEND, BackendErrorKind::Unavailable))?,
        );
        let part = Part::stream_with_length(
            reqwest::Body::wrap_stream(ReaderStream::new(stream)),
            valid.size_bytes as u64,
        )
        .file_name(valid.filename.clone())
        .mime_str(valid.mime_type)
        .map_err(|_| error(SEND, BackendErrorKind::InvalidInput))?;
        let request = self
            .http
            .post(format!("{}/file_uploads/{id}/send", self.api_root))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", MARKDOWN_VERSION)
            .multipart(Form::new().part("file", part));
        // One-shot streaming bodies are deliberately not retryable. The shared
        // transport still performs admission and rate-limit accounting.
        let mut response = self.send(request, false, SEND).await?;
        if response.status().as_u16() != 200 {
            return Err(error(SEND, classify(response.status().as_u16())));
        }
        let mut receipt = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(SEND, BackendErrorKind::Unavailable))?
        {
            if receipt.len().saturating_add(chunk.len()) > MAX_RECEIPT_BYTES {
                return Err(error(SEND, BackendErrorKind::Internal));
            }
            receipt.extend_from_slice(&chunk);
        }
        let result: Value = serde_json::from_slice(&receipt)
            .map_err(|_| error(SEND, BackendErrorKind::Internal))?;
        if upload_id(&result, "uploaded", SEND)? != id {
            return Err(error(SEND, BackendErrorKind::Internal));
        }
        if let Some(length) = result["content_length"].as_u64()
            && length != valid.size_bytes as u64
        {
            return Err(error(SEND, BackendErrorKind::Internal));
        }
        Ok(UploadedFile {
            id,
            status: UploadStatus::Uploaded,
            filename: valid.filename,
            mime_type: valid.mime_type.into(),
            size_bytes: valid.size_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    const FIXTURE: &[u8] = b"small fixture";
    fn receipt(status: &str) -> String {
        json!({"object":"file_upload","id":ID,"status":status,"content_length":FIXTURE.len()})
            .to_string()
    }

    async fn upstream(
        steps: Vec<(u16, String)>,
    ) -> (
        NotionClient,
        tokio::task::JoinHandle<Vec<(String, Vec<u8>)>>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-secret")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut seen = Vec::new();
            for (status, body) in steps {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut wire = Vec::new();
                let (headers, start, length, chunked) = loop {
                    let mut buf = [0_u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    wire.extend_from_slice(&buf[..n]);
                    if let Some(end) = wire.windows(4).position(|v| v == b"\r\n\r\n") {
                        let headers = String::from_utf8(wire[..end].to_vec()).unwrap();
                        let lower = headers.to_ascii_lowercase();
                        let length = lower.lines().find_map(|line| {
                            line.strip_prefix("content-length: ")?.parse::<usize>().ok()
                        });
                        let chunked = lower.contains("transfer-encoding: chunked");
                        break (headers, end + 4, length, chunked);
                    }
                };
                if let Some(length) = length {
                    while wire.len() < start + length {
                        let mut buf = [0_u8; 8192];
                        let n = socket.read(&mut buf).await.unwrap();
                        assert!(n > 0);
                        wire.extend_from_slice(&buf[..n]);
                    }
                } else if chunked {
                    while !wire[start..].windows(5).any(|v| v == b"0\r\n\r\n") {
                        let mut buf = [0_u8; 8192];
                        let n = socket.read(&mut buf).await.unwrap();
                        assert!(n > 0);
                        wire.extend_from_slice(&buf[..n]);
                    }
                } else {
                    panic!("fixture request had no body framing");
                }
                seen.push((headers, wire[start..].to_vec()));
                socket.write_all(format!(
                    "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                ).as_bytes()).await.unwrap();
            }
            seen
        });
        (client, task)
    }

    #[tokio::test]
    async fn creates_then_streams_small_fixture_with_same_credential_and_no_attachment() {
        let (client, task) =
            upstream(vec![(200, receipt("pending")), (200, receipt("uploaded"))]).await;
        let file = DownloadedFile::test_fixture(FIXTURE);
        let uploaded = client
            .upload_single_part(
                &file,
                "notes.txt",
                "text/plain",
                FileValidationPolicy::default(),
            )
            .await
            .unwrap();
        assert_eq!(uploaded.id, ID);
        assert_eq!(uploaded.status, UploadStatus::Uploaded);
        assert_eq!(uploaded.size_bytes, FIXTURE.len());
        let requests = task.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].0.starts_with("POST /v1/file_uploads HTTP/1.1"));
        assert_eq!(
            serde_json::from_slice::<Value>(&requests[0].1).unwrap(),
            json!({
                "mode":"single_part", "filename":"notes.txt", "content_type":"text/plain"
            })
        );
        assert!(
            requests[1]
                .0
                .starts_with(&format!("POST /v1/file_uploads/{ID}/send HTTP/1.1"))
        );
        for (headers, _) in &requests {
            let lower = headers.to_ascii_lowercase();
            assert!(lower.contains("authorization: bearer fixture-secret"));
            assert!(lower.contains(&format!("notion-version: {MARKDOWN_VERSION}")));
        }
        assert!(
            requests[1]
                .0
                .to_ascii_lowercase()
                .contains("multipart/form-data; boundary=")
        );
        assert!(requests[1].1.windows(FIXTURE.len()).any(|v| v == FIXTURE));
        assert!(
            requests[1]
                .1
                .windows(b"name=\"file\"".len())
                .any(|v| v == b"name=\"file\"")
        );
        assert!(
            !requests
                .iter()
                .any(|(headers, _)| headers.contains("/blocks/"))
        );
    }

    #[tokio::test]
    async fn rejects_bad_bytes_before_create() {
        let client = NotionClient::integration("fixture-secret").unwrap();
        let file = DownloadedFile::test_fixture(b"not a PNG");
        let err = client
            .upload_single_part(
                &file,
                "image.png",
                "image/png",
                FileValidationPolicy::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, BackendErrorKind::InvalidInput);
        assert_eq!(err.operation, CREATE);
    }

    #[tokio::test]
    async fn failed_create_and_send_do_not_report_uploaded() {
        for (steps, operation, kind) in [
            (
                vec![(403, "{}".into())],
                CREATE,
                BackendErrorKind::PermissionDenied,
            ),
            (
                vec![
                    (200, receipt("pending")),
                    (503, "private sentinelsecret".into()),
                ],
                SEND,
                BackendErrorKind::Unavailable,
            ),
            (
                vec![(200, receipt("pending")), (200, receipt("pending"))],
                SEND,
                BackendErrorKind::Internal,
            ),
        ] {
            let (client, task) = upstream(steps).await;
            let file = DownloadedFile::test_fixture(FIXTURE);
            let err = client
                .upload_single_part(
                    &file,
                    "notes.txt",
                    "text/plain",
                    FileValidationPolicy::default(),
                )
                .await
                .unwrap_err();
            assert_eq!(err.operation, operation);
            assert_eq!(err.kind, kind);
            assert!(!format!("{err:?} {err}").contains("sentinelsecret"));
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn refuses_invalid_create_receipt_without_sending_bytes() {
        let (client, task) = upstream(vec![(
            200,
            json!({
                "object":"file_upload", "id":"not-an-id", "status":"pending"
            })
            .to_string(),
        )])
        .await;
        let file = DownloadedFile::test_fixture(FIXTURE);
        assert_eq!(
            client
                .upload_single_part(
                    &file,
                    "notes.txt",
                    "text/plain",
                    FileValidationPolicy::default()
                )
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Internal
        );
        assert_eq!(task.await.unwrap().len(), 1);
    }
}
