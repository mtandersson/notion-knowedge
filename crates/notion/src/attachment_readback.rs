//! Bounded, credential-safe authoritative read-back for native Notion attachments.
//!
//! This helper never retries a mutation, and no caller may claim that an
//! attachment exists unless a fresh block GET matches the expected receipt.
use crate::{
    NotionClient,
    pages::{self, PAGE_VERSION},
};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use reqwest::header::AUTHORIZATION;
use serde_json::Value;
use std::time::Duration;

const MAX_ATTEMPTS: usize = 3;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;
const DELAYS_MS: [u64; MAX_ATTEMPTS - 1] = [150, 300];

/// Ephemeral identity of the provider's native append receipt. No Debug or
/// serialization: hosted resource paths and signed parameters are private.
pub(crate) struct AttachmentSource {
    upload_id: String,
    hosted: Option<HostedResource>,
}

#[derive(PartialEq, Eq)]
struct HostedResource {
    origin: String,
    path: String,
    resource_query: Vec<(String, String)>,
}

impl HostedResource {
    fn parse(value: &Value) -> Option<Self> {
        if value["type"] != "file" {
            return None;
        }
        let input = value["file"]["url"].as_str()?;
        if input.len() > 8192 || input.chars().any(|c| c.is_control() || c.is_whitespace()) {
            return None;
        }
        let url = reqwest::Url::parse(input).ok()?;
        if url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        // Signing/expiry parameters rotate; other query fields may identify an
        // object version and must remain equal. Unknown fields fail closed.
        let mut resource_query: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| {
                !matches!(
                    key.to_ascii_lowercase().as_str(),
                    "x-amz-algorithm"
                        | "x-amz-credential"
                        | "x-amz-date"
                        | "x-amz-expires"
                        | "x-amz-signedheaders"
                        | "x-amz-signature"
                        | "x-amz-security-token"
                        | "awsaccesskeyid"
                        | "signature"
                        | "expires"
                )
            })
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        let mut distinct = std::collections::BTreeSet::new();
        if resource_query.iter().any(|(key, _)| !distinct.insert(key)) {
            return None;
        }
        resource_query.sort();
        Some(Self {
            origin: url.origin().ascii_serialization(),
            path: url.path().to_owned(),
            resource_query,
        })
    }
}

impl AttachmentSource {
    pub(crate) fn from_receipt(upload_id: &str, value: &Value) -> Self {
        Self {
            upload_id: upload_id.to_owned(),
            hosted: HostedResource::parse(value),
        }
    }
    pub(crate) fn matches(&self, value: &Value) -> bool {
        match value["type"].as_str() {
            Some("file_upload") => {
                value["file_upload"]["id"]
                    .as_str()
                    .and_then(pages::uuid)
                    .as_deref()
                    == Some(self.upload_id.as_str())
            }
            Some("file") => match (&self.hosted, HostedResource::parse(value)) {
                (Some(expected), Some(observed)) => *expected == observed,
                _ => false,
            },
            _ => false,
        }
    }
}

fn failure(operation: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation,
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}

impl NotionClient {
    /// GET the exact block returned by Notion's append receipt, retrying only
    /// absent (404) or not-yet-matching reads for eventual consistency.
    ///
    /// The caller supplies a strict matcher for its authenticated target page,
    /// block ID, uploaded source, caption and optional filename. A positive
    /// result MUST come from a complete fresh Notion response; no local cache,
    /// append receipt or assumed attachment can satisfy this check.
    ///
    /// HTTP 401/403/429/5xx and malformed or oversized responses fail
    /// immediately (the shared read-only transport has its own bounded policy).
    /// 404 and a syntactically valid but mismatched block are at most 3 reads.
    /// Cancellation does not issue any further writes.
    pub(crate) async fn verify_attached_block<F>(
        &self,
        block_id: &str,
        operation: &'static str,
        matches: F,
    ) -> Result<(), BackendError>
    where
        F: Fn(&Value) -> bool,
    {
        if pages::uuid(block_id).as_deref() != Some(block_id) {
            return Err(failure(operation, BackendErrorKind::InvalidInput));
        }
        for attempt in 0..MAX_ATTEMPTS {
            let request = self
                .http
                .get(format!("{}/blocks/{block_id}", self.api_root))
                .header(AUTHORIZATION, self.authorization.clone())
                .header("Notion-Version", PAGE_VERSION);
            let mut response = self.send(request, true, operation).await?;
            let status = response.status().as_u16();
            if status != 200 && status != 404 {
                return Err(failure(
                    operation,
                    match status {
                        400 => BackendErrorKind::InvalidInput,
                        401 => BackendErrorKind::Unauthenticated,
                        403 => BackendErrorKind::PermissionDenied,
                        409 => BackendErrorKind::Conflict,
                        429 => BackendErrorKind::RateLimited,
                        500..=599 => BackendErrorKind::Unavailable,
                        _ => BackendErrorKind::Internal,
                    },
                ));
            }
            if status == 200 {
                let mut body = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|_| failure(operation, BackendErrorKind::Unavailable))?
                {
                    if body.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
                        return Err(failure(operation, BackendErrorKind::Internal));
                    }
                    body.extend_from_slice(&chunk);
                }
                let value: Value = serde_json::from_slice(&body)
                    .map_err(|_| failure(operation, BackendErrorKind::Internal))?;
                if matches(&value) {
                    return Ok(());
                }
            }
            if let Some(ms) = DELAYS_MS.get(attempt) {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
            }
        }
        Err(failure(operation, BackendErrorKind::Conflict))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const BLOCK: &str = "88888888-8888-8888-8888-888888888888";
    const OP: &str = "notion.attachment.verify.test";

    async fn fixture(
        replies: Vec<(u16, String)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-secret")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut paths = Vec::new();
            for (status, body) in replies {
                let (mut stream, _) =
                    tokio::time::timeout(Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut received = Vec::new();
                loop {
                    let mut data = [0u8; 2048];
                    let n = stream.read(&mut data).await.unwrap();
                    assert!(n > 0);
                    received.extend_from_slice(&data[..n]);
                    if received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8(received).unwrap();
                assert!(request.starts_with(&format!("GET /v1/blocks/{BLOCK} HTTP/1.1")));
                assert!(
                    request
                        .to_ascii_lowercase()
                        .contains("authorization: bearer fixture-secret")
                );
                paths.push(request.lines().next().unwrap().to_owned());
                stream.write_all(format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len(),
                ).as_bytes()).await.unwrap();
            }
            paths
        });
        (client, task)
    }

    #[tokio::test]
    async fn immediate_match_needs_one_read() {
        let (client, server) = fixture(vec![(200, json!({"found":true}).to_string())]).await;
        client
            .verify_attached_block(BLOCK, OP, |v| v["found"] == true)
            .await
            .unwrap();
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn retries_not_found_and_stale_read_before_confirming_exact_block() {
        let (client, server) = fixture(vec![
            (404, "{}".into()),
            (200, json!({"found":false}).to_string()),
            (200, json!({"found":true}).to_string()),
        ])
        .await;
        client
            .verify_attached_block(BLOCK, OP, |v| v["found"] == true)
            .await
            .unwrap();
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn exhausted_not_found_and_wrong_block_are_failures() {
        for (status, body) in [
            (404, "{}".to_owned()),
            (200, json!({"found":false}).to_string()),
        ] {
            let (client, server) = fixture(vec![(status, body.clone()); MAX_ATTEMPTS]).await;
            let error = client
                .verify_attached_block(BLOCK, OP, |v| v["found"] == true)
                .await
                .unwrap_err();
            assert_eq!(error.kind, BackendErrorKind::Conflict);
            assert_eq!(error.operation, OP);
            assert_eq!(server.await.unwrap().len(), MAX_ATTEMPTS);
        }
    }

    #[tokio::test]
    async fn permission_denial_is_not_retried() {
        let (client, server) = fixture(vec![(403, "secret upstream details".into())]).await;
        let error = client
            .verify_attached_block(BLOCK, OP, |_| false)
            .await
            .unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::PermissionDenied);
        assert!(!format!("{error:?}").contains("secret"));
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn malformed_response_is_not_retried_or_disclosed() {
        let (client, server) = fixture(vec![(200, "private sentinel text".into())]).await;
        let error = client
            .verify_attached_block(BLOCK, OP, |_| true)
            .await
            .unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::Internal);
        assert!(!format!("{error:?}").contains("sentinel"));
        assert_eq!(server.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn invalid_uuid_fails_before_network() {
        let client = NotionClient::integration("fixture-secret").unwrap();
        assert_eq!(
            client
                .verify_attached_block("../secret", OP, |_| true)
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::InvalidInput
        );
    }
}

#[cfg(test)]
mod source_identity_tests {
    use super::*;
    use serde_json::json;
    const UPLOAD: &str = "12345678-1234-1234-1234-123456789abc";
    fn hosted(url: &str) -> Value {
        json!({"type":"file","file":{"url":url}})
    }

    #[test]
    fn hosted_signature_rotation_keeps_the_same_canonical_resource() {
        let receipt = hosted(
            "https://FILES.notion.so:443/object?versionId=original&X-Amz-Signature=old&X-Amz-Date=old",
        );
        let expected = AttachmentSource::from_receipt(UPLOAD, &receipt);
        assert!(expected.matches(&hosted(
            "https://files.notion.so/object?X-Amz-Date=new&X-Amz-Signature=new&versionId=original"
        )));
        for changed in [
            "https://different.notion.so/object?versionId=original",
            "https://files.notion.so/replacement?versionId=original",
            "https://files.notion.so/object?versionId=replacement",
            "https://files.notion.so:444/object?versionId=original",
        ] {
            assert!(!expected.matches(&hosted(changed)));
        }
    }
    #[test]
    fn mixed_readback_forms_require_a_provable_source_identity() {
        let hosted_receipt =
            AttachmentSource::from_receipt(UPLOAD, &hosted("https://files.notion.so/object"));
        assert!(hosted_receipt.matches(&json!({"type":"file_upload","file_upload":{"id":UPLOAD}})));
        let upload_receipt = AttachmentSource::from_receipt(
            UPLOAD,
            &json!({"type":"file_upload","file_upload":{"id":UPLOAD}}),
        );
        assert!(upload_receipt.matches(&json!({"type":"file_upload","file_upload":{"id":UPLOAD}})));
        assert!(!upload_receipt.matches(&hosted("https://files.notion.so/object")));
        assert!(!hosted_receipt.matches(&json!({"type":"file_upload","file_upload":{"id":"12345678-1234-1234-1234-123456789abd"}})));
    }
    #[test]
    fn malformed_hosted_urls_cannot_be_an_identity_anchor() {
        for url in [
            "http://files.notion.so/object",
            "https://",
            "https://user:password@files.notion.so/object",
            "https://files.notion.so/object#fragment",
            "https://files.notion.so/with whitespace",
        ] {
            let receipt = hosted(url);
            let expected = AttachmentSource::from_receipt(UPLOAD, &receipt);
            assert!(!expected.matches(&receipt));
        }
    }
}
