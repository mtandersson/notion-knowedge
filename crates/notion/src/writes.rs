//! Non-destructive, synchronous Markdown write primitives.
use crate::NotionClient;
use notion_knowledge_core::backend::{
    AppendPageContent, BackendError, BackendErrorKind, CreatePage, PageId,
};
use reqwest::{Method, header::AUTHORIZATION};
use serde_json::{Value, json};

pub const MARKDOWN_VERSION: &str = "2026-03-11";

/// Authoritative creation receipt; not an incomplete core Page metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatedPage {
    pub id: PageId,
    pub url: String,
}

fn failure(operation: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation,
        kind,
        retry_after: None,
    }
}

fn valid_id(id: &PageId) -> bool {
    let raw: String = id.0.chars().filter(|c| *c != '-').collect();
    (id.0.len() == 32
        || (id.0.len() == 36 && [8, 13, 18, 23].iter().all(|i| id.0.as_bytes()[*i] == b'-')))
        && raw.len() == 32
        && raw.bytes().all(|c| c.is_ascii_hexdigit())
}

impl NotionClient {
    /// Create below an explicit parent (including a configured root supplied by composition).
    /// The integration must have access to that parent. Never creates a workspace root.
    pub async fn create_page(&self, request: CreatePage) -> Result<CreatedPage, BackendError> {
        let operation = "notion.create";
        if !valid_id(&request.parent_page_id)
            || request.title.is_empty()
            || request.title.chars().count() > 2000
        {
            return Err(failure(operation, BackendErrorKind::InvalidInput));
        }
        let body = json!({"parent":{"page_id":request.parent_page_id.0},
            "properties":{"title":{"type":"title","title":[{"type":"text","text":{"content":request.title}}]}},
            "markdown":request.markdown});
        let result = self.write(Method::POST, "/pages", body, operation).await?;
        let id = PageId(result["id"].as_str().unwrap_or_default().to_owned());
        let url = result["url"].as_str().unwrap_or_default();
        if result["object"] != "page" || !valid_id(&id) || !url.starts_with("https://") {
            return Err(failure(operation, BackendErrorKind::Internal));
        }
        Ok(CreatedPage {
            id,
            url: url.to_owned(),
        })
    }

    /// Append only: no selection, replacement, deletion, batching or automatic retries.
    pub async fn append_content(&self, request: AppendPageContent) -> Result<(), BackendError> {
        let operation = "notion.append";
        if !valid_id(&request.page_id) || request.markdown.trim().is_empty() {
            return Err(failure(operation, BackendErrorKind::InvalidInput));
        }
        let result = self.write(Method::PATCH, &format!("/pages/{}/markdown", request.page_id.0),
            json!({"type":"insert_content","insert_content":{"content":request.markdown,"position":{"type":"end"}}}), operation).await?;
        if result["object"] != "page_markdown"
            || result["id"]
                .as_str()
                .map(|id| id.replace('-', "").to_lowercase())
                != Some(request.page_id.0.replace('-', "").to_lowercase())
        {
            return Err(failure(operation, BackendErrorKind::Internal));
        }
        Ok(())
    }

    async fn write(
        &self,
        method: Method,
        path: &str,
        body: Value,
        operation: &'static str,
    ) -> Result<Value, BackendError> {
        // Notion request payload limit. Reject before a potentially non-idempotent mutation.
        let body = serde_json::to_vec(&body)
            .map_err(|_| failure(operation, BackendErrorKind::InvalidInput))?;
        if body.len() > 500 * 1024 {
            return Err(failure(operation, BackendErrorKind::InvalidInput));
        }
        let mut response = self
            .http
            .request(method, format!("{}{path}", self.api_root))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", MARKDOWN_VERSION)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| failure(operation, BackendErrorKind::Unavailable))?;
        if response.status().as_u16() != 200 {
            let kind = match response.status().as_u16() {
                400 => BackendErrorKind::InvalidInput,
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                404 => BackendErrorKind::NotFound,
                409 => BackendErrorKind::Conflict,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            };
            return Err(failure(operation, kind));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure(operation, BackendErrorKind::Unavailable))?
        {
            if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
                return Err(failure(operation, BackendErrorKind::Internal));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| failure(operation, BackendErrorKind::Internal))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    const MARKDOWN: &str = "# Heading\n\n**Bold** and [link](https://example.com)\n- Item\n- [ ] Task\n\n```rust\nlet x = 1;\n```";

    async fn mock(
        status: u16,
        body: &str,
    ) -> (NotionClient, tokio::task::JoinHandle<(String, Value)>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential").unwrap();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let body = body.to_owned();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (headers, start, length) = loop {
                let mut buf = [0; 4096];
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|l| {
                            l.to_lowercase()
                                .strip_prefix("content-length: ")
                                .map(|v| v.parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (headers, end + 4, length);
                }
            };
            while bytes.len() < start + length {
                let mut buf = [0; 4096];
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            (
                headers,
                serde_json::from_slice(&bytes[start..start + length]).unwrap(),
            )
        });
        (client, server)
    }

    #[tokio::test]
    async fn creation_uses_explicit_root_and_returns_authoritative_identity() {
        let (client, server) = mock(
            200,
            &format!(r#"{{"object":"page","id":"{ID}","url":"https://www.notion.so/{ID}"}}"#),
        )
        .await;
        let page = client
            .create_page(CreatePage {
                parent_page_id: PageId(ID.into()),
                title: "Title".into(),
                markdown: MARKDOWN.into(),
            })
            .await
            .unwrap();
        assert_eq!(page.id, PageId(ID.into()));
        assert_eq!(page.url, format!("https://www.notion.so/{ID}"));
        let (headers, body) = server.await.unwrap();
        assert!(headers.starts_with("POST /v1/pages HTTP/1.1"));
        assert!(
            headers
                .to_lowercase()
                .contains("authorization: bearer test-credential")
        );
        assert!(headers.contains(MARKDOWN_VERSION));
        assert_eq!(body["parent"]["page_id"], ID);
        assert_eq!(
            body["properties"]["title"]["title"][0]["text"]["content"],
            "Title"
        );
        assert_eq!(body["markdown"], MARKDOWN);
        assert!(body.get("children").is_none());
    }

    #[tokio::test]
    async fn append_preserves_markdown_and_requests_only_end_insertion() {
        let (client, server) = mock(
            200,
            &format!(r#"{{"object":"page_markdown","id":"{ID}","markdown":"existing\nnew"}}"#),
        )
        .await;
        client
            .append_content(AppendPageContent {
                page_id: PageId(ID.into()),
                markdown: MARKDOWN.into(),
            })
            .await
            .unwrap();
        let (headers, body) = server.await.unwrap();
        assert!(headers.starts_with(&format!("PATCH /v1/pages/{ID}/markdown HTTP/1.1")));
        assert_eq!(
            body,
            json!({"type":"insert_content","insert_content":{"content":MARKDOWN,"position":{"type":"end"}}})
        );
    }

    #[tokio::test]
    async fn writes_surface_sanitized_failures_without_retrying_or_replacing() {
        for (status, kind) in [
            (400, BackendErrorKind::InvalidInput),
            (403, BackendErrorKind::PermissionDenied),
            (404, BackendErrorKind::NotFound),
            (429, BackendErrorKind::RateLimited),
            (503, BackendErrorKind::Unavailable),
        ] {
            let (client, server) = mock(status, "private content test-credential").await;
            let error = client
                .append_content(AppendPageContent {
                    page_id: PageId(ID.into()),
                    markdown: MARKDOWN.into(),
                })
                .await
                .unwrap_err();
            assert_eq!(error.kind, kind);
            assert!(!format!("{error:?}").contains("test-credential"));
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn invalid_inputs_are_rejected_before_network_mutation() {
        let client = NotionClient::integration("test-credential").unwrap();
        for id in ["", "../users/me", "12345678-1234-1234-1234-123456789abz"] {
            assert_eq!(
                client
                    .append_content(AppendPageContent {
                        page_id: PageId(id.into()),
                        markdown: MARKDOWN.into()
                    })
                    .await
                    .unwrap_err()
                    .kind,
                BackendErrorKind::InvalidInput
            );
        }
        assert_eq!(
            client
                .create_page(CreatePage {
                    parent_page_id: PageId(ID.into()),
                    title: "x".repeat(2001),
                    markdown: String::new()
                })
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::InvalidInput
        );
        assert_eq!(
            client
                .append_content(AppendPageContent {
                    page_id: PageId(ID.into()),
                    markdown: "x".repeat(500 * 1024)
                })
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::InvalidInput
        );
    }

    #[tokio::test]
    async fn malformed_or_async_receipts_are_not_reported_as_completed_writes() {
        for (status, body) in [
            (202, r#"{"object":"async_task"}"#),
            (200, "{}"),
            (200, "invalid"),
        ] {
            let (client, server) = mock(status, body).await;
            assert_eq!(
                client
                    .append_content(AppendPageContent {
                        page_id: PageId(ID.into()),
                        markdown: MARKDOWN.into()
                    })
                    .await
                    .unwrap_err()
                    .kind,
                BackendErrorKind::Internal
            );
            server.await.unwrap();
        }
    }
}
