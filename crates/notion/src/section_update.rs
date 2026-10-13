//! Exactly one synchronous, non-retried Notion Markdown search-and-replace.
//! This adapter is not exposed as an MCP mutation tool.
use crate::{NotionClient, pages::uuid};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind, UpdateSectionContent};
use reqwest::Method;
use serde_json::json;

fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.update_section",
        retry_after: None,
        committed_page_id: None,
    }
}

impl NotionClient {
    pub(crate) async fn update_section_content(
        &self,
        request: UpdateSectionContent,
    ) -> Result<(), BackendError> {
        let id = uuid(&request.page_id.0).ok_or_else(|| error(BackendErrorKind::InvalidInput))?;
        if id != request.page_id.0
            || request.old_str.is_empty()
            || request.old_str == request.new_str
            || request.old_str.len() > 1024 * 1024
            || request.new_str.len() > 200_000
            || request.old_str.contains('\0')
            || request.new_str.contains('\0')
        {
            return Err(error(BackendErrorKind::InvalidInput));
        }
        // Never allow whole-page replacement, deletion of child content or
        // matching multiple ranges. The transport sends a non-retryable PATCH.
        let receipt = self
            .write(
                Method::PATCH,
                &format!("/pages/{id}/markdown"),
                json!({
                    "type": "update_content",
                    "update_content": {
                        "content_updates": [{
                            "old_str": request.old_str,
                            "new_str": request.new_str,
                            "replace_all_matches": false
                        }],
                        "allow_deleting_content": false
                    },
                    "allow_async": false
                }),
                "notion.update_section",
            )
            .await?;
        if receipt["object"] != "page_markdown"
            || receipt["id"].as_str().and_then(uuid).as_deref() != Some(id.as_str())
            || receipt["truncated"] != false
            || receipt["unknown_block_ids"] != json!([])
        {
            return Err(error(BackendErrorKind::Internal));
        }
        // A successful PATCH receipt is not sufficient: the core workflow
        // reads the complete authoritative Markdown again before commitment.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::backend::PageId;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PAGE: &str = "12345678-1234-1234-1234-123456789abc";
    fn update() -> UpdateSectionContent {
        UpdateSectionContent {
            page_id: PageId(PAGE.into()),
            old_str: "Old paragraph\n".into(),
            new_str: "New paragraph\n".into(),
        }
    }
    async fn mock(
        status: u16,
        response: String,
    ) -> (NotionClient, tokio::task::JoinHandle<(String, Value)>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-key")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (headers, start, length) = loop {
                let mut buf = [0; 2048];
                let len = stream.read(&mut buf).await.unwrap();
                assert!(len > 0);
                bytes.extend_from_slice(&buf[..len]);
                if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .and_then(|v| v.parse::<usize>().ok())
                        })
                        .unwrap_or(0);
                    break (headers, end + 4, len);
                }
            };
            while bytes.len() < start + length {
                let mut buf = [0; 2048];
                let len = stream.read(&mut buf).await.unwrap();
                assert!(len > 0);
                bytes.extend_from_slice(&buf[..len]);
            }
            let data: Value = serde_json::from_slice(&bytes[start..start + length]).unwrap();
            stream.write_all(format!(
                "HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",
                response.len()
            ).as_bytes()).await.unwrap();
            (headers, data)
        });
        (client, server)
    }

    #[tokio::test]
    async fn sends_one_exact_bounded_patch_without_deletion() {
        let result =
            json!({"object":"page_markdown","id":PAGE,"truncated":false,"unknown_block_ids":[]})
                .to_string();
        let (client, server) = mock(200, result).await;
        client.update_section_content(update()).await.unwrap();
        let (header, body) = server.await.unwrap();
        assert!(header.starts_with(&format!("PATCH /v1/pages/{PAGE}/markdown ")));
        assert!(
            header
                .to_ascii_lowercase()
                .contains("notion-version: 2026-03-11")
        );
        assert_eq!(body["type"], "update_content");
        assert!(body.get("replace_content").is_none());
        assert_eq!(
            body["update_content"]["content_updates"][0]["old_str"],
            "Old paragraph\n"
        );
        assert_eq!(
            body["update_content"]["content_updates"][0]["new_str"],
            "New paragraph\n"
        );
        assert_eq!(
            body["update_content"]["content_updates"][0]["replace_all_matches"],
            false
        );
        assert_eq!(body["update_content"]["allow_deleting_content"], false);
        assert_eq!(body["allow_async"], false);
        assert_eq!(client.request_metrics().attempts, 1);
        assert_eq!(client.request_metrics().retries, 0);
    }

    #[tokio::test]
    async fn rejects_invalid_input_without_network() {
        let client = NotionClient::integration("fixture-key")
            .unwrap()
            .without_retries();
        for bad in [
            UpdateSectionContent {
                old_str: String::new(),
                ..update()
            },
            UpdateSectionContent {
                new_str: "Old paragraph\n".into(),
                ..update()
            },
            UpdateSectionContent {
                page_id: PageId("../escape".into()),
                ..update()
            },
            UpdateSectionContent {
                old_str: "\0".into(),
                ..update()
            },
        ] {
            assert_eq!(
                client.update_section_content(bad).await.unwrap_err().kind,
                BackendErrorKind::InvalidInput
            );
        }
        assert_eq!(client.request_metrics().attempts, 0);
    }

    #[tokio::test]
    async fn conflict_async_and_malformed_response_never_count_as_success() {
        for (status, body) in [
            (409, r#"{"object":"error"}"#),
            (202, r#"{"object":"async_task"}"#),
            (
                200,
                r#"{"object":"page_markdown","id":"12345678-1234-1234-1234-123456789abc","truncated":true,"unknown_block_ids":[]}"#,
            ),
        ] {
            let (client, server) = mock(status, body.into()).await;
            assert!(client.update_section_content(update()).await.is_err());
            server.await.unwrap();
            assert_eq!(client.request_metrics().attempts, 1);
            assert_eq!(client.request_metrics().retries, 0);
        }
    }
}
