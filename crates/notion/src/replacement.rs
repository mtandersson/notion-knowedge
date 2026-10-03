//! Explicit, fail-closed replacement of ordinary Markdown pages.
use crate::{NotionClient, pages::uuid};
use notion_knowledge_core::backend::{
    BackendError, BackendErrorKind, PageContent, ReplacePageContent,
};
use pulldown_cmark::{CodeBlockKind, Event, LinkType, Options, Parser, Tag};
use reqwest::Method;
use serde_json::json;

fn failure(operation: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation,
        kind,
        retry_after: None,
    }
}

/// Conservative allowlist: enhanced Notion XML, embeds, images and extension
/// constructs need preservation semantics this primitive cannot guarantee.
fn supported(markdown: &str) -> bool {
    Parser::new_ext(markdown, Options::all()).all(|event| match event {
        Event::Start(Tag::Heading {
            id, classes, attrs, ..
        }) => id.is_none() && classes.is_empty() && attrs.is_empty(),
        Event::Start(Tag::Link { link_type, .. }) => {
            !matches!(link_type, LinkType::WikiLink { .. })
        }
        Event::Start(tag) => matches!(
            tag,
            Tag::Paragraph
                | Tag::BlockQuote(None)
                | Tag::CodeBlock(CodeBlockKind::Fenced(_))
                | Tag::List(_)
                | Tag::Item
                | Tag::Emphasis
                | Tag::Strong
                | Tag::Strikethrough
        ),
        Event::End(_)
        | Event::Text(_)
        | Event::Code(_)
        | Event::SoftBreak
        | Event::HardBreak
        | Event::Rule
        | Event::TaskListMarker(_) => true,
        _ => false,
    })
}

impl NotionClient {
    /// Composition must explicitly grant replacement authority. Disabled by
    /// default; this does not grant upstream integration permissions or enable
    /// MCP tools, and does not change the separate create/append primitives.
    pub fn with_replacement_access(mut self, enabled: bool) -> Self {
        self.replacement_enabled = enabled;
        self
    }

    /// Replace one explicit UUID target, never child pages/databases. Verify
    /// fresh authoritative content after the mutation, not just its receipt.
    /// No automatic retry; post-mutation failure means reconciliation is needed.
    pub async fn replace_content(
        &self,
        request: ReplacePageContent,
    ) -> Result<PageContent, BackendError> {
        let operation = "notion.replace";
        if !self.replacement_enabled {
            return Err(failure(operation, BackendErrorKind::PermissionDenied));
        }
        let id = uuid(&request.page_id.0)
            .ok_or_else(|| failure(operation, BackendErrorKind::InvalidInput))?;
        if !supported(&request.markdown) || request.markdown.len() > 490 * 1024 {
            return Err(failure(operation, BackendErrorKind::UnsupportedContent));
        }
        let before = self.read_content(&id).await?;
        if before.page.archived || !supported(&before.markdown) {
            return Err(failure(
                "notion.replace.preflight",
                BackendErrorKind::UnsupportedContent,
            ));
        }
        let receipt = self.write(Method::PATCH, &format!("/pages/{id}/markdown"),
            json!({"type":"replace_content","replace_content":{"new_str":request.markdown,"allow_deleting_content":false},"allow_async":false}), operation).await?;
        if receipt["object"] != "page_markdown"
            || receipt["id"].as_str().and_then(uuid).as_deref() != Some(&id)
            || receipt["truncated"] != false
            || receipt["unknown_block_ids"] != json!([])
        {
            return Err(failure(
                "notion.replace.receipt",
                BackendErrorKind::Internal,
            ));
        }
        let after = self.read_content(&id).await.map_err(|mut error| {
            error.operation = "notion.replace.verify";
            error
        })?;
        if after.page.archived || after.markdown != request.markdown {
            return Err(failure("notion.replace.verify", BackendErrorKind::Conflict));
        }
        Ok(after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::backend::PageId;
    use serde_json::Value;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    fn request(markdown: &str) -> ReplacePageContent {
        ReplacePageContent {
            page_id: PageId(ID.into()),
            markdown: markdown.into(),
        }
    }
    fn metadata() -> String {
        json!({"object":"page","id":ID,"url":format!("https://www.notion.so/{ID}"),"archived":false,"last_edited_time":"2026-10-03T12:30:00Z","properties":{"Title":{"id":"title","type":"title","title":[{"plain_text":"Hello"}]}}}).to_string()
    }
    fn content(markdown: &str) -> String {
        json!({"object":"page_markdown","id":ID,"markdown":markdown,"truncated":false,"unknown_block_ids":[]}).to_string()
    }
    async fn mock(
        responses: Vec<(u16, String)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .without_retries()
            .with_replacement_access(true);
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in responses {
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
                            .find_map(|line| {
                                line.to_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(|value| value.parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (headers, end + 4, length);
                    }
                };
                while bytes.len() < start + length {
                    let mut buf = [0; 4096];
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let request = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[start..start + length]).unwrap()
                };
                requests.push((headers, request));
                stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (client, server)
    }
    #[tokio::test]
    async fn explicit_replacement_is_verified_by_a_fresh_authoritative_read() {
        let markdown = "# Heading\n\nEdited **text**\n";
        let (client, server) = mock(vec![
            (200, metadata()),
            (200, content("Old\n")),
            (200, content(markdown)),
            (200, metadata()),
            (200, content(markdown)),
        ])
        .await;
        assert_eq!(
            client
                .replace_content(request(markdown))
                .await
                .unwrap()
                .markdown,
            markdown
        );
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(
            requests[2]
                .0
                .starts_with(&format!("PATCH /v1/pages/{ID}/markdown HTTP/1.1"))
        );
        assert_eq!(
            requests[2].1,
            json!({"type":"replace_content","replace_content":{"new_str":markdown,"allow_deleting_content":false},"allow_async":false})
        );
        assert!(
            requests[4]
                .0
                .starts_with(&format!("GET /v1/pages/{ID}/markdown HTTP/1.1"))
        );
    }
    #[tokio::test]
    async fn disabled_access_invalid_targets_and_unsupported_input_make_no_requests() {
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .without_retries();
        client.api_root = "http://127.0.0.1:1".into();
        assert_eq!(
            client
                .replace_content(request("edited"))
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::PermissionDenied
        );
        let client = client.with_replacement_access(true);
        let mut invalid = request("edited");
        invalid.page_id.0 = "../pages".into();
        assert_eq!(
            client.replace_content(invalid).await.unwrap_err().kind,
            BackendErrorKind::InvalidInput
        );
        for markdown in [
            "<unknown/>",
            "\t<unknown/>",
            "\t![image](https://example.test/x)",
            "# Heading {#id .class}",
            "[[Wiki link]]",
            "<page url=\"https://notion.so/x\">child</page>",
            "![image](https://example.test/x)",
            "| A | B |\n|---|---|\n|1|2|",
            "Text[^1]\n\n[^1]: Footnote",
        ] {
            assert_eq!(
                client
                    .replace_content(request(markdown))
                    .await
                    .unwrap_err()
                    .kind,
                BackendErrorKind::UnsupportedContent
            );
        }
        assert!(supported("```html\n<unknown/>\n```\n"));
    }
    #[tokio::test]
    async fn existing_enhanced_content_is_never_destroyed_by_replacement() {
        let (client, server) = mock(vec![
            (200, metadata()),
            (200, content("\t<unknown alt=\"embed\"/>")),
        ])
        .await;
        assert_eq!(
            client
                .replace_content(request("edited"))
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::UnsupportedContent
        );
        assert_eq!(server.await.unwrap().len(), 2);
    }
    #[tokio::test]
    async fn post_write_mismatch_is_not_reported_as_success() {
        let (client, server) = mock(vec![
            (200, metadata()),
            (200, content("old")),
            (200, content("edited")),
            (200, metadata()),
            (200, content("different")),
        ])
        .await;
        let error = client.replace_content(request("edited")).await.unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::Conflict);
        assert_eq!(error.operation, "notion.replace.verify");
        server.await.unwrap();
    }
    #[tokio::test]
    async fn upstream_write_failures_have_sanitized_operation_context_and_no_retry() {
        for (status, kind) in [
            (400, BackendErrorKind::InvalidInput),
            (403, BackendErrorKind::PermissionDenied),
            (409, BackendErrorKind::Conflict),
            (429, BackendErrorKind::RateLimited),
            (503, BackendErrorKind::Unavailable),
        ] {
            let (client, server) = mock(vec![
                (200, metadata()),
                (200, content("old")),
                (status, "private test-credential".into()),
            ])
            .await;
            let error = client.replace_content(request("edited")).await.unwrap_err();
            assert_eq!(error.kind, kind);
            assert_eq!(error.operation, "notion.replace");
            assert!(!format!("{error:?}").contains("test-credential"));
            assert_eq!(server.await.unwrap().len(), 3);
        }
    }
}
