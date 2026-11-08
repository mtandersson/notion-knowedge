//! Lossless authoritative enhanced-Markdown reads.
use crate::{
    NotionClient,
    pages::{PAGE_VERSION, page_id},
};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind, PageContent};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;

fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.read_content",
        retry_after: None,
        committed_page_id: None,
    }
}
#[derive(Deserialize)]
struct Markdown {
    object: String,
    id: String,
    markdown: String,
    truncated: bool,
    unknown_block_ids: Vec<String>,
}
impl NotionClient {
    /// Read exact metadata and complete enhanced Markdown without following links.
    /// Unsupported blocks remain visible as Notion's `<unknown .../>` markers.
    /// Incomplete content fails; no partial success or cache is returned.
    pub async fn read_content(&self, input: &str) -> Result<PageContent, BackendError> {
        let id = page_id(input)?;
        let page = self.fetch_page(&id.0).await?;
        let request = self
            .http
            .get(format!("{}/pages/{}/markdown", self.api_root, id.0))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", PAGE_VERSION);
        let mut response = self.send(request, true, "notion.read_content").await?;
        if response.status().as_u16() != 200 {
            return Err(error(match response.status().as_u16() {
                400 => BackendErrorKind::InvalidInput,
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                404 => BackendErrorKind::NotFound,
                409 => BackendErrorKind::Conflict,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            }));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(BackendErrorKind::Unavailable))?
        {
            if body.len() + chunk.len() > 8 * 1024 * 1024 {
                return Err(error(BackendErrorKind::Internal));
            }
            body.extend_from_slice(&chunk);
        }
        let content: Markdown =
            serde_json::from_slice(&body).map_err(|_| error(BackendErrorKind::Internal))?;
        if content.object != "page_markdown" || page_id(&content.id).ok().as_ref() != Some(&id) {
            return Err(error(BackendErrorKind::Internal));
        }
        if content.truncated
            || !content.unknown_block_ids.is_empty()
            || crate::reference_completeness::unverifiable(&content.markdown)
        {
            return Err(error(BackendErrorKind::UnsupportedContent));
        }
        // Do not trim/indent/rewrite: tabs encode children, spaces and fences encode code.
        Ok(PageContent {
            page,
            markdown: content.markdown,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::backend::{NotionRead, PageId};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
    async fn read(status: u16, body: String) -> (Result<PageContent, BackendError>, Vec<String>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let metadata = json!({"object":"page","id":ID,"url":format!("https://www.notion.so/{ID}"),"archived":false,"last_edited_time":"2026-10-03T12:30:00Z","properties":{"Title":{"id":"title","type":"title","title":[{"plain_text":"Hello"}]}}}).to_string();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (status, body) in [(200, metadata), (status, body)] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 2048];
                    let n = stream.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                requests.push(String::from_utf8(request).unwrap());
            }
            requests
        });
        // Fresh MCP reads use this production port; validation must apply here too.
        let result = NotionRead::read_content(&client, &PageId(ID.into())).await;
        (result, server.await.unwrap())
    }
    fn response(markdown: &str) -> Value {
        json!({"object":"page_markdown","id":ID,"markdown":markdown,"truncated":false,"unknown_block_ids":[]})
    }
    #[tokio::test]
    async fn nested_markdown_links_and_literal_code_are_lossless_and_deterministic() {
        let markdown = "# Heading\n\nParagraph [link](https://example.test/a?q=x#anchor).\n\n- Parent\n\t1. Child\n\t\tNested **text**\n\n```rust\n  let x = \"[literal](not-a-link)\";\n```\n";
        let (first, requests) = read(200, response(markdown).to_string()).await;
        let second = read(200, response(markdown).to_string()).await.0.unwrap();
        let first = first.unwrap();
        assert_eq!(first, second);
        assert_eq!(first.markdown, markdown);
        assert_eq!(first.page.title, "Hello");
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with(&format!("GET /v1/pages/{ID}/markdown HTTP/1.1")));
        let headers = requests[1].to_lowercase();
        assert!(headers.contains("authorization: bearer test-credential\r\n"));
        assert!(headers.contains(&format!("notion-version: {PAGE_VERSION}\r\n")));
    }
    #[tokio::test]
    async fn unsupported_blocks_remain_visible_and_empty_pages_are_valid() {
        for markdown in [
            "",
            "<unknown url=\"https://notion.so/block\" alt=\"embed\"/>\n",
        ] {
            assert_eq!(
                read(200, response(markdown).to_string())
                    .await
                    .0
                    .unwrap()
                    .markdown,
                markdown
            );
        }
    }
    #[tokio::test]
    async fn inline_synced_content_and_empty_originals_are_complete_without_following_references() {
        for markdown in [
            String::new(),
            format!("<synced_block url=\"https://notion.so/{ID}\">\n</synced_block>"),
            format!("<synced-block url=\"https://notion.so/{ID}\" />"),
            format!(
                "<synced_block_reference url=\"https://notion.so/{ID}\">\n\tKnown text.\n</synced_block_reference>"
            ),
            format!(
                "<synced-block-reference url=\"https://notion.so/{ID}\">\nKnown text.\n</synced-block-reference>"
            ),
            format!(
                "<synced_block_reference url=\"https://notion.so/{ID}\">\n```html\n<synced_block_reference />\n```\n</synced_block_reference>"
            ),
        ] {
            let (content, requests) = read(200, response(&markdown).to_string()).await;
            assert_eq!(content.unwrap().markdown, markdown);
            assert_eq!(
                requests.len(),
                2,
                "content validation must never fetch an original"
            );
        }
    }

    #[tokio::test]
    async fn unresolved_copied_content_never_returns_a_complete_page() {
        for (name, body) in [
            ("synced_block_reference", ""),
            ("synced_block_reference", "\n\t \n"),
            ("synced-block-reference", "\n\n"),
            ("synced_block_reference", "\n<!-- no rendered content -->\n"),
        ] {
            let markdown = format!(
                "Visible host text.\n<{name} url=\"https://notion.so/{ID}\">{body}</{name}>"
            );
            let (result, requests) = read(200, response(&markdown).to_string()).await;
            assert_eq!(
                result.unwrap_err().kind,
                BackendErrorKind::UnsupportedContent
            );
            assert_eq!(requests.len(), 2);
        }
        for markdown in [
            format!("<synced_block_reference url=\"https://notion.so/{ID}\"/>"),
            format!("<synced_block_reference url=\"https://notion.so/{ID}\">unfinished"),
            "</synced_block_reference>".into(),
            "<synced_block_reference>text</synced_block_reference>".into(),
            "<synced_block_reference url=\"https://example.test/outside\">text</synced_block_reference>".into(),
            format!("<synced_block_reference url=\"https://notion.so/{ID}\" notice=\"unavailable\">text</synced_block_reference>"),
            format!("<synced_block_reference url=\"https://notion.so/{ID}\">text</synced-block-reference>"),
        ] {
            assert_eq!(read(200, response(&markdown).to_string()).await.0.unwrap_err().kind, BackendErrorKind::UnsupportedContent);
        }
    }

    #[tokio::test]
    async fn copied_reference_examples_are_literal_in_code_comments_and_escapes() {
        let reference = format!(
            "<synced_block_reference url=\"https://notion.so/{ID}\"></synced_block_reference>"
        );
        for markdown in [
            format!("```html\n{reference}\n```"),
            format!("`{reference}`"),
            format!("\\{}", reference.replace("</", "\\</")),
            format!("<!--\n{reference}\n-->"),
            format!("<code>{reference}</code>"),
            format!("<CODE>{reference}</CODE>"),
            format!("<PRE>{reference}</PRE>"),
            format!("<SCRIPT>{reference}</SCRIPT>"),
            format!("<STYLE>{reference}</STYLE>"),
            format!("<callout>\n\t```html\n\t{reference}\n\t```\n</callout>"),
        ] {
            assert_eq!(
                read(200, response(&markdown).to_string())
                    .await
                    .0
                    .unwrap()
                    .markdown,
                markdown
            );
        }
    }

    #[tokio::test]
    async fn raw_literal_text_cannot_hide_following_unresolved_content() {
        let reference = format!(
            "<synced_block_reference url=\"https://notion.so/{ID}\"></synced_block_reference>"
        );
        for prefix in [
            r#"<script>const example = "<code>";</script>"#,
            r#"<style>example: "<pre>";</style>"#,
            r#"<style>example: "</☃☃</</";</style>"#,
            r#"<SCRIPT>const example = "<code data-x='";</SCRIPT >"#,
            r#"<STYLE>example: '<synced_block_reference url="unfinished';</STYLE>"#,
        ] {
            let markdown = format!("{prefix}\n{reference}");
            let (result, requests) = read(200, response(&markdown).to_string()).await;
            assert_eq!(
                result.unwrap_err().kind,
                BackendErrorKind::UnsupportedContent
            );
            assert_eq!(requests.len(), 2);
        }
    }

    #[tokio::test]
    async fn raw_html_fences_do_not_hide_following_copied_references() {
        let markdown = format!(
            "<script>\nconst template = `\n```\n`;\n</script>\n<synced_block_reference url=\"https://notion.so/{ID}\"></synced_block_reference>"
        );
        let example = format!("{markdown}\n");
        let split = example.find("<synced_block_reference").unwrap();
        let literal = format!(
            "{}\n```xml\n{}\n```\n",
            &example[..split],
            &example[split..]
        );
        assert!(read(200, response(&literal).to_string()).await.0.is_ok());
        let (result, _) = read(200, response(&markdown).to_string()).await;
        assert_eq!(
            result.unwrap_err().kind,
            BackendErrorKind::UnsupportedContent
        );
    }

    #[tokio::test]
    async fn synced_reference_validation_has_a_finite_page_budget() {
        let reference = format!(
            "<synced_block_reference url=\"https://notion.so/{ID}\">Known text.</synced_block_reference>\n"
        );
        assert!(
            read(200, response(&reference.repeat(128)).to_string())
                .await
                .0
                .is_ok()
        );
        assert_eq!(
            read(200, response(&reference.repeat(129)).to_string())
                .await
                .0
                .unwrap_err()
                .kind,
            BackendErrorKind::UnsupportedContent
        );
    }

    #[tokio::test]
    async fn incomplete_or_malformed_content_never_returns_partial_success() {
        let mut cases = vec![(Value::Null, BackendErrorKind::Internal)];
        let mut value = response("private partial content");
        value["truncated"] = json!(true);
        cases.push((value, BackendErrorKind::UnsupportedContent));
        let mut value = response("private partial content");
        value["unknown_block_ids"] = json!([ID]);
        cases.push((value, BackendErrorKind::UnsupportedContent));
        for key in ["object", "id", "markdown", "truncated", "unknown_block_ids"] {
            let mut value = response("private partial content");
            value.as_object_mut().unwrap().remove(key);
            cases.push((value, BackendErrorKind::Internal));
        }
        let mut value = response("private partial content");
        value["id"] = json!("aaaaaaaa-1234-1234-1234-123456789abc");
        cases.push((value, BackendErrorKind::Internal));
        for (value, kind) in cases {
            let failure = read(200, value.to_string()).await.0.unwrap_err();
            assert_eq!(failure.kind, kind);
            assert!(!format!("{failure:?} {failure}").contains("private partial content"));
        }
        assert_eq!(
            read(200, "x".repeat(8 * 1024 * 1024 + 1))
                .await
                .0
                .unwrap_err()
                .kind,
            BackendErrorKind::Internal
        );
    }
    #[tokio::test]
    async fn single_attempt_upstream_failures_are_sanitized() {
        for (status, kind) in [
            (401, BackendErrorKind::Unauthenticated),
            (403, BackendErrorKind::PermissionDenied),
            (404, BackendErrorKind::NotFound),
            (429, BackendErrorKind::RateLimited),
            (503, BackendErrorKind::Unavailable),
            (302, BackendErrorKind::Internal),
        ] {
            let (result, requests) = read(status, "private test-credential".into()).await;
            let failure = result.unwrap_err();
            assert_eq!(failure.kind, kind);
            assert_eq!(requests.len(), 2);
            assert!(!format!("{failure:?} {failure}").contains("test-credential"));
        }
    }
}
