//! Production implementation of the provider-independent authoritative ports.
use crate::NotionClient;
use notion_knowledge_core::backend::{
    AppendPageContent, BackendFuture, CreatePage, NotionRead, NotionWrite, Page, PageContent,
    PageId, ReplacePageContent,
};

impl NotionRead for NotionClient {
    fn fetch_page<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(NotionClient::fetch_page(self, &page_id.0))
    }

    fn read_content<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(NotionClient::read_content(self, &page_id.0))
    }
}

impl NotionWrite for NotionClient {
    fn replace_content(&self, request: ReplacePageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move { Ok(NotionClient::replace_content(self, request).await?.page) })
    }

    fn append_content(&self, request: AppendPageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            let id = request.page_id.clone();
            NotionClient::append_content(self, request).await?;
            // The receipt alone is not complete authoritative metadata. A failed
            // read after a successful write must never replay the mutation.
            NotionClient::fetch_page(self, &id.0)
                .await
                .map_err(|mut error| {
                    error.operation = "notion.append.verify";
                    error.committed_page_id = Some(id.clone());
                    error
                })
        })
    }

    fn create_page(&self, request: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            let created = NotionClient::create_page(self, request).await?;
            NotionClient::fetch_page(self, &created.id.0)
                .await
                .map_err(|mut error| {
                    error.operation = "notion.create.verify";
                    error.committed_page_id = Some(created.id.clone());
                    error
                })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::backend::{BackendErrorKind, NotionBackend};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ID: &str = "12345678-1234-1234-1234-123456789abc";
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
    async fn authoritative_ports_integrate_authenticated_reads_and_distinct_writes() {
        let (client, server) = mock(vec![
            (200, metadata()), // exact metadata
            (200, metadata()),
            (200, content("Old\n")), // content
            (200, metadata()),
            (200, metadata()), // create receipt + fresh metadata
            (200, content("Appended\n")),
            (200, metadata()), // append
            (200, metadata()),
            (200, content("Old\n")), // replacement preflight
            (200, content("New\n")), // mutation
            (200, metadata()),
            (200, content("New\n")), // verification
        ])
        .await;
        let backend: &dyn NotionBackend = &client;
        let id = PageId(ID.into());
        assert_eq!(backend.fetch_page(&id).await.unwrap().title, "Hello");
        assert_eq!(backend.read_content(&id).await.unwrap().markdown, "Old\n");
        assert_eq!(
            backend
                .create_page(CreatePage {
                    parent_page_id: id.clone(),
                    title: "Created".into(),
                    markdown: "Initial\n".into(),
                })
                .await
                .unwrap()
                .id,
            id
        );
        assert_eq!(
            backend
                .append_content(AppendPageContent {
                    page_id: id.clone(),
                    markdown: "Appended\n".into(),
                })
                .await
                .unwrap()
                .id,
            id
        );
        assert_eq!(
            backend
                .replace_content(ReplacePageContent {
                    page_id: id.clone(),
                    markdown: "New\n".into(),
                })
                .await
                .unwrap()
                .id,
            id
        );
        let requests = server.await.unwrap();
        assert!(
            requests
                .iter()
                .all(|(h, _)| h.contains("authorization: Bearer test-credential"))
        );
        assert!(requests[3].0.starts_with("POST /v1/pages "));
        assert_eq!(requests[5].1["type"], "insert_content");
        assert_eq!(requests[9].1["type"], "replace_content");
        assert_eq!(client.request_metrics().attempts, 12);
    }

    #[tokio::test]
    async fn abstract_reads_retry_transient_failure_through_shared_transport() {
        let (mut client, server) = mock(vec![(503, "unavailable".into()), (200, metadata())]).await;
        client.transport = Default::default();
        let backend: &dyn NotionRead = &client;
        assert_eq!(
            backend.fetch_page(&PageId(ID.into())).await.unwrap().title,
            "Hello"
        );
        assert_eq!(client.request_metrics().retries, 1);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|(h, _)| h.starts_with("GET ")));
    }

    #[tokio::test]
    async fn post_creation_read_failure_reports_reconciliation_without_replaying_create() {
        let (client, server) = mock(vec![
            (200, metadata()),
            (403, "private upstream body".into()),
            (200, metadata()),
        ])
        .await;
        let backend: &dyn NotionBackend = &client;
        let error = backend
            .create_page(CreatePage {
                parent_page_id: PageId("abcdefab-1234-1234-1234-123456789abc".into()),
                title: "Created".into(),
                markdown: "Text".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::PermissionDenied);
        assert_eq!(error.operation, "notion.create.verify");
        assert_eq!(error.committed_page_id, Some(PageId(ID.into())));
        assert!(!error.to_string().contains("private"));
        let target = error.committed_page_id.unwrap();
        assert_eq!(backend.fetch_page(&target).await.unwrap().id, target);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(
            requests[0].1["parent"]["page_id"],
            "abcdefab-1234-1234-1234-123456789abc"
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(h, _)| h.starts_with("POST "))
                .count(),
            1
        );
        assert!(requests[1..].iter().all(|(h, _)| h.starts_with("GET ")));
    }

    #[tokio::test]
    async fn abstract_replacement_retains_explicit_authority_gate() {
        let (client, server) = mock(vec![]).await;
        let client = client.with_replacement_access(false);
        let backend: &dyn NotionWrite = &client;
        let error = backend
            .replace_content(ReplacePageContent {
                page_id: PageId(ID.into()),
                markdown: "Text".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::PermissionDenied);
        assert_eq!(error.committed_page_id, None);
        assert_eq!(client.request_metrics().attempts, 0);
        assert!(server.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn post_append_read_failure_preserves_acknowledged_target_without_replay() {
        let (client, server) = mock(vec![(200, content("Text")), (403, "private".into())]).await;
        let backend: &dyn NotionWrite = &client;
        let error = backend
            .append_content(AppendPageContent {
                page_id: PageId(ID.into()),
                markdown: "Text".into(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.operation, "notion.append.verify");
        assert_eq!(error.committed_page_id, Some(PageId(ID.into())));
        assert_eq!(error.kind, BackendErrorKind::PermissionDenied);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].0.starts_with("PATCH "));
        assert!(requests[1].0.starts_with("GET "));
    }
}
