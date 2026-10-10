//! Canonical, scope-verified single-page refresh preparation.
//! No index or embedding side effects. The final guarded commit must still
//! revalidate source revision, physical ancestry and owner generation.
use std::sync::Arc;

use crate::{NotionClient, links::extract_relationships};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageContent, PageId},
    chunking::{ChunkConfig, chunk_document},
    fingerprint::{content_hash, identify_chunks},
    indexed::{IndexedChunk, IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
    lifecycle::LifecycleScope,
    root_scope::RootScopeGate,
};

/// A prepared *proposal*, never an authorization token for later index commits.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedPageSnapshot {
    pub document: IndexedDocument,
    pub chunks: Vec<IndexedChunk>,
}

fn failure(kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation: "notion.prepare_scoped_page",
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}

fn valid_previous(
    selected: &PageId,
    root: &PageId,
    scope: &LifecycleScope,
    previous: &[IndexedChunk],
) -> bool {
    previous.len() <= 100_000
        && previous.iter().all(|chunk| {
            chunk.schema_version == SchemaVersion::V1
                && chunk.metadata.page_id == selected.0
                && chunk.metadata.source.workspace_id == scope.workspace_id
                && chunk.metadata.source.root_page_id == root.0
        })
}

impl NotionClient {
    /// Read exactly one chosen page. The root MUST be part of the trusted
    /// operator scope; neither caller-provided page IDs nor index provenance
    /// are sufficient authority to perform even the first content read.
    ///
    /// Complete source reads are bracketed with live physical ancestry and
    /// full metadata/content equality checks. Notion offers no atomic snapshot
    /// or CAS, so commit users MUST check again under #254's actual index guard.
    pub async fn prepare_scoped_page(
        &self,
        selected: &PageId,
        root: &PageId,
        scope: &LifecycleScope,
        previous: &[IndexedChunk],
        config: ChunkConfig,
    ) -> Result<ScopedPageSnapshot, BackendError> {
        if !scope.roots.contains(root)
            || config.target_chars == 0
            || config.overlap_chars >= config.target_chars
            || !valid_previous(selected, root, scope, previous)
        {
            return Err(failure(BackendErrorKind::InvalidInput));
        }
        let gate = RootScopeGate::new(Arc::new(self.clone()), scope.clone())
            .map_err(|_| failure(BackendErrorKind::InvalidInput))?;
        let permit = gate
            .authorize(selected)
            .await
            .map_err(|_| failure(BackendErrorKind::PermissionDenied))?;
        if !permit.belongs_to_any(std::slice::from_ref(&root.0)) {
            return Err(failure(BackendErrorKind::PermissionDenied));
        }

        let before = self.read_content(&selected.0).await?;
        if before.page.id != *selected || before.page.archived {
            return Err(failure(BackendErrorKind::Conflict));
        }
        let (database_id, data_source_id) = permit.physical_containers();
        let metadata = IndexedMetadata {
            page_id: selected.0.clone(),
            block_id: None,
            url: before.page.url.clone(),
            title: before.page.title.clone(),
            heading_path: Vec::new(),
            last_edited_time: before.page.last_edited_time.clone(),
            source: SourceMetadata {
                workspace_id: scope.workspace_id.clone(),
                root_page_id: root.0.clone(),
                database_id: database_id.map(str::to_owned),
                data_source_id: data_source_id.map(str::to_owned),
            },
            properties: before.page.properties.clone(),
        };
        let document = IndexedDocument {
            schema_version: SchemaVersion::V1,
            metadata,
            text: before.markdown.clone(),
            content_hash: content_hash(&before.markdown),
            links: extract_relationships(&before).links,
        };
        let drafts = chunk_document(&document, config)
            .map_err(|_| failure(BackendErrorKind::InvalidInput))?;
        let mut chunks =
            identify_chunks(drafts, previous).map_err(|_| failure(BackendErrorKind::Conflict))?;
        for chunk in &mut chunks {
            chunk.links = extract_relationships(&PageContent {
                page: before.page.clone(),
                markdown: chunk.text.clone(),
            })
            .links;
        }

        // Source may change between metadata and Markdown reads, or during
        // the local chunk preparation. Compare *all* authoritative fields,
        // not just timestamps, then physically revalidate original ancestry.
        let after = self.read_content(&selected.0).await?;
        if before != after {
            return Err(failure(BackendErrorKind::Conflict));
        }
        gate.revalidate(&permit, scope)
            .await
            .map_err(|_| failure(BackendErrorKind::Conflict))?;
        Ok(ScopedPageSnapshot { document, chunks })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::{discovery::ExclusionRules, indexed::PropertyValue};
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PAGE: &str = "11111111-1111-1111-1111-111111111111";
    const ROOT: &str = "22222222-2222-2222-2222-222222222222";
    const OTHER: &str = "33333333-3333-3333-3333-333333333333";
    const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
    const REVISION: &str = "2026-10-10T10:10:00Z";

    fn scope() -> LifecycleScope {
        LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        }
    }

    fn metadata(id: &str, parent: &str, title: &str, revision: &str) -> Value {
        json!({
            "object": "page",
            "id": id,
            "url": format!("https://www.notion.so/{id}"),
            "last_edited_time": revision,
            "in_trash": false,
            "parent": if parent == "workspace" {
                json!({"type":"workspace","workspace":true})
            } else {
                json!({"type":"page_id","page_id":parent})
            },
            "properties": {
                "Title": {
                    "id":"title",
                    "type":"title",
                    "title":[{"plain_text":title}]
                },
                "priority": {
                    "id":"priority",
                    "type":"number",
                    "number":4
                }
            }
        })
    }
    fn markdown(body: &str) -> Value {
        json!({
            "object":"page_markdown",
            "id":PAGE,
            "markdown":body,
            "truncated":false,
            "unknown_block_ids":[]
        })
    }
    type Route = (String, Value);
    fn chain(parent: &str) -> Vec<Route> {
        vec![
            (
                format!("pages/{PAGE}"),
                metadata(PAGE, parent, "Current", REVISION),
            ),
            (
                format!("pages/{parent}"),
                metadata(parent, "workspace", "Root", REVISION),
            ),
            (
                format!("pages/{parent}"),
                metadata(parent, "workspace", "Root", REVISION),
            ),
            (
                format!("pages/{PAGE}"),
                metadata(PAGE, parent, "Current", REVISION),
            ),
        ]
    }
    fn full(body: &str, title: &str) -> Vec<Route> {
        let mut routes = chain(ROOT);
        for _ in 0..2 {
            routes.push((
                format!("pages/{PAGE}"),
                metadata(PAGE, ROOT, title, REVISION),
            ));
            routes.push((format!("pages/{PAGE}/markdown"), markdown(body)));
        }
        routes.extend(chain(ROOT));
        routes
    }

    async fn fixture(routes: Vec<Route>) -> (NotionClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-credential")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (path, value) in routes {
                let (mut stream, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut received = Vec::new();
                while !received.windows(4).any(|v| v == b"\r\n\r\n") {
                    let mut bytes = [0; 2048];
                    let n = stream.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    received.extend_from_slice(&bytes[..n]);
                }
                let headers = String::from_utf8(received).unwrap();
                assert!(
                    headers.starts_with(&format!("GET /v1/{path} HTTP/1.1\r\n")),
                    "{headers}"
                );
                assert!(headers.contains("authorization: Bearer fixture-credential"));
                assert!(headers.contains("notion-version: 2026-03-11"));
                requests.push(path);
                let body = value.to_string();
                stream.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                ).as_bytes()).await.unwrap();
            }
            requests
        });
        (client, task)
    }

    async fn prepare(
        routes: Vec<Route>,
        previous: &[IndexedChunk],
    ) -> Result<(ScopedPageSnapshot, Vec<String>), BackendError> {
        let (client, server) = fixture(routes).await;
        let result = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                previous,
                ChunkConfig::default(),
            )
            .await?;
        Ok((result, server.await.unwrap()))
    }

    #[tokio::test]
    async fn selects_only_target_and_preserves_canonical_metadata_links_and_stable_ids() {
        let text = format!(
            "# Intro\n\nUnchanged [linked](https://www.notion.so/{OTHER}).\n\n## Detail\n\nA sentence."
        );
        let (first, requests) = prepare(full(&text, "Current"), &[]).await.unwrap();
        assert_eq!(requests.len(), 12);
        assert!(requests.iter().all(|path| path == &format!("pages/{PAGE}")
            || path == &format!("pages/{ROOT}")
            || path == &format!("pages/{PAGE}/markdown")));
        assert_eq!(first.document.text, text);
        assert_eq!(first.document.metadata.source.workspace_id, WORKSPACE);
        assert_eq!(first.document.metadata.source.root_page_id, ROOT);
        assert_eq!(
            first.document.metadata.properties.get("priority"),
            Some(&PropertyValue::Number(4.0))
        );
        assert!(!first.chunks.is_empty());
        assert!(first.document.links.iter().any(|link| {
            matches!(link, notion_knowledge_core::indexed::LinkTarget::Page { page_id } if page_id == OTHER)
        }));

        // Property-only metadata change must update citations but preserve
        // chunk IDs and content fingerprints, without fetching linked pages.
        let (second, _) = prepare(full(&text, "Renamed"), &first.chunks)
            .await
            .unwrap();
        assert_eq!(second.document.metadata.title, "Renamed");
        assert_eq!(second.chunks.len(), first.chunks.len());
        assert_eq!(
            second
                .chunks
                .iter()
                .map(|c| &c.chunk_id)
                .collect::<Vec<_>>(),
            first.chunks.iter().map(|c| &c.chunk_id).collect::<Vec<_>>()
        );
        assert_eq!(
            second
                .chunks
                .iter()
                .map(|c| &c.content_hash)
                .collect::<Vec<_>>(),
            first
                .chunks
                .iter()
                .map(|c| &c.content_hash)
                .collect::<Vec<_>>()
        );
        assert!(
            second
                .chunks
                .iter()
                .all(|chunk| chunk.metadata.title == "Renamed")
        );
    }

    #[tokio::test]
    async fn changed_section_keeps_unmodified_chunk_identity() {
        let a = "# Title\n\nStable paragraph.\n\n## Different\n\nOld content.\n";
        let b = "# Title\n\nStable paragraph.\n\n## Different\n\nNew content.\n";
        let (old, _) = prepare(full(a, "Current"), &[]).await.unwrap();
        let (new, _) = prepare(full(b, "Current"), &old.chunks).await.unwrap();
        assert!(
            old.chunks
                .iter()
                .any(|old_chunk| new.chunks.iter().any(|new_chunk| {
                    old_chunk.chunk_id == new_chunk.chunk_id && old_chunk.text == new_chunk.text
                }))
        );
        assert_ne!(old.document.content_hash, new.document.content_hash);
    }

    #[tokio::test]
    async fn out_of_scope_and_exclusions_deny_before_any_content_read() {
        let (client, server) = fixture(chain(OTHER)).await;
        let failure = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                &[],
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.kind, BackendErrorKind::PermissionDenied);
        assert_eq!(server.await.unwrap().len(), 4);

        let mut forbidden = scope();
        forbidden.exclusions.page_ids.insert(PAGE.into());
        let (client, server) = fixture(chain(ROOT)).await;
        let failure = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &forbidden,
                &[],
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.kind, BackendErrorKind::PermissionDenied);
        assert_eq!(server.await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn previous_foreign_or_corrupt_page_state_rejected_without_network() {
        let (baseline, _) = prepare(full("Simple text", "Current"), &[]).await.unwrap();
        let mut wrong = baseline.chunks;
        assert!(!wrong.is_empty());
        wrong[0].metadata.page_id = OTHER.into();
        let client = NotionClient::integration("test-credential")
            .unwrap()
            .without_retries();
        let err = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                &wrong,
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, BackendErrorKind::InvalidInput);
        wrong[0].metadata.page_id = PAGE.into();
        wrong[0].content_hash = "forged".into();
        let (client, server) =
            fixture(full("Simple text", "Current").into_iter().take(6).collect()).await;
        let err = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                &wrong,
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, BackendErrorKind::Conflict);
        assert_eq!(server.await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn metadata_or_markdown_race_is_conflict_not_snapshot() {
        let mut routes = full("# One\n", "Current");
        routes[7] = (format!("pages/{PAGE}/markdown"), markdown("# New\n"));
        let (client, server) = fixture(routes.into_iter().take(8).collect()).await;
        let failure = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                &[],
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.kind, BackendErrorKind::Conflict);
        assert_eq!(server.await.unwrap().len(), 8);
    }

    #[tokio::test]
    async fn changed_physical_ancestry_during_final_check_is_conflict() {
        let mut routes = full("Content\n", "Current");
        routes[8].1["last_edited_time"] = json!("2026-10-10T10:11:00Z");
        let (client, server) = fixture(routes).await;
        let failure = client
            .prepare_scoped_page(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &scope(),
                &[],
                ChunkConfig::default(),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.kind, BackendErrorKind::Conflict);
        assert_eq!(server.await.unwrap().len(), 12);
    }
}
