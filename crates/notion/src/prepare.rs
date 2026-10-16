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
    lifecycle::{LifecycleEvidence, LifecycleScope},
    root_scope::{RootScopeGate, ScopeError},
};

/// A prepared *proposal*, never an authorization token for later index commits.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedPageSnapshot {
    pub document: IndexedDocument,
    pub chunks: Vec<IndexedChunk>,
    pub evidence: LifecycleEvidence,
    /// Authoritative names from the same fully bracketed read as membership.
    pub relation_names: std::collections::BTreeMap<String, String>,
    pub(crate) source: PageContent,
}

fn failure(kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation: "notion.prepare_scoped_page",
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}

fn valid_previous(selected: &PageId, scope: &LifecycleScope, previous: &[IndexedChunk]) -> bool {
    previous.len() <= 100_000
        && previous.iter().all(|chunk| {
            chunk.schema_version == SchemaVersion::V1
                && chunk.metadata.page_id == selected.0
                && chunk.metadata.source.workspace_id == scope.workspace_id
                && scope
                    .roots
                    .iter()
                    .any(|root| root.0 == chunk.metadata.source.root_page_id)
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
            || !valid_previous(selected, scope, previous)
        {
            return Err(failure(BackendErrorKind::InvalidInput));
        }
        let gate = RootScopeGate::new(Arc::new(self.clone()), scope.clone())
            .map_err(|_| failure(BackendErrorKind::InvalidInput))?;
        let permit = gate
            .authorize(selected)
            .await
            .map_err(|error| match error {
                ScopeError::Denied => failure(BackendErrorKind::PermissionDenied),
                ScopeError::Unverifiable => failure(BackendErrorKind::Unavailable),
                ScopeError::Changed => failure(BackendErrorKind::Conflict),
                ScopeError::InvalidConfiguration => failure(BackendErrorKind::InvalidInput),
            })?;
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
        Ok(ScopedPageSnapshot {
            document,
            chunks,
            evidence: permit.lifecycle_evidence().clone(),
            relation_names: before.page.relation_names.clone(),
            source: before,
        })
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
        fixture_status(routes.into_iter().map(|(p, v)| (p, 200, v)).collect()).await
    }

    async fn fixture_status(
        routes: Vec<(String, u16, Value)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-credential")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (path, status, value) in routes {
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
                    "HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
            .with_fast_fixture_transport();
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
        for field in ["workspace", "root"] {
            let mut foreign = wrong.clone();
            if field == "workspace" {
                foreign[0].metadata.source.workspace_id = OTHER.into();
            } else {
                foreign[0].metadata.source.root_page_id = OTHER.into();
            }
            let err = client
                .prepare_scoped_page(
                    &PageId(PAGE.into()),
                    &PageId(ROOT.into()),
                    &scope(),
                    &foreign,
                    ChunkConfig::default(),
                )
                .await
                .unwrap_err();
            assert_eq!(err.kind, BackendErrorKind::InvalidInput);
        }
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
    fn relations(mut routes: Vec<Route>, name: &str, targets: &[&str], second: bool) -> Vec<Route> {
        for (path, value) in &mut routes {
            if path == &format!("pages/{PAGE}") {
                value["properties"][name] = json!({"id":"rel%3A1","type":"relation","relation":targets.iter().map(|id| json!({"id":id})).collect::<Vec<_>>(),"has_more":false});
                if second {
                    value["properties"]["Other relation"] = json!({"id":"rel2","type":"relation","relation":[{"id":OTHER}],"has_more":false});
                }
            }
        }
        routes
    }
    fn target_chain(parent: &str) -> Vec<Route> {
        vec![
            (
                format!("pages/{OTHER}"),
                metadata(OTHER, parent, "Target", REVISION),
            ),
            (
                format!("pages/{parent}"),
                metadata(parent, "workspace", "Ancestor", REVISION),
            ),
            (
                format!("pages/{parent}"),
                metadata(parent, "workspace", "Ancestor", REVISION),
            ),
            (
                format!("pages/{OTHER}"),
                metadata(OTHER, parent, "Target", REVISION),
            ),
        ]
    }
    fn source_read(name: &str, targets: &[&str]) -> Vec<Route> {
        relations(
            vec![
                (
                    format!("pages/{PAGE}"),
                    metadata(PAGE, ROOT, "Current", REVISION),
                ),
                (format!("pages/{PAGE}/markdown"), markdown("Body")),
            ],
            name,
            targets,
            false,
        )
    }
    fn validation(name: &str, targets: &[&str], target: Vec<Route>) -> Vec<Route> {
        let mut routes = chain(ROOT);
        routes.extend(source_read(name, targets));
        routes.extend(target);
        routes.extend(source_read(name, targets));
        routes.extend(chain(ROOT));
        routes
    }
    async fn proposal(
        adapter: &crate::relation_proposal::RelationProposalAdapter,
    ) -> Result<crate::relation_proposal::RelationProposal, BackendError> {
        adapter
            .prepare(
                &PageId(PAGE.into()),
                &PageId(ROOT.into()),
                &[],
                ChunkConfig::default(),
            )
            .await
    }
    #[tokio::test]
    async fn relation_proposals_keep_names_membership_duplicates_and_zero_edges_together() {
        use crate::relation_proposal::RelationProposalAdapter;
        use notion_knowledge_core::graph::GraphTarget;
        for (name, targets, second, expected) in [
            ("Related", vec![OTHER, OTHER], true, 2),
            ("Renamed", vec![OTHER], false, 1),
            ("Empty", vec![], false, 0),
        ] {
            let mut routes = relations(full("Body", "Current"), name, &targets, second);
            if !targets.is_empty() || second {
                routes.extend(target_chain(ROOT));
            }
            let (client, server) = fixture(routes).await;
            let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
            let result = proposal(&adapter).await.unwrap();
            assert_eq!(result.edges().len(), expected);
            // The standalone sidecar feeds the merged typed owned-commit API,
            // without introducing a production Notion -> retrieval dependency.
            let bound_scope = notion_knowledge_core::reconciliation::ReconciliationScope::new(
                vec![ROOT.into()],
                vec![],
                "policy",
                "generation-1",
            )
            .unwrap();
            let binding = notion_knowledge_retrieval::commit::CommitBinding::new(
                "chunks",
                WORKSPACE,
                &bound_scope,
                "generation-1",
                1,
            )
            .unwrap();
            let projection = notion_knowledge_retrieval::commit::RelationProjection::new(
                &binding,
                PAGE,
                false,
                result.edges().to_vec(),
            )
            .unwrap();
            assert_eq!(projection.edges().len(), result.edges().len());
            assert!(
                result
                    .edges()
                    .iter()
                    .all(|edge| projection.edges().contains(edge))
            );
            assert_eq!(result.snapshot().relation_names["rel%3A1"], name);
            for edge in result.edges() {
                assert_eq!(
                    edge.target(),
                    &GraphTarget::Page {
                        page_id: OTHER.into()
                    }
                );
                assert_eq!(
                    edge.provenance(),
                    format!("property:{}", &edge.relation_type()[9..])
                );
            }
            if expected > 0 {
                assert_eq!(result.edges()[0].display_name(), Some(name));
            }
            let requests = server.await.unwrap();
            assert!(
                requests
                    .iter()
                    .all(|p| !p.ends_with("/markdown") || p == &format!("pages/{PAGE}/markdown"))
            );
        }
        // Removed properties produce a complete empty replacement too.
        let (client, server) = fixture(full("Body", "Current")).await;
        let result = proposal(&RelationProposalAdapter::new(client, scope()).unwrap())
            .await
            .unwrap();
        assert!(result.edges().is_empty());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn relation_labels_never_authorize_outside_excluded_broken_or_inaccessible_targets() {
        use crate::relation_proposal::RelationProposalAdapter;
        use notion_knowledge_core::graph::GraphTarget;
        for variant in 0..5 {
            let mut config = scope();
            let mut routes: Vec<_> =
                relations(full("Body", "Current"), "Authorized root", &[OTHER], false)
                    .into_iter()
                    .map(|(p, v)| (p, 200, v))
                    .collect();
            if variant < 2 {
                let ancestor = if variant == 0 { WORKSPACE } else { ROOT };
                if variant == 1 {
                    config.exclusions.page_ids.insert(OTHER.into());
                }
                routes.extend(target_chain(ancestor).into_iter().map(|(p, v)| (p, 200, v)));
            } else {
                routes.push((
                    format!("pages/{OTHER}"),
                    if variant == 2 {
                        404
                    } else if variant == 3 {
                        403
                    } else {
                        200
                    },
                    json!({"private":"never expose"}),
                ));
            }
            let (client, server) = fixture_status(routes).await;
            let result = proposal(&RelationProposalAdapter::new(client, config).unwrap())
                .await
                .unwrap();
            assert_eq!(
                result.edges()[0].target(),
                &GraphTarget::Unresolved {
                    reference: OTHER.into()
                }
            );
            assert!(result.targets()[OTHER].is_err());
            server.await.unwrap(); // Exact routes exclude target content/discovery.
        }
    }
    #[tokio::test]
    async fn owned_relation_revalidation_rejects_source_and_target_changes_without_timestamp_help()
    {
        use crate::relation_proposal::RelationProposalAdapter;
        for variant in 0..4 {
            let mut routes = relations(full("Body", "Current"), "Related", &[OTHER], false);
            routes.extend(target_chain(ROOT));
            if variant < 2 {
                routes.extend(chain(ROOT));
                routes.extend(source_read(
                    if variant == 0 { "Renamed" } else { "Related" },
                    if variant == 0 { &[OTHER] } else { &[] },
                ));
            } else {
                let mut target = target_chain(if variant == 2 { WORKSPACE } else { ROOT });
                if variant == 3 {
                    for (_, v) in &mut target {
                        v["last_edited_time"] = json!("2026-10-10T11:11:00Z");
                    }
                }
                // Failure is detected immediately after the target pass.
                routes.extend(chain(ROOT));
                routes.extend(source_read("Related", &[OTHER]));
                routes.extend(target);
            }
            let (client, server) = fixture(routes).await;
            let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
            let result = proposal(&adapter).await.unwrap();
            assert!(adapter.revalidate(&result).await.is_err());
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn current_policy_owner_fences_old_proposals_and_waits_for_acknowledgment_guard() {
        use crate::relation_proposal::RelationProposalAdapter;
        for variant in 0..4 {
            let (client, server) = fixture(full("Body", "Current")).await;
            let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
            let result = proposal(&adapter).await.unwrap();
            let mut changed = scope();
            changed.generation += 1;
            match variant {
                0 => changed.workspace_id = OTHER.into(),
                1 => changed.roots = vec![PageId(OTHER.into())],
                2 => {
                    changed.exclusions.page_ids.insert(OTHER.into());
                }
                _ => {}
            }
            adapter.replace_policy(changed).await.unwrap();
            assert!(adapter.revalidate(&result).await.is_err());
            assert!(adapter.replace_policy(scope()).await.is_err()); // No ABA policy reuse.
            server.await.unwrap();
        }
        let mut routes = relations(full("Body", "Current"), "Related", &[OTHER], false);
        routes.extend(target_chain(ROOT));
        routes.extend(validation("Related", &[OTHER], target_chain(ROOT)));
        let (client, server) = fixture(routes).await;
        let adapter = RelationProposalAdapter::new(client.clone(), scope()).unwrap();
        let result = proposal(&adapter).await.unwrap();
        let other_owner = RelationProposalAdapter::new(client, scope()).unwrap();
        assert!(other_owner.revalidate(&result).await.is_err());
        let authority = adapter.revalidate(&result).await.unwrap();
        let mut next = scope();
        next.generation += 1;
        let update = adapter.replace_policy(next);
        tokio::pin!(update);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut update)
                .await
                .is_err()
        );
        drop(authority);
        update.await.unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn relation_preparation_rejects_policy_replacement_during_authoritative_reads() {
        use crate::relation_proposal::RelationProposalAdapter;
        let (client, server) = fixture(full("Body", "Current")).await;
        let observer = client.clone();
        let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
        let worker = adapter.clone();
        let preparing = tokio::spawn(async move { proposal(&worker).await });
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while observer.request_metrics().attempts == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut next = scope();
        next.generation += 1;
        adapter.replace_policy(next).await.unwrap();
        assert!(preparing.await.unwrap().is_err());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn relation_commit_rejects_source_physical_move_after_preparation() {
        use crate::relation_proposal::RelationProposalAdapter;
        let mut routes = full("Body", "Current");
        routes.extend(chain(OTHER));
        let (client, server) = fixture(routes).await;
        let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
        let result = proposal(&adapter).await.unwrap();
        assert!(adapter.revalidate(&result).await.is_err());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn denied_relation_target_changes_require_fresh_preparation() {
        use crate::relation_proposal::RelationProposalAdapter;
        const OUTSIDE: &str = "55555555-5555-5555-5555-555555555555";
        for excluded in [false, true] {
            let mut config = scope();
            if excluded {
                config.exclusions.page_ids.insert(OTHER.into());
            }
            let initial_parent = if excluded { ROOT } else { WORKSPACE };
            let mut routes = relations(full("Body", "Current"), "Related", &[OTHER], false);
            routes.extend(target_chain(initial_parent));
            routes.extend(chain(ROOT));
            routes.extend(source_read("Related", &[OTHER]));
            routes.extend(target_chain(OUTSIDE));
            let (client, server) = fixture(routes).await;
            let adapter = RelationProposalAdapter::new(client, config).unwrap();
            let result = proposal(&adapter).await.unwrap();
            assert!(result.targets()[OTHER].is_err());
            assert_eq!(
                adapter.revalidate(&result).await.err().unwrap().kind,
                BackendErrorKind::Conflict
            );
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn unresolved_relation_failure_class_changes_require_fresh_preparation() {
        use crate::relation_proposal::RelationProposalAdapter;
        let mut routes: Vec<_> = relations(full("Body", "Current"), "Related", &[OTHER], false)
            .into_iter()
            .map(|(p, v)| (p, 200, v))
            .collect();
        routes.push((
            format!("pages/{OTHER}"),
            403,
            json!({"private":"unavailable"}),
        ));
        routes.extend(chain(ROOT).into_iter().map(|(p, v)| (p, 200, v)));
        routes.extend(
            source_read("Related", &[OTHER])
                .into_iter()
                .map(|(p, v)| (p, 200, v)),
        );
        routes.push((format!("pages/{OTHER}"), 404, json!({"private":"missing"})));
        let (client, server) = fixture_status(routes).await;
        let adapter = RelationProposalAdapter::new(client, scope()).unwrap();
        let result = proposal(&adapter).await.unwrap();
        assert!(result.targets()[OTHER].is_err());
        assert_eq!(
            adapter.revalidate(&result).await.err().unwrap().kind,
            BackendErrorKind::Conflict
        );
        server.await.unwrap();
    }
}
