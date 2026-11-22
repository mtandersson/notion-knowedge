//! Canonical single-page preparation from content already read. No network,
//! index or embedding side effects. Scope comes from the walk that reached the
//! page (or the caller's lifecycle read); it is not re-checked here or at commit.
use crate::{NotionClient, links::extract_relationships, media_identity::canonical_content};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageContent, PageId},
    chunking::{ChunkConfig, ChunkDraft, chunk_document},
    fingerprint::{content_hash, identify_chunks},
    indexed::{IndexedChunk, IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
    inventory::InventoryParent,
    lifecycle::LifecycleScope,
};
use std::collections::BTreeMap;

use crate::walk::WalkedPage;

/// A prepared *proposal*, never an authorization token for later index commits.
#[derive(Debug, Clone, PartialEq)]
pub struct ScopedPageSnapshot {
    pub document: IndexedDocument,
    pub chunks: Vec<IndexedChunk>,
    /// Where the walk (or the caller's lifecycle read) placed the page.
    pub parent: InventoryParent,
    /// Relation property names from the same metadata read as the content.
    pub relation_names: std::collections::BTreeMap<String, String>,
    pub(crate) source: PageContent,
}

/// Fixed allowance for per-value metadata/allocation overhead in estimates.
const RETAINED_OVERHEAD: usize = 1024;

fn metadata_retained_bytes(metadata: &notion_knowledge_core::indexed::IndexedMetadata) -> usize {
    serde_json::to_vec(metadata).map_or(RETAINED_OVERHEAD * 16, |v| v.len())
}

/// Conservative heap-retention estimate for one indexed chunk.
pub fn chunk_retained_bytes(chunk: &IndexedChunk) -> usize {
    RETAINED_OVERHEAD
        + chunk.chunk_id.len()
        + chunk.text.len()
        + chunk.content_hash.len()
        + chunk.links.len() * 256
        + metadata_retained_bytes(&chunk.metadata)
}

impl ScopedPageSnapshot {
    /// Conservative heap-retention estimate used to bound prefetched pages.
    pub fn retained_bytes(&self) -> usize {
        RETAINED_OVERHEAD
            + self.document.text.len()
            + self.document.links.len() * 256
            + metadata_retained_bytes(&self.document.metadata)
            + self.source.markdown.len()
            + self.chunks.iter().map(chunk_retained_bytes).sum::<usize>()
            + self
                .relation_names
                .iter()
                .map(|(k, v)| k.len() + v.len() + 64)
                .sum::<usize>()
    }
    /// The content this snapshot was prepared from.
    pub fn source(&self) -> &PageContent {
        &self.source
    }

    /// Finalize a partition of individual draft inputs before any embedding
    /// or commit. Regenerate the drafts only for a page requiring extra cuts,
    /// so every piece retains its exact source range and citation metadata.
    pub fn split_chunks(
        &mut self,
        cuts: BTreeMap<usize, Vec<usize>>,
        previous: &[IndexedChunk],
        config: ChunkConfig,
    ) -> Result<(), BackendError> {
        if cuts.is_empty() {
            return Ok(());
        }
        let drafts = chunk_document(&self.document, config)
            .map_err(|_| failure(BackendErrorKind::InvalidInput))?;
        let mut split = Vec::with_capacity(drafts.len());
        for (index, draft) in drafts.into_iter().enumerate() {
            let Some(points) = cuts.get(&index) else {
                split.push(draft);
                continue;
            };
            let mut start = 0;
            for end in points
                .iter()
                .copied()
                .chain(std::iter::once(draft.text.len()))
            {
                let text = draft
                    .text
                    .get(start..end)
                    .filter(|text| !text.trim().is_empty())
                    .ok_or_else(|| failure(BackendErrorKind::InvalidInput))?;
                split.push(ChunkDraft {
                    metadata: draft.metadata.clone(),
                    text: text.to_owned(),
                    source_bytes: draft.source_bytes.start + start..draft.source_bytes.start + end,
                });
                start = end;
            }
        }
        self.chunks = identify_content_chunks(split, previous, &self.source)?;
        Ok(())
    }
}

fn failure(kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation: "notion.prepare_page",
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

/// Chunk one page's content. Pure: the caller supplies content it already
/// read once, and the parent and root the walk placed it under.
pub(crate) fn prepare_content(
    content: PageContent,
    parent: InventoryParent,
    root: &PageId,
    scope: &LifecycleScope,
    previous: &[IndexedChunk],
    config: ChunkConfig,
) -> Result<ScopedPageSnapshot, BackendError> {
    let selected = content.page.id.clone();
    if !scope.roots.contains(root)
        || config.target_chars == 0
        || config.overlap_chars >= config.target_chars
        || !valid_previous(&selected, scope, previous)
    {
        return Err(failure(BackendErrorKind::InvalidInput));
    }
    let source = canonical_content(content);
    if source.page.archived {
        return Err(failure(BackendErrorKind::Conflict));
    }
    let (database_id, data_source_id) = match &parent {
        InventoryParent::DataSource {
            data_source_id,
            database_id,
            ..
        } => (Some(database_id.clone()), Some(data_source_id.clone())),
        _ => (None, None),
    };
    let metadata = IndexedMetadata {
        page_id: selected.0.clone(),
        block_id: None,
        url: source.page.url.clone(),
        title: source.page.title.clone(),
        heading_path: Vec::new(),
        last_edited_time: source.page.last_edited_time.clone(),
        source: SourceMetadata {
            workspace_id: scope.workspace_id.clone(),
            root_page_id: root.0.clone(),
            database_id,
            data_source_id,
        },
        properties: source.page.properties.clone(),
    };
    let document = IndexedDocument {
        schema_version: SchemaVersion::V1,
        metadata,
        text: source.markdown.clone(),
        content_hash: content_hash(&source.markdown),
        links: extract_relationships(&source).links,
    };
    let drafts =
        chunk_document(&document, config).map_err(|_| failure(BackendErrorKind::InvalidInput))?;
    let chunks = identify_content_chunks(drafts, previous, &source)?;
    Ok(ScopedPageSnapshot {
        document,
        chunks,
        parent,
        relation_names: source.page.relation_names.clone(),
        source,
    })
}

fn identify_content_chunks(
    drafts: Vec<ChunkDraft>,
    previous: &[IndexedChunk],
    source: &PageContent,
) -> Result<Vec<IndexedChunk>, BackendError> {
    let mut chunks =
        identify_chunks(drafts, previous).map_err(|_| failure(BackendErrorKind::Conflict))?;
    for chunk in &mut chunks {
        chunk.links = extract_relationships(&PageContent {
            page: source.page.clone(),
            markdown: chunk.text.clone(),
        })
        .links;
    }
    Ok(chunks)
}

impl NotionClient {
    /// Prepare a page from the walk's single read. Makes no requests: the
    /// walk already read its metadata and Markdown and decided its scope.
    pub fn prepare_walked_page(
        &self,
        walked: &WalkedPage,
        scope: &LifecycleScope,
        previous: &[IndexedChunk],
        config: ChunkConfig,
    ) -> Result<ScopedPageSnapshot, BackendError> {
        let content = walked
            .content
            .clone()
            .filter(|_| walked.skip.is_none())
            .ok_or_else(|| failure(BackendErrorKind::UnsupportedContent))?;
        if content.page.id.0 != walked.entry.page_id {
            return Err(failure(BackendErrorKind::InvalidInput));
        }
        prepare_content(
            content,
            walked.entry.parent.clone(),
            &PageId(walked.entry.root_id.clone()),
            scope,
            previous,
            config,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::{
        discovery::ExclusionRules, indexed::PropertyValue, inventory::InventoryEntry,
    };
    use serde_json::{Value, json};

    const PAGE: &str = "11111111-1111-1111-1111-111111111111";
    const ROOT: &str = "22222222-2222-2222-2222-222222222222";
    const OTHER: &str = "33333333-3333-3333-3333-333333333333";
    const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
    const REVISION: &str = "2026-10-10T10:10:00Z";
    /// A target whose inventory read fails.
    const UNREADABLE: &str = "66666666-6666-6666-6666-666666666666";

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

    /// The page as the walk delivers it: one metadata and one Markdown read.
    fn walked_with(value: &Value, body: &str, parent: InventoryParent) -> WalkedPage {
        WalkedPage {
            entry: InventoryEntry {
                page_id: PAGE.into(),
                parent,
                root_id: ROOT.into(),
                last_edited_time: value["last_edited_time"].as_str().unwrap().into(),
                in_trash: false,
            },
            content: Some(PageContent {
                page: crate::pages::metadata(value, &PageId(PAGE.into())).unwrap(),
                markdown: body.into(),
            }),
            skip: None,
            children: Default::default(),
            timings: Default::default(),
        }
    }
    fn walked(body: &str, title: &str) -> WalkedPage {
        walked_with(
            &metadata(PAGE, ROOT, title, REVISION),
            body,
            InventoryParent::Page(ROOT.into()),
        )
    }
    /// A client whose every request would fail: preparation must not make one.
    fn offline() -> NotionClient {
        let mut client = NotionClient::integration("fixture-credential")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = "http://127.0.0.1:9/v1".into();
        client
    }
    fn prepare(walked: &WalkedPage, previous: &[IndexedChunk]) -> ScopedPageSnapshot {
        offline()
            .prepare_walked_page(walked, &scope(), previous, ChunkConfig::default())
            .unwrap()
    }

    #[test]
    fn prepare_walked_page_makes_no_requests() {
        let client = offline();
        let page = walked("# Intro\n\nBody.\n", "Current");
        let snapshot = client
            .prepare_walked_page(&page, &scope(), &[], ChunkConfig::default())
            .unwrap();
        assert_eq!(client.request_metrics().attempts, 0);
        assert_eq!(
            snapshot.document.metadata.last_edited_time,
            page.entry.last_edited_time
        );
        assert_eq!(snapshot.parent, page.entry.parent);
    }

    #[test]
    fn selects_only_target_and_preserves_canonical_metadata_links_and_stable_ids() {
        let text = format!(
            "# Intro\n\nUnchanged [linked](https://www.notion.so/{OTHER}).\n\n## Detail\n\nA sentence."
        );
        let first = prepare(&walked(&text, "Current"), &[]);
        assert_eq!(first.document.text, text);
        assert_eq!(first.document.metadata.source.workspace_id, WORKSPACE);
        assert_eq!(first.document.metadata.source.root_page_id, ROOT);
        assert_eq!(first.document.metadata.source.database_id, None);
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
        let second = prepare(&walked(&text, "Renamed"), &first.chunks);
        assert_eq!(second.document.metadata.title, "Renamed");
        assert_eq!(
            second
                .chunks
                .iter()
                .map(|c| (&c.chunk_id, &c.content_hash))
                .collect::<Vec<_>>(),
            first
                .chunks
                .iter()
                .map(|c| (&c.chunk_id, &c.content_hash))
                .collect::<Vec<_>>()
        );
        assert!(
            second
                .chunks
                .iter()
                .all(|chunk| chunk.metadata.title == "Renamed")
        );
    }

    #[test]
    fn hosted_media_rotation_keeps_snapshot_identity() {
        let first = prepare(
            &walked(&crate::media_identity::tests::body("1"), "Current"),
            &[],
        );
        let second = prepare(
            &walked(&crate::media_identity::tests::body("2"), "Current"),
            &first.chunks,
        );
        assert_eq!(first.document, second.document);
        assert_eq!(first.chunks, second.chunks);
    }

    #[test]
    fn changed_section_keeps_unmodified_chunk_identity() {
        let a = "# Title\n\nStable paragraph.\n\n## Different\n\nOld content.\n";
        let b = "# Title\n\nStable paragraph.\n\n## Different\n\nNew content.\n";
        let old = prepare(&walked(a, "Current"), &[]);
        let new = prepare(&walked(b, "Current"), &old.chunks);
        assert!(
            old.chunks
                .iter()
                .any(|old_chunk| new.chunks.iter().any(|new_chunk| {
                    old_chunk.chunk_id == new_chunk.chunk_id && old_chunk.text == new_chunk.text
                }))
        );
        assert_ne!(old.document.content_hash, new.document.content_hash);
    }

    #[test]
    fn row_page_metadata_carries_database_and_data_source_ids() {
        let mut value = metadata(PAGE, ROOT, "Row", REVISION);
        value["parent"] = json!({"type":"data_source_id","data_source_id":OTHER});
        let parent = InventoryParent::DataSource {
            data_source_id: OTHER.into(),
            database_id: WORKSPACE.into(),
            owner_page_id: ROOT.into(),
        };
        let snapshot = prepare(&walked_with(&value, "Row body", parent.clone()), &[]);
        assert_eq!(
            snapshot.document.metadata.source.database_id.as_deref(),
            Some(WORKSPACE)
        );
        assert_eq!(
            snapshot.document.metadata.source.data_source_id.as_deref(),
            Some(OTHER)
        );
        assert!(
            snapshot
                .chunks
                .iter()
                .all(|c| c.metadata.source == snapshot.document.metadata.source)
        );
        assert_eq!(snapshot.parent, parent);
    }

    #[test]
    fn skipped_incomplete_or_foreign_walked_pages_are_not_prepared() {
        let client = offline();
        let mut skipped = walked("Body", "Current");
        skipped.skip = Some(notion_knowledge_core::discovery::SkipReason::Archived);
        let mut incomplete = walked("Body", "Current");
        incomplete.content = None;
        let mut foreign = walked("Body", "Current");
        foreign.entry.root_id = OTHER.into();
        for (page, kind) in [
            (skipped, BackendErrorKind::UnsupportedContent),
            (incomplete, BackendErrorKind::UnsupportedContent),
            (foreign, BackendErrorKind::InvalidInput),
        ] {
            let error = client
                .prepare_walked_page(&page, &scope(), &[], ChunkConfig::default())
                .unwrap_err();
            assert_eq!(error.kind, kind);
        }
    }

    #[test]
    fn previous_foreign_or_corrupt_page_state_rejected_without_network() {
        let page = walked("Simple text", "Current");
        let baseline = prepare(&page, &[]);
        let client = offline();
        let mut wrong = baseline.chunks;
        assert!(!wrong.is_empty());
        wrong[0].metadata.page_id = OTHER.into();
        let prepare = |previous: &[IndexedChunk]| {
            client
                .prepare_walked_page(&page, &scope(), previous, ChunkConfig::default())
                .unwrap_err()
                .kind
        };
        assert_eq!(prepare(&wrong), BackendErrorKind::InvalidInput);
        wrong[0].metadata.page_id = PAGE.into();
        for field in ["workspace", "root"] {
            let mut foreign = wrong.clone();
            if field == "workspace" {
                foreign[0].metadata.source.workspace_id = OTHER.into();
            } else {
                foreign[0].metadata.source.root_page_id = OTHER.into();
            }
            assert_eq!(prepare(&foreign), BackendErrorKind::InvalidInput);
        }
        wrong[0].content_hash = "forged".into();
        assert_eq!(prepare(&wrong), BackendErrorKind::Conflict);
        assert_eq!(client.request_metrics().attempts, 0);
    }

    fn with_relations(name: &str, targets: &[&str], second: bool) -> WalkedPage {
        let mut value = metadata(PAGE, ROOT, "Current", REVISION);
        value["properties"][name] = json!({"id":"rel%3A1","type":"relation","relation":targets.iter().map(|id| json!({"id":id})).collect::<Vec<_>>(),"has_more":false});
        if second {
            value["properties"]["Other relation"] =
                json!({"id":"rel2","type":"relation","relation":[{"id":OTHER}],"has_more":false});
        }
        walked_with(&value, "Body", InventoryParent::Page(ROOT.into()))
    }
    /// The last walk's placement of pages: each id with its ancestors, nearest
    /// first and ending at its root.
    struct Placement(
        std::collections::BTreeMap<String, notion_knowledge_core::inventory::PlacedChain>,
    );
    impl notion_knowledge_core::inventory::InventoryPlacement for Placement {
        fn placed_chain(
            &self,
            page_id: &str,
        ) -> Result<
            Option<notion_knowledge_core::inventory::PlacedChain>,
            notion_knowledge_core::sync_state::SyncStateError,
        > {
            match page_id {
                UNREADABLE => Err(notion_knowledge_core::sync_state::SyncStateError::CorruptState),
                _ => Ok(self.0.get(page_id).cloned()),
            }
        }
    }
    /// `OTHER` placed as a database row under the root.
    fn placed_row() -> std::sync::Arc<Placement> {
        std::sync::Arc::new(Placement(std::collections::BTreeMap::from([(
            OTHER.to_owned(),
            notion_knowledge_core::inventory::PlacedChain {
                ancestors: vec![ROOT.into()],
                data_sources: vec![("source".into(), "database".into())],
            },
        )])))
    }
    fn placed(entries: &[(&str, &[&str])]) -> std::sync::Arc<Placement> {
        std::sync::Arc::new(Placement(
            entries
                .iter()
                .map(|(id, chain)| {
                    (
                        (*id).to_owned(),
                        notion_knowledge_core::inventory::PlacedChain {
                            ancestors: chain.iter().map(|s| (*s).to_owned()).collect(),
                            data_sources: vec![],
                        },
                    )
                })
                .collect(),
        ))
    }
    async fn proposal(
        adapter: &crate::relation_proposal::RelationProposalAdapter,
        page: &WalkedPage,
    ) -> Result<crate::relation_proposal::RelationProposal, BackendError> {
        adapter
            .prepare_walked(page, &[], ChunkConfig::default())
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
            let client = offline();
            let adapter = RelationProposalAdapter::new(client.clone(), scope())
                .unwrap()
                .with_inventory(placed(&[(OTHER, &[ROOT])]));
            let result = proposal(&adapter, &with_relations(name, &targets, second))
                .await
                .unwrap();
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
            // Targets resolve from the inventory; nothing is read again.
            assert_eq!(client.request_metrics().attempts, 0);
        }
    }

    #[tokio::test]
    async fn unresolved_projection_keeps_references_without_any_request() {
        use crate::relation_proposal::{ProjectionMode, RelationProposalAdapter};
        use notion_knowledge_core::graph::GraphTarget;
        for targets in [vec![], vec![OTHER, OTHER]] {
            let client = offline();
            let adapter = RelationProposalAdapter::new(client.clone(), scope())
                .unwrap()
                .with_projection_mode(ProjectionMode::UnresolvedTargets);
            let prepared = proposal(&adapter, &with_relations("Related", &targets, false))
                .await
                .unwrap();
            assert!(prepared.targets().is_empty()); // No fabricated denied observations.
            assert!(prepared.edges().iter().all(|e| matches!(e.target(), GraphTarget::Unresolved { reference } if reference == OTHER)));
            let other_mode = adapter
                .clone()
                .with_projection_mode(ProjectionMode::ResolveTargets);
            assert!(other_mode.revalidate(&prepared).await.is_err()); // Even empty proposals are mode-bound.
            drop(adapter.revalidate(&prepared).await.unwrap());
            assert_eq!(client.request_metrics().attempts, 0);
        }
    }

    #[tokio::test]
    async fn targets_absent_outside_or_excluded_in_the_inventory_stay_unresolved() {
        use crate::relation_proposal::RelationProposalAdapter;
        use notion_knowledge_core::graph::GraphTarget;
        const EXCLUDED: &str = "55555555-5555-5555-5555-555555555555";
        let cases: [(&str, Option<std::sync::Arc<Placement>>); 8] = [
            ("row of excluded data source", Some(placed_row())),
            ("row of excluded database", Some(placed_row())),
            ("no inventory", None),
            ("absent from inventory", Some(placed(&[]))),
            ("under another root", Some(placed(&[(OTHER, &[WORKSPACE])]))),
            ("excluded page", Some(placed(&[(OTHER, &[ROOT])]))),
            (
                "under excluded descendant",
                Some(placed(&[(OTHER, &[EXCLUDED, ROOT])])),
            ),
            (
                "under excluded page",
                Some(placed(&[(OTHER, &[EXCLUDED, ROOT])])),
            ),
        ];
        for (case, inventory) in cases {
            let mut config = scope();
            match case {
                "excluded page" => {
                    config.exclusions.page_ids.insert(OTHER.into());
                }
                "under excluded descendant" => {
                    config.exclusions.descendants_of.insert(EXCLUDED.into());
                }
                "under excluded page" => {
                    config.exclusions.page_ids.insert(EXCLUDED.into());
                }
                "row of excluded data source" => {
                    config
                        .exclusions
                        .source_types
                        .insert(notion_knowledge_core::discovery::SourceType::DataSource);
                }
                "row of excluded database" => {
                    config
                        .exclusions
                        .source_types
                        .insert(notion_knowledge_core::discovery::SourceType::Database);
                }
                _ => {}
            }
            let client = offline();
            let mut adapter = RelationProposalAdapter::new(client.clone(), config).unwrap();
            if let Some(inventory) = inventory {
                adapter = adapter.with_inventory(inventory);
            }
            let result = proposal(&adapter, &with_relations("Related", &[OTHER], false))
                .await
                .unwrap();
            assert_eq!(
                result.edges()[0].target(),
                &GraphTarget::Unresolved {
                    reference: OTHER.into()
                },
                "{case}"
            );
            assert!(result.targets()[OTHER].is_err(), "{case}");
            assert_eq!(client.request_metrics().attempts, 0, "{case}");
        }
    }

    #[tokio::test]
    async fn unreadable_inventory_fails_the_proposal_instead_of_dropping_the_target() {
        use crate::relation_proposal::RelationProposalAdapter;
        let client = offline();
        let adapter = RelationProposalAdapter::new(client.clone(), scope())
            .unwrap()
            .with_inventory(placed(&[]));
        let error = proposal(&adapter, &with_relations("Related", &[UNREADABLE], false))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, BackendErrorKind::Unavailable);
        assert_eq!(client.request_metrics().attempts, 0);
    }

    #[tokio::test]
    async fn current_policy_owner_fences_old_proposals_and_waits_for_acknowledgment_guard() {
        use crate::relation_proposal::RelationProposalAdapter;
        for variant in 0..4 {
            let adapter = RelationProposalAdapter::new(offline(), scope()).unwrap();
            let result = proposal(&adapter, &walked("Body", "Current"))
                .await
                .unwrap();
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
        }
        let client = offline();
        let adapter = RelationProposalAdapter::new(client.clone(), scope()).unwrap();
        let result = proposal(&adapter, &walked("Body", "Current"))
            .await
            .unwrap();
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
    }

    #[tokio::test]
    async fn walk_children_and_inventory_placed_targets_resolve_without_requests() {
        use crate::relation_proposal::RelationProposalAdapter;
        use notion_knowledge_core::graph::GraphTarget;
        // A target the walk vouched for as its child: resolved with no request.
        let client = offline();
        let adapter = RelationProposalAdapter::new(client.clone(), scope()).unwrap();
        let mut page = with_relations("Related", &[OTHER], false);
        page.children.insert(OTHER.into());
        let result = proposal(&adapter, &page).await.unwrap();
        assert_eq!(
            result.edges()[0].target(),
            &GraphTarget::Page {
                page_id: OTHER.into()
            }
        );
        assert_eq!(client.request_metrics().attempts, 0);

        // The same target, not a child but placed under the root by the
        // inventory: resolved with no request either.
        let client = offline();
        let adapter = RelationProposalAdapter::new(client.clone(), scope())
            .unwrap()
            .with_inventory(placed(&[(OTHER, &[ROOT])]));
        let result = proposal(&adapter, &with_relations("Related", &[OTHER], false))
            .await
            .unwrap();
        assert!(result.targets()[OTHER].is_ok());
        // A row of an allowed database, and a target that is itself an
        // excluded-descendants page (only its descendants are excluded).
        let mut config = scope();
        config.exclusions.descendants_of.insert(OTHER.into());
        for inventory in [placed_row(), placed(&[(OTHER, &[ROOT])])] {
            let adapter = RelationProposalAdapter::new(client.clone(), config.clone())
                .unwrap()
                .with_inventory(inventory);
            let row = proposal(&adapter, &with_relations("Related", &[OTHER], false))
                .await
                .unwrap();
            assert!(row.targets()[OTHER].is_ok());
        }
        assert_eq!(
            result.edges()[0].target(),
            &GraphTarget::Page {
                page_id: OTHER.into()
            }
        );
        assert_eq!(client.request_metrics().attempts, 0);
    }
}
