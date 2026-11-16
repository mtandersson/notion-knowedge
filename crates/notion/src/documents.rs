//! Read-only scoped canonical snapshots; no index or embedding side effects.
use crate::{
    NotionClient,
    link_graph::page_link_edges,
    links::extract_relationships,
    media_identity::canonical_content,
    pages::page_id,
    walk::{TreeWalk, WalkedPage},
};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, Page, PageContent, PageId},
    chunking::{ChunkConfig, chunk_document},
    discovery::ExclusionRules,
    fingerprint::{content_hash, identify_chunks},
    graph::GraphEdgeStore,
    indexed::{IndexedChunk, IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
    inventory::InventoryParent,
    lifecycle::LifecycleScope,
    relations::relation_edges,
    sync_state::{SyncStateError, validate_identifier},
};

#[derive(Debug, serde::Serialize)]
pub struct DocumentSnapshot {
    /// The full authorized walk, even when only one page's content was kept.
    pub walk: TreeWalk,
    pub documents: Vec<IndexedDocument>,
    pub chunks: Vec<IndexedChunk>,
}

/// Persist relation-property edges only from a *completed* authorized walk.
/// The caller must first ensure this snapshot is current for its trusted root,
/// commit lease and exclusion policy. This method cannot grant graph traversal.
impl DocumentSnapshot {
    /// Snapshots are single-root: exactly one walked entry has a root parent.
    fn root(&self) -> Result<&str, SyncStateError> {
        let mut roots = self
            .walk
            .entries
            .iter()
            .filter(|e| e.parent == InventoryParent::Root);
        match (roots.next(), roots.next()) {
            (Some(root), None) => {
                validate_identifier(&root.page_id)?;
                Ok(&root.page_id)
            }
            _ => Err(SyncStateError::InvalidInput),
        }
    }
    /// Persist graph links from the lossless page bodies in this authorized
    /// snapshot. Independent relation-property edges are never touched.
    /// The caller supplies the fresh authorized walk and commit fence.
    pub fn persist_link_edges(&self, store: &dyn GraphEdgeStore) -> Result<(), SyncStateError> {
        use std::collections::BTreeSet;

        let root = self.root()?;
        let allowed = self.walk.readable();

        let mut prepared = Vec::with_capacity(self.documents.len());
        let mut seen = BTreeSet::new();
        let mut workspace = None;
        for doc in &self.documents {
            if !seen.insert(doc.metadata.page_id.clone())
                || doc.metadata.source.root_page_id != root
                || !allowed.contains(&doc.metadata.page_id)
            {
                return Err(SyncStateError::InvalidInput);
            }
            let current = &doc.metadata.source.workspace_id;
            validate_identifier(current)?;
            if workspace.is_some_and(|previous| previous != current) {
                return Err(SyncStateError::InvalidInput);
            }
            workspace = Some(current);

            let content = PageContent {
                page: Page {
                    relation_names: Default::default(),
                    id: PageId(doc.metadata.page_id.clone()),
                    url: doc.metadata.url.clone(),
                    title: doc.metadata.title.clone(),
                    last_edited_time: doc.metadata.last_edited_time.clone(),
                    archived: false,
                    // Relation properties are indexed by #61, not this hook.
                    properties: Default::default(),
                },
                markdown: doc.text.clone(),
            };
            prepared.push((
                doc.metadata.page_id.as_str(),
                page_link_edges(&content, &allowed)?,
            ));
        }

        // All pages are prevalidated. Each page's SQLite replacement is atomic.
        // Multi-page snapshots require the caller's own fenced index commit.
        for (page_id, edges) in prepared {
            store.replace_page_link_edges(page_id, &edges)?;
        }
        Ok(())
    }

    pub fn persist_relation_edges(&self, store: &dyn GraphEdgeStore) -> Result<(), SyncStateError> {
        use std::collections::BTreeSet;

        // A selected-page snapshot has the *full* authorized walk, even when
        // only the selected page's content was kept.
        let root = self.root()?;
        let allowed = self.walk.readable();
        let mut prepared = Vec::with_capacity(self.documents.len());
        let mut seen = BTreeSet::new();
        let mut workspace = None;
        for doc in &self.documents {
            if !seen.insert(doc.metadata.page_id.clone())
                || doc.metadata.source.root_page_id != root
                || !allowed.contains(&doc.metadata.page_id)
            {
                return Err(SyncStateError::InvalidInput);
            }
            let current = &doc.metadata.source.workspace_id;
            validate_identifier(current)?;
            if workspace.is_some_and(|previous| previous != current) {
                return Err(SyncStateError::InvalidInput);
            }
            workspace = Some(current);
            prepared.push((
                doc.metadata.page_id.as_str(),
                relation_edges(&doc.metadata, &allowed)?,
            ));
        }

        // All data is validated before the first write. Each page replacement
        // is its own SQLite transaction; a multi-page index commit still needs
        // its existing application-level fence / reconciliation coordinator.
        for (page_id, edges) in prepared {
            store.replace_page_relation_edges(page_id, &edges)?;
        }
        Ok(())
    }
}

fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.discover_documents",
        retry_after: None,
        committed_page_id: None,
    }
}

impl NotionClient {
    /// Walk one root and keep each readable page's walked content. Any walk
    /// failure discards the entire in-memory snapshot.
    async fn walk_documents(
        &self,
        root: &PageId,
        workspace_id: &str,
        rules: &ExclusionRules,
    ) -> Result<(TreeWalk, Vec<WalkedPage>), BackendError> {
        let scope = LifecycleScope {
            workspace_id: workspace_id.into(),
            generation: 0,
            roots: vec![root.clone()],
            exclusions: rules.clone(),
        };
        let (send, mut receive) = tokio::sync::mpsc::channel(8);
        let walk = async move {
            let send = send;
            self.walk_tree(&scope, Some(&send)).await
        };
        let collect = async {
            let mut pages = Vec::new();
            while let Some(page) = receive.recv().await {
                if page.skip.is_none() {
                    pages.push(page);
                }
            }
            pages
        };
        let (walk, mut pages) = futures_util::future::join(walk, collect).await;
        pages.sort_by(|a, b| a.entry.page_id.cmp(&b.entry.page_id));
        Ok((walk?, pages))
    }

    /// Assemble one fresh root snapshot from a single walk. Only walked,
    /// readable pages are assembled. Workspace identity is trusted caller
    /// provenance, not an authorization claim.
    pub async fn discover_documents(
        &self,
        root: &str,
        workspace_id: &str,
        rules: &ExclusionRules,
        config: ChunkConfig,
    ) -> Result<DocumentSnapshot, BackendError> {
        let root = page_id(root)?;
        if workspace_id.is_empty()
            || config.target_chars == 0
            || config.overlap_chars >= config.target_chars
        {
            return Err(error(BackendErrorKind::InvalidInput));
        }
        let (walk, pages) = self.walk_documents(&root, workspace_id, rules).await?;
        assemble_documents(root.0, workspace_id, walk, pages, config)
    }

    /// A fresh walk authorizes exactly one selected page. Other walked pages
    /// and referenced targets never reach chunking.
    pub async fn discover_selected_document(
        &self,
        root: &str,
        selected: &str,
        workspace_id: &str,
        rules: &ExclusionRules,
        config: ChunkConfig,
    ) -> Result<DocumentSnapshot, BackendError> {
        let root = page_id(root)?;
        let selected = page_id(selected)?;
        if workspace_id.is_empty()
            || config.target_chars == 0
            || config.overlap_chars >= config.target_chars
        {
            return Err(error(BackendErrorKind::InvalidInput));
        }
        let (walk, mut pages) = self.walk_documents(&root, workspace_id, rules).await?;
        pages.retain(|page| page.entry.page_id == selected.0);
        if pages.is_empty() {
            return Err(error(BackendErrorKind::PermissionDenied));
        }
        assemble_documents(root.0, workspace_id, walk, pages, config)
    }
}

fn assemble_documents(
    root: String,
    workspace_id: &str,
    walk: TreeWalk,
    pages: Vec<WalkedPage>,
    config: ChunkConfig,
) -> Result<DocumentSnapshot, BackendError> {
    let mut documents = Vec::new();
    let mut chunks = Vec::new();
    for walked in pages {
        // Truncated or partially rendered pages are never partial successes.
        let content = canonical_content(
            walked
                .content
                .ok_or_else(|| error(BackendErrorKind::UnsupportedContent))?,
        );
        let document = IndexedDocument {
            schema_version: SchemaVersion::V1,
            metadata: IndexedMetadata {
                page_id: content.page.id.0.clone(),
                block_id: None,
                url: content.page.url.clone(),
                title: content.page.title.clone(),
                heading_path: Vec::new(),
                last_edited_time: content.page.last_edited_time.clone(),
                source: SourceMetadata {
                    workspace_id: workspace_id.into(),
                    root_page_id: root.clone(),
                    database_id: None,
                    data_source_id: None,
                },
                properties: content.page.properties.clone(),
            },
            text: content.markdown.clone(),
            content_hash: content_hash(&content.markdown),
            links: extract_relationships(&content).links,
        };
        let drafts =
            chunk_document(&document, config).map_err(|_| error(BackendErrorKind::InvalidInput))?;
        let mut identified =
            identify_chunks(drafts, &[]).map_err(|_| error(BackendErrorKind::Internal))?;
        for chunk in &mut identified {
            chunk.links = extract_relationships(&PageContent {
                page: content.page.clone(),
                markdown: chunk.text.clone(),
            })
            .links;
        }
        chunks.extend(identified);
        documents.push(document);
    }
    Ok(DocumentSnapshot {
        walk,
        documents,
        chunks,
    })
}
