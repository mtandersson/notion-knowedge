//! Read-only scoped canonical snapshots; no index or embedding side effects.
use crate::{NotionClient, links::extract_relationships, pages::page_id};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageContent},
    chunking::{ChunkConfig, chunk_document},
    discovery::{DiscoveryReport, ExclusionRules},
    fingerprint::{content_hash, identify_chunks},
    graph::GraphEdgeStore,
    indexed::{IndexedChunk, IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
    relations::relation_edges,
    sync_state::{SyncStateError, validate_identifier},
};

#[derive(Debug, serde::Serialize)]
pub struct DocumentSnapshot {
    pub discovery: DiscoveryReport,
    pub documents: Vec<IndexedDocument>,
    pub chunks: Vec<IndexedChunk>,
}

/// Persist relation-property edges only from a *completed* authorized crawl.
/// The caller must first ensure this snapshot is current for its trusted root,
/// commit lease and exclusion policy. This method cannot grant graph traversal.
impl DocumentSnapshot {
    pub fn persist_relation_edges(&self, store: &dyn GraphEdgeStore) -> Result<(), SyncStateError> {
        use std::collections::BTreeSet;

        // A selected-page snapshot has the *full* authorized discovery set,
        // even when only the selected page's content was read.
        if self.discovery.roots.len() != 1 {
            return Err(SyncStateError::InvalidInput);
        }
        let root = &self.discovery.roots[0];
        validate_identifier(root)?;
        let allowed: BTreeSet<_> = self.discovery.pages.iter().map(|p| p.id.clone()).collect();
        if allowed.len() != self.discovery.pages.len() {
            return Err(SyncStateError::InvalidInput);
        }
        let mut prepared = Vec::with_capacity(self.documents.len());
        let mut seen = BTreeSet::new();
        let mut workspace = None;
        for doc in &self.documents {
            if !seen.insert(doc.metadata.page_id.clone())
                || doc.metadata.source.root_page_id != *root
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
    /// Assemble one fresh root snapshot. Only successfully discovered page IDs
    /// reach exact reads. Any failure discards the entire in-memory snapshot.
    /// Workspace identity is trusted caller provenance, not an authorization claim.
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
        let discovery = self
            .crawl_with_exclusions(std::slice::from_ref(&root), rules)
            .await?;
        self.assemble_documents(root.0, workspace_id, discovery, config)
            .await
    }

    /// Fresh discovery authorizes exactly one selected page. Other discovered
    /// pages and referenced targets never reach content extraction.
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
        let discovery = self
            .crawl_with_exclusions(std::slice::from_ref(&root), rules)
            .await?;
        if !discovery.pages.iter().any(|page| page.id == selected.0) {
            return Err(error(BackendErrorKind::PermissionDenied));
        }
        let mut selected_report = discovery.clone();
        selected_report.pages.retain(|page| page.id == selected.0);
        let mut snapshot = self
            .assemble_documents(root.0, workspace_id, selected_report, config)
            .await?;
        snapshot.discovery = discovery;
        Ok(snapshot)
    }

    async fn assemble_documents(
        &self,
        root: String,
        workspace_id: &str,
        discovery: DiscoveryReport,
        config: ChunkConfig,
    ) -> Result<DocumentSnapshot, BackendError> {
        let mut documents = Vec::new();
        let mut chunks = Vec::new();
        for discovered in &discovery.pages {
            let content = self.read_content(&discovered.id).await?;
            if content.page.archived {
                return Err(error(BackendErrorKind::Conflict));
            }
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
            let drafts = chunk_document(&document, config)
                .map_err(|_| error(BackendErrorKind::InvalidInput))?;
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
            discovery,
            documents,
            chunks,
        })
    }
}
