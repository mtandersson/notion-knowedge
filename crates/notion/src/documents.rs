//! Read-only scoped canonical snapshots; no index or embedding side effects.
use crate::{NotionClient, links::extract_relationships, pages::page_id};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageContent},
    chunking::{ChunkConfig, chunk_document},
    discovery::{DiscoveryReport, ExclusionRules},
    fingerprint::{content_hash, identify_chunks},
    indexed::{IndexedChunk, IndexedDocument, IndexedMetadata, SchemaVersion, SourceMetadata},
};

#[derive(Debug, serde::Serialize)]
pub struct DocumentSnapshot {
    pub discovery: DiscoveryReport,
    pub documents: Vec<IndexedDocument>,
    pub chunks: Vec<IndexedChunk>,
}

fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.discover_documents",
        retry_after: None,
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
                        root_page_id: root.0.clone(),
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
