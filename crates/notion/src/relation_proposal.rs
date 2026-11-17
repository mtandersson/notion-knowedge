//! Relation proposals for a prepared page. No discovery, target content or writes.
//! Source scope comes from the walk; only relation targets are authorized here,
//! from the walk inventory rather than a live ancestor read.
use crate::{
    NotionClient,
    prepare::{ScopedPageSnapshot, prepare_content},
    walk::WalkedPage,
};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, Page, PageContent, PageId},
    chunking::ChunkConfig,
    discovery::SourceType,
    graph::GraphEdge,
    indexed::{IndexedChunk, IndexedMetadata, PropertyValue, SourceMetadata},
    inventory::{InventoryParent, InventoryPlacement},
    lifecycle::{LifecycleEvidence, LifecycleScope},
    relations::relation_edges_with_names,
    root_scope::{RootScopeGate, ScopeError},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

type TargetOutcomes = BTreeMap<String, Result<(), ScopeError>>;
type CandidateProjection = (Vec<GraphEdge>, TargetOutcomes);

fn conflict() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Conflict,
        operation: "notion.relation_proposal",
        retry_after: None,
        committed_page_id: None,
    }
}

/// Operator-owned optional target enrichment. Source authorization is always fresh.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProjectionMode {
    #[default]
    ResolveTargets,
    UnresolvedTargets,
}

/// Trusted composition-owned adapter. Clones share one current policy owner.
/// Construct from operator configuration, never request/relation metadata.
#[derive(Clone)]
pub struct RelationProposalAdapter {
    client: NotionClient,
    mode: ProjectionMode,
    policy: Arc<RwLock<LifecycleScope>>,
    inventory: Option<Arc<dyn InventoryPlacement>>,
}

/// Complete effect plus source snapshot. Private fields prevent mixing labels,
/// membership or authority between prepared reads. Edges convey no authority.
pub struct RelationProposal {
    snapshot: ScopedPageSnapshot,
    edges: Vec<GraphEdge>,
    links: Vec<GraphEdge>,
    targets: TargetOutcomes,
    scope: LifecycleScope,
    owner: Arc<RwLock<LifecycleScope>>,
    mode: ProjectionMode,
}
impl RelationProposal {
    pub fn snapshot(&self) -> &ScopedPageSnapshot {
        &self.snapshot
    }
    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }
    pub fn links(&self) -> &[GraphEdge] {
        &self.links
    }
    /// Conservative heap-retention estimate of the prepared content and edges.
    pub fn retained_bytes(&self) -> usize {
        self.snapshot.retained_bytes() + (self.edges.len() + self.links.len()) * 512
    }
    /// Per-candidate physical authorization outcomes; failures remain unresolved.
    pub fn targets(&self) -> &TargetOutcomes {
        &self.targets
    }
}

/// Keep this guard alive until the owned operation finishes effects and final
/// acknowledgment. Policy replacement waits; dropping it grants no authority.
#[must_use = "hold authority through effects and acknowledgment"]
pub struct RelationAuthority {
    _policy: OwnedRwLockReadGuard<LifecycleScope>,
}

impl RelationProposalAdapter {
    pub fn new(client: NotionClient, policy: LifecycleScope) -> Result<Self, ScopeError> {
        RootScopeGate::new(Arc::new(client.clone()), policy.clone())?;
        Ok(Self {
            client,
            mode: ProjectionMode::default(),
            policy: Arc::new(RwLock::new(policy)),
            inventory: None,
        })
    }
    /// Resolve relation and link targets through this walk inventory. Without
    /// one, every target other than a walked child stays unresolved.
    pub fn with_inventory(mut self, inventory: Arc<dyn InventoryPlacement>) -> Self {
        self.inventory = Some(inventory);
        self
    }
    /// Membership from the inventory alone, never a Notion request: the
    /// target must be placed under a current root, with no excluded page on
    /// its chain and no ancestor whose descendants are excluded. A target the
    /// inventory does not hold stays unresolved until a walk places it.
    fn placed(
        &self,
        target: &str,
        scope: &LifecycleScope,
    ) -> Result<Result<(), ScopeError>, BackendError> {
        let Some(inventory) = &self.inventory else {
            return Ok(Err(ScopeError::Unverifiable));
        };
        let placed = match inventory.placed_chain(target) {
            Ok(Some(placed)) => placed,
            Ok(None) => return Ok(Err(ScopeError::Unverifiable)),
            Err(_) => {
                return Err(BackendError {
                    kind: BackendErrorKind::Unavailable,
                    operation: "notion.relation_proposal.inventory",
                    retry_after: None,
                    committed_page_id: None,
                });
            }
        };
        let chain = &placed.ancestors;
        let root = chain.last().map_or(target, String::as_str);
        if !scope.roots.iter().any(|r| r.0 == root) {
            return Ok(Err(ScopeError::Denied));
        }
        let excluded = std::iter::once(target)
            .chain(chain.iter().map(String::as_str))
            .any(|id| scope.exclusions.exclusion(id, SourceType::Page).is_some())
            || chain
                .iter()
                .any(|id| scope.exclusions.descendants_of.contains(id))
            || placed.data_sources.iter().any(|(source, database)| {
                scope
                    .exclusions
                    .exclusion(source, SourceType::DataSource)
                    .is_some()
                    || scope
                        .exclusions
                        .exclusion(database, SourceType::Database)
                        .is_some()
            });
        Ok(if excluded {
            Err(ScopeError::Denied)
        } else {
            Ok(())
        })
    }
    /// Whether any of these targets is now placed in scope by the inventory,
    /// so that a page whose projection left it unresolved would resolve it.
    pub async fn any_resolvable(&self, targets: &[String]) -> Result<bool, BackendError> {
        if self.mode != ProjectionMode::ResolveTargets {
            return Ok(false);
        }
        let scope = self.policy.read().await.clone();
        for target in targets {
            if self.placed(target, &scope)?.is_ok() {
                return Ok(true);
            }
        }
        Ok(false)
    }
    /// Trusted immutable composition setting; never infer from source references.
    pub fn with_projection_mode(mut self, mode: ProjectionMode) -> Self {
        self.mode = mode;
        self
    }
    pub fn projection_mode(&self) -> ProjectionMode {
        self.mode
    }
    /// Serialize current operator policy replacement with in-flight commit authority.
    /// Generation must increase, even if the operator later restores old settings.
    pub async fn replace_policy(&self, next: LifecycleScope) -> Result<(), ScopeError> {
        RootScopeGate::new(Arc::new(self.client.clone()), next.clone())?;
        let mut current = self.policy.write().await;
        if next.generation <= current.generation {
            return Err(ScopeError::Changed);
        }
        *current = next;
        Ok(())
    }
    fn targets(
        &self,
        metadata: &IndexedMetadata,
        names: &BTreeMap<String, String>,
        content: Option<&PageContent>,
        scope: &LifecycleScope,
        children: &BTreeSet<String>,
    ) -> Result<CandidateProjection, BackendError> {
        let mut targets = BTreeSet::new();
        for value in metadata.properties.values() {
            if let PropertyValue::PageIds(ids) = value {
                targets.extend(ids.iter().cloned());
            }
        }
        if let Some(content) = content {
            for edge in crate::link_graph::page_link_edges(
                content,
                &BTreeSet::from([metadata.page_id.clone()]),
            )
            .map_err(|_| conflict())?
            {
                match edge.target() {
                    notion_knowledge_core::graph::GraphTarget::Page { page_id } => {
                        targets.insert(page_id.clone());
                    }
                    notion_knowledge_core::graph::GraphTarget::Unresolved { reference }
                        if !reference.starts_with("invalid-page-link:") =>
                    {
                        targets.insert(reference.clone());
                    }
                    _ => {}
                }
            }
        }
        // Validate bounds before making candidate requests.
        relation_edges_with_names(metadata, names, &BTreeSet::from([metadata.page_id.clone()]))
            .map_err(|_| conflict())?;
        let mut outcomes = BTreeMap::new();
        let mut allowed = BTreeSet::from([metadata.page_id.clone()]);
        if self.mode == ProjectionMode::ResolveTargets {
            // The page itself is in scope by the walk; only other targets
            // need their own authorization, from local state.
            for target in targets.into_iter().filter(|t| *t != metadata.page_id) {
                let outcome = if children.contains(&target) {
                    Ok(())
                } else {
                    self.placed(&target, scope)?
                };
                if outcome.is_ok() {
                    allowed.insert(target.clone());
                }
                outcomes.insert(target, outcome);
            }
        }
        let mut edges =
            relation_edges_with_names(metadata, names, &allowed).map_err(|_| conflict())?;
        if let Some(content) = content {
            edges.extend(
                crate::link_graph::page_link_edges(content, &allowed).map_err(|_| conflict())?,
            );
        }
        Ok((edges, outcomes))
    }
    /// Read-only metadata proposal for reconciliation planning. It never fetches
    /// Markdown or confers commit authority; apply must prepare and revalidate.
    pub async fn inspect_metadata(
        &self,
        page: &Page,
        evidence: &LifecycleEvidence,
    ) -> Result<Vec<GraphEdge>, BackendError> {
        let scope = self.policy.read().await.clone();
        if scope != evidence.scope
            || evidence
                .ancestry
                .first()
                .is_none_or(|node| node.id != page.id.0)
            || page.archived
        {
            return Err(conflict());
        }
        let root = match &evidence.status {
            notion_knowledge_core::lifecycle::LifecycleStatus::Allowed { roots } => {
                roots.first().ok_or_else(conflict)?
            }
            _ => return Err(conflict()),
        };
        let metadata = IndexedMetadata {
            page_id: page.id.0.clone(),
            block_id: None,
            url: page.url.clone(),
            title: page.title.clone(),
            heading_path: vec![],
            last_edited_time: page.last_edited_time.clone(),
            source: SourceMetadata {
                workspace_id: scope.workspace_id.clone(),
                root_page_id: root.clone(),
                database_id: None,
                data_source_id: None,
            },
            properties: page.properties.clone(),
        };
        let (edges, _) = self.targets(
            &metadata,
            &page.relation_names,
            None,
            &scope,
            &BTreeSet::new(),
        )?;
        if *self.policy.read().await != scope {
            return Err(conflict());
        }
        Ok(edges)
    }
    /// Propose a page from the walk's single read. No request is made:
    /// targets are authorized from the walk's children and inventory.
    pub async fn prepare_walked(
        &self,
        walked: &WalkedPage,
        previous: &[IndexedChunk],
        chunks: ChunkConfig,
    ) -> Result<RelationProposal, BackendError> {
        let scope = self.policy.read().await.clone();
        let snapshot = self
            .client
            .prepare_walked_page(walked, &scope, previous, chunks)?;
        self.propose(snapshot, scope, &walked.children).await
    }

    /// Propose a page from content the caller already read, for example to
    /// prepare it again after its indexed chunks changed.
    pub async fn prepare_content(
        &self,
        content: PageContent,
        parent: InventoryParent,
        root: &PageId,
        previous: &[IndexedChunk],
        chunks: ChunkConfig,
    ) -> Result<RelationProposal, BackendError> {
        let scope = self.policy.read().await.clone();
        let snapshot = prepare_content(content, parent, root, &scope, previous, chunks)?;
        self.propose(snapshot, scope, &BTreeSet::new()).await
    }

    /// Read one page once (metadata and Markdown) and propose it. For a
    /// single-page caller whose own lifecycle read placed the page.
    pub async fn prepare_read(
        &self,
        page: &PageId,
        parent: InventoryParent,
        root: &PageId,
        previous: &[IndexedChunk],
        chunks: ChunkConfig,
    ) -> Result<RelationProposal, BackendError> {
        let content = self.client.read_content(&page.0).await?;
        if content.page.id != *page {
            return Err(conflict());
        }
        self.prepare_content(content, parent, root, previous, chunks)
            .await
    }

    async fn propose(
        &self,
        snapshot: ScopedPageSnapshot,
        scope: LifecycleScope,
        children: &BTreeSet<String>,
    ) -> Result<RelationProposal, BackendError> {
        let (edges, targets) = self.targets(
            &snapshot.document.metadata,
            &snapshot.relation_names,
            Some(snapshot.source()),
            &scope,
            children,
        )?;
        if *self.policy.read().await != scope {
            return Err(conflict());
        }
        let (links, edges) = edges
            .into_iter()
            .partition(|edge| edge.relation_type().starts_with("link:"));
        Ok(RelationProposal {
            snapshot,
            edges,
            links,
            targets,
            scope,
            owner: self.policy.clone(),
            mode: self.mode,
        })
    }
    /// Retain the same current policy owner for an authoritative removal.
    /// The caller's lifecycle read is not repeated.
    pub async fn revalidate_removal(
        &self,
        evidence: &LifecycleEvidence,
    ) -> Result<RelationAuthority, BackendError> {
        let scope = self.policy.clone().read_owned().await;
        if *scope != evidence.scope
            || matches!(
                evidence.status,
                notion_knowledge_core::lifecycle::LifecycleStatus::Allowed { .. }
            )
        {
            return Err(conflict());
        }
        Ok(RelationAuthority { _policy: scope })
    }

    /// Hold the current policy for a removal a complete walk decided.
    pub async fn removal_authority(
        &self,
        scope: &LifecycleScope,
    ) -> Result<RelationAuthority, BackendError> {
        let current = self.policy.clone().read_owned().await;
        if *current != *scope {
            return Err(conflict());
        }
        Ok(RelationAuthority { _policy: current })
    }

    /// Invoke INSIDE the operation-owned commit, before any effects or receipt.
    /// Checks only that the proposal belongs to this adapter's current policy
    /// and mode, and holds that policy through acknowledgment. Source is not
    /// read again: a later upstream edit or move is picked up by the next walk.
    pub async fn revalidate(
        &self,
        proposal: &RelationProposal,
    ) -> Result<RelationAuthority, BackendError> {
        if self.mode != proposal.mode || !Arc::ptr_eq(&self.policy, &proposal.owner) {
            return Err(conflict());
        }
        let scope = self.policy.clone().read_owned().await;
        if *scope != proposal.scope {
            return Err(conflict());
        }
        Ok(RelationAuthority { _policy: scope })
    }
}
