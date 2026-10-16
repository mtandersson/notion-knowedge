//! Physically authorized relation proposals. No discovery, target content or writes.
use crate::{NotionClient, prepare::ScopedPageSnapshot};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, Page, PageContent, PageId},
    chunking::ChunkConfig,
    graph::GraphEdge,
    indexed::{IndexedChunk, IndexedMetadata, PropertyValue, SourceMetadata},
    lifecycle::{LifecycleEvidence, LifecycleScope, PageLifecycle},
    relations::relation_edges_with_names,
    root_scope::{RootScopeGate, ScopeError},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};

type TargetOutcomes = BTreeMap<String, Result<(), ScopeError>>;
type TargetObservation = Result<LifecycleEvidence, BackendErrorKind>;
type TargetEvidence = BTreeMap<String, TargetObservation>;
type CandidateProjection = (Vec<GraphEdge>, TargetOutcomes, TargetEvidence);

// Record the real provider observation without weakening RootScopeGate's
// independent verification. Denied chains and sanitized failure classes must
// also remain stable; a denied-to-denied move still requires fresh preparation.
struct RecordingLifecycle {
    client: NotionClient,
    observation: Mutex<Option<TargetObservation>>,
}
impl PageLifecycle for RecordingLifecycle {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            let result = self.client.lifecycle(page, scope).await;
            *self.observation.lock().map_err(|_| conflict())? =
                Some(result.as_ref().cloned().map_err(|failure| failure.kind));
            result
        })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        self.client.revalidate_lifecycle(evidence, scope)
    }
}

fn conflict() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Conflict,
        operation: "notion.relation_proposal",
        retry_after: None,
        committed_page_id: None,
    }
}

/// Trusted composition-owned adapter. Clones share one current policy owner.
/// Construct from operator configuration, never request/relation metadata.
#[derive(Clone)]
pub struct RelationProposalAdapter {
    client: NotionClient,
    policy: Arc<RwLock<LifecycleScope>>,
}

/// Complete effect plus source snapshot. Private fields prevent mixing labels,
/// membership or authority between prepared reads. Edges convey no authority.
pub struct RelationProposal {
    snapshot: ScopedPageSnapshot,
    edges: Vec<GraphEdge>,
    links: Vec<GraphEdge>,
    targets: TargetOutcomes,
    evidence: TargetEvidence,
    owner: Arc<RwLock<LifecycleScope>>,
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
            policy: Arc::new(RwLock::new(policy)),
        })
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
    async fn targets(
        &self,
        metadata: &IndexedMetadata,
        names: &BTreeMap<String, String>,
        content: Option<&PageContent>,
        source: &LifecycleEvidence,
        scope: &LifecycleScope,
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
        let mut evidence = BTreeMap::new();
        let mut allowed = BTreeSet::from([metadata.page_id.clone()]);
        for target in targets {
            let recording = Arc::new(RecordingLifecycle {
                client: self.client.clone(),
                observation: Mutex::new(None),
            });
            let gate =
                RootScopeGate::new(recording.clone(), scope.clone()).map_err(|_| conflict())?;
            let outcome = gate.authorize(&PageId(target.clone())).await.map(|_| ());
            // Invalid candidate IDs are rejected before provider I/O. Preserve
            // that stable failure without inventing physical evidence.
            let observed = recording
                .observation
                .lock()
                .map_err(|_| conflict())?
                .take()
                .unwrap_or(Err(BackendErrorKind::InvalidInput));
            if target == metadata.page_id && observed.as_ref().ok() != Some(source) {
                return Err(conflict());
            }
            evidence.insert(target.clone(), observed);
            if outcome.is_ok() {
                allowed.insert(target.clone());
            }
            outcomes.insert(target, outcome);
        }
        let mut edges =
            relation_edges_with_names(metadata, names, &allowed).map_err(|_| conflict())?;
        if let Some(content) = content {
            edges.extend(
                crate::link_graph::page_link_edges(content, &allowed).map_err(|_| conflict())?,
            );
        }
        Ok((edges, outcomes, evidence))
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
        let (edges, _, _) = self
            .targets(&metadata, &page.relation_names, None, evidence, &scope)
            .await?;
        if self.client.fetch_page(&page.id.0).await? != *page || *self.policy.read().await != scope
        {
            return Err(conflict());
        }
        self.client.revalidate_lifecycle(evidence, &scope).await?;
        Ok(edges)
    }
    pub async fn prepare(
        &self,
        page: &PageId,
        root: &PageId,
        previous: &[IndexedChunk],
        chunks: ChunkConfig,
    ) -> Result<RelationProposal, BackendError> {
        let scope = self.policy.read().await.clone();
        let snapshot = self
            .client
            .prepare_scoped_page(page, root, &scope, previous, chunks)
            .await?;
        let (edges, targets, evidence) = self
            .targets(
                &snapshot.document.metadata,
                &snapshot.relation_names,
                Some(&snapshot.source),
                &snapshot.evidence,
                &scope,
            )
            .await?;
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
            evidence,
            owner: self.policy.clone(),
        })
    }
    /// Retain the same current policy owner for an authoritative removal.
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
        self.client.revalidate_lifecycle(evidence, &scope).await?;
        Ok(RelationAuthority { _policy: scope })
    }

    /// Invoke INSIDE the operation-owned commit, before any effects or receipt.
    /// Freshly check source content/membership/names, physical chains and current
    /// policy; keep the returned guard alive through acknowledgment. Notion has
    /// no atomic snapshot/CAS: a later upstream edit requires another refresh.
    pub async fn revalidate(
        &self,
        proposal: &RelationProposal,
    ) -> Result<RelationAuthority, BackendError> {
        if !Arc::ptr_eq(&self.policy, &proposal.owner) {
            return Err(conflict());
        }
        let scope = self.policy.clone().read_owned().await;
        if *scope != proposal.snapshot.evidence.scope {
            return Err(conflict());
        }
        self.client
            .revalidate_lifecycle(&proposal.snapshot.evidence, &scope)
            .await?;
        let page = &proposal.snapshot.source.page.id.0;
        if self.client.read_content(page).await? != proposal.snapshot.source {
            return Err(conflict());
        }
        let (edges, targets, evidence) = self
            .targets(
                &proposal.snapshot.document.metadata,
                &proposal.snapshot.relation_names,
                Some(&proposal.snapshot.source),
                &proposal.snapshot.evidence,
                &scope,
            )
            .await?;
        let (links, edges): (Vec<_>, Vec<_>) = edges
            .into_iter()
            .partition(|edge| edge.relation_type().starts_with("link:"));
        if edges != proposal.edges
            || links != proposal.links
            || targets != proposal.targets
            || evidence != proposal.evidence
        {
            return Err(conflict());
        }
        // Bracket candidate work with the full authoritative source, then its chain.
        if self.client.read_content(page).await? != proposal.snapshot.source {
            return Err(conflict());
        }
        self.client
            .revalidate_lifecycle(&proposal.snapshot.evidence, &scope)
            .await?;
        Ok(RelationAuthority { _policy: scope })
    }
}
