//! Physically authorized relation proposals. No discovery, target content or writes.
use crate::{NotionClient, prepare::ScopedPageSnapshot};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    chunking::ChunkConfig,
    graph::GraphEdge,
    indexed::{IndexedChunk, PropertyValue},
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
        snapshot: &ScopedPageSnapshot,
        scope: &LifecycleScope,
    ) -> Result<CandidateProjection, BackendError> {
        let mut targets = BTreeSet::new();
        for value in snapshot.document.metadata.properties.values() {
            if let PropertyValue::PageIds(ids) = value {
                targets.extend(ids.iter().cloned());
            }
        }
        // Validate bounds before making candidate requests.
        relation_edges_with_names(
            &snapshot.document.metadata,
            &snapshot.relation_names,
            &BTreeSet::from([snapshot.document.metadata.page_id.clone()]),
        )
        .map_err(|_| conflict())?;
        let mut outcomes = BTreeMap::new();
        let mut evidence = BTreeMap::new();
        let mut allowed = BTreeSet::from([snapshot.document.metadata.page_id.clone()]);
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
            if target == snapshot.document.metadata.page_id
                && observed.as_ref().ok() != Some(&snapshot.evidence)
            {
                return Err(conflict());
            }
            evidence.insert(target.clone(), observed);
            if outcome.is_ok() {
                allowed.insert(target.clone());
            }
            outcomes.insert(target, outcome);
        }
        let edges = relation_edges_with_names(
            &snapshot.document.metadata,
            &snapshot.relation_names,
            &allowed,
        )
        .map_err(|_| conflict())?;
        Ok((edges, outcomes, evidence))
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
        let (edges, targets, evidence) = self.targets(&snapshot, &scope).await?;
        if *self.policy.read().await != scope {
            return Err(conflict());
        }
        Ok(RelationProposal {
            snapshot,
            edges,
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
        let (edges, targets, evidence) = self.targets(&proposal.snapshot, &scope).await?;
        if edges != proposal.edges || targets != proposal.targets || evidence != proposal.evidence {
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
