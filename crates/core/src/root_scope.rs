//! Server-side root authorization for authoritative Notion page operations.
//!
//! This is a reusable policy gate; callers must place it before *every* read,
//! search result disclosure, or mutation and revalidate near a mutation. It
//! deliberately keeps no positive authorization cache.

use std::{collections::BTreeSet, sync::Arc};

use crate::{
    backend::PageId,
    discovery::SourceType,
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleScope, LifecycleStatus, PageLifecycle,
        PhysicalParent,
    },
};

/// No source IDs, private page metadata, or raw backend errors are exposed to a
/// consumer denied by the policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScopeError {
    InvalidConfiguration,
    Denied,
    Unverifiable,
    Changed,
}

impl std::fmt::Display for ScopeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "root-scope authorization failed: {self:?}")
    }
}
impl std::error::Error for ScopeError {}

fn canonical_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase(),
        })
}

fn valid_scope(scope: &LifecycleScope) -> bool {
    if !canonical_id(&scope.workspace_id)
        || scope.roots.is_empty()
        || scope.roots.len() > 100
        || scope.roots.iter().any(|root| !canonical_id(&root.0))
        || scope
            .exclusions
            .page_ids
            .iter()
            .chain(&scope.exclusions.descendants_of)
            .any(|id| !canonical_id(id))
    {
        return false;
    }
    let distinct: BTreeSet<_> = scope.roots.iter().map(|root| &root.0).collect();
    distinct.len() == scope.roots.len()
}

/// Immutable, trusted policy snapshot. Construct only from authenticated
/// server-side workspace/root configuration, never from an MCP request.
///
/// Recreate this gate when the operator's scope generation changes. For
/// mutation verification, pass the newly loaded scope to `revalidate`.
#[derive(Clone)]
pub struct RootScopeGate {
    source: Arc<dyn PageLifecycle>,
    scope: LifecycleScope,
}

/// Unforgeable positive evidence for exactly one selected page and policy
/// generation. Its fields are not public and its constructor is private.
pub struct AuthorizedPage {
    evidence: LifecycleEvidence,
}

impl AuthorizedPage {
    /// Restrict a verified physical root to the effective operator/caller
    /// intersection. Indexed root labels are never proof of membership.
    pub fn belongs_to_any(&self, roots: &[String]) -> bool {
        matches!(&self.evidence.status, LifecycleStatus::Allowed { roots: physical }
            if physical.iter().any(|id| roots.iter().any(|root| root == id)))
    }
}

impl RootScopeGate {
    /// Trust roots only from this server-constructed policy.
    pub fn configured_scope(&self) -> &LifecycleScope {
        &self.scope
    }

    pub fn new(source: Arc<dyn PageLifecycle>, scope: LifecycleScope) -> Result<Self, ScopeError> {
        if !valid_scope(&scope) {
            return Err(ScopeError::InvalidConfiguration);
        }
        Ok(Self { source, scope })
    }

    /// A direct page ID is not proof of authority. Fetch fresh physical
    /// ancestry and verify every edge, root and exclusion before proceeding.
    pub async fn authorize(&self, page: &PageId) -> Result<AuthorizedPage, ScopeError> {
        if !canonical_id(&page.0) {
            return Err(ScopeError::Denied);
        }
        let evidence = self
            .source
            .lifecycle(page, &self.scope)
            .await
            .map_err(|_| ScopeError::Unverifiable)?;
        self.verify(page, &evidence)?;
        Ok(AuthorizedPage { evidence })
    }

    /// Re-fetch chain metadata through the provider before a sensitive use.
    /// A caller must provide CURRENT server-side policy configuration; if it
    /// differs, the old permit must be discarded and authorized afresh.
    ///
    /// Success is not a Notion transaction lock or atomic compare-and-swap.
    pub async fn revalidate(
        &self,
        permit: &AuthorizedPage,
        current_scope: &LifecycleScope,
    ) -> Result<(), ScopeError> {
        if !valid_scope(current_scope) || *current_scope != self.scope {
            return Err(ScopeError::Changed);
        }
        let page = permit
            .evidence
            .ancestry
            .first()
            .ok_or(ScopeError::Unverifiable)?;
        self.verify(&PageId(page.id.clone()), &permit.evidence)?;
        self.source
            .revalidate_lifecycle(&permit.evidence, current_scope)
            .await
            .map_err(|_| ScopeError::Changed)
    }

    fn verify(&self, page: &PageId, evidence: &LifecycleEvidence) -> Result<(), ScopeError> {
        if evidence.scope != self.scope || evidence.ancestry.is_empty() {
            return Err(ScopeError::Unverifiable);
        }
        let nodes = &evidence.ancestry;
        if nodes[0].kind != LifecycleKind::Page || nodes[0].id != page.0 {
            return Err(ScopeError::Unverifiable);
        }
        let mut observed = BTreeSet::new();
        for (index, node) in nodes.iter().enumerate() {
            if !canonical_id(&node.id)
                || node.revision.is_empty()
                || node.inactive
                || !observed.insert((node.kind, node.id.as_str()))
            {
                return Err(ScopeError::Unverifiable);
            }
            match &node.parent {
                PhysicalParent::Object { kind, id } => {
                    let next = nodes.get(index + 1).ok_or(ScopeError::Unverifiable)?;
                    if next.kind != *kind || next.id != *id {
                        return Err(ScopeError::Unverifiable);
                    }
                }
                PhysicalParent::Workspace => {
                    if index + 1 != nodes.len() {
                        return Err(ScopeError::Unverifiable);
                    }
                }
            }
            let source_type = match node.kind {
                LifecycleKind::Page => Some(SourceType::Page),
                LifecycleKind::Database => Some(SourceType::Database),
                LifecycleKind::DataSource => Some(SourceType::DataSource),
                LifecycleKind::Block => None,
            };
            if source_type
                .and_then(|kind| self.scope.exclusions.exclusion(&node.id, kind))
                .is_some()
                || (index > 0
                    && node.kind == LifecycleKind::Page
                    && self.scope.exclusions.descendants_of.contains(&node.id))
            {
                return Err(ScopeError::Denied);
            }
        }
        // A linked or related page does not count as physical ancestry.
        let actual_roots: BTreeSet<_> = self
            .scope
            .roots
            .iter()
            .filter(|root| {
                nodes
                    .iter()
                    .any(|node| node.kind == LifecycleKind::Page && node.id == root.0)
            })
            .map(|root| root.0.as_str())
            .collect();
        let LifecycleStatus::Allowed { roots } = &evidence.status else {
            return Err(ScopeError::Denied);
        };
        let claimed_roots: BTreeSet<_> = roots.iter().map(String::as_str).collect();
        if actual_roots.is_empty()
            || actual_roots != claimed_roots
            || claimed_roots.len() != roots.len()
        {
            return Err(ScopeError::Denied);
        }
        Ok(())
    }
}
