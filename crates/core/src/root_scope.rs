//! Server-side root authorization for authoritative Notion page operations.
//!
//! [`ReadScopeGate`] authorizes reads, search result disclosure and graph
//! lookups from the page inventory the last tree walk recorded: it is
//! synchronous and never calls Notion, and its staleness is bounded by
//! reconciliation. It keeps no positive authorization cache.

use std::{collections::BTreeSet, sync::Arc};

use crate::{
    backend::PageId,
    discovery::SourceType,
    inventory::{InventoryEntry, InventoryParent},
    lifecycle::LifecycleScope,
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

/// Reject a malformed operator policy (IDs, root count, exclusions).
pub fn validate_scope(scope: &LifecycleScope) -> Result<(), ScopeError> {
    if valid_scope(scope) {
        Ok(())
    } else {
        Err(ScopeError::InvalidConfiguration)
    }
}

/// Where the last tree walk placed each page. Implemented by the sync state
/// store over its page inventory; never by a Notion client.
pub trait Membership: Send + Sync {
    /// The page's own inventory entry, then each ancestor's entry up to and
    /// including the configured root it was walked from. `None` when the
    /// page or any link of its chain is absent from the inventory, or the
    /// chain repeats.
    fn parent_chain(&self, page: &str) -> Result<Option<Vec<InventoryEntry>>, ScopeError>;
}

/// Read authorization from the page inventory. A page is readable only when
/// the last walk reached it through pages and data sources none of which is
/// trashed, unreadable or excluded, from a root the current policy still
/// configures. Moves and exclusions in Notion take effect at the next walk.
#[derive(Clone)]
pub struct ReadScopeGate {
    membership: Arc<dyn Membership>,
    scope: LifecycleScope,
}

/// Proof that one page sat under these configured roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePermit {
    roots: Vec<String>,
}

impl ScopePermit {
    /// Restrict the permit to the effective operator/caller intersection.
    pub fn belongs_to_any(&self, roots: &[String]) -> bool {
        self.roots.iter().any(|root| roots.contains(root))
    }
}

/// How a read is authorized: from the page inventory the operator's own
/// walk records.
#[derive(Clone)]
pub enum ReadAuthority {
    Inventory(ReadScopeGate),
}

impl ReadAuthority {
    pub fn configured_scope(&self) -> &LifecycleScope {
        match self {
            Self::Inventory(gate) => gate.configured_scope(),
        }
    }

    /// Inventory reads never call Notion.
    pub async fn authorize(&self, page: &PageId) -> Result<ScopePermit, ScopeError> {
        match self {
            Self::Inventory(gate) => gate.authorize(page),
        }
    }
}

impl ReadScopeGate {
    pub fn new(membership: Arc<dyn Membership>, scope: LifecycleScope) -> Result<Self, ScopeError> {
        if !valid_scope(&scope) {
            return Err(ScopeError::InvalidConfiguration);
        }
        Ok(Self { membership, scope })
    }

    /// Trust roots only from this server-constructed policy.
    pub fn configured_scope(&self) -> &LifecycleScope {
        &self.scope
    }

    /// A direct page ID is not proof of authority: check the page's recorded
    /// chain against the current roots and exclusions. No network.
    pub fn authorize(&self, page: &PageId) -> Result<ScopePermit, ScopeError> {
        let exclusions = &self.scope.exclusions;
        if !canonical_id(&page.0) || exclusions.source_types.contains(&SourceType::Page) {
            return Err(ScopeError::Denied);
        }
        let chain = self
            .membership
            .parent_chain(&page.0)
            .map_err(|_| ScopeError::Unverifiable)?
            .ok_or(ScopeError::Denied)?;
        if chain.first().is_none_or(|entry| entry.page_id != page.0) {
            return Err(ScopeError::Unverifiable);
        }
        for (index, entry) in chain.iter().enumerate() {
            // An unreadable (404/403) or trashed link vouches for nothing below it.
            if entry.in_trash
                || entry.last_edited_time.is_empty()
                || exclusions.page_ids.contains(&entry.page_id)
                || (index > 0 && exclusions.descendants_of.contains(&entry.page_id))
            {
                return Err(ScopeError::Denied);
            }
            let next = chain.get(index + 1).map(|next| next.page_id.as_str());
            match &entry.parent {
                InventoryParent::Root => {
                    if next.is_some() {
                        return Err(ScopeError::Unverifiable);
                    }
                    if !self.scope.roots.iter().any(|root| root.0 == entry.page_id) {
                        return Err(ScopeError::Denied);
                    }
                    return Ok(ScopePermit {
                        roots: vec![entry.page_id.clone()],
                    });
                }
                InventoryParent::Page(parent) => {
                    if next != Some(parent.as_str()) {
                        return Err(ScopeError::Unverifiable);
                    }
                }
                InventoryParent::DataSource {
                    data_source_id,
                    database_id,
                    owner_page_id,
                } => {
                    if exclusions
                        .exclusion(data_source_id, SourceType::DataSource)
                        .or_else(|| exclusions.exclusion(database_id, SourceType::Database))
                        .is_some()
                    {
                        return Err(ScopeError::Denied);
                    }
                    if next != Some(owner_page_id.as_str()) {
                        return Err(ScopeError::Unverifiable);
                    }
                }
            }
        }
        Err(ScopeError::Unverifiable)
    }
}

/// An in-memory inventory keyed by page ID, for compositions and tests that
/// hold the walk's entries directly.
impl Membership for std::collections::BTreeMap<String, InventoryEntry> {
    fn parent_chain(&self, page: &str) -> Result<Option<Vec<InventoryEntry>>, ScopeError> {
        let mut chain: Vec<InventoryEntry> = Vec::new();
        let mut current = page;
        loop {
            let Some(entry) = self.get(current) else {
                return Ok(None);
            };
            if chain.iter().any(|seen| seen.page_id == entry.page_id) {
                return Ok(None);
            }
            chain.push(entry.clone());
            current = match &entry.parent {
                InventoryParent::Root => return Ok(Some(chain)),
                InventoryParent::Page(id) => id,
                InventoryParent::DataSource { owner_page_id, .. } => owner_page_id,
            };
        }
    }
}
