//! Trusted operator scope: workspace, configured roots and exclusions.
use crate::{backend::PageId, discovery::ExclusionRules};

/// Trusted composition configuration, never copied from a webhook or page response.
/// Workspace identity binds the credential externally: Notion's workspace parent
/// carries only `true`, and cannot prove a workspace UUID itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LifecycleScope {
    pub workspace_id: String,
    pub generation: u64,
    pub roots: Vec<PageId>,
    pub exclusions: ExclusionRules,
}
