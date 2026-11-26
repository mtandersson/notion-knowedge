//! Validated, deny-first operation allowlist/blocklist for every MCP transport (#86).
//!
//! Never trust requested operation names as authority. Unknown capabilities
//! are not runnable or advertised, even when the operator allows a category.
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyListError {
    Allowlist,
    Blocklist,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Category {
    Read,
    Write,
}

/// Tool names plus the policy-only selectors `knowledge_query_view`,
/// `knowledge_create_row`, `knowledge_update_row` and `knowledge_append`, which
/// govern operations of `knowledge_get`, `knowledge_create_page` and
/// `knowledge_update_page` but are not callable tools.
const OPERATIONS: [(&str, Category); 14] = [
    ("knowledge_search", Category::Read),
    ("knowledge_get", Category::Read),
    ("knowledge_related", Category::Read),
    ("knowledge_sync_status", Category::Read),
    (crate::view::SELECTOR, Category::Read),
    ("knowledge_upload_file", Category::Write),
    ("knowledge_file_refresh", Category::Write),
    ("knowledge_create_page", Category::Write),
    ("knowledge_update_page", Category::Write),
    (crate::row_write::CREATE_SELECTOR, Category::Write),
    (crate::row_write::UPDATE_SELECTOR, Category::Write),
    ("knowledge_append", Category::Write),
    ("knowledge_update_section", Category::Write),
    ("knowledge_archive_page", Category::Write),
];

fn known(name: &str) -> Option<Category> {
    OPERATIONS
        .iter()
        .find(|(operation, _)| *operation == name)
        .map(|(_, category)| *category)
}

fn valid_selector(selector: &str) -> bool {
    matches!(selector, "read" | "write" | "admin") || known(selector).is_some()
}

/// The allowlist is unrestricted when absent; an explicitly empty allowlist
/// disables all tools. Blocklist always wins. Neither list can add a tool.
#[derive(Clone, Debug, Default)]
pub struct CapabilityPolicy {
    allowlist: Option<BTreeSet<String>>,
    blocklist: BTreeSet<String>,
}

impl CapabilityPolicy {
    fn parse_list(value: &str) -> Option<BTreeSet<String>> {
        if value.len() > 2048 {
            return None;
        }
        if value.is_empty() {
            return Some(BTreeSet::new());
        }
        let mut entries = BTreeSet::new();
        for item in value.split(',') {
            let item = item.trim();
            if !valid_selector(item) {
                return None;
            }
            entries.insert(item.to_owned());
        }
        Some(entries)
    }

    pub fn from_lists(
        allowlist: Option<&str>,
        blocklist: Option<&str>,
    ) -> Result<Self, PolicyListError> {
        let allowlist = allowlist
            .map(Self::parse_list)
            .transpose_option()
            .ok_or(PolicyListError::Allowlist)?;
        let blocklist = blocklist
            .map(Self::parse_list)
            .transpose_option()
            .ok_or(PolicyListError::Blocklist)?
            .unwrap_or_default();
        Ok(Self {
            allowlist,
            blocklist,
        })
    }

    /// A merged tool is reachable when its own selector, or the selector of any
    /// operation it subsumes, is allowed, so existing allowlists keep working.
    /// A blocklisted merged name or category still blocks it. Each call is then
    /// checked against its own operation's selector.
    pub fn permits_via(&self, operation: &str, subsumed: &[&str]) -> bool {
        let Some(category) = known(operation) else {
            return false;
        };
        let category = match category {
            Category::Read => "read",
            Category::Write => "write",
        };
        if self.blocklist.contains(operation) || self.blocklist.contains(category) {
            return false;
        }
        self.allowlist.as_ref().is_none_or(|allow| {
            allow.contains(operation)
                || allow.contains(category)
                || subsumed.iter().any(|name| self.permits(name))
        })
    }

    /// Fail closed on all unregistered operations. Blocklist takes priority
    /// over allowlist, including category-vs-exact-name combinations.
    pub fn permits(&self, operation: &str) -> bool {
        let Some(category) = known(operation) else {
            return false;
        };
        let category = match category {
            Category::Read => "read",
            Category::Write => "write",
        };
        if self.blocklist.contains(operation) || self.blocklist.contains(category) {
            return false;
        }
        self.allowlist
            .as_ref()
            .is_none_or(|allow| allow.contains(operation) || allow.contains(category))
    }
}

/// Convert an Option<Option<T>> from conditional parsing into the desired
/// Option<T>, without treating malformed user input as an absent setting.
trait TransposeOption<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}

impl<T> TransposeOption<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            Some(Some(value)) => Some(Some(value)),
            Some(None) => None,
            None => Some(None),
        }
    }
}
