//! Provider-independent aliases for canonical Notion page identities (#64).
//! Only explicitly configured normalized metadata fields are inspected: never body text.

use std::collections::BTreeSet;

use crate::{
    indexed::{IndexedMetadata, PropertyValue},
    sync_state::{SyncStateError, validate_identifier},
};

const MAX_NAME_CHARS: usize = 256;
const MAX_PAGE_ALIASES: usize = 128;
const MAX_PROPERTY_IDS: usize = 32;

/// One source of an alternative page name. Identifiers and names cannot be
/// rewritten by query callers into a new canonical Notion page identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageAlias {
    key: String,
    display: String,
    provenance: String,
}

impl PageAlias {
    pub fn new(name: &str, provenance: &str) -> Result<Self, SyncStateError> {
        validate_identifier(provenance)?;
        let key = normalized(name)?;
        Ok(Self {
            key,
            display: name.trim().to_owned(),
            provenance: provenance.to_owned(),
        })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn display(&self) -> &str {
        &self.display
    }

    pub fn provenance(&self) -> &str {
        &self.provenance
    }
}

/// Stable whitespace-folded, Unicode-lowercase match key. Non-ASCII names
/// such as Swedish å, ä, ö are preserved, not transliterated or discarded.
/// An empty/oversized/unsafe query is an error, not a match-all lookup.
pub fn normalized(name: &str) -> Result<String, SyncStateError> {
    if name.chars().any(char::is_control) {
        return Err(SyncStateError::InvalidInput);
    }
    let collapsed = name.split_whitespace().collect::<Vec<_>>().join(" ");
    let key = collapsed.to_lowercase();
    if key.is_empty() || key.chars().count() > MAX_NAME_CHARS {
        return Err(SyncStateError::InvalidInput);
    }
    Ok(key)
}

/// Explicitly configured sources. Property keys are stable normalized Notion
/// property IDs, never display labels or substring matching.
#[derive(Debug, Clone)]
pub struct AliasSources {
    title: bool,
    property_ids: BTreeSet<String>,
}

impl AliasSources {
    pub fn new(title: bool, property_ids: Vec<String>) -> Result<Self, SyncStateError> {
        if property_ids.len() > MAX_PROPERTY_IDS {
            return Err(SyncStateError::InvalidInput);
        }
        let mut ids = BTreeSet::new();
        for id in property_ids {
            validate_identifier(&id)?;
            if id.len() > 128 {
                return Err(SyncStateError::InvalidInput);
            }
            ids.insert(id);
        }
        if !title && ids.is_empty() {
            return Err(SyncStateError::InvalidInput);
        }
        Ok(Self { title, property_ids: ids })
    }

    /// Extract only text/string-valued configured properties; numeric, relation
    /// IDs, dates and people are never guessed as alternate names.
    pub fn extract(&self, metadata: &IndexedMetadata) -> Result<Vec<PageAlias>, SyncStateError> {
        validate_identifier(&metadata.page_id)?;
        validate_identifier(&metadata.source.workspace_id)?;
        validate_identifier(&metadata.source.root_page_id)?;
        let mut aliases = BTreeSet::new();
        let mut insert = |value: &str, source: &str| -> Result<(), SyncStateError> {
            if value.trim().is_empty() {
                return Ok(());
            }
            aliases.insert(PageAlias::new(value, source)?);
            if aliases.len() > MAX_PAGE_ALIASES {
                return Err(SyncStateError::InvalidInput);
            }
            Ok(())
        };
        if self.title {
            insert(&metadata.title, "title")?;
        }
        for id in &self.property_ids {
            let source = format!("property:{id}");
            match metadata.properties.get(id) {
                Some(PropertyValue::Text(value)) => insert(value, &source)?,
                Some(PropertyValue::Strings(values)) => {
                    for value in values {
                        insert(value, &source)?;
                    }
                }
                _ => {}
            }
        }
        Ok(aliases.into_iter().collect())
    }
}

/// Rebuildable derived index port. Caller must ensure the provided namespace
/// is the authoritative workspace and physical allowed root before invoking it.
pub trait PageAliasStore: Send + Sync {
    /// Sync directly from an already authorized, normalized indexed page.
    /// Failed extraction occurs *before* any old mapping is removed.
    fn replace_indexed_aliases(
        &self,
        metadata: &IndexedMetadata,
        sources: &AliasSources,
    ) -> Result<(), SyncStateError> {
        let names = sources.extract(metadata)?;
        self.replace_page_aliases(
            &metadata.source.workspace_id,
            &metadata.source.root_page_id,
            &metadata.page_id,
            &names,
        )
    }

    /// Atomically replaces all aliases for exactly this page in its trusted
    /// workspace/root namespace, including empty replacement (tombstone).
    fn replace_page_aliases(
        &self,
        workspace_id: &str,
        root_page_id: &str,
        page_id: &str,
        aliases: &[PageAlias],
    ) -> Result<(), SyncStateError>;

    /// Returns sorted, DISTINCT canonical page IDs. Never choose among
    /// ambiguous candidates or fall back to fuzzy/unscoped matches.
    fn lookup_alias(
        &self,
        workspace_id: &str,
        root_page_id: &str,
        alias: &str,
    ) -> Result<Vec<String>, SyncStateError>;
}
