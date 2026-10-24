//! Complete derived alias effects; proposals confer no source authority.
use super::*;
use notion_knowledge_core::{
    aliases::{AliasSources, PageAlias},
    indexed::IndexedMetadata,
};
use sha2::{Digest, Sha256};

const VERSION: i64 = 1;
const MAX_COMPATIBLE_GENERATIONS: i64 = 1024;
const MAX_COMPATIBILITY_HASH_BYTES: usize = 64 * 1024 * 1024;

/// Explicit offline transition maintenance, never a serving-time schema write.
pub(super) fn ensure_history_index(db: &Connection) -> Result<(), CommitError> {
    db.execute_batch("CREATE INDEX IF NOT EXISTS main.index_commit_generations_policy_epoch ON index_commit_generations(scope,epoch)").map_err(sql)?;
    validate_history_index(db)
}
pub(super) fn validate_history_index(db: &Connection) -> Result<(), CommitError> {
    let complete: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM pragma_index_list('index_commit_generations','main') WHERE name='index_commit_generations_policy_epoch' AND partial=0)", [], |r|r.get(0)).map_err(sql)?;
    let mut statement = db
        .prepare("SELECT name,coll FROM pragma_index_xinfo('index_commit_generations_policy_epoch','main') WHERE key=1 ORDER BY seqno LIMIT 3")
        .map_err(sql)?;
    let mut columns = statement.query([]).map_err(sql)?;
    for expected in ["scope", "epoch"] {
        let row = columns
            .next()
            .map_err(sql)?
            .ok_or(CommitError::BindingMismatch)?;
        if row.get::<_, String>(0).map_err(sql)? != expected
            || row.get::<_, String>(1).map_err(sql)? != "BINARY"
        {
            return Err(CommitError::BindingMismatch);
        }
    }
    if !complete || columns.next().map_err(sql)?.is_some() {
        return Err(CommitError::BindingMismatch);
    }
    Ok(())
}

/// Exact replacement in one trusted workspace/root namespace. Configuration,
/// empty extraction and tombstone status are all part of the durable effect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasProjection {
    binding: CommitBinding,
    root: String,
    page: String,
    tombstone: bool,
    clean_owned_roots: bool,
    configuration: String,
    aliases: Vec<PageAlias>,
    effect: String,
}
impl AliasProjection {
    /// Extract before beginning any operation or removing existing aliases.
    pub fn from_indexed(
        binding: &CommitBinding,
        metadata: &IndexedMetadata,
        sources: &AliasSources,
    ) -> Result<Self, CommitError> {
        if metadata.source.workspace_id != binding.workspace {
            return Err(CommitError::BindingMismatch);
        }
        let aliases = sources
            .extract(metadata)
            .map_err(|_| CommitError::InvalidInput)?;
        // Match PageAliasStore's canonical key/provenance uniqueness. Extraction
        // is ordered, so equivalent spellings retain the same first display.
        let mut seen = std::collections::BTreeSet::new();
        let aliases = aliases
            .into_iter()
            .filter(|a| seen.insert((a.key().to_owned(), a.provenance().to_owned())))
            .collect();
        Self::new(
            binding,
            &metadata.source.root_page_id,
            &metadata.page_id,
            false,
            sources,
            aliases,
        )
    }
    /// Build from current exact authoritative metadata after lifecycle validation.
    pub fn from_page(
        binding: &CommitBinding,
        page: &notion_knowledge_core::backend::Page,
        root: &str,
        sources: &AliasSources,
    ) -> Result<Self, CommitError> {
        Self::from_indexed(
            binding,
            &IndexedMetadata {
                page_id: page.id.0.clone(),
                block_id: None,
                url: page.url.clone(),
                title: page.title.clone(),
                heading_path: vec![],
                last_edited_time: page.last_edited_time.clone(),
                source: notion_knowledge_core::indexed::SourceMetadata {
                    workspace_id: binding.workspace.clone(),
                    root_page_id: root.into(),
                    database_id: None,
                    data_source_id: None,
                },
                properties: page.properties.clone(),
            },
            sources,
        )
    }
    pub fn tombstone(
        binding: &CommitBinding,
        root: &str,
        page: &str,
        sources: &AliasSources,
    ) -> Result<Self, CommitError> {
        Self::new(binding, root, page, true, sources, vec![])
    }
    fn new(
        binding: &CommitBinding,
        root: &str,
        page: &str,
        tombstone: bool,
        sources: &AliasSources,
        aliases: Vec<PageAlias>,
    ) -> Result<Self, CommitError> {
        validate(page).map_err(|_| CommitError::InvalidInput)?;
        if !binding.roots.iter().any(|r| r == root) {
            return Err(CommitError::BindingMismatch);
        }
        let configuration = sources.identity();
        let mut projection = Self {
            binding: binding.clone(),
            root: root.into(),
            page: page.into(),
            tombstone,
            clean_owned_roots: false,
            configuration,
            aliases,
            effect: String::new(),
        };
        projection.effect = projection.identity();
        Ok(projection)
    }
    /// Installed physical-root transitions remove stale aliases from other
    /// roots owned by this same binding, preserving every external namespace.
    pub fn with_owned_root_cleanup(mut self) -> Self {
        self.clean_owned_roots = true;
        self.effect = self.identity();
        self
    }
    pub fn page_id(&self) -> &str {
        &self.page
    }
    pub fn root_page_id(&self) -> &str {
        &self.root
    }
    pub fn configuration_identity(&self) -> &str {
        &self.configuration
    }
    pub fn aliases(&self) -> &[PageAlias] {
        &self.aliases
    }
    fn identity(&self) -> String {
        self.identity_for_generation(&self.binding.generation)
    }
    fn identity_for_generation(&self, generation: &str) -> String {
        let mut hash = Sha256::new();
        // Length-prefix every field: no ambiguous concatenation or plaintext
        // source names in receipts/proposal identities.
        for field in [
            self.binding.workspace.as_str(),
            self.binding.scope.as_str(),
            generation,
            self.root.as_str(),
            self.page.as_str(),
            self.configuration.as_str(),
        ] {
            hash.update((field.len() as u64).to_le_bytes());
            hash.update(field);
        }
        hash.update([u8::from(self.tombstone)]);
        if self.clean_owned_roots {
            hash.update(b"owned-root-cleanup-v1");
        }
        for alias in &self.aliases {
            for field in [alias.key(), alias.display(), alias.provenance()] {
                hash.update((field.len() as u64).to_le_bytes());
                hash.update(field);
            }
        }
        format!("alias-v1:{:x}", hash.finalize())
    }
    /// Adopted receipts retain their exact old proposal hashes. Only the
    /// physical generation may vary; current receipt/marker fences are checked
    /// by callers before this comparison. No stored hash grants source access.
    fn matches_effect(&self, db: &Connection, effect: &str) -> Result<bool, CommitError> {
        if effect == self.effect {
            return Ok(true);
        }
        if effect.len() != 73 || !effect.starts_with("alias-v1:") {
            return Ok(false);
        }
        let indexed: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='index' AND name='index_commit_generations_policy_epoch')", [], |r|r.get(0)).map_err(sql)?;
        if !indexed {
            return Ok(false);
        }
        validate_history_index(db)?;
        let count: i64 = db.query_row("SELECT COUNT(*) FROM (SELECT 1 FROM main.index_commit_generations INDEXED BY index_commit_generations_policy_epoch WHERE scope=?1 AND epoch<?2 LIMIT ?3)", params![self.binding.scope,self.binding.epoch,MAX_COMPATIBLE_GENERATIONS+1], |r|r.get(0)).map_err(sql)?;
        if count > MAX_COMPATIBLE_GENERATIONS {
            return Err(CommitError::ProjectionRepairRequired);
        }
        let mut statement = db.prepare("SELECT CASE WHEN length(CAST(generation AS BLOB)) BETWEEN 1 AND 4096 THEN generation ELSE NULL END FROM main.index_commit_generations INDEXED BY index_commit_generations_policy_epoch WHERE scope=?1 AND epoch<?2 ORDER BY epoch DESC LIMIT ?3").map_err(sql)?;
        let mut charged = 0usize;
        for generation in statement
            .query_map(
                params![
                    self.binding.scope,
                    self.binding.epoch,
                    MAX_COMPATIBLE_GENERATIONS
                ],
                |r| r.get::<_, String>(0),
            )
            .map_err(sql)?
        {
            let generation = generation.map_err(sql)?;
            validate(&generation).map_err(|_| CommitError::ProjectionRepairRequired)?;
            let fields = [
                &self.binding.workspace[..],
                &self.binding.scope,
                &generation,
                &self.root,
                &self.page,
                &self.configuration,
            ];
            let mut bytes = 1usize
                + if self.clean_owned_roots {
                    b"owned-root-cleanup-v1".len()
                } else {
                    0
                };
            for field in fields.into_iter().chain(
                self.aliases
                    .iter()
                    .flat_map(|alias| [alias.key(), alias.display(), alias.provenance()]),
            ) {
                bytes = bytes
                    .checked_add(8)
                    .and_then(|n| n.checked_add(field.len()))
                    .ok_or(CommitError::ProjectionRepairRequired)?;
            }
            charged = charged
                .checked_add(bytes)
                .ok_or(CommitError::ProjectionRepairRequired)?;
            if charged > MAX_COMPATIBILITY_HASH_BYTES {
                return Err(CommitError::ProjectionRepairRequired);
            }
            if self.identity_for_generation(&generation) == effect {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(super) fn matches_operation(
        &self,
        binding: &CommitBinding,
        operation: &PageOperation,
    ) -> bool {
        self.binding == *binding
            && self.page == operation.checkpoint.page_id()
            && self.tombstone == operation.checkpoint.is_tombstone()
    }
    pub(super) fn complete(&self, db: &Connection) -> Result<bool, CommitError> {
        let marker: Option<(String, i64, String)> = db
            .query_row(
                "SELECT generation,version,effect FROM page_alias_projections WHERE page_id=?1",
                [&self.page],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(sql)?;
        let Some((generation, version, effect)) = marker else {
            return Ok(false);
        };
        if generation != self.binding.generation
            || version != VERSION
            || !self.matches_effect(db, &effect)?
        {
            return Ok(false);
        }
        let checkpoint: Option<bool> = db
            .query_row(
                "SELECT tombstoned FROM page_sync_state WHERE page_id=?1",
                [&self.page],
                |r| r.get(0),
            )
            .optional()
            .map_err(sql)?;
        if checkpoint != Some(self.tombstone) {
            return Ok(false);
        }
        let mut statement = db.prepare("SELECT alias_key,display_name,provenance FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3 ORDER BY alias_key,display_name,provenance").map_err(sql)?;
        let actual = statement
            .query_map(params![self.binding.workspace, self.root, self.page], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })
            .map_err(sql)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(sql)?;
        let expected: Vec<_> = self
            .aliases
            .iter()
            .map(|a| {
                (
                    a.key().to_owned(),
                    a.display().to_owned(),
                    a.provenance().to_owned(),
                )
            })
            .collect();
        if self.clean_owned_roots {
            for root in &self.binding.roots {
                if root != &self.root && db.query_row("SELECT EXISTS(SELECT 1 FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3)", params![self.binding.workspace,root,self.page],|r|r.get::<_,bool>(0)).map_err(sql)? { return Ok(false); }
            }
        }
        Ok(actual == expected)
    }
    pub(super) fn install(&self, db: &Connection) -> Result<(), CommitError> {
        db.execute(
            "DELETE FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3",
            params![self.binding.workspace, self.root, self.page],
        )
        .map_err(sql)?;
        if self.clean_owned_roots {
            for root in &self.binding.roots {
                if root != &self.root {
                    db.execute("DELETE FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3",params![self.binding.workspace,root,self.page]).map_err(sql)?;
                }
            }
        }
        for alias in &self.aliases {
            db.execute(
                "INSERT INTO graph_aliases VALUES(?1,?2,?3,?4,?5,?6)",
                params![
                    self.binding.workspace,
                    self.root,
                    self.page,
                    alias.key(),
                    alias.display(),
                    alias.provenance()
                ],
            )
            .map_err(sql)?;
        }
        db.execute("INSERT INTO page_alias_projections VALUES(?1,?2,?3,?4) ON CONFLICT(page_id) DO UPDATE SET generation=excluded.generation,version=excluded.version,effect=excluded.effect", params![self.page,self.binding.generation,VERSION,self.effect]).map_err(sql)?;
        Ok(())
    }
}
fn proposal(db: &Connection, id: &str) -> Result<Option<String>, CommitError> {
    db.query_row(
        "SELECT effect FROM alias_operation_proposals WHERE operation_id=?1",
        [id],
        |r| r.get(0),
    )
    .optional()
    .map_err(sql)
}
fn repair_prefix(original: &str) -> String {
    format!("alias-repair:{:x}:", Sha256::digest(original.as_bytes()))
}
fn unfinished(db: &Connection, original: &str) -> Result<bool, CommitError> {
    let prefix = repair_prefix(original);
    db.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE operation_id>=?1 AND operation_id<?2 AND status IN(0,2))", params![prefix,format!("{prefix}~")], |r|r.get(0)).map_err(sql)
}
pub(super) fn validate_applied(
    db: &Connection,
    operation: &PageOperation,
) -> Result<(), CommitError> {
    let previous = proposal(db, &operation.id)?;
    if !proposal_matches(db, operation, previous.as_deref())? || unfinished(db, &operation.id)? {
        return Err(CommitError::ProjectionRepairRequired);
    }
    if let Some(projection) = &operation.alias_projection
        && !projection.complete(db)?
    {
        return Err(CommitError::ProjectionRepairRequired);
    }
    Ok(())
}
fn proposal_matches(
    db: &Connection,
    operation: &PageOperation,
    previous: Option<&str>,
) -> Result<bool, CommitError> {
    match (&operation.alias_projection, previous) {
        (None, None) => Ok(true),
        (Some(projection), Some(effect)) => {
            let current: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM index_commit_receipts WHERE operation_id=?1 AND generation=?2 AND page_id=?3 AND status IN(0,1,2))", params![operation.id,projection.binding.generation,projection.page], |r|r.get(0)).map_err(sql)?;
            Ok(current && projection.matches_effect(db, effect)?)
        }
        _ => Ok(false),
    }
}
pub(super) fn persist_proposal(
    db: &Connection,
    operation: &PageOperation,
    existing: bool,
) -> Result<(), CommitError> {
    let previous = proposal(db, &operation.id)?;
    let expected = operation
        .alias_projection
        .as_ref()
        .map(|p| p.effect.as_str());
    let matches = proposal_matches(db, operation, previous.as_deref())?;
    if matches {
        return Ok(());
    }
    if !matches && existing && !(operation.retry_latest_refresh || operation.retry_latest_lifecycle)
    {
        return Err(CommitError::Conflict);
    }
    db.execute(
        "DELETE FROM alias_operation_proposals WHERE operation_id=?1",
        [&operation.id],
    )
    .map_err(sql)?;
    if let Some(effect) = expected {
        db.execute(
            "INSERT INTO alias_operation_proposals VALUES(?1,?2)",
            params![operation.id, effect],
        )
        .map_err(sql)?;
    }
    Ok(())
}
impl IndexCommitCoordinator {
    /// Check the exact current stored rows and configuration, including empty
    /// and tombstoned pages. Existing derived rows never confer source authority.
    pub fn current_alias_projection_complete(
        &self,
        page: &str,
        sources: &AliasSources,
    ) -> Result<bool, CommitError> {
        self.identities()?;
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db.transaction().map_err(sql)?;
        self.validate_binding(&tx)?;
        let result = current_complete(&tx, &self.binding, page, sources)?;
        tx.commit().map_err(sql)?;
        Ok(result)
    }
    /// Trusted local composition seam. Names resolve to all distinct scoped
    /// IDs; they grant no content access. Pending effects or incompatible source
    /// configuration fail closed under the same current snapshot read guard.
    #[cfg(feature = "local-lancedb")]
    pub fn lookup_canonical_alias(
        &self,
        sources: &AliasSources,
        alias: &str,
    ) -> Result<Vec<String>, CommitError> {
        use notion_knowledge_core::aliases::PageAliasStore;
        let _guard = self.search_guard()?;
        let owned: std::collections::BTreeSet<_> = self.owned_page_ids()?.into_iter().collect();
        for page in &owned {
            if !self.current_alias_projection_complete(page, sources)? {
                return Err(CommitError::ProjectionRepairRequired);
            }
        }
        let mut candidates = std::collections::BTreeSet::new();
        for root in &self.binding.roots {
            candidates.extend(
                self.state
                    .lookup_alias(&self.binding.workspace, root, alias)
                    .map_err(|_| CommitError::InvalidInput)?,
            );
        }
        if candidates.iter().any(|page| !owned.contains(page)) {
            return Err(CommitError::ProjectionRepairRequired);
        }
        Ok(candidates.into_iter().collect())
    }
    /// Select only a root already owned by this immutable binding. Used to
    /// remove aliases when current negative lifecycle evidence has no allowed root.
    pub fn alias_root(&self, page: &str) -> Result<String, CommitError> {
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        for root in &self.binding.roots {
            let exists: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3)", params![self.binding.workspace,root,page],|r|r.get(0)).map_err(sql)?;
            if exists {
                return Ok(root.clone());
            }
        }
        self.binding
            .roots
            .first()
            .cloned()
            .ok_or(CommitError::BindingMismatch)
    }
    /// Exact compatible-state read; never repairs, migrates or acquires a lease.
    pub fn alias_projection_complete(
        &self,
        projection: &AliasProjection,
    ) -> Result<bool, CommitError> {
        self.identities()?;
        if projection.binding != self.binding {
            return Err(CommitError::BindingMismatch);
        }
        let mut db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        let tx = db.transaction().map_err(sql)?;
        self.validate_binding(&tx)?;
        let result = projection.complete(&tx)?;
        tx.commit().map_err(sql)?;
        Ok(result)
    }
    /// Resolve the durable unfinished repair before checking a reverted source.
    /// Both the original and repair IDs must retain current page authority.
    pub fn alias_repair_id(&self, original: &str, page: &str) -> Result<String, CommitError> {
        validate(original).map_err(|_| CommitError::InvalidInput)?;
        validate(page).map_err(|_| CommitError::InvalidInput)?;
        self.identities()?;
        let db = self
            .state
            .lock_connection()
            .map_err(|_| CommitError::Unavailable)?;
        self.validate_binding(&db)?;
        let valid: bool = db.query_row("SELECT generation=?2 AND page_id=?3 AND status=1 FROM index_commit_receipts WHERE operation_id=?1",params![original,self.binding.generation,page],|r|r.get(0)).optional().map_err(sql)?.unwrap_or(false);
        if !valid {
            return Err(CommitError::Conflict);
        }
        let prefix = repair_prefix(original);
        let mut query = db.prepare("SELECT operation_id,status,generation=?3 AND page_id=?4 FROM index_commit_receipts WHERE operation_id>=?1 AND operation_id<?2").map_err(sql)?;
        let mut latest = None;
        for row in query
            .query_map(
                params![prefix, format!("{prefix}~"), self.binding.generation, page],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, bool>(2)?,
                    ))
                },
            )
            .map_err(sql)?
        {
            let (id, status, authority) = row.map_err(sql)?;
            if !authority {
                return Err(CommitError::Conflict);
            }
            let sequence = id[prefix.len()..]
                .parse::<u64>()
                .map_err(|_| CommitError::Conflict)?;
            if latest.is_none_or(|(last, _)| sequence > last) {
                latest = Some((sequence, status));
            }
        }
        let next = match latest {
            Some((n, 0 | 2)) => n,
            Some((n, _)) => n.checked_add(1).ok_or(CommitError::Conflict)?,
            None => 0,
        };
        Ok(format!("{prefix}{next}"))
    }
    pub fn unfinished_alias_repair_id(
        &self,
        original: &str,
        page: &str,
    ) -> Result<Option<String>, CommitError> {
        let id = self.alias_repair_id(original, page)?;
        Ok(self
            .receipt(&id)?
            .filter(|r| matches!(r.status, ReceiptStatus::Pending | ReceiptStatus::Failed))
            .map(|_| id))
    }
}

fn current_complete(
    db: &Connection,
    binding: &CommitBinding,
    page: &str,
    sources: &AliasSources,
) -> Result<bool, CommitError> {
    let tombstone: Option<bool> = db
        .query_row(
            "SELECT tombstoned FROM page_sync_state WHERE page_id=?1",
            [page],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    let Some(tombstone) = tombstone else {
        return Ok(false);
    };
    for root in &binding.roots {
        let mut statement = db.prepare("SELECT display_name,provenance FROM graph_aliases WHERE workspace_id=?1 AND root_page_id=?2 AND page_id=?3 ORDER BY alias_key,display_name,provenance").map_err(sql)?;
        let aliases = statement
            .query_map(params![binding.workspace, root, page], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(sql)?
            .map(|r| {
                let (name, provenance) = r.map_err(sql)?;
                PageAlias::new(&name, &provenance).map_err(|_| CommitError::InvalidInput)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let projection = AliasProjection::new(binding, root, page, tombstone, sources, aliases)?;
        if projection.clone().with_owned_root_cleanup().complete(db)? || projection.complete(db)? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[test]
    fn historical_alias_hashing_enforces_total_byte_budget() {
        let store = SqliteSyncStateStore::open_in_memory().unwrap();
        let db = store.lock_connection().unwrap();
        ensure_history_index(&db).unwrap();
        let policy =
            ReconciliationScope::new(vec!["root".into()], vec![], "policy", "source").unwrap();
        let binding = CommitBinding::new("chunks", "workspace", &policy, "target", 3000).unwrap();
        for epoch in 1..=501 {
            db.execute(
                "INSERT INTO index_commit_generations VALUES(?1,?2,?3)",
                params![format!("generation-{epoch}"), policy.identity(), epoch],
            )
            .unwrap();
        }
        let aliases = (0..128)
            .map(|i| {
                PageAlias::new(&format!("{i:03}{}", "𐐀".repeat(250)), "property:names").unwrap()
            })
            .collect();
        let projection = AliasProjection::new(
            &binding,
            "root",
            "page",
            false,
            &AliasSources::new(false, vec!["names".into()]).unwrap(),
            aliases,
        )
        .unwrap();
        let old = projection.identity_for_generation("generation-1");
        assert_eq!(
            projection.matches_effect(&db, &old),
            Err(CommitError::ProjectionRepairRequired)
        );
        assert_eq!(projection.matches_effect(&db, &projection.effect), Ok(true));
    }
}
