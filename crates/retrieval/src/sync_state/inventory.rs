//! Store methods for the disposable `page_inventory` table.
use super::{SqliteSyncStateStore, map_sqlite_error};
use notion_knowledge_core::{
    discovery::SkipReason,
    inventory::{
        InventoryCounts, InventoryDiff, InventoryEntry, InventoryOutcome, InventoryParent,
    },
    reconciliation::FailureClass,
    sync_state::SyncStateError,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use std::collections::BTreeSet;

/// Parent chains longer than this are treated as unplaced.
const MAX_CHAIN: usize = 10_000;

type ParentColumns<'a> = (
    &'static str,
    Option<&'a str>,
    Option<&'a str>,
    Option<&'a str>,
);

fn parent_columns(parent: &InventoryParent) -> ParentColumns<'_> {
    match parent {
        InventoryParent::Root => ("root", None, None, None),
        InventoryParent::Page(id) => ("page", Some(id), None, None),
        InventoryParent::DataSource {
            data_source_id,
            database_id,
            owner_page_id,
        } => (
            "data_source",
            Some(data_source_id),
            Some(database_id),
            Some(owner_page_id),
        ),
    }
}

fn read_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<Option<InventoryEntry>> {
    let kind: String = row.get(1)?;
    let parent_id: Option<String> = row.get(2)?;
    let database_id: Option<String> = row.get(3)?;
    let owner: Option<String> = row.get(7)?;
    let parent = match (kind.as_str(), parent_id, database_id, owner) {
        ("root", None, None, None) => InventoryParent::Root,
        ("page", Some(id), None, None) => InventoryParent::Page(id),
        ("data_source", Some(data_source_id), Some(database_id), Some(owner_page_id)) => {
            InventoryParent::DataSource {
                data_source_id,
                database_id,
                owner_page_id,
            }
        }
        _ => return Ok(None),
    };
    Ok(Some(InventoryEntry {
        page_id: row.get(0)?,
        parent,
        root_id: row.get(4)?,
        last_edited_time: row.get(5)?,
        in_trash: row.get(6)?,
    }))
}

const SELECT: &str = "SELECT page_id, parent_kind, parent_id, database_id, root_id, \
                      last_edited_time, in_trash, database_parent_id FROM page_inventory";

fn entry(db: &Connection, page_id: &str) -> Result<Option<InventoryEntry>, SyncStateError> {
    db.query_row(
        &format!("{SELECT} WHERE page_id = ?1"),
        [page_id],
        read_entry,
    )
    .optional()
    .map_err(map_sqlite_error)?
    .map_or(Ok(None), |entry| {
        entry.ok_or(SyncStateError::CorruptState).map(Some)
    })
}

/// Upsert one entry and classify it against the previous row. Outcomes stay.
fn upsert(
    db: &Connection,
    run_id: &str,
    next: &InventoryEntry,
    diff: &mut InventoryDiff,
) -> Result<(), SyncStateError> {
    let previous = entry(db, &next.page_id)?;
    let id = next.page_id.clone();
    match previous {
        None => diff.added.push(id),
        Some(previous) if previous.parent != next.parent || previous.root_id != next.root_id => {
            diff.moved.push(id)
        }
        Some(previous) if !previous.in_trash && next.in_trash => diff.trashed.push(id),
        Some(previous) if previous != *next => diff.changed.push(id),
        Some(_) => diff.unchanged += 1,
    }
    let (kind, parent_id, database_id, owner) = parent_columns(&next.parent);
    db.execute(
        "INSERT INTO page_inventory (page_id, parent_kind, parent_id, database_id, root_id,
             last_edited_time, in_trash, walk_run, database_parent_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
         ON CONFLICT(page_id) DO UPDATE SET parent_kind = excluded.parent_kind,
             parent_id = excluded.parent_id, database_id = excluded.database_id,
             database_parent_id = excluded.database_parent_id,
             root_id = excluded.root_id, last_edited_time = excluded.last_edited_time,
             in_trash = excluded.in_trash, walk_run = excluded.walk_run",
        params![
            next.page_id,
            kind,
            parent_id,
            database_id,
            next.root_id,
            next.last_edited_time,
            next.in_trash,
            run_id,
            owner
        ],
    )
    .map_err(map_sqlite_error)?;
    Ok(())
}

impl SqliteSyncStateStore {
    /// Replace the inventory with one complete walk in a single transaction.
    /// Rows the walk did not reach are deleted, so callers must pass only
    /// complete walks; use [`Self::upsert_inventory_entry`] otherwise.
    pub fn replace_inventory(
        &self,
        run_id: &str,
        entries: &[InventoryEntry],
    ) -> Result<InventoryDiff, SyncStateError> {
        let mut db = self.lock_connection()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite_error)?;
        let mut diff = InventoryDiff::default();
        let mut reached = BTreeSet::new();
        for next in entries {
            if !reached.insert(next.page_id.as_str()) {
                return Err(SyncStateError::InvalidInput);
            }
            upsert(&tx, run_id, next, &mut diff)?;
        }
        {
            let mut statement = tx
                .prepare("SELECT page_id FROM page_inventory WHERE walk_run != ?1")
                .map_err(map_sqlite_error)?;
            diff.removed = statement
                .query_map([run_id], |row| row.get(0))
                .map_err(map_sqlite_error)?
                .collect::<Result<_, _>>()
                .map_err(map_sqlite_error)?;
        }
        tx.execute("DELETE FROM page_inventory WHERE walk_run != ?1", [run_id])
            .map_err(map_sqlite_error)?;
        tx.commit().map_err(map_sqlite_error)?;
        for ids in [
            &mut diff.added,
            &mut diff.changed,
            &mut diff.moved,
            &mut diff.removed,
            &mut diff.trashed,
        ] {
            ids.sort();
        }
        Ok(diff)
    }

    /// Record one reached page without deleting any other entry.
    pub fn upsert_inventory_entry(
        &self,
        run_id: &str,
        entry: &InventoryEntry,
    ) -> Result<InventoryDiff, SyncStateError> {
        let db = self.lock_connection()?;
        let mut diff = InventoryDiff::default();
        upsert(&db, run_id, entry, &mut diff)?;
        Ok(diff)
    }

    /// Persist one walk. A complete walk replaces the inventory; an incomplete
    /// one only upserts what it reached and never deletes unreached entries.
    pub fn record_walk(
        &self,
        run_id: &str,
        entries: &[InventoryEntry],
        complete: bool,
    ) -> Result<InventoryDiff, SyncStateError> {
        if complete {
            return self.replace_inventory(run_id, entries);
        }
        let mut db = self.lock_connection()?;
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite_error)?;
        let mut diff = InventoryDiff::default();
        for entry in entries {
            upsert(&tx, run_id, entry, &mut diff)?;
        }
        tx.commit().map_err(map_sqlite_error)?;
        Ok(diff)
    }

    /// Unknown pages are ignored: only reached pages carry an outcome.
    pub fn record_inventory_outcome(
        &self,
        page_id: &str,
        outcome: &InventoryOutcome,
        now_unix: i64,
    ) -> Result<(), SyncStateError> {
        let (kind, detail, revision) = match outcome {
            InventoryOutcome::Indexed { revision } => ("indexed", None, Some(revision.as_str())),
            InventoryOutcome::Skipped(reason) => ("skipped", Some(reason.as_str()), None),
            InventoryOutcome::Failed(class) => ("failed", Some(class.as_str()), None),
        };
        self.lock_connection()?
            .execute(
                "UPDATE page_inventory SET outcome = ?2, outcome_detail = ?3,
                     indexed_revision = COALESCE(?4, indexed_revision), last_fetched_at = ?5
                 WHERE page_id = ?1",
                params![page_id, kind, detail, revision, now_unix],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    pub fn inventory_entry(&self, page_id: &str) -> Result<Option<InventoryEntry>, SyncStateError> {
        entry(&*self.lock_connection()?, page_id)
    }

    /// Ancestors from the parent up to the root page, excluding the page
    /// itself. `None` when the chain leaves the inventory or repeats.
    pub fn inventory_parent_chain(
        &self,
        page_id: &str,
    ) -> Result<Option<Vec<String>>, SyncStateError> {
        let db = self.lock_connection()?;
        let mut chain = Vec::new();
        let mut seen = BTreeSet::from([page_id.to_owned()]);
        let mut current = page_id.to_owned();
        loop {
            let Some(next) = entry(&db, &current)? else {
                return Ok(None);
            };
            let parent = match next.parent {
                InventoryParent::Root => return Ok(Some(chain)),
                InventoryParent::Page(id) => id,
                InventoryParent::DataSource { owner_page_id, .. } => owner_page_id,
            };
            if !seen.insert(parent.clone()) || chain.len() >= MAX_CHAIN {
                return Ok(None);
            }
            chain.push(parent.clone());
            current = parent;
        }
    }

    /// Of `candidates` (indexed pages a complete walk did not reach), those to
    /// keep for one more walk: a page of `inaccessible` (404/403 in this
    /// walk) whose previous outcome was not already that skip, and every
    /// candidate whose previous parent chain passes through such a page.
    /// Call before the walk replaces the inventory. A second walk with the
    /// same result finds the skip already recorded and holds nothing.
    pub fn held_removals(
        &self,
        inaccessible: &[String],
        candidates: &[String],
    ) -> Result<BTreeSet<String>, SyncStateError> {
        let mut new = BTreeSet::new();
        {
            let db = self.lock_connection()?;
            for id in inaccessible {
                let confirmed = db
                    .query_row(
                        "SELECT outcome = 'skipped' AND outcome_detail = ?2
                         FROM page_inventory WHERE page_id = ?1",
                        params![id, SkipReason::Inaccessible.as_str()],
                        |row| row.get::<_, Option<bool>>(0),
                    )
                    .optional()
                    .map_err(map_sqlite_error)?
                    .flatten()
                    .unwrap_or(false);
                if !confirmed {
                    new.insert(id.as_str());
                }
            }
        }
        if new.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut held = BTreeSet::new();
        // A database or data source has no row of its own: its rows match by
        // their parent, and after one walk they have left the inventory.
        let under_new = |id: &str| -> Result<bool, SyncStateError> {
            Ok(new.contains(id)
                || matches!(
                    self.inventory_entry(id)?.map(|e| e.parent),
                    Some(InventoryParent::DataSource { data_source_id, database_id, .. })
                        if new.contains(data_source_id.as_str()) || new.contains(database_id.as_str())
                ))
        };
        for page in candidates {
            let mut hold = under_new(page)?;
            for ancestor in self.inventory_parent_chain(page)?.unwrap_or_default() {
                hold = hold || under_new(&ancestor)?;
            }
            if hold {
                held.insert(page.clone());
            }
        }
        Ok(held)
    }

    pub fn inventory_counts(&self) -> Result<InventoryCounts, SyncStateError> {
        self.lock_connection()?
            .query_row(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE outcome = 'indexed'),
                     COUNT(*) FILTER (WHERE outcome = 'skipped'),
                     COUNT(*) FILTER (WHERE outcome = 'failed'),
                     COUNT(*) FILTER (WHERE outcome IS NULL)
                 FROM page_inventory",
                [],
                |row| {
                    Ok(InventoryCounts {
                        total: row.get::<_, i64>(0)? as u64,
                        indexed: row.get::<_, i64>(1)? as u64,
                        skipped: row.get::<_, i64>(2)? as u64,
                        failed: row.get::<_, i64>(3)? as u64,
                        pending: row.get::<_, i64>(4)? as u64,
                    })
                },
            )
            .map_err(map_sqlite_error)
    }

    /// Skipped and failed pages, ordered by id. Ids and typed reasons only.
    pub fn out_of_sync_pages(
        &self,
        limit: usize,
    ) -> Result<Vec<(String, InventoryOutcome)>, SyncStateError> {
        let db = self.lock_connection()?;
        let mut statement = db
            .prepare(
                "SELECT page_id, outcome, outcome_detail FROM page_inventory
                 WHERE outcome IN ('skipped', 'failed') ORDER BY page_id LIMIT ?1",
            )
            .map_err(map_sqlite_error)?;
        let rows = statement
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })
            .map_err(map_sqlite_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite_error)?;
        rows.into_iter()
            .map(|(id, kind, detail)| {
                let detail = detail.unwrap_or_default();
                let outcome = match kind.as_str() {
                    "skipped" => SkipReason::parse(&detail).map(InventoryOutcome::Skipped),
                    _ => FailureClass::parse(&detail).map(InventoryOutcome::Failed),
                };
                outcome.map(|o| (id, o)).ok_or(SyncStateError::CorruptState)
            })
            .collect()
    }
}
