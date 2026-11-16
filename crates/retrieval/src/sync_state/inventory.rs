//! Store methods for the disposable `page_inventory` table.
use super::{SqliteSyncStateStore, map_sqlite_error};
use notion_knowledge_core::{
    discovery::SkipReason,
    inventory::{
        DriftCounts, InventoryCounts, InventoryDiff, InventoryEntry, InventoryOutcome,
        InventoryParent, SyncRecord,
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

    pub fn record_sync(&self, sync: &SyncRecord) -> Result<(), SyncStateError> {
        let d = &sync.drift;
        self.lock_connection()?
            .execute(
                "INSERT OR REPLACE INTO inventory_walk (singleton, run_id, mode, complete, held,
                     added, changed, moved, removed, trashed, unplaced, finished_at)
                 VALUES (1, ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    sync.run_id,
                    if sync.incremental {
                        "incremental"
                    } else {
                        "full"
                    },
                    sync.complete,
                    sync.held as i64,
                    d.added as i64,
                    d.changed as i64,
                    d.moved as i64,
                    d.removed as i64,
                    d.trashed as i64,
                    d.unplaced as i64,
                    sync.finished_at
                ],
            )
            .map_err(map_sqlite_error)?;
        Ok(())
    }

    pub fn last_sync(&self) -> Result<Option<SyncRecord>, SyncStateError> {
        let n = |row: &rusqlite::Row<'_>, i| row.get::<_, i64>(i).map(|v| v as u64);
        self.lock_connection()?
            .query_row(
                "SELECT run_id, mode, complete, held, added, changed, moved, removed, trashed,
                     unplaced, finished_at FROM inventory_walk WHERE singleton = 1",
                [],
                |row| {
                    Ok(SyncRecord {
                        run_id: row.get(0)?,
                        incremental: row.get::<_, String>(1)? == "incremental",
                        complete: row.get(2)?,
                        held: n(row, 3)?,
                        drift: DriftCounts {
                            added: n(row, 4)?,
                            changed: n(row, 5)?,
                            moved: n(row, 6)?,
                            removed: n(row, 7)?,
                            trashed: n(row, 8)?,
                            unplaced: n(row, 9)?,
                        },
                        finished_at: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(map_sqlite_error)
    }

    /// The newest edit time the inventory holds: where an incremental search
    /// stops. Notion's own clock, so local clock skew cannot skip a change.
    pub fn inventory_high_water(&self) -> Result<Option<String>, SyncStateError> {
        self.lock_connection()?
            .query_row(
                "SELECT MAX(last_edited_time) FROM page_inventory WHERE last_edited_time != ''",
                [],
                |row| row.get(0),
            )
            .map_err(map_sqlite_error)
    }

    /// The inventory entry of a data-source row's siblings: how a new row is
    /// placed under the page that tagged its database.
    pub fn inventory_data_source(
        &self,
        data_source_id: &str,
    ) -> Result<Option<InventoryEntry>, SyncStateError> {
        let db = self.lock_connection()?;
        db.query_row(
            &format!("{SELECT} WHERE parent_kind = 'data_source' AND parent_id = ?1 LIMIT 1"),
            [data_source_id],
            read_entry,
        )
        .optional()
        .map_err(map_sqlite_error)?
        .map_or(Ok(None), |entry| {
            entry.ok_or(SyncStateError::CorruptState).map(Some)
        })
    }

    /// When `page_id` was last indexed at exactly `revision`, if it was.
    pub fn inventory_settled_at(
        &self,
        page_id: &str,
        revision: &str,
    ) -> Result<Option<i64>, SyncStateError> {
        self.lock_connection()?
            .query_row(
                "SELECT last_fetched_at FROM page_inventory
                 WHERE page_id = ?1 AND outcome = 'indexed' AND indexed_revision = ?2",
                params![page_id, revision],
                |row| row.get(0),
            )
            .optional()
            .map(Option::flatten)
            .map_err(map_sqlite_error)
    }

    /// The recorded outcome of one page, if it has one.
    pub fn inventory_outcome(
        &self,
        page_id: &str,
    ) -> Result<Option<InventoryOutcome>, SyncStateError> {
        let row: Option<(Option<String>, Option<String>, Option<String>)> = self
            .lock_connection()?
            .query_row(
                "SELECT outcome, outcome_detail, indexed_revision FROM page_inventory WHERE page_id = ?1",
                [page_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(map_sqlite_error)?;
        Ok(match row {
            Some((Some(kind), detail, revision)) => {
                let detail = detail.unwrap_or_default();
                Some(match kind.as_str() {
                    "indexed" => InventoryOutcome::Indexed {
                        revision: revision.unwrap_or_default(),
                    },
                    "skipped" => InventoryOutcome::Skipped(
                        SkipReason::parse(&detail).ok_or(SyncStateError::CorruptState)?,
                    ),
                    _ => InventoryOutcome::Failed(
                        FailureClass::parse(&detail).ok_or(SyncStateError::CorruptState)?,
                    ),
                })
            }
            _ => None,
        })
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

impl notion_knowledge_core::inventory::InventoryPlacement for SqliteSyncStateStore {
    fn placed_chain(&self, page_id: &str) -> Option<Vec<String>> {
        let entry = self.inventory_entry(page_id).ok()??;
        // A trashed or unreadable child, or a page the walk skipped, is not
        // a placed target.
        if entry.in_trash
            || entry.last_edited_time.is_empty()
            || matches!(
                self.inventory_outcome(page_id).ok()?,
                Some(InventoryOutcome::Skipped(_))
            )
        {
            return None;
        }
        self.inventory_parent_chain(page_id).ok()?
    }
}
