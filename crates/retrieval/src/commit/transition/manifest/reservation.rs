//! Read-only proof of the sole SQL delta allowed when reserving a choice.
//! A manifest is descriptive evidence, never physical or serving authority.
use super::*;
use std::cell::Cell;

struct Reservation<'a> {
    target: &'a CommitBinding,
    found: Cell<bool>,
}

impl Normalizer for Reservation<'_> {
    fn keep_row(
        &self,
        table: &str,
        columns: &[String],
        row: &Row<'_>,
    ) -> Result<bool, CommitError> {
        if table != "index_commit_generations" {
            return Ok(true);
        }
        if columns != ["generation", "scope", "epoch", "rowid"] {
            return Err(CommitError::BindingMismatch);
        }
        let generation: String = row.get(0).map_err(sql)?;
        let scope: String = row.get(1).map_err(sql)?;
        let epoch: i64 = row.get(2).map_err(sql)?;
        let desired_epoch =
            i64::try_from(self.target.epoch()).map_err(|_| CommitError::InvalidInput)?;
        if generation == self.target.generation() || epoch == desired_epoch {
            if generation != self.target.generation()
                || scope != self.target.scope()
                || epoch != desired_epoch
                || self.found.replace(true)
            {
                return Err(CommitError::BindingMismatch);
            }
            return Ok(false);
        }
        // A later reservation cannot be hidden behind an older intent.
        if epoch >= desired_epoch {
            return Err(CommitError::Conflict);
        }
        Ok(true)
    }

    fn finish_table(&self, table: &str, physical: u64, logical: u64) -> Result<(), CommitError> {
        if table == "index_commit_generations"
            && (!self.found.get() || physical.checked_sub(logical) != Some(1))
        {
            return Err(CommitError::BindingMismatch);
        }
        Ok(())
    }
}

/// Capture all persisted SQL, omitting exactly one matching reservation row.
/// The caller must compare this result with a freshly verified pre-reservation
/// manifest under the same owner. This function neither validates that owner
/// nor authorizes reservation, native publication, or service admission.
/// Physical scan budgets include the omitted row. No SQL is modified.
pub fn capture_reservation_preimage(
    db: &rusqlite::Transaction<'_>,
    target: &CommitBinding,
    limits: ManifestLimits,
) -> Result<SqlManifest, CommitError> {
    let normalize = Reservation {
        target,
        found: Cell::new(false),
    };
    let manifest = capture_normalized(db, limits, &normalize)?;
    if !normalize.found.get() {
        return Err(CommitError::BindingMismatch);
    }
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> CommitBinding {
        CommitBinding::new(
            "chunks",
            "workspace",
            &ReconciliationScope::new(vec!["root".into()], vec![], "policy", "test").unwrap(),
            "selected",
            9,
        )
        .unwrap()
    }

    fn database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE index_commit_generations(generation TEXT,scope TEXT,epoch INTEGER);
             INSERT INTO index_commit_generations VALUES('old','scope',2);
             CREATE TABLE receipts(id TEXT,status INTEGER);
             INSERT INTO receipts VALUES('retained',1);",
        )
        .unwrap();
        db
    }

    fn reserve(db: &Connection, target: &CommitBinding) {
        db.execute(
            "INSERT INTO index_commit_generations VALUES(?1,?2,?3)",
            params![
                target.generation(),
                target.scope(),
                i64::try_from(target.epoch()).unwrap()
            ],
        )
        .unwrap();
    }

    #[test]
    fn reservation_preimage_preserves_all_other_sql_and_does_not_modify_the_database() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let before = capture_sql_manifest(&tx, ManifestLimits::default()).unwrap();
        let target = target();
        reserve(&tx, &target);
        let reserved = capture_sql_manifest(&tx, ManifestLimits::default()).unwrap();
        assert_eq!(reserved.rows, before.rows + 1);
        assert_eq!(
            capture_reservation_preimage(&tx, &target, ManifestLimits::default()).unwrap(),
            before
        );
        assert_eq!(
            capture_sql_manifest(&tx, ManifestLimits::default()).unwrap(),
            reserved
        );
        tx.execute("UPDATE receipts SET status=2", []).unwrap();
        assert_ne!(
            capture_reservation_preimage(&tx, &target, ManifestLimits::default()).unwrap(),
            before
        );
        tx.execute_batch("CREATE TABLE queued_work(id TEXT)")
            .unwrap();
        assert_ne!(
            capture_reservation_preimage(&tx, &target, ManifestLimits::default()).unwrap(),
            before
        );
    }

    #[test]
    fn reservation_preimage_rejects_missing_conflicting_duplicate_and_later_reservations() {
        for extra in [
            None,
            Some("INSERT INTO index_commit_generations VALUES('selected','wrong',9)"),
            Some("INSERT INTO index_commit_generations VALUES('selected','wrong',8)"),
            Some("INSERT INTO index_commit_generations VALUES('other','scope',9)"),
            Some("INSERT INTO index_commit_generations VALUES('later','scope',10)"),
        ] {
            let mut db = database();
            let tx = db.transaction().unwrap();
            let target = target();
            if let Some(extra) = extra {
                reserve(&tx, &target);
                tx.execute_batch(extra).unwrap();
            }
            assert!(capture_reservation_preimage(&tx, &target, ManifestLimits::default()).is_err());
        }
    }

    #[test]
    fn reservation_preimage_charges_the_omitted_row_against_the_scan_budget() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let target = target();
        let limits = ManifestLimits {
            rows: 9,
            ..ManifestLimits::default()
        };
        // Two schema objects, five column-metadata rows and two original data
        // rows exhaust this budget; the reservation adds one physical row.
        capture_sql_manifest(&tx, limits).unwrap();
        reserve(&tx, &target);
        assert!(capture_reservation_preimage(&tx, &target, limits).is_err());
    }

    #[test]
    fn reservation_preimage_rejects_duplicate_exact_rows_and_retains_unrelated_reservations() {
        let mut db = database();
        let tx = db.transaction().unwrap();
        let target = target();
        let before = capture_sql_manifest(&tx, ManifestLimits::default()).unwrap();
        reserve(&tx, &target);
        tx.execute(
            "INSERT INTO index_commit_generations VALUES('unrelated','scope',3)",
            [],
        )
        .unwrap();
        assert_ne!(
            capture_reservation_preimage(&tx, &target, ManifestLimits::default()).unwrap(),
            before
        );
        reserve(&tx, &target);
        assert!(capture_reservation_preimage(&tx, &target, ManifestLimits::default()).is_err());
    }
}
