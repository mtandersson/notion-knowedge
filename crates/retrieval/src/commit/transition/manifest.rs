//! Bounded logical manifests for the complete main operational database.
//! Physical page layout is excluded; typed values, duplicate multiplicity,
//! persisted schemas, queues and history are retained. Only the explicitly
//! validated derived generation-history index is omitted from schema identity.
use super::*;
use rusqlite::{Row, types::ValueRef};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy)]
pub struct ManifestLimits {
    pub objects: u64,
    pub tables: u64,
    pub columns: u64,
    pub rows: u64,
    pub row_bytes: u64,
    pub total_bytes: u64,
    pub digest_bytes: u64,
}
impl Default for ManifestLimits {
    fn default() -> Self {
        Self {
            objects: 512,
            tables: 128,
            columns: 128,
            rows: 1_000_000,
            row_bytes: 128 * 1024 * 1024,
            total_bytes: 512 * 1024 * 1024,
            digest_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SqlManifest {
    pub sha256: String,
    pub tables: u64,
    pub rows: u64,
}

pub(super) trait Normalizer {
    fn keep_row(
        &self,
        _table: &str,
        _columns: &[String],
        _row: &Row<'_>,
    ) -> Result<bool, CommitError> {
        Ok(true)
    }
    fn value<'a>(
        &'a self,
        _table: &str,
        _columns: &[String],
        _row: &'a Row<'_>,
        _column: usize,
        value: ValueRef<'a>,
    ) -> Result<ValueRef<'a>, CommitError> {
        Ok(value)
    }
    fn finish_table(&self, _table: &str, _physical: u64, _logical: u64) -> Result<(), CommitError> {
        Ok(())
    }
}
struct Unmodified;
impl Normalizer for Unmodified {}

fn identifier(value: &str) -> Result<String, CommitError> {
    if value.is_empty() || value.len() > 4096 || value.as_bytes().contains(&0) {
        return Err(CommitError::InvalidInput);
    }
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}
fn sentinel(value: u64) -> Result<i64, CommitError> {
    i64::try_from(value)
        .ok()
        .and_then(|v| v.checked_add(1))
        .ok_or(CommitError::InvalidInput)
}
fn field(hash: &mut Sha256, bytes: &[u8]) {
    hash.update((bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
}
fn cell(hash: &mut Sha256, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => hash.update(b"n"),
        ValueRef::Integer(value) => {
            hash.update(b"i");
            hash.update(value.to_le_bytes());
        }
        ValueRef::Real(value) => {
            hash.update(b"r");
            hash.update(value.to_bits().to_le_bytes());
        }
        ValueRef::Text(value) => {
            hash.update(b"t");
            field(hash, value);
        }
        ValueRef::Blob(value) => {
            hash.update(b"b");
            field(hash, value);
        }
    }
}

struct Budget {
    rows: u64,
    bytes: u64,
}
impl Budget {
    fn preflight(
        &mut self,
        db: &Connection,
        from: &str,
        columns: &[String],
        limits: ManifestLimits,
    ) -> Result<u64, CommitError> {
        let count: i64 = db
            .query_row(
                &format!("SELECT COUNT(*) FROM (SELECT 1 FROM {from} LIMIT ?1)"),
                [sentinel(limits.rows)?],
                |r| r.get(0),
            )
            .map_err(sql)?;
        let count = u64::try_from(count).map_err(|_| CommitError::InvalidInput)?;
        self.rows = self
            .rows
            .checked_add(count)
            .ok_or(CommitError::InvalidInput)?;
        if self.rows > limits.rows
            || count
                .checked_mul(32)
                .is_none_or(|bytes| bytes > limits.digest_bytes)
        {
            return Err(CommitError::InvalidInput);
        }
        let mut sizes = Vec::new();
        for name in columns {
            // octet_length(column) uses storage metadata. The factor also bounds
            // UTF-16 to UTF-8 conversion before sqlite3_column_text is called.
            sizes.push(format!(
                "(coalesce(octet_length({}),0)*4+16)",
                identifier(name)?
            ));
        }
        let size = format!("64+{}", sizes.join("+"));
        let (oversized, bytes): (bool, i64) = db
            .query_row(
                &format!("SELECT coalesce(max({size}),0)>?1,coalesce(sum({size}),0) FROM {from}"),
                [i64::try_from(limits.row_bytes).map_err(|_| CommitError::InvalidInput)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(bytes).map_err(|_| CommitError::InvalidInput)?)
            .ok_or(CommitError::InvalidInput)?;
        if oversized || self.bytes > limits.total_bytes {
            return Err(CommitError::InvalidInput);
        }
        Ok(count)
    }
}

/// Read within a retained transaction. Captures all persisted table contents,
/// not just the records whose generation will change. It grants no authority.
pub fn capture_sql_manifest(
    db: &rusqlite::Transaction<'_>,
    limits: ManifestLimits,
) -> Result<SqlManifest, CommitError> {
    capture_normalized(db, limits, &Unmodified)
}

pub(super) fn capture_normalized(
    db: &rusqlite::Transaction<'_>,
    limits: ManifestLimits,
    normalize: &impl Normalizer,
) -> Result<SqlManifest, CommitError> {
    if [
        limits.objects,
        limits.tables,
        limits.columns,
        limits.rows,
        limits.row_bytes,
        limits.total_bytes,
        limits.digest_bytes,
    ]
    .contains(&0)
    {
        return Err(CommitError::InvalidInput);
    }
    let indexed:bool = db.query_row("SELECT EXISTS(SELECT 1 FROM main.sqlite_schema WHERE type='index' AND name='index_commit_generations_policy_epoch')",[],|r|r.get(0)).map_err(sql)?;
    if indexed {
        super::super::alias_projection::validate_history_index(db)?;
    }
    let schema = "main.sqlite_schema WHERE NOT(type='index' AND name='index_commit_generations_policy_epoch')";
    let objects: i64 = db
        .query_row(
            &format!("SELECT COUNT(*) FROM (SELECT 1 FROM {schema} LIMIT ?1)"),
            [sentinel(limits.objects)?],
            |r| r.get(0),
        )
        .map_err(sql)?;
    if objects as u64 > limits.objects {
        return Err(CommitError::InvalidInput);
    }
    let mut budget = Budget { rows: 0, bytes: 64 };
    budget.preflight(
        db,
        schema,
        &[
            "type".into(),
            "name".into(),
            "tbl_name".into(),
            "sql".into(),
            "rowid".into(),
        ],
        limits,
    )?;
    let mut hash = Sha256::new();
    field(&mut hash, b"nk-operational-sql-manifest-v1");
    let mut tables = Vec::new();
    {
        let mut statement = db.prepare(&format!("SELECT type,CASE WHEN octet_length(name) BETWEEN 1 AND 4096 THEN name ELSE NULL END,CASE WHEN octet_length(tbl_name) BETWEEN 1 AND 4096 THEN tbl_name ELSE NULL END,CASE WHEN sql IS NULL OR octet_length(sql)<=65536 THEN sql ELSE NULL END,sql IS NOT NULL,rowid FROM {schema} ORDER BY type,name")).map_err(sql)?;
        let mut rows = statement.query([]).map_err(sql)?;
        while let Some(row) = rows.next().map_err(sql)? {
            let kind: String = row.get(0).map_err(sql)?;
            let name: String = row.get(1).map_err(sql)?;
            let table: String = row.get(2).map_err(sql)?;
            let declaration: Option<String> = row.get(3).map_err(sql)?;
            if row.get::<_, bool>(4).map_err(sql)? && declaration.is_none() {
                return Err(CommitError::InvalidInput);
            }
            if !matches!(kind.as_str(), "table" | "index" | "trigger" | "view") {
                return Err(CommitError::InvalidInput);
            }
            field(&mut hash, b"schema");
            field(&mut hash, kind.as_bytes());
            field(&mut hash, name.as_bytes());
            field(&mut hash, table.as_bytes());
            cell(
                &mut hash,
                declaration
                    .as_deref()
                    .map_or(ValueRef::Null, |sql| ValueRef::Text(sql.as_bytes())),
            );
            cell(&mut hash, row.get_ref(5).map_err(sql)?);
            if kind == "table" {
                if tables.len() as u64 >= limits.tables {
                    return Err(CommitError::InvalidInput);
                }
                tables.push(name);
            }
        }
    }
    let mut logical_rows = 0u64;
    for table in &tables {
        let (kind, without_rowid): (String, bool) = db
            .query_row(
                "SELECT type,wr FROM pragma_table_list WHERE schema='main' AND name=?1",
                [table],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(sql)?;
        if kind != "table" {
            return Err(CommitError::InvalidInput);
        }
        field(&mut hash, b"table");
        field(&mut hash, table.as_bytes());
        let column_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM (SELECT 1 FROM pragma_table_xinfo(?1,'main') LIMIT ?2)",
                params![table, sentinel(limits.columns)?],
                |r| r.get(0),
            )
            .map_err(sql)?;
        if column_count <= 0 || column_count as u64 > limits.columns {
            return Err(CommitError::InvalidInput);
        }
        identifier(table)?;
        let info = format!("pragma_table_xinfo('{}','main')", table.replace('\'', "''"));
        budget.preflight(
            db,
            &info,
            &[
                "name".into(),
                "type".into(),
                "notnull".into(),
                "dflt_value".into(),
                "pk".into(),
                "hidden".into(),
            ],
            limits,
        )?;
        let mut columns = Vec::new();
        {
            let mut statement = db.prepare("SELECT CASE WHEN octet_length(name) BETWEEN 1 AND 4096 THEN name ELSE NULL END,type,\"notnull\",dflt_value,pk,hidden FROM pragma_table_xinfo(?1,'main') ORDER BY cid").map_err(sql)?;
            let mut rows = statement.query([table]).map_err(sql)?;
            while let Some(row) = rows.next().map_err(sql)? {
                if row.get::<_, i64>(5).map_err(sql)? != 0 {
                    return Err(CommitError::InvalidInput);
                }
                let name: String = row.get(0).map_err(sql)?;
                field(&mut hash, b"column");
                for column in 0..5 {
                    cell(&mut hash, row.get_ref(column).map_err(sql)?);
                }
                columns.push(name);
            }
        }
        let rowid = if without_rowid {
            None
        } else {
            ["rowid", "_rowid_", "oid"].into_iter().find(|alias| {
                !columns
                    .iter()
                    .any(|column| column.eq_ignore_ascii_case(alias))
            })
        };
        let extra = if let Some(alias) = rowid {
            field(&mut hash, b"observable-rowid");
            field(&mut hash, alias.as_bytes());
            columns.push(alias.into());
            format!(",{}", identifier(alias)?)
        } else {
            field(&mut hash, b"no-observable-rowid");
            String::new()
        };
        let quoted = format!("main.{}", identifier(table)?);
        let count = budget.preflight(db, &quoted, &columns, limits)?;
        let mut row_hashes = Vec::<[u8; 32]>::with_capacity(
            usize::try_from(count).map_err(|_| CommitError::InvalidInput)?,
        );
        let mut statement = db
            .prepare(&format!("SELECT *{extra} FROM {quoted}"))
            .map_err(sql)?;
        if statement.column_count() != columns.len() {
            return Err(CommitError::InvalidInput);
        }
        let mut rows = statement.query([]).map_err(sql)?;
        let mut physical = 0u64;
        while let Some(row) = rows.next().map_err(sql)? {
            physical += 1;
            if physical > count {
                return Err(CommitError::Conflict);
            }
            if !normalize.keep_row(table, &columns, row)? {
                continue;
            }
            let mut row_hash = Sha256::new();
            field(&mut row_hash, b"row-v1");
            for column in 0..columns.len() {
                cell(
                    &mut row_hash,
                    normalize.value(
                        table,
                        &columns,
                        row,
                        column,
                        row.get_ref(column).map_err(sql)?,
                    )?,
                );
            }
            row_hashes.push(row_hash.finalize().into());
        }
        if physical != count {
            return Err(CommitError::Conflict);
        }
        normalize.finish_table(table, physical, row_hashes.len() as u64)?;
        logical_rows = logical_rows
            .checked_add(row_hashes.len() as u64)
            .ok_or(CommitError::InvalidInput)?;
        row_hashes.sort_unstable();
        hash.update((row_hashes.len() as u64).to_le_bytes());
        for digest in row_hashes {
            hash.update(digest);
        }
    }
    Ok(SqlManifest {
        sha256: format!("{:x}", hash.finalize()),
        tables: tables.len() as u64,
        rows: logical_rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    struct Observe {
        fields: Cell<usize>,
    }
    impl Normalizer for Observe {
        fn value<'a>(
            &'a self,
            _table: &str,
            _columns: &[String],
            _row: &'a Row<'_>,
            _column: usize,
            value: ValueRef<'a>,
        ) -> Result<ValueRef<'a>, CommitError> {
            self.fields.set(self.fields.get() + 1);
            Ok(value)
        }
    }
    #[test]
    fn oversized_payload_is_rejected_before_payload_fields_or_digest_buffers() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE payload(value BLOB); INSERT INTO payload VALUES(zeroblob(10000));",
        )
        .unwrap();
        let transaction = db.unchecked_transaction().unwrap();
        let observe = Observe {
            fields: Cell::new(0),
        };
        assert!(
            capture_normalized(
                &transaction,
                ManifestLimits {
                    row_bytes: 1000,
                    ..ManifestLimits::default()
                },
                &observe
            )
            .is_err()
        );
        assert_eq!(observe.fields.get(), 0);
        db.execute_batch("DELETE FROM payload; INSERT INTO payload VALUES('first'),('second')")
            .unwrap();
        assert!(
            capture_normalized(
                &transaction,
                ManifestLimits {
                    digest_bytes: 32,
                    ..ManifestLimits::default()
                },
                &observe
            )
            .is_err()
        );
        assert_eq!(observe.fields.get(), 0);
        assert!(
            capture_normalized(
                &transaction,
                ManifestLimits {
                    digest_bytes: 1,
                    ..ManifestLimits::default()
                },
                &observe
            )
            .is_err()
        );
        assert_eq!(observe.fields.get(), 0);
    }
}
