#![cfg(all(unix, feature = "local-lancedb"))]
use notion_knowledge_retrieval::{
    commit::transition::manifest::*, sync_state::SqliteSyncStateStore,
};
use rusqlite::Connection;

fn bounded_snapshot(
    db: &Connection,
    limits: ManifestLimits,
) -> Result<SqlManifest, notion_knowledge_retrieval::commit::CommitError> {
    let transaction = db.unchecked_transaction().unwrap();
    let result = capture_sql_manifest(&transaction, limits);
    match result {
        Ok(manifest) => {
            transaction.commit().unwrap();
            Ok(manifest)
        }
        Err(error) => Err(error),
    }
}
fn snapshot(db: &Connection) -> SqlManifest {
    bounded_snapshot(db, ManifestLimits::default()).unwrap()
}

#[test]
fn manifest_preserves_typed_values_multiplicity_and_observable_row_identity() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE payload(id INTEGER PRIMARY KEY,value); INSERT INTO payload VALUES(1,1); CREATE TABLE duplicates(value); INSERT INTO duplicates VALUES('same'),('same');").unwrap();
    let original = snapshot(&db);
    for update in [
        "UPDATE payload SET value=CAST(1 AS REAL)",
        "UPDATE payload SET value='1'",
        "UPDATE payload SET value=x'31'",
        "UPDATE payload SET value=NULL",
    ] {
        db.execute_batch(update).unwrap();
        assert_ne!(snapshot(&db), original);
        db.execute_batch("UPDATE payload SET value=1").unwrap();
        assert_eq!(snapshot(&db), original);
    }
    db.execute("UPDATE payload SET value=?1", [1.25f64])
        .unwrap();
    let real = snapshot(&db);
    db.execute(
        "UPDATE payload SET value=?1",
        [f64::from_bits(1.25f64.to_bits() + 1)],
    )
    .unwrap();
    assert_ne!(snapshot(&db), real);
    db.execute_batch("UPDATE payload SET value=1; DELETE FROM duplicates WHERE rowid=2")
        .unwrap();
    let missing = snapshot(&db);
    assert_ne!(missing.sha256, original.sha256);
    assert_eq!(missing.rows + 1, original.rows);
    db.execute_batch("INSERT INTO duplicates(rowid,value) VALUES(2,'same')")
        .unwrap();
    assert_eq!(snapshot(&db), original);
    db.execute_batch("UPDATE duplicates SET rowid=99 WHERE rowid=2")
        .unwrap();
    assert_ne!(snapshot(&db), original);
}

#[test]
fn manifest_is_independent_of_scan_order_and_quotes_schema_identifiers() {
    let first = Connection::open_in_memory().unwrap();
    let second = Connection::open_in_memory().unwrap();
    for db in [&first, &second] {
        db.execute_batch(
            "CREATE TABLE \"odd\"\"table\"(id INTEGER PRIMARY KEY,\"data\"\"column\" TEXT)",
        )
        .unwrap();
    }
    first
        .execute_batch("INSERT INTO \"odd\"\"table\" VALUES(1,'å'),(2,'b')")
        .unwrap();
    second
        .execute_batch("INSERT INTO \"odd\"\"table\" VALUES(2,'b'),(1,'å')")
        .unwrap();
    assert_eq!(snapshot(&first), snapshot(&second));
    first
        .execute_batch("PRAGMA reverse_unordered_selects=ON")
        .unwrap();
    assert_eq!(snapshot(&first), snapshot(&second));
}

#[test]
fn temporary_names_cannot_shadow_main_schema_or_payload() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE payload(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO main.payload VALUES(1,'main state');").unwrap();
    let original = snapshot(&db);
    db.execute_batch("CREATE TEMP TABLE payload(id INTEGER PRIMARY KEY,unrelated BLOB); INSERT INTO temp.payload VALUES(1,zeroblob(100000));").unwrap();
    assert_eq!(snapshot(&db), original);
    db.execute_batch("UPDATE main.payload SET value='changed main state'")
        .unwrap();
    assert_ne!(snapshot(&db), original);
    db.execute_batch("UPDATE main.payload SET value='main state'; DELETE FROM temp.payload;")
        .unwrap();
    assert_eq!(snapshot(&db), original);
}

#[test]
fn manifest_includes_schema_history_and_internal_autoincrement_state() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE sequence_test(id INTEGER PRIMARY KEY AUTOINCREMENT,value TEXT); INSERT INTO sequence_test(value) VALUES('acknowledged'); DELETE FROM sequence_test;").unwrap();
    let original = snapshot(&db);
    assert_eq!(original.tables, 2);
    db.execute_batch("UPDATE sqlite_sequence SET seq=2")
        .unwrap();
    assert_ne!(snapshot(&db), original);
    db.execute_batch("UPDATE sqlite_sequence SET seq=1")
        .unwrap();
    assert_eq!(snapshot(&db), original);
    db.execute_batch("CREATE VIEW retained_view AS SELECT id,value FROM sequence_test")
        .unwrap();
    let with_view = snapshot(&db);
    assert_ne!(with_view, original);
    db.execute_batch("DROP VIEW retained_view; CREATE TRIGGER retained_trigger BEFORE INSERT ON sequence_test BEGIN SELECT 1; END;").unwrap();
    assert_ne!(snapshot(&db), original);
}

#[test]
fn only_the_verified_generation_lookup_index_is_excluded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("operational.sqlite");
    drop(SqliteSyncStateStore::open(&path).unwrap());
    let db = Connection::open(path).unwrap();
    let original = snapshot(&db);
    let objects: i64 = db
        .query_row("SELECT count(*) FROM sqlite_schema", [], |r| r.get(0))
        .unwrap();
    let exact_limit = ManifestLimits {
        objects: objects as u64,
        ..ManifestLimits::default()
    };
    assert_eq!(bounded_snapshot(&db, exact_limit).unwrap(), original);
    db.execute_batch("CREATE INDEX index_commit_generations_policy_epoch ON index_commit_generations(scope,epoch)").unwrap();
    assert_eq!(snapshot(&db), original);
    assert_eq!(bounded_snapshot(&db, exact_limit).unwrap(), original);
    db.execute_batch("CREATE TEMP TABLE index_commit_generations(generation TEXT,scope TEXT,epoch INTEGER); CREATE INDEX temp.index_commit_generations_policy_epoch ON index_commit_generations(scope COLLATE NOCASE,epoch)").unwrap();
    assert_eq!(snapshot(&db), original);
    db.execute_batch("DROP INDEX temp.index_commit_generations_policy_epoch; CREATE INDEX temp.index_commit_generations_policy_epoch ON index_commit_generations(scope,epoch)").unwrap();
    db.execute_batch("DROP INDEX main.index_commit_generations_policy_epoch; CREATE INDEX main.index_commit_generations_policy_epoch ON index_commit_generations(scope COLLATE NOCASE,epoch)").unwrap();
    assert!(bounded_snapshot(&db, ManifestLimits::default()).is_err());
}

#[test]
fn manifest_rejects_unsupported_schema_and_enforces_explicit_limits() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE payload(id INTEGER PRIMARY KEY,value); INSERT INTO payload VALUES(1,zeroblob(10000));").unwrap();
    assert!(
        bounded_snapshot(
            &db,
            ManifestLimits {
                row_bytes: 1000,
                ..ManifestLimits::default()
            }
        )
        .is_err()
    );
    assert!(
        bounded_snapshot(
            &db,
            ManifestLimits {
                total_bytes: 1000,
                ..ManifestLimits::default()
            }
        )
        .is_err()
    );
    assert!(
        bounded_snapshot(
            &db,
            ManifestLimits {
                digest_bytes: 1,
                ..ManifestLimits::default()
            }
        )
        .is_err()
    );
    assert!(
        bounded_snapshot(
            &db,
            ManifestLimits {
                columns: 1,
                ..ManifestLimits::default()
            }
        )
        .is_err()
    );
    db.execute_batch("CREATE TABLE unsupported(value INTEGER,derived INTEGER GENERATED ALWAYS AS (value+1) STORED)").unwrap();
    assert!(bounded_snapshot(&db, ManifestLimits::default()).is_err());
}
