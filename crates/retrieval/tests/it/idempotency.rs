use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
    },
};

use notion_knowledge_core::idempotency::{
    IdempotencyError, MutationClaim, MutationOperation, Reservation, VerifiedReceipt,
    WriteIdempotencyStore,
};
use notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore;
use rusqlite::Connection;

static NEXT: AtomicU64 = AtomicU64::new(1);
const KEY: &str = "5f62e9ab-6e23-4a16-926b-4280bd9b2d3e";

struct TestDb(PathBuf);

impl TestDb {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "nk-idempotency-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        Self(dir.join("ledger.sqlite"))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if let Some(dir) = self.0.parent() {
            let _ = fs::remove_dir_all(dir);
        }
    }
}

fn claim(key: &str, payload: &str) -> MutationClaim {
    MutationClaim::new(
        "trusted-workspace-1",
        key,
        MutationOperation::Append,
        "page-123",
        &MutationClaim::payload_sha256(payload.as_bytes()),
    )
    .unwrap()
}

fn receipt() -> VerifiedReceipt {
    VerifiedReceipt::from_readback(
        "page-123".into(),
        "https://www.notion.so/page-123".into(),
        "2026-10-10T10:00:00Z".into(),
    )
    .unwrap()
}

#[test]
fn timeout_then_retry_never_replays_an_unknown_write() {
    let db = TestDb::new();
    let operation = claim(KEY, "append text");
    let store = SqliteIdempotencyStore::initialize_new(db.path()).unwrap();
    assert_eq!(store.begin(&operation).unwrap(), Reservation::ExecuteOnce);
    assert_eq!(store.begin(&operation).unwrap(), Reservation::Reconcile);

    // A timed-out upstream PATCH may have committed. Never send it again.
    store.mark_uncertain(&operation).unwrap();
    drop(store);

    let reopened = SqliteIdempotencyStore::open_existing(db.path()).unwrap();
    assert_eq!(reopened.begin(&operation).unwrap(), Reservation::Reconcile);

    // A trusted workflow resolves the unknown outcome with fresh read-back.
    reopened.record_verified(&operation, &receipt()).unwrap();
    assert_eq!(
        reopened.begin(&operation).unwrap(),
        Reservation::Replay(receipt())
    );
    drop(reopened);
    assert_eq!(
        SqliteIdempotencyStore::open_existing(db.path())
            .unwrap()
            .begin(&operation)
            .unwrap(),
        Reservation::Replay(receipt())
    );
}

#[test]
fn identical_key_with_changed_payload_target_or_operation_is_rejected() {
    let store = SqliteIdempotencyStore::open_in_memory().unwrap();
    let original = claim(KEY, "text-one");
    assert_eq!(store.begin(&original).unwrap(), Reservation::ExecuteOnce);
    assert_eq!(
        store.begin(&claim(KEY, "text-two")),
        Err(IdempotencyError::KeyConflict)
    );
    for variant in [
        MutationClaim::new(
            "trusted-workspace-1",
            KEY,
            MutationOperation::CreatePage,
            "page-123",
            &MutationClaim::payload_sha256(b"text-one"),
        )
        .unwrap(),
        MutationClaim::new(
            "trusted-workspace-1",
            KEY,
            MutationOperation::Append,
            "page-456",
            &MutationClaim::payload_sha256(b"text-one"),
        )
        .unwrap(),
    ] {
        assert_eq!(store.begin(&variant), Err(IdempotencyError::KeyConflict));
        assert_eq!(
            store.mark_uncertain(&variant),
            Err(IdempotencyError::KeyConflict)
        );
        assert_eq!(
            store.record_verified(&variant, &receipt()),
            Err(IdempotencyError::KeyConflict)
        );
    }
    let separate_workspace = MutationClaim::new(
        "trusted-workspace-2",
        KEY,
        MutationOperation::Append,
        "page-123",
        &MutationClaim::payload_sha256(b"text-one"),
    )
    .unwrap();
    assert_eq!(
        store.begin(&separate_workspace).unwrap(),
        Reservation::ExecuteOnce
    );
}

#[test]
fn committing_requires_a_reservation_and_never_changes_an_existing_receipt() {
    let store = SqliteIdempotencyStore::open_in_memory().unwrap();
    let operation = claim(KEY, "append text");
    assert_eq!(
        store.record_verified(&operation, &receipt()),
        Err(IdempotencyError::NotReserved)
    );
    store.begin(&operation).unwrap();
    store.record_verified(&operation, &receipt()).unwrap();
    store.record_verified(&operation, &receipt()).unwrap();
    assert_eq!(
        store.mark_uncertain(&operation),
        Err(IdempotencyError::AlreadyCommitted)
    );
    let mut changed = receipt();
    changed.last_edited_time = "2026-10-11T10:00:00Z".into();
    assert_eq!(
        store.record_verified(&operation, &changed),
        Err(IdempotencyError::KeyConflict)
    );
    assert_eq!(
        store.begin(&operation).unwrap(),
        Reservation::Replay(receipt())
    );
}

#[test]
fn simultaneous_process_connections_reserve_only_once() {
    let db = TestDb::new();
    SqliteIdempotencyStore::initialize_new(db.path()).unwrap();
    let barrier = Arc::new(Barrier::new(8));
    let path = db.path().to_owned();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            std::thread::spawn(move || {
                let store = SqliteIdempotencyStore::open_existing(path).unwrap();
                barrier.wait();
                store.begin(&claim(KEY, "same mutation")).unwrap()
            })
        })
        .collect();
    let decisions: Vec<_> = threads
        .into_iter()
        .map(|task| task.join().unwrap())
        .collect();
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == Reservation::ExecuteOnce)
            .count(),
        1
    );
    assert_eq!(
        decisions
            .iter()
            .filter(|decision| **decision == Reservation::Reconcile)
            .count(),
        7
    );
}

#[test]
fn ledger_loss_and_malformed_preconditions_fail_closed() {
    let db = TestDb::new();
    assert!(matches!(
        SqliteIdempotencyStore::open_existing(db.path()),
        Err(IdempotencyError::Unavailable)
    ));
    let store = SqliteIdempotencyStore::initialize_new(db.path()).unwrap();
    assert_eq!(
        store.begin(&claim(KEY, "body")).unwrap(),
        Reservation::ExecuteOnce
    );
    drop(store);
    assert!(matches!(
        SqliteIdempotencyStore::initialize_new(db.path()),
        Err(IdempotencyError::Unavailable)
    ));
    fs::remove_file(db.path()).unwrap();
    assert!(matches!(
        SqliteIdempotencyStore::open_existing(db.path()),
        Err(IdempotencyError::Unavailable)
    ));

    for key in ["", "too-short", "token with spaces"] {
        assert_eq!(
            MutationClaim::new(
                "trusted-workspace-1",
                key,
                MutationOperation::Append,
                "page-123",
                &MutationClaim::payload_sha256(b"payload"),
            ),
            Err(IdempotencyError::InvalidInput)
        );
    }
    assert_eq!(
        MutationClaim::new(
            "trusted-workspace-1",
            KEY,
            MutationOperation::Append,
            "page-123",
            "invalid"
        ),
        Err(IdempotencyError::InvalidInput)
    );
}

#[test]
fn the_database_stores_only_hashes_of_request_identifiers() {
    let db = TestDb::new();
    let store = SqliteIdempotencyStore::initialize_new(db.path()).unwrap();
    let operation = claim(KEY, "private markdown that must not be stored");
    store.begin(&operation).unwrap();
    drop(store);
    let connection = Connection::open(db.path()).unwrap();
    let (key, payload): (String, String) = connection
        .query_row(
            "SELECT key_digest, request_digest FROM mutation_claims",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(key, operation.key_digest());
    assert_eq!(payload, operation.request_digest());
    assert!(!key.contains(KEY));
    assert!(!payload.contains("private markdown"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(db.path()).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[test]
fn untrusted_receipt_urls_are_never_stored_for_replay() {
    for url in [
        "http://www.notion.so/page-123",
        "https://attacker.example/page-123",
        "https://notion.so.attacker.example/page-123",
        "https://user:pass@www.notion.so/page-123",
    ] {
        assert_eq!(
            VerifiedReceipt::from_readback(
                "page-123".into(),
                url.into(),
                "2026-10-10T10:00:00Z".into(),
            ),
            Err(IdempotencyError::InvalidInput)
        );
    }
}
