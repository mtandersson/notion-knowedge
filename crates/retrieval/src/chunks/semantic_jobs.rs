//! Derived retry state for the current canonical text projection. This file is
//! owned by the disposable index directory, not the operational database.
use super::{ChunkTableError, EmbeddedChunk, HashMap, IndexedChunk, Path};
use notion_knowledge_core::embedding::EmbeddingError;
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use sha2::{Digest, Sha256};

const FILE: &str = "semantic-projection.sqlite";
const FORMAT: i64 = 1;
const RETRY_DELAY_SECONDS: i64 = 30;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Job {
    pub page: String,
    pub revision: String,
    pub text: String,
    pub due: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SemanticBacklog {
    pub queued: u64,
    pub model_unavailable: u64,
    pub input_limit: u64,
    pub rejected: u64,
}

pub(super) fn fingerprint(chunks: &[IndexedChunk]) -> Result<String, ChunkTableError> {
    let mut ordered = chunks.iter().collect::<Vec<_>>();
    ordered.sort_by(|a, b| a.chunk_id.cmp(&b.chunk_id));
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&ordered)?)
    ))
}

fn connect(directory: &Path, writable: bool) -> Result<Connection, ChunkTableError> {
    let flags = if writable {
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let db = Connection::open_with_flags(directory.join(FILE), flags)
        .map_err(|_| ChunkTableError::Storage)?;
    if writable {
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS projection_jobs (
                page TEXT PRIMARY KEY NOT NULL,
                revision TEXT NOT NULL,
                text TEXT NOT NULL,
                failure INTEGER NOT NULL CHECK(failure BETWEEN 0 AND 3),
                due INTEGER,
                reusable TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS projection_due ON projection_jobs(due, page);",
        )
        .map_err(|_| ChunkTableError::Storage)?;
    }
    Ok(db)
}

pub(super) fn installed(directory: &Path) -> Result<bool, ChunkTableError> {
    let Some(database) = reader(directory)? else {
        return Ok(false);
    };
    let version: i64 = database
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(|_| ChunkTableError::Storage)?;
    match version {
        0 => Ok(false),
        FORMAT => Ok(true),
        _ => Err(ChunkTableError::InvalidSchema(
            "unsupported text projection format".into(),
        )),
    }
}

pub(super) fn mark_installed(directory: &Path) -> Result<(), ChunkTableError> {
    if !installed(directory)? {
        connect(directory, true)?
            .pragma_update(None, "user_version", FORMAT)
            .map_err(|_| ChunkTableError::Storage)?;
    }
    Ok(())
}

fn reader(directory: &Path) -> Result<Option<Connection>, ChunkTableError> {
    if directory.join(FILE).try_exists()? {
        Ok(Some(connect(directory, false)?))
    } else {
        Ok(None)
    }
}

pub(super) fn for_page(directory: &Path, page: &str) -> Result<Option<Job>, ChunkTableError> {
    let Some(db) = reader(directory)? else {
        return Ok(None);
    };
    db.query_row(
        "SELECT page, revision, text, due FROM projection_jobs WHERE page = ?1",
        [page],
        |row| {
            Ok(Job {
                page: row.get(0)?,
                revision: row.get(1)?,
                text: row.get(2)?,
                due: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(|_| ChunkTableError::Storage)
}

pub(super) fn next(directory: &Path, now: i64) -> Result<Option<String>, ChunkTableError> {
    let Some(db) = reader(directory)? else {
        return Ok(None);
    };
    db.query_row(
        "SELECT page FROM projection_jobs WHERE due <= ?1 ORDER BY due, page LIMIT 1",
        [now],
        |row| row.get(0),
    )
    .optional()
    .map_err(|_| ChunkTableError::Storage)
}

pub(super) fn enqueue(
    directory: &Path,
    page: &str,
    revision: &str,
    text: &str,
    reusable: &[EmbeddedChunk],
    now: i64,
) -> Result<(), ChunkTableError> {
    let reusable = reusable
        .iter()
        .map(|row| (&row.chunk.chunk_id, &row.vector))
        .collect::<HashMap<_, _>>();
    let reusable = serde_json::to_string(&reusable)?;
    connect(directory, true)?
        .execute(
            "INSERT INTO projection_jobs(page, revision, text, reusable, failure, due)
           VALUES(?1, ?2, ?3, ?4, 0, ?5)
         ON CONFLICT(page) DO UPDATE SET revision = excluded.revision,
             text = excluded.text, reusable = excluded.reusable, failure = 0, due = excluded.due
           WHERE projection_jobs.revision <> excluded.revision
              OR projection_jobs.text <> excluded.text",
            params![page, revision, text, reusable, now],
        )
        .map_err(|_| ChunkTableError::Storage)?;
    Ok(())
}

pub(super) fn reusable(
    directory: &Path,
    job: &Job,
) -> Result<HashMap<String, Vec<f32>>, ChunkTableError> {
    let Some(db) = reader(directory)? else {
        return Ok(HashMap::new());
    };
    let raw: Option<String> = db
        .query_row(
            "SELECT reusable FROM projection_jobs WHERE page = ?1 AND revision = ?2 AND text = ?3",
            params![job.page, job.revision, job.text],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| ChunkTableError::Storage)?;
    raw.map(|json| serde_json::from_str(&json).map_err(Into::into))
        .transpose()
        .map(Option::unwrap_or_default)
}

pub(super) fn clear(directory: &Path, page: &str) -> Result<(), ChunkTableError> {
    if directory.join(FILE).try_exists()? {
        connect(directory, true)?
            .execute("DELETE FROM projection_jobs WHERE page = ?1", [page])
            .map_err(|_| ChunkTableError::Storage)?;
    }
    Ok(())
}

pub(super) fn fail(
    directory: &Path,
    job: &Job,
    error: EmbeddingError,
    now: i64,
) -> Result<(), ChunkTableError> {
    let (failure, due) = match error {
        EmbeddingError::InputLimit => (2, None),
        // Assets can be repaired in place; model identity remains fixed.
        EmbeddingError::UnsupportedModel
        | EmbeddingError::Unavailable
        | EmbeddingError::ResourceExhausted => (1, Some(now.saturating_add(RETRY_DELAY_SECONDS))),
        _ => (3, None),
    };
    connect(directory, true)?
        .execute(
            "UPDATE projection_jobs SET failure = ?4, due = ?5
           WHERE page = ?1 AND revision = ?2 AND text = ?3",
            params![job.page, job.revision, job.text, failure, due],
        )
        .map_err(|_| ChunkTableError::Storage)?;
    Ok(())
}

pub(super) fn backlog(directory: &Path) -> Result<SemanticBacklog, ChunkTableError> {
    let Some(db) = reader(directory)? else {
        return Ok(SemanticBacklog::default());
    };
    db.query_row(
        "SELECT COUNT(CASE WHEN failure=0 THEN 1 END), COUNT(CASE WHEN failure=1 THEN 1 END),
                COUNT(CASE WHEN failure=2 THEN 1 END), COUNT(CASE WHEN failure=3 THEN 1 END)
           FROM projection_jobs",
        [],
        |row| {
            Ok(SemanticBacklog {
                queued: row.get::<_, i64>(0)? as u64,
                model_unavailable: row.get::<_, i64>(1)? as u64,
                input_limit: row.get::<_, i64>(2)? as u64,
                rejected: row.get::<_, i64>(3)? as u64,
            })
        },
    )
    .map_err(|_| ChunkTableError::Storage)
}
