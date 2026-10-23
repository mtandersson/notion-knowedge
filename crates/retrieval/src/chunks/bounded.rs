//! Bounded native candidate projection for the opt-in lexical executor.
//! This module does not install a new public MCP search policy.
use super::*;
use arrow_array::BooleanArray;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const CANDIDATE_BYTES: usize = 96 * 1024 * 1024;
pub const CANDIDATE_ROWS: usize = 8_000;
const METADATA_FIELDS: [(&str, usize); 8] = [
    ("chunk_id", 128),
    ("page_id", 128),
    ("root_page_id", 128),
    ("block_id", 128),
    ("url", 2048),
    ("title", 1024),
    ("heading_path_json", 4096),
    ("last_edited_time", 64),
];
fn bound_error() -> ChunkTableError {
    ChunkTableError::InvalidRows("lexical candidate resource limit exceeded".into())
}

/// Trusted process-wide zero-queue admission. Each permit reserves the complete
/// 96 MiB candidate envelope before any backend candidate materialization.
/// Runtime composition must share this pool across all installed search paths.
#[derive(Clone)]
pub struct CandidateBudget {
    slots: Arc<Semaphore>,
}
impl CandidateBudget {
    pub fn new(max_requests: u32) -> Result<Self, ChunkTableError> {
        if !(1..=16).contains(&max_requests) {
            return Err(bound_error());
        }
        Ok(Self {
            slots: Arc::new(Semaphore::new(max_requests as usize)),
        })
    }
    pub fn try_reserve(&self) -> Result<CandidateReservation, ChunkTableError> {
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| bound_error())?;
        Ok(CandidateReservation {
            _permit: permit,
            rows: 0,
            bytes: 64 * 1024,
        })
    }
}
/// Keep alive until candidate merging is complete. Counters conservatively
/// include consumed backend batches and owned candidates even after release.
pub struct CandidateReservation {
    _permit: OwnedSemaphorePermit,
    rows: usize,
    bytes: usize,
}
impl CandidateReservation {
    fn charge(&mut self, rows: usize, bytes: usize) -> Result<(), ChunkTableError> {
        let next_rows = self.rows.checked_add(rows).ok_or_else(bound_error)?;
        let next_bytes = self.bytes.checked_add(bytes).ok_or_else(bound_error)?;
        if next_rows > CANDIDATE_ROWS || next_bytes > CANDIDATE_BYTES {
            return Err(bound_error());
        }
        self.rows = next_rows;
        self.bytes = next_bytes;
        Ok(())
    }
}
fn projection() -> Select {
    let mut columns = Vec::new();
    let mut overflow = Vec::new();
    for (field, limit) in METADATA_FIELDS {
        let too_large = format!("coalesce(char_length({field}) > {limit}, false)");
        overflow.push(too_large.clone());
        columns.push((
            field.into(),
            format!("CAST(left({field}, {limit}) AS STRING)"),
        ));
    }
    columns.push(("text".into(), "CAST(left(text, 2000) AS STRING)".into()));
    columns.push(("_metadata_overflow".into(), overflow.join(" OR ")));
    Select::Dynamic(columns)
}
impl LanceChunkTable {
    /// Low-level bounded native probe. The caller owns the pinned snapshot and
    /// validated scope predicate. No generated dictionary syntax is accepted by
    /// the installed handler until the later runtime integration ticket.
    pub async fn bounded_fts_query_scoped(
        &self,
        column: &str,
        query: &str,
        limit: usize,
        predicate: Option<&str>,
        reservation: &mut CandidateReservation,
    ) -> Result<Vec<FtsIndexHit>, ChunkTableError> {
        if query.chars().take(4097).count() > 4096
            || query.trim().is_empty()
            || !(1..=400).contains(&limit)
            || !CHUNK_FTS_INDEXES.iter().any(|(_, field)| *field == column)
        {
            return Err(bound_error());
        }
        let fts = FullTextSearchQuery::new(query.to_owned())
            .with_column(column.to_owned())
            .map_err(|_| bound_error())?;
        let mut search = self
            .table
            .query()
            .full_text_search(fts)
            .select(projection())
            .limit(limit);
        if let Some(predicate) = predicate {
            search = search.only_if(predicate);
        }
        let mut stream = search.execute().await?;
        let mut results = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            if results
                .len()
                .checked_add(batch.num_rows())
                .is_none_or(|rows| rows > limit)
            {
                return Err(bound_error());
            }
            let overflow = batch
                .column_by_name("_metadata_overflow")
                .and_then(|c| c.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(bound_error)?;
            if overflow.null_count() != 0 || overflow.iter().flatten().any(|v| v) {
                return Err(bound_error());
            }
            // Include Arrow buffer storage, decoded strings/records and merge
            // keys/provenance/container overhead before constructing hit vectors.
            let arrow = batch.get_array_memory_size();
            let heading_bytes = string_column(&batch, "heading_path_json")?
                .iter()
                .flatten()
                .try_fold(0usize, |sum, text| {
                    sum.checked_add(text.len()).ok_or_else(bound_error)
                })?;
            let owned = arrow
                .checked_add(heading_bytes.checked_mul(32).ok_or_else(bound_error)?)
                .and_then(|bytes| bytes.checked_add(batch.num_rows().checked_mul(8192)?))
                .ok_or_else(bound_error)?;
            reservation.charge(
                batch.num_rows(),
                arrow.checked_add(owned).ok_or_else(bound_error)?,
            )?;
            for field in METADATA_FIELDS.iter().map(|f| f.0).chain(["text"]) {
                let array = string_column(&batch, field)?;
                if field != "block_id" && array.null_count() != 0 {
                    return Err(bound_error());
                }
            }
            let hits = decode_fts_hits(&[batch])?;
            results.extend(hits);
        }
        Ok(results)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn candidate_admission_never_queues_and_enforces_rows_and_bytes() {
        let pool = CandidateBudget::new(1).unwrap();
        let mut first = pool.try_reserve().unwrap();
        assert!(pool.try_reserve().is_err());
        assert!(first.charge(CANDIDATE_ROWS, 0).is_ok());
        assert!(first.charge(1, 0).is_err());
        drop(first);
        let mut second = pool.try_reserve().unwrap();
        assert!(second.charge(0, CANDIDATE_BYTES).is_err());
        assert!(CandidateBudget::new(0).is_err());
        assert!(CandidateBudget::new(17).is_err());
    }
    #[tokio::test]
    async fn backend_projection_bounds_unicode_text_and_rejects_oversized_metadata() {
        use super::super::tests::{embedding, fts_chunk, temp_database};
        let path = temp_database("bounded-fts");
        let metadata = embedding("bounded-fts-v1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .unwrap();
        let text = format!("harbour {}", "ö".repeat(10_000));
        let rows = [EmbeddedChunk::new(
            fts_chunk("page-a", "chunk-a", "Harbour", &text),
            vec![1.0, 0.0, 0.0],
        )];
        table.upsert(&metadata, &rows).await.unwrap();
        table.optimize_fts_index().await.unwrap();
        let pool = CandidateBudget::new(1).unwrap();
        let mut reservation = pool.try_reserve().unwrap();
        let hits = table
            .bounded_fts_query_scoped(
                "text",
                "harbour",
                4,
                Some("page_id = 'page-a'"),
                &mut reservation,
            )
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].text.chars().count(), 2000);
        assert_eq!(hits[0].page_id, "page-a");
        assert_eq!(hits[0].text, text.chars().take(2000).collect::<String>());
        assert!(
            table
                .bounded_fts_query_scoped(
                    "text",
                    "harbour",
                    4,
                    Some("page_id = 'excluded'"),
                    &mut reservation
                )
                .await
                .unwrap()
                .is_empty()
        );
        drop(reservation);
        let oversized = fts_chunk("page-b", "chunk-b", &"title".repeat(300), "harbour");
        table
            .upsert(
                &metadata,
                &[EmbeddedChunk::new(oversized, vec![0.0, 1.0, 0.0])],
            )
            .await
            .unwrap();
        table.optimize_fts_index().await.unwrap();
        let mut reservation = pool.try_reserve().unwrap();
        assert!(
            table
                .bounded_fts_query_scoped("text", "harbour", 4, None, &mut reservation)
                .await
                .is_err()
        );
        drop(table);
        std::fs::remove_dir_all(path).unwrap();
    }
}
