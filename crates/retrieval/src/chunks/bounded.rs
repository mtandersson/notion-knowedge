//! Bounded native candidate projection for the opt-in lexical executor.
//! This module does not install a new public MCP search policy.
use super::*;
use crate::lexical_dictionary::{Counters, Dictionary};
use arrow_array::BooleanArray;
use lancedb::index::scalar::PhraseQuery;
use std::{
    future::Future,
    time::{Duration, Instant},
};
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
            bytes: 24 * 1024 * 1024,
            cpu: Duration::ZERO,
        })
    }
}
/// Keep alive until candidate merging is complete. Counters conservatively
/// include consumed backend batches and owned candidates even after release.
pub struct CandidateReservation {
    _permit: OwnedSemaphorePermit,
    rows: usize,
    bytes: usize,
    cpu: Duration,
}
impl CandidateReservation {
    fn account_cpu(&mut self, start: Instant) -> Result<(), ChunkTableError> {
        self.cpu = self
            .cpu
            .checked_add(start.elapsed())
            .ok_or_else(bound_error)?;
        if self.cpu > Duration::from_millis(50) {
            return Err(bound_error());
        }
        Ok(())
    }
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
fn projection(column: &str, raw_phrase: Option<&str>) -> Select {
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
    let phrase_match = raw_phrase
        .map(|phrase| {
            let literal = format!("'{}'", phrase.replace('\'', "''"));
            if matches!(column, "chunk_id" | "page_id") {
                format!("{column} = {literal}")
            } else {
                format!("contains(lower({column}),lower({literal}))")
            }
        })
        .unwrap_or_else(|| "true".into());
    columns.push(("_phrase_match".into(), phrase_match));
    Select::Dynamic(columns)
}
struct Branch<'a> {
    column: &'a str,
    fts: FullTextSearchQuery,
    raw_phrase: Option<&'a str>,
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
        self.bounded_branch(
            Branch {
                column,
                fts,
                raw_phrase: quoted_phrase(query),
            },
            limit,
            predicate,
            reservation,
        )
        .await
    }
    async fn bounded_branch(
        &self,
        branch: Branch<'_>,
        limit: usize,
        predicate: Option<&str>,
        reservation: &mut CandidateReservation,
    ) -> Result<Vec<FtsIndexHit>, ChunkTableError> {
        let mut search = self
            .table
            .query()
            .full_text_search(branch.fts)
            .select(projection(branch.column, branch.raw_phrase))
            .limit(limit);
        if let Some(predicate) = predicate {
            search = search.only_if(predicate);
        }
        let mut stream = search.execute().await?;
        let mut results = Vec::new();
        while let Some(batch) = stream.try_next().await? {
            let cpu_start = Instant::now();
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
            let phrase_match = batch
                .column_by_name("_phrase_match")
                .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(bound_error)?;
            let retained: Vec<bool> = phrase_match
                .iter()
                .map(|value| value.unwrap_or(false))
                .collect();
            let hits = decode_fts_hits(&[batch])?;
            results.extend(
                hits.into_iter()
                    .zip(retained)
                    .filter_map(|(hit, keep)| keep.then_some(hit)),
            );
            reservation.account_cpu(cpu_start)?;
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

/// Additive internal evidence, separate from authoritative source citations.
/// Contains no private query text or generated phrase strings.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LexicalMatch {
    Raw {
        field: String,
    },
    LexicalDictionaryV1 {
        manifest_digest: String,
        field: String,
        planner_ordinal: u8,
    },
}
#[derive(Debug)]
pub struct LexicalCandidate {
    pub hit: SearchHit,
    pub matches: Vec<LexicalMatch>,
}
pub struct BoundedLexicalResult {
    pub candidates: Vec<LexicalCandidate>,
    pub planning: Counters,
}
fn has_raw(candidate: &LexicalCandidate) -> bool {
    candidate
        .matches
        .iter()
        .any(|e| matches!(e, LexicalMatch::Raw { .. }))
}
fn generated_count(candidate: &LexicalCandidate) -> u32 {
    candidate
        .matches
        .iter()
        .fold(0u8, |mask, evidence| match evidence {
            LexicalMatch::LexicalDictionaryV1 {
                planner_ordinal, ..
            } => mask | (1u8 << planner_ordinal),
            _ => mask,
        })
        .count_ones()
}
fn earliest(candidate: &LexicalCandidate) -> u8 {
    candidate
        .matches
        .iter()
        .filter_map(|e| match e {
            LexicalMatch::LexicalDictionaryV1 {
                planner_ordinal, ..
            } => Some(*planner_ordinal),
            _ => None,
        })
        .min()
        .unwrap_or(0)
}
fn compare_candidates(a: &LexicalCandidate, b: &LexicalCandidate) -> std::cmp::Ordering {
    b.hit
        .score
        .total_cmp(&a.hit.score)
        .then_with(|| has_raw(b).cmp(&has_raw(a)))
        .then_with(|| generated_count(a).cmp(&generated_count(b)))
        .then_with(|| earliest(a).cmp(&earliest(b)))
        .then_with(|| a.hit.source.page_id.cmp(&b.hit.source.page_id))
        .then_with(|| a.hit.source.chunk_id.cmp(&b.hit.source.chunk_id))
}
fn merge_hit(
    best: &mut HashMap<(String, String), LexicalCandidate>,
    hit: FtsIndexHit,
    band: f32,
    evidence: LexicalMatch,
) -> Result<(), ChunkTableError> {
    let native = hit.score.max(0.0);
    let score = band + native / (1.0 + native);
    if !score.is_finite() {
        return Err(bound_error());
    }
    let key = (hit.page_id.clone(), hit.chunk_id.clone());
    if let Some(existing) = best.get_mut(&key) {
        existing.hit.score = existing.hit.score.max(score);
        if !existing.matches.contains(&evidence) {
            if existing.matches.len() == 20 {
                return Err(bound_error());
            }
            existing.matches.push(evidence);
            existing.matches.sort();
        }
    } else {
        let text = notion_knowledge_core::search::snippet(&hit.text, 2000);
        best.insert(
            key,
            LexicalCandidate {
                hit: SearchHit {
                    matched_paths: vec![],
                    text,
                    score,
                    source: SearchSource {
                        last_edited_time: hit.last_edited_time,
                        page_id: hit.page_id,
                        chunk_id: hit.chunk_id,
                        url: hit.url,
                        title: hit.title,
                        heading_path: hit.heading_path,
                        block_id: hit.block_id,
                    },
                },
                matches: vec![evidence],
            },
        );
    }
    Ok(())
}
fn syntactic_identifier(query: &str) -> bool {
    notion_knowledge_notion::pages::page_id(query).is_ok()
        || query.strip_prefix("nk-chunk-v1:").is_some_and(|suffix| {
            !suffix.is_empty()
                && query.len() <= 128
                && !suffix.chars().any(|c| c.is_whitespace() || c.is_control())
        })
}
impl LanceChunkTable {
    /// Trusted internal execution primitive. Runtime installation/binding is
    /// deliberately separate. The caller's existing tool permit stays held;
    /// cancellation/deadline drop this complete future, never detached work.
    pub async fn expanded_lexical_search<C>(
        &self,
        query: LexicalQuery,
        dictionary: &Dictionary,
        pool: &CandidateBudget,
        deadline: tokio::time::Instant,
        cancel: C,
    ) -> Result<BoundedLexicalResult, ChunkTableError>
    where
        C: Future<Output = ()> + Send,
    {
        tokio::select! { biased;
            _=cancel=>Err(bound_error()),
            _=tokio::time::sleep_until(deadline)=>Err(bound_error()),
            result=self.expanded_lexical_inner(query,dictionary,pool)=>result,
        }
    }
    pub(super) async fn expanded_lexical_inner(
        &self,
        query: LexicalQuery,
        dictionary: &Dictionary,
        pool: &CandidateBudget,
    ) -> Result<BoundedLexicalResult, ChunkTableError> {
        validate_lexical_query(&query)?;
        let mut reservation = pool.try_reserve()?;
        let snapshot = self.read_snapshot().await?;
        let predicate = snapshot
            .bounded_metadata_predicate(&query, &mut reservation)
            .await?;
        let limit = query.limit.saturating_mul(4).min(400);
        let mut best = HashMap::new();
        let mut exact_identifier = syntactic_identifier(query.query.trim());
        for (field, band) in [
            ("chunk_id", 4.0),
            ("page_id", 4.0),
            ("title", 2.0),
            ("text", 1.0),
        ] {
            let hits = snapshot
                .bounded_fts_query_scoped(
                    field,
                    &query.query,
                    limit,
                    predicate.as_deref(),
                    &mut reservation,
                )
                .await?;
            let start = Instant::now();
            for hit in hits {
                exact_identifier |=
                    hit.chunk_id == query.query.trim() || hit.page_id == query.query.trim();
                merge_hit(
                    &mut best,
                    hit,
                    band,
                    LexicalMatch::Raw {
                        field: field.into(),
                    },
                )?;
            }
            reservation.account_cpu(start)?;
        }
        let start = Instant::now();
        let plan = dictionary.plan(&query.query, exact_identifier);
        reservation.account_cpu(start)?;
        for phrase in plan.phrases() {
            for (field, band) in [("title", 2.0), ("text", 1.0)] {
                // Native typed PhraseQuery prevents operator interpretation;
                // it never builds FTS syntax from dictionary text.
                let fts = FullTextSearchQuery::new_query(
                    PhraseQuery::new(phrase.text().to_owned()).into(),
                )
                .with_column(field.into())
                .map_err(|_| bound_error())?;
                let hits = snapshot
                    .bounded_branch(
                        Branch {
                            column: field,
                            fts,
                            raw_phrase: None,
                        },
                        limit,
                        predicate.as_deref(),
                        &mut reservation,
                    )
                    .await?;
                let start = Instant::now();
                for hit in hits {
                    merge_hit(
                        &mut best,
                        hit,
                        band,
                        LexicalMatch::LexicalDictionaryV1 {
                            manifest_digest: dictionary.manifest_digest().into(),
                            field: field.into(),
                            planner_ordinal: phrase.planner_ordinal(),
                        },
                    )?;
                }
                reservation.account_cpu(start)?;
            }
        }
        let start = Instant::now();
        let mut candidates: Vec<_> = best.into_values().collect();
        candidates.sort_by(compare_candidates);
        candidates.truncate(query.limit);
        reservation.account_cpu(start)?;
        Ok(BoundedLexicalResult {
            candidates,
            planning: plan.counters,
        })
    }
    async fn bounded_metadata_predicate(
        &self,
        query: &LexicalQuery,
        reservation: &mut CandidateReservation,
    ) -> Result<Option<String>, ChunkTableError> {
        let scalar = lexical_filter_predicate(query)?;
        if query.metadata.edited.is_none() && query.metadata.properties.is_none() {
            return Ok(scalar);
        }
        let mut scan=self.table.query().select(Select::Dynamic(vec![
            ("chunk_id".into(),"CAST(left(chunk_id,128) AS STRING)".into()),
            ("last_edited_time".into(),"CAST(left(last_edited_time,64) AS STRING)".into()),
            ("properties_json".into(),"CAST(left(properties_json,4096) AS STRING)".into()),
            ("_metadata_overflow".into(),"char_length(chunk_id)>128 OR char_length(last_edited_time)>64 OR char_length(properties_json)>4096".into()),
        ])).limit(CANDIDATE_ROWS+1);
        if let Some(predicate) = &scalar {
            scan = scan.only_if(predicate);
        }
        let mut stream = scan.execute().await?;
        let mut ids = Vec::new();
        let mut rows = 0;
        while let Some(batch) = stream.try_next().await? {
            let start = Instant::now();
            rows += batch.num_rows();
            if rows > CANDIDATE_ROWS {
                return Err(bound_error());
            }
            let overflow = batch
                .column_by_name("_metadata_overflow")
                .and_then(|a| a.as_any().downcast_ref::<BooleanArray>())
                .ok_or_else(bound_error)?;
            if overflow.null_count() != 0 || overflow.iter().flatten().any(|v| v) {
                return Err(bound_error());
            }
            reservation.charge(
                0,
                batch
                    .get_array_memory_size()
                    .checked_mul(40)
                    .ok_or_else(bound_error)?,
            )?;
            let chunk_ids = string_column(&batch, "chunk_id")?;
            let edited = string_column(&batch, "last_edited_time")?;
            let properties = string_column(&batch, "properties_json")?;
            if chunk_ids.null_count() != 0
                || edited.null_count() != 0
                || properties.null_count() != 0
            {
                return Err(bound_error());
            }
            for row in 0..batch.num_rows() {
                let values = serde_json::from_str(properties.value(row))
                    .map_err(|_| ChunkTableError::Serialization)?;
                if query
                    .metadata
                    .matches_values(edited.value(row), &values)
                    .map_err(|_| bound_error())?
                {
                    ids.push(chunk_ids.value(row).to_owned());
                }
            }
            reservation.account_cpu(start)?;
        }
        let start = Instant::now();
        let matches = if ids.is_empty() {
            "FALSE".into()
        } else {
            format!("({})", id_list_predicate("chunk_id", &ids))
        };
        let predicate = Some(match scalar {
            Some(scope) => format!("({scope}) AND {matches}"),
            None => matches,
        });
        reservation.account_cpu(start)?;
        Ok(predicate)
    }
}

#[cfg(test)]
mod execution_tests {
    use super::super::tests::{embedding, fts_chunk, temp_database};
    use super::*;
    fn query(text: &str) -> LexicalQuery {
        LexicalQuery {
            query: text.into(),
            limit: 10,
            page_ids: None,
            root_page_ids: Some(vec!["root".into()]),
            metadata: Default::default(),
        }
    }
    async fn run(
        table: &LanceChunkTable,
        text: &str,
        dictionary: &Dictionary,
        pool: &CandidateBudget,
    ) -> BoundedLexicalResult {
        table
            .expanded_lexical_search(
                query(text),
                dictionary,
                pool,
                tokio::time::Instant::now() + Duration::from_secs(30),
                std::future::pending(),
            )
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn literal_crosslanguage_matches_retain_original_sources_and_scoped_provenance() {
        let path = temp_database("dictionary-native");
        let metadata = embedding("dictionary-native-v1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata.clone())
            .await
            .unwrap();
        let mut excluded = fts_chunk(
            "bbbbbbbb-bbbb-bbbb-cafe-bbbbbbbbbbbb",
            "excluded-chunk",
            "Hamn",
            "Får aldrig lämnas ut",
        );
        excluded.metadata.source.root_page_id = "private-root".into();
        let chunks = [
            fts_chunk(
                "english",
                "english-chunk",
                "Harbour",
                "Original English waterfront notes.",
            ),
            fts_chunk(
                "swedish",
                "swedish-chunk",
                "Hamn",
                "Original svensk text om båtar.",
            ),
            fts_chunk("coffee", "coffee-chunk", "Kafé", "Originala kaffebetyg."),
            fts_chunk(
                "phrase",
                "phrase-chunk",
                "Skyddad hamn",
                "En kaj bakom en pir.",
            ),
            fts_chunk(
                "near",
                "near-chunk",
                "Skyddad mycket stor hamn",
                "Annan kaj.",
            ),
            excluded,
        ];
        let rows: Vec<_> = chunks
            .into_iter()
            .map(|c| EmbeddedChunk::new(c, vec![1.0, 0.0, 0.0]))
            .collect();
        table.upsert(&metadata, &rows).await.unwrap();
        table.optimize_fts_index().await.unwrap();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[
            ("harbour", "hamn"),
            ("safe harbour", "skyddad hamn"),
            ("cafe", "kafé"),
        ]);
        let pool = CandidateBudget::new(1).unwrap();
        // An old non-positioned index cannot complete a dictionary phrase
        // branch: failure discards even successfully obtained raw candidates.
        assert!(
            table
                .expanded_lexical_search(
                    query("harbour"),
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    std::future::pending()
                )
                .await
                .is_err()
        );
        table
            .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
            .await
            .unwrap();
        let forward = run(&table, "harbour", &dictionary, &pool).await;
        let swedish = forward
            .candidates
            .iter()
            .find(|c| c.hit.source.page_id == "swedish")
            .expect("literal Swedish candidate");
        assert_eq!(swedish.hit.text, "Original svensk text om båtar.");
        assert_eq!(swedish.hit.source.url, "https://example.invalid/swedish");
        assert!(swedish.matches.iter().any(|e|matches!(e,LexicalMatch::LexicalDictionaryV1{manifest_digest,field,planner_ordinal:0} if manifest_digest==dictionary.manifest_digest() && field=="title")));
        assert!(
            !forward
                .candidates
                .iter()
                .any(|c| c.hit.source.page_id == "bbbbbbbb-bbbb-bbbb-cafe-bbbbbbbbbbbb")
        );
        let reverse = run(&table, "hamn", &dictionary, &pool).await;
        assert!(
            reverse
                .candidates
                .iter()
                .any(|c| c.hit.source.page_id == "english")
        );
        let phrase = run(&table, "safe harbour", &dictionary, &pool).await;
        assert!(
            phrase
                .candidates
                .iter()
                .any(|c| c.hit.source.page_id == "phrase")
        );
        assert!(
            !phrase
                .candidates
                .iter()
                .any(|c| c.hit.source.page_id == "near")
        );
        let exact = run(&table, "swedish-chunk", &dictionary, &pool).await;
        assert_eq!(exact.planning.matched_units, 0);
        assert_eq!(exact.candidates[0].hit.source.chunk_id, "swedish-chunk");
        let quoted = run(&table, "\"harbour\"", &dictionary, &pool).await;
        assert_eq!(quoted.planning.matched_units, 0);
        assert!(
            !quoted
                .candidates
                .iter()
                .any(|c| c.hit.source.page_id == "swedish")
        );
        for identifier in [
            "aaaaaaaa-aaaa-aaaa-cafe-aaaaaaaaaaaa",
            "bbbbbbbb-bbbb-bbbb-cafe-bbbbbbbbbbbb",
            "nk-chunk-v1:cafe",
        ] {
            let missing_or_excluded = run(&table, identifier, &dictionary, &pool).await;
            assert_eq!(missing_or_excluded.planning.matched_units, 0);
            assert!(!missing_or_excluded.candidates.iter().any(|candidate| {
                candidate
                    .matches
                    .iter()
                    .any(|e| matches!(e, LexicalMatch::LexicalDictionaryV1 { .. }))
            }));
        }
        let repeated = run(&table, "harbour", &dictionary, &pool).await;
        let ids = |r: &BoundedLexicalResult| {
            r.candidates
                .iter()
                .map(|c| (&c.hit.source.page_id, c.hit.score, c.matches.clone()))
                .map(|(id, score, matches)| (id.clone(), score, matches))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&forward), ids(&repeated));
        // Raw English candidates succeed first; a later translated title branch
        // overflows metadata. No earlier partial result is returned.
        let bad = fts_chunk(
            "oversized-derived",
            "oversized-derived-chunk",
            &"hamn ".repeat(300),
            "Original source",
        );
        table
            .upsert(&metadata, &[EmbeddedChunk::new(bad, vec![1.0, 0.0, 0.0])])
            .await
            .unwrap();
        table.optimize_fts_index().await.unwrap();
        assert!(
            table
                .expanded_lexical_search(
                    query("harbour"),
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    std::future::pending()
                )
                .await
                .is_err()
        );
        drop(table);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn cancelled_expired_or_unadmitted_requests_do_not_return_partial_hits() {
        let path = temp_database("dictionary-controls");
        let metadata = embedding("dictionary-controls-v1", 3);
        let table = LanceChunkTable::create(&path, "chunks", metadata)
            .await
            .unwrap();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("harbour", "hamn")]);
        let pool = CandidateBudget::new(1).unwrap();
        assert!(
            table
                .expanded_lexical_search(
                    query("harbour"),
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    std::future::ready(())
                )
                .await
                .is_err()
        );
        assert!(
            table
                .expanded_lexical_search(
                    query("harbour"),
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now(),
                    std::future::pending()
                )
                .await
                .is_err()
        );
        let reservation = pool.try_reserve().unwrap();
        assert!(
            table
                .expanded_lexical_search(
                    query("harbour"),
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    std::future::pending()
                )
                .await
                .is_err()
        );
        drop(reservation);
        assert!(pool.try_reserve().is_ok());
        drop(table);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::super::tests::{embedding, fts_chunk, temp_database};
    use super::*;
    use notion_knowledge_core::search_filters::{DateRange, PropertyFilter};
    #[tokio::test]
    async fn metadata_filters_precede_candidate_limits_and_phrase_checks_use_complete_source() {
        let path = temp_database("expanded-metadata");
        let embedding = embedding("expanded-metadata-v1", 3);
        let table = LanceChunkTable::create(&path, "chunks", embedding.clone())
            .await
            .unwrap();
        let mut chunks = Vec::new();
        for ordinal in 0..12 {
            let mut chunk = fts_chunk(
                &format!("wrong-{ordinal}"),
                &format!("wrong-chunk-{ordinal}"),
                "Harbour hamn",
                "harbour hamn",
            );
            chunk.metadata.last_edited_time = "2025-01-01T00:00:00Z".into();
            chunk
                .metadata
                .properties
                .insert("state".into(), PropertyValue::Text("closed".into()));
            chunks.push(chunk);
        }
        let mut allowed = fts_chunk(
            "allowed",
            "allowed-chunk",
            "Pier notes",
            "En hamn vid kusten.",
        );
        allowed
            .metadata
            .properties
            .insert("state".into(), PropertyValue::Text("ready".into()));
        chunks.push(allowed);
        let mut oversized = fts_chunk("out-of-root", "out-of-root-chunk", "Harbour", "hamn");
        oversized.metadata.source.root_page_id = "excluded-root".into();
        oversized
            .metadata
            .properties
            .insert("large".into(), PropertyValue::Text("x".repeat(5000)));
        chunks.push(oversized.clone());
        let long = fts_chunk(
            "long-source",
            "long-chunk",
            "Late phrase",
            &format!("{}safe harbour", "z ".repeat(1500)),
        );
        chunks.push(long);
        let rows: Vec<_> = chunks
            .into_iter()
            .map(|c| EmbeddedChunk::new(c, vec![1.0, 0.0, 0.0]))
            .collect();
        table.upsert(&embedding, &rows).await.unwrap();
        table
            .rebuild_fts_index(&FtsIndexConfig::lexical_expansion_v1())
            .await
            .unwrap();
        let dictionary = crate::lexical_dictionary::tests::dictionary(&[("harbour", "hamn")]);
        let pool = CandidateBudget::new(1).unwrap();
        let mut query = LexicalQuery {
            query: "harbour".into(),
            limit: 1,
            page_ids: None,
            root_page_ids: Some(vec!["root".into()]),
            metadata: Default::default(),
        };
        query.metadata.edited = Some(DateRange {
            from: Some("2026-01-01T00:00:00Z".into()),
            until: None,
        });
        query.metadata.properties = Some(vec![PropertyFilter::Equals {
            property_id: "state".into(),
            value: PropertyValue::Text("ready".into()),
        }]);
        let result = table
            .expanded_lexical_search(
                query.clone(),
                &dictionary,
                &pool,
                tokio::time::Instant::now() + Duration::from_secs(30),
                std::future::pending(),
            )
            .await
            .unwrap();
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.candidates[0].hit.source.page_id, "allowed");
        let quote = LexicalQuery {
            query: "\"safe harbour\"".into(),
            limit: 1,
            page_ids: Some(vec!["long-source".into()]),
            root_page_ids: Some(vec!["root".into()]),
            metadata: Default::default(),
        };
        let result = table
            .expanded_lexical_search(
                quote,
                &dictionary,
                &pool,
                tokio::time::Instant::now() + Duration::from_secs(30),
                std::future::pending(),
            )
            .await
            .unwrap();
        assert_eq!(result.candidates.len(), 1);
        assert_eq!(result.candidates[0].hit.source.page_id, "long-source");
        assert_eq!(result.candidates[0].hit.text.chars().count(), 2000);
        assert!(!result.candidates[0].hit.text.contains("harbour"));
        oversized.metadata.source.root_page_id = "root".into();
        table
            .upsert(
                &embedding,
                &[EmbeddedChunk::new(oversized, vec![1.0, 0.0, 0.0])],
            )
            .await
            .unwrap();
        table.optimize_fts_index().await.unwrap();
        assert!(
            table
                .expanded_lexical_search(
                    query,
                    &dictionary,
                    &pool,
                    tokio::time::Instant::now() + Duration::from_secs(30),
                    std::future::pending()
                )
                .await
                .is_err()
        );
        drop(table);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[cfg(test)]
mod ranking_tests {
    use super::*;
    fn hit(page: &str) -> FtsIndexHit {
        FtsIndexHit {
            page_id: page.into(),
            chunk_id: format!("chunk-{page}"),
            root_page_id: "root".into(),
            block_id: None,
            url: format!("https://example.invalid/{page}"),
            title: "Original title".into(),
            heading_path: vec![],
            last_edited_time: "2026-10-01T00:00:00Z".into(),
            text: "Original source text".into(),
            score: 1.0,
        }
    }
    fn derived(ordinal: u8) -> LexicalMatch {
        LexicalMatch::LexicalDictionaryV1 {
            manifest_digest: "public-asset-digest".into(),
            field: "title".into(),
            planner_ordinal: ordinal,
        }
    }
    #[test]
    fn equal_scores_prefer_raw_then_fewer_generated_queries_then_ordinal_and_ids() {
        let mut best = HashMap::new();
        merge_hit(
            &mut best,
            hit("z-raw"),
            2.0,
            LexicalMatch::Raw {
                field: "title".into(),
            },
        )
        .unwrap();
        merge_hit(&mut best, hit("a-many"), 2.0, derived(0)).unwrap();
        merge_hit(&mut best, hit("a-many"), 2.0, derived(1)).unwrap();
        merge_hit(&mut best, hit("b-later"), 2.0, derived(1)).unwrap();
        merge_hit(&mut best, hit("c-early"), 2.0, derived(0)).unwrap();
        merge_hit(&mut best, hit("d-early"), 2.0, derived(0)).unwrap();
        let mut candidates: Vec<_> = best.into_values().collect();
        candidates.sort_by(compare_candidates);
        let pages: Vec<_> = candidates
            .iter()
            .map(|c| c.hit.source.page_id.as_str())
            .collect();
        assert_eq!(pages, ["z-raw", "c-early", "d-early", "b-later", "a-many"]);
        assert!(candidates.iter().all(|c| c.hit.score == 2.5));
    }
    #[test]
    fn repeated_polysemous_evidence_never_adds_scores_or_replaces_source_citations() {
        let mut best = HashMap::new();
        merge_hit(&mut best, hit("source"), 2.0, derived(0)).unwrap();
        merge_hit(&mut best, hit("source"), 2.0, derived(0)).unwrap();
        merge_hit(&mut best, hit("source"), 2.0, derived(1)).unwrap();
        merge_hit(
            &mut best,
            hit("source"),
            1.0,
            LexicalMatch::Raw {
                field: "text".into(),
            },
        )
        .unwrap();
        let candidate = best.into_values().next().unwrap();
        assert_eq!(candidate.hit.score, 2.5);
        assert_eq!(candidate.matches.len(), 3);
        assert_eq!(candidate.hit.text, "Original source text");
        assert_eq!(candidate.hit.source.url, "https://example.invalid/source");
        assert!(has_raw(&candidate));
    }
    #[test]
    fn expansion_cpu_overflow_fails_and_releases_admission() {
        let pool = CandidateBudget::new(1).unwrap();
        let mut reservation = pool.try_reserve().unwrap();
        let earlier = Instant::now()
            .checked_sub(Duration::from_millis(51))
            .unwrap();
        assert!(reservation.account_cpu(earlier).is_err());
        drop(reservation);
        assert!(pool.try_reserve().is_ok());
    }
}
