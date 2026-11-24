//! Commit canonical text first and use the existing vector table as its derived
//! semantic projection. Only the source/text revision receives the page receipt.
use super::*;
use crate::chunks::{semantic_jobs as jobs, text, windows};
use notion_knowledge_core::chunking::table_section_boundaries;

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Local text revision and the result of model work prepared outside the writer
/// lease. Commit verifies both again before changing the searchable projection.
pub struct PreparedSemanticProjection {
    job: jobs::Job,
    vectors: Option<Result<ProjectionStep, ChunkTableError>>,
}

enum ProjectionStep {
    Complete(Vec<EmbeddedChunk>),
    Staged(Vec<EmbeddedChunk>, Option<EmbeddingError>),
}

const INFERENCES_PER_STEP: usize = 8;

impl GuardedChunkTable {
    /// Imported v1 rows predate the publication table. A v2 receipt always
    /// writes its revision before touching the text, including a replay
    /// interrupted between the LanceDB merge and SQLite acknowledgment.
    pub(crate) fn text_matches_revision(
        &self,
        page: &str,
        revision: &str,
        chunks: &[IndexedChunk],
    ) -> Result<bool, ChunkTableError> {
        match jobs::text_revision(&self.index_directory, page)? {
            Some((current, text)) => Ok(current == revision && text == jobs::fingerprint(chunks)?),
            None => Ok(true),
        }
    }

    pub(crate) async fn commit_text_with_authority<F, Fut, Authority>(
        &self,
        operation: PageOperation,
        previous: Vec<IndexedChunk>,
        chunks: Vec<IndexedChunk>,
        lease: Lease,
        clock: Clock,
        verify: F,
    ) -> Result<(CommitOutcome, ChunkDiffMetrics), CommitError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Authority, FailureClass>> + Send + 'static,
        Authority: Send + 'static,
    {
        let page = operation.checkpoint().page_id().to_owned();
        let revision = operation.revision().to_owned();
        let deletion = operation.action() == PageAction::Delete;
        if operation.action() == PageAction::Unchanged
            || (deletion && !chunks.is_empty())
            || chunks.iter().any(|chunk| {
                chunk.metadata.source.workspace_id != self.binding().workspace()
                    || !self
                        .binding()
                        .roots()
                        .contains(&chunk.metadata.source.root_page_id)
                    || operation
                        .checkpoint()
                        .last_edited_time()
                        .is_some_and(|time| chunk.metadata.last_edited_time != time)
            })
        {
            return Err(CommitError::InvalidInput);
        }
        validate_page_snapshot(&page, &chunks).map_err(|_| CommitError::InvalidInput)?;
        let diff = chunk_diff(&previous, &chunks);
        let fingerprint =
            jobs::fingerprint(&chunks).map_err(|e| CommitError::Operation(failure(e)))?;
        let this = self.clone();
        let maintenance = (lease.clone(), clock.clone());
        let owned_clock = clock.clone();
        let outcome = self
            .coordinator
            .submit_with_authority(
                operation.replacing_page_projection(),
                lease,
                clock,
                move |context| async move {
                    let text = this.provision_text(&context).await.map_err(failure)?;
                    let actual = text.page_chunks(&page).await.map_err(failure)?;
                    if actual != previous {
                        return Err(FailureClass::Conflict);
                    }
                    verify_no_collisions(&text, &page, chunks.iter().map(|c| &c.chunk_id)).await?;
                    context.revalidate().map_err(fence_failure)?;
                    let authority = verify().await?;
                    context.revalidate().map_err(fence_failure)?;
                    lexical_binding::verify(&this.index_directory, &this.lexical_binding)
                        .map_err(failure)?;
                    jobs::set_text_revision(&this.index_directory, &page, &revision, &fingerprint)
                        .map_err(failure)?;
                    let prior_fingerprint = jobs::fingerprint(&actual).map_err(failure)?;
                    let changed = deletion || prior_fingerprint != fingerprint;
                    if changed {
                        let semantic = this.open_semantic().await.map_err(failure)?;
                        let old = semantic
                            .rows_matching(page_predicate(&page))
                            .await
                            .map_err(failure)?;
                        let staged =
                            match jobs::for_page(&this.index_directory, &page).map_err(failure)? {
                                Some(job) if job.text == prior_fingerprint => {
                                    jobs::reusable(&this.index_directory, &job).map_err(failure)?
                                }
                                _ => HashMap::new(),
                            };
                        let (reusable, missing) =
                            reusable_rows(&old, &actual, &chunks, staged).map_err(failure)?;
                        // Persist the retry before changing either table. An
                        // interrupted receipt cannot authorize searching; a retry
                        // also compares the exact current text before any vector work.
                        if missing {
                            jobs::enqueue(
                                &this.index_directory,
                                &page,
                                &revision,
                                &fingerprint,
                                &reusable,
                                owned_clock(),
                            )
                            .map_err(failure)?;
                        } else {
                            jobs::clear(&this.index_directory, &page).map_err(failure)?;
                        }
                        let _merge = page_timings::timer(Bucket::LanceMerge);
                        // An edited page with any blocked vector leaves the
                        // active projection in one deletion. Its reusable
                        // vectors survive only in the revision-bound job.
                        if !old.is_empty() || (!missing && !reusable.is_empty()) {
                            let visible = if missing { &[] } else { reusable.as_slice() };
                            replace_vectors(&semantic, &page, visible)
                                .await
                                .map_err(failure)?;
                        }
                        context.revalidate().map_err(fence_failure)?;
                        text::replace(&text, context.binding(), &page, &chunks)
                            .await
                            .map_err(failure)?;
                        if !missing {
                            let projected = jobs::vector_fingerprint(&reusable).map_err(failure)?;
                            jobs::set_semantic_revision(&this.index_directory, &page, &projected)
                                .map_err(failure)?;
                        }
                    } else {
                        this.repair_semantic_if_missing(
                            &context,
                            &page,
                            &revision,
                            &chunks,
                            owned_clock(),
                        )
                        .await?;
                    }
                    Ok(authority)
                },
            )
            .await?;
        if outcome == CommitOutcome::Applied {
            self.optimize_after_commit(maintenance.0, maintenance.1)
                .await;
        }
        Ok((outcome, diff))
    }

    /// Preserve ordinary reconciliation's repair of missing derived rows, using
    /// the unchanged canonical source and preserving compatible vectors.
    pub(crate) async fn repair_semantic_if_missing(
        &self,
        context: &crate::commit::CommitContext,
        page: &str,
        revision: &str,
        chunks: &[IndexedChunk],
        now: i64,
    ) -> Result<(), FailureClass> {
        if chunks.is_empty() {
            return Ok(());
        }
        if !self.text_projection_installed().map_err(failure)? {
            return Ok(());
        }
        let text = jobs::fingerprint(chunks).map_err(failure)?;
        if let Some(job) = jobs::for_page(&self.index_directory, page).map_err(failure)? {
            if job.text == text {
                return Ok(());
            }
            context.revalidate().map_err(fence_failure)?;
            jobs::clear(&self.index_directory, page).map_err(failure)?;
        }
        let semantic = self.open_semantic().await.map_err(failure)?;
        let current = semantic.page_chunks(page).await.map_err(failure)?;
        let expected = jobs::semantic_revision(&self.index_directory, page)
            .map_err(failure)?
            .unwrap_or_else(|| text.clone());
        if jobs::fingerprint(&current).map_err(failure)? == expected {
            return Ok(());
        }
        let old = semantic
            .rows_matching(page_predicate(page))
            .await
            .map_err(failure)?;
        let (reusable, missing) =
            reusable_rows(&old, chunks, chunks, HashMap::new()).map_err(failure)?;
        context.revalidate().map_err(fence_failure)?;
        jobs::set_text_revision(&self.index_directory, page, revision, &text).map_err(failure)?;
        let _merge = page_timings::timer(Bucket::LanceMerge);
        if missing {
            jobs::enqueue(&self.index_directory, page, revision, &text, &reusable, now)
                .map_err(failure)?;
            replace_vectors(&semantic, page, &[])
                .await
                .map_err(failure)?;
        } else {
            replace_vectors(&semantic, page, &reusable)
                .await
                .map_err(failure)?;
            let projected = jobs::vector_fingerprint(&reusable).map_err(failure)?;
            jobs::set_semantic_revision(&self.index_directory, page, &projected)
                .map_err(failure)?;
        }
        Ok(())
    }

    /// Next retry for the sync worker. Admission is serialized by its existing
    /// writer lease. New source commits can supersede this work while embedding.
    pub fn next_semantic_retry(&self, now: i64) -> Result<Option<String>, ChunkTableError> {
        self.coordinator
            .identities()
            .map_err(|_| ChunkTableError::InvalidPath)?;
        if self
            .coordinator
            .has_unfinished_commits()
            .map_err(|_| ChunkTableError::Storage)?
        {
            return Ok(None);
        }
        jobs::next(&self.index_directory, now)
    }

    pub async fn retry_semantic(
        &self,
        page: &str,
        provider: &dyn EmbeddingProvider,
        lease: Lease,
        clock: Clock,
    ) -> Result<bool, CommitError> {
        let now = clock();
        let remaining = lease.expires_at.saturating_sub(now);
        if remaining <= 1 {
            return Ok(false);
        }
        let timeout = std::time::Duration::from_secs((remaining - 1) as u64);
        match self.prepare_semantic(page, provider, now, timeout).await {
            Ok(Some(prepared)) => self.complete_semantic(prepared, lease, clock).await,
            Ok(None) | Err(CommitError::Conflict | CommitError::LeaseLost) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Take a local, revision-bound input snapshot under a read fence, then
    /// release the fence before waiting on the model.
    pub async fn prepare_semantic(
        &self,
        page: &str,
        provider: &dyn EmbeddingProvider,
        now: i64,
        timeout: std::time::Duration,
    ) -> Result<Option<PreparedSemanticProjection>, CommitError> {
        let index_failure = |e| CommitError::Operation(failure(e));
        let guard = self.coordinator.search_guard()?;
        let Some(job) = jobs::for_page(&self.index_directory, page).map_err(index_failure)? else {
            return Ok(None);
        };
        if !job.due.is_some_and(|due| due <= now) {
            return Ok(None);
        }
        let text = self.open_canonical().await.map_err(index_failure)?;
        let canonical = text
            .page_chunks_in_source_order(page)
            .await
            .map_err(index_failure)?;
        if jobs::fingerprint(&canonical).map_err(index_failure)? != job.text {
            return Ok(Some(PreparedSemanticProjection { job, vectors: None }));
        }
        let semantic = self.open_semantic().await.map_err(index_failure)?;
        let old = semantic
            .rows_matching(page_predicate(page))
            .await
            .map_err(index_failure)?;
        let staged = jobs::reusable(&self.index_directory, &job).map_err(index_failure)?;
        drop(guard);
        let deadline = tokio::time::Instant::now() + timeout;
        let vectors = self
            .prepare_vectors(provider, &canonical, &old, staged, deadline)
            .await;
        Ok(Some(PreparedSemanticProjection {
            job,
            vectors: Some(vectors),
        }))
    }

    /// Publish the complete projection, or update retry status for this same job.
    pub async fn complete_semantic(
        &self,
        prepared: PreparedSemanticProjection,
        lease: Lease,
        clock: Clock,
    ) -> Result<bool, CommitError> {
        let PreparedSemanticProjection { job, vectors } = prepared;
        let this = self.clone();
        let owned_clock = clock.clone();
        let maintenance = (lease.clone(), clock.clone());
        let wrote = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owned_wrote = wrote.clone();
        self.coordinator
            .maintain(lease, clock, move |context| async move {
                let current_job =
                    jobs::for_page(&this.index_directory, &job.page).map_err(failure)?;
                if current_job.as_ref() != Some(&job)
                    || this
                        .coordinator
                        .has_unfinished_commits()
                        .map_err(fence_failure)?
                {
                    return Ok(());
                }
                let text = this.open_canonical().await.map_err(failure)?;
                let canonical = text.page_chunks(&job.page).await.map_err(failure)?;
                context.revalidate().map_err(fence_failure)?;
                if jobs::fingerprint(&canonical).map_err(failure)? != job.text {
                    return jobs::clear(&this.index_directory, &job.page).map_err(failure);
                }
                let Some(result) = vectors else {
                    return Ok(());
                };
                match result {
                    Ok(ProjectionStep::Complete(rows)) => {
                        let semantic = this.open_semantic().await.map_err(failure)?;
                        verify_no_collisions(
                            &semantic,
                            &job.page,
                            rows.iter().map(|r| &r.chunk.chunk_id),
                        )
                        .await?;
                        context.revalidate().map_err(fence_failure)?;
                        let _merge = page_timings::timer(Bucket::LanceMerge);
                        replace_vectors(&semantic, &job.page, &rows)
                            .await
                            .map_err(failure)?;
                        // The model may have recovered after initializing an
                        // empty index with no ANN. Build a first native index
                        // before marking the complete projection available.
                        if !semantic
                            .table
                            .list_indices()
                            .await
                            .map_err(|_| FailureClass::Index)?
                            .iter()
                            .any(|index| index.name == CHUNK_VECTOR_INDEX_NAME)
                        {
                            semantic
                                .ensure_vector_index(&VectorIndexConfig {
                                    distance: VectorDistance::Cosine,
                                    ..Default::default()
                                })
                                .await
                                .map_err(failure)?;
                        }
                        let fingerprint = jobs::vector_fingerprint(&rows).map_err(failure)?;
                        jobs::set_semantic_revision(&this.index_directory, &job.page, &fingerprint)
                            .map_err(failure)?;
                        jobs::clear(&this.index_directory, &job.page).map_err(failure)?;
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "operation": "semantic_projection",
                                "outcome": "published",
                                "chunks": rows.len(),
                            })
                        );
                        owned_wrote.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok(ProjectionStep::Staged(rows, error)) => {
                        jobs::stage(&this.index_directory, &job, &rows).map_err(failure)?;
                        if let Some(error) = error {
                            jobs::fail(&this.index_directory, &job, error, owned_clock())
                                .map_err(failure)?;
                        }
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "operation":"semantic_projection",
                                "outcome":"staged",
                                "chunks":rows.len(),
                                "reason":error.map(|failure|format!("{failure:?}")),
                            })
                        );
                    }
                    Err(ChunkTableError::Embedding(error)) => {
                        jobs::fail(&this.index_directory, &job, error, owned_clock())
                            .map_err(failure)?;
                        eprintln!(
                            "{}",
                            serde_json::json!({
                                "operation": "semantic_projection",
                                "outcome": "embedding_deferred",
                                "reason": format!("{error:?}"),
                            })
                        );
                    }
                    Err(error) => return Err(failure(error)),
                }
                Ok(())
            })
            .await?;
        let published = wrote.load(std::sync::atomic::Ordering::SeqCst);
        if published {
            self.optimize_after_commit(maintenance.0, maintenance.1)
                .await;
        }
        Ok(published)
    }

    pub(super) async fn open_semantic(&self) -> Result<LanceChunkTable, ChunkTableError> {
        open_current(
            &self.index_directory,
            &self.table_name,
            &self.embedding,
            &self.lexical_binding,
        )
        .await
    }

    async fn prepare_vectors(
        &self,
        provider: &dyn EmbeddingProvider,
        canonical: &[IndexedChunk],
        old: &[StoredChunk],
        mut staged: HashMap<String, Vec<f32>>,
        deadline: tokio::time::Instant,
    ) -> Result<ProjectionStep, ChunkTableError> {
        self.embedding.ensure_compatible(provider.metadata())?;
        let existing: HashMap<_, _> = old.iter().map(|row| (row.chunk_id.as_str(), row)).collect();
        let mut cached_windows = cached_windows(
            staged
                .keys()
                .map(String::as_str)
                .chain(existing.keys().copied()),
        )?;
        let mut vectors = Vec::new();
        let mut attempts = 0;
        let _embed = page_timings::timer(Bucket::Embed);
        let mut retained_vector = |part: &IndexedChunk| {
            existing
                .get(part.chunk_id.as_str())
                .filter(|row| reusable(row, part))
                .map(|row| row.vector.clone())
                .or_else(|| staged.remove(&part.chunk_id))
        };
        for chunk in canonical {
            if let Some(vector) = retained_vector(chunk) {
                vectors.push(EmbeddedChunk::new(chunk.clone(), vector));
                continue;
            }
            let fingerprint = windows::digest(&chunk.text);
            let prior = cached_windows.remove(&chunk.chunk_id).unwrap_or_default();
            let (parts, _) = rehydrate_windows(chunk, &fingerprint, prior)?;
            let mut previous = None;
            for part in parts {
                let Some(vector) = retained_vector(&part) else {
                    break;
                };
                previous = Some(
                    windows::parse(&part.chunk_id)?
                        .ok_or(EmbeddingError::InvalidInput)?
                        .range,
                );
                vectors.push(EmbeddedChunk::new(part, vector));
            }
            if previous
                .as_ref()
                .is_some_and(|range| range.end == chunk.text.len())
            {
                continue;
            }
            // The provider owns only one bounded window computation.
            // Its next call can resume at the last staged UTF-8 range.
            let input: Arc<str> = Arc::from(chunk.text.as_str());
            let boundaries: Arc<[usize]> = structural_boundaries(&input).into();
            while previous.as_ref().map_or(0, |range| range.end) < input.len() {
                if attempts == INFERENCES_PER_STEP {
                    return Ok(ProjectionStep::Staged(vectors, None));
                }
                let step = async {
                    let covered = previous.as_ref().map_or(0, |range| range.end);
                    let range = provider
                        .next_embedding_window(input.clone(), boundaries.clone(), previous.clone())
                        .await?
                        .filter(|range| {
                            range.start <= covered
                                && range.end > covered
                                && range.end <= input.len()
                        })
                        .ok_or(EmbeddingError::InvalidInput)?;
                    let part = if range.start == 0 && range.end == input.len() {
                        chunk.clone()
                    } else {
                        windows::row(chunk, &fingerprint, range.clone())?
                    };
                    let vector = embedding::embed_batch(provider, std::slice::from_ref(&part.text))
                        .await?
                        .pop()
                        .ok_or(EmbeddingError::InvalidOutput)?;
                    Ok::<_, ChunkTableError>((range, EmbeddedChunk::new(part, vector)))
                };
                match tokio::time::timeout_at(deadline, step)
                    .await
                    .unwrap_or(Err(EmbeddingError::Unavailable.into()))
                {
                    Ok((range, row)) => {
                        attempts += 1;
                        previous = Some(range);
                        vectors.push(row);
                    }
                    Err(ChunkTableError::Embedding(error)) if attempts > 0 => {
                        return Ok(ProjectionStep::Staged(vectors, Some(error)));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        validate_rows(&vectors, self.embedding.dimension())?;
        Ok(ProjectionStep::Complete(vectors))
    }
}

fn page_predicate(page: &str) -> String {
    format!("page_id = {}", sql_string(page))
}

fn chunk_diff(previous: &[IndexedChunk], chunks: &[IndexedChunk]) -> ChunkDiffMetrics {
    let mut old = previous
        .iter()
        .map(|chunk| (chunk.chunk_id.as_str(), &chunk.content_hash))
        .collect::<HashMap<_, _>>();
    let mut diff = ChunkDiffMetrics::default();
    for chunk in chunks {
        match old.remove(chunk.chunk_id.as_str()) {
            Some(hash) if hash == &chunk.content_hash => diff.skipped += 1,
            Some(_) => diff.changed += 1,
            None => diff.added += 1,
        }
    }
    diff.removed = old.len();
    diff
}

fn reusable(old: &StoredChunk, chunk: &IndexedChunk) -> bool {
    old.content_hash == chunk.content_hash && old.text == chunk.text
}

fn reusable_rows(
    rows: &[StoredChunk],
    previous: &[IndexedChunk],
    chunks: &[IndexedChunk],
    mut staged: HashMap<String, Vec<f32>>,
) -> Result<(Vec<EmbeddedChunk>, bool), ChunkTableError> {
    let mut old = HashMap::with_capacity(rows.len());
    for row in rows {
        if old.insert(row.chunk_id.as_str(), row).is_some() {
            return Err(ChunkTableError::InvalidRows(
                "duplicate semantic chunk identity".into(),
            ));
        }
    }
    let previous: HashMap<_, _> = previous
        .iter()
        .map(|chunk| (chunk.chunk_id.as_str(), chunk))
        .collect();
    let mut windowed =
        cached_windows(staged.keys().map(String::as_str).chain(old.keys().copied()))?;
    let mut vectors = Vec::new();
    let mut missing = false;
    for chunk in chunks {
        let admitted = old
            .get(chunk.chunk_id.as_str())
            .filter(|row| reusable(row, chunk))
            .map(|row| row.vector.clone())
            .or_else(|| {
                let previous = previous.get(chunk.chunk_id.as_str())?;
                if previous.content_hash != chunk.content_hash || previous.text != chunk.text {
                    return None;
                }
                staged.remove(&chunk.chunk_id)
            });
        if let Some(vector) = admitted {
            vectors.push(EmbeddedChunk::new(chunk.clone(), vector));
            continue;
        }
        let prior = windowed.remove(&chunk.chunk_id).unwrap_or_default();
        let (parts, _) = rehydrate_windows(chunk, &windows::digest(&chunk.text), prior)?;
        let mut covered = 0;
        for part in parts {
            let vector = old
                .get(part.chunk_id.as_str())
                .filter(|row| reusable(row, &part))
                .map(|row| row.vector.clone())
                .or_else(|| staged.remove(&part.chunk_id));
            let Some(vector) = vector else {
                break;
            };
            let window = windows::parse(&part.chunk_id)?.ok_or(EmbeddingError::InvalidInput)?;
            covered = window.range.end;
            vectors.push(EmbeddedChunk::new(part, vector));
        }
        missing |= covered != chunk.text.len();
    }
    Ok((vectors, missing))
}

type CachedWindow = (String, std::ops::Range<usize>);

fn cached_windows<'a>(
    ids: impl Iterator<Item = &'a str>,
) -> Result<HashMap<String, Vec<CachedWindow>>, ChunkTableError> {
    let mut map: HashMap<String, Vec<CachedWindow>> = HashMap::new();
    for id in ids {
        if let Some(window) = windows::parse(id)? {
            map.entry(window.anchor())
                .or_default()
                .push((id.to_owned(), window.range));
        }
    }
    Ok(map)
}

fn rehydrate_windows(
    chunk: &IndexedChunk,
    fingerprint: &str,
    mut prior: Vec<CachedWindow>,
) -> Result<(Vec<IndexedChunk>, usize), ChunkTableError> {
    prior.sort_unstable_by(|(a, x), (b, y)| (x.start, x.end, a).cmp(&(y.start, y.end, b)));
    prior.dedup();
    let mut covered = 0;
    let mut cached = Vec::new();
    for (id, range) in prior {
        if range.start > covered || range.end <= covered || range.end > chunk.text.len() {
            break;
        }
        let row = windows::row(chunk, fingerprint, range.clone())?;
        if row.chunk_id != id {
            break;
        }
        covered = range.end;
        cached.push(row);
    }
    Ok((cached, covered))
}

fn structural_boundaries(text: &str) -> Vec<usize> {
    let mut boundaries = table_section_boundaries(text);
    boundaries.extend(
        text.match_indices('\n')
            .map(|(offset, _)| offset + 1)
            .filter(|&offset| offset < text.len()),
    );
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
}

async fn verify_no_collisions<'a>(
    table: &LanceChunkTable,
    page: &str,
    ids: impl Iterator<Item = &'a String>,
) -> Result<(), FailureClass> {
    let ids: Vec<_> = ids.cloned().collect();
    for group in ids.chunks(100) {
        let collisions = table
            .rows_matching(id_list_predicate("chunk_id", group))
            .await
            .map_err(failure)?;
        if collisions.iter().any(|row| row.page_id != page) {
            return Err(FailureClass::Conflict);
        }
    }
    Ok(())
}

async fn replace_vectors(
    semantic: &LanceChunkTable,
    page: &str,
    rows: &[EmbeddedChunk],
) -> Result<(), ChunkTableError> {
    let predicate = page_predicate(page);
    if rows.is_empty() {
        semantic.table.delete(&predicate).await?;
        return Ok(());
    }
    let batch = rows_to_batch(rows, semantic.embedding_metadata())?;
    let schema = batch.schema();
    let mut merge = semantic.table.merge_insert(&["page_id", "chunk_id"]);
    merge
        .when_matched_update_all(None)
        .when_not_matched_insert_all()
        .when_not_matched_by_source_delete(Some(predicate));
    merge
        .execute(Box::new(RecordBatchIterator::new([Ok(batch)], schema)))
        .await?;
    Ok(())
}
