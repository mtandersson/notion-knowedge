//! Commit canonical text first and use the existing vector table as its derived
//! semantic projection. Only the source/text revision receives the page receipt.
use super::*;
use crate::chunks::{semantic_jobs as jobs, text};
use notion_knowledge_core::{
    chunking::{ChunkDraft, table_section_boundaries},
    fingerprint::identify_chunks,
};

type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

/// Local text revision and the result of model work prepared outside the writer
/// lease. Commit verifies both again before changing the searchable projection.
pub struct PreparedSemanticProjection {
    job: jobs::Job,
    vectors: Option<Result<Vec<EmbeddedChunk>, ChunkTableError>>,
}

impl GuardedChunkTable {
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
            .submit_with_authority(operation, lease, clock, move |context| async move {
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
                if deletion || jobs::fingerprint(&actual).map_err(failure)? != fingerprint {
                    let semantic = this.open_semantic().await.map_err(failure)?;
                    let old = semantic
                        .rows_matching(page_predicate(&page))
                        .await
                        .map_err(failure)?;
                    let (reusable, missing) = reusable_rows(&old, &chunks).map_err(failure)?;
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
                    if !old.is_empty() {
                        let visible = if missing { &[] } else { reusable.as_slice() };
                        replace_vectors(&semantic, &page, visible)
                            .await
                            .map_err(failure)?;
                    }
                    context.revalidate().map_err(fence_failure)?;
                    text::replace(&text, context.binding(), &page, &chunks)
                        .await
                        .map_err(failure)?;
                }
                Ok(authority)
            })
            .await?;
        if outcome == CommitOutcome::Applied {
            self.optimize_after_commit(maintenance.0, maintenance.1)
                .await;
        }
        Ok((outcome, diff))
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
        let prepared = self.prepare_semantic(page, provider, now, timeout).await?;
        match prepared {
            Some(prepared) => self.complete_semantic(prepared, lease, clock).await,
            None => Ok(false),
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
        let canonical = text.page_chunks(page).await.map_err(index_failure)?;
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
        let vectors = tokio::time::timeout(
            timeout,
            self.prepare_vectors(provider, &canonical, &old, staged),
        )
        .await
        .unwrap_or(Err(ChunkTableError::Embedding(EmbeddingError::Unavailable)));
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
                    Ok(rows) => {
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
                        jobs::clear(&this.index_directory, &job.page).map_err(failure)?;
                        owned_wrote.store(true, std::sync::atomic::Ordering::SeqCst);
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
    ) -> Result<Vec<EmbeddedChunk>, ChunkTableError> {
        self.embedding.ensure_compatible(provider.metadata())?;
        let chunks = semantic_chunks(provider, canonical, old, &staged).await?;
        let existing = old
            .iter()
            .map(|row| (row.chunk_id.as_str(), row))
            .collect::<HashMap<_, _>>();
        let mut vectors = Vec::with_capacity(chunks.len());
        let _embed = page_timings::timer(Bucket::Embed);
        for chunk in chunks {
            let vector = if let Some(old) = existing.get(chunk.chunk_id.as_str())
                && reusable(old, &chunk)
            {
                old.vector.clone()
            } else if let Some(vector) = staged.remove(&chunk.chunk_id) {
                vector
            } else {
                embedding::embed_batch(provider, std::slice::from_ref(&chunk.text))
                    .await?
                    .pop()
                    .ok_or(EmbeddingError::InvalidOutput)?
            };
            vectors.push(EmbeddedChunk::new(chunk, vector));
        }
        validate_rows(&vectors, self.embedding.dimension())?;
        Ok(vectors)
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
    chunks: &[IndexedChunk],
) -> Result<(Vec<EmbeddedChunk>, bool), ChunkTableError> {
    let mut old = HashMap::with_capacity(rows.len());
    for row in rows {
        if old.insert(row.chunk_id.as_str(), row).is_some() {
            return Err(ChunkTableError::InvalidRows(
                "duplicate semantic chunk identity".into(),
            ));
        }
    }
    let rows: Vec<_> = chunks
        .iter()
        .filter_map(|chunk| {
            let existing = old
                .get(chunk.chunk_id.as_str())
                .filter(|old| reusable(old, chunk))?;
            Some(EmbeddedChunk::new(chunk.clone(), existing.vector.clone()))
        })
        .collect();
    let missing = rows.len() < chunks.len();
    Ok((rows, missing))
}

async fn semantic_chunks(
    provider: &dyn EmbeddingProvider,
    canonical: &[IndexedChunk],
    old: &[StoredChunk],
    staged: &HashMap<String, Vec<f32>>,
) -> Result<Vec<IndexedChunk>, ChunkTableError> {
    let existing: HashSet<_> = old
        .iter()
        .map(|row| (row.chunk_id.as_str(), row.text.as_str()))
        .collect();
    let mut cuts = Vec::with_capacity(canonical.len());
    for chunk in canonical {
        let boundaries = table_section_boundaries(&chunk.text);
        let points = if boundaries.is_empty()
            || existing.contains(&(chunk.chunk_id.as_str(), chunk.text.as_str()))
            || staged.contains_key(&chunk.chunk_id)
        {
            Vec::new()
        } else {
            provider.partition_input(&chunk.text, boundaries).await?
        };
        cuts.push(points);
    }
    if cuts.iter().all(Vec::is_empty) {
        return Ok(canonical.to_vec());
    }
    let mut drafts = Vec::new();
    let mut parents = Vec::new();
    for (chunk, points) in canonical.iter().zip(cuts) {
        let mut start = 0;
        for end in points.into_iter().chain(std::iter::once(chunk.text.len())) {
            let body = chunk
                .text
                .get(start..end)
                .filter(|part| !part.trim().is_empty())
                .ok_or(EmbeddingError::InvalidInput)?;
            drafts.push(ChunkDraft {
                text: body.to_owned(),
                source_bytes: start..end,
                metadata: chunk.metadata.clone(),
            });
            parents.push(chunk);
            start = end;
        }
    }
    let mut chunks =
        identify_chunks(drafts, canonical).map_err(|_| EmbeddingError::InvalidInput)?;
    for (part, parent) in chunks.iter_mut().zip(parents) {
        part.links.clone_from(&parent.links);
    }
    Ok(chunks)
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
