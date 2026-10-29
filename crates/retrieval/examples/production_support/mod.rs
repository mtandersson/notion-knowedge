use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::ExclusionRules,
    embedding::EmbeddingProvider,
    graph::{GraphEdge, GraphTarget},
    graph_boost::{GraphBoostConfig, GraphBoostedHybrid},
    hybrid::{HybridFusion, ReciprocalRankFusion},
    indexed::{
        IndexedChunk, IndexedMetadata, LinkTarget, PropertyValue, SchemaVersion, SourceMetadata,
    },
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
    reconciliation::ReconciliationJournal,
    root_scope::RootScopeGate,
    search::{HybridSearch, LexicalQuery, LexicalSearch, SemanticQuery, SemanticSearch},
    source::{SourceExpandQuery, SourceExpansion, StableSourceRef},
    sync_state::PageSyncState,
};
use notion_knowledge_retrieval::{
    chunks::{
        GuardedChunkTable, LanceSemanticSearch, VectorDistance, VectorIndexConfig,
        bounded::CandidateBudget,
        lexical_binding::{LexicalCachePlan, LexicalIndexBinding},
    },
    commit::{
        CommitBinding, CommitOutcome, IndexCommitCoordinator, PageAction, PageOperation,
        RelationProjection,
    },
    graph::CommittedGraph,
    lexical_runtime::LexicalSearchRuntime,
    planner::production_scope,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    os::unix::fs::DirBuilderExt,
    path::Path,
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
type Error = Box<dyn std::error::Error>;
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    schema_version: u32,
    dataset_id: String,
    as_of: String,
    mode: String,
    top_k: usize,
    pages: Vec<Page>,
    queries: Vec<Query>,
    graph_boost: Option<Boost>,
}
#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct Page {
    id: String,
    title: String,
    language: String,
    status: String,
    updated_at: String,
    links: Vec<String>,
    chunks: Vec<Chunk>,
}
#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct Chunk {
    id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Query {
    id: String,
    text: String,
    language: String,
    category: String,
}
#[derive(Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
struct Boost {
    enabled: bool,
    max_hops: usize,
    weight: f64,
    candidate_limit: usize,
}

pub fn parse(raw: &str) -> Result<Request, Error> {
    let r: Request = serde_json::from_str(raw)?;
    if r.schema_version != 1
        || !["vector", "fts", "hybrid"].contains(&r.mode.as_str())
        || !(1..=100).contains(&r.top_k)
        || r.pages.is_empty()
        || r.pages.len() > 1000
        || r.queries.is_empty()
    {
        return Err("unsupported evaluation request".into());
    }
    let pages: BTreeSet<_> = r.pages.iter().map(|p| &p.id).collect();
    let chunks: BTreeSet<_> = r
        .pages
        .iter()
        .flat_map(|p| p.chunks.iter().map(|c| &c.id))
        .collect();
    if pages.len() != r.pages.len()
        || chunks.len() != r.pages.iter().map(|p| p.chunks.len()).sum::<usize>()
        || r.pages.iter().any(|p| {
            p.chunks.is_empty()
                || p.links.iter().any(|id| !pages.contains(id))
                || p.chunks
                    .iter()
                    .any(|c| c.text.is_empty() || c.text.chars().count() > 2000)
        })
        || r.queries
            .iter()
            .map(|q| &q.id)
            .collect::<BTreeSet<_>>()
            .len()
            != r.queries.len()
    {
        return Err("invalid fixed corpus identities".into());
    }
    if let Some(b) = &r.graph_boost {
        boost_config(b)
            .validate()
            .map_err(|_| "invalid graph configuration")?;
        if r.mode != "hybrid" || r.top_k != b.candidate_limit {
            return Err("incomplete graph candidate request".into());
        }
    }
    Ok(r)
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn page_id(id: &str) -> String {
    let h = hash(id.as_bytes());
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}
fn chunk_id(id: &str) -> String {
    hash(id.as_bytes())
}
fn index_fingerprint(directory: &Path) -> Result<String, Error> {
    fn visit(root: &Path, path: &Path, files: &mut BTreeMap<String, String>) -> Result<(), Error> {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_symlink() {
                return Err("unexpected index symlink".into());
            }
            if kind.is_dir() {
                visit(root, &entry.path(), files)?;
            } else if kind.is_file() {
                files.insert(
                    entry
                        .path()
                        .strip_prefix(root)?
                        .to_string_lossy()
                        .into_owned(),
                    hash(&fs::read(entry.path())?),
                );
            } else {
                return Err("unexpected index file type".into());
            }
        }
        Ok(())
    }
    let mut files = BTreeMap::new();
    visit(directory, directory, &mut files)?;
    Ok(hash(&serde_json::to_vec(&files)?))
}
fn revision(p: &Page) -> String {
    format!("{}T00:00:00Z", p.updated_at)
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs() as i64
}
fn boost_config(b: &Boost) -> GraphBoostConfig {
    GraphBoostConfig {
        enabled: b.enabled,
        max_hops: b.max_hops,
        weight: b.weight,
        candidate_limit: b.candidate_limit,
    }
}
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}

/// Independent fixed authoritative fixture; indexed labels never authorize a page.
struct Authority(BTreeMap<String, String>);
impl Authority {
    fn evidence(
        &self,
        page: &PageId,
        scope: &LifecycleScope,
    ) -> Result<LifecycleEvidence, BackendError> {
        let revision = self.0.get(&page.0).ok_or(BackendError {
            kind: BackendErrorKind::NotFound,
            operation: "fixture.lifecycle",
            retry_after: None,
            committed_page_id: None,
        })?;
        Ok(LifecycleEvidence {
            scope: scope.clone(),
            status: LifecycleStatus::Allowed {
                roots: vec![ROOT.into()],
            },
            ancestry: vec![
                LifecycleNode {
                    kind: LifecycleKind::Page,
                    id: page.0.clone(),
                    revision: revision.clone(),
                    inactive: false,
                    parent: PhysicalParent::Object {
                        kind: LifecycleKind::Page,
                        id: ROOT.into(),
                    },
                },
                LifecycleNode {
                    kind: LifecycleKind::Page,
                    id: ROOT.into(),
                    revision: "2026-03-02T00:00:00Z".into(),
                    inactive: false,
                    parent: PhysicalParent::Workspace,
                },
            ],
        })
    }
}
impl PageLifecycle for Authority {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move { self.evidence(page, scope) })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if self.evidence(&PageId(evidence.ancestry[0].id.clone()), scope)? != *evidence {
                return Err(BackendError {
                    kind: BackendErrorKind::Conflict,
                    operation: "fixture.revalidate",
                    retry_after: None,
                    committed_page_id: None,
                });
            }
            Ok(())
        })
    }
}

pub async fn run(
    provider: Arc<dyn EmbeddingProvider>,
    directory: &Path,
    r: Request,
) -> Result<Value, Error> {
    run_with_lexical(provider, directory, r, None).await
}

pub async fn run_with_lexical(
    provider: Arc<dyn EmbeddingProvider>,
    directory: &Path,
    r: Request,
    lexical: Option<LexicalCachePlan>,
) -> Result<Value, Error> {
    let started = Instant::now();
    if lexical.is_some() && r.graph_boost.as_ref().is_some_and(|b| b.enabled) {
        return Err("lexical evaluation requires graph OFF".into());
    }
    let scope = scope();
    let gate = RootScopeGate::new(
        Arc::new(Authority(
            r.pages
                .iter()
                .map(|p| (page_id(&p.id), revision(p)))
                .collect(),
        )),
        scope.clone(),
    )?;
    let corpus_hash = hash(&serde_json::to_vec(&r.pages)?);
    let mut config = json!({"vector":"IVF_FLAT", "distance":"Cosine", "partitions":1,"nprobes":1,"sample_rate":8,"max_iterations":20,"fts":"production default body/title/identifier indices", "rrf":{"rank_constant":60,"semantic_weight":1,"lexical_weight":1,"candidate_limit":100}});
    if let Some(plan) = &lexical {
        config["lexical"] = json!({
            "plan": plan.identity(), "plan_sha256": plan.identity().sha256(),
            "admission": 1, "deadline_seconds": 60,
            "adapter": "notion_knowledge_retrieval::lexical_runtime::LexicalSearchRuntime",
        });
        config["fts"] = json!(
            "production lexical-expansion-v1 positional body/title and raw identifier profile"
        );
    }
    let mapping: BTreeMap<_, _> = r.pages.iter().flat_map(|p| p.chunks.iter().map(|c| (c.id.clone(), json!({"page_id":page_id(&p.id),"chunk_id":chunk_id(&c.id),"root_page_id":ROOT,"indexed_last_edited_time":revision(p)})))).collect();
    let manifest = json!({"schema_version":1,"dataset_id":r.dataset_id,"as_of":r.as_of,"corpus_sha256":corpus_hash,"model":provider.metadata(),"preprocessing":"raw unchanged chunk.text, no title/status/date prefix and no query instruction", "configuration":config,"sources":mapping,"workspace_id":WORKSPACE,"root_page_id":ROOT});
    let index = directory.join("index");
    let state = directory.join("state.sqlite");
    let binding = CommitBinding::new(
        "chunks",
        WORKSPACE,
        &production_scope(&scope).map_err(|_| "invalid production scope")?,
        "1",
        1,
    )?;
    let initial = match fs::DirBuilder::new().mode(0o700).create(directory) {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(e) => return Err(e.into()),
    };
    let coordinator = if initial {
        fs::DirBuilder::new().mode(0o700).create(&index)?;
        IndexCommitCoordinator::initialize(&index, &state, binding)?
    } else {
        let existing: Value = serde_json::from_slice(&fs::read(directory.join("manifest.json"))?)?;
        if existing != manifest {
            return Err("incompatible corpus/model/index manifest".into());
        }
        IndexCommitCoordinator::open_read_only(&index, &state, binding)?
    };
    let table = if let Some(plan) = &lexical {
        if initial {
            let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(now);
            let lease = coordinator.state().acquire_lease(now(), 3600)?;
            let table = GuardedChunkTable::create_fresh_lexical(
                &index,
                "chunks",
                provider.metadata().clone(),
                coordinator.clone(),
                plan.identity(),
                lease.clone(),
                clock,
            )
            .await?;
            coordinator.state().release_lease(&lease, now())?;
            table
        } else {
            GuardedChunkTable::bind_with_lexical_plan(
                &index,
                "chunks",
                provider.metadata().clone(),
                coordinator.clone(),
                plan.identity(),
            )?
        }
    } else {
        GuardedChunkTable::bind(
            &index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
        )?
    };
    if initial {
        let clock: Arc<dyn Fn() -> i64 + Send + Sync> = Arc::new(now);
        let lease = coordinator.state().acquire_lease(now(), 3600)?;
        if lexical.is_none() {
            table.create_empty(lease.clone(), clock.clone()).await?;
        }
        for page in &r.pages {
            let id = page_id(&page.id);
            let permit = gate.authorize(&PageId(id.clone())).await?;
            let chunks: Vec<_> = page
                .chunks
                .iter()
                .map(|c| IndexedChunk {
                    schema_version: SchemaVersion::V1,
                    chunk_id: chunk_id(&c.id),
                    text: c.text.clone(),
                    content_hash: hash(c.text.as_bytes()),
                    metadata: IndexedMetadata {
                        page_id: id.clone(),
                        block_id: None,
                        url: format!("https://www.notion.so/{id}"),
                        title: page.title.clone(),
                        heading_path: vec![],
                        last_edited_time: revision(page),
                        source: SourceMetadata {
                            workspace_id: WORKSPACE.into(),
                            root_page_id: ROOT.into(),
                            database_id: None,
                            data_source_id: None,
                        },
                        properties: BTreeMap::from([
                            ("status".into(), PropertyValue::Text(page.status.clone())),
                            (
                                "updated_at".into(),
                                PropertyValue::Date {
                                    start: page.updated_at.clone(),
                                    end: None,
                                },
                            ),
                        ]),
                    },
                    links: page
                        .links
                        .iter()
                        .map(|p| LinkTarget::Page {
                            page_id: page_id(p),
                        })
                        .collect(),
                })
                .collect();
            let edges = page
                .links
                .iter()
                .enumerate()
                .map(|(i, target)| {
                    GraphEdge::new(
                        id.clone(),
                        GraphTarget::Page {
                            page_id: page_id(target),
                        },
                        "link:page".into(),
                        format!("markdown:{i}"),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let operation = PageOperation::new(
                &format!("fixture-{}", id),
                &revision(page),
                PageAction::Refresh,
                PageSyncState::present(
                    id.clone(),
                    hash(&serde_json::to_vec(page)?),
                    Some(revision(page)),
                )?,
            )?
            .with_relation_projection(
                RelationProjection::new(coordinator.binding(), &id, false, vec![])?
                    .with_links(edges)?,
            )?;
            let prepared = table
                .prepare_page(provider.as_ref(), operation, &chunks)
                .await?;
            let commit_gate = gate.clone();
            let commit_scope = scope.clone();
            let outcome = table
                .commit_page(prepared, lease.clone(), clock.clone(), move || async move {
                    commit_gate
                        .revalidate(&permit, &commit_scope)
                        .await
                        .map_err(|_| notion_knowledge_core::reconciliation::FailureClass::Conflict)
                })
                .await?;
            if outcome != CommitOutcome::Applied {
                return Err("page commit did not apply".into());
            }
        }
        table
            .ensure_vector_index(
                VectorIndexConfig {
                    distance: VectorDistance::Cosine,
                    num_partitions: Some(1),
                    sample_rate: 8,
                    max_iterations: 20,
                },
                lease.clone(),
                clock,
            )
            .await?;
        coordinator.state().release_lease(&lease, now())?;
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(directory.join("manifest.json"))?;
        serde_json::to_writer_pretty(&mut file, &manifest)?;
        file.write_all(b"\n")?;
    }
    let version = table.inventory_version().await?;
    let state_hash = hash(&fs::read(&state)?);
    let index_hash = index_fingerprint(&index)?;
    let pinned = json!({"inventory_version":version,"operational_sha256":state_hash,"index_sha256":index_hash});
    if initial {
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("snapshot.json"))?;
        serde_json::to_writer_pretty(file, &pinned)?;
    } else if serde_json::from_slice::<Value>(&fs::read(directory.join("snapshot.json"))?)?
        != pinned
    {
        return Err("persisted evaluation snapshot changed".into());
    }
    let signature = hash(format!("{corpus_hash}:{version}:{state_hash}:{index_hash}").as_bytes());
    let snapshot = Arc::new(table.search_snapshot().await?);
    let expected = r.pages.iter().map(|p| p.chunks.len()).sum::<usize>();
    if snapshot.count_rows().await? != expected {
        return Err("incomplete persisted corpus".into());
    }
    let (semantic, lexical_search, hybrid): (
        Arc<dyn SemanticSearch>,
        Arc<dyn LexicalSearch>,
        Arc<dyn HybridSearch>,
    ) = if let Some(plan) = &lexical {
        table.probe_lexical_cache().await?;
        let runtime = Arc::new(LexicalSearchRuntime::new(
            table.clone(),
            provider.clone(),
            plan.clone(),
            CandidateBudget::new(1)?,
            std::time::Duration::from_secs(60),
        )?);
        (runtime.clone(), runtime.clone(), runtime)
    } else {
        let semantic = Arc::new(LanceSemanticSearch::new(
            snapshot.clone(),
            provider.clone(),
            1,
        )?);
        let hybrid = Arc::new(
            HybridFusion::new(
                semantic.clone(),
                snapshot.clone(),
                ReciprocalRankFusion::default(),
            )
            .map_err(|_| "invalid fusion")?,
        );
        (semantic, snapshot.clone(), hybrid)
    };
    let hybrid: Arc<dyn HybridSearch> = if let Some(b) = &r.graph_boost {
        Arc::new(
            GraphBoostedHybrid::new(
                hybrid,
                Arc::new(CommittedGraph::new(coordinator.clone())),
                boost_config(b),
            )
            .map_err(|_| "invalid graph")?
            .with_root_gate(gate.clone()),
        )
    } else {
        hybrid
    };
    // Exercise the same filters on real backends, not merely metadata labels.
    let foreign_root = "55555555-5555-5555-5555-555555555555";
    let denied_query = SemanticQuery {
        query: r.queries[0].text.clone(),
        limit: 1,
        page_ids: None,
        root_page_ids: Some(vec![foreign_root.into()]),
        metadata: Default::default(),
    };
    if !semantic
        .search(denied_query.clone())
        .await
        .map_err(|_| "root probe unavailable")?
        .is_empty()
        || !hybrid
            .search(denied_query.clone())
            .await
            .map_err(|_| "root probe unavailable")?
            .is_empty()
        || !LexicalSearch::search(
            lexical_search.as_ref(),
            LexicalQuery {
                query: denied_query.query,
                limit: 1,
                page_ids: None,
                root_page_ids: denied_query.root_page_ids,
                metadata: Default::default(),
            },
        )
        .await
        .map_err(|_| "root probe unavailable")?
        .is_empty()
        || table
            .expand(SourceExpandQuery {
                refs: vec![StableSourceRef::Chunk(chunk_id(&r.pages[0].chunks[0].id))],
                max_chars: 2000,
                root_page_ids: vec![foreign_root.into()],
            })
            .await
            .is_ok()
        || gate.authorize(&PageId(foreign_root.into())).await.is_ok()
    {
        return Err("root boundary disclosed unauthorized content".into());
    }
    let reverse: BTreeMap<_, _> = r
        .pages
        .iter()
        .flat_map(|p| {
            p.chunks
                .iter()
                .map(move |c| (chunk_id(&c.id), (&c.id, &c.text, p)))
        })
        .collect();
    let mut results = vec![];
    let mut timings = vec![];
    for q in &r.queries {
        let timer = Instant::now();
        let query = SemanticQuery {
            query: q.text.clone(),
            limit: r.top_k,
            page_ids: None,
            root_page_ids: Some(vec![ROOT.into()]),
            metadata: Default::default(),
        };
        let hits = match r.mode.as_str() {
            "vector" => semantic.search(query).await,
            "fts" => {
                LexicalSearch::search(
                    lexical_search.as_ref(),
                    LexicalQuery {
                        query: query.query,
                        limit: query.limit,
                        page_ids: query.page_ids,
                        root_page_ids: query.root_page_ids,
                        metadata: query.metadata,
                    },
                )
                .await
            }
            "hybrid" => hybrid.search(query).await,
            _ => return Err("unsupported mode".into()),
        }
        .map_err(|_| "production search unavailable")?;
        let mut ranked = vec![];
        let mut scores = vec![];
        for hit in hits {
            let (id, text, page) = reverse
                .get(&hit.source.chunk_id)
                .ok_or("unknown production source")?;
            let permit = gate.authorize(&PageId(hit.source.page_id.clone())).await?;
            gate.revalidate(&permit, &scope).await?;
            if hit.text != **text
                || hit.source.page_id != page_id(&page.id)
                || hit.source.last_edited_time != revision(page)
            {
                return Err("source/provenance mismatch".into());
            }
            let expanded = table
                .expand(SourceExpandQuery {
                    refs: vec![StableSourceRef::Chunk(hit.source.chunk_id.clone())],
                    max_chars: 2000,
                    root_page_ids: vec![ROOT.into()],
                })
                .await
                .map_err(|_| "guarded source expansion failed")?;
            // Production chunk expansion returns its complete heading section.
            // These unchanged fixtures have page-level headings, so all chunks
            // on that page contribute in their persisted source order.
            let section = page
                .chunks
                .iter()
                .map(|c| c.text.as_str())
                .collect::<Vec<_>>()
                .join("\n\n");
            let section_ids: Vec<_> = page.chunks.iter().map(|c| chunk_id(&c.id)).collect();
            if expanded.len() != 1
                || expanded[0].text != section.chars().take(2000).collect::<String>()
                || expanded[0].truncated != (section.chars().count() > 2000)
                || expanded[0].provenance.chunk_ids != section_ids
                || expanded[0].provenance.page_id != page_id(&page.id)
                || expanded[0].provenance.root_page_id != ROOT
                || expanded[0].provenance.indexed_last_edited_time != revision(page)
            {
                return Err("source expansion changed fixed content".into());
            }
            ranked.push(*id);
            let mut evidence =
                json!({"source_id":id,"score":hit.score,"matched_paths":hit.matched_paths});
            if lexical.is_some() {
                evidence["lexical_matches"] = serde_json::to_value(&hit.lexical_matches)?;
            }
            scores.push(evidence);
        }
        results.push(json!({"query_id":q.id,"ranked_source_ids":ranked,"baseline_scores":scores}));
        timings.push(json!({"query_id":q.id,"language":q.language,"category":q.category,"search_and_authorization_ms":timer.elapsed().as_secs_f64()*1000.}));
    }
    if table.inventory_version().await? != version
        || hash(&fs::read(&state)?) != state_hash
        || index_fingerprint(&index)? != index_hash
    {
        return Err("query mutated persistent state".into());
    }
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join("measurements.jsonl"))?;
    serde_json::to_writer(
        &mut log,
        &json!({"mode":r.mode,"graph_boost":r.graph_boost,"index_version":version,"index_signature":signature,"elapsed_ms":started.elapsed().as_secs_f64()*1000.,"queries":timings}),
    )?;
    log.write_all(b"\n")?;
    let mut output = json!({"schema_version":1,"dataset_id":r.dataset_id,"mode":r.mode,"index_signature":signature,"results":results});
    if let Some(plan) = &lexical {
        output["lexical_identity"] = serde_json::to_value(LexicalIndexBinding::for_cache(
            &index,
            provider.metadata().clone(),
            coordinator.binding(),
            plan.identity().clone(),
        )?)?;
        output["input_sha256"] = json!(hash(&serde_json::to_vec(&json!({
            "dataset_id": r.dataset_id, "as_of": r.as_of, "pages": r.pages,
            "queries": r.queries.iter().map(|q| json!({"id":q.id,"text":q.text,"language":q.language,"category":q.category})).collect::<Vec<_>>()
        }))?));
    }
    if let Some(b) = r.graph_boost {
        output["graph_boost"] = serde_json::to_value(b)?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::embedding::{EmbeddingFuture, EmbeddingMetadata};
    struct BoundaryProvider(EmbeddingMetadata);
    impl EmbeddingProvider for BoundaryProvider {
        fn metadata(&self) -> &EmbeddingMetadata {
            &self.0
        }
        fn embed_batch<'a>(&'a self, inputs: &'a [String]) -> EmbeddingFuture<'a> {
            // Only a storage/protocol boundary fixture; never quality evidence.
            Box::pin(async move { Ok(inputs.iter().map(|_| vec![1., 0., 0.]).collect()) })
        }
    }
    fn boundary_request() -> Request {
        let dataset: Value = serde_json::from_str(include_str!(
            "../../../../eval/retrieval/exact-identifiers-v1.json"
        ))
        .unwrap();
        let queries: Vec<_> = dataset["queries"].as_array().unwrap().iter().map(|q| json!({"id":q["id"],"text":q["text"],"language":q["language"],"category":q["category"]})).collect();
        parse(&json!({"schema_version":1,"dataset_id":dataset["dataset_id"],"as_of":dataset["as_of"],"mode":"fts","top_k":3,"pages":dataset["pages"],
            "queries":queries}).to_string()).unwrap()
    }
    #[tokio::test]
    async fn fixed_snapshot_reuses_real_indexes_and_rejects_changes_or_missing_state() {
        let root = std::env::temp_dir().join(format!(
            "nk-production-eval-boundary-{}-{}",
            std::process::id(),
            now()
        ));
        let provider: Arc<dyn EmbeddingProvider> = Arc::new(BoundaryProvider(
            EmbeddingMetadata::new("fixture".into(), "boundary-only".into(), "1".into(), 3)
                .unwrap(),
        ));
        let first = run(provider.clone(), &root, boundary_request())
            .await
            .unwrap();
        let second = run(provider.clone(), &root, boundary_request())
            .await
            .unwrap();
        assert_eq!(
            first, second,
            "same persisted corpus/index must preserve ranks and signature"
        );
        // Real FTS ranks on all unchanged exact-ID cases must meet the existing
        // 75% top1/top3, 50%-per-language and >=one-per-family floors. Judgments
        // live only in this assertion, never in the adapter input/ranker.
        let dataset: Value = serde_json::from_str(include_str!(
            "../../../../eval/retrieval/exact-identifiers-v1.json"
        ))
        .unwrap();
        let mut top1 = 0;
        let mut top3 = 0;
        let mut languages = BTreeMap::<String, usize>::new();
        let mut families = BTreeMap::<String, usize>::new();
        for (query, row) in dataset["queries"]
            .as_array()
            .unwrap()
            .iter()
            .zip(first["results"].as_array().unwrap())
        {
            assert_eq!(query["id"], row["query_id"]);
            let ranks = row["ranked_source_ids"].as_array().unwrap();
            let correct = &query["relevant"][0]["source_id"];
            top1 += usize::from(ranks.first() == Some(correct));
            let found = usize::from(ranks.iter().take(3).any(|id| id == correct));
            top3 += found;
            *languages
                .entry(query["language"].as_str().unwrap().into())
                .or_default() += found;
            *families
                .entry(
                    query["id"]
                        .as_str()
                        .unwrap()
                        .rsplit_once('-')
                        .unwrap()
                        .0
                        .into(),
                )
                .or_default() += found;
        }
        assert!(
            top1 >= 6 && top3 >= 6,
            "unchanged lexical exact-ID floors: top1={top1}/8 top3={top3}/8"
        );
        assert!(languages.values().all(|count| *count >= 2));
        assert!(families.values().all(|count| *count >= 1));
        let mut changed = boundary_request();
        changed.pages[0].chunks[0].text.push_str(" changed");
        assert!(run(provider.clone(), &root, changed).await.is_err());
        // Same row count but modified durable index bytes is not this corpus snapshot.
        let index = root.join("index");
        fs::write(
            index.join("unexpected-fragment"),
            b"different fixed snapshot",
        )
        .unwrap();
        assert!(
            run(provider.clone(), &root, boundary_request())
                .await
                .is_err()
        );
        fs::remove_file(index.join("unexpected-fragment")).unwrap();
        fs::rename(index.join("chunks.lance"), index.join("missing.lance")).unwrap();
        assert!(
            run(provider.clone(), &root, boundary_request())
                .await
                .is_err()
        );
        fs::rename(index.join("missing.lance"), index.join("chunks.lance")).unwrap();
        for filename in ["snapshot.json", "manifest.json"] {
            let path = root.join(filename);
            let contents = fs::read(&path).unwrap();
            fs::remove_file(&path).unwrap();
            assert!(
                run(provider.clone(), &root, boundary_request())
                    .await
                    .is_err()
            );
            fs::write(path, contents).unwrap();
        }
        // A real authorized later commit changes a title while retaining all
        // chunk identities and row count. It must not become benchmark input.
        let binding = CommitBinding::new(
            "chunks",
            WORKSPACE,
            &production_scope(&scope()).unwrap(),
            "1",
            1,
        )
        .unwrap();
        let coordinator =
            IndexCommitCoordinator::open_existing(&index, root.join("state.sqlite"), binding)
                .unwrap();
        let table = GuardedChunkTable::bind(
            &index,
            "chunks",
            provider.metadata().clone(),
            coordinator.clone(),
        )
        .unwrap();
        let page = &boundary_request().pages[0];
        let id = page_id(&page.id);
        let mut chunks = table.page_chunks(&id).await.unwrap();
        let rows = table
            .search_snapshot()
            .await
            .unwrap()
            .count_rows()
            .await
            .unwrap();
        chunks[0].metadata.title = "Changed canonical title".into();
        let lease = coordinator.state().acquire_lease(now(), 120).unwrap();
        let operation = PageOperation::new(
            "later-authorized-edit",
            &revision(page),
            PageAction::Refresh,
            PageSyncState::present(id, "changed-source-checkpoint".into(), Some(revision(page)))
                .unwrap(),
        )
        .unwrap();
        let prepared = table
            .prepare_page(provider.as_ref(), operation, &chunks)
            .await
            .unwrap();
        table
            .commit_page(prepared, lease.clone(), Arc::new(now), || async { Ok(()) })
            .await
            .unwrap();
        coordinator.state().release_lease(&lease, now()).unwrap();
        assert_eq!(
            table
                .search_snapshot()
                .await
                .unwrap()
                .count_rows()
                .await
                .unwrap(),
            rows
        );
        assert!(run(provider, &root, boundary_request()).await.is_err());
        fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn unchanged_multichunk_fixture_preserves_production_heading_expansion() {
        let dataset: Value = serde_json::from_str(include_str!(
            "../../../../eval/retrieval/personal-knowledge-v1.json"
        ))
        .unwrap();
        let request = parse(&json!({"schema_version":1,"dataset_id":dataset["dataset_id"],"as_of":dataset["as_of"],"mode":"fts","top_k":10,"pages":dataset["pages"],
            "queries":[{"id":"boundary-heading","text":"Project Lighthouse","language":"en","category":"exact_name"}]}).to_string()).unwrap();
        let root = std::env::temp_dir().join(format!(
            "nk-production-eval-heading-{}-{}",
            std::process::id(),
            now()
        ));
        let provider: Arc<dyn EmbeddingProvider> = Arc::new(BoundaryProvider(
            EmbeddingMetadata::new("fixture".into(), "boundary-only".into(), "1".into(), 3)
                .unwrap(),
        ));
        let output = run(provider, &root, request).await.unwrap();
        let ids = output["results"][0]["ranked_source_ids"]
            .as_array()
            .unwrap();
        assert!(
            dataset["pages"][0]["chunks"]
                .as_array()
                .unwrap()
                .iter()
                .all(|c| ids.contains(&c["id"]))
        );
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn production_request_rejects_judgments_and_unsupported_modes() {
        let mut request = json!({"schema_version":1,"dataset_id":"test","as_of":"2026-03-02","mode":"hybrid","top_k":3,"pages":[{"id":"page","title":"Title","language":"en","status":"active","updated_at":"2026-03-02","links":[],"chunks":[{"id":"chunk","text":"unchanged"}]}],"queries":[{"id":"q","text":"question","language":"en","category":"semantic"}]});
        assert!(parse(&request.to_string()).is_ok());
        request["queries"][0]["relevant"] = json!([{"source_id":"chunk","grade":3}]);
        assert!(parse(&request.to_string()).is_err());
        request["queries"][0]
            .as_object_mut()
            .unwrap()
            .remove("relevant");
        request["mode"] = json!("guess");
        assert!(parse(&request.to_string()).is_err());
    }
}
