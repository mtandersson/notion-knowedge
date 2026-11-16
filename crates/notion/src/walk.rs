//! Walk the tree down from the configured root. A page is in scope iff this
//! walk reached it: child pages come from `<page>` tags in each page's
//! Markdown and rows from data-source queries. Links and mentions are never
//! followed. Search may supply page metadata (see [`crate::search`]) but
//! never decides which pages are reached.
use crate::{
    NotionClient,
    content::MarkdownBody,
    links::child_tags,
    pages::{PAGE_VERSION, inactive, metadata, page_id},
    search::SearchMetadata,
};
use futures_util::{StreamExt, stream::FuturesUnordered};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageContent, PageId},
    discovery::{ExclusionRules, SkipReason, SkippedContent, SourceType, inaccessible},
    inventory::{InventoryEntry, InventoryParent},
    lifecycle::LifecycleScope,
    logging::{SyncCount, SyncPhase, SyncProgressLog, sync_count, sync_task, sync_total_pages},
};
use reqwest::{Method, header::AUTHORIZATION};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeSet, VecDeque};

// Fail explicitly rather than silently truncate enormous or malformed trees.
const MAX_NODES: usize = 100_000;
const MAX_LIST_PAGES: usize = 10_000;
const MAX_BLOCK_DEPTH: usize = 64;
const FETCH_CONCURRENCY: usize = 8;

fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.walk",
        retry_after: None,
        committed_page_id: None,
    }
}
fn id(value: &Value) -> Result<String, BackendError> {
    value
        .as_str()
        .and_then(|v| page_id(v).ok())
        .map(|v| v.0)
        .ok_or_else(|| error(BackendErrorKind::Internal))
}

/// One reached page. `content` is set only for a readable, complete page;
/// `skip` is set for a page that is inventoried but must not be indexed.
#[derive(Debug, Clone, PartialEq)]
pub struct WalkedPage {
    pub entry: InventoryEntry,
    pub content: Option<PageContent>,
    pub skip: Option<SkipReason>,
    /// Child pages tagged in this page whose search listing names this page
    /// as their physical parent: the walk's own child test, so relation and
    /// link targets among them need no further authorization.
    pub children: BTreeSet<String>,
}

/// The result of one walk. `entries` holds every reached page once (sorted by
/// id), including skipped ones. Only a `complete` walk may authorize deletes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TreeWalk {
    pub run_id: String,
    pub entries: Vec<InventoryEntry>,
    pub skipped: Vec<SkippedContent>,
    pub complete: bool,
    pub calls: u64,
}
impl TreeWalk {
    /// The one definition of a walk that may replace the inventory and plan
    /// deletions. Unreadable (404/403) children are typed skips and do not
    /// lower it; transient failures already fail the walk, and a repeated
    /// cursor or truncated body clears `complete`.
    pub fn authoritative(&self) -> bool {
        self.complete
    }
    /// Reached pages that were not skipped: the pages that may be indexed.
    pub fn readable(&self) -> BTreeSet<String> {
        let skipped: BTreeSet<_> = self.skipped.iter().map(|s| s.id.as_str()).collect();
        self.entries
            .iter()
            .filter(|e| !skipped.contains(e.page_id.as_str()))
            .map(|e| e.page_id.clone())
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChildRef {
    Page(String),
    Database(String),
}
/// Physical children named in enhanced Markdown, in document order.
pub(crate) fn child_references(markdown: &str) -> Vec<ChildRef> {
    child_tags(markdown)
        .into_iter()
        .filter_map(|(name, url)| {
            let id = page_id(&url).ok()?.0;
            Some(if name == "page" {
                ChildRef::Page(id)
            } else {
                ChildRef::Database(id)
            })
        })
        .collect()
}

enum Node {
    Page {
        id: String,
        parent: InventoryParent,
        root: String,
        /// Rows carry their metadata from the data-source query.
        row: Option<Value>,
    },
    Database {
        id: String,
        owner: String,
        root: String,
    },
}
impl Node {
    /// The id and the page that claims it as a child.
    fn claim(&self) -> (String, String) {
        match self {
            Self::Page {
                id,
                parent: InventoryParent::Page(parent),
                ..
            } => (id.clone(), parent.clone()),
            Self::Page { id, .. } => (id.clone(), String::new()),
            Self::Database { id, owner, .. } => (id.clone(), owner.clone()),
        }
    }
    fn id(&self) -> &str {
        match self {
            Self::Page { id, .. } | Self::Database { id, .. } => id,
        }
    }
}
#[derive(Default)]
struct Visit {
    pages: Vec<WalkedPage>,
    skipped: Vec<SkippedContent>,
    children: Vec<Node>,
    calls: u64,
    incomplete: bool,
    /// Tagged under a page that is not its physical parent. The id stays
    /// unvisited so a later tag from its real parent is still walked.
    rejected: Option<(String, String)>,
    /// The visited node's id.
    id: String,
}
impl Visit {
    fn skip(&mut self, id: &str, reason: SkipReason) {
        self.skipped.push(SkippedContent {
            id: id.to_owned(),
            reason,
        });
    }
    fn skip_page(&mut self, entry: InventoryEntry, reason: SkipReason) {
        self.skip(&entry.page_id, reason);
        self.pages.push(WalkedPage {
            entry,
            content: None,
            skip: Some(reason),
            children: BTreeSet::new(),
        });
    }
}

impl NotionClient {
    pub(crate) async fn walk_request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, BackendError> {
        let mut request = self
            .http
            .request(method, format!("{}/{path}", self.api_root))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", PAGE_VERSION);
        if let Some(body) = body {
            request = request.json(body);
        }
        let mut response = self.send(request, true, "notion.walk").await?;
        if response.status().as_u16() != 200 {
            return Err(error(match response.status().as_u16() {
                400 => BackendErrorKind::InvalidInput,
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                404 => BackendErrorKind::NotFound,
                409 => BackendErrorKind::Conflict,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            }));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(BackendErrorKind::Unavailable))?
        {
            if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
                return Err(error(BackendErrorKind::Internal));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| error(BackendErrorKind::Internal))
    }

    /// Whether `value`'s physical parent is page `expected`, through any
    /// nesting blocks. A child tag can never widen scope to another page.
    async fn parent_is(
        &self,
        value: &Value,
        expected: &str,
        visit: &mut Visit,
    ) -> Result<bool, BackendError> {
        let mut current = value.clone();
        for _ in 0..MAX_BLOCK_DEPTH {
            let parent = &current["parent"];
            match parent["type"].as_str() {
                Some("page_id") => return Ok(id(&parent["page_id"])? == expected),
                Some("block_id") => {
                    let block = id(&parent["block_id"])?;
                    visit.calls += 1;
                    current = match self
                        .walk_request(Method::GET, &format!("blocks/{block}"), None)
                        .await
                    {
                        Ok(value) if value["object"] == "block" && id(&value["id"])? == block => {
                            value
                        }
                        Ok(_) => return Err(error(BackendErrorKind::Internal)),
                        // Ancestry cannot be proven: not a rejection the walk
                        // may silently rely on for deletes.
                        Err(e) if inaccessible(e.kind) => {
                            visit.incomplete = true;
                            return Ok(false);
                        }
                        Err(e) => return Err(e),
                    };
                }
                _ => return Ok(false),
            }
        }
        visit.incomplete = true;
        Ok(false)
    }

    async fn visit_page(
        &self,
        id: String,
        parent: InventoryParent,
        root: String,
        row: Option<Value>,
        rules: &ExclusionRules,
        search: &SearchMetadata,
    ) -> Result<Visit, BackendError> {
        let mut visit = Visit {
            id: id.clone(),
            ..Default::default()
        };
        let is_root = parent == InventoryParent::Root;
        let entry = |edited: &str, in_trash: bool| InventoryEntry {
            page_id: id.clone(),
            parent: parent.clone(),
            root_id: root.clone(),
            last_edited_time: edited.to_owned(),
            in_trash,
        };
        if let Some(reason) = rules.exclusion(&id, SourceType::Page) {
            visit.skip_page(entry("", false), reason);
            return Ok(visit);
        }
        let page_ref = PageId(id.clone());
        // A complete search listing under the same page parent replaces the
        // metadata read; anything else (missing, partial, moved, nested under
        // a block, or the root) is read directly.
        let listed = match &parent {
            InventoryParent::Page(expected) => search
                .get(&id)
                .filter(|listed| listed.parent_page_id.as_ref() == Some(expected))
                .map(|listed| listed.page.clone()),
            _ => None,
        };
        let value = match (listed.is_some(), row) {
            (true, _) => Value::Null,
            (false, Some(value)) => value,
            (false, None) => {
                visit.calls += 1;
                match self.fetch_page_value(&page_ref).await {
                    Ok(value) => value,
                    // A configured root must be readable; children may be typed skips.
                    Err(e) if !is_root && inaccessible(e.kind) => {
                        visit.skip_page(entry("", false), SkipReason::Inaccessible);
                        return Ok(visit);
                    }
                    Err(e) => return Err(e),
                }
            }
        };
        let page = match listed {
            Some(page) => page,
            None => metadata(&value, &page_ref)?,
        };
        if let InventoryParent::Page(expected) = &parent
            && !value.is_null()
            && !self.parent_is(&value, expected, &mut visit).await?
        {
            // Not physically under this page, for example a linked view.
            visit.rejected = Some((id.clone(), expected.clone()));
            return Ok(visit);
        }
        let entry = entry(&page.last_edited_time, page.archived);
        if page.archived {
            visit.skip_page(entry, SkipReason::Archived);
            return Ok(visit);
        }
        visit.calls += 1;
        let body = match self.markdown_body(&page_ref).await {
            Ok(body) => body,
            // Readable metadata but no body is not a revoked permission: keep
            // the page out of this walk's index but do not let it authorize
            // deletes.
            Err(e) if !is_root && inaccessible(e.kind) => {
                visit.incomplete = true;
                visit.skip_page(entry, SkipReason::Inaccessible);
                return Ok(visit);
            }
            Err(e) => return Err(e),
        };
        let complete = !body.truncated && body.unknown_block_ids.is_empty();
        let mut children = child_references(&body.markdown);
        self.follow_unknown_blocks(&body, &mut children, &mut visit)
            .await?;
        // descendants_of keeps the page and prunes everything below it.
        let pruned = rules.descendants_of.contains(&id);
        let listed_children = children
            .iter()
            .filter_map(|child| match child {
                ChildRef::Page(child) => Some(child),
                ChildRef::Database(_) => None,
            })
            .filter(|child| {
                !pruned
                    && rules.exclusion(child, SourceType::Page).is_none()
                    && search.get(*child).is_some_and(|listed| {
                        listed.parent_page_id.as_ref() == Some(&id) && !listed.page.archived
                    })
            })
            .cloned()
            .collect();
        visit.pages.push(WalkedPage {
            entry,
            content: complete.then_some(PageContent {
                page,
                markdown: body.markdown,
            }),
            skip: None,
            children: listed_children,
        });
        if pruned {
            return Ok(visit);
        }
        for child in children {
            visit.children.push(match child {
                ChildRef::Page(child) => Node::Page {
                    id: child,
                    parent: InventoryParent::Page(id.clone()),
                    root: root.clone(),
                    row: None,
                },
                ChildRef::Database(child) => Node::Database {
                    id: child,
                    owner: id.clone(),
                    root: root.clone(),
                },
            });
        }
        Ok(visit)
    }

    /// Blocks Notion did not render inline are read through the same endpoint
    /// so their child tags are still reached. A 404/403 block is recorded as
    /// an `inaccessible` skip; any other failure fails the whole walk.
    async fn follow_unknown_blocks(
        &self,
        body: &MarkdownBody,
        children: &mut Vec<ChildRef>,
        visit: &mut Visit,
    ) -> Result<(), BackendError> {
        if body.truncated && body.unknown_block_ids.is_empty() {
            visit.incomplete = true;
        }
        let mut pending: VecDeque<_> = body.unknown_block_ids.iter().cloned().collect();
        let mut seen = BTreeSet::new();
        while let Some(block) = pending.pop_front() {
            let block = page_id(&block)?;
            if !seen.insert(block.0.clone()) {
                continue;
            }
            if seen.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
            visit.calls += 1;
            match self.markdown_body(&block).await {
                Ok(part) => {
                    if part.truncated && part.unknown_block_ids.is_empty() {
                        visit.incomplete = true;
                    }
                    children.extend(child_references(&part.markdown));
                    pending.extend(part.unknown_block_ids);
                }
                // Permanently unreadable (for example a synced block from a
                // page the integration cannot see): a typed skip, not doubt.
                Err(e) if inaccessible(e.kind) => visit.skip(&block.0, SkipReason::Inaccessible),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    async fn visit_database(
        &self,
        id: String,
        owner: String,
        root: String,
        rules: &ExclusionRules,
    ) -> Result<Visit, BackendError> {
        let mut visit = Visit {
            id: id.clone(),
            ..Default::default()
        };
        if let Some(reason) = rules.exclusion(&id, SourceType::Database) {
            visit.skip(&id, reason);
            return Ok(visit);
        }
        visit.calls += 1;
        let value = match self
            .walk_request(Method::GET, &format!("databases/{id}"), None)
            .await
        {
            Ok(value) => value,
            Err(e) if inaccessible(e.kind) => {
                visit.skip(&id, SkipReason::Inaccessible);
                return Ok(visit);
            }
            Err(e) => return Err(e),
        };
        if value["object"] != "database" || self::id(&value["id"])? != id {
            return Err(error(BackendErrorKind::Internal));
        }
        // A linked view of a database elsewhere is not a physical child.
        if !self.parent_is(&value, &owner, &mut visit).await? {
            visit.rejected = Some((id, owner));
            return Ok(visit);
        }
        if inactive(&value).ok_or_else(|| error(BackendErrorKind::Internal))? {
            visit.skip(&id, SkipReason::Archived);
            return Ok(visit);
        }
        let sources = value["data_sources"]
            .as_array()
            .ok_or_else(|| error(BackendErrorKind::Internal))?;
        for source in sources {
            let source = self::id(&source["id"])?;
            if let Some(reason) = rules.exclusion(&source, SourceType::DataSource) {
                visit.skip(&source, reason);
                continue;
            }
            let parent = InventoryParent::DataSource {
                data_source_id: source.clone(),
                database_id: id.clone(),
                owner_page_id: owner.clone(),
            };
            self.query_rows(&source, &parent, &root, &mut visit).await?;
        }
        Ok(visit)
    }

    async fn query_rows(
        &self,
        source: &str,
        parent: &InventoryParent,
        root: &str,
        visit: &mut Visit,
    ) -> Result<(), BackendError> {
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        for _ in 0..MAX_LIST_PAGES {
            let mut body = json!({"page_size": 100});
            if let Some(cursor) = &cursor {
                body["start_cursor"] = json!(cursor);
            }
            visit.calls += 1;
            let value = match self
                .walk_request(
                    Method::POST,
                    &format!("data_sources/{source}/query"),
                    Some(&body),
                )
                .await
            {
                Ok(value) => value,
                Err(e) if inaccessible(e.kind) => {
                    visit.skip(source, SkipReason::Inaccessible);
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
            let results = value["results"]
                .as_array()
                .filter(|_| value["object"] == "list")
                .ok_or_else(|| error(BackendErrorKind::Internal))?;
            for row in results {
                // Wiki sources also list child data sources; those are not rows.
                if row["object"] != "page" {
                    continue;
                }
                let row_id = id(&row["id"])?;
                if row["parent"]["type"] != "data_source_id"
                    || id(&row["parent"]["data_source_id"])? != source
                {
                    visit.skip(&row_id, SkipReason::OutsideScope);
                    continue;
                }
                visit.children.push(Node::Page {
                    id: row_id,
                    parent: parent.clone(),
                    root: root.to_owned(),
                    row: Some(row.clone()),
                });
            }
            if visit.children.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
            if value["has_more"] != true {
                return Ok(());
            }
            // A repeated or missing cursor would loop or truncate: keep the rows
            // already listed but mark the walk incomplete so nothing is deleted.
            match value["next_cursor"].as_str() {
                Some(next) if !next.is_empty() && cursors.insert(next.to_owned()) => {
                    cursor = Some(next.to_owned());
                }
                _ => {
                    visit.incomplete = true;
                    return Ok(());
                }
            }
        }
        visit.incomplete = true;
        Ok(())
    }

    /// Walk every configured root. Each reached page is sent to `sink` as soon
    /// as it is read, so content preparation can overlap the walk. Any request
    /// failure other than an unreadable child fails the whole walk.
    pub async fn walk_tree(
        &self,
        scope: &LifecycleScope,
        sink: Option<&tokio::sync::mpsc::Sender<WalkedPage>>,
    ) -> Result<TreeWalk, BackendError> {
        self.walk(scope, sink, SearchMetadata::new(), 0).await
    }

    /// [`Self::walk_tree`] that first lists search metadata, so a reached
    /// page whose listing is complete costs only its Markdown read. A failed
    /// listing (other than revoked credentials) only costs those reads back.
    pub async fn walk_tree_with_search(
        &self,
        scope: &LifecycleScope,
        sink: Option<&tokio::sync::mpsc::Sender<WalkedPage>>,
    ) -> Result<TreeWalk, BackendError> {
        let mut calls = 0;
        let search = match self.search_pages(&mut calls).await {
            Ok(search) => search,
            Err(e) if e.kind == BackendErrorKind::Unauthenticated => return Err(e),
            Err(_) => SearchMetadata::new(),
        };
        self.walk(scope, sink, search, calls).await
    }

    /// Read one placed hit the way the walk reads a reached page, from the
    /// hit's own metadata: one Markdown read. `None` when the hit is not
    /// physically under `parent` after all (for example a linked view).
    /// Children it tags are not followed; new children are hits themselves.
    pub async fn read_placed(
        &self,
        scope: &LifecycleScope,
        hit: &crate::search::SearchHit,
        parent: InventoryParent,
        root: String,
    ) -> Result<Option<WalkedPage>, BackendError> {
        let rules = Self::rules(scope)?;
        let visit = self
            .visit_page(
                hit.page_id.clone(),
                parent,
                root,
                Some(hit.value.clone()),
                &rules,
                &SearchMetadata::new(),
            )
            .await?;
        if visit.rejected.is_some() {
            return Ok(None);
        }
        Ok(visit.pages.into_iter().next())
    }

    fn rules(scope: &LifecycleScope) -> Result<ExclusionRules, BackendError> {
        let normalize = |ids: &BTreeSet<String>| {
            ids.iter()
                .map(|v| page_id(v).map(|v| v.0))
                .collect::<Result<BTreeSet<_>, _>>()
        };
        Ok(ExclusionRules {
            page_ids: normalize(&scope.exclusions.page_ids)?,
            descendants_of: normalize(&scope.exclusions.descendants_of)?,
            source_types: scope.exclusions.source_types.clone(),
        })
    }

    async fn walk(
        &self,
        scope: &LifecycleScope,
        sink: Option<&tokio::sync::mpsc::Sender<WalkedPage>>,
        search: SearchMetadata,
        search_calls: u64,
    ) -> Result<TreeWalk, BackendError> {
        let search = &search;
        let rules = Self::rules(scope)?;
        if scope.roots.is_empty() {
            return Err(error(BackendErrorKind::InvalidInput));
        }
        let mut queue = VecDeque::new();
        for root in &scope.roots {
            let root = page_id(&root.0)?.0;
            queue.push_back(Node::Page {
                id: root.clone(),
                parent: InventoryParent::Root,
                root,
                row: None,
            });
        }
        let rules = &rules;
        let mut visited = BTreeSet::new();
        // (id, claimed parent) pairs already rejected, so a repeated wrong
        // tag is not re-read while the real parent's tag still is.
        let mut rejected = BTreeSet::new();
        let mut deferred: std::collections::BTreeMap<String, Vec<Node>> = Default::default();
        let mut accepted = BTreeSet::new();
        let mut walk = TreeWalk {
            run_id: notion_knowledge_core::logging::CorrelationId::new()
                .as_str()
                .to_owned(),
            entries: Vec::new(),
            skipped: Vec::new(),
            complete: true,
            calls: search_calls,
        };
        let mut progress = SyncProgressLog::new(SyncPhase::Discovery);
        let mut processed = 0;
        progress.report(0, None, 0, 0, false);
        let mut pending = FuturesUnordered::new();
        // Pages waiting for sink capacity. In-flight reads keep being polled
        // while the sink is full, and a read starts only while in-flight
        // reads plus waiting pages stay under FETCH_CONCURRENCY, so at most
        // that many walked pages are held here at once.
        let mut outbox: VecDeque<WalkedPage> = VecDeque::new();
        loop {
            while pending.len() + outbox.len() < FETCH_CONCURRENCY {
                let Some(node) = queue.pop_front() else { break };
                if rejected.contains(&node.claim()) {
                    continue;
                }
                if !visited.insert(node.id().to_owned()) {
                    // The first claim may still be rejected (a linked view
                    // read in flight); keep this one until that is known.
                    if !accepted.contains(node.id()) {
                        deferred.entry(node.id().to_owned()).or_default().push(node);
                    }
                    continue;
                }
                if visited.len() > MAX_NODES {
                    return Err(error(BackendErrorKind::Internal));
                }
                let task = visited.len();
                pending.push(sync_task(SyncPhase::Discovery, task, async move {
                    match node {
                        Node::Page {
                            id,
                            parent,
                            root,
                            row,
                        } => self.visit_page(id, parent, root, row, rules, search).await,
                        Node::Database { id, owner, root } => {
                            self.visit_database(id, owner, root, rules).await
                        }
                    }
                }));
            }
            let visit = match (sink, outbox.is_empty(), pending.is_empty()) {
                (_, true, true) => break,
                (Some(sink), false, true) => {
                    let permit = sink
                        .reserve()
                        .await
                        .map_err(|_| error(BackendErrorKind::Unavailable))?;
                    permit.send(outbox.pop_front().expect("outbox is not empty"));
                    continue;
                }
                (Some(sink), false, false) => {
                    let reserve = std::pin::pin!(sink.reserve());
                    match futures_util::future::select(reserve, pending.next()).await {
                        futures_util::future::Either::Left((permit, _)) => {
                            let permit =
                                permit.map_err(|_| error(BackendErrorKind::Unavailable))?;
                            permit.send(outbox.pop_front().expect("outbox is not empty"));
                            continue;
                        }
                        futures_util::future::Either::Right((visit, _)) => visit,
                    }
                }
                _ => pending.next().await,
            };
            let Some(visit) = visit else { continue };
            let visit = visit?;
            walk.calls += visit.calls;
            walk.complete &= !visit.incomplete;
            if let Some(claim) = visit.rejected {
                visited.remove(&claim.0);
                queue.extend(deferred.remove(&claim.0).unwrap_or_default());
                rejected.insert(claim);
            } else {
                // Settled: later claims for this id are plain repeats.
                deferred.remove(&visit.id);
                accepted.insert(visit.id.clone());
            }
            walk.skipped.extend(visit.skipped);
            queue.extend(visit.children);
            for page in visit.pages {
                if page.skip.is_none() {
                    sync_count(SyncCount::DiscoveredPage, 1);
                }
                walk.entries.push(page.entry.clone());
                if sink.is_some() {
                    outbox.push_back(page);
                }
            }
            processed += 1;
            progress.report(
                processed,
                None,
                walk.entries.len(),
                walk.skipped.len(),
                false,
            );
            if queue.len() + pending.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
        }
        progress.report(
            processed,
            Some(processed),
            walk.entries.len(),
            walk.skipped.len(),
            true,
        );
        walk.entries.sort_by(|a, b| a.page_id.cmp(&b.page_id));
        walk.skipped.sort_by(|a, b| a.id.cmp(&b.id));
        sync_total_pages(walk.readable().len());
        Ok(walk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::{
        backend::PageId,
        discovery::{ExclusionRules, SkipReason, SkippedContent},
        inventory::InventoryParent,
        lifecycle::{LifecycleScope, LifecycleStatus},
    };
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const ROOT: &str = "00000000-0000-0000-0000-000000000001";
    const P: &str = "00000000-0000-0000-0000-000000000002";
    const A: &str = "00000000-0000-0000-0000-000000000003";
    const D: &str = "00000000-0000-0000-0000-000000000004";
    const S: &str = "00000000-0000-0000-0000-000000000005";
    const ROW1: &str = "00000000-0000-0000-0000-000000000006";
    const ROW2: &str = "00000000-0000-0000-0000-000000000007";
    const X: &str = "00000000-0000-0000-0000-000000000008";
    const Q: &str = "00000000-0000-0000-0000-000000000009";
    const O: &str = "00000000-0000-0000-0000-00000000000a";

    type Routes = BTreeMap<String, (u16, Value)>;

    fn url(id: &str) -> String {
        format!("https://www.notion.so/{}", id.replace('-', ""))
    }
    fn page_tag(id: &str) -> String {
        format!("<page url=\"{}\">Child</page>\n", url(id))
    }
    fn parent(kind: &str, id: &str) -> Value {
        match kind {
            "workspace" => json!({"type":"workspace","workspace":true}),
            kind => json!({"type":kind, kind:id}),
        }
    }
    fn metadata(id: &str, parent: Value) -> Value {
        json!({"object":"page","id":id,"url":url(id),"in_trash":false,
            "last_edited_time":"2026-11-01T00:00:00.000Z","parent":parent,
            "properties":{"Name":{"id":"title","type":"title","title":[{"plain_text":"Private title"}]}}})
    }
    fn markdown(id: &str, body: &str, unknown: &[&str]) -> Value {
        json!({"object":"page_markdown","id":id,"markdown":body,"truncated":false,"unknown_block_ids":unknown})
    }
    /// A page reached through a page parent, with its Markdown body.
    fn add_page(routes: &mut Routes, id: &str, parent: Value, body: &str) {
        routes.insert(format!("GET /v1/pages/{id}"), (200, metadata(id, parent)));
        routes.insert(
            format!("GET /v1/pages/{id}/markdown"),
            (200, markdown(id, body, &[])),
        );
    }
    async fn serve(routes: Routes) -> (NotionClient, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    let mut bytes = [0; 4096];
                    let n = stream.read(&mut bytes).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    request.extend_from_slice(&bytes[..n]);
                }
                let text = String::from_utf8_lossy(&request);
                let line = text.lines().next().unwrap_or_default();
                let route = line.rsplit_once(' ').map_or(line, |(route, _)| route);
                let route = route.split('?').next().unwrap().to_owned();
                seen.lock().unwrap().push(route.clone());
                let (status, body) = routes
                    .get(&route)
                    .cloned()
                    .unwrap_or((404, json!({"object":"error","code":"object_not_found"})));
                let body = body.to_string();
                let _ = stream
                    .write_all(
                        format!(
                            "HTTP/1.1 {status} R\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                        .as_bytes(),
                    )
                    .await;
            }
        });
        (client, requests)
    }
    fn scope(exclusions: ExclusionRules) -> LifecycleScope {
        LifecycleScope {
            workspace_id: "00000000-0000-0000-0000-0000000000ff".into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions,
        }
    }
    /// root -> P -> database D (data source S) -> rows ROW1, ROW2.
    fn nested_tree() -> Routes {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(P));
        add_page(
            &mut routes,
            P,
            parent("page_id", ROOT),
            &format!("Intro\n<database url=\"{}\">Table</database>\n", url(D)),
        );
        routes.insert(
            format!("GET /v1/databases/{D}"),
            (
                200,
                json!({"object":"database","id":D,"in_trash":false,
                    "last_edited_time":"2026-11-01T00:00:00.000Z",
                    "parent":parent("page_id", P),"data_sources":[{"id":S,"name":"Table"}]}),
            ),
        );
        routes.insert(
            format!("GET /v1/data_sources/{S}"),
            (
                200,
                json!({"object":"data_source","id":S,"in_trash":false,
                    "last_edited_time":"2026-11-01T00:00:00.000Z",
                    "parent":{"type":"database_id","database_id":D}}),
            ),
        );
        let rows: Vec<_> = [ROW1, ROW2]
            .iter()
            .map(|id| metadata(id, parent("data_source_id", S)))
            .collect();
        routes.insert(
            format!("POST /v1/data_sources/{S}/query"),
            (
                200,
                json!({"object":"list","results":rows,"has_more":false,"next_cursor":null}),
            ),
        );
        for row in [ROW1, ROW2] {
            add_page(&mut routes, row, parent("data_source_id", S), "Row body\n");
            routes.insert(
                format!("GET /v1/pages/{row}/markdown"),
                (200, markdown(row, "Row body\n", &[])),
            );
        }
        routes
    }
    fn ids(walk: &TreeWalk) -> Vec<&str> {
        walk.entries.iter().map(|e| e.page_id.as_str()).collect()
    }

    #[test]
    fn child_references_extracts_page_and_database_tags_but_not_mentions() {
        let markdown = format!(
            "{}<database url=\"{}\" inline=\"true\">Table</database>\n\
             See <mention-page url=\"{}\"/> and [link]({}).\n\
             ```\n{}```\n\\{}",
            page_tag(A),
            url(D),
            url(O),
            url(O),
            page_tag(O),
            page_tag(O)
        );
        assert_eq!(
            child_references(&markdown),
            [ChildRef::Page(A.into()), ChildRef::Database(D.into())]
        );
    }

    #[tokio::test]
    async fn walk_reaches_nested_pages_and_rows_with_expected_call_count() {
        let (client, requests) = serve(nested_tree()).await;
        let (sink, mut pages) = tokio::sync::mpsc::channel(16);
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), Some(&sink))
            .await
            .unwrap();
        drop(sink);
        assert!(walk.complete);
        assert!(walk.skipped.is_empty());
        assert_eq!(ids(&walk), [ROOT, P, ROW1, ROW2]);
        let parents: Vec<_> = walk.entries.iter().map(|e| e.parent.clone()).collect();
        let row = InventoryParent::DataSource {
            data_source_id: S.into(),
            database_id: D.into(),
            owner_page_id: P.into(),
        };
        assert_eq!(
            parents,
            [
                InventoryParent::Root,
                InventoryParent::Page(ROOT.into()),
                row.clone(),
                row
            ]
        );
        assert!(walk.entries.iter().all(|e| e.root_id == ROOT));
        // Two reads for each non-row page, one database read, one query and
        // one Markdown read per row: rows take their metadata from the query.
        assert_eq!(walk.calls, 2 * 2 + 1 + 1 + 2);
        assert_eq!(requests.lock().unwrap().len() as u64, walk.calls);
        let mut streamed = Vec::new();
        while let Some(page) = pages.recv().await {
            assert!(page.content.is_some());
            streamed.push(page.entry.page_id);
        }
        streamed.sort();
        assert_eq!(streamed, [ROOT, P, ROW1, ROW2]);
    }

    #[tokio::test]
    async fn walk_follows_unknown_block_ids_and_types_404_as_inaccessible() {
        let mut routes = Routes::new();
        routes.insert(
            format!("GET /v1/pages/{ROOT}"),
            (200, metadata(ROOT, parent("workspace", ""))),
        );
        routes.insert(
            format!("GET /v1/pages/{ROOT}/markdown"),
            (200, markdown(ROOT, "Visible\n<unknown/>\n", &[X])),
        );
        routes.insert(
            format!("GET /v1/pages/{X}/markdown"),
            (200, markdown(X, &page_tag(Q), &[])),
        );
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(walk.complete);
        assert!(
            requests
                .lock()
                .unwrap()
                .contains(&format!("GET /v1/pages/{Q}"))
        );
        assert_eq!(
            walk.skipped,
            [SkippedContent {
                id: Q.into(),
                reason: SkipReason::Inaccessible
            }]
        );
        // The unreadable child still counts in the inventory, once.
        assert_eq!(ids(&walk), [ROOT, Q]);
        assert_eq!(walk.entries[1].parent, InventoryParent::Page(ROOT.into()));
    }

    #[tokio::test]
    async fn walk_reaches_repeated_child_once_with_first_parent() {
        let mut routes = Routes::new();
        add_page(
            &mut routes,
            ROOT,
            parent("workspace", ""),
            &format!("{}{}", page_tag(P), page_tag(A)),
        );
        add_page(
            &mut routes,
            A,
            parent("page_id", ROOT),
            &format!("{}{}", page_tag(P), page_tag(ROOT)),
        );
        add_page(&mut routes, P, parent("page_id", ROOT), "Leaf\n");
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(walk.complete);
        assert_eq!(ids(&walk), [ROOT, P, A]);
        assert_eq!(walk.entries[1].parent, InventoryParent::Page(ROOT.into()));
        let requests = requests.lock().unwrap();
        for id in [ROOT, P, A] {
            let reads = requests
                .iter()
                .filter(|r| **r == format!("GET /v1/pages/{id}/markdown"))
                .count();
            assert_eq!(reads, 1, "{id} is read once");
        }
    }

    #[tokio::test]
    async fn walk_prunes_excluded_subtree_without_reading_its_content() {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(A));
        add_page(&mut routes, A, parent("page_id", ROOT), &page_tag(P));
        add_page(&mut routes, P, parent("page_id", A), "Never read\n");
        let exclusions = ExclusionRules {
            page_ids: [A.to_owned()].into(),
            ..Default::default()
        };
        let (client, requests) = serve(routes).await;
        let walk = client.walk_tree(&scope(exclusions), None).await.unwrap();
        assert!(walk.complete);
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| !r.contains(A) && !r.contains(P))
        );
        assert_eq!(
            walk.skipped,
            [SkippedContent {
                id: A.into(),
                reason: SkipReason::ExcludedPage
            }]
        );
        assert_eq!(ids(&walk), [ROOT, A]);
    }

    #[tokio::test]
    async fn walk_keeps_descendant_excluded_page_but_not_its_children() {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(A));
        add_page(&mut routes, A, parent("page_id", ROOT), &page_tag(P));
        add_page(&mut routes, P, parent("page_id", A), "Never read\n");
        let exclusions = ExclusionRules {
            descendants_of: [A.to_owned()].into(),
            ..Default::default()
        };
        let (client, requests) = serve(routes).await;
        let walk = client.walk_tree(&scope(exclusions), None).await.unwrap();
        assert_eq!(ids(&walk), [ROOT, A]);
        assert!(walk.skipped.is_empty());
        assert!(requests.lock().unwrap().iter().all(|r| !r.contains(P)));
    }

    #[tokio::test]
    async fn walk_never_follows_links_outside_root() {
        let mut routes = Routes::new();
        add_page(
            &mut routes,
            ROOT,
            parent("workspace", ""),
            &format!(
                "[outside]({})\n<mention-page url=\"{}\"/>\n",
                url(O),
                url(O)
            ),
        );
        add_page(&mut routes, O, parent("workspace", ""), "Outside\n");
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert_eq!(ids(&walk), [ROOT]);
        assert!(requests.lock().unwrap().iter().all(|r| !r.contains(O)));
    }

    #[tokio::test]
    async fn walk_rejects_a_child_tag_whose_page_lives_elsewhere() {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(O));
        add_page(&mut routes, O, parent("workspace", ""), &page_tag(P));
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert_eq!(ids(&walk), [ROOT]);
        assert!(
            walk.skipped.is_empty(),
            "an outside page is not inventoried"
        );
        assert!(
            !requests
                .lock()
                .unwrap()
                .contains(&format!("GET /v1/pages/{O}/markdown"))
        );
    }

    #[tokio::test]
    async fn linked_view_reached_first_does_not_hide_the_real_subtree() {
        // Root tags A then P. A holds a linked view of D; D really lives in P.
        let mut routes = nested_tree();
        add_page(
            &mut routes,
            ROOT,
            parent("workspace", ""),
            &format!("{}{}", page_tag(A), page_tag(P)),
        );
        add_page(
            &mut routes,
            A,
            parent("page_id", ROOT),
            &format!("<database url=\"{}\">View</database>\n", url(D)),
        );
        let (client, _) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(walk.authoritative());
        assert_eq!(ids(&walk), [ROOT, P, A, ROW1, ROW2]);
        assert_eq!(
            walk.entries[3].parent,
            InventoryParent::DataSource {
                data_source_id: S.into(),
                database_id: D.into(),
                owner_page_id: P.into(),
            }
        );
    }

    #[tokio::test]
    async fn unreadable_unknown_block_is_a_typed_skip_not_doubt() {
        let mut routes = Routes::new();
        routes.insert(
            format!("GET /v1/pages/{ROOT}"),
            (200, metadata(ROOT, parent("workspace", ""))),
        );
        routes.insert(
            format!("GET /v1/pages/{ROOT}/markdown"),
            (200, markdown(ROOT, "Visible\n", &[X])),
        );
        let (client, _) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(walk.authoritative());
        assert_eq!(
            walk.skipped,
            [SkippedContent {
                id: X.into(),
                reason: SkipReason::Inaccessible
            }]
        );
    }

    #[tokio::test]
    async fn transient_child_failures_fail_the_walk_instead_of_skipping() {
        let mut routes = Routes::new();
        routes.insert(
            format!("GET /v1/pages/{ROOT}"),
            (200, metadata(ROOT, parent("workspace", ""))),
        );
        routes.insert(
            format!("GET /v1/pages/{ROOT}/markdown"),
            (200, markdown(ROOT, "Visible\n", &[X])),
        );
        routes.insert(format!("GET /v1/pages/{X}/markdown"), (500, json!({})));
        let (client, _) = serve(routes).await;
        let error = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind,
            notion_knowledge_core::backend::BackendErrorKind::Unavailable
        );
    }

    #[tokio::test]
    async fn unreadable_body_after_readable_metadata_is_not_authoritative() {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(P));
        routes.insert(
            format!("GET /v1/pages/{P}"),
            (200, metadata(P, parent("page_id", ROOT))),
        );
        let (client, _) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(!walk.authoritative());
        assert_eq!(
            walk.skipped,
            [SkippedContent {
                id: P.into(),
                reason: SkipReason::Inaccessible
            }]
        );
    }

    #[tokio::test]
    async fn unprovable_block_ancestry_is_not_authoritative() {
        let mut routes = Routes::new();
        add_page(&mut routes, ROOT, parent("workspace", ""), &page_tag(P));
        // P sits in a block the integration cannot read.
        add_page(&mut routes, P, parent("block_id", X), "Nested\n");
        let (client, _) = serve(routes).await;
        let walk = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(!walk.authoritative());
        assert_eq!(ids(&walk), [ROOT]);
    }

    #[tokio::test]
    async fn unreadable_root_fails_the_walk() {
        let (client, _) = serve(Routes::new()).await;
        let error = client
            .walk_tree(&scope(ExclusionRules::default()), None)
            .await
            .unwrap_err();
        assert_eq!(
            error.kind,
            notion_knowledge_core::backend::BackendErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn walk_matches_old_lifecycle_membership_on_fixture_tree() {
        let (client, _) = serve(nested_tree()).await;
        let scope = scope(ExclusionRules::default());
        let walk = client.walk_tree(&scope, None).await.unwrap();
        let mut allowed = Vec::new();
        for id in [ROOT, P, ROW1, ROW2, A, O] {
            if let Ok(evidence) = client.resolve_lifecycle(&PageId(id.into()), &scope).await
                && matches!(evidence.status, LifecycleStatus::Allowed { .. })
            {
                allowed.push(id);
            }
        }
        assert_eq!(ids(&walk), allowed);
    }

    /// Root -> pages A, P, Q (all tagged in the root's Markdown).
    fn flat_tree() -> Routes {
        let mut routes = Routes::new();
        let tags = [A, P, Q].map(page_tag).concat();
        add_page(&mut routes, ROOT, parent("workspace", ""), &tags);
        for id in [A, P, Q] {
            add_page(&mut routes, id, parent("page_id", ROOT), "Body\n");
        }
        routes
    }
    fn listing(pages: Vec<Value>) -> (u16, Value) {
        (
            200,
            json!({"object":"list","results":pages,"has_more":false,"next_cursor":null}),
        )
    }
    fn fetched(requests: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        let mut ids: Vec<_> = requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|r| r.strip_prefix("GET /v1/pages/"))
            .filter(|r| !r.ends_with("/markdown"))
            .map(str::to_owned)
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn walk_uses_search_metadata_and_skips_fetch_page() {
        let mut routes = flat_tree();
        let listed = [A, P, Q].map(|id| metadata(id, parent("page_id", ROOT)));
        routes.insert("POST /v1/search".into(), listing(listed.to_vec()));
        let (client, requests) = serve(routes).await;
        let (sink, mut pages) = tokio::sync::mpsc::channel(16);
        let walk = client
            .walk_tree_with_search(&scope(ExclusionRules::default()), Some(&sink))
            .await
            .unwrap();
        drop(sink);
        assert!(walk.authoritative());
        assert_eq!(ids(&walk), [ROOT, P, A, Q]);
        // One listing, the root's two reads, then one Markdown read per page.
        assert_eq!(fetched(&requests), [ROOT]);
        assert_eq!(walk.calls, 1 + 2 + 3);
        assert_eq!(requests.lock().unwrap().len() as u64, walk.calls);
        while let Some(page) = pages.recv().await {
            assert_eq!(page.content.unwrap().page.title, "Private title");
        }
    }

    #[tokio::test]
    async fn walk_falls_back_to_fetch_page_for_missing_partial_or_moved_entries() {
        let mut routes = flat_tree();
        let mut partial = metadata(P, parent("page_id", ROOT));
        partial.as_object_mut().unwrap().remove("properties");
        let moved = metadata(Q, parent("page_id", O));
        // A is missing from the listing.
        routes.insert("POST /v1/search".into(), listing(vec![partial, moved]));
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree_with_search(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert!(walk.authoritative());
        assert_eq!(ids(&walk), [ROOT, P, A, Q]);
        let mut expected = vec![ROOT.to_owned(), A.into(), P.into(), Q.into()];
        expected.sort();
        assert_eq!(fetched(&requests), expected);
    }

    #[tokio::test]
    async fn search_only_pages_outside_walk_are_never_indexed() {
        let mut routes = flat_tree();
        let mut listed = [A, P, Q]
            .map(|id| metadata(id, parent("page_id", ROOT)))
            .to_vec();
        listed.push(metadata(O, parent("page_id", ROOT)));
        listed.push(metadata(X, parent("workspace", "")));
        routes.insert("POST /v1/search".into(), listing(listed));
        add_page(&mut routes, O, parent("page_id", ROOT), "Untagged\n");
        let (client, requests) = serve(routes).await;
        let walk = client
            .walk_tree_with_search(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert_eq!(ids(&walk), [ROOT, P, A, Q]);
        assert!(!walk.readable().contains(O) && !walk.readable().contains(X));
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|r| !r.contains(O) && !r.contains(X))
        );
    }

    #[tokio::test]
    async fn failed_search_listing_falls_back_to_reading_every_page() {
        // No search route: the listing 404s and the walk reads metadata itself.
        let (client, requests) = serve(flat_tree()).await;
        let walk = client
            .walk_tree_with_search(&scope(ExclusionRules::default()), None)
            .await
            .unwrap();
        assert_eq!(ids(&walk), [ROOT, P, A, Q]);
        assert_eq!(fetched(&requests).len(), 4);
    }

    #[tokio::test]
    async fn walk_vouches_only_for_listed_unexcluded_children_of_the_page() {
        let mut routes = flat_tree();
        // A is listed under ROOT, P is listed under another page, Q is missing.
        let listed = vec![
            metadata(A, parent("page_id", ROOT)),
            metadata(P, parent("page_id", O)),
        ];
        routes.insert("POST /v1/search".into(), listing(listed));
        let (client, _) = serve(routes.clone()).await;
        let (sink, mut pages) = tokio::sync::mpsc::channel(16);
        client
            .walk_tree_with_search(&scope(ExclusionRules::default()), Some(&sink))
            .await
            .unwrap();
        drop(sink);
        let mut root = None;
        while let Some(page) = pages.recv().await {
            if page.entry.page_id == ROOT {
                root = Some(page);
            }
        }
        assert_eq!(root.unwrap().children, BTreeSet::from([A.to_owned()]));

        // An excluded child, and every child of a pruned page, is never vouched for.
        for exclusions in [
            ExclusionRules {
                page_ids: BTreeSet::from([A.to_owned()]),
                ..Default::default()
            },
            ExclusionRules {
                descendants_of: BTreeSet::from([ROOT.to_owned()]),
                ..Default::default()
            },
        ] {
            let (client, _) = serve(routes.clone()).await;
            let (sink, mut pages) = tokio::sync::mpsc::channel(16);
            client
                .walk_tree_with_search(&scope(exclusions), Some(&sink))
                .await
                .unwrap();
            drop(sink);
            while let Some(page) = pages.recv().await {
                assert!(page.children.is_empty(), "{:?}", page.entry.page_id);
            }
        }
    }
}
