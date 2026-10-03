//! Walk physical ancestry only, never global search or cross-page references.
use crate::{NotionClient, pages::page_id};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::{
        DiscoveredPage, DiscoveryReport, ScopedDiscovery, SkipReason, SkippedContent, inaccessible,
    },
};
use reqwest::{Method, header::AUTHORIZATION};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const VERSION: &str = "2026-03-11";
// Fail explicitly rather than silently truncate enormous/malformed graphs.
const MAX_NODES: usize = 100_000;
const MAX_LIST_PAGES: usize = 10_000;
fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.discover",
        retry_after: None,
    }
}
fn text(value: &Value) -> Result<&str, BackendError> {
    value
        .as_str()
        .ok_or_else(|| error(BackendErrorKind::Internal))
}
fn id(value: &Value) -> Result<String, BackendError> {
    page_id(text(value)?)
        .map(|v| v.0)
        .map_err(|_| error(BackendErrorKind::Internal))
}
fn parent(value: &Value, expected: &str, kinds: &[&str]) -> Result<bool, BackendError> {
    let kind = text(&value["parent"]["type"])?;
    if !kinds.contains(&kind) {
        return Ok(false);
    }
    Ok(id(&value["parent"][kind])? == expected)
}
fn archived(value: &Value) -> Result<bool, BackendError> {
    let archived = value["archived"]
        .as_bool()
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    // Older objects may omit in_trash; archived remains authoritative.
    Ok(archived || value["in_trash"] == true)
}
fn page(value: &Value, expected: &str) -> Result<DiscoveredPage, BackendError> {
    if value["object"] != "page" || id(&value["id"])? != expected {
        return Err(error(BackendErrorKind::Internal));
    }
    let url = text(&value["url"])?.to_owned();
    if page_id(&url)
        .map_err(|_| error(BackendErrorKind::Internal))?
        .0
        != expected
    {
        return Err(error(BackendErrorKind::Internal));
    }
    let properties = value["properties"]
        .as_object()
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    let titles: Vec<_> = properties
        .values()
        .filter(|p| p["type"] == "title")
        .collect();
    if titles.len() != 1 {
        return Err(error(BackendErrorKind::Internal));
    }
    let title = titles[0]["title"]
        .as_array()
        .ok_or_else(|| error(BackendErrorKind::Internal))?
        .iter()
        .map(|v| text(&v["plain_text"]))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    Ok(DiscoveredPage {
        id: expected.to_owned(),
        url,
        title,
    })
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Page,
    Block,
    Database,
    DataSource,
}
struct Node {
    kind: Kind,
    id: String,
    parent: Option<String>,
}
impl NotionClient {
    async fn crawl_request(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
    ) -> Result<Value, BackendError> {
        let mut request = self
            .http
            .request(method, format!("{}/{path}", self.api_root))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", VERSION);
        if let Some(body) = body {
            request = request.json(body);
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| error(BackendErrorKind::Unavailable))?;
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
    async fn crawl_list(&self, node: &Node) -> Result<Vec<Value>, BackendError> {
        let mut cursor: Option<String> = None;
        let mut cursors = BTreeSet::new();
        let mut values = Vec::new();
        for _ in 0..MAX_LIST_PAGES {
            let value = if node.kind == Kind::DataSource {
                let mut body = json!({"page_size":100});
                if let Some(cursor) = &cursor {
                    body["start_cursor"] = json!(cursor);
                }
                self.crawl_request(
                    Method::POST,
                    &format!("data_sources/{}/query", node.id),
                    Some(&body),
                )
                .await?
            } else {
                let mut url =
                    reqwest::Url::parse(&format!("{}/blocks/{}/children", self.api_root, node.id))
                        .map_err(|_| error(BackendErrorKind::Internal))?;
                url.query_pairs_mut().append_pair("page_size", "100");
                if let Some(cursor) = &cursor {
                    url.query_pairs_mut().append_pair("start_cursor", cursor);
                }
                // Path/query are adapter generated; strip only the known API root.
                self.crawl_request(
                    Method::GET,
                    url.as_str()
                        .strip_prefix(&format!("{}/", self.api_root))
                        .ok_or_else(|| error(BackendErrorKind::Internal))?,
                    None,
                )
                .await?
            };
            if value["object"] != "list" {
                return Err(error(BackendErrorKind::Internal));
            }
            let items = value["results"]
                .as_array()
                .ok_or_else(|| error(BackendErrorKind::Internal))?;
            if values.len() + items.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
            values.extend(items.iter().cloned());
            if !value["has_more"]
                .as_bool()
                .ok_or_else(|| error(BackendErrorKind::Internal))?
            {
                return Ok(values);
            }
            let next = text(&value["next_cursor"])?.to_owned();
            if next.is_empty() || !cursors.insert(next.clone()) {
                return Err(error(BackendErrorKind::Internal));
            }
            cursor = Some(next);
        }
        Err(error(BackendErrorKind::Internal))
    }
    /// Complete, read-only physical descendant discovery. No index writes, search,
    /// link following, cached progress or persisted secrets. Safely restartable.
    pub async fn crawl_roots(&self, roots: &[PageId]) -> Result<DiscoveryReport, BackendError> {
        if roots.is_empty() {
            return Err(error(BackendErrorKind::InvalidInput));
        }
        let roots = roots
            .iter()
            .map(|v| page_id(&v.0).map(|v| v.0))
            .collect::<Result<BTreeSet<_>, _>>()?;
        let mut queue: VecDeque<_> = roots
            .iter()
            .map(|id| Node {
                kind: Kind::Page,
                id: id.clone(),
                parent: None,
            })
            .collect();
        let mut visited = BTreeSet::new();
        let mut pages = BTreeMap::new();
        let mut skipped = BTreeMap::new();
        while let Some(node) = queue.pop_front() {
            if !visited.insert((node.kind, node.id.clone())) {
                continue;
            }
            if visited.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
            let result = self
                .crawl_node(&node, &roots, &mut queue, &mut pages, &mut skipped)
                .await;
            if let Err(e) = result {
                // A configured root must be readable. Only descendants' access
                // failures are legitimate skips; outage/rate-limit never succeed.
                if node.parent.is_some() && inaccessible(e.kind) {
                    skipped.insert(node.id.clone(), SkipReason::Inaccessible);
                } else {
                    return Err(e);
                }
            }
            if queue.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
        }
        Ok(DiscoveryReport {
            roots: roots.into_iter().collect(),
            pages: pages.into_values().collect(),
            skipped: skipped
                .into_iter()
                .map(|(id, reason)| SkippedContent { id, reason })
                .collect(),
        })
    }
    async fn source_owner_in_scope(
        &self,
        source: &str,
        roots: &BTreeSet<String>,
    ) -> Result<Option<String>, BackendError> {
        let mut current = source.to_owned();
        let mut endpoint = "data_sources";
        let mut object = "data_source";
        let mut seen = BTreeSet::new();
        let mut owner = None;
        loop {
            if endpoint == "pages" && roots.contains(&current) {
                return Ok(owner);
            }
            if !seen.insert((endpoint, current.clone())) {
                return Ok(None);
            }
            if seen.len() > MAX_NODES {
                return Err(error(BackendErrorKind::Internal));
            }
            let value = self
                .crawl_request(Method::GET, &format!("{endpoint}/{current}"), None)
                .await?;
            if value["object"] != object || id(&value["id"])? != current {
                return Err(error(BackendErrorKind::Internal));
            }
            if archived(&value)? {
                return Ok(None);
            }
            let kind = text(&value["parent"]["type"])?;
            match kind {
                "page_id" => {
                    endpoint = "pages";
                    object = "page";
                }
                "block_id" => {
                    endpoint = "blocks";
                    object = "block";
                }
                "database_id" => {
                    endpoint = "databases";
                    object = "database";
                }
                "data_source_id" => {
                    endpoint = "data_sources";
                    object = "data_source";
                }
                "workspace" => return Ok(None),
                _ => return Err(error(BackendErrorKind::Internal)),
            }
            current = id(&value["parent"][kind])?;
            if owner.is_none() {
                owner = Some(current.clone());
            }
        }
    }
    async fn crawl_node(
        &self,
        node: &Node,
        roots: &BTreeSet<String>,
        queue: &mut VecDeque<Node>,
        pages: &mut BTreeMap<String, DiscoveredPage>,
        skipped: &mut BTreeMap<String, SkipReason>,
    ) -> Result<(), BackendError> {
        {
            let endpoint = match node.kind {
                Kind::Page => "pages",
                Kind::Database => "databases",
                Kind::Block => "blocks",
                Kind::DataSource => "data_sources",
            };
            let value = self
                .crawl_request(Method::GET, &format!("{endpoint}/{}", node.id), None)
                .await?;
            let object = match node.kind {
                Kind::Page => "page",
                Kind::Database => "database",
                Kind::Block => "block",
                Kind::DataSource => "data_source",
            };
            if value["object"] != object || id(&value["id"])? != node.id {
                return Err(error(BackendErrorKind::Internal));
            }
            if let Some(expected) = &node.parent {
                let kinds: &[&str] = if node.kind == Kind::DataSource {
                    &["database_id", "data_source_id"]
                } else if node.kind == Kind::Page {
                    &["page_id", "block_id", "data_source_id"]
                } else {
                    &["page_id", "block_id"]
                };
                if !parent(&value, expected, kinds)? {
                    skipped.insert(node.id.clone(), SkipReason::OutsideScope);
                    return Ok(());
                }
            }
            if archived(&value)? {
                skipped.insert(node.id.clone(), SkipReason::Archived);
                return Ok(());
            }
            if node.kind == Kind::Block {
                let kind = text(&value["type"])?;
                if kind == "link_to_page" {
                    return Ok(());
                }
                if kind == "synced_block" {
                    let origin = value["synced_block"]
                        .get("synced_from")
                        .ok_or_else(|| error(BackendErrorKind::Internal))?;
                    if !origin.is_null() {
                        skipped.insert(node.id.clone(), SkipReason::SyncedReference);
                        return Ok(());
                    }
                }
            }
            if node.kind == Kind::Database {
                for source in value["data_sources"]
                    .as_array()
                    .ok_or_else(|| error(BackendErrorKind::Internal))?
                {
                    queue.push_back(Node {
                        kind: Kind::DataSource,
                        id: id(&source["id"])?,
                        parent: Some(node.id.clone()),
                    });
                }
                return Ok(());
            }
            if node.kind == Kind::Page {
                pages.insert(node.id.clone(), page(&value, &node.id)?);
            }
        }
        for value in self.crawl_list(node).await? {
            let child_id = id(&value["id"])?;
            if node.kind == Kind::DataSource {
                if value["object"] == "data_source" {
                    // Wiki queries expose child databases' data sources. Check
                    // their actual ancestry; a linked external source is not scope.
                    let owner = match self.source_owner_in_scope(&child_id, roots).await {
                        Ok(owner) => owner,
                        Err(e) if inaccessible(e.kind) => {
                            skipped.insert(child_id, SkipReason::Inaccessible);
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    if let Some(owner) = owner {
                        queue.push_back(Node {
                            kind: Kind::DataSource,
                            id: child_id,
                            parent: Some(owner),
                        });
                    } else {
                        skipped.insert(child_id, SkipReason::OutsideScope);
                    }
                    continue;
                }
                if value["object"] != "page" {
                    return Err(error(BackendErrorKind::Internal));
                }
                if !parent(&value, &node.id, &["data_source_id"])? {
                    skipped.insert(child_id, SkipReason::OutsideScope);
                    continue;
                }
                queue.push_back(Node {
                    kind: Kind::Page,
                    id: child_id,
                    parent: Some(node.id.clone()),
                });
                continue;
            }
            if value["object"] != "block" {
                return Err(error(BackendErrorKind::Internal));
            }
            if !parent(&value, &node.id, &["page_id", "block_id"])? {
                skipped.insert(child_id, SkipReason::OutsideScope);
                continue;
            }
            if archived(&value)? {
                skipped.insert(child_id, SkipReason::Archived);
                continue;
            }
            let kind = text(&value["type"])?;
            let node_kind = match kind {
                "child_page" => Kind::Page,
                "child_database" => Kind::Database,
                // Referenced content is not a physical descendant, even if the
                // API exposes synced children from a shared outside page.
                "link_to_page" => continue,
                "synced_block"
                    if !value["synced_block"]
                        .get("synced_from")
                        .ok_or_else(|| error(BackendErrorKind::Internal))?
                        .is_null() =>
                {
                    skipped.insert(child_id, SkipReason::SyncedReference);
                    continue;
                }
                _ => {
                    if !value["has_children"]
                        .as_bool()
                        .ok_or_else(|| error(BackendErrorKind::Internal))?
                    {
                        continue;
                    }
                    Kind::Block
                }
            };
            queue.push_back(Node {
                kind: node_kind,
                id: child_id,
                parent: Some(node.id.clone()),
            });
        }
        Ok(())
    }
}
impl ScopedDiscovery for NotionClient {
    fn discover<'a>(&'a self, roots: &'a [PageId]) -> BackendFuture<'a, DiscoveryReport> {
        Box::pin(self.crawl_roots(roots))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const ROOT: &str = "00000000-0000-0000-0000-000000000001";
    const CHILD: &str = "00000000-0000-0000-0000-000000000002";
    const BLOCK: &str = "00000000-0000-0000-0000-000000000003";
    const DB: &str = "00000000-0000-0000-0000-000000000004";
    const SOURCE: &str = "00000000-0000-0000-0000-000000000005";
    const ROW: &str = "00000000-0000-0000-0000-000000000006";
    const OUT: &str = "00000000-0000-0000-0000-000000000007";
    fn parent_value(kind: &str, id: &str) -> Value {
        json!({"type":kind,kind:id})
    }
    fn page_value(id: &str, parent: Value) -> Value {
        json!({"object":"page","id":id,"parent":parent,"archived":false,"url":format!("https://www.notion.so/{id}"),"properties":{"Title":{"id":"title","type":"title","title":[{"plain_text":id}]}}})
    }
    fn block(id: &str, parent: &str, kind: &str, children: bool) -> Value {
        json!({"object":"block","id":id,"parent":parent_value("block_id",parent),"archived":false,"type":kind,"has_children":children})
    }
    fn list(values: Vec<Value>, cursor: Option<&str>) -> Value {
        json!({"object":"list","results":values,"has_more":cursor.is_some(),"next_cursor":cursor})
    }
    fn database(id: &str, parent: &str, sources: Vec<&str>) -> Value {
        json!({"object":"database","id":id,"parent":parent_value("page_id",parent),"archived":false,"data_sources":sources.into_iter().map(|id|json!({"id":id})).collect::<Vec<_>>()})
    }
    fn source(id: &str, parent: &str) -> Value {
        json!({"object":"data_source","id":id,"parent":parent_value("database_id",parent),"archived":false})
    }
    fn get(path: String, value: Value) -> (String, u16, Value) {
        (format!("GET /v1/{path}"), 200, value)
    }
    fn children(id: &str, values: Vec<Value>) -> (String, u16, Value) {
        get(
            format!("blocks/{id}/children?page_size=100"),
            list(values, None),
        )
    }
    async fn mock(
        steps: Vec<(String, u16, Value)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential").unwrap();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (expected, status, value) in steps {
                let (mut socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 4096];
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..i]);
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(|n| n.parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        if bytes.len() >= i + 4 + length {
                            break;
                        }
                    }
                }
                let request = String::from_utf8(bytes).unwrap();
                assert!(
                    request.starts_with(&format!("{expected} HTTP/1.1\r\n")),
                    "expected {expected}, got {request}"
                );
                assert!(
                    request
                        .to_lowercase()
                        .contains(&format!("notion-version: {VERSION}\r\n"))
                );
                assert!(
                    request
                        .to_lowercase()
                        .contains("authorization: bearer test-credential\r\n")
                );
                let body = value.to_string();
                socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                requests.push(request);
            }
            requests
        });
        (client, task)
    }
    fn roots() -> Vec<PageId> {
        vec![PageId(ROOT.into())]
    }
    #[tokio::test]
    async fn physical_descendants_include_nested_pages_and_all_database_rows_without_following_references()
     {
        let mut link = block(OUT, ROOT, "link_to_page", true);
        link["link_to_page"] = json!({"type":"page_id","page_id":OUT});
        let mut synced = block(
            "00000000-0000-0000-0000-000000000008",
            ROOT,
            "synced_block",
            true,
        );
        synced["synced_block"] = json!({"synced_from":{"block_id":OUT}});
        let steps = vec![
            get(
                format!("pages/{ROOT}"),
                page_value(ROOT, json!({"type":"workspace","workspace":true})),
            ),
            get(
                format!("blocks/{ROOT}/children?page_size=100"),
                list(
                    vec![
                        block(BLOCK, ROOT, "toggle", true),
                        block(DB, ROOT, "child_database", false),
                        link,
                        synced,
                    ],
                    Some("a +/?"),
                ),
            ),
            get(
                format!("blocks/{ROOT}/children?page_size=100&start_cursor=a+%2B%2F%3F"),
                list(
                    vec![
                        block(CHILD, ROOT, "child_page", false),
                        block(CHILD, ROOT, "child_page", false),
                    ],
                    None,
                ),
            ),
            get(
                format!("blocks/{BLOCK}"),
                block(BLOCK, ROOT, "toggle", true),
            ),
            children(
                BLOCK,
                vec![
                    block(CHILD, BLOCK, "child_page", false),
                    block(BLOCK, BLOCK, "toggle", true),
                ],
            ),
            get(format!("databases/{DB}"), database(DB, ROOT, vec![SOURCE])),
            get(
                format!("pages/{CHILD}"),
                page_value(CHILD, parent_value("page_id", ROOT)),
            ),
            children(CHILD, vec![block(ROOT, CHILD, "child_page", false)]),
            get(format!("data_sources/{SOURCE}"), source(SOURCE, DB)),
            (
                format!("POST /v1/data_sources/{SOURCE}/query"),
                200,
                list(
                    vec![page_value(ROW, parent_value("data_source_id", SOURCE))],
                    Some("row-next"),
                ),
            ),
            (
                format!("POST /v1/data_sources/{SOURCE}/query"),
                200,
                list(
                    vec![page_value(ROW, parent_value("data_source_id", SOURCE))],
                    None,
                ),
            ),
            get(
                format!("pages/{ROW}"),
                page_value(ROW, parent_value("data_source_id", SOURCE)),
            ),
            children(ROW, vec![]),
        ];
        let (client, task) = mock(steps).await;
        let report = client
            .crawl_roots(&[PageId(ROOT.into()), PageId(ROOT.replace('-', ""))])
            .await
            .unwrap();
        assert_eq!(report.roots, vec![ROOT]);
        assert_eq!(
            report
                .pages
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec![ROOT, CHILD, ROW]
        );
        assert_eq!(report.skipped.len(), 1);
        assert_eq!(report.skipped[0].reason, SkipReason::SyncedReference);
        let requests = task.await.unwrap();
        let queries: Vec<_> = requests.iter().filter(|r| r.starts_with("POST")).collect();
        assert_eq!(queries.len(), 2);
        assert!(queries[1].contains("\"start_cursor\":\"row-next\""));
        assert!(
            requests
                .iter()
                .all(|r| r.starts_with("GET") || r.contains("/query HTTP"))
        );
    }
    #[tokio::test]
    async fn moved_pages_and_external_linked_data_sources_are_excluded_before_content_reads() {
        let steps = vec![
            get(
                format!("pages/{ROOT}"),
                page_value(ROOT, json!({"type":"workspace","workspace":true})),
            ),
            children(
                ROOT,
                vec![
                    block(CHILD, ROOT, "child_page", false),
                    block(DB, ROOT, "child_database", false),
                    block(ROW, OUT, "child_page", false),
                ],
            ),
            get(
                format!("pages/{CHILD}"),
                page_value(CHILD, parent_value("page_id", OUT)),
            ),
            get(format!("databases/{DB}"), database(DB, ROOT, vec![SOURCE])),
            get(format!("data_sources/{SOURCE}"), source(SOURCE, OUT)),
        ];
        let (client, task) = mock(steps).await;
        let report = client.crawl_roots(&roots()).await.unwrap();
        assert_eq!(report.pages.len(), 1);
        assert_eq!(report.pages[0].id, ROOT);
        assert_eq!(report.skipped.len(), 3);
        assert!(
            report
                .skipped
                .iter()
                .all(|s| s.reason == SkipReason::OutsideScope)
        );
        task.await.unwrap();
    }
    #[tokio::test]
    async fn failed_pagination_can_restart_without_checkpoint_loss_or_duplicate_output() {
        let prefix = || {
            vec![
                get(
                    format!("pages/{ROOT}"),
                    page_value(ROOT, json!({"type":"workspace","workspace":true})),
                ),
                get(
                    format!("blocks/{ROOT}/children?page_size=100"),
                    list(vec![block(CHILD, ROOT, "child_page", false)], Some("next")),
                ),
            ]
        };
        let mut steps = prefix();
        steps.push((
            format!("GET /v1/blocks/{ROOT}/children?page_size=100&start_cursor=next"),
            503,
            json!({"message":"private test-credential"}),
        ));
        steps.extend(prefix());
        steps.push(get(
            format!("blocks/{ROOT}/children?page_size=100&start_cursor=next"),
            list(vec![], None),
        ));
        steps.push(get(
            format!("pages/{CHILD}"),
            page_value(CHILD, parent_value("page_id", ROOT)),
        ));
        steps.push(children(CHILD, vec![]));
        let (client, task) = mock(steps).await;
        let error = client.crawl_roots(&roots()).await.unwrap_err();
        assert_eq!(error.kind, BackendErrorKind::Unavailable);
        assert!(!error.to_string().contains("test-credential"));
        let report = client.crawl_roots(&roots()).await.unwrap();
        assert_eq!(report.pages.len(), 2);
        task.await.unwrap();
    }
    #[tokio::test]
    async fn repeated_or_missing_pagination_cursors_fail_instead_of_hanging_or_truncating() {
        for second in [Some("same"), None] {
            let mut last = list(vec![], second);
            last["has_more"] = json!(true);
            let (client, task) = mock(vec![
                get(
                    format!("pages/{ROOT}"),
                    page_value(ROOT, json!({"type":"workspace","workspace":true})),
                ),
                get(
                    format!("blocks/{ROOT}/children?page_size=100"),
                    list(vec![], Some("same")),
                ),
                get(
                    format!("blocks/{ROOT}/children?page_size=100&start_cursor=same"),
                    last,
                ),
            ])
            .await;
            assert_eq!(
                client.crawl_roots(&roots()).await.unwrap_err().kind,
                BackendErrorKind::Internal
            );
            task.await.unwrap();
        }
    }
    #[tokio::test]
    async fn unreadable_descendants_are_reported_but_unreadable_roots_fail() {
        for status in [403, 404] {
            let (client, task) = mock(vec![
                get(
                    format!("pages/{ROOT}"),
                    page_value(ROOT, json!({"type":"workspace","workspace":true})),
                ),
                children(ROOT, vec![block(CHILD, ROOT, "child_page", false)]),
                (format!("GET /v1/pages/{CHILD}"), status, json!({})),
            ])
            .await;
            let report = client.crawl_roots(&roots()).await.unwrap();
            assert_eq!(report.pages.len(), 1);
            assert_eq!(report.skipped[0].reason, SkipReason::Inaccessible);
            task.await.unwrap();
            let (client, task) =
                mock(vec![(format!("GET /v1/pages/{ROOT}"), status, json!({}))]).await;
            assert!(client.crawl_roots(&roots()).await.is_err());
            task.await.unwrap();
        }
    }
    #[tokio::test]
    async fn wiki_sources_require_actual_ancestry_to_a_configured_root() {
        let nested = "00000000-0000-0000-0000-000000000009";
        let steps = vec![
            get(
                format!("pages/{ROOT}"),
                page_value(ROOT, json!({"type":"workspace","workspace":true})),
            ),
            children(ROOT, vec![block(DB, ROOT, "child_database", false)]),
            get(format!("databases/{DB}"), database(DB, ROOT, vec![SOURCE])),
            get(format!("data_sources/{SOURCE}"), source(SOURCE, DB)),
            (
                format!("POST /v1/data_sources/{SOURCE}/query"),
                200,
                list(vec![source(nested, CHILD), source(OUT, ROW)], None),
            ),
            get(format!("data_sources/{nested}"), source(nested, CHILD)),
            get(
                format!("databases/{CHILD}"),
                database(CHILD, ROOT, vec![nested]),
            ),
            get(format!("data_sources/{OUT}"), source(OUT, ROW)),
            get(
                format!("databases/{ROW}"),
                database(ROW, "00000000-0000-0000-0000-000000000010", vec![OUT]),
            ),
            get(
                "pages/00000000-0000-0000-0000-000000000010".into(),
                page_value(
                    "00000000-0000-0000-0000-000000000010",
                    json!({"type":"workspace","workspace":true}),
                ),
            ),
            get(format!("data_sources/{nested}"), source(nested, CHILD)),
            (
                format!("POST /v1/data_sources/{nested}/query"),
                200,
                list(
                    vec![page_value(BLOCK, parent_value("data_source_id", nested))],
                    None,
                ),
            ),
            get(
                format!("pages/{BLOCK}"),
                page_value(BLOCK, parent_value("data_source_id", nested)),
            ),
            children(BLOCK, vec![]),
        ];
        let (client, task) = mock(steps).await;
        let report = client.crawl_roots(&roots()).await.unwrap();
        assert_eq!(
            report
                .pages
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec![ROOT, BLOCK]
        );
        assert_eq!(report.skipped[0].id, OUT);
        assert_eq!(report.skipped[0].reason, SkipReason::OutsideScope);
        task.await.unwrap();
    }
    #[tokio::test]
    async fn distinct_roots_and_original_synced_content_are_discovered_while_archived_pages_are_skipped()
     {
        let mut synced = block(BLOCK, ROOT, "synced_block", true);
        synced["synced_block"] = json!({"synced_from":null});
        let mut old = page_value(CHILD, parent_value("page_id", ROOT));
        old["archived"] = json!(true);
        let (client, task) = mock(vec![
            get(
                format!("pages/{ROOT}"),
                page_value(ROOT, json!({"type":"workspace","workspace":true})),
            ),
            children(
                ROOT,
                vec![synced.clone(), block(CHILD, ROOT, "child_page", false)],
            ),
            get(
                format!("pages/{OUT}"),
                page_value(OUT, json!({"type":"workspace","workspace":true})),
            ),
            children(OUT, vec![]),
            get(format!("blocks/{BLOCK}"), synced.clone()),
            children(BLOCK, vec![block(ROW, BLOCK, "child_page", false)]),
            get(format!("pages/{CHILD}"), old),
            get(
                format!("pages/{ROW}"),
                page_value(ROW, parent_value("block_id", BLOCK)),
            ),
            children(ROW, vec![]),
        ])
        .await;
        let report = client
            .crawl_roots(&[PageId(OUT.into()), PageId(ROOT.into())])
            .await
            .unwrap();
        assert_eq!(report.roots, vec![ROOT, OUT]);
        assert_eq!(
            report
                .pages
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec![ROOT, ROW, OUT]
        );
        assert_eq!(
            report.skipped,
            vec![SkippedContent {
                id: CHILD.into(),
                reason: SkipReason::Archived
            }]
        );
        task.await.unwrap();
    }
    #[tokio::test]
    async fn containers_moved_outside_scope_are_not_explored() {
        let (client, task) = mock(vec![
            get(
                format!("pages/{ROOT}"),
                page_value(ROOT, json!({"type":"workspace","workspace":true})),
            ),
            children(ROOT, vec![block(BLOCK, ROOT, "toggle", true)]),
            get(format!("blocks/{BLOCK}"), block(BLOCK, OUT, "toggle", true)),
        ])
        .await;
        let report = client.crawl_roots(&roots()).await.unwrap();
        assert_eq!(report.pages.len(), 1);
        assert_eq!(
            report.skipped,
            vec![SkippedContent {
                id: BLOCK.into(),
                reason: SkipReason::OutsideScope
            }]
        );
        task.await.unwrap();
    }
    #[tokio::test]
    async fn empty_or_invalid_scope_is_rejected_without_upstream_requests() {
        let client = NotionClient::integration("test-credential").unwrap();
        for roots in [vec![], vec![PageId("invalid".into())]] {
            assert_eq!(
                client.crawl_roots(&roots).await.unwrap_err().kind,
                BackendErrorKind::InvalidInput
            );
        }
    }
}
