//! Dev-only scripted Notion HTTP fake shared by the retrieval and server
//! integration tests. It serves a small fixed tree under [`ROOT`]: the root's
//! Markdown tags its child pages (and an optional database), the way the real
//! enhanced-Markdown endpoint does, so the production walk discovers them.
//! Tests script scenarios by mutating [`Model`] through [`FakeNotion::change`].
use notion_knowledge_notion::NotionClient;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PAGE: &str = "11111111-1111-1111-1111-111111111111";
pub const OTHER: &str = "55555555-5555-5555-5555-555555555555";
pub const ROOT: &str = "22222222-2222-2222-2222-222222222222";
pub const FOREIGN: &str = "33333333-3333-3333-3333-333333333333";
pub const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
/// The database tagged in the root's Markdown when [`Model::rows`] is not empty.
pub const DATABASE: &str = "77777777-7777-7777-7777-777777777777";
pub const DATA_SOURCE: &str = "88888888-8888-8888-8888-888888888888";

#[derive(Clone)]
pub struct ExtraPage {
    pub title: String,
    /// Whether the root's Markdown tags this page as a child.
    pub visible: bool,
    pub archived: bool,
}

#[derive(Clone)]
pub struct Model {
    pub markdown: String,
    pub rotate_media: bool,
    pub empty_content: bool,
    pub unsupported_page_content: bool,
    /// The next this-many reads of [`PAGE`]'s Markdown are truncated.
    pub unsupported_page_reads: usize,
    pub unauthenticated_content: bool,
    pub unauthenticated_page_metadata: bool,
    pub title: String,
    pub revision: String,
    pub parent: String,
    pub archived: bool,
    pub root_revision: String,
    /// Leave [`PAGE`] out of the root's child tags while it stays fetchable.
    pub hide_page: bool,
    /// A synced-block reference to [`FOREIGN`] in the root's Markdown.
    pub copied_reference: bool,
    pub inaccessible_other: bool,
    pub inaccessible_foreign: bool,
    pub foreign_parent: String,
    pub inaccessible_root: bool,
    pub inaccessible_page: bool,
    pub race_metadata: bool,
    pub race_metadata_once: bool,
    pub race_move: bool,
    pub level: u64,
    pub relation_name: Option<String>,
    pub relation_property_id: String,
    pub relation_targets: Vec<String>,
    /// Adds the alias multi-select and an unselected rich-text property.
    pub alias_properties: bool,
    pub alias_names: Vec<String>,
    pub extra: BTreeMap<String, ExtraPage>,
    /// Row page ids of [`DATA_SOURCE`]; empty means no database is tagged.
    pub rows: Vec<String>,
    /// The data-source query keeps returning the same cursor.
    pub repeat_query_cursor: bool,
}
impl Default for Model {
    fn default() -> Self {
        Self {
            markdown: "# Intro\n\nSaffron ravioli.\n\n## Other\n\nAn evergreen section.\n".into(),
            rotate_media: false,
            empty_content: false,
            unsupported_page_content: false,
            unsupported_page_reads: 0,
            unauthenticated_content: false,
            unauthenticated_page_metadata: false,
            title: "First".into(),
            revision: "2026-10-10T12:00:00Z".into(),
            parent: ROOT.into(),
            archived: false,
            root_revision: "2026-10-10T11:00:00Z".into(),
            hide_page: false,
            copied_reference: false,
            inaccessible_other: false,
            inaccessible_foreign: false,
            foreign_parent: "workspace".into(),
            inaccessible_root: false,
            inaccessible_page: false,
            race_metadata: false,
            race_metadata_once: false,
            race_move: false,
            level: 3,
            relation_name: None,
            relation_property_id: "related-stable".into(),
            relation_targets: vec![],
            alias_properties: false,
            alias_names: vec![],
            extra: Default::default(),
            rows: vec![],
            repeat_query_cursor: false,
        }
    }
}

/// Holds one matching request until the test releases it.
#[derive(Default)]
pub struct RequestPause {
    /// When set, pause only while another process holds this directory lock.
    pub owned_directory: Mutex<Option<PathBuf>>,
    pub armed: AtomicBool,
    pub started: tokio::sync::Notify,
    pub proceed: tokio::sync::Notify,
}
impl RequestPause {
    fn ready(&self) -> bool {
        if !self.armed.load(Ordering::SeqCst) {
            return false;
        }
        if let Some(directory) = self.owned_directory.lock().unwrap().as_ref() {
            let guard = fs::File::open(directory).unwrap();
            match guard.try_lock() {
                Ok(()) => return false,
                Err(fs::TryLockError::WouldBlock) => {}
                Err(error) => panic!("fixture cannot observe directory guard: {error:?}"),
            }
        }
        self.armed.swap(false, Ordering::SeqCst)
    }
}

pub struct FakeNotion {
    pub state: Arc<Mutex<Model>>,
    /// Every request, counted.
    pub attempts: Arc<AtomicUsize>,
    /// Markdown reads of any page.
    pub body_reads: Arc<AtomicUsize>,
    pub target_body_reads: Arc<AtomicUsize>,
    pub foreign_body_reads: Arc<AtomicUsize>,
    /// Distinct Markdown routes read by indexing preparation (not by a walk).
    pub markdown_pages: Arc<Mutex<BTreeSet<String>>>,
    markdown_reads: Arc<Mutex<BTreeMap<String, usize>>>,
    /// The next response is a 503.
    pub fail_once: Arc<AtomicBool>,
    /// Pauses the next `pages/{PAGE}` metadata read.
    pub pause: Arc<RequestPause>,
    /// Pauses the next response to [`OTHER`]'s Markdown. The first such read is
    /// the walk's, so the walk stays open while other reached pages proceed.
    pub discovery_pause: Arc<RequestPause>,
    requests: Arc<Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
    pub port: u16,
}

fn tag(id: &str, title: &str) -> String {
    format!(
        "<page url=\"https://www.notion.so/{}\">{title}</page>\n",
        id.replace('-', "")
    )
}

pub fn page_metadata(id: &str, parent: &str, title: &str, revision: &str, archived: bool) -> Value {
    json!({
        "object": "page",
        "id": id,
        "url": format!("https://www.notion.so/{id}"),
        "last_edited_time": revision,
        "in_trash": archived,
        "parent": if parent == "workspace" {
            json!({"type":"workspace","workspace":true})
        } else if parent == DATA_SOURCE {
            json!({"type":"data_source_id","data_source_id":parent,"database_id":DATABASE})
        } else {
            json!({"type":"page_id","page_id":parent})
        },
        "properties": {
            "Title": {
                "id":"title", "type":"title",
                "title":[{"plain_text":title}]
            },
            "level": {
                "id":"level", "type":"number", "number":3
            }
        }
    })
}

fn markdown(id: &str, body: impl Into<Value>, truncated: bool) -> Value {
    json!({"object":"page_markdown","id":id,"markdown":body.into(),
        "truncated":truncated,"unknown_block_ids":[]})
}

/// The root's child tags. Real Notion bumps a parent's `last_edited_time`
/// when its child blocks change, so the root revision follows this list.
fn root_children(snapshot: &Model) -> String {
    let mut children = tag(OTHER, "Separate");
    if snapshot.copied_reference {
        children.push_str(&format!(
            "<synced_block_reference url=\"https://www.notion.so/{}\"></synced_block_reference>\n",
            FOREIGN.replace('-', "")
        ));
    }
    for (id, page) in &snapshot.extra {
        if page.visible {
            children.push_str(&tag(id, &page.title));
        }
    }
    if !snapshot.hide_page {
        children.push_str(&tag(PAGE, "First"));
    }
    if !snapshot.rows.is_empty() {
        children.push_str(&format!(
            "<database url=\"https://www.notion.so/{}\">Table</database>\n",
            DATABASE.replace('-', "")
        ));
    }
    children
}

fn root_revision(snapshot: &Model) -> String {
    let children = root_children(snapshot);
    if children == root_children(&Model::default()) {
        return snapshot.root_revision.clone();
    }
    let seconds = children
        .bytes()
        .fold(7u64, |h, b| h.wrapping_mul(31).wrapping_add(b as u64))
        % 3600;
    format!("2026-10-10T10:{:02}:{:02}Z", seconds / 60, seconds % 60)
}

fn respond(route: &str, snapshot: Model, bodies: usize) -> Value {
    let not_found = json!({"object":"error", "code":"object_not_found"});
    if (snapshot.unauthenticated_content && route == format!("/v1/pages/{PAGE}/markdown"))
        || (snapshot.unauthenticated_page_metadata && route == format!("/v1/pages/{PAGE}"))
    {
        return json!({"object":"error", "code":"unauthorized"});
    }
    if (snapshot.inaccessible_root && route == format!("/v1/pages/{ROOT}"))
        || (snapshot.inaccessible_other && route == format!("/v1/pages/{OTHER}"))
        || (snapshot.inaccessible_foreign && route == format!("/v1/pages/{FOREIGN}"))
        || (snapshot.inaccessible_page && route == format!("/v1/pages/{PAGE}"))
    {
        return not_found;
    }
    let row = snapshot
        .rows
        .iter()
        .find(|id| route.starts_with(&format!("/v1/pages/{id}")));
    if route == format!("/v1/pages/{ROOT}/markdown") {
        markdown(
            ROOT,
            format!("# Root\n\nRoot overview.\n{}", root_children(&snapshot)),
            false,
        )
    } else if route == format!("/v1/pages/{PAGE}/markdown") {
        markdown(
            PAGE,
            if snapshot.rotate_media {
                snapshot
                    .markdown
                    .replace("AUTH_GENERATION", &bodies.to_string())
            } else {
                snapshot.markdown
            },
            snapshot.unsupported_page_content,
        )
    } else if route == format!("/v1/pages/{PAGE}") {
        let mut page = page_metadata(
            PAGE,
            &snapshot.parent,
            &snapshot.title,
            &snapshot.revision,
            snapshot.archived,
        );
        page["properties"]["level"]["number"] = json!(snapshot.level);
        if snapshot.alias_properties {
            page["properties"]["Alternate names"] = json!({"id":"names","type":"multi_select","multi_select":snapshot.alias_names.iter().map(|name|json!({"id":"choice","name":name})).collect::<Vec<_>>()});
            page["properties"]["Unselected"] = json!({"id":"not-selected","type":"rich_text","rich_text":[{"type":"text","plain_text":"Never an alias","text":{"content":"Never an alias"}}]});
        }
        if let Some(name) = &snapshot.relation_name {
            page["properties"][name] = json!({"id":snapshot.relation_property_id, "type":"relation", "relation": snapshot.relation_targets.iter().map(|id|json!({"id":id})).collect::<Vec<_>>(),"has_more":false});
        }
        page
    } else if route == format!("/v1/pages/{OTHER}/markdown") {
        markdown(OTHER, "# Separate\n\nUntouched cinnamon.\n", false)
    } else if route == format!("/v1/pages/{OTHER}") {
        page_metadata(OTHER, ROOT, "Separate", "2026-10-10T11:00:00Z", false)
    } else if let Some((id, page)) = snapshot
        .extra
        .iter()
        .find(|(id, _)| route == format!("/v1/pages/{id}"))
    {
        page_metadata(id, ROOT, &page.title, "2026-10-10T12:00:00Z", page.archived)
    } else if let Some((id, page)) = snapshot
        .extra
        .iter()
        .find(|(id, _)| route == format!("/v1/pages/{id}/markdown"))
    {
        markdown(
            id,
            format!("# {}\n\nExtra source contents.\n", page.title),
            false,
        )
    } else if let Some(id) = row {
        if route.ends_with("/markdown") {
            markdown(id, "# Row\n\nRow contents.\n", false)
        } else {
            page_metadata(id, DATA_SOURCE, "Row", "2026-10-10T12:00:00Z", false)
        }
    } else if route == format!("/v1/databases/{DATABASE}") && !snapshot.rows.is_empty() {
        json!({"object":"database","id":DATABASE,"in_trash":false,
            "last_edited_time":"2026-10-10T11:00:00Z",
            "parent":{"type":"page_id","page_id":ROOT},"data_sources":[{"id":DATA_SOURCE}]})
    } else if route == format!("/v1/data_sources/{DATA_SOURCE}") && !snapshot.rows.is_empty() {
        json!({"object":"data_source","id":DATA_SOURCE,"in_trash":false,
            "last_edited_time":"2026-10-10T11:00:00Z",
            "parent":{"type":"database_id","database_id":DATABASE}})
    } else if route == format!("/v1/data_sources/{DATA_SOURCE}/query") && !snapshot.rows.is_empty()
    {
        let rows: Vec<_> = snapshot
            .rows
            .iter()
            .map(|id| page_metadata(id, DATA_SOURCE, "Row", "2026-10-10T12:00:00Z", false))
            .collect();
        if snapshot.repeat_query_cursor {
            json!({"object":"list","results":rows,"has_more":true,"next_cursor":"same-cursor"})
        } else {
            json!({"object":"list","results":rows,"has_more":false,"next_cursor":null})
        }
    } else if route == format!("/v1/pages/{ROOT}") {
        page_metadata(ROOT, "workspace", "Root", &root_revision(&snapshot), false)
    } else if route == format!("/v1/pages/{FOREIGN}") {
        page_metadata(
            FOREIGN,
            &snapshot.foreign_parent,
            "Foreign",
            "2026-10-10T11:00:00Z",
            false,
        )
    } else {
        not_found
    }
}

impl FakeNotion {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(Model::default()));
        let attempts = Arc::new(AtomicUsize::new(0));
        let body_reads = Arc::new(AtomicUsize::new(0));
        let target_body_reads = Arc::new(AtomicUsize::new(0));
        let foreign_body_reads = Arc::new(AtomicUsize::new(0));
        let markdown_pages = Arc::new(Mutex::new(BTreeSet::new()));
        let markdown_counts = Arc::new(Mutex::new(BTreeMap::new()));
        let read_counts = markdown_counts.clone();
        let fail_once = Arc::new(AtomicBool::new(false));
        let pause = Arc::new(RequestPause::default());
        let discovery_pause = Arc::new(RequestPause::default());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (values, counts, bodies, target_bodies, foreign_bodies, markdown_reads, fail) = (
            state.clone(),
            attempts.clone(),
            body_reads.clone(),
            target_body_reads.clone(),
            foreign_body_reads.clone(),
            markdown_pages.clone(),
            fail_once.clone(),
        );
        let (boundary, discovery, log) = (pause.clone(), discovery_pause.clone(), requests.clone());
        let server = tokio::spawn(async move {
            let mut pending_responses = tokio::task::JoinSet::new();
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut part = [0u8; 2048];
                    let size = stream.read(&mut part).await.unwrap();
                    if size == 0 {
                        break None;
                    }
                    bytes.extend_from_slice(&part[..size]);
                    if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        break Some(end + 4);
                    }
                };
                let Some(header_end) = header_end else {
                    continue;
                };
                let req = String::from_utf8_lossy(&bytes[..header_end]).into_owned();
                // Drain a JSON body so closing never resets the connection.
                let length = req
                    .lines()
                    .find_map(|l| {
                        l.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                    })
                    .unwrap_or(0);
                while bytes.len() < header_end + length {
                    let mut part = [0u8; 2048];
                    let size = stream.read(&mut part).await.unwrap();
                    if size == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&part[..size]);
                }
                assert!(req.contains("authorization: Bearer fixture-credential"));
                assert!(req.contains("notion-version: 2026-03-11"));
                let route = req
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let post = req.starts_with("POST ");
                assert!(
                    req.starts_with("GET /v1/")
                        || (post
                            && route.starts_with("/v1/data_sources/")
                            && route.ends_with("/query"))
                );
                counts.fetch_add(1, Ordering::SeqCst);
                log.lock().unwrap().push(route.clone());
                if route == format!("/v1/pages/{PAGE}") && boundary.ready() {
                    boundary.started.notify_one();
                    boundary.proceed.notified().await;
                }
                if route == format!("/v1/pages/{FOREIGN}/markdown") {
                    foreign_bodies.fetch_add(1, Ordering::SeqCst);
                }
                if route == format!("/v1/pages/{OTHER}/markdown") {
                    target_bodies.fetch_add(1, Ordering::SeqCst);
                }
                // The walk reads a page's metadata once (rows: never) before
                // its Markdown; preparation first checks lifecycle, re-reading
                // the page's metadata at least twice. That tells them apart.
                if let Some(page) = route.strip_prefix("/v1/pages/") {
                    let mut counts = read_counts.lock().unwrap();
                    if let Some(page) = page.strip_suffix("/markdown") {
                        bodies.fetch_add(1, Ordering::SeqCst);
                        if counts.insert(page.to_owned(), 0).unwrap_or(0) >= 2 {
                            markdown_reads.lock().unwrap().insert(route.clone());
                        }
                    } else if !page.contains('/') {
                        *counts.entry(page.to_owned()).or_insert(0) += 1;
                    }
                }
                let snapshot = {
                    let mut state = values.lock().unwrap();
                    let mut snapshot = state.clone();
                    if route == format!("/v1/pages/{PAGE}/markdown")
                        && state.unsupported_page_reads > 0
                    {
                        state.unsupported_page_reads -= 1;
                        snapshot.unsupported_page_content = true;
                    }
                    if route == format!("/v1/pages/{PAGE}") {
                        if state.race_metadata {
                            state.title.push('!');
                        }
                        if state.race_metadata_once {
                            state.race_metadata_once = false;
                            state.title.push('!');
                        }
                        if state.race_move {
                            state.parent = if snapshot.parent == ROOT {
                                FOREIGN.into()
                            } else {
                                ROOT.into()
                            };
                        }
                    }
                    snapshot
                };
                let empty = snapshot.empty_content;
                let mut response = respond(&route, snapshot, bodies.load(Ordering::SeqCst));
                if empty && response["object"] == "page_markdown" {
                    response["markdown"] = json!("");
                }
                let bad = fail.swap(false, Ordering::SeqCst);
                let status = if bad {
                    503
                } else if response["code"] == "unauthorized" {
                    401
                } else if response.get("code").is_some() {
                    404
                } else {
                    200
                };
                let body = if bad { json!({}) } else { response }.to_string();
                let payload = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                if route == format!("/v1/pages/{OTHER}/markdown")
                    && discovery.armed.swap(false, Ordering::SeqCst)
                {
                    let barrier = discovery.clone();
                    pending_responses.spawn(async move {
                        barrier.started.notify_one();
                        barrier.proceed.notified().await;
                        let _ = stream.write_all(payload.as_bytes()).await;
                    });
                } else {
                    let _ = stream.write_all(payload.as_bytes()).await;
                }
            }
        });
        Self {
            state,
            attempts,
            body_reads,
            target_body_reads,
            foreign_body_reads,
            markdown_pages,
            markdown_reads: markdown_counts,
            fail_once,
            pause,
            discovery_pause,
            requests,
            server,
            port,
        }
    }
    pub fn client(&self) -> Arc<NotionClient> {
        Arc::new(
            NotionClient::integration("fixture-credential")
                .unwrap()
                .with_loopback_fixture(self.port),
        )
    }
    pub fn change(&self, change: impl FnOnce(&mut Model)) {
        change(&mut self.state.lock().unwrap());
    }
    /// Forget prepared pages and read counts, so the next walk counts afresh.
    pub fn reset_markdown_pages(&self) {
        self.markdown_reads.lock().unwrap().clear();
        self.markdown_pages.lock().unwrap().clear();
    }
    /// Request routes in arrival order.
    pub fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for FakeNotion {
    fn drop(&mut self) {
        self.server.abort();
    }
}
