//! Server-only, scope-constrained Notion page create with canonical readback.
//! A successful POST receipt is never proof of a visible authorized page.
use crate::{NotionClient, pages, writes::MARKDOWN_VERSION};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    idempotency::{
        MutationClaim, MutationOperation, Reservation, VerifiedReceipt, WriteIdempotencyStore,
    },
    lifecycle::LifecycleScope,
    root_scope::{RootScopeGate, ScopeError},
    write_refresh::{MutationRefreshQueue, RefreshKey, WriteCompletion, admit_verified},
};
use reqwest::header::AUTHORIZATION;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

const CREATE: &str = "notion.page.scoped_create";
const VERIFY: &str = "notion.page.scoped_create.verify";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedPageCreated {
    pub page_id: PageId,
    pub parent_page_id: PageId,
    pub url: String,
    pub verified: bool,
    pub last_edited_time: String,
}

#[derive(Debug, Clone)]
pub struct ScopedPageCreateInput {
    pub parent_page_id: PageId,
    pub title: String,
    pub markdown: String,
    /// Optional Notion emoji icon: never accepts arbitrary properties JSON.
    pub icon_emoji: Option<String>,
}

fn err(op: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation: op,
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}
fn scope_error(e: ScopeError) -> BackendError {
    err(
        CREATE,
        match e {
            ScopeError::Denied => BackendErrorKind::PermissionDenied,
            ScopeError::Changed => BackendErrorKind::Conflict,
            ScopeError::Unverifiable => BackendErrorKind::Unavailable,
            ScopeError::InvalidConfiguration => BackendErrorKind::InvalidInput,
        },
    )
}
fn safe_text(s: &str) -> bool {
    !s.chars().any(|ch| {
        ch.is_control()
            || matches!(
                ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'
            )
    })
}
fn valid(req: &ScopedPageCreateInput) -> bool {
    pages::uuid(&req.parent_page_id.0).as_deref()==Some(req.parent_page_id.0.as_str())
        && !req.title.trim().is_empty()
        && req.title.chars().count()<=200
        && safe_text(&req.title)
        && req.markdown.len()<=250*1024
        && req.markdown.chars().all(|ch| (!ch.is_control() || matches!(ch, '\r' | '\n' | '\t')) && !matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}'))
        && req.icon_emoji.as_deref().is_none_or(|s|
            !s.is_empty() && s.chars().count()<=8 && safe_text(s) && !s.chars().any(char::is_whitespace))
}
fn created_id(receipt: &Value) -> Option<(PageId, String)> {
    if receipt["object"] != "page" || receipt["in_trash"] == true || receipt["archived"] == true {
        return None;
    }
    let id = pages::uuid(receipt["id"].as_str()?)?;
    let url = receipt["url"].as_str()?;
    if !url.starts_with("https://") || url.len() > 4096 {
        return None;
    }
    Some((PageId(id), url.into()))
}
fn verified_page(value: &Value, id: &PageId, parent: &PageId, url: &str, title: &str) -> bool {
    if created_id(value).is_none_or(|(got, read_url)| got != *id || read_url != url) {
        return false;
    }
    if value["parent"]["type"] != "page_id"
        || value["parent"]["page_id"]
            .as_str()
            .and_then(pages::uuid)
            .as_deref()
            != Some(parent.0.as_str())
    {
        return false;
    }
    value["properties"]["title"]["title"]
        .as_array()
        .is_some_and(|items| items.len() == 1 && items[0]["plain_text"].as_str() == Some(title))
}

impl NotionClient {
    /// Durable once-only creation plus independent local refresh admission.
    /// A crash between verified ledger commit and queue admission is repaired
    /// by replaying the same key; replay never issues another creation POST.
    pub async fn create_scoped_page_once_and_refresh(
        &self,
        request: &ScopedPageCreateInput,
        scope: &LifecycleScope,
        ledger: &dyn WriteIdempotencyStore,
        queue: &dyn MutationRefreshQueue,
        idempotency_key: &str,
    ) -> Result<WriteCompletion, BackendError> {
        if !valid(request) {
            return Err(err(CREATE, BackendErrorKind::InvalidInput));
        }
        let gate =
            RootScopeGate::new(Arc::new(self.clone()), scope.clone()).map_err(scope_error)?;
        gate.authorize(&request.parent_page_id)
            .await
            .map_err(scope_error)?;
        let payload = serde_json::to_vec(
            &json!({"title":request.title,"markdown":request.markdown,"icon":request.icon_emoji}),
        )
        .map_err(|_| err(CREATE, BackendErrorKind::InvalidInput))?;
        let claim = MutationClaim::new(
            &scope.workspace_id,
            idempotency_key,
            MutationOperation::CreatePage,
            &request.parent_page_id.0,
            &MutationClaim::payload_sha256(&payload),
        )
        .map_err(|_| err(CREATE, BackendErrorKind::InvalidInput))?;
        let (receipt, replayed) = match ledger
            .begin(&claim)
            .map_err(|_| err(CREATE, BackendErrorKind::Conflict))?
        {
            Reservation::Replay(receipt) => {
                let post = gate
                    .authorize(&PageId(receipt.page_id.clone()))
                    .await
                    .map_err(scope_error)?;
                gate.revalidate(&post, scope).await.map_err(scope_error)?;
                (receipt, true)
            }
            Reservation::Reconcile => return Err(err(CREATE, BackendErrorKind::Conflict)),
            Reservation::ExecuteOnce => {
                ledger
                    .mark_uncertain(&claim)
                    .map_err(|_| err(CREATE, BackendErrorKind::Unavailable))?;
                let created = self.create_scoped_page(request, scope).await?;
                let receipt = VerifiedReceipt::from_readback(
                    created.page_id.0,
                    created.url,
                    created.last_edited_time,
                )
                .map_err(|_| err(VERIFY, BackendErrorKind::Conflict))?;
                ledger
                    .record_verified(&claim, &receipt)
                    .map_err(|_| err(VERIFY, BackendErrorKind::Unavailable))?;
                (receipt, false)
            }
        };
        Ok(admit_verified(
            queue,
            &RefreshKey {
                workspace_id: scope.workspace_id.clone(),
                mutation_digest: claim.key_digest().to_owned(),
                page_id: receipt.page_id.clone(),
            },
            receipt,
            replayed,
        ))
    }
    /// Only trusted server-owned `scope` may authorize the explicit parent.
    /// The physical parent ancestry is fetched and revalidated before POST.
    /// No uncertain POST is replayed, even if readback never materializes.
    pub async fn create_scoped_page(
        &self,
        request: &ScopedPageCreateInput,
        scope: &LifecycleScope,
    ) -> Result<ScopedPageCreated, BackendError> {
        if !valid(request) {
            return Err(err(CREATE, BackendErrorKind::InvalidInput));
        }
        let gate =
            RootScopeGate::new(Arc::new(self.clone()), scope.clone()).map_err(scope_error)?;
        let permit = gate
            .authorize(&request.parent_page_id)
            .await
            .map_err(scope_error)?;
        gate.revalidate(&permit, scope).await.map_err(scope_error)?;
        let mut body = json!({
            "parent":{"type":"page_id","page_id":request.parent_page_id.0},
            "properties":{"title":{"type":"title","title":[{"type":"text","text":{"content":request.title}}]}},
            "markdown":request.markdown
        });
        if let Some(emoji) = &request.icon_emoji {
            body["icon"] = json!({"type":"emoji","emoji":emoji});
        }
        let receipt = self
            .write(reqwest::Method::POST, "/pages", body, CREATE)
            .await?;
        let (id, url) =
            created_id(&receipt).ok_or_else(|| err(CREATE, BackendErrorKind::Conflict))?;
        // Strongly check the receipt's parent when available, then read the
        // authoritative created page. The receipt cannot stand in for a GET.
        if receipt["parent"]["type"] != "page_id"
            || receipt["parent"]["page_id"]
                .as_str()
                .and_then(pages::uuid)
                .as_deref()
                != Some(request.parent_page_id.0.as_str())
        {
            return Err(err(CREATE, BackendErrorKind::Conflict));
        }
        for attempt in 0..3 {
            let http = self
                .http
                .get(format!("{}/pages/{}", self.api_root, id.0))
                .header(AUTHORIZATION, self.authorization.clone())
                .header("Notion-Version", MARKDOWN_VERSION);
            let mut response = self.send(http, true, VERIFY).await?;
            match response.status().as_u16() {
                404 => {}
                200 => {
                    let mut data = Vec::new();
                    while let Some(chunk) = response
                        .chunk()
                        .await
                        .map_err(|_| err(VERIFY, BackendErrorKind::Unavailable))?
                    {
                        if data.len().saturating_add(chunk.len()) > 64 * 1024 {
                            return Err(err(VERIFY, BackendErrorKind::Internal));
                        }
                        data.extend_from_slice(&chunk);
                    }
                    let page: Value = serde_json::from_slice(&data)
                        .map_err(|_| err(VERIFY, BackendErrorKind::Internal))?;
                    if verified_page(&page, &id, &request.parent_page_id, &url, &request.title) {
                        if request.icon_emoji.as_deref().is_some_and(|emoji| {
                            page["icon"]["type"] != "emoji"
                                || page["icon"]["emoji"].as_str() != Some(emoji)
                        }) {
                            return Err(err(VERIFY, BackendErrorKind::Conflict));
                        }
                        let content = self.read_content(&id.0).await?;
                        if content.page.id != id
                            || content.page.archived
                            || content.page.url != url
                            || content.page.title != request.title
                            || content.markdown != request.markdown
                        {
                            return Err(err(VERIFY, BackendErrorKind::Conflict));
                        }
                        // The mutation changed the new page's revision. Fresh
                        // physical authorization, not the parent's pre-write permit.
                        let post = gate.authorize(&id).await.map_err(scope_error)?;
                        gate.revalidate(&post, scope).await.map_err(scope_error)?;
                        return Ok(ScopedPageCreated {
                            page_id: id,
                            parent_page_id: request.parent_page_id.clone(),
                            url,
                            verified: true,
                            last_edited_time: content.page.last_edited_time,
                        });
                    }
                }
                401 => return Err(err(VERIFY, BackendErrorKind::Unauthenticated)),
                403 => return Err(err(VERIFY, BackendErrorKind::PermissionDenied)),
                429 => return Err(err(VERIFY, BackendErrorKind::RateLimited)),
                500..=599 => return Err(err(VERIFY, BackendErrorKind::Unavailable)),
                _ => return Err(err(VERIFY, BackendErrorKind::Conflict)),
            }
            if attempt < 2 {
                self.transport
                    .provider_delay(Duration::from_millis(150 * (attempt + 1)))
                    .await;
            }
        }
        Err(err(VERIFY, BackendErrorKind::Conflict))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PARENT: &str = "11111111-1111-1111-1111-111111111111";
    const CHILD: &str = "22222222-2222-2222-2222-222222222222";
    fn input() -> ScopedPageCreateInput {
        ScopedPageCreateInput {
            parent_page_id: PageId(PARENT.into()),
            title: "A new page".into(),
            markdown: "Body".into(),
            icon_emoji: Some("📄".into()),
        }
    }
    fn receipt() -> Value {
        json!({"object":"page","id":CHILD,"url":format!("https://www.notion.so/{CHILD}"),
            "parent":{"type":"page_id","page_id":PARENT},"in_trash":false})
    }
    fn complete_read() -> Value {
        let mut page = receipt();
        page["last_edited_time"] = json!("2026-10-11T10:00:00Z");
        page["icon"] = json!({"type":"emoji","emoji":"📄"});
        page["properties"] =
            json!({"title":{"id":"title","type":"title","title":[{"plain_text":"A new page"}]}});
        page
    }
    fn child_chain() -> Vec<(String, u16, Value)> {
        const ROOT: &str = "33333333-3333-3333-3333-333333333333";
        vec![
            (
                format!("GET /v1/pages/{CHILD}"),
                200,
                node(CHILD, Some(PARENT)),
            ),
            (
                format!("GET /v1/pages/{PARENT}"),
                200,
                node(PARENT, Some(ROOT)),
            ),
            (format!("GET /v1/pages/{ROOT}"), 200, node(ROOT, None)),
            (format!("GET /v1/pages/{ROOT}"), 200, node(ROOT, None)),
            (
                format!("GET /v1/pages/{PARENT}"),
                200,
                node(PARENT, Some(ROOT)),
            ),
            (
                format!("GET /v1/pages/{CHILD}"),
                200,
                node(CHILD, Some(PARENT)),
            ),
        ]
    }
    #[test]
    fn input_rejects_bad_parent_content_and_icons() {
        let mut q = input();
        assert!(valid(&q));
        q.parent_page_id = PageId("../escape".into());
        assert!(!valid(&q));
        q = input();
        q.title = "".into();
        assert!(!valid(&q));
        q = input();
        q.icon_emoji = Some("secret\n".into());
        assert!(!valid(&q));
        q = input();
        q.markdown = "unsafe\u{202e}suffix".into();
        assert!(!valid(&q));
        q = input();
        q.markdown = "Title\n\nBody".into();
        assert!(valid(&q));
        q = input();
        q.markdown = "X".repeat(250 * 1024 + 1);
        assert!(!valid(&q));
    }

    #[test]
    fn readback_rejects_wrong_parent_title_url_and_deleted_status() {
        let mut p = receipt();
        p["properties"] = json!({"title":{"title":[{"plain_text":"A new page"}]}});
        assert!(verified_page(
            &p,
            &PageId(CHILD.into()),
            &PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),
            "A new page"
        ));
        p["parent"]["page_id"] = json!(CHILD);
        assert!(!verified_page(
            &p,
            &PageId(CHILD.into()),
            &PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),
            "A new page"
        ));
        p["parent"]["page_id"] = json!(PARENT);
        p["in_trash"] = json!(true);
        assert!(!verified_page(
            &p,
            &PageId(CHILD.into()),
            &PageId(PARENT.into()),
            &format!("https://www.notion.so/{CHILD}"),
            "A new page"
        ));
    }
    fn scope() -> LifecycleScope {
        use notion_knowledge_core::discovery::ExclusionRules;
        LifecycleScope {
            workspace_id: "44444444-4444-4444-4444-444444444444".into(),
            generation: 1,
            roots: vec![PageId("33333333-3333-3333-3333-333333333333".into())],
            exclusions: ExclusionRules::default(),
        }
    }
    fn node(id: &str, parent: Option<&str>) -> Value {
        json!({"object":"page","id":id,"last_edited_time":"2026-10-11T10:00:00Z",
        "in_trash":false,"parent":match parent {
            Some(parent)=>json!({"type":"page_id","page_id":parent}),
            None=>json!({"type":"workspace","workspace":true})
        }})
    }
    fn chain(parent: &str) -> Vec<(String, u16, Value)> {
        let page = node(PARENT, Some(parent));
        let root = node(parent, None);
        vec![
            (format!("GET /v1/pages/{PARENT}"), 200, page.clone()),
            (format!("GET /v1/pages/{parent}"), 200, root.clone()),
            (format!("GET /v1/pages/{parent}"), 200, root),
            (format!("GET /v1/pages/{PARENT}"), 200, page),
        ]
    }
    async fn fixture(
        expected: Vec<(String, u16, Value)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-secret")
            .unwrap()
            .with_fast_fixture_transport();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (path, status, value) in expected {
                let (mut socket, _) =
                    tokio::time::timeout(Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut b = [0u8; 4096];
                    let n = socket.read(&mut b).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&b[..n]);
                    if let Some(k) = bytes.windows(4).position(|a| a == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&bytes[..k]).to_lowercase();
                        assert!(headers.starts_with(&path.to_lowercase()), "expected {path}");
                        assert!(headers.contains("authorization: bearer fixture-secret"));
                        let length = headers
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length: ")
                                    .and_then(|n| n.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (k + 4, length);
                    }
                };
                while bytes.len() < end + length {
                    let mut b = [0u8; 4096];
                    let n = socket.read(&mut b).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&b[..n]);
                }
                let body = if length > 0 {
                    serde_json::from_slice(&bytes[end..end + length]).unwrap()
                } else {
                    Value::Null
                };
                requests.push((path, body));
                let json = value.to_string();
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",json.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (client, task)
    }

    #[tokio::test]
    async fn scoped_create_verifies_complete_body_and_fresh_child_scope_after_mutation() {
        const ROOT: &str = "33333333-3333-3333-3333-333333333333";
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(("POST /v1/pages".into(), 200, receipt()));
        steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
        steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
        steps.push((format!("GET /v1/pages/{CHILD}/markdown"),200,json!({"object":"page_markdown","id":CHILD,"markdown":"Body","truncated":false,"unknown_block_ids":[]})));
        steps.extend(child_chain());
        steps.extend(child_chain());
        let (client, task) = fixture(steps).await;
        let created = client.create_scoped_page(&input(), &scope()).await.unwrap();
        assert!(created.verified);
        assert_eq!(created.page_id.0, CHILD);
        assert_eq!(created.parent_page_id.0, PARENT);
        let calls = task.await.unwrap();
        assert_eq!(calls.len(), 24);
        assert_eq!(calls[8].1["icon"]["emoji"], "📄");
        assert_eq!(calls[8].1["markdown"], "Body");
    }

    #[tokio::test]
    async fn created_metadata_cannot_hide_mismatched_or_incomplete_authoritative_content() {
        const ROOT: &str = "33333333-3333-3333-3333-333333333333";
        for (markdown, truncated, unknown, kind) in [
            ("Different", false, vec![], BackendErrorKind::Conflict),
            ("Body", true, vec![], BackendErrorKind::UnsupportedContent),
            (
                "Body",
                false,
                vec!["unknown"],
                BackendErrorKind::UnsupportedContent,
            ),
        ] {
            let mut steps = chain(ROOT);
            steps.extend(chain(ROOT));
            steps.push(("POST /v1/pages".into(), 200, receipt()));
            steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
            steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
            steps.push((format!("GET /v1/pages/{CHILD}/markdown"),200,json!({"object":"page_markdown","id":CHILD,"markdown":markdown,"truncated":truncated,"unknown_block_ids":unknown})));
            let (client, task) = fixture(steps).await;
            assert_eq!(
                client
                    .create_scoped_page(&input(), &scope())
                    .await
                    .unwrap_err()
                    .kind,
                kind
            );
            assert_eq!(
                task.await
                    .unwrap()
                    .iter()
                    .filter(|(p, _)| p.starts_with("POST"))
                    .count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn once_only_create_admits_refresh_and_replays_without_another_post() {
        use notion_knowledge_core::write_refresh::IndexStatus;
        const ROOT: &str = "33333333-3333-3333-3333-333333333333";
        let mut steps = chain(ROOT); // outer trusted workflow authorization
        steps.extend(chain(ROOT));
        steps.extend(chain(ROOT));
        steps.push(("POST /v1/pages".into(), 200, receipt()));
        steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
        steps.push((format!("GET /v1/pages/{CHILD}"), 200, complete_read()));
        steps.push((format!("GET /v1/pages/{CHILD}/markdown"),200,json!({"object":"page_markdown","id":CHILD,"markdown":"Body","truncated":false,"unknown_block_ids":[]})));
        steps.extend(child_chain());
        steps.extend(child_chain());
        steps.extend(chain(ROOT)); // replay still authorizes both parent and child
        steps.extend(child_chain());
        steps.extend(child_chain());
        let (client, task) = fixture(steps).await;
        let dir = tempfile::tempdir().unwrap();
        let ledger =
            notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(
                dir.path().join("writes.sqlite"),
            )
            .unwrap();
        let queue = notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(
            dir.path().join("sync.sqlite"),
        )
        .unwrap();
        let first = client
            .create_scoped_page_once_and_refresh(
                &input(),
                &scope(),
                &ledger,
                &queue,
                "create-once-readback-test-1234",
            )
            .await
            .unwrap();
        assert_eq!(first.index, IndexStatus::Queued);
        assert!(!first.replayed);
        let replay = client
            .create_scoped_page_once_and_refresh(
                &input(),
                &scope(),
                &ledger,
                &queue,
                "create-once-readback-test-1234",
            )
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.receipt, first.receipt);
        assert_eq!(replay.refresh, first.refresh);
        assert_eq!(
            task.await
                .unwrap()
                .iter()
                .filter(|(path, _)| path.starts_with("POST"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn failed_parent_scope_never_calls_post() {
        const OUTSIDE: &str = "55555555-5555-5555-5555-555555555555";
        let (client, task) = fixture(chain(OUTSIDE)).await;
        assert_eq!(
            client
                .create_scoped_page(&input(), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::PermissionDenied
        );
        let calls = task.await.unwrap();
        assert_eq!(calls.len(), 4);
        assert!(calls.iter().all(|(path, _)| path.starts_with("GET")));
    }

    #[tokio::test]
    async fn missing_created_page_retries_reads_but_never_replays_post() {
        const ROOT: &str = "33333333-3333-3333-3333-333333333333";
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(("POST /v1/pages".into(), 200, receipt()));
        for _ in 0..3 {
            steps.push((
                format!("GET /v1/pages/{CHILD}"),
                404,
                json!({"code":"not_found"}),
            ));
        }
        let (client, task) = fixture(steps).await;
        let err = client
            .create_scoped_page(&input(), &scope())
            .await
            .unwrap_err();
        assert_eq!(err.kind, BackendErrorKind::Conflict);
        let calls = task.await.unwrap();
        assert_eq!(
            calls
                .iter()
                .filter(|(path, _)| path.starts_with("POST"))
                .count(),
            1
        );
        assert_eq!(calls.len(), 12);
    }
}
