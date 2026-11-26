//! Actual MCP transport plus durable production ledger/audit/refresh adapters.
//! Source effects are local fixtures; no credentials or provider network calls.
use notion_knowledge_core::{
    audit::{AuditError, AuditEvent, AuditOutcome, AuditRetention, AuditStore},
    backend::{
        AppendPageContent, BackendError, BackendErrorKind, BackendFuture, CreatePage, NotionRead,
        NotionSectionWrite, NotionWrite, Page, PageContent, PageId, ReplacePageContent,
        UpdateSectionContent,
    },
    destructive::{
        ConfirmationDecision, ConfirmationFuture, ConfirmationProvider, ConfirmationRequest,
    },
    discovery::ExclusionRules,
    lifecycle::LifecycleScope,
    write_refresh::{IndexStatus, MutationRefreshQueue, RefreshKey, RefreshQueueError},
};
use notion_knowledge_mcp::{KnowledgeServer, mutations::MutationRuntime};
use notion_knowledge_retrieval::{
    audit::SqliteAuditStore, idempotency::SqliteIdempotencyStore, sync_state::SqliteSyncStateStore,
};
use rmcp::{ServerHandler, ServiceExt};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::Notify,
};
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const CHILD: &str = "dddddddd-dddd-dddd-dddd-dddddddddddd";
const OTHER: &str = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee";
const REV: &str = "2026-10-10T12:00:00Z";
const BODY: &str = "# Intro\n\nOriginal.\n\n## Target\n\nOld text.\n\n## Tail\n\nUntouched.\n";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: ExclusionRules::default(),
    }
}
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "nk-mutation-dispatch-{}-{}",
            std::process::id(),
            notion_knowledge_core::logging::CorrelationId::new().as_str()
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Audit {
    inner: SqliteAuditStore,
    events: Mutex<Vec<AuditEvent>>,
    fail_start: AtomicBool,
    fail_terminal: AtomicBool,
}
impl AuditStore for Audit {
    fn record(&self, event: &AuditEvent, now: i64) -> Result<(), AuditError> {
        if (event.outcome() == AuditOutcome::Attempted && self.fail_start.load(Ordering::SeqCst))
            || (event.outcome() != AuditOutcome::Attempted
                && self.fail_terminal.load(Ordering::SeqCst))
        {
            return Err(AuditError::Unavailable);
        }
        self.inner.record(event, now)?;
        self.events.lock().unwrap().push(event.clone());
        Ok(())
    }
    fn prune(&self, now: i64) -> Result<u64, AuditError> {
        self.inner.prune(now)
    }
}
struct Queue {
    inner: SqliteSyncStateStore,
    fail: AtomicBool,
    calls: AtomicUsize,
}
impl MutationRefreshQueue for Queue {
    fn admit(&self, key: &RefreshKey) -> Result<IndexStatus, RefreshQueueError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(RefreshQueueError::Unavailable)
        } else {
            self.inner.admit(key)
        }
    }
    fn status(&self, key: &RefreshKey) -> Result<Option<IndexStatus>, RefreshQueueError> {
        self.inner.status(key)
    }
}
#[derive(Clone)]
struct Document {
    title: String,
    body: String,
    revision: u32,
}
struct Source {
    pages: Mutex<HashMap<String, Document>>,
    effects: AtomicUsize,
    reads: AtomicUsize,
    audit: Arc<Audit>,
    bad_readback: AtomicBool,
    omit_final_lf: AtomicBool,
    pause: AtomicBool,
    entered: Notify,
    proceed: Notify,
    /// The next provider effect fails with this error and changes nothing.
    refuse: Mutex<Option<BackendError>>,
    /// The next page metadata read fails with this error.
    refuse_read: Mutex<Option<BackendError>>,
    parent_is_database: AtomicBool,
}
fn refusal(kind: BackendErrorKind, committed: Option<&str>) -> BackendError {
    BackendError {
        kind,
        operation: "fixture.effect",
        retry_after: None,
        committed_page_id: committed.map(|id| PageId(id.into())),
    }
}
impl Source {
    fn refused(&self) -> Result<(), BackendError> {
        self.refuse.lock().unwrap().take().map_or(Ok(()), Err)
    }
    fn page(&self, id: &str) -> Page {
        let pages = self.pages.lock().unwrap();
        let d = pages.get(id).unwrap();
        Page {
            id: PageId(id.into()),
            title: d.title.clone(),
            url: format!("https://www.notion.so/{id}"),
            last_edited_time: format!("2026-10-10T12:00:{:02}Z", d.revision),
            archived: false,
            properties: Default::default(),
            relation_names: Default::default(),
        }
    }
    fn before_effect(&self) {
        assert_eq!(
            self.audit.events.lock().unwrap().last().unwrap().outcome(),
            AuditOutcome::Attempted,
            "durable audit must precede a provider effect"
        );
        self.effects.fetch_add(1, Ordering::SeqCst);
    }
    async fn after_effect(&self) {
        if self.pause.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.proceed.notified().await;
        }
    }
}
impl NotionRead for Source {
    fn fetch_page<'a>(&'a self, id: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if let Some(error) = self.refuse_read.lock().unwrap().take() {
                return Err(error);
            }
            Ok(self.page(&id.0))
        })
    }
    fn is_database<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, bool> {
        Box::pin(async move { Ok(self.parent_is_database.load(Ordering::SeqCst)) })
    }
    fn read_content<'a>(&'a self, id: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let body = if self.bad_readback.load(Ordering::SeqCst)
                && self.effects.load(Ordering::SeqCst) > 0
            {
                "INCOMPLETE READBACK".into()
            } else {
                self.pages.lock().unwrap().get(&id.0).unwrap().body.clone()
            };
            Ok(PageContent {
                page: self.page(&id.0),
                markdown: body,
            })
        })
    }
}
impl NotionWrite for Source {
    fn append_content(&self, q: AppendPageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            self.before_effect();
            self.refused()?;
            {
                let mut pages = self.pages.lock().unwrap();
                let d = pages.get_mut(&q.page_id.0).unwrap();
                if !d.body.is_empty() && !d.body.ends_with('\n') {
                    d.body.push('\n');
                }
                d.body.push_str(&q.markdown);
                if self.omit_final_lf.load(Ordering::SeqCst) && d.body.ends_with('\n') {
                    d.body.pop();
                }
                d.revision += 1;
            }
            self.after_effect().await;
            Ok(self.page(&q.page_id.0))
        })
    }
    fn create_page(&self, q: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            self.before_effect();
            self.refused()?;
            let mut body = q.markdown;
            if self.omit_final_lf.load(Ordering::SeqCst) && body.ends_with('\n') {
                body.pop();
            }
            self.pages.lock().unwrap().insert(
                CHILD.into(),
                Document {
                    title: q.title,
                    body,
                    revision: 1,
                },
            );
            self.after_effect().await;
            Ok(self.page(CHILD))
        })
    }
    fn replace_content(&self, _: ReplacePageContent) -> BackendFuture<'_, Page> {
        panic!("whole-page replacement is never installed")
    }
}
impl NotionSectionWrite for Source {
    fn update_section(&self, q: UpdateSectionContent) -> BackendFuture<'_, ()> {
        Box::pin(async move {
            self.before_effect();
            self.refused()?;
            {
                let mut pages = self.pages.lock().unwrap();
                let d = pages.get_mut(&q.page_id.0).unwrap();
                assert_eq!(d.body.matches(&q.old_str).count(), 1);
                d.body = d.body.replacen(&q.old_str, &q.new_str, 1);
                d.revision += 1;
            }
            self.after_effect().await;
            Ok(())
        })
    }
}
struct Confirmation {
    approve: AtomicBool,
    requests: Mutex<Vec<ConfirmationRequest>>,
    change: Mutex<Option<Arc<Source>>>,
}
impl ConfirmationProvider for Confirmation {
    fn decide<'a>(&'a self, q: &'a ConfirmationRequest) -> ConfirmationFuture<'a> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(q.clone());
            if let Some(source) = self.change.lock().unwrap().take() {
                let mut pages = source.pages.lock().unwrap();
                let page = pages.get_mut(PAGE).unwrap();
                page.body.push_str("Human edit during confirmation.\n");
                page.revision += 1;
            }
            if self.approve.load(Ordering::SeqCst) {
                ConfirmationDecision::Approved
            } else {
                ConfirmationDecision::Declined
            }
        })
    }
}
struct Fixture {
    _dir: Dir,
    source: Arc<Source>,
    audit: Arc<Audit>,
    queue: Arc<Queue>,
    ledger: Arc<SqliteIdempotencyStore>,
    confirmation: Arc<Confirmation>,
}
impl Fixture {
    fn new() -> Self {
        let dir = Dir::new();
        let audit = Arc::new(Audit {
            inner: SqliteAuditStore::open(dir.0.join("audit.sqlite"), AuditRetention::default())
                .unwrap(),
            events: Mutex::new(vec![]),
            fail_start: AtomicBool::new(false),
            fail_terminal: AtomicBool::new(false),
        });
        let source = Arc::new(Source {
            pages: Mutex::new(HashMap::from([
                (
                    ROOT.into(),
                    Document {
                        title: "Root".into(),
                        body: "".into(),
                        revision: 0,
                    },
                ),
                (
                    PAGE.into(),
                    Document {
                        title: "Existing".into(),
                        body: BODY.into(),
                        revision: 0,
                    },
                ),
                (
                    OTHER.into(),
                    Document {
                        title: "Other parent".into(),
                        body: "".into(),
                        revision: 0,
                    },
                ),
            ])),
            effects: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            audit: audit.clone(),
            bad_readback: AtomicBool::new(false),
            omit_final_lf: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: Notify::new(),
            proceed: Notify::new(),
            refuse: Mutex::new(None),
            refuse_read: Mutex::new(None),
            parent_is_database: AtomicBool::new(false),
        });
        let queue = Arc::new(Queue {
            inner: SqliteSyncStateStore::open(dir.0.join("sync.sqlite")).unwrap(),
            fail: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let ledger =
            Arc::new(SqliteIdempotencyStore::initialize_new(dir.0.join("ledger.sqlite")).unwrap());
        Self {
            _dir: dir,
            source,
            audit,
            queue,
            ledger,
            confirmation: Arc::new(Confirmation {
                approve: AtomicBool::new(true),
                requests: Mutex::new(vec![]),
                change: Mutex::new(None),
            }),
        }
    }
    fn runtime(&self, confirmed: bool) -> Arc<MutationRuntime> {
        let mut runtime = MutationRuntime::new(
            self.source.clone(),
            scope(),
            self.ledger.clone(),
            self.queue.clone(),
            self.audit.clone(),
            notion_knowledge_core::audit::AuditActor::ApprovedUser,
        )
        .unwrap();
        if confirmed {
            runtime = runtime.with_confirmation(self.confirmation.clone());
        }
        Arc::new(runtime)
    }
    fn server(&self, confirmed: bool) -> KnowledgeServer {
        KnowledgeServer::default()
            .and_mutations(self.runtime(confirmed))
            .with_read_only(false)
            .with_destructive_writes_enabled(true)
    }
}
struct Task(tokio::task::JoinHandle<()>);
impl Drop for Task {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn invoke(handler: KnowledgeServer, name: &str, args: Value) -> Value {
    request(handler, "tools/call", json!({"name":name,"arguments":args})).await
}

async fn request(handler: KnowledgeServer, method: &str, params: Value) -> Value {
    let (client, server) = tokio::io::duplex(65536);
    let _task = Task(tokio::spawn(async move {
        let _ = handler.serve(server).await.unwrap().waiting().await;
    }));
    let (reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(reader);
    for message in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"mutation-fixture","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":method,"params":params}),
    ] {
        writer
            .write_all(format!("{message}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        if message["id"] == 2 {
            return serde_json::from_str(&line).unwrap();
        }
        writer
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
    }
    unreachable!()
}
fn append(key: &str) -> Value {
    json!({"operation":"append_markdown","root_page_id":ROOT,"page_id":PAGE,"markdown":"\nNew append.\n","idempotency_key":key,"expected_last_edited_time":REV})
}
fn create(key: &str) -> Value {
    json!({"root_page_id":ROOT,"parent_page_id":PAGE,"title":"Created title","markdown":"Created exact body.\n","idempotency_key":key})
}
fn section(key: &str) -> Value {
    json!({"root_page_id":ROOT,"page_id":PAGE,"markdown":"Replacement.\n\n","section_anchor":{"level":2,"heading":"Target"},"idempotency_key":key,"expected_last_edited_time":REV})
}
fn output(response: &Value) -> &Value {
    assert!(response.get("error").is_none(), "{response}");
    &response["result"]["structuredContent"]
}

#[tokio::test]
async fn create_append_and_section_dispatch_verify_source_and_return_separate_index_status() {
    for (name, args) in [
        (
            "knowledge_create_page",
            create("create-through-mcp-fixture"),
        ),
        (
            "knowledge_update_page",
            append("append-through-mcp-fixture"),
        ),
        (
            "knowledge_update_section",
            section("section-through-mcp-fixture"),
        ),
    ] {
        let f = Fixture::new();
        let handler = f.server(true);
        let response = invoke(handler.clone(), name, args.clone()).await;
        let out = output(&response);
        assert_eq!(out["write"]["status"], "verified", "{response}");
        assert_eq!(out["write"]["replayed"], false);
        assert_eq!(out["index"]["status"], "queued");
        assert_eq!(out["audit"]["status"], "recorded");
        let replay = invoke(handler, name, args).await;
        assert_eq!(output(&replay)["write"]["replayed"], true);
        assert_eq!(
            output(&replay)["index"]["refresh_id"],
            out["index"]["refresh_id"]
        );
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        if name == "knowledge_update_section" {
            assert_eq!(
                f.confirmation.requests.lock().unwrap().len(),
                1,
                "verified replay cannot consume a second one-use approval"
            );
        }
        let target = if name == "knowledge_create_page" {
            CHILD
        } else {
            PAGE
        };
        assert_eq!(out["write"]["page_id"], target);
        if name == "knowledge_update_section" {
            let pages = f.source.pages.lock().unwrap();
            let body = &pages.get(PAGE).unwrap().body;
            assert!(body.starts_with("# Intro\n\nOriginal."));
            assert!(body.contains("## Tail\n\nUntouched."));
            assert!(!body.contains("Old text."));
        }
        let events = f.audit.events.lock().unwrap();
        assert_eq!(events.first().unwrap().outcome(), AuditOutcome::Attempted);
        assert_eq!(events.last().unwrap().outcome(), AuditOutcome::Succeeded);
        assert_eq!(events[1].target_page_id(), Some(target));
    }
}

#[tokio::test]
async fn installed_writes_explain_root_selection_and_allow_retry_without_consuming_the_key() {
    let discovery = Fixture::new();
    let catalog = request(discovery.server(false), "tools/list", json!({})).await;
    let tools = catalog["result"]["tools"].as_array().unwrap();
    for (name, destination, args) in [
        (
            "knowledge_create_page",
            "parent_page_id",
            create("correct-root-create-fixture"),
        ),
        (
            "knowledge_update_page",
            "page_id",
            append("correct-root-append-fixture"),
        ),
    ] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        let fields = &tool["inputSchema"]["properties"];
        let root_description = fields["root_page_id"]["description"].as_str().unwrap();
        assert!(root_description.contains("knowledge_get"));
        assert!(root_description.contains("sources[].provenance.root_page_id"));
        assert!(
            fields[destination]["description"]
                .as_str()
                .unwrap()
                .contains("Page UUID")
        );
        let response_properties = &tool["outputSchema"]["properties"]["write"]["properties"];
        assert_eq!(
            response_properties["reason"]["enum"],
            json!([
                "root_not_configured",
                "not_found",
                "permission_denied",
                "invalid_request",
                "parent_is_database"
            ])
        );
        assert_eq!(response_properties["message"]["type"], "string");
        assert!(!tool.to_string().contains(ROOT));

        let f = Fixture::new();
        let handler = f.server(false);
        let mut wrong_root = args.clone();
        wrong_root["root_page_id"] = args[destination].clone();
        let rejected = invoke(handler.clone(), name, wrong_root).await;
        assert_eq!(rejected["result"]["isError"], true);
        let out = output(&rejected);
        assert_eq!(out["write"]["status"], "not_authorized");
        assert_eq!(out["write"]["reason"], "root_not_configured");
        assert_eq!(
            out["write"]["message"],
            "Use sources[].provenance.root_page_id from knowledge_get on an indexed page under the intended root. Use the destination page for that lookup when it is indexed. Supply the destination separately as parent_page_id for create or page_id for append or section updates."
        );
        assert_eq!(out["index"]["status"], "not_attempted");
        assert_eq!(out["audit"]["status"], "recorded");
        assert!(!rejected.to_string().contains(ROOT));
        assert!(!rejected.to_string().contains(PAGE));
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
        assert_eq!(f.source.reads.load(Ordering::SeqCst), 0);
        assert_eq!(f.queue.calls.load(Ordering::SeqCst), 0);
        let outcomes = f
            .audit
            .events
            .lock()
            .unwrap()
            .iter()
            .map(AuditEvent::outcome)
            .collect::<Vec<_>>();
        assert_eq!(outcomes, [AuditOutcome::Attempted, AuditOutcome::Denied]);

        let retried = invoke(handler, name, args).await;
        let out = output(&retried);
        assert_eq!(out["write"]["status"], "verified");
        assert_eq!(out["write"]["replayed"], false);
        assert!(out["write"].get("reason").is_none());
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn normalized_final_line_feed_preserves_verified_receipts_and_once_only_replay() {
    for (name, args) in [
        ("knowledge_create_page", create("normalized-create-key")),
        ("knowledge_update_page", append("normalized-append-key")),
    ] {
        let f = Fixture::new();
        f.source.omit_final_lf.store(true, Ordering::SeqCst);
        let handler = f.server(true);
        let response = invoke(handler.clone(), name, args.clone()).await;
        let out = output(&response);
        assert_eq!(out["write"]["status"], "verified", "{response}");
        assert_eq!(out["write"]["replayed"], false);
        assert_eq!(out["audit"]["status"], "recorded");
        assert_eq!(out["index"]["status"], "queued");
        let replay = invoke(handler, name, args).await;
        assert_eq!(output(&replay)["write"]["replayed"], true);
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        let events = f.audit.events.lock().unwrap();
        assert_eq!(events[1].outcome(), AuditOutcome::Succeeded);
    }
}

#[tokio::test]
async fn append_verifies_the_provider_separator_and_replays_without_adding_another_paragraph() {
    let f = Fixture::new();
    f.source.pages.lock().unwrap().get_mut(PAGE).unwrap().body = "Original paragraph.".into();
    let handler = f.server(false);
    let mut args = append("append-with-provider-separator");
    args["markdown"] = json!("New paragraph.");
    let response = invoke(handler.clone(), "knowledge_update_page", args.clone()).await;
    assert_eq!(
        output(&response)["write"]["status"],
        "verified",
        "{response}"
    );
    let replay = invoke(handler, "knowledge_update_page", args).await;
    assert_eq!(output(&replay)["write"]["replayed"], true);
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.source.pages.lock().unwrap().get(PAGE).unwrap().body,
        "Original paragraph.\nNew paragraph."
    );
}

#[tokio::test]
async fn defaults_and_absent_confirmation_hide_or_reject_uninstalled_mutations() {
    let f = Fixture::new();
    assert!(
        f.server(false)
            .get_tool("knowledge_update_section")
            .is_none()
    );
    for handler in [
        f.server(true).with_read_only(true),
        f.server(true).with_capability_policy(
            notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(None, Some("write"))
                .unwrap(),
        ),
    ] {
        assert!(handler.get_tool("knowledge_create_page").is_none());
        assert!(handler.get_tool("knowledge_update_page").is_none());
        let response = invoke(
            handler,
            "knowledge_create_page",
            create("denied-create-default-key"),
        )
        .await;
        assert!(response.get("error").is_some());
    }
    assert!(
        f.server(true)
            .with_destructive_writes_enabled(false)
            .get_tool("knowledge_update_section")
            .is_none()
    );
    let handler = f.server(true).with_write_design_preview();
    assert!(handler.get_tool("knowledge_archive_page").is_none());
    assert!(
        !handler
            .tool_catalog()
            .iter()
            .any(|t| t.name == "knowledge_archive_page")
    );
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    assert_eq!(
        f.source.reads.load(Ordering::SeqCst),
        0,
        "outer policy gates precede every authoritative port"
    );
}

#[tokio::test]
async fn untrusted_authority_confirmation_fields_and_root_claims_cannot_grant_a_write() {
    let f = Fixture::new();
    for extra in [
        "actor",
        "credentials",
        "confirmed",
        "confirmation_id",
        "workspace_id",
    ] {
        let mut args = append("unknown-fields-must-not-write");
        args[extra] = json!(true);
        assert!(
            invoke(f.server(true), "knowledge_update_page", args)
                .await
                .get("error")
                .is_some()
        );
    }
    let mut foreign = append("foreign-root-claim-must-deny");
    foreign["root_page_id"] = json!(OTHER);
    let response = invoke(f.server(true), "knowledge_update_page", foreign).await;
    assert_eq!(output(&response)["write"]["status"], "not_authorized");
    let mut schema_root = section("forged-confirmation-id-key");
    schema_root["confirmation_id"] = json!("forged");
    assert!(
        invoke(f.server(true), "knowledge_update_section", schema_root)
            .await
            .get("error")
            .is_some()
    );
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    assert!(f.confirmation.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn trusted_confirmation_is_required_and_binds_the_complete_section_intent() {
    let f = Fixture::new();
    f.confirmation.approve.store(false, Ordering::SeqCst);
    let first = section("declined-section-intent-01");
    let mut changed = first.clone();
    changed["markdown"] = json!("Different replacement.\n\n");
    changed["idempotency_key"] = json!("declined-section-intent-02");
    for args in [first, changed] {
        let result = invoke(f.server(true), "knowledge_update_section", args).await;
        let write = &output(&result)["write"];
        assert_eq!(write["status"], "not_authorized");
        assert!(write.get("reason").is_none());
    }
    let requests = f.confirmation.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0].request_sha256(), requests[1].request_sha256());
    assert_eq!(requests[0].target_id(), PAGE);
    assert!(requests[0].trusted_scope().starts_with("scope-"));
    let original = requests[0].section_preview().unwrap();
    assert_eq!(original.section_anchor, "## Target");
    assert_eq!(original.replacement_body, "Replacement.\n\n");
    assert_ne!(original.original_body, original.replacement_body);
    assert_eq!(
        requests[1].section_preview().unwrap().replacement_body,
        "Different replacement.\n\n"
    );
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn revision_conflict_and_authoritative_readback_failure_never_report_verified_success() {
    let f = Fixture::new();
    let mut stale = append("stale-revision-never-writes");
    stale["expected_last_edited_time"] = json!("2026-10-09T12:00:00Z");
    let result = invoke(f.server(false), "knowledge_update_page", stale).await;
    assert_eq!(output(&result)["write"]["status"], "revision_conflict");
    assert_eq!(output(&result)["write"]["page_id"], PAGE);
    assert_eq!(output(&result)["write"]["current_last_edited_time"], REV);
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    for (name, args) in [
        (
            "knowledge_create_page",
            create("unknown-create-readback-key"),
        ),
        (
            "knowledge_update_page",
            append("unknown-append-readback-key"),
        ),
        (
            "knowledge_update_section",
            section("unknown-section-readback-key"),
        ),
    ] {
        let f = Fixture::new();
        f.source.bad_readback.store(true, Ordering::SeqCst);
        for _ in 0..2 {
            let result = invoke(f.server(true), name, args.clone()).await;
            assert_eq!(output(&result)["write"]["status"], "outcome_unknown");
            assert!(!result.to_string().contains("INCOMPLETE READBACK"));
        }
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        assert_eq!(f.queue.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn queue_or_index_failure_preserves_verified_receipt_and_replay_never_repeats_write() {
    let f = Fixture::new();
    f.queue.fail.store(true, Ordering::SeqCst);
    let args = append("queue-failure-preserve-write");
    let first = invoke(f.server(false), "knowledge_update_page", args.clone()).await;
    assert_eq!(output(&first)["write"]["status"], "verified");
    assert_eq!(output(&first)["index"]["status"], "unavailable");
    f.queue.fail.store(false, Ordering::SeqCst);
    let next = invoke(f.server(false), "knowledge_update_page", args.clone()).await;
    assert_eq!(output(&next)["write"]["replayed"], true);
    assert_eq!(output(&next)["index"]["status"], "queued");
    let claim = f
        .queue
        .inner
        .claim_mutation_refresh(WORKSPACE, 100, 10)
        .unwrap()
        .unwrap();
    f.queue
        .inner
        .complete_mutation_refresh(&claim, 101, false)
        .unwrap();
    let failed = invoke(f.server(false), "knowledge_update_page", args).await;
    assert_eq!(output(&failed)["write"]["status"], "verified");
    assert_eq!(output(&failed)["index"]["status"], "failed");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn audit_failure_before_effect_denies_and_terminal_failure_retains_verified_receipt() {
    let f = Fixture::new();
    f.audit.fail_start.store(true, Ordering::SeqCst);
    assert!(
        invoke(
            f.server(false),
            "knowledge_update_page",
            append("failed-audit-before-effect")
        )
        .await
        .get("error")
        .is_some()
    );
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    f.audit.fail_start.store(false, Ordering::SeqCst);
    f.audit.fail_terminal.store(true, Ordering::SeqCst);
    let args = append("terminal-audit-failure-key");
    let result = invoke(f.server(false), "knowledge_update_page", args.clone()).await;
    assert_eq!(output(&result)["write"]["status"], "verified");
    assert_eq!(output(&result)["audit"]["status"], "unavailable");
    assert_eq!(result["result"]["isError"], true);
    f.audit.fail_terminal.store(false, Ordering::SeqCst);
    let replay = invoke(f.server(false), "knowledge_update_page", args).await;
    assert_eq!(output(&replay)["write"]["replayed"], true);
    assert_eq!(output(&replay)["audit"]["status"], "recorded");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_tool_budget_and_durable_claim_prevent_concurrent_duplicate_effects() {
    let f = Fixture::new();
    let handler = f.server(false).with_tool_capacity(8);
    f.source.pause.store(true, Ordering::SeqCst);
    let first_handler = handler.clone();
    let args = append("concurrent-shared-budget-key");
    let first_args = args.clone();
    let task =
        tokio::spawn(
            async move { invoke(first_handler, "knowledge_update_page", first_args).await },
        );
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.source.entered.notified(),
    )
    .await
    .unwrap();
    let busy = invoke(handler.clone(), "knowledge_update_page", args.clone()).await;
    assert!(busy.to_string().contains("tool_busy"));
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    f.source.proceed.notify_one();
    let first = task.await.unwrap();
    assert_eq!(output(&first)["write"]["status"], "verified");
    let replay = invoke(handler, "knowledge_update_page", args).await;
    assert_eq!(output(&replay)["write"]["replayed"], true);
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_sessions_share_durable_claim_even_when_tool_capacity_allows_both() {
    let f = Fixture::new();
    let handler = f.server(false).with_tool_capacity(16);
    f.source.pause.store(true, Ordering::SeqCst);
    let args = append("durable-concurrent-reservation-key");
    let initial = args.clone();
    let first = handler.clone();
    let task = tokio::spawn(async move { invoke(first, "knowledge_update_page", initial).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.source.entered.notified(),
    )
    .await
    .unwrap();
    let competing = invoke(handler, "knowledge_update_page", args).await;
    assert_eq!(output(&competing)["write"]["status"], "outcome_unknown");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    f.source.proceed.notify_one();
    assert_eq!(output(&task.await.unwrap())["write"]["status"], "verified");
}

#[tokio::test]
async fn confirmation_wait_is_followed_by_fresh_revision_preflight() {
    let f = Fixture::new();
    *f.confirmation.change.lock().unwrap() = Some(f.source.clone());
    let result = invoke(
        f.server(true),
        "knowledge_update_section",
        section("confirmation-changed-revision-key"),
    )
    .await;
    assert_eq!(output(&result)["write"]["status"], "revision_conflict");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    let pages = f.source.pages.lock().unwrap();
    let body = &pages.get(PAGE).unwrap().body;
    assert!(body.contains("Old text."));
    assert!(body.contains("Human edit during confirmation."));
}

#[tokio::test]
async fn reopened_durable_ledger_and_queue_replay_the_verified_receipt_without_source_effect() {
    for (name, args) in [
        (
            "knowledge_create_page",
            create("reopened-create-ledger-key"),
        ),
        (
            "knowledge_update_page",
            append("reopened-append-ledger-key"),
        ),
        (
            "knowledge_update_section",
            section("reopened-section-ledger-key"),
        ),
    ] {
        let f = Fixture::new();
        let first = invoke(f.server(true), name, args.clone()).await;
        let ledger = Arc::new(
            SqliteIdempotencyStore::open_existing(f._dir.0.join("ledger.sqlite")).unwrap(),
        );
        let queue = Arc::new(Queue {
            inner: SqliteSyncStateStore::open_existing(f._dir.0.join("sync.sqlite")).unwrap(),
            fail: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let runtime = MutationRuntime::new(
            f.source.clone(),
            scope(),
            ledger,
            queue,
            f.audit.clone(),
            notion_knowledge_core::audit::AuditActor::ApprovedUser,
        )
        .unwrap()
        .with_confirmation(f.confirmation.clone());
        let server = KnowledgeServer::default()
            .with_read_only(false)
            .with_destructive_writes_enabled(true)
            .and_mutations(Arc::new(runtime));
        let replay = invoke(server, name, args).await;
        assert_eq!(output(&replay)["write"]["replayed"], true);
        assert_eq!(
            output(&replay)["write"]["page_id"],
            output(&first)["write"]["page_id"]
        );
        assert_eq!(output(&replay)["index"], output(&first)["index"]);
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        if name == "knowledge_update_section" {
            assert_eq!(f.confirmation.requests.lock().unwrap().len(), 1);
        }
        for file in ["ledger.sqlite", "audit.sqlite", "sync.sqlite"] {
            let bytes = std::fs::read(f._dir.0.join(file)).unwrap();
            for private in [
                BODY.as_bytes(),
                b"Created exact body.",
                b"Replacement.",
                b"New append.",
            ] {
                assert!(
                    !bytes.windows(private.len()).any(|chunk| chunk == private),
                    "private source content persisted in {file}"
                );
            }
        }
    }
}

#[tokio::test]
async fn transport_cancellation_after_effect_is_indeterminate_and_cannot_repeat_the_mutation() {
    for (name, args) in [
        (
            "knowledge_create_page",
            create("cancelled-create-ledger-key"),
        ),
        (
            "knowledge_update_page",
            append("cancelled-append-ledger-key"),
        ),
        (
            "knowledge_update_section",
            section("cancelled-section-ledger-key"),
        ),
    ] {
        let f = Fixture::new();
        f.source.pause.store(true, Ordering::SeqCst);
        let handler = f.server(true);
        let initial = args.clone();
        let first = handler.clone();
        let task = tokio::spawn(async move { invoke(first, name, initial).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            f.source.entered.notified(),
        )
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if f.audit
                    .events
                    .lock()
                    .unwrap()
                    .last()
                    .is_some_and(|event| event.outcome() == AuditOutcome::Indeterminate)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        f.source.proceed.notify_one();
        let replay = invoke(handler, name, args).await;
        assert_eq!(output(&replay)["write"]["status"], "outcome_unknown");
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        assert_eq!(f.queue.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn write_tool_definitions_stay_compact() {
    let catalog = request(Fixture::new().server(false), "tools/list", json!({})).await;
    let tools = catalog["result"]["tools"].as_array().unwrap();
    let writes: Vec<_> = tools
        .iter()
        .filter(|tool| {
            matches!(
                tool["name"].as_str().unwrap(),
                "knowledge_create_page" | "knowledge_update_page"
            )
        })
        .collect();
    assert_eq!(
        writes.len(),
        2,
        "{:?}",
        tools
            .iter()
            .map(|tool| tool["name"].clone())
            .collect::<Vec<_>>()
    );
    let total: usize = writes.iter().map(|tool| tool.to_string().len()).sum();
    let output = writes[0]["outputSchema"].to_string().len();
    // Limits sit just above the sizes when this guard was added (see #812),
    // plus the refusal reasons of #826.
    assert!(output <= 900, "write result schema grew to {output} B");
    assert!(total <= 4600, "write definitions grew to {total} B");
}

#[tokio::test]
async fn merged_update_page_offers_only_operations_the_policy_permits() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let blocked = Fixture::new().server(false).with_capability_policy(
        CapabilityPolicy::from_lists(None, Some("knowledge_update_row")).unwrap(),
    );
    let catalog = request(blocked.clone(), "tools/list", json!({})).await;
    let update = catalog["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|tool| tool["name"] == "knowledge_update_page")
        .unwrap();
    assert_eq!(
        update["inputSchema"]["properties"]["operation"]["enum"],
        json!(["append_markdown"])
    );
    assert!(update["inputSchema"]["properties"]["properties"].is_null());

    let patch = json!({"operation":"patch_properties","root_page_id":ROOT,"page_id":PAGE,
        "idempotency_key":"merged-patch-denied-fixture","expected_row_revision":"0".repeat(64),
        "properties":{"checkbox-id":{"type":"checkbox","value":true}}});
    let denied = invoke(blocked, "knowledge_update_page", patch).await;
    assert!(denied["error"].is_object(), "{denied}");
}

fn is_rejected(response: &Value) -> bool {
    response["error"].is_object() || response["result"]["isError"] == true
}

#[tokio::test]
async fn merged_append_replays_and_rejects_smuggled_fields() {
    let f = Fixture::new();
    let handler = f.server(true);
    let key = "append-replay-fixture-key";
    let first = invoke(handler.clone(), "knowledge_update_page", append(key)).await;
    assert_eq!(output(&first)["write"]["status"], "verified", "{first}");
    let replay = invoke(handler.clone(), "knowledge_update_page", append(key)).await;
    assert_eq!(output(&replay)["write"]["replayed"], true, "{replay}");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);

    let mut row_field = append("append-with-row-revision-fixture");
    row_field["expected_row_revision"] = json!("0".repeat(64));
    let mut page_field = create("create-with-properties-fixture");
    page_field["properties"] = json!({});
    let mut null_source = create("create-with-null-source-fixture");
    null_source["data_source_id"] = Value::Null;
    let mut bad_operation = append("append-bad-operation-fixture");
    bad_operation["operation"] = json!("APPEND_MARKDOWN");
    let mut missing_operation = append("append-missing-operation-fixture");
    missing_operation
        .as_object_mut()
        .unwrap()
        .remove("operation");
    for (name, args) in [
        ("knowledge_update_page", row_field),
        ("knowledge_create_page", page_field),
        ("knowledge_create_page", null_source),
        ("knowledge_update_page", bad_operation),
        ("knowledge_update_page", missing_operation),
    ] {
        let response = invoke(handler.clone(), name, args).await;
        assert!(is_rejected(&response), "{name}: {response}");
    }
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn existing_allowlists_keep_reaching_append_through_the_merged_tool() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let fixtures: Vec<Fixture> = (0..3).map(|_| Fixture::new()).collect();
    let allow = |index: usize, list: Option<&str>, block: Option<&str>| {
        fixtures[index]
            .server(true)
            .with_capability_policy(CapabilityPolicy::from_lists(list, block).unwrap())
    };
    let listed = |handler: KnowledgeServer| async move {
        let catalog = request(handler, "tools/list", json!({})).await;
        catalog["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let only_append = allow(0, Some("read,knowledge_append"), None);
    let names = listed(only_append.clone()).await;
    assert!(
        names.contains(&"knowledge_update_page".to_owned()),
        "{names:?}"
    );
    assert!(!names.contains(&"knowledge_create_page".to_owned()));
    let response = invoke(
        only_append.clone(),
        "knowledge_update_page",
        append("allowlisted-append-fixture"),
    )
    .await;
    assert_eq!(
        output(&response)["write"]["status"],
        "verified",
        "{response}"
    );
    // Page creation still needs its own selector.
    let denied = invoke(
        only_append,
        "knowledge_create_page",
        create("allowlisted-create-fixture"),
    )
    .await;
    assert!(denied["error"].is_object(), "{denied}");

    let blocked = allow(1, None, Some("knowledge_update_page"));
    assert!(
        !listed(blocked.clone())
            .await
            .contains(&"knowledge_update_page".to_owned())
    );
    let denied = invoke(
        blocked,
        "knowledge_update_page",
        append("blocked-merged-fixture"),
    )
    .await;
    assert!(denied["error"].is_object(), "{denied}");

    let read_only = fixtures[2].server(true).with_read_only(true);
    let denied = invoke(
        read_only,
        "knowledge_update_page",
        append("read-only-merged-fixture"),
    )
    .await;
    assert!(denied["error"].is_object(), "{denied}");
}

#[tokio::test]
async fn retired_alias_names_are_not_callable_tools_but_remain_selectors() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let f = Fixture::new();
    let handler = f.server(true);
    let listed = request(handler.clone(), "tools/list", json!({})).await;
    for name in [
        "knowledge_append",
        "knowledge_create_row",
        "knowledge_update_row",
        "knowledge_query_view",
    ] {
        assert!(handler.get_tool(name).is_none(), "{name}");
        assert!(!listed.to_string().contains(&format!("\"name\":\"{name}\"")));
        let mut args = append("retired-alias-name-fixture");
        args.as_object_mut().unwrap().remove("operation");
        let response = invoke(handler.clone(), name, args).await;
        assert_eq!(
            response["error"]["message"], "unknown tool",
            "{name}: {response}"
        );
        // Operator allowlists naming the operation keep parsing.
        assert!(
            CapabilityPolicy::from_lists(Some(name), None).is_ok(),
            "{name}"
        );
    }
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
}

#[test]
fn update_page_selector_alone_grants_no_operation() {
    use notion_knowledge_mcp::capabilities::CapabilityPolicy;
    let handler = Fixture::new().server(true).with_capability_policy(
        CapabilityPolicy::from_lists(Some("knowledge_update_page"), None).unwrap(),
    );
    assert!(handler.get_tool("knowledge_update_page").is_none());
    assert!(
        handler
            .tool_catalog()
            .iter()
            .all(|tool| tool.name != "knowledge_update_page")
    );
}

fn outcomes(f: &Fixture) -> Vec<AuditOutcome> {
    f.audit
        .events
        .lock()
        .unwrap()
        .iter()
        .map(AuditEvent::outcome)
        .collect()
}

#[tokio::test]
async fn definite_notion_refusals_fail_cleanly_and_release_the_key_for_a_retry() {
    for (name, args, kind, status, reason) in [
        (
            "knowledge_create_page",
            create("refused-create-not-found"),
            BackendErrorKind::NotFound,
            "not_authorized",
            "not_found",
        ),
        (
            "knowledge_create_page",
            create("refused-create-forbidden"),
            BackendErrorKind::PermissionDenied,
            "not_authorized",
            "permission_denied",
        ),
        (
            "knowledge_update_page",
            append("refused-append-forbidden"),
            BackendErrorKind::PermissionDenied,
            "not_authorized",
            "permission_denied",
        ),
        (
            "knowledge_update_section",
            section("refused-section-invalid"),
            BackendErrorKind::InvalidInput,
            "invalid_input",
            "invalid_request",
        ),
    ] {
        let f = Fixture::new();
        let handler = f.server(true);
        *f.source.refuse.lock().unwrap() = Some(refusal(kind, None));
        let refused = invoke(handler.clone(), name, args.clone()).await;
        assert_eq!(refused["result"]["isError"], true);
        let out = output(&refused);
        assert_eq!(out["write"]["status"], status, "{refused}");
        assert_eq!(out["write"]["reason"], reason);
        let message = out["write"]["message"].as_str().unwrap();
        assert!(message.contains("Nothing was"), "{message}");
        assert!(
            message.ends_with("The same idempotency key can be retried."),
            "{message}"
        );
        assert_eq!(out["index"]["status"], "not_attempted");
        assert_eq!(out["audit"]["status"], "recorded");
        for id in [ROOT, PAGE, CHILD] {
            assert!(!refused.to_string().contains(id), "{refused}");
        }
        assert_eq!(
            outcomes(&f),
            [AuditOutcome::Attempted, AuditOutcome::Failed]
        );

        let retried = invoke(handler, name, args).await;
        let out = output(&retried);
        assert_eq!(out["write"]["status"], "verified", "{retried}");
        assert_eq!(out["write"]["replayed"], false);
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 2);
        assert_eq!(f.queue.calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn a_database_given_as_the_parent_page_points_to_data_source_id() {
    let f = Fixture::new();
    f.source.parent_is_database.store(true, Ordering::SeqCst);
    *f.source.refuse.lock().unwrap() = Some(refusal(BackendErrorKind::NotFound, None));
    let handler = f.server(false);
    let refused = invoke(
        handler.clone(),
        "knowledge_create_page",
        create("database-parent-key"),
    )
    .await;
    let out = output(&refused);
    assert_eq!(out["write"]["status"], "invalid_input", "{refused}");
    assert_eq!(out["write"]["reason"], "parent_is_database");
    let message = out["write"]["message"].as_str().unwrap();
    assert!(message.contains("data_source_id"), "{message}");
    assert!(!message.contains("knowledge_create_row"));
    assert!(!refused.to_string().contains(PAGE));

    // The key was released: the same request runs again once the parent is fixed.
    f.source.parent_is_database.store(false, Ordering::SeqCst);
    let retried = invoke(
        handler,
        "knowledge_create_page",
        create("database-parent-key"),
    )
    .await;
    assert_eq!(output(&retried)["write"]["status"], "verified", "{retried}");
}

#[tokio::test]
async fn ambiguous_failures_and_refused_verify_reads_stay_outcome_unknown() {
    for error in [
        refusal(BackendErrorKind::Unavailable, None),
        refusal(BackendErrorKind::RateLimited, None),
        refusal(BackendErrorKind::Internal, None),
        // A verify read after a successful POST names the committed page: a
        // 404 there is read-after-write lag, not a refusal.
        refusal(BackendErrorKind::NotFound, Some(CHILD)),
    ] {
        let f = Fixture::new();
        let handler = f.server(false);
        let args = create("ambiguous-create-failure");
        *f.source.refuse.lock().unwrap() = Some(error);
        for _ in 0..2 {
            let result = invoke(handler.clone(), "knowledge_create_page", args.clone()).await;
            assert_eq!(output(&result)["write"]["status"], "outcome_unknown");
        }
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
        assert_eq!(
            outcomes(&f),
            [
                AuditOutcome::Attempted,
                AuditOutcome::Indeterminate,
                AuditOutcome::Attempted,
                AuditOutcome::Indeterminate
            ]
        );
    }
}

#[tokio::test]
async fn failures_before_any_effect_release_the_key() {
    for (name, args) in [
        ("knowledge_update_page", append("preflight-append-key")),
        ("knowledge_update_section", section("preflight-section-key")),
    ] {
        let f = Fixture::new();
        let handler = f.server(true);
        *f.source.refuse_read.lock().unwrap() = Some(refusal(BackendErrorKind::NotFound, None));
        let refused = invoke(handler.clone(), name, args.clone()).await;
        let out = output(&refused);
        assert_eq!(out["write"]["status"], "not_authorized", "{refused}");
        assert_eq!(out["write"]["reason"], "not_found");
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);

        *f.source.refuse_read.lock().unwrap() = Some(refusal(BackendErrorKind::Unavailable, None));
        let unavailable = invoke(handler.clone(), name, args.clone()).await;
        assert_eq!(output(&unavailable)["write"]["status"], "unavailable");

        let retried = invoke(handler, name, args).await;
        assert_eq!(output(&retried)["write"]["status"], "verified", "{retried}");
        assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    }
}
