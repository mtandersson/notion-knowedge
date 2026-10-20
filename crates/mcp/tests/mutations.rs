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
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
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
fn failure() -> BackendError {
    BackendError {
        kind: BackendErrorKind::Unavailable,
        operation: "fixture",
        retry_after: None,
        committed_page_id: None,
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
    parent: Option<String>,
    title: String,
    body: String,
    revision: u32,
}
struct Source {
    pages: Mutex<HashMap<String, Document>>,
    effects: AtomicUsize,
    reads: AtomicUsize,
    audit: Arc<Audit>,
    deny: AtomicBool,
    bad_readback: AtomicBool,
    wrong_parent: AtomicBool,
    pause: AtomicBool,
    entered: Notify,
    proceed: Notify,
}
impl Source {
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
            Ok(self.page(&id.0))
        })
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
            {
                let mut pages = self.pages.lock().unwrap();
                let d = pages.get_mut(&q.page_id.0).unwrap();
                d.body.push_str(&q.markdown);
                d.revision += 1;
            }
            self.after_effect().await;
            Ok(self.page(&q.page_id.0))
        })
    }
    fn create_page(&self, q: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            self.before_effect();
            self.pages.lock().unwrap().insert(
                CHILD.into(),
                Document {
                    parent: Some(if self.wrong_parent.load(Ordering::SeqCst) {
                        OTHER.into()
                    } else {
                        q.parent_page_id.0
                    }),
                    title: q.title,
                    body: q.markdown,
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
impl PageLifecycle for Source {
    fn lifecycle<'a>(
        &'a self,
        id: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if self.deny.load(Ordering::SeqCst) {
                return Err(failure());
            }
            let pages = self.pages.lock().unwrap();
            let mut ancestry = vec![];
            let mut current = id.0.clone();
            loop {
                let d = pages.get(&current).ok_or_else(failure)?;
                ancestry.push(LifecycleNode {
                    kind: LifecycleKind::Page,
                    id: current.clone(),
                    revision: format!("2026-10-10T12:00:{:02}Z", d.revision),
                    inactive: false,
                    parent: d
                        .parent
                        .as_ref()
                        .map(|id| PhysicalParent::Object {
                            kind: LifecycleKind::Page,
                            id: id.clone(),
                        })
                        .unwrap_or(PhysicalParent::Workspace),
                });
                if let Some(parent) = &d.parent {
                    current = parent.clone();
                } else {
                    break;
                }
            }
            let roots = scope
                .roots
                .iter()
                .filter(|root| ancestry.iter().any(|n| n.id == root.0))
                .map(|id| id.0.clone())
                .collect();
            Ok(LifecycleEvidence {
                scope: scope.clone(),
                status: LifecycleStatus::Allowed { roots },
                ancestry,
            })
        })
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let fresh = self
                .lifecycle(&PageId(evidence.ancestry[0].id.clone()), scope)
                .await?;
            if fresh == *evidence {
                Ok(())
            } else {
                Err(failure())
            }
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
                        parent: None,
                        title: "Root".into(),
                        body: "".into(),
                        revision: 0,
                    },
                ),
                (
                    PAGE.into(),
                    Document {
                        parent: Some(ROOT.into()),
                        title: "Existing".into(),
                        body: BODY.into(),
                        revision: 0,
                    },
                ),
                (
                    OTHER.into(),
                    Document {
                        parent: Some(ROOT.into()),
                        title: "Other parent".into(),
                        body: "".into(),
                        revision: 0,
                    },
                ),
            ])),
            effects: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            audit: audit.clone(),
            deny: AtomicBool::new(false),
            bad_readback: AtomicBool::new(false),
            wrong_parent: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            entered: Notify::new(),
            proceed: Notify::new(),
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
    let (client, server) = tokio::io::duplex(65536);
    let _task = Task(tokio::spawn(async move {
        let _ = handler.serve(server).await.unwrap().waiting().await;
    }));
    let (reader, mut writer) = tokio::io::split(client);
    let mut reader = BufReader::new(reader);
    for message in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"mutation-fixture","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":name,"arguments":args}}),
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
    json!({"root_page_id":ROOT,"page_id":PAGE,"markdown":"\nNew append.\n","idempotency_key":key,"expected_last_edited_time":REV})
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
        ("knowledge_append", append("append-through-mcp-fixture")),
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
async fn defaults_oauth_guards_and_absent_confirmation_hide_or_reject_uninstalled_mutations() {
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
        f.server(true)
            .with_http_authorization_required()
            .with_read_only(false),
        KnowledgeServer::default()
            .with_request_bound_content()
            .and_mutations(f.runtime(true))
            .with_read_only(false),
    ] {
        assert!(handler.get_tool("knowledge_create_page").is_none());
        assert!(handler.get_tool("knowledge_append").is_none());
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
            invoke(f.server(true), "knowledge_append", args)
                .await
                .get("error")
                .is_some()
        );
    }
    let mut foreign = append("foreign-root-claim-must-deny");
    foreign["root_page_id"] = json!(OTHER);
    let response = invoke(f.server(true), "knowledge_append", foreign).await;
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
        assert_eq!(output(&result)["write"]["status"], "not_authorized");
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
    let result = invoke(f.server(false), "knowledge_append", stale).await;
    assert_eq!(output(&result)["write"]["status"], "revision_conflict");
    assert_eq!(output(&result)["write"]["page_id"], PAGE);
    assert_eq!(output(&result)["write"]["current_last_edited_time"], REV);
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 0);
    for (name, args) in [
        (
            "knowledge_create_page",
            create("unknown-create-readback-key"),
        ),
        ("knowledge_append", append("unknown-append-readback-key")),
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
    let f = Fixture::new();
    f.source.wrong_parent.store(true, Ordering::SeqCst);
    let result = invoke(
        f.server(false),
        "knowledge_create_page",
        create("create-wrong-parent-must-deny"),
    )
    .await;
    assert_eq!(output(&result)["write"]["status"], "outcome_unknown");
    assert_eq!(f.queue.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn queue_or_index_failure_preserves_verified_receipt_and_replay_never_repeats_write() {
    let f = Fixture::new();
    f.queue.fail.store(true, Ordering::SeqCst);
    let args = append("queue-failure-preserve-write");
    let first = invoke(f.server(false), "knowledge_append", args.clone()).await;
    assert_eq!(output(&first)["write"]["status"], "verified");
    assert_eq!(output(&first)["index"]["status"], "unavailable");
    f.queue.fail.store(false, Ordering::SeqCst);
    let next = invoke(f.server(false), "knowledge_append", args.clone()).await;
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
    let failed = invoke(f.server(false), "knowledge_append", args).await;
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
            "knowledge_append",
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
    let result = invoke(f.server(false), "knowledge_append", args.clone()).await;
    assert_eq!(output(&result)["write"]["status"], "verified");
    assert_eq!(output(&result)["audit"]["status"], "unavailable");
    assert_eq!(result["result"]["isError"], true);
    f.audit.fail_terminal.store(false, Ordering::SeqCst);
    let replay = invoke(f.server(false), "knowledge_append", args).await;
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
        tokio::spawn(async move { invoke(first_handler, "knowledge_append", first_args).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.source.entered.notified(),
    )
    .await
    .unwrap();
    let busy = invoke(handler.clone(), "knowledge_append", args.clone()).await;
    assert!(busy.to_string().contains("tool_busy"));
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    f.source.proceed.notify_one();
    let first = task.await.unwrap();
    assert_eq!(output(&first)["write"]["status"], "verified");
    let replay = invoke(handler, "knowledge_append", args).await;
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
    let task = tokio::spawn(async move { invoke(first, "knowledge_append", initial).await });
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        f.source.entered.notified(),
    )
    .await
    .unwrap();
    let competing = invoke(handler, "knowledge_append", args).await;
    assert_eq!(output(&competing)["write"]["status"], "outcome_unknown");
    assert_eq!(f.source.effects.load(Ordering::SeqCst), 1);
    f.source.proceed.notify_one();
    assert_eq!(output(&task.await.unwrap())["write"]["status"], "verified");
}

#[tokio::test]
async fn confirmation_wait_is_followed_by_fresh_scope_and_revision_preflight() {
    let f = Fixture::new();
    *f.confirmation.change.lock().unwrap() = Some(f.source.clone());
    let result = invoke(
        f.server(true),
        "knowledge_update_section",
        section("confirmation-changed-revision-key"),
    )
    .await;
    assert_eq!(output(&result)["write"]["status"], "not_authorized");
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
        ("knowledge_append", append("reopened-append-ledger-key")),
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
        ("knowledge_append", append("cancelled-append-ledger-key")),
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
