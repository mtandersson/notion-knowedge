//! Actual shipping HTTP composition with real TLS downloads and native receipts.
use super::installed_writes::{call, connect, rpc};
use super::*;
use notion_knowledge_core::{
    backend::PageId,
    embedding::{EmbeddingError, EmbeddingFuture, EmbeddingMetadata, EmbeddingProvider},
    lifecycle::LifecycleScope,
};
use notion_knowledge_notion::file_download::DownloadPolicy;
use serde_json::{Value, json};
use std::{collections::VecDeque, sync::Mutex, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tokio_util::sync::CancellationToken;
const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
const PAGE: &str = "cccccccc-cccc-cccc-cccc-cccccccccccc";
const URL: &str = "https://fixture.example/file?token=temporary-file-secret";
fn scope() -> LifecycleScope {
    LifecycleScope {
        workspace_id: WORKSPACE.into(),
        generation: 1,
        roots: vec![PageId(ROOT.into())],
        exclusions: Default::default(),
    }
}
struct Source {
    policy: DownloadPolicy,
    count: Arc<AtomicUsize>,
    paths: Arc<Mutex<Vec<std::path::PathBuf>>>,
    server: tokio::task::JoinHandle<()>,
    pause: Arc<AtomicBool>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}
impl Drop for Source {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Source {
    async fn new(replies: Vec<(Vec<u8>, u16)>) -> Self {
        let certificate = include_bytes!("../../../../notion/tests/fixtures/ingest-cert.pem");
        let key = PrivateKeyDer::try_from(
            include_bytes!("../../../../notion/tests/fixtures/ingest-key.der").to_vec(),
        )
        .unwrap();
        let tls = ServerConfig::builder_with_provider(Arc::new(
            tokio_rustls::rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_slice(certificate).unwrap()],
            key,
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let paths = Arc::new(Mutex::new(Vec::new()));
        let policy = DownloadPolicy::new(
            &["fixture.example"],
            10 * 1024 * 1024,
            Duration::from_secs(10),
        )
        .unwrap()
        .with_tls_fixture(
            listener.local_addr().unwrap(),
            reqwest::Certificate::from_pem(certificate).unwrap(),
            paths.clone(),
        );
        let count = Arc::new(AtomicUsize::new(0));
        let seen = count.clone();
        let pause = Arc::new(AtomicBool::new(false));
        let paused = pause.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let signaled = entered.clone();
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let held = release.clone();
        let server = tokio::spawn(async move {
            let mut replies: VecDeque<_> = replies.into();
            while let Ok((socket, _)) = listener.accept().await {
                let mut tls = TlsAcceptor::from(Arc::new(tls.clone()))
                    .accept(socket)
                    .await
                    .unwrap();
                let mut headers = Vec::new();
                loop {
                    let mut bytes = [0; 4096];
                    let n = tls.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    headers.extend_from_slice(&bytes[..n]);
                    if headers.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let headers = String::from_utf8(headers).unwrap();
                assert!(headers.starts_with("GET /file?token=temporary-file-secret "));
                assert!(!headers.to_lowercase().contains("authorization:"));
                seen.fetch_add(1, Ordering::SeqCst);
                let (bytes, status) = replies.pop_front().expect("no implicit download retry");
                if paused.swap(false, Ordering::SeqCst) {
                    signaled.notify_one();
                    let _hold = held.acquire().await.unwrap();
                }
                let _=tls.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",bytes.len()).as_bytes()).await;
                for chunk in bytes.chunks(16384) {
                    if tls.write_all(chunk).await.is_err() {
                        break;
                    }
                }
                let _ = tls.flush().await;
                let _ = tls.shutdown().await;
            }
        });
        Self {
            policy,
            count,
            paths,
            server,
            pause,
            entered,
            release,
        }
    }
    fn cleaned(&self) {
        assert!(self.paths.lock().unwrap().iter().all(|path| !path.exists()));
    }
}
struct Model {
    inner: Arc<dyn EmbeddingProvider>,
    fail: AtomicBool,
}
impl EmbeddingProvider for Model {
    fn metadata(&self) -> &EmbeddingMetadata {
        self.inner.metadata()
    }
    fn embed_batch<'a>(&'a self, text: &'a [String]) -> EmbeddingFuture<'a> {
        Box::pin(async move {
            if self.fail.load(Ordering::SeqCst) {
                Err(EmbeddingError::Unavailable)
            } else {
                self.inner.embed_batch(text).await
            }
        })
    }
}
async fn harness() -> Harness {
    let h = Harness::with_authority(
        grant(WORKSPACE, "user-a"),
        StatusCode::OK,
        None,
        Duration::ZERO,
        Some(scope()),
        true,
    )
    .await;
    fs::set_permissions(h._dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
    *h.document.lock().unwrap() = Some(("Saffron file receipt evidence".into(), 0));
    h
}
fn settings(h: &Harness, seed: &content_index::Seeded, source: &Source) -> Config {
    let mut values = installed_writes::config_values(h, seed);
    for (key, value) in [
        ("NK_FILE_INGESTION", "true"),
        ("NK_FILE_SOURCE_HOSTS", "fixture.example"),
        ("NK_FILE_PLAN", "paid"),
        ("NK_FILE_MAX_BYTES", "10485760"),
        ("NK_FILE_MULTIPART_THRESHOLD_BYTES", "5242880"),
        ("NK_FILE_PART_BYTES", "5242880"),
    ] {
        values.insert(key.into(), value.into());
    }
    values.insert(
        "NK_FILE_AUDIT_FILE".into(),
        h._dir
            .path()
            .join("file-audit.sqlite")
            .to_str()
            .unwrap()
            .into(),
    );
    let mut settings = Config::from_lookup(|key| values.get(key).map(Into::into)).unwrap();
    settings.file_ingestion.as_mut().unwrap().download = source.policy.clone();
    settings
}
struct Installed {
    url: String,
    stop: CancellationToken,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Installed {
    async fn stop(self) {
        self.stop.cancel();
        tokio::time::timeout(Duration::from_secs(5), self.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
async fn start(
    h: &Harness,
    mut settings: Config,
    provider: Arc<dyn EmbeddingProvider>,
) -> Installed {
    let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    settings.http_bind = address;
    drop(socket);
    let url = format!("http://{address}/mcp");
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let credentials = h.login.callback.as_ref().unwrap().credentials.clone();
    let login = h.login.clone();
    let server = tokio::spawn(async move {
        crate::http::serve_with_oauth(
            settings,
            notion_knowledge_mcp::KnowledgeServer::default(),
            None,
            server_stop,
            Some(credentials),
            Some(login),
            Some(provider),
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    Installed { url, stop, server }
}
async fn login(h: &Harness, url: &str, permissions: &str) -> (String, String) {
    let code = h.approved_code_with_scope(permissions).await;
    let token = resource::redeem(h, &code).await;
    let session = connect(h, url, &token).await;
    (token, session)
}
fn input(name: &str, mime: &str) -> Value {
    json!({"file_id":"opaque-chatgpt-file","download_url":URL,"file_name":name,"mime_type":mime})
}
async fn status(
    h: &Harness,
    installed: &Installed,
    token: &str,
    session: &str,
    receipt: &Value,
    retry: bool,
) -> Value {
    call(h,&installed.url,token,session,"knowledge_file_refresh",json!({"page_id":receipt["page_id"],"mutation_digest":receipt["mutation_digest"],"retry":retry})).await
}
async fn wait_status(
    h: &Harness,
    installed: &Installed,
    token: &str,
    session: &str,
    receipt: &Value,
    expected: &str,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current = status(h, installed, token, session, receipt, false).await;
            if current["result"]["structuredContent"]["index_status"] == expected {
                break;
            }
            assert!(current["error"].is_null(), "{current}");
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
}
fn initialize(settings: &Config) {
    let path = &settings.oauth_write_stores.as_ref().unwrap().ledger;
    if !path.exists() {
        drop(
            notion_knowledge_retrieval::idempotency::SqliteIdempotencyStore::initialize_new(path)
                .unwrap(),
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shipping_http_native_image_file_multipart_and_refresh_restart_retry_preserve_source_outcomes()
 {
    let h = harness().await;
    let mut pdf = vec![b'x'; 5 * 1024 * 1024 + 1];
    pdf[..5].copy_from_slice(b"%PDF-");
    let source = Source::new(vec![
        (b"\x89PNG\r\n\x1a\nfixture".to_vec(), 200),
        (b"receipt-note text".to_vec(), 200),
        (pdf, 200),
    ])
    .await;
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let model = Arc::new(Model {
        inner: seed.provider.clone(),
        fail: true.into(),
    });
    let config = settings(&h, &seed, &source);
    initialize(&config);
    let installed = start(&h, config, model.clone()).await;
    let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
    let tools = resource::reply(
        rpc(
            &h,
            &installed.url,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":9,"method":"tools/list"}),
        )
        .await,
    )
    .await;
    assert!(
        tools.to_string().contains("knowledge_upload_file")
            && tools.to_string().contains("openai/fileParams")
            && tools.to_string().contains("knowledge_file_refresh")
    );
    let response=call(&h,&installed.url,&token,&session,"knowledge_upload_file",json!({"target":PAGE,"caption":" Safe caption ","files":[input("receipt-image.png","image/png"),input("receipt-note.txt","text/plain")]})).await;
    let output = &response["result"]["structuredContent"];
    assert_eq!(output["files"][0]["index"], 0, "{response}");
    assert_eq!(output["files"][1]["index"], 1);
    assert_eq!(output["files"][0]["status"], "verified", "{response}");
    assert_eq!(output["files"][1]["status"], "verified", "{response}");
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 2);
    assert_eq!(h.files.read_back.load(Ordering::SeqCst), 2);
    assert!(
        h.files
            .blocks
            .lock()
            .unwrap()
            .values()
            .any(|block| block["type"] == "image")
    );
    assert!(
        h.files
            .blocks
            .lock()
            .unwrap()
            .values()
            .any(|block| block["type"] == "file")
    );
    let receipt = output["files"][0]["refresh"].clone();
    assert_eq!(receipt, output["files"][1]["refresh"]);
    wait_status(&h, &installed, &token, &session, &receipt, "failed").await;
    // A same-grant semantic receipt must not be retried through the file tool.
    let db = rusqlite::Connection::open(&seed.state).unwrap();
    let semantic_digest = "f".repeat(64);
    db.execute("INSERT INTO mutation_refresh(workspace_id,mutation_digest,page_id,authority,state) SELECT workspace_id,?1,page_id,authority,3 FROM mutation_refresh WHERE mutation_digest=?2",rusqlite::params![semantic_digest,receipt["mutation_digest"].as_str().unwrap()]).unwrap();
    let denied = status(
        &h,
        &installed,
        &token,
        &session,
        &json!({"page_id":PAGE,"mutation_digest":semantic_digest}),
        true,
    )
    .await;
    assert_eq!(denied["result"]["isError"], true, "{denied}");
    assert_eq!(
        db.query_row(
            "SELECT state FROM mutation_refresh WHERE mutation_digest=?1",
            [&semantic_digest],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        3
    );
    let unknown = status(
        &h,
        &installed,
        &token,
        &session,
        &json!({"page_id":PAGE,"mutation_digest":"0".repeat(64)}),
        true,
    )
    .await;
    assert_eq!(unknown["result"]["isError"], true, "{unknown}");
    drop(db);
    installed.stop().await;
    model.fail.store(false, Ordering::SeqCst);
    let installed = start(&h, settings(&h, &seed, &source), model.clone()).await;
    let session = connect(&h, &installed.url, &token).await;
    let retried = status(&h, &installed, &token, &session, &receipt, true).await;
    assert!(retried["error"].is_null(), "{retried}");
    wait_status(&h, &installed, &token, &session, &receipt, "completed").await;
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 2);
    assert_eq!(h.files.created.load(Ordering::SeqCst), 2);
    let search = call(
        &h,
        &installed.url,
        &token,
        &session,
        "knowledge_search",
        json!({"query":"receipt-note","mode":"lexical","limit":5}),
    )
    .await;
    assert!(search.to_string().contains("receipt-note.txt"), "{search}");
    let multipart = call(
        &h,
        &installed.url,
        &token,
        &session,
        "knowledge_upload_file",
        json!({"target":PAGE,"file":input("large-evidence.pdf","application/pdf")}),
    )
    .await;
    assert_eq!(
        multipart["result"]["structuredContent"]["files"][0]["status"], "verified",
        "{multipart}"
    );
    assert_eq!(h.files.sent.load(Ordering::SeqCst), 4);
    assert_eq!(h.files.completed.load(Ordering::SeqCst), 1);
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 3);
    assert_eq!(source.count.load(Ordering::SeqCst), 3);
    source.cleaned();
    let audit = rusqlite::Connection::open(h._dir.path().join("file-audit.sqlite")).unwrap();
    assert_eq!(
        audit
            .query_row(
                "SELECT count(*) FROM agent_audit_events WHERE actor!='approved_user'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
        0
    );
    let dump = fs::read(h._dir.path().join("file-audit.sqlite")).unwrap();
    assert!(!String::from_utf8_lossy(&dump).contains("temporary-file-secret"));
    installed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_file_authority_and_url_denials_never_download_or_mutate() {
    let h = harness().await;
    let source = Source::new(vec![]).await;
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let config = settings(&h, &seed, &source);
    initialize(&config);
    let model = Arc::new(Model {
        inner: seed.provider.clone(),
        fail: false.into(),
    });
    let installed = start(&h, config, model).await;
    let (readonly, read_session) = login(&h, &installed.url, "knowledge:read").await;
    let denied = call(
        &h,
        &installed.url,
        &readonly,
        &read_session,
        "knowledge_upload_file",
        json!({"target":PAGE,"file":input("denied.txt","text/plain")}),
    )
    .await;
    assert!(denied["error"].is_object(), "{denied}");
    let listed = resource::reply(
        rpc(
            &h,
            &installed.url,
            &readonly,
            Some(&read_session),
            json!({"jsonrpc":"2.0","id":9,"method":"tools/list"}),
        )
        .await,
    )
    .await;
    assert!(!listed.to_string().contains("knowledge_upload_file"));
    assert!(!listed.to_string().contains("knowledge_file_refresh"));
    let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
    let denied=rpc(&h,&installed.url,"unknown-invalid-token",Some(&session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"file":input("invalid.txt","text/plain")}}})).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    h.clock.store(601, Ordering::SeqCst);
    let denied=rpc(&h,&installed.url,&token,Some(&session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"file":input("expired.txt","text/plain")}}})).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    h.clock.store(0, Ordering::SeqCst);
    for url in [
        "http://fixture.example/file",
        "https://127.0.0.1/file",
        "https://unapproved.example/file",
    ] {
        let mut file = input("unsafe.txt", "text/plain");
        file["download_url"] = json!(url);
        let result = call(
            &h,
            &installed.url,
            &token,
            &session,
            "knowledge_upload_file",
            json!({"target":PAGE,"file":file}),
        )
        .await;
        assert_eq!(
            result["result"]["structuredContent"]["files"][0]["status"], "failed",
            "{result}"
        );
    }
    let outside=call(&h,&installed.url,&token,&session,"knowledge_upload_file",json!({"target":"dddddddd-dddd-dddd-dddd-dddddddddddd","file":input("outside.txt","text/plain")})).await;
    assert!(
        outside["result"]["isError"] == true
            && outside["result"]["content"][0]["text"] == "file_upload_denied",
        "{outside}"
    );
    h.login.flow.revoke("chatgpt", &token).unwrap();
    let denied=rpc(&h,&installed.url,&token,Some(&session),json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"file":input("revoked.txt","text/plain")}}})).await;
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(source.count.load(Ordering::SeqCst), 0);
    assert_eq!(h.files.created.load(Ordering::SeqCst), 0);
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 0);
    source.cleaned();
    installed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_cancelled_download_retains_orphan_and_configured_operator_recovers_without_upload()
 {
    let h = harness().await;
    let source = Source::new(vec![(b"cancelled file".to_vec(), 200)]).await;
    source.pause.store(true, Ordering::SeqCst);
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let mut config = settings(&h, &seed, &source);
    initialize(&config);
    let past = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        - 121;
    config.file_ingestion.as_mut().unwrap().source_clock = Some(Arc::new(move || past));
    let model = Arc::new(Model {
        inner: seed.provider.clone(),
        fail: false.into(),
    });
    let installed = start(&h, config, model.clone()).await;
    let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
    let pending = rpc(
        &h,
        &installed.url,
        &token,
        Some(&session),
        json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"file":input("cancel.txt","text/plain")}}}),
    );
    let cancel = async {
        tokio::time::timeout(Duration::from_secs(5), source.entered.notified())
            .await
            .unwrap();
        assert!(rpc(&h,&installed.url,&token,Some(&session),json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3,"reason":"cancel before upload"}})).await.status().is_success());
        source.release.add_permits(1);
    };
    let (response, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(pending, cancel)
    })
    .await
    .unwrap();
    // Cancellation can close the HTTP response; its receipt is not required for recovery.
    drop(response);
    installed.stop().await;
    let config = settings(&h, &seed, &source);
    let runtime = h.login.callback.as_ref().unwrap().credentials.clone();
    let list = crate::file_recovery_command::run_fixture(
        &config,
        &["--file-recovery".into(), "list".into()],
        runtime.clone(),
    )
    .await
    .unwrap();
    let receipts = list["held_intents"]
        .as_array()
        .expect("bounded orphan receipt list");
    assert_eq!(receipts.len(), 1, "{list}");
    assert_eq!(receipts[0]["source_intent"], "recovery_required");
    assert_ne!(receipts[0]["index_status"], "completed");
    let receipt = receipts[0]["refresh"].clone();
    let args = |action: &str| {
        vec![
            "--file-recovery".into(),
            action.into(),
            "--page".into(),
            PAGE.into(),
            "--digest".into(),
            receipt["mutation_digest"].as_str().unwrap().into(),
        ]
    };
    let current =
        crate::file_recovery_command::run_fixture(&config, &args("status"), runtime.clone())
            .await
            .unwrap();
    assert_eq!(current["source_intent"], "recovery_required");
    let mut wrong = settings(&h, &seed, &source);
    wrong.notion_scope.as_mut().unwrap().generation += 1;
    let reads = h.page_reads.load(Ordering::SeqCst);
    assert!(
        crate::file_recovery_command::run_fixture(&wrong, &args("status"), runtime.clone())
            .await
            .is_err()
    );
    assert_eq!(h.page_reads.load(Ordering::SeqCst), reads);

    assert!(
        crate::file_recovery_command::run_fixture(&config, &args("release"), runtime.clone())
            .await
            .is_err()
    );
    let mut release = args("release");
    release.extend([
        "--all-source-owners-stopped".into(),
        "--provider-quiescent".into(),
    ]);
    let released = crate::file_recovery_command::run_fixture(&config, &release, runtime)
        .await
        .unwrap();
    assert_eq!(released["source_replayed"], false);
    let installed = start(&h, settings(&h, &seed, &source), model).await;
    let session = connect(&h, &installed.url, &token).await;
    wait_status(&h, &installed, &token, &session, &receipt, "completed").await;
    assert_eq!(h.files.created.load(Ordering::SeqCst), 0);
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 0);
    assert_eq!(source.count.load(Ordering::SeqCst), 1);
    source.cleaned();
    installed.stop().await;
    h.approved_code().await;
    let reads = h.page_reads.load(Ordering::SeqCst);
    assert!(
        crate::file_recovery_command::run_fixture(
            &config,
            &args("status"),
            h.login.callback.as_ref().unwrap().credentials.clone()
        )
        .await
        .is_err()
    );
    assert_eq!(h.page_reads.load(Ordering::SeqCst), reads);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_inflight_attachment_loss_retains_uncertainty_and_never_attempts_second_file() {
    for invalidation in ["cancel", "logout", "replacement"] {
        let h = harness().await;
        let source = Source::new(vec![
            (b"accepted file".to_vec(), 200),
            (b"must remain unattempted".to_vec(), 200),
        ])
        .await;
        let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let config = settings(&h, &seed, &source);
        initialize(&config);
        let installed = start(
            &h,
            config,
            Arc::new(Model {
                inner: seed.provider.clone(),
                fail: false.into(),
            }),
        )
        .await;
        let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
        h.files.pause_attach.store(true, Ordering::SeqCst);
        let pending = rpc(
            &h,
            &installed.url,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"files":[input("accepted.txt","text/plain"),input("unattempted.txt","text/plain")]}}}),
        );
        let invalidate = async {
            tokio::time::timeout(Duration::from_secs(5), h.files.entered.notified())
                .await
                .unwrap();
            match invalidation {
                "cancel" => {
                    assert!(rpc(&h,&installed.url,&token,Some(&session),json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":3,"reason":"cancel accepted effect"}})).await.status().is_success());
                }
                "logout" => {
                    h.login.flow.revoke("chatgpt", &token).unwrap();
                }
                _ => {
                    h.approved_code().await;
                }
            }
            h.files.release.add_permits(1);
        };
        let (response, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(pending, invalidate)
        })
        .await
        .unwrap();
        if let Ok(body) = response.bytes().await
            && let Ok(reply) = serde_json::from_slice::<Value>(&body)
        {
            assert_ne!(
                reply["result"]["structuredContent"]["files"][0]["status"], "verified",
                "{invalidation}: {reply}"
            );
        }
        // An effect accepted while authority was valid can finish after cancellation.
        tokio::time::timeout(Duration::from_secs(5), async {
            while h.files.blocks.lock().unwrap().len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(h.files.created.load(Ordering::SeqCst), 1, "{invalidation}");
        assert_eq!(h.files.sent.load(Ordering::SeqCst), 1);
        assert_eq!(h.files.attached.load(Ordering::SeqCst), 1);
        assert_eq!(source.count.load(Ordering::SeqCst), 1);
        let db = rusqlite::Connection::open(&seed.state).unwrap();
        let held: i64 = db
            .query_row(
                "SELECT count(*) FROM file_refresh_intents WHERE phase='held'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            held, 1,
            "accepted effect must not be falsely completed: {invalidation}"
        );
        source.cleaned();
        installed.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_partial_batch_preserves_verified_source_and_quarantines_upload_only_uncertainty()
{
    let h = harness().await;
    let source = Source::new(vec![
        (b"first verified".to_vec(), 200),
        (b"second ambiguous".to_vec(), 200),
    ])
    .await;
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let config = settings(&h, &seed, &source);
    initialize(&config);
    let installed = start(
        &h,
        config,
        Arc::new(Model {
            inner: seed.provider.clone(),
            fail: false.into(),
        }),
    )
    .await;
    let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
    h.files.fail_send.store(2, Ordering::SeqCst);
    let response=call(&h,&installed.url,&token,&session,"knowledge_upload_file",json!({"target":PAGE,"files":[input("first.txt","text/plain"),input("ambiguous.txt","text/plain")]})).await;
    let output = &response["result"]["structuredContent"];
    assert_eq!(output["files"][0]["status"], "verified", "{response}");
    assert_eq!(output["files"][0]["index"], 0);
    assert_eq!(output["files"][1]["index"], 1);
    assert_eq!(output["files"][1]["status"], "indeterminate", "{response}");
    assert!(output["files"][1]["upload_id"].is_string());
    assert!(output["files"][1]["block_id"].is_null());
    assert_eq!(output["partial_success"], true);
    assert_eq!(output["files"][0]["index_status"], "held");
    assert_eq!(output["files"][0]["refresh"], output["files"][1]["refresh"]);
    assert_eq!(h.files.created.load(Ordering::SeqCst), 2);
    assert_eq!(h.files.sent.load(Ordering::SeqCst), 2);
    assert_eq!(h.files.attached.load(Ordering::SeqCst), 1);
    source.cleaned();
    installed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_file_discovery_and_dispatch_respect_each_exact_capability() {
    for (allow, block, upload, refresh) in [
        (Some("knowledge_upload_file"), None, true, false),
        (Some("knowledge_file_refresh"), None, false, true),
        (Some("write"), Some("knowledge_file_refresh"), true, false),
    ] {
        let h = harness().await;
        let source = Source::new(vec![]).await;
        let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
        let mut config = settings(&h, &seed, &source);
        initialize(&config);
        config.capability_policy =
            notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(allow, block).unwrap();
        let installed = start(
            &h,
            config,
            Arc::new(Model {
                inner: seed.provider.clone(),
                fail: false.into(),
            }),
        )
        .await;
        let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
        let listed = resource::reply(
            rpc(
                &h,
                &installed.url,
                &token,
                Some(&session),
                json!({"jsonrpc":"2.0","id":9,"method":"tools/list"}),
            )
            .await,
        )
        .await;
        let names: Vec<_> = listed["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect();
        assert_eq!(names.contains(&"knowledge_upload_file"), upload, "{listed}");
        assert_eq!(
            names.contains(&"knowledge_file_refresh"),
            refresh,
            "{listed}"
        );
        let (name, args) = if !upload {
            (
                "knowledge_upload_file",
                json!({"target":PAGE,"file":input("blocked.txt","text/plain")}),
            )
        } else {
            (
                "knowledge_file_refresh",
                json!({"page_id":PAGE,"mutation_digest":"0".repeat(64),"retry":true}),
            )
        };
        let blocked = call(&h, &installed.url, &token, &session, name, args).await;
        assert!(blocked["error"].is_object(), "{blocked}");
        assert_eq!(source.count.load(Ordering::SeqCst), 0);
        assert_eq!(h.files.created.load(Ordering::SeqCst), 0);
        installed.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_oauth_files_remain_absent_without_explicit_opt_in() {
    let h = harness().await;
    let source = Source::new(vec![]).await;
    let seed = content_index::seed(h._dir.path(), &scope(), PAGE).await;
    let mut config = settings(&h, &seed, &source);
    initialize(&config);
    config.file_ingestion = None;
    let installed = start(
        &h,
        config,
        Arc::new(Model {
            inner: seed.provider.clone(),
            fail: false.into(),
        }),
    )
    .await;
    let (token, session) = login(&h, &installed.url, "knowledge:read knowledge:write").await;
    let listed = resource::reply(
        rpc(
            &h,
            &installed.url,
            &token,
            Some(&session),
            json!({"jsonrpc":"2.0","id":9,"method":"tools/list"}),
        )
        .await,
    )
    .await;
    for name in ["knowledge_upload_file", "knowledge_file_refresh"] {
        assert!(!listed.to_string().contains(name), "{listed}");
    }
    let denied = call(
        &h,
        &installed.url,
        &token,
        &session,
        "knowledge_upload_file",
        json!({"target":PAGE,"file":input("disabled.txt","text/plain")}),
    )
    .await;
    assert!(denied["error"].is_object(), "{denied}");
    assert_eq!(source.count.load(Ordering::SeqCst), 0);
    assert_eq!(h.files.created.load(Ordering::SeqCst), 0);
    installed.stop().await;
}
