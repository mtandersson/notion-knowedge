use super::*;
use notion_knowledge_core::{
    audit::{AuditActor, AuditError, AuditEvent, AuditRetention, AuditStore},
    discovery::ExclusionRules,
    file_ingest::{FileIngestContext, FileIngestRequest, IngestStatus, ingest_file},
    root_scope::RootScopeGate,
};
use notion_knowledge_retrieval::audit::SqliteAuditStore;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const OUTSIDE: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
const UPLOAD: &str = "55555555-5555-5555-5555-555555555555";
const BLOCK: &str = "66666666-6666-6666-6666-666666666666";
const SECRET_URL: &str = "https://fixture.example/file?token=signed-secret";
#[derive(Default)]
struct SourceCount {
    value: AtomicUsize,
    changed: tokio::sync::Notify,
}
impl SourceCount {
    fn load(&self, order: Ordering) -> usize {
        self.value.load(order)
    }
    fn record(&self) {
        self.value.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
    async fn wait(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.load(Ordering::SeqCst) > 0 {
                return;
            }
            changed.await;
        }
    }
}
type RecordedRequest = (String, Vec<u8>);
#[derive(Default)]
struct Requests {
    entries: Mutex<Vec<RecordedRequest>>,
    changed: tokio::sync::Notify,
}
impl Requests {
    fn lock(&self) -> std::sync::LockResult<std::sync::MutexGuard<'_, Vec<RecordedRequest>>> {
        self.entries.lock()
    }
    async fn wait_for(&self, matches: impl Fn(&[RecordedRequest]) -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if matches(&self.lock().unwrap()) {
                return;
            }
            changed.await;
        }
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
fn chain(parent: &str) -> Vec<Value> {
    let node = |id: &str, parent: Option<&str>| json!({"object":"page","id":id,"last_edited_time":"2026-10-11T09:00:00Z","in_trash":false,"parent": parent.map(|v| json!({"type":"page_id","page_id":v})).unwrap_or(json!({"type":"workspace","workspace":true}))});
    vec![
        node(PAGE, Some(parent)),
        node(parent, None),
        node(parent, None),
        node(PAGE, Some(parent)),
    ]
}
fn block(image: bool, readback: bool) -> Value {
    let kind = if image { "image" } else { "file" };
    let mut value = json!({"object":"block","id":BLOCK,"type":kind,"in_trash":false,"parent":{"type":"page_id","page_id":PAGE}});
    if readback {
        value[kind] = json!({"type":"file_upload","file_upload":{"id":UPLOAD},"caption":[{"type":"text","plain_text":"Safe caption"}],"name":"document.txt"});
    }
    value
}
async fn read_request<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> (String, Vec<u8>) {
    let mut wire = Vec::new();
    let (head, end, length) = loop {
        let mut buf = [0; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        assert!(n > 0);
        wire.extend_from_slice(&buf[..n]);
        if let Some(end) = wire.windows(4).position(|v| v == b"\r\n\r\n") {
            let head = String::from_utf8(wire[..end].to_vec()).unwrap();
            let length = head
                .to_ascii_lowercase()
                .lines()
                .find_map(|v| v.strip_prefix("content-length: ")?.parse::<usize>().ok());
            break (head, end + 4, length);
        }
    };
    if let Some(length) = length {
        while wire.len() < end + length {
            let mut buf = [0; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            wire.extend_from_slice(&buf[..n]);
        }
    } else if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        while !wire[end..].windows(5).any(|v| v == b"0\r\n\r\n") {
            let mut buf = [0; 4096];
            let n = stream.read(&mut buf).await.unwrap();
            assert!(n > 0);
            wire.extend_from_slice(&buf[..n]);
        }
    }
    (head, wire[end..].to_vec())
}
async fn source(
    bytes: Vec<u8>,
    status: u16,
    slow: bool,
) -> (
    DownloadPolicy,
    Arc<SourceCount>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<std::path::PathBuf>>>,
) {
    source_many(vec![(bytes, status, slow)], 64).await
}
async fn source_many(
    files: Vec<(Vec<u8>, u16, bool)>,
    maximum: usize,
) -> (
    DownloadPolicy,
    Arc<SourceCount>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<std::path::PathBuf>>>,
) {
    let certificate = include_bytes!("../../tests/fixtures/ingest-cert.pem");
    let key = PrivateKeyDer::Pkcs8(
        include_bytes!("../../tests/fixtures/ingest-key.der")
            .to_vec()
            .into(),
    );
    let tls = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_slice(certificate).unwrap()],
            key,
        )
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let paths = Arc::new(Mutex::new(Vec::new()));
    let policy = DownloadPolicy::new(&["fixture.example"], maximum, Duration::from_secs(20))
        .unwrap()
        .with_tls_fixture(
            listener.local_addr().unwrap(),
            reqwest::Certificate::from_pem(certificate).unwrap(),
            paths.clone(),
        );
    let count = Arc::new(SourceCount::default());
    let observed = count.clone();
    let handle = tokio::spawn(async move {
        for (bytes, status, slow) in files {
            let (socket, _) = listener.accept().await.unwrap();
            let mut tls = TlsAcceptor::from(Arc::new(tls.clone()))
                .accept(socket)
                .await
                .unwrap();
            let (headers, _) = read_request(&mut tls).await;
            assert!(headers.starts_with("GET /file?token=signed-secret "));
            assert!(!headers.to_ascii_lowercase().contains("authorization:"));
            observed.record();
            tls.write_all(
                format!(
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    bytes.len()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
            if slow {
                std::future::pending::<()>().await;
            }
            for chunk in bytes.chunks(16384) {
                if tls.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
            // Async TLS writes may retain the final encrypted record after
            // write_all returns. Flush it and send close_notify before dropping
            // the socket; otherwise a complete fixture body can be truncated.
            // Cancellation/error cases may already have closed the client.
            let _ = tls.shutdown().await;
        }
    });
    (policy, count, handle, paths)
}
async fn notion(
    steps: Vec<(u16, Value)>,
) -> (NotionClient, Arc<Requests>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = NotionClient::integration("fixture-secret")
        .unwrap()
        .with_fast_fixture_transport();
    client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Requests::default());
    let observed = requests.clone();
    let handle = tokio::spawn(async move {
        for (status, value) in steps {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(
                request
                    .0
                    .to_ascii_lowercase()
                    .contains("authorization: bearer fixture-secret")
            );
            observed.lock().unwrap().push(request);
            observed.changed.notify_waiters();
            if status == 0 {
                std::future::pending::<()>().await;
            }
            let body = value.to_string();
            socket.write_all(format!("HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
    });
    (client, requests, handle)
}
fn request(image: bool) -> FileIngestRequest {
    FileIngestRequest {
        target: PageId(PAGE.into()),
        temporary_url: SECRET_URL.into(),
        filename: Some(
            if image {
                "../../image.png"
            } else {
                "document.txt"
            }
            .into(),
        ),
        declared_mime: Some(if image { "image/png" } else { "text/plain" }.into()),
        caption: Some(" Safe caption ".into()),
    }
}
fn full_steps(image: bool, readback_fails: bool, send_fails: bool) -> Vec<(u16, Value)> {
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
    steps.push((
        200,
        json!({"object":"file_upload","id":UPLOAD,"status":"pending"}),
    ));
    steps.push((
        if send_fails { 500 } else { 200 },
        json!({"object":"file_upload","id":UPLOAD,"status":"uploaded"}),
    ));
    if !send_fails {
        // Application permit revalidation, then native attachment's independent
        // authorize/revalidate: all are real authoritative page HTTP reads.
        for _ in 0..3 {
            steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
        }
        steps.push((200, json!({"object":"list","results":[block(image,false)]})));
        for _ in 0..if readback_fails { 3 } else { 1 } {
            steps.push((if readback_fails { 404 } else { 200 }, block(image, true)));
        }
    }
    steps
}
struct AuditFixture {
    store: SqliteAuditStore,
    fail_at: Option<usize>,
    calls: AtomicUsize,
}
impl AuditStore for AuditFixture {
    fn record(&self, event: &AuditEvent, time: i64) -> Result<(), AuditError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_at == Some(call) {
            Err(AuditError::Unavailable)
        } else {
            self.store.record(event, time)
        }
    }
    fn prune(&self, time: i64) -> Result<u64, AuditError> {
        self.store.prune(time)
    }
}
fn audit(path: &std::path::Path, fail_at: Option<usize>) -> AuditFixture {
    AuditFixture {
        store: SqliteAuditStore::open(path, AuditRetention::default()).unwrap(),
        fail_at,
        calls: AtomicUsize::new(0),
    }
}
fn context<'a>(gate: &'a RootScopeGate, audit: &'a dyn AuditStore) -> FileIngestContext<'a> {
    FileIngestContext {
        gate,
        audit,
        actor: AuditActor::Server,
        correlation_id: "request_0123456789abcdef",
        permitted: true,
    }
}

#[tokio::test]
async fn native_image_and_document_are_verified_and_durably_audited_without_payloads() {
    for image in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit.sqlite");
        let audit = audit(&path, None);
        let bytes = if image {
            b"\x89PNG\r\n\x1a\nfixture".to_vec()
        } else {
            b"document fixture".to_vec()
        };
        let (download, count, source_task, paths) = source(bytes.clone(), 200, false).await;
        let (client, requests, notion_task) = notion(full_steps(image, false, false)).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(image)).await;
        assert_eq!(result.status, IngestStatus::Verified, "{result:?}");
        assert!(result.terminal_audited);
        assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
        assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        let (_, body) = requests
            .iter()
            .find(|(h, _)| h.starts_with("PATCH"))
            .unwrap();
        let sent: Value = serde_json::from_slice(body).unwrap();
        let kind = if image { "image" } else { "file" };
        assert_eq!(sent["children"][0]["type"], kind);
        assert_eq!(sent["children"][0][kind]["file_upload"]["id"], UPLOAD);
        assert_eq!(
            sent["children"][0][kind]["caption"][0]["text"]["content"],
            "Safe caption"
        );
        let db = rusqlite::Connection::open(&path).unwrap();
        let outcomes: Vec<String> = db
            .prepare("SELECT outcome FROM agent_audit_events ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(outcomes, ["attempted", "attempted", "succeeded"]);
        drop(db);
        drop(audit);
        let persisted = std::fs::read(path).unwrap();
        for secret in [
            b"signed-secret".as_slice(),
            b"fixture-secret",
            b"Safe caption",
            bytes.as_slice(),
            b"document.txt",
        ] {
            assert!(!persisted.windows(secret.len()).any(|v| v == secret));
        }
    }
}

fn assert_clean(paths: &Arc<Mutex<Vec<std::path::PathBuf>>>) {
    let paths = paths.lock().unwrap();
    assert!(!paths.is_empty());
    assert!(paths.iter().all(|p| !p.exists()));
}

#[tokio::test]
async fn scope_denial_and_mismatched_policies_precede_every_file_effect() {
    for mismatched in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, count, source_task, paths) = source(b"document".to_vec(), 200, false).await;
        let (client, requests, notion_task) = notion(if mismatched {
            vec![]
        } else {
            chain(OUTSIDE).into_iter().map(|v| (200, v)).collect()
        })
        .await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let mut adapter_scope = scope();
        if mismatched {
            adapter_scope.generation += 1;
        }
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            adapter_scope,
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
        assert_eq!(result.status, IngestStatus::Denied);
        assert!(result.terminal_audited);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(paths.lock().unwrap().is_empty());
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(h, _)| h.starts_with("GET"))
        );
        source_task.abort();
        notion_task.await.unwrap();
    }
}
#[tokio::test]
async fn mime_size_and_expired_sources_fail_before_notion_upload_and_clean_up() {
    for (bytes, status, limit, failure) in [
        (
            b"not an image".to_vec(),
            200,
            64,
            IngestFailure::InvalidInput,
        ),
        (
            b"\x89PNG\r\n\x1a\nfixture".to_vec(),
            200,
            8,
            IngestFailure::InvalidInput,
        ),
        (vec![0; 65], 200, 64, IngestFailure::InvalidInput),
        (b"expired".to_vec(), 403, 64, IngestFailure::SourceExpired),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, _, source_task, paths) = source(bytes, status, false).await;
        let (client, requests, notion_task) =
            notion(chain(ROOT).into_iter().map(|v| (200, v)).collect()).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(limit).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(true)).await;
        assert_eq!(result.status, IngestStatus::Failed);
        assert_eq!(result.failure, Some(failure));
        assert!(result.terminal_audited);
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(h, _)| h.starts_with("GET"))
        );
    }
}
#[tokio::test]
async fn upstream_and_readback_ambiguity_keep_known_references_without_write_replay() {
    for send_fails in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, _, source_task, paths) =
            source(b"document fixture".to_vec(), 200, false).await;
        let (client, requests, notion_task) =
            notion(full_steps(false, !send_fails, send_fails)).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
        assert_eq!(result.status, IngestStatus::Indeterminate);
        assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
        assert_eq!(
            result.references.block_id.as_deref(),
            if send_fails { None } else { Some(BLOCK) }
        );
        assert!(result.terminal_audited);
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        let requests = requests.lock().unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            usize::from(!send_fails)
        );
        assert_eq!(
            requests
                .iter()
                .filter(|(h, _)| h.starts_with("POST /v1/file_uploads "))
                .count(),
            1
        );
    }
}
#[tokio::test]
async fn unavailable_audit_prevents_effects_or_preserves_verified_but_unaudited_receipt() {
    for fail_at in [1, 2, 3] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), Some(fail_at));
        let (download, count, source_task, paths) =
            source(b"document fixture".to_vec(), 200, false).await;
        let steps = match fail_at {
            1 => vec![],
            2 => chain(ROOT).into_iter().map(|v| (200, v)).collect(),
            _ => full_steps(false, false, false),
        };
        let (client, requests, notion_task) = notion(steps).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
        assert_eq!(result.failure, Some(IngestFailure::AuditUnavailable));
        if fail_at == 3 {
            assert_eq!(result.status, IngestStatus::VerifiedAuditFailed);
            assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
            assert!(!result.terminal_audited);
        } else {
            assert_eq!(result.status, IngestStatus::Failed);
            assert!(
                requests
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|(h, _)| h.starts_with("GET"))
            );
        }
        if fail_at == 1 {
            assert_eq!(count.load(Ordering::SeqCst), 0);
            source_task.abort();
        } else {
            source_task.await.unwrap();
            assert_clean(&paths);
        }
        notion_task.await.unwrap();
    }
}
#[tokio::test]
async fn changed_target_authority_after_download_prevents_upload() {
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, _, source_task, paths) = source(b"document fixture".to_vec(), 200, false).await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(chain(OUTSIDE).into_iter().map(|v| (200, v)));
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter = NotionFileIngestAdapter::new(
        client,
        download,
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
    assert_eq!(result.status, IngestStatus::Denied);
    assert!(result.references.upload_id.is_none());
    assert!(
        requests
            .lock()
            .unwrap()
            .iter()
            .all(|(h, _)| h.starts_with("GET"))
    );
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.await.unwrap();
}
#[tokio::test]
async fn cancelled_or_timed_out_download_removes_temporary_bytes_and_records_failed_attempt() {
    let _clock = crate::test_clock::TestClock::new().await;
    for cancel in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit");
        let audit = audit(&path, None);
        let (download, count, source_task, paths) =
            source(b"document fixture".to_vec(), 200, true).await;
        let (client, _, notion_task) =
            notion(chain(ROOT).into_iter().map(|v| (200, v)).collect()).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let ctx = context(&gate, &audit);
        let mut future = Box::pin(ingest_file(&adapter, &ctx, request(false)));
        tokio::select! {
            result = &mut future => panic!("download should still be pending: {result:?}"),
            _ = count.wait() => {}
        }
        if cancel {
            drop(future);
        } else {
            crate::test_clock::TestClock::advance(Duration::from_secs(20)).await;
            let result = future.await;
            assert_eq!(result.failure, Some(IngestFailure::DownloadUnavailable));
        }
        assert_clean(&paths);
        source_task.abort();
        notion_task.await.unwrap();
        let db = rusqlite::Connection::open(path).unwrap();
        let terminal: String = db
            .query_row(
                "SELECT outcome FROM agent_audit_events ORDER BY id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(terminal, "failed");
    }
}

#[tokio::test]
async fn omitted_file_hints_use_observed_type_and_safe_fallback_name() {
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, _, source_task, paths) =
        source(b"\x89PNG\r\n\x1a\nfixture".to_vec(), 200, false).await;
    let (client, requests, notion_task) = notion(full_steps(true, false, false)).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter = NotionFileIngestAdapter::new(
        client,
        download,
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    let mut req = request(true);
    req.filename = None;
    req.declared_mime = None;
    let result = ingest_file(&adapter, &context(&gate, &audit), req).await;
    assert_eq!(result.status, IngestStatus::Verified);
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_clean(&paths);
    let requests = requests.lock().unwrap();
    let (_, body) = requests
        .iter()
        .find(|(h, _)| h.starts_with("POST /v1/file_uploads "))
        .unwrap();
    let create: Value = serde_json::from_slice(body).unwrap();
    assert_eq!(create["filename"], "uploaded.png");
    assert_eq!(create["content_type"], "image/png");
}
#[tokio::test]
async fn scope_change_after_upload_denies_attachment_and_keeps_upload_reference() {
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, _, source_task, paths) = source(b"document fixture".to_vec(), 200, false).await;
    let mut steps = full_steps(false, false, true);
    steps[9].0 = 200;
    steps.extend(chain(OUTSIDE).into_iter().map(|v| (200, v)));
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter = NotionFileIngestAdapter::new(
        client,
        download,
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
    assert_eq!(result.status, IngestStatus::Denied);
    assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
    assert!(result.references.block_id.is_none());
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_clean(&paths);
    assert!(
        requests
            .lock()
            .unwrap()
            .iter()
            .all(|(h, _)| !h.starts_with("PATCH"))
    );
}

#[tokio::test]
async fn cancellation_during_upload_cleans_up_and_audits_indeterminate_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("audit");
    let audit = audit(&path, None);
    let (download, _, source_task, paths) = source(b"document fixture".to_vec(), 200, false).await;
    let mut steps = full_steps(false, false, true);
    steps[9].0 = 0;
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter = NotionFileIngestAdapter::new(
        client,
        download,
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    let ctx = context(&gate, &audit);
    let mut future = Box::pin(ingest_file(&adapter, &ctx, request(false)));
    tokio::select! { _= &mut future => panic!("upload should still be pending"), _=requests.wait_for(|entries| entries.iter().any(|(h,_)|h.starts_with(&format!("POST /v1/file_uploads/{UPLOAD}/send"))))=>{} }
    drop(future);
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.abort();
    let db = rusqlite::Connection::open(path).unwrap();
    let terminal: String = db
        .query_row(
            "SELECT outcome FROM agent_audit_events ORDER BY id DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(terminal, "indeterminate");
    assert_eq!(
        requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(h, _)| h.starts_with("POST /v1/file_uploads "))
            .count(),
        1
    );
}
#[tokio::test]
async fn failed_terminal_audit_preserves_upstream_ambiguity_and_known_upload() {
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), Some(3));
    let (download, _, source_task, paths) = source(b"document fixture".to_vec(), 200, false).await;
    let (client, _, notion_task) = notion(full_steps(false, false, true)).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter = NotionFileIngestAdapter::new(
        client,
        download,
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
    assert_eq!(result.status, IngestStatus::Indeterminate);
    assert_eq!(
        result.failure,
        Some(IngestFailure::Upstream(
            notion_knowledge_core::backend::BackendErrorKind::Unavailable
        ))
    );
    assert!(!result.terminal_audited);
    assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.await.unwrap();
}

#[tokio::test]
async fn invalid_targets_denied_operation_and_unsafe_caption_precede_file_effects() {
    for case in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, count, source_task, paths) = source(b"document".to_vec(), 200, false).await;
        let (client, requests, notion_task) = notion(if case == 2 {
            chain(ROOT).into_iter().map(|v| (200, v)).collect()
        } else {
            vec![]
        })
        .await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let mut ctx = context(&gate, &audit);
        let mut req = request(false);
        match case {
            0 => req.target = PageId(SECRET_URL.into()),
            1 => ctx.permitted = false,
            _ => req.caption = Some("\u{202e}unsafe caption".into()),
        }
        let result = ingest_file(&adapter, &ctx, req).await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(paths.lock().unwrap().is_empty());
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(h, _)| h.starts_with("GET"))
        );
        assert!(!format!("{result:?}").contains("signed-secret"));
        if case == 0 {
            assert_eq!(result.failure, Some(IngestFailure::InvalidInput));
            assert!(result.references.target.is_none());
        } else if case == 1 {
            assert_eq!(result.status, IngestStatus::Denied);
        } else {
            assert_eq!(result.failure, Some(IngestFailure::InvalidInput));
        }
        source_task.abort();
        notion_task.await.unwrap();
    }
}

#[tokio::test]
async fn optional_hints_preserve_supported_text_extensions_without_overriding_binary_evidence() {
    let adapter = NotionFileIngestAdapter::new(
        NotionClient::integration("fixture-secret").unwrap(),
        DownloadPolicy::new(&["fixture.example"], 64, Duration::from_secs(20)).unwrap(),
        FileValidationPolicy::for_single_part(64).unwrap(),
        scope(),
    );
    for (name, bytes, want) in [
        (
            Some("notes.md"),
            b"# plain UTF-8".as_slice(),
            "text/markdown",
        ),
        (Some("data.csv"), b"a,b\n1,2", "text/csv"),
        (None, b"plain UTF-8", "text/plain"),
    ] {
        let file = DownloadedFile::test_fixture(bytes);
        assert_eq!(
            adapter
                .validated(&file, name, None)
                .await
                .unwrap()
                .mime_type,
            want
        );
    }
    let binary = DownloadedFile::test_fixture(b"\x89PNG\r\n\x1a\nfixture");
    assert_eq!(
        adapter.validated(&binary, Some("notes.md"), None).await,
        Err(IngestFailure::InvalidInput)
    );
}

fn hosted_steps(image: bool, mismatch: Option<&str>, mixed: bool) -> Vec<(u16, Value)> {
    let kind = if image { "image" } else { "file" };
    let hosted = |url: &str| json!({"type":"file","file":{"url":url,"expiry_time":"2026-10-14T12:00:00Z"},"caption":[{"type":"text","plain_text":"Safe caption"}],"name":"document.txt"});
    let mut steps = full_steps(image, mismatch.is_some() || mixed, false);
    for (status, value) in &mut steps {
        if value["object"] == "list" {
            value["results"][0][kind] = if mixed {
                json!({"type":"file_upload","file_upload":{"id":UPLOAD}})
            } else {
                hosted(
                    "https://files.notion.so/private-resource?versionId=original&X-Amz-Signature=append-signature&X-Amz-Expires=3600",
                )
            };
        } else if value["object"] == "block" {
            *status = 200;
            value[kind]=hosted(mismatch.unwrap_or("https://files.notion.so/private-resource?X-Amz-Signature=rotated-signature&versionId=original&X-Amz-Expires=1800"));
        }
    }
    steps
}

#[tokio::test]
async fn hosted_image_and_document_readback_match_append_resource_with_rotated_signature() {
    for image in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("audit");
        let audit = audit(&path, None);
        let bytes = if image {
            b"\x89PNG\r\n\x1a\nfixture".to_vec()
        } else {
            b"document fixture".to_vec()
        };
        let (download, _, source_task, paths) = source(bytes, 200, false).await;
        let (client, requests, notion_task) = notion(hosted_steps(image, None, false)).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(image)).await;
        assert_eq!(result.status, IngestStatus::Verified, "{result:?}");
        assert!(result.terminal_audited);
        assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
        assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            1
        );
        let result_debug = format!("{result:?}");
        drop(audit);
        let persisted = std::fs::read(path).unwrap();
        for secret in [
            "private-resource",
            "append-signature",
            "rotated-signature",
            "files.notion.so",
        ] {
            assert!(!result_debug.contains(secret));
            assert!(
                !persisted
                    .windows(secret.len())
                    .any(|v| v == secret.as_bytes())
            );
        }
    }
}

#[tokio::test]
async fn different_hosted_file_at_same_block_remains_indeterminate_with_known_refs_and_no_replay() {
    for image in [true, false] {
        for different in [
            "https://files.notion.so/replacement?versionId=original&X-Amz-Signature=rotated",
            "https://different.notion.so/private-resource?versionId=original&X-Amz-Signature=rotated",
            "https://files.notion.so/private-resource?versionId=replacement&X-Amz-Signature=rotated",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("audit");
            let audit = audit(&path, None);
            let bytes = if image {
                b"\x89PNG\r\n\x1a\nfixture".to_vec()
            } else {
                b"document fixture".to_vec()
            };
            let (download, _, source_task, paths) = source(bytes, 200, false).await;
            let (client, requests, notion_task) =
                notion(hosted_steps(image, Some(different), false)).await;
            let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
            let adapter = NotionFileIngestAdapter::new(
                client,
                download,
                FileValidationPolicy::for_single_part(64).unwrap(),
                scope(),
            );
            let result = ingest_file(&adapter, &context(&gate, &audit), request(image)).await;
            assert_eq!(result.status, IngestStatus::Indeterminate);
            assert!(result.terminal_audited);
            assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
            assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
            assert_clean(&paths);
            source_task.await.unwrap();
            notion_task.await.unwrap();
            let requests = requests.lock().unwrap();
            assert_eq!(
                requests
                    .iter()
                    .filter(|(h, _)| h.starts_with("PATCH"))
                    .count(),
                1
            );
            assert_eq!(
                requests
                    .iter()
                    .filter(|(h, _)| h.starts_with(&format!("GET /v1/blocks/{BLOCK}")))
                    .count(),
                3
            );
            let db = rusqlite::Connection::open(path).unwrap();
            let terminal: String = db
                .query_row(
                    "SELECT outcome FROM agent_audit_events ORDER BY id DESC LIMIT 1",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(terminal, "indeterminate");
        }
    }
}

#[tokio::test]
async fn hosted_readback_without_append_resource_anchor_is_honestly_unverified() {
    for image in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let bytes = if image {
            b"\x89PNG\r\n\x1a\nfixture".to_vec()
        } else {
            b"document fixture".to_vec()
        };
        let (download, _, source_task, paths) = source(bytes, 200, false).await;
        let (client, requests, notion_task) = notion(hosted_steps(image, None, true)).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(image)).await;
        assert_eq!(result.status, IngestStatus::Indeterminate);
        assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
        assert!(result.terminal_audited);
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn hosted_receipt_followed_by_exact_upload_id_readback_verifies_the_same_native_file() {
    for image in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let bytes = if image {
            b"\x89PNG\r\n\x1a\nfixture".to_vec()
        } else {
            b"document fixture".to_vec()
        };
        let (download, _, source_task, paths) = source(bytes, 200, false).await;
        let mut steps = hosted_steps(image, None, false);
        let kind = if image { "image" } else { "file" };
        for (_, value) in &mut steps {
            if value["object"] == "block" {
                value[kind] = json!({"type":"file_upload","file_upload":{"id":UPLOAD},"caption":[{"type":"text","plain_text":"Safe caption"}],"name":"document.txt"});
            }
        }
        let (client, requests, notion_task) = notion(steps).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::for_single_part(64).unwrap(),
            scope(),
        );
        let result = ingest_file(&adapter, &context(&gate, &audit), request(image)).await;
        assert_eq!(result.status, IngestStatus::Verified);
        assert!(result.terminal_audited);
        assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
        assert_eq!(result.references.block_id.as_deref(), Some(BLOCK));
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn joined_paid_plan_selects_exact_threshold_and_verifies_multipart_bytes_in_order() {
    const MIB: usize = 1024 * 1024;
    for size in [20 * MIB, 20 * MIB + 1] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let bytes: Vec<u8> = (0..size)
            .map(|i| b'a' + ((i / (1024 * 1024)) % 26) as u8)
            .collect();
        let (download, count, source_task, paths) =
            source_many(vec![(bytes.clone(), 200, false)], 30 * MIB).await;
        let mut steps = full_steps(false, false, false);
        if size > 20 * MIB {
            let receipt = |status: &str, sent| json!({"object":"file_upload","id":UPLOAD,"status":status,"number_of_parts":{"total":3,"sent":sent}});
            steps.splice(
                8..10,
                vec![
                    (200, receipt("pending", 0)),
                    (200, receipt("pending", 1)),
                    (200, receipt("pending", 2)),
                    (200, receipt("pending", 3)),
                    (200, receipt("uploaded", 3)),
                    (200, receipt("uploaded", 3)),
                ],
            );
        }
        let (client, requests, notion_task) = notion(steps).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::default(),
            scope(),
        )
        .with_multipart(MultipartPolicy::new(30 * MIB, 20 * MIB, 10 * MIB).unwrap());
        let outcome = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
        assert_eq!(outcome.status, IngestStatus::Verified, "{outcome:?}");
        assert!(outcome.terminal_audited);
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        let created: Value = serde_json::from_slice(
            &requests
                .iter()
                .find(|(h, _)| h.starts_with("POST /v1/file_uploads "))
                .unwrap()
                .1,
        )
        .unwrap();
        assert_eq!(
            created["mode"],
            if size > 20 * MIB {
                "multi_part"
            } else {
                "single_part"
            }
        );
        let sends: Vec<_> = requests
            .iter()
            .filter(|(h, _)| h.contains("/send "))
            .collect();
        assert_eq!(sends.len(), if size > 20 * MIB { 3 } else { 1 });
        for (index, (_, body)) in sends.iter().enumerate() {
            let marker = b"name=\"file\"";
            let start = body
                .windows(marker.len())
                .position(|v| v == marker)
                .unwrap();
            let start = start
                + body[start..]
                    .windows(4)
                    .position(|v| v == b"\r\n\r\n")
                    .unwrap()
                + 4;
            let end = start
                + body[start..]
                    .windows(4)
                    .position(|v| v == b"\r\n--")
                    .unwrap();
            let expected = if size > 20 * MIB {
                &bytes[index * 10 * MIB..((index + 1) * 10 * MIB).min(size)]
            } else {
                &bytes[..]
            };
            assert_eq!(&body[start..end], expected);
            if size > 20 * MIB {
                let field = format!("name=\"part_number\"\r\n\r\n{}\r\n", index + 1);
                assert!(body.windows(field.len()).any(|v| v == field.as_bytes()));
            }
        }
    }
}

#[tokio::test]
async fn ordered_batch_reports_later_failure_and_preserves_verified_image_and_document() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, count, source_task, paths) = source_many(
        vec![
            (b"\x89PNG\r\n\x1a\nimage".to_vec(), 200, false),
            (b"document".to_vec(), 200, false),
            (b"bad png".to_vec(), 200, false),
        ],
        64,
    )
    .await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(true, false, false));
    const UPLOAD2: &str = "77777777-7777-7777-7777-777777777777";
    const BLOCK2: &str = "88888888-8888-8888-8888-888888888888";
    steps.extend(
        full_steps(false, false, false)
            .into_iter()
            .map(|(status, value)| {
                (
                    status,
                    serde_json::from_str(
                        &value
                            .to_string()
                            .replace(UPLOAD, UPLOAD2)
                            .replace(BLOCK, BLOCK2),
                    )
                    .unwrap(),
                )
            }),
    );
    steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope());
    let mut batch = FileIngestBatch::new(
        vec![request(true), request(false), request(true)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    batch
        .execute(&adapter, &context(&gate, &audit))
        .await
        .unwrap();
    assert_eq!(
        batch
            .results()
            .iter()
            .map(|r| (r.index, r.outcome.as_ref().unwrap().status))
            .collect::<Vec<_>>(),
        vec![
            (0, IngestStatus::Verified),
            (1, IngestStatus::Verified),
            (2, IngestStatus::Failed)
        ]
    );
    assert!(batch.partial_success());
    assert_clean(&paths);
    assert_eq!(
        batch.results()[0]
            .outcome
            .as_ref()
            .unwrap()
            .references
            .upload_id
            .as_deref(),
        Some(UPLOAD)
    );
    assert_eq!(
        batch.results()[1]
            .outcome
            .as_ref()
            .unwrap()
            .references
            .upload_id
            .as_deref(),
        Some(UPLOAD2)
    );
    assert_eq!(
        batch.results()[1]
            .outcome
            .as_ref()
            .unwrap()
            .references
            .block_id
            .as_deref(),
        Some(BLOCK2)
    );
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 3);
    assert!(
        batch
            .execute(&adapter, &context(&gate, &audit))
            .await
            .is_err()
    );
    let requests = requests.lock().unwrap();
    let patches: Vec<Value> = requests
        .iter()
        .filter(|(h, _)| h.starts_with("PATCH"))
        .map(|(_, b)| serde_json::from_slice(b).unwrap())
        .collect();
    assert_eq!(patches.len(), 2);
    assert_eq!(patches[0]["children"][0]["type"], "image");
    assert_eq!(patches[1]["children"][0]["type"], "file");
}

#[tokio::test]
async fn batch_terminal_audit_loss_stops_before_next_download_and_retains_receipt() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), Some(3));
    let (download, count, source_task, paths) = source(b"document".to_vec(), 200, false).await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(false, false, false));
    let (client, _, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope());
    let mut batch = FileIngestBatch::new(
        vec![request(false), request(false)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    batch
        .execute(&adapter, &context(&gate, &audit))
        .await
        .unwrap();
    let first = batch.results()[0].outcome.as_ref().unwrap();
    assert_eq!(first.status, IngestStatus::VerifiedAuditFailed);
    assert_eq!(first.references.block_id.as_deref(), Some(BLOCK));
    assert!(batch.results()[1].outcome.is_none());
    assert!(batch.partial_success());
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn canceled_later_readback_preserves_prior_success_and_current_known_references() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, count, source_task, paths) = source_many(
        vec![
            (b"document".to_vec(), 200, false),
            (b"later document".to_vec(), 200, false),
        ],
        64,
    )
    .await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(false, false, false));
    let mut later = full_steps(false, false, false);
    later.last_mut().unwrap().0 = 0;
    steps.extend(later);
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope());
    let mut batch = FileIngestBatch::new(
        vec![request(false), request(false), request(false)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    let ctx = context(&gate, &audit);
    let mut future = Box::pin(batch.execute(&adapter, &ctx));
    tokio::select! {
        result=&mut future => panic!("unexpected completion: {result:?}"),
        _=requests.wait_for(|entries| entries.iter().filter(|(h,_)|h.starts_with("GET /v1/blocks/")).count()==2) => {}
    }
    drop(future);
    assert_eq!(
        batch.results()[0].outcome.as_ref().unwrap().status,
        IngestStatus::Verified
    );
    let current = batch.results()[1].outcome.as_ref().unwrap();
    assert_eq!(current.status, IngestStatus::Indeterminate);
    assert_eq!(current.references.upload_id.as_deref(), Some(UPLOAD));
    assert_eq!(current.references.block_id.as_deref(), Some(BLOCK));
    assert!(batch.results()[2].outcome.is_none());
    assert!(batch.partial_success());
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.abort();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let db = rusqlite::Connection::open(dir.path().join("audit")).unwrap();
    let outcomes: Vec<String> = db
        .prepare("SELECT outcome FROM agent_audit_events ORDER BY id")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        outcomes,
        [
            "attempted",
            "attempted",
            "succeeded",
            "attempted",
            "attempted",
            "indeterminate"
        ]
    );
    assert!(batch.execute(&adapter, &ctx).await.is_err());
}

#[tokio::test]
async fn batch_scope_change_stops_after_prior_verified_attachment_before_next_source() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, count, source_task, paths) = source(b"document".to_vec(), 200, false).await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(false, false, false));
    steps.extend(chain(OUTSIDE).into_iter().map(|v| (200, v)));
    let (client, _, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope());
    let mut batch = FileIngestBatch::new(
        vec![request(false), request(false), request(false)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    batch
        .execute(&adapter, &context(&gate, &audit))
        .await
        .unwrap();
    assert_eq!(
        batch.results()[0].outcome.as_ref().unwrap().status,
        IngestStatus::Verified
    );
    assert_eq!(
        batch.results()[1].outcome.as_ref().unwrap().status,
        IngestStatus::Denied
    );
    assert!(batch.results()[2].outcome.is_none());
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_clean(&paths);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn canceled_multipart_send_retains_created_upload_and_cleans_owned_source() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    const MIB: usize = 1024 * 1024;
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, _, source_task, paths) =
        source_many(vec![(vec![b'x'; 5 * MIB + 1], 200, false)], 10 * MIB).await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    for _ in 0..2 {
        steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
    }
    steps.push((200,json!({"object":"file_upload","id":UPLOAD,"status":"pending","number_of_parts":{"total":2,"sent":0}})));
    steps.push((0, json!({})));
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope())
            .with_multipart(MultipartPolicy::new(10 * MIB, 5 * MIB, 5 * MIB).unwrap());
    let mut batch = FileIngestBatch::new(
        vec![request(false), request(false)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    let ctx = context(&gate, &audit);
    let mut future = Box::pin(batch.execute(&adapter, &ctx));
    tokio::select! {
        result=&mut future => panic!("unexpected completion: {result:?}"),
        _=requests.wait_for(|entries| entries.iter().any(|(h,_)|h.contains("/send "))) => {}
    }
    drop(future);
    let current = batch.results()[0].outcome.as_ref().unwrap();
    assert_eq!(current.status, IngestStatus::Indeterminate);
    assert_eq!(current.references.upload_id.as_deref(), Some(UPLOAD));
    assert!(current.references.block_id.is_none());
    assert!(batch.results()[1].outcome.is_none());
    assert_clean(&paths);
    source_task.await.unwrap();
    notion_task.abort();
}

#[test]
fn batch_shape_count_and_common_target_are_admitted_before_execution() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    assert!(FileBatchPolicy::new(0).is_err());
    assert!(FileBatchPolicy::new(21).is_err());
    assert!(FileIngestBatch::new(vec![], FileBatchPolicy::default()).is_err());
    assert!(
        FileIngestBatch::new(
            (0..6).map(|_| request(false)).collect(),
            FileBatchPolicy::default()
        )
        .is_err()
    );
    let mut other = request(false);
    other.target = PageId(OUTSIDE.into());
    assert!(FileIngestBatch::new(vec![request(false), other], FileBatchPolicy::default()).is_err());
    let mut other = request(false);
    other.caption = Some("bad\0caption".into());
    assert!(FileIngestBatch::new(vec![request(false), other], FileBatchPolicy::default()).is_err());
}

#[tokio::test]
async fn joined_multipart_interruptions_and_completion_failures_retain_upload_without_append() {
    const MIB: usize = 1024 * 1024;
    for failure_stage in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, _, source_task, paths) =
            source_many(vec![(vec![b'x'; 5 * MIB + 1], 200, false)], 10 * MIB).await;
        let mut steps: Vec<_> = chain(ROOT)
            .into_iter()
            .chain(chain(ROOT))
            .map(|v| (200, v))
            .collect();
        let receipt = |status: &str, sent| json!({"object":"file_upload","id":UPLOAD,"status":status,"number_of_parts":{"total":2,"sent":sent}});
        steps.push((200, receipt("pending", 0)));
        if failure_stage == 0 {
            steps.push((503, json!({})));
        } else {
            steps.push((200, receipt("pending", 1)));
            steps.push((200, receipt("pending", 2)));
            steps.push((
                200,
                receipt(
                    if failure_stage == 1 {
                        "pending"
                    } else {
                        "uploaded"
                    },
                    2,
                ),
            ));
            if failure_stage == 2 {
                steps.push((200, receipt("pending", 2)));
            }
        }
        let (client, requests, notion_task) = notion(steps).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::default(),
            scope(),
        )
        .with_multipart(MultipartPolicy::new(10 * MIB, 5 * MIB, 5 * MIB).unwrap());
        let result = ingest_file(&adapter, &context(&gate, &audit), request(false)).await;
        assert_eq!(result.status, IngestStatus::Indeterminate, "{result:?}");
        assert!(result.terminal_audited);
        assert_eq!(result.references.upload_id.as_deref(), Some(UPLOAD));
        assert!(result.references.block_id.is_none());
        assert_clean(&paths);
        source_task.await.unwrap();
        notion_task.await.unwrap();
        let requests = requests.lock().unwrap();
        assert!(!requests.iter().any(|(h, _)| h.starts_with("PATCH")));
        assert_eq!(
            requests
                .iter()
                .filter(|(h, _)| h.contains("/send "))
                .count(),
            if failure_stage == 0 { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn batch_inner_attachment_authority_loss_stops_remaining_effects_with_known_upload() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    for changed in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let audit = audit(&dir.path().join("audit"), None);
        let (download, count, source_task, paths) = source_many(
            vec![
                (b"document".to_vec(), 200, false),
                (b"later document".to_vec(), 200, false),
            ],
            64,
        )
        .await;
        let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
        steps.extend(full_steps(false, false, false));
        let mut later = full_steps(false, false, false);
        if changed {
            // Inner attachment authorize succeeded; its revalidation now sees
            // a changed authoritative edit receipt without leaving the root.
            later.truncate(22);
            for (_, value) in &mut later[18..22] {
                value["last_edited_time"] = json!("2026-10-14T00:00:00Z");
            }
        } else {
            // Inner attachment authorize itself becomes unverifiable. The
            // fixture serves no further item, even if authority could recover.
            later.truncate(15);
            later[14] = (503, json!({}));
        }
        steps.extend(later);
        let (client, requests, notion_task) = notion(steps).await;
        let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
        let adapter = NotionFileIngestAdapter::new(
            client,
            download,
            FileValidationPolicy::default(),
            scope(),
        );
        let mut batch = FileIngestBatch::new(
            vec![request(false), request(false), request(false)],
            FileBatchPolicy::default(),
        )
        .unwrap();
        batch
            .execute(&adapter, &context(&gate, &audit))
            .await
            .unwrap();
        assert_eq!(
            batch.results()[0].outcome.as_ref().unwrap().status,
            IngestStatus::Verified
        );
        let lost = batch.results()[1].outcome.as_ref().unwrap();
        assert_eq!(lost.status, IngestStatus::Denied, "{lost:?}");
        assert_eq!(lost.failure, Some(IngestFailure::Denied));
        assert_eq!(lost.references.upload_id.as_deref(), Some(UPLOAD));
        assert!(lost.references.block_id.is_none());
        assert!(batch.results()[2].outcome.is_none());
        assert!(batch.partial_success());
        source_task.await.unwrap();
        notion_task.await.unwrap();
        assert_clean(&paths);
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn ordinary_upstream_failure_can_continue_to_next_verified_item_without_replay() {
    use notion_knowledge_core::file_ingest::{FileBatchPolicy, FileIngestBatch};
    let dir = tempfile::tempdir().unwrap();
    let audit = audit(&dir.path().join("audit"), None);
    let (download, count, source_task, paths) = source_many(
        vec![
            (b"first".to_vec(), 200, false),
            (b"second".to_vec(), 200, false),
        ],
        64,
    )
    .await;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(false, false, true));
    steps.extend(full_steps(false, false, false));
    let (client, requests, notion_task) = notion(steps).await;
    let gate = RootScopeGate::new(Arc::new(client.clone()), scope()).unwrap();
    let adapter =
        NotionFileIngestAdapter::new(client, download, FileValidationPolicy::default(), scope());
    let mut batch = FileIngestBatch::new(
        vec![request(false), request(false)],
        FileBatchPolicy::default(),
    )
    .unwrap();
    batch
        .execute(&adapter, &context(&gate, &audit))
        .await
        .unwrap();
    assert_eq!(
        batch.results()[0].outcome.as_ref().unwrap().status,
        IngestStatus::Indeterminate
    );
    assert_eq!(
        batch.results()[1].outcome.as_ref().unwrap().status,
        IngestStatus::Verified
    );
    assert!(batch.partial_success());
    source_task.await.unwrap();
    notion_task.await.unwrap();
    assert_clean(&paths);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests
            .iter()
            .filter(|(h, _)| h.contains("/send "))
            .count(),
        2
    );
    assert_eq!(
        requests
            .iter()
            .filter(|(h, _)| h.starts_with("PATCH"))
            .count(),
        1
    );
}
