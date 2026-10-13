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
    Arc<AtomicUsize>,
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
    let policy = DownloadPolicy::new(&["fixture.example"], 64, Duration::from_secs(2))
        .unwrap()
        .with_tls_fixture(
            listener.local_addr().unwrap(),
            reqwest::Certificate::from_pem(certificate).unwrap(),
            paths.clone(),
        );
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let handle = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = TlsAcceptor::from(Arc::new(tls))
            .accept(socket)
            .await
            .unwrap();
        let (headers, _) = read_request(&mut tls).await;
        assert!(headers.starts_with("GET /file?token=signed-secret "));
        assert!(!headers.to_ascii_lowercase().contains("authorization:"));
        observed.fetch_add(1, Ordering::SeqCst);
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
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        for chunk in bytes.chunks(3) {
            if tls.write_all(chunk).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    (policy, count, handle, paths)
}
async fn notion(
    steps: Vec<(u16, Value)>,
) -> (
    NotionClient,
    Arc<Mutex<Vec<(String, Vec<u8>)>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = NotionClient::integration("fixture-secret")
        .unwrap()
        .without_retries();
    client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
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
        if cancel {
            let mut future = Box::pin(ingest_file(&adapter, &ctx, request(false)));
            tokio::select! { _= &mut future => panic!("download should still be pending"), _=async {while count.load(Ordering::SeqCst)==0 {tokio::task::yield_now().await;}}=>{} }
            drop(future);
        } else {
            let result = ingest_file(&adapter, &ctx, request(false)).await;
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
    tokio::select! { _= &mut future => panic!("upload should still be pending"), _=async {loop {if requests.lock().unwrap().iter().any(|(h,_)|h.starts_with(&format!("POST /v1/file_uploads/{UPLOAD}/send"))) {break;}tokio::task::yield_now().await;}}=>{} }
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
        DownloadPolicy::new(&["fixture.example"], 64, Duration::from_secs(2)).unwrap(),
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
