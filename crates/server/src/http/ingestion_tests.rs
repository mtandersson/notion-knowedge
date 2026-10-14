use super::*;
use crate::{
    config::Config,
    root_scope::{self, Transport},
};
use notion_knowledge_notion::{NotionClient, file_download::DownloadPolicy};
use rmcp::ServiceExt;
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
type Requests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
const PAGE: &str = "11111111-1111-1111-1111-111111111111";
const ROOT: &str = "22222222-2222-2222-2222-222222222222";
const OUTSIDE: &str = "33333333-3333-3333-3333-333333333333";
const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
const UPLOAD: &str = "55555555-5555-5555-5555-555555555555";
const BLOCK: &str = "66666666-6666-6666-6666-666666666666";
const SECRET_URL: &str = "https://fixture.example/file?token=signed-secret";
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
    source_many(vec![(bytes, status, slow)], 64).await
}
async fn source_many(
    files: Vec<(Vec<u8>, u16, bool)>,
    maximum: usize,
) -> (
    DownloadPolicy,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<std::path::PathBuf>>>,
) {
    let certificate = include_bytes!("../../../notion/tests/fixtures/ingest-cert.pem");
    let key = PrivateKeyDer::Pkcs8(
        include_bytes!("../../../notion/tests/fixtures/ingest-key.der")
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
    let count = Arc::new(AtomicUsize::new(0));
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
            for chunk in bytes.chunks(16384) {
                if tls.write_all(chunk).await.is_err() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }
    });
    (policy, count, handle, paths)
}
async fn notion(steps: Vec<(u16, Value)>) -> (NotionClient, Requests, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = NotionClient::integration("fixture-secret").unwrap();
    client = client.with_loopback_fixture(listener.local_addr().unwrap().port());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let observed = requests.clone();
    let handle = tokio::spawn(async move {
        let mut attached: Option<Value> = None;
        for (status, mut value) in steps {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(
                request
                    .0
                    .to_ascii_lowercase()
                    .contains("authorization: bearer fixture-secret")
            );
            if request.0.starts_with("PATCH") {
                assert!(
                    request
                        .0
                        .starts_with(&format!("PATCH /v1/blocks/{PAGE}/children "))
                );
                let payload: Value = serde_json::from_slice(&request.1).unwrap();
                let child = payload["children"][0].clone();
                let kind = child["type"].as_str().unwrap();
                assert_eq!(child[kind]["file_upload"]["id"], UPLOAD);
                assert_eq!(child[kind]["caption"][0]["text"]["content"], "Safe caption");
                attached = Some(child);
            }
            if request.0.starts_with(&format!("GET /v1/blocks/{BLOCK} ")) {
                let child = attached.as_ref().expect("readback requires actual append");
                let kind = child["type"].as_str().unwrap();
                value[kind]["file_upload"]["id"] = child[kind]["file_upload"]["id"].clone();
                value["type"] = child["type"].clone();
            }
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
const BEARER: &str = "local-fixture-bearer-with-32-characters";
fn config(path: &std::path::Path, bearer: bool) -> Config {
    let values = std::collections::HashMap::from([
        ("NK_FILE_INGESTION", "true".to_owned()),
        ("NK_READ_ONLY", "false".into()),
        ("NK_NOTION_AUTH", "integration".into()),
        ("NOTION_TOKEN", "fixture-secret".into()),
        ("NK_NOTION_SCOPE_ROOTS", ROOT.into()),
        ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE.into()),
        ("NK_FILE_SOURCE_HOSTS", "fixture.example".into()),
        ("NK_FILE_AUDIT_FILE", path.display().to_string()),
    ]);
    Config::from_lookup(|key| {
        if bearer && key == "NK_HTTP_BEARER_TOKEN" {
            return Some(BEARER.into());
        }
        if bearer && key == "NK_HTTP_AUTH" {
            return Some("bearer".into());
        }
        values.get(key).map(std::ffi::OsString::from)
    })
    .unwrap()
}
async fn fixture(
    image: bool,
    http: bool,
) -> (
    Config,
    notion_knowledge_mcp::KnowledgeServer,
    tempfile::TempDir,
    Requests,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<std::path::PathBuf>>>,
) {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut config = config(&dir.path().join("audit.sqlite"), http);
    let bytes = if image {
        b"\x89PNG\r\n\x1a\nfixture".to_vec()
    } else {
        b"document fixture".to_vec()
    };
    let (download, _, source, paths) = source(bytes, 200, false).await;
    config.file_ingestion.as_mut().unwrap().download = download;
    let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
    steps.extend(full_steps(image, false, false));
    let (client, requests, notion) = notion(steps).await;
    let handler = root_scope::attach_client(
        notion_knowledge_mcp::KnowledgeServer::default().with_read_only(false),
        &config,
        if http {
            Transport::Http
        } else {
            Transport::Stdio
        },
        client,
    )
    .unwrap();
    (config, handler, dir, requests, source, notion, paths)
}
fn upload(image: bool) -> Value {
    json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"target":PAGE,"caption":" Safe caption ","file":{"download_url":SECRET_URL,"file_id":"opaque-id","file_name":if image {"image.png"} else {"document.txt"},"mime_type":if image {"image/png"} else {"text/plain"}}}}})
}
fn initialize() -> Value {
    json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"installed-ingest-fixture","version":"1"}}})
}
fn assert_result(
    result: &Value,
    image: bool,
    requests: &Requests,
    dir: &tempfile::TempDir,
    paths: &Arc<Mutex<Vec<std::path::PathBuf>>>,
) {
    let output = &result["result"]["structuredContent"];
    assert_eq!(output["files"][0]["status"], "verified", "{result}");
    assert_eq!(output["files"][0]["upload_id"], UPLOAD);
    assert_eq!(output["files"][0]["block_id"], BLOCK);
    assert_eq!(output["files"][0]["terminal_audited"], true);
    let requests = requests.lock().unwrap();
    let sent: Value = serde_json::from_slice(
        &requests
            .iter()
            .find(|(h, _)| h.starts_with("PATCH"))
            .unwrap()
            .1,
    )
    .unwrap();
    assert_eq!(
        sent["children"][0]["type"],
        if image { "image" } else { "file" }
    );
    assert!(
        requests
            .iter()
            .any(|(h, _)| h.starts_with(&format!("GET /v1/blocks/{BLOCK}")))
    );
    assert!(paths.lock().unwrap().iter().all(|p| !p.exists()));
    let db = rusqlite::Connection::open(dir.path().join("audit.sqlite")).unwrap();
    let succeeded: i64 = db
        .query_row(
            "SELECT count(*) FROM agent_audit_events WHERE outcome='succeeded' AND actor='server'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(succeeded, 1);
    let persisted = std::fs::read(dir.path().join("audit.sqlite")).unwrap();
    for secret in [
        "signed-secret",
        "fixture-secret",
        "document fixture",
        "Safe caption",
        "document.txt",
        "image.png",
    ] {
        assert!(
            !persisted
                .windows(secret.len())
                .any(|v| v == secret.as_bytes())
        );
        assert!(!result.to_string().contains(secret));
    }
}
#[tokio::test]
async fn installed_stdio_native_image_and_file_join_https_notion_and_durable_audit() {
    for image in [true, false] {
        let (_, handler, dir, requests, source, notion, paths) = fixture(image, false).await;
        let (server, client) = tokio::io::duplex(65536);
        let task = tokio::spawn(async move {
            handler
                .serve(server)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let (read, mut write) = tokio::io::split(client);
        let mut read = BufReader::new(read);
        let mut result = Value::Null;
        for request in [
            initialize(),
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
            upload(image),
        ] {
            write
                .write_all(format!("{request}\n").as_bytes())
                .await
                .unwrap();
            let mut line = String::new();
            read.read_line(&mut line).await.unwrap();
            result = serde_json::from_str(&line).unwrap();
            if request["id"] == 1 {
                write
                    .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                    .await
                    .unwrap();
            }
            if request["id"] == 2 {
                assert!(
                    result["result"]["tools"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|t| t["name"] == "knowledge_upload_file")
                );
            }
        }
        assert_result(&result, image, &requests, &dir, &paths);
        drop(write);
        drop(read);
        task.await.unwrap();
        source.await.unwrap();
        notion.await.unwrap();
    }
}
async fn frame(mut response: reqwest::Response) -> Value {
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let mut data = String::new();
    while let Some(chunk) = response.chunk().await.unwrap() {
        data.push_str(std::str::from_utf8(&chunk).unwrap());
        for line in data.lines().filter_map(|l| l.strip_prefix("data: ")) {
            if let Ok(v) = serde_json::from_str::<Value>(line)
                && v.get("id").is_some()
            {
                return v;
            }
        }
    }
    panic!("missing response");
}
#[tokio::test]
async fn installed_authenticated_http_attaches_and_rejects_unauthenticated_requests() {
    for image in [true, false] {
        let (mut config, handler, dir, requests, source, notion, paths) =
            fixture(image, true).await;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        config.http_bind = listener.local_addr().unwrap();
        drop(listener);
        let url = format!("http://{}/mcp", config.http_bind);
        let ready = format!("http://{}/livez", config.http_bind);
        let shutdown = tokio_util::sync::CancellationToken::new();
        let cancel = shutdown.clone();
        let task = tokio::spawn(async move {
            serve_installed(
                config,
                handler,
                #[cfg(all(unix, feature = "webhook-worker"))]
                None,
                shutdown,
            )
            .await
            .unwrap();
        });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if client.get(&ready).send().await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let post = || {
            client
                .post(&url)
                .header("accept", "application/json, text/event-stream")
        };
        assert_eq!(
            post().json(&initialize()).send().await.unwrap().status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        let response = post()
            .bearer_auth(BEARER)
            .json(&initialize())
            .send()
            .await
            .unwrap();
        let session = response.headers()["mcp-session-id"]
            .to_str()
            .unwrap()
            .to_owned();
        frame(response).await;
        let session_post = || {
            post()
                .header("mcp-session-id", &session)
                .header("mcp-protocol-version", "2025-03-26")
        };
        assert_eq!(
            session_post()
                .json(&upload(image))
                .send()
                .await
                .unwrap()
                .status(),
            reqwest::StatusCode::UNAUTHORIZED
        );
        session_post()
            .bearer_auth(BEARER)
            .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
            .send()
            .await
            .unwrap();
        let catalog = frame(
            session_post()
                .bearer_auth(BEARER)
                .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert!(
            catalog["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t["name"] == "knowledge_upload_file")
        );
        let result = frame(
            session_post()
                .bearer_auth(BEARER)
                .json(&upload(image))
                .send()
                .await
                .unwrap(),
        )
        .await;
        assert_result(&result, image, &requests, &dir, &paths);
        cancel.cancel();
        task.await.unwrap();
        source.await.unwrap();
        notion.await.unwrap();
    }
}
async fn stdio_call(handler: notion_knowledge_mcp::KnowledgeServer, request: Value) -> Value {
    let (server, client) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        handler
            .serve(server)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);
    write
        .write_all(format!("{}\n", initialize()).as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    read.read_line(&mut line).await.unwrap();
    write
        .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
        .await
        .unwrap();
    write
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    line.clear();
    read.read_line(&mut line).await.unwrap();
    let value = serde_json::from_str(&line).unwrap();
    drop(write);
    drop(read);
    task.await.unwrap();
    value
}
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}
#[tokio::test]
async fn installed_transports_ordered_batch_keeps_success_and_reports_source_failure() {
    for http in [false, true] {
        let dir = private_dir();
        let mut cfg = config(&dir.path().join("audit.sqlite"), http);
        let (download, count, source, paths) = source_many(
            vec![
                (b"document fixture".to_vec(), 200, false),
                (b"expired".to_vec(), 403, false),
            ],
            64,
        )
        .await;
        cfg.file_ingestion.as_mut().unwrap().download = download;
        let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
        steps.extend(full_steps(false, false, false));
        steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
        let (client, requests, notion) = notion(steps).await;
        let handler = root_scope::attach_client(
            notion_knowledge_mcp::KnowledgeServer::default().with_read_only(false),
            &cfg,
            if http {
                Transport::Http
            } else {
                Transport::Stdio
            },
            client,
        )
        .unwrap();
        let mut request = upload(false);
        let file = request["params"]["arguments"]["file"].take();
        request["params"]["arguments"]
            .as_object_mut()
            .unwrap()
            .remove("file");
        request["params"]["arguments"]["files"] = json!([file.clone(), file]);
        let result = if http {
            http_call(cfg, handler, request).await
        } else {
            stdio_call(handler, request).await
        };
        let files = &result["result"]["structuredContent"]["files"];
        assert_eq!(files[0]["index"], 0);
        assert_eq!(files[0]["status"], "verified");
        assert_eq!(files[1]["index"], 1);
        assert_eq!(files[1]["status"], "failed");
        assert_eq!(files[1]["failure"], "source_expired");
        assert_eq!(
            result["result"]["structuredContent"]["partial_success"],
            true
        );
        source.await.unwrap();
        notion.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        let db = rusqlite::Connection::open(dir.path().join("audit.sqlite")).unwrap();
        let outcomes:Vec<String>=db.prepare("SELECT outcome FROM agent_audit_events WHERE outcome IN ('succeeded','failed') ORDER BY id").unwrap().query_map([],|r|r.get(0)).unwrap().map(Result::unwrap).collect();
        assert_eq!(outcomes, ["succeeded", "failed"]);
        let persisted = std::fs::read(dir.path().join("audit.sqlite")).unwrap();
        for secret in [
            "signed-secret",
            "fixture-secret",
            "document fixture",
            "document.txt",
        ] {
            assert!(
                !persisted
                    .windows(secret.len())
                    .any(|v| v == secret.as_bytes())
            );
        }
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            1
        );
        assert!(paths.lock().unwrap().iter().all(|p| !p.exists()));
    }
}
#[tokio::test]
async fn installed_transports_select_single_and_multipart_from_observed_bytes() {
    const MIB: usize = 1024 * 1024;
    for (http, size) in [
        (false, 5 * MIB),
        (false, 5 * MIB + 1),
        (true, 5 * MIB),
        (true, 5 * MIB + 1),
    ] {
        let dir = private_dir();
        let mut cfg = config(&dir.path().join("audit.sqlite"), http);
        let (download, _, source, paths) = source_many(
            vec![(
                (0..size)
                    .map(|i| if i < 5 * MIB { b'x' } else { b'y' })
                    .collect(),
                200,
                false,
            )],
            10 * MIB,
        )
        .await;
        let settings = cfg.file_ingestion.as_mut().unwrap();
        settings.download = download;
        settings.maximum_bytes = 10 * MIB;
        settings.multipart = Some(
            notion_knowledge_notion::file_multipart::MultipartPolicy::new(
                10 * MIB,
                5 * MIB,
                5 * MIB,
            )
            .unwrap(),
        );
        let mut execution = full_steps(false, false, false);
        if size > 5 * MIB {
            let receipt = |status: &str, sent| json!({"object":"file_upload","id":UPLOAD,"status":status,"number_of_parts":{"total":2,"sent":sent}});
            execution.splice(
                8..10,
                vec![
                    (200, receipt("pending", 0)),
                    (200, receipt("pending", 1)),
                    (200, receipt("pending", 2)),
                    (200, receipt("uploaded", 2)),
                    (200, receipt("uploaded", 2)),
                ],
            );
        }
        let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
        steps.extend(execution);
        let (client, requests, notion) = notion(steps).await;
        let handler = root_scope::attach_client(
            notion_knowledge_mcp::KnowledgeServer::default().with_read_only(false),
            &cfg,
            if http {
                Transport::Http
            } else {
                Transport::Stdio
            },
            client,
        )
        .unwrap();
        let result = if http {
            http_call(cfg, handler, upload(false)).await
        } else {
            stdio_call(handler, upload(false)).await
        };
        assert_eq!(
            result["result"]["structuredContent"]["files"][0]["status"], "verified",
            "{result}"
        );
        source.await.unwrap();
        notion.await.unwrap();
        assert!(paths.lock().unwrap().iter().all(|p| !p.exists()));
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
            if size > 5 * MIB {
                "multi_part"
            } else {
                "single_part"
            }
        );
        if size > 5 * MIB {
            assert_eq!(created["number_of_parts"], 2);
            let complete: Vec<_> = requests
                .iter()
                .enumerate()
                .filter(|(_, (h, _))| h.contains("/complete "))
                .collect();
            assert_eq!(complete.len(), 1);
            let final_get = requests
                .iter()
                .position(|(h, _)| h.starts_with(&format!("GET /v1/file_uploads/{UPLOAD} ")))
                .unwrap();
            let append = requests
                .iter()
                .position(|(h, _)| h.starts_with("PATCH"))
                .unwrap();
            assert!(complete[0].0 < final_get && final_get < append);
        }
        let sends: Vec<_> = requests
            .iter()
            .filter(|(h, _)| h.contains("/send "))
            .collect();
        assert_eq!(sends.len(), if size > 5 * MIB { 2 } else { 1 });
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
            let end = body[start..]
                .windows(4)
                .position(|v| v == b"\r\n--")
                .unwrap()
                + start;
            assert_eq!(
                &body[start..end],
                vec![
                    if index == 0 { b'x' } else { b'y' };
                    if index == 0 { size.min(5 * MIB) } else { 1 }
                ]
            );
            if size > 5 * MIB {
                let part = format!("name=\"part_number\"\r\n\r\n{}\r\n", index + 1);
                assert!(body.windows(part.len()).any(|v| v == part.as_bytes()));
            }
        }
    }
}
#[tokio::test]
async fn installed_transport_scope_and_effective_policy_deny_before_source_or_writes() {
    for mode in [
        "scope",
        "excluded",
        "read_only",
        "blocked",
        "unauthenticated_http",
    ] {
        let dir = private_dir();
        let mut cfg = config(&dir.path().join("audit.sqlite"), false);
        if mode == "excluded" {
            cfg.notion_scope
                .as_mut()
                .unwrap()
                .exclusions
                .page_ids
                .insert(PAGE.into());
        }
        let steps = if mode == "excluded" {
            chain(ROOT).into_iter().map(|v| (200, v)).collect()
        } else if mode == "scope" {
            chain(OUTSIDE).into_iter().map(|v| (200, v)).collect()
        } else {
            vec![]
        };
        let (client, requests, notion) = notion(steps).await;
        let handler = root_scope::attach_client(
            notion_knowledge_mcp::KnowledgeServer::default().with_read_only(mode == "read_only"),
            &cfg,
            if mode == "unauthenticated_http" {
                Transport::Http
            } else {
                Transport::Stdio
            },
            client,
        )
        .unwrap()
        .with_capability_policy(
            notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(
                None,
                if mode == "blocked" {
                    Some("write")
                } else {
                    None
                },
            )
            .unwrap(),
        );
        let result = stdio_call(handler, upload(false)).await;
        match mode {
            "scope" | "excluded" => {
                assert_eq!(result["result"]["structuredContent"]["failure"], "denied")
            }
            "read_only" | "blocked" => assert_eq!(result["error"]["code"], -32602),
            _ => assert!(
                result["result"]["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("file_upload_unavailable")
            ),
        }
        notion.await.unwrap();
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(h, _)| h.starts_with("GET"))
        );
        assert!(!dir.path().join("audit.sqlite").exists() || mode != "unauthenticated_http");
    }
}
#[tokio::test]
async fn installed_stdio_cancellation_during_multipart_send_or_readback_cleans_and_audits_ambiguity()
 {
    const MIB: usize = 1024 * 1024;
    for (wire, during_send) in [(true, true), (true, false), (false, true), (false, false)] {
        let dir = private_dir();
        let mut cfg = config(&dir.path().join("audit.sqlite"), false);
        let bytes = if during_send {
            vec![b'x'; 5 * MIB + 1]
        } else {
            b"document fixture".to_vec()
        };
        let (download, _, source, paths) = source_many(vec![(bytes, 200, false)], 10 * MIB).await;
        let settings = cfg.file_ingestion.as_mut().unwrap();
        settings.download = download;
        if during_send {
            settings.maximum_bytes = 10 * MIB;
            settings.multipart = Some(
                notion_knowledge_notion::file_multipart::MultipartPolicy::new(
                    10 * MIB,
                    5 * MIB,
                    5 * MIB,
                )
                .unwrap(),
            );
        }
        let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
        if during_send {
            for _ in 0..2 {
                steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
            }
            steps.push((200,json!({"object":"file_upload","id":UPLOAD,"status":"pending","number_of_parts":{"total":2,"sent":0}})));
            steps.push((0, json!({})));
        } else {
            steps.extend(full_steps(false, false, false));
            steps.last_mut().unwrap().0 = 0;
        }
        let (client, requests, notion) = notion(steps).await;
        if !wire {
            use notion_knowledge_mcp::upload::FileIngestion;
            let runtime = crate::file_ingestion::Runtime::new(
                &cfg,
                cfg.file_ingestion.as_ref().unwrap(),
                client,
            )
            .unwrap();
            let mut input = upload(false)["params"]["arguments"].clone();
            let file = input["file"].take();
            input.as_object_mut().unwrap().remove("file");
            input["files"] = json!([file.clone(), file]);
            let input = serde_json::from_value(input).unwrap();
            let cancellation = tokio_util::sync::CancellationToken::new();
            let cancel = cancellation.clone();
            let task = tokio::spawn(async move {
                runtime
                    .ingest(input, Box::pin(cancellation.cancelled_owned()))
                    .await
                    .unwrap()
            });
            tokio::time::timeout(Duration::from_secs(20), async {
                loop {
                    if requests.lock().unwrap().iter().any(|(h, _)| {
                        if during_send {
                            h.contains("/send ")
                        } else {
                            h.starts_with(&format!("GET /v1/blocks/{BLOCK} "))
                        }
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            cancel.cancel();
            let result = task.await.unwrap();
            assert_eq!(result["cancelled"], true);
            assert_eq!(result["failure"], "cancelled");
            assert_eq!(result["files"][0]["status"], "indeterminate");
            assert_eq!(result["files"][0]["upload_id"], UPLOAD);
            assert_eq!(
                result["files"][0]["block_id"],
                if during_send {
                    Value::Null
                } else {
                    json!(BLOCK)
                }
            );
            assert_eq!(result["files"][1]["status"], "unattempted");
            assert!(paths.lock().unwrap().iter().all(|p| !p.exists()));
            let db = rusqlite::Connection::open(dir.path().join("audit.sqlite")).unwrap();
            let n: i64 = db
                .query_row(
                    "SELECT count(*) FROM agent_audit_events WHERE outcome='indeterminate'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(n, 1);
            source.await.unwrap();
            notion.abort();
            continue;
        }
        let handler = root_scope::attach_client(
            notion_knowledge_mcp::KnowledgeServer::default().with_read_only(false),
            &cfg,
            Transport::Stdio,
            client,
        )
        .unwrap();
        let (server, client) = tokio::io::duplex(65536);
        let server = tokio::spawn(async move {
            handler
                .serve(server)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let (read, mut write) = tokio::io::split(client);
        let mut read = BufReader::new(read);
        write
            .write_all(format!("{}\n", initialize()).as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
        write
            .write_all(format!("{}\n", upload(false)).as_bytes())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if requests.lock().unwrap().iter().any(|(h, _)| {
                    if during_send {
                        h.contains("/send ")
                    } else {
                        h.starts_with(&format!("GET /v1/blocks/{BLOCK} "))
                    }
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        write.write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":3}}\n").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let db = rusqlite::Connection::open(dir.path().join("audit.sqlite")).unwrap();
                let n: i64 = db
                    .query_row(
                        "SELECT count(*) FROM agent_audit_events WHERE outcome='indeterminate'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                if n == 1 && paths.lock().unwrap().iter().all(|p| !p.exists()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("POST /v1/file_uploads "))
                .count(),
            1
        );
        assert_eq!(
            requests
                .lock()
                .unwrap()
                .iter()
                .filter(|(h, _)| h.starts_with("PATCH"))
                .count(),
            if during_send { 0 } else { 1 }
        );
        drop(write);
        drop(read);
        server.await.unwrap();
        source.await.unwrap();
        notion.abort();
    }
}
async fn http_call(
    mut config: Config,
    handler: notion_knowledge_mcp::KnowledgeServer,
    request: Value,
) -> Value {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    config.http_bind = listener.local_addr().unwrap();
    drop(listener);
    let url = format!("http://{}/mcp", config.http_bind);
    let ready = format!("http://{}/livez", config.http_bind);
    let shutdown = tokio_util::sync::CancellationToken::new();
    let cancel = shutdown.clone();
    let task = tokio::spawn(async move {
        serve_installed(
            config,
            handler,
            #[cfg(all(unix, feature = "webhook-worker"))]
            None,
            shutdown,
        )
        .await
        .unwrap();
    });
    let client = reqwest::Client::builder().no_proxy().build().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client.get(&ready).send().await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let post = || {
        client
            .post(&url)
            .header("accept", "application/json, text/event-stream")
    };
    assert_eq!(
        post().json(&initialize()).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    let response = post()
        .bearer_auth(BEARER)
        .json(&initialize())
        .send()
        .await
        .unwrap();
    let session = response.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    frame(response).await;
    let session_post = || {
        post()
            .header("mcp-session-id", &session)
            .header("mcp-protocol-version", "2025-03-26")
    };
    assert_eq!(
        session_post().json(&request).send().await.unwrap().status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    session_post()
        .bearer_auth(BEARER)
        .json(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
        .send()
        .await
        .unwrap();
    let _catalog = frame(
        session_post()
            .bearer_auth(BEARER)
            .json(&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .send()
            .await
            .unwrap(),
    )
    .await;
    let result = frame(
        session_post()
            .bearer_auth(BEARER)
            .json(&request)
            .send()
            .await
            .unwrap(),
    )
    .await;
    cancel.cancel();
    task.await.unwrap();
    result
}
#[tokio::test]
async fn installed_transports_reject_mismatched_observed_type_before_upload() {
    for http in [false, true] {
        let dir = private_dir();
        let mut cfg = config(&dir.path().join("audit.sqlite"), http);
        let (download, count, source, paths) =
            source(b"document fixture".to_vec(), 200, false).await;
        cfg.file_ingestion.as_mut().unwrap().download = download;
        let mut steps: Vec<_> = chain(ROOT).into_iter().map(|v| (200, v)).collect();
        steps.extend(chain(ROOT).into_iter().map(|v| (200, v)));
        let (client, requests, notion) = notion(steps).await;
        let handler = root_scope::attach_client(
            notion_knowledge_mcp::KnowledgeServer::default().with_read_only(false),
            &cfg,
            if http {
                Transport::Http
            } else {
                Transport::Stdio
            },
            client,
        )
        .unwrap();
        let result = if http {
            http_call(cfg, handler, upload(true)).await
        } else {
            stdio_call(handler, upload(true)).await
        };
        assert_eq!(
            result["result"]["structuredContent"]["files"][0]["status"],
            "failed"
        );
        assert_eq!(
            result["result"]["structuredContent"]["files"][0]["failure"],
            "invalid_input"
        );
        source.await.unwrap();
        notion.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(
            requests
                .lock()
                .unwrap()
                .iter()
                .all(|(h, _)| h.starts_with("GET"))
        );
        assert!(paths.lock().unwrap().iter().all(|p| !p.exists()));
    }
}
