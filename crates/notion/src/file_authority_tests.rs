//! Actual provider boundaries for file effects, independent of shipping installation.
use crate::{
    NotionClient, RequestAuthority, file_download::DownloadedFile, file_multipart::MultipartPolicy,
    file_validation::FileValidationPolicy,
};
use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
};
const ID: &str = "11111111-1111-1111-1111-111111111111";
struct Authority {
    active: AtomicBool,
    writable: AtomicBool,
    last_mutation: AtomicBool,
    checks: AtomicUsize,
    changed: Notify,
}
impl Authority {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            active: true.into(),
            writable: true.into(),
            last_mutation: false.into(),
            checks: 0.into(),
            changed: Notify::new(),
        })
    }
}
impl RequestAuthority for Authority {
    fn validate(&self, mutation: bool) -> Result<(), BackendError> {
        self.last_mutation.store(mutation, Ordering::SeqCst);
        self.checks.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        if self.active.load(Ordering::SeqCst) && (!mutation || self.writable.load(Ordering::SeqCst))
        {
            Ok(())
        } else {
            Err(BackendError {
                kind: BackendErrorKind::Unauthenticated,
                operation: "fixture.current_grant",
                retry_after: None,
                committed_page_id: None,
            })
        }
    }
}
async fn provider(
    authority: Arc<Authority>,
    replies: Vec<(String, Value)>,
    revoke_receipt: Option<usize>,
) -> (NotionClient, tokio::task::JoinHandle<Vec<String>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = NotionClient::integration("local-fixture-only")
        .unwrap()
        .with_fast_fixture_transport()
        .with_request_authority(authority.clone());
    client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut observed = Vec::new();
        for (index, (expected, body)) in replies.into_iter().enumerate() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut wire = Vec::new();
            let (start, length, chunked) = loop {
                let mut bytes = [0; 65536];
                let n = stream.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                wire.extend_from_slice(&bytes[..n]);
                if let Some(end) = wire.windows(4).position(|v| v == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&wire[..end]).to_lowercase();
                    assert!(headers.starts_with(&expected.to_lowercase()), "{headers}");
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length: ")?.parse::<usize>().ok()
                        })
                        .unwrap_or(0);
                    break (
                        end + 4,
                        length,
                        headers.contains("transfer-encoding: chunked"),
                    );
                }
            };
            while wire.len() < start + length || (chunked && !wire[start..].ends_with(b"0\r\n\r\n"))
            {
                let mut bytes = [0; 65536];
                let n = stream.read(&mut bytes).await.unwrap();
                assert!(n > 0);
                wire.extend_from_slice(&bytes[..n]);
            }
            // Read requests use read admission, every effect (including raw
            // multipart bodies) must have asked for mutation permission.
            assert_eq!(
                authority.last_mutation.load(Ordering::SeqCst),
                !expected.starts_with("GET"),
                "{expected}"
            );
            observed.push(expected);
            let body = body.to_string();
            let checked = authority.checks.load(Ordering::SeqCst);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            if revoke_receipt == Some(index) {
                // Wait for the transport's post-header validation, then revoke
                // while the authoritative receipt body is still unavailable.
                loop {
                    let notified = authority.changed.notified();
                    if authority.checks.load(Ordering::SeqCst) > checked {
                        break;
                    }
                    notified.await;
                }
                authority.active.store(false, Ordering::SeqCst);
            }
            stream.write_all(body.as_bytes()).await.unwrap();
        }
        observed
    });
    (client, server)
}
fn receipt(state: &str, total: usize, sent: usize) -> Value {
    json!({"object":"file_upload","id":ID,"status":state,"number_of_parts":{"total":total,"sent":sent}})
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("bounded provider exchange")
}
#[tokio::test]
async fn authority_denial_before_create_has_no_possible_effect_or_reference() {
    let authority = Authority::new();
    authority.active.store(false, Ordering::SeqCst);
    let (client, server) = provider(authority, vec![], None).await;
    let failure = bounded(client.upload_single_part_detailed(
        &DownloadedFile::test_fixture(b"notes"),
        "notes.txt",
        "text/plain",
        FileValidationPolicy::default(),
    ))
    .await
    .unwrap_err();
    assert!(failure.authority_lost);
    assert!(!failure.effect_possible);
    assert_eq!(failure.upload_id, None);
    assert_eq!(client.request_metrics().attempts, 0);
    assert!(server.await.unwrap().is_empty());
}
#[tokio::test]
async fn single_part_requires_mutation_permission_and_rechecks_complete_receipts() {
    for revoked in [None, Some(0), Some(1)] {
        let authority = Authority::new();
        let mut replies = vec![("POST /v1/file_uploads ".into(), receipt("pending", 1, 0))];
        if revoked != Some(0) {
            replies.push((
                format!("POST /v1/file_uploads/{ID}/send "),
                receipt("uploaded", 1, 1),
            ));
        }
        let (client, server) = provider(authority, replies, revoked).await;
        let result = bounded(client.upload_single_part_detailed(
            &DownloadedFile::test_fixture(b"notes"),
            "notes.txt",
            "text/plain",
            FileValidationPolicy::default(),
        ))
        .await;
        if revoked.is_some() {
            let failure = result.unwrap_err();
            assert!(failure.authority_lost && failure.effect_possible);
            assert_eq!(failure.upload_id.as_deref(), Some(ID));
        } else {
            assert_eq!(result.unwrap().id, ID);
        }
        let paths = bounded(server).await.unwrap();
        assert_eq!(paths.len(), if revoked == Some(0) { 1 } else { 2 });
        assert_eq!(client.request_metrics().attempts, paths.len() as u64);
    }
}
#[tokio::test]
async fn multipart_stops_on_lost_part_complete_or_verification_receipt_without_replay() {
    const MIB: usize = 1024 * 1024;
    for revoked in [None, Some(1), Some(3), Some(4)] {
        let authority = Authority::new();
        let all = vec![
            ("POST /v1/file_uploads ".into(), receipt("pending", 2, 0)),
            (
                format!("POST /v1/file_uploads/{ID}/send "),
                receipt("pending", 2, 1),
            ),
            (
                format!("POST /v1/file_uploads/{ID}/send "),
                receipt("pending", 2, 2),
            ),
            (
                format!("POST /v1/file_uploads/{ID}/complete "),
                receipt("uploaded", 2, 2),
            ),
            (
                format!("GET /v1/file_uploads/{ID} "),
                receipt("uploaded", 2, 2),
            ),
        ];
        let steps = revoked.map_or(5, |index| index + 1);
        let (client, server) =
            provider(authority, all.into_iter().take(steps).collect(), revoked).await;
        let mut bytes = vec![b'x'; 5 * MIB + 1];
        bytes[..5].copy_from_slice(b"%PDF-");
        let result = bounded(client.upload_multipart_detailed(
            &DownloadedFile::test_fixture(&bytes),
            "proof.pdf",
            "application/pdf",
            MultipartPolicy::new(10 * MIB, 5 * MIB, 5 * MIB).unwrap(),
        ))
        .await;
        if revoked.is_some() {
            let failure = result.unwrap_err();
            assert!(failure.authority_lost && failure.effect_possible);
            assert_eq!(failure.upload_id.as_deref(), Some(ID));
        } else {
            assert_eq!(result.unwrap().id, ID);
        }
        assert_eq!(bounded(server).await.unwrap().len(), steps);
        assert_eq!(client.request_metrics().attempts, steps as u64);
    }
}

#[tokio::test]
async fn write_permission_lost_during_admission_prevents_create_or_stream_send() {
    use crate::test_clock::TestClock;
    use tokio::time::Instant;
    for before_create in [true, false] {
        let mut clock = TestClock::new().await;
        let authority = Authority::new();
        let response = if before_create {
            ("GET /v1/probe ".into(), json!({}))
        } else {
            ("POST /v1/file_uploads ".into(), receipt("pending", 1, 0))
        };
        let (mut client, server) = provider(authority.clone(), vec![response], None).await;
        clock.handle().track(&mut client);
        if before_create {
            clock
                .run(client.send(
                    client.http.get(format!("{}/probe", client.api_root)),
                    true,
                    "fixture.read",
                ))
                .await
                .unwrap();
        }
        let file = DownloadedFile::test_fixture(b"notes");
        let (pending, until) = clock
            .park_until_wait(client.upload_single_part_detailed(
                &file,
                "notes.txt",
                "text/plain",
                FileValidationPolicy::default(),
            ))
            .await;
        authority.writable.store(false, Ordering::SeqCst);
        // It remains a valid read credential: only the mutation grant was lost.
        authority.validate(false).unwrap();
        TestClock::advance(until.saturating_duration_since(Instant::now())).await;
        let failure = pending.await.unwrap_err();
        assert!(failure.authority_lost);
        assert_eq!(failure.effect_possible, !before_create);
        assert_eq!(
            failure.upload_id.as_deref(),
            if before_create { None } else { Some(ID) }
        );
        assert_eq!(client.request_metrics().attempts, 1);
        assert_eq!(server.await.unwrap().len(), 1);
        drop(clock);
        tokio::time::resume();
    }
}

#[tokio::test]
async fn image_and_file_attachment_receipt_loss_keeps_known_blocks_but_never_verifies() {
    use crate::file_upload::{UploadStatus, UploadedFile};
    use notion_knowledge_core::{backend::PageId, lifecycle::LifecycleScope};
    const PAGE: &str = "22222222-2222-2222-2222-222222222222";
    const ROOT: &str = "33333333-3333-3333-3333-333333333333";
    const BLOCK: &str = "44444444-4444-4444-4444-444444444444";
    for image in [false, true] {
        for revoked in [None, Some(8), Some(9)] {
            let authority = Authority::new();
            let node = |id: &str, parent: Option<&str>| json!({"object":"page","id":id,"last_edited_time":"2026-10-11T09:00:00Z","in_trash":false,"parent":match parent {Some(p)=>json!({"type":"page_id","page_id":p}),None=>json!({"type":"workspace","workspace":true})}});
            let mut replies = Vec::new();
            for _ in 0..2 {
                for (id, parent) in [
                    (PAGE, Some(ROOT)),
                    (ROOT, None),
                    (ROOT, None),
                    (PAGE, Some(ROOT)),
                ] {
                    replies.push((format!("GET /v1/pages/{id} "), node(id, parent)));
                }
            }
            let kind = if image { "image" } else { "file" };
            let filename = if image { "proof.png" } else { "proof.pdf" };
            let body =
                json!({"type":"file_upload","file_upload":{"id":ID},"caption":[],"name":filename});
            let block = json!({"object":"block","id":BLOCK,"type":kind,"in_trash":false,"parent":{"type":"page_id","page_id":PAGE},kind:body});
            replies.push((
                format!("PATCH /v1/blocks/{PAGE}/children "),
                json!({"object":"list","results":[block.clone()]}),
            ));
            if revoked != Some(8) {
                replies.push((format!("GET /v1/blocks/{BLOCK} "), block));
            }
            let steps = replies.len();
            let (client, server) = provider(authority, replies, revoked).await;
            let upload = UploadedFile {
                id: ID.into(),
                status: UploadStatus::Uploaded,
                filename: filename.into(),
                mime_type: if image {
                    "image/png"
                } else {
                    "application/pdf"
                }
                .into(),
                size_bytes: 16,
            };
            let scope = LifecycleScope {
                workspace_id: "55555555-5555-5555-5555-555555555555".into(),
                generation: 1,
                roots: vec![PageId(ROOT.into())],
                exclusions: Default::default(),
            };
            let result = if image {
                bounded(client.attach_uploaded_image_detailed(
                    &PageId(PAGE.into()),
                    &upload,
                    None,
                    &scope,
                ))
                .await
                .map(|value| value.verified)
            } else {
                bounded(client.attach_uploaded_file_detailed(
                    &PageId(PAGE.into()),
                    &upload,
                    None,
                    &scope,
                ))
                .await
                .map(|value| value.verified)
            };
            if revoked.is_some() {
                let failure = result.unwrap_err();
                assert!(failure.authority_lost && failure.effect_possible);
                assert_eq!(failure.upload_id.as_deref(), Some(ID));
                assert_eq!(failure.block_id.as_deref(), Some(BLOCK));
            } else {
                assert!(result.unwrap());
            }
            let paths = bounded(server).await.unwrap();
            assert_eq!(paths.len(), steps);
            assert_eq!(paths.iter().filter(|p| p.starts_with("PATCH")).count(), 1);
            assert_eq!(client.request_metrics().attempts, steps as u64);
        }
    }
}
