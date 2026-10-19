//! Real subprocess signals, owned native effects and durable HTTP admission.
use super::*;
use notion_knowledge_core::{reconciliation::FailureClass, sync_state::PageSyncState};
use notion_knowledge_retrieval::commit::{PageAction, PageOperation};
use std::{path::Path, process::Stdio};

async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

/// Child entrypoint uses the SAME process runtime and HTTP supervision as main.
#[test]
#[ignore = "subprocess fixture invoked explicitly by the acceptance test"]
fn native_process_fixture() {
    let control = std::env::var_os("NK112_CONTROL").expect("fixture control path");
    let control = std::path::PathBuf::from(control);
    let port: u16 = std::env::var("NK112_PORT").unwrap().parse().unwrap();
    let result = notion_knowledge_server::shutdown::run(async move {
        let runtime = TestRuntime::new().await;
        let _persistent = runtime._dir.keep();
        let coordinator = runtime.worker.coordinator.clone();
        let fixed_now = now_ms() / 1000;
        let lease = coordinator.state().acquire_lease(fixed_now, 3600).unwrap();
        // The real process must either commit or roll back this transaction.
        rusqlite::Connection::open(&runtime.db)
            .unwrap()
            .execute_batch("CREATE TABLE shutdown_fixture(value TEXT NOT NULL)")
            .unwrap();
        let values = std::collections::BTreeMap::from([
            ("NK_WEBHOOK_MODE", "verified"),
            ("NK_WEBHOOK_VERIFICATION_TOKEN", "fixture-signing"),
            ("NK_WEBHOOK_WORKSPACE_ID", WORKSPACE),
            ("NK_WEBHOOK_INTEGRATION_ID", INTEGRATION),
            ("NK_WEBHOOK_SUBSCRIPTION_ID", SUB),
            ("NK_WEBHOOK_STATE_FILE", "/tmp/fixture.sqlite"),
        ]);
        let mut config = notion_knowledge_server::config::Config::from_lookup(|key| {
            values.get(key).map(std::ffi::OsString::from)
        })
        .unwrap();
        config.http_bind = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        config.shutdown_timeout_seconds = 1;
        config.webhook_state_file = Some(runtime.db.clone());
        let worker = runtime.worker.clone();
        let serving = tokio::spawn(notion_knowledge_server::http::serve_composed(
            config,
            notion_knowledge_mcp::KnowledgeServer::default(),
            Some(worker),
            CancellationToken::new(),
        ));
        tokio::time::timeout(Duration::from_secs(5), async {
            while reqwest::get(format!("http://127.0.0.1:{port}/livez"))
                .await
                .is_err()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        std::fs::write(
            control.join("ready.json"),
            json!({"index":runtime.index,"state":runtime.db}).to_string(),
        )
        .unwrap();
        // Parent first obtains an actual HTTP200 durable receipt, while no
        // transaction is held. Then this native owned effect takes the fence.
        wait_file(&control.join("begin")).await;
        let db = runtime.db.clone();
        let entered = control.join("entered");
        let release = control.join("release");
        let operation = PageOperation::new(
            "shutdown-native-effect",
            "fixture-native-revision",
            PageAction::Refresh,
            PageSyncState::present(
                OTHER.into(),
                "native-effect".into(),
                Some("fixture-native-revision".into()),
            )
            .unwrap(),
        )
        .unwrap();
        let native = coordinator.submit(
            operation,
            lease.clone(),
            Arc::new(move || fixed_now),
            move |context| async move {
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                let mut db = rusqlite::Connection::open(db).unwrap();
                let tx = db.transaction().unwrap();
                tx.execute("INSERT INTO shutdown_fixture VALUES('completed')", [])
                    .unwrap();
                std::fs::write(entered, b"native guard and SQLite transaction held").unwrap();
                // An intentionally uninterruptible native callback. Its real owned
                // thread, directory guard and transaction survive observer drop.
                while !release.exists() {
                    std::thread::sleep(Duration::from_millis(5));
                }
                tx.commit().unwrap();
                context.revalidate().map_err(|_| FailureClass::Conflict)?;
                Ok(())
            },
        );
        let owner = coordinator.clone();
        let native = tokio::spawn(async move {
            let result = native.await;
            owner
                .state()
                .release_lease(&lease, now_ms() / 1000)
                .unwrap();
            result
        });
        let result = serving.await.unwrap();
        if result.is_ok() {
            native.await.unwrap().unwrap();
        }
        result
    })
    .unwrap();
    if result.is_err() {
        std::process::exit(1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_drains_or_rolls_back_native_work_and_preserves_acknowledged_webhooks() {
    use hmac::{Hmac, Mac};
    for complete in [true, false] {
        let control = tempfile::tempdir().unwrap();
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "shutdown_process::native_process_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .env("NK112_CONTROL", control.path())
            .env("NK112_PORT", port.to_string())
            .env("TMPDIR", control.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        wait_file(&control.path().join("ready.json")).await;
        let ready: Value =
            serde_json::from_slice(&std::fs::read(control.path().join("ready.json")).unwrap())
                .unwrap();
        let index = std::path::PathBuf::from(ready["index"].as_str().unwrap());
        let db = std::path::PathBuf::from(ready["state"].as_str().unwrap());
        let admitted = event(PAGE, "page.created");
        let body=json!({"id":admitted.id,"timestamp":admitted.timestamp,"workspace_id":WORKSPACE,"subscription_id":SUB,"integration_id":INTEGRATION,"type":admitted.event_type,"entity":{"id":PAGE,"type":"page"},"attempt_number":1}).to_string();
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(b"fixture-signing").unwrap();
        mac.update(body.as_bytes());
        let signature = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let response = reqwest::Client::new()
            .post(format!("http://127.0.0.1:{port}/webhooks/notion"))
            .header("X-Notion-Signature", format!("sha256={signature}"))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            200,
            "event must be acknowledged durably before interruption"
        );
        std::fs::write(control.path().join("begin"), b"start").unwrap();
        wait_file(&control.path().join("entered")).await;
        let guard = std::fs::File::open(&index).unwrap();
        assert!(
            guard.try_lock().is_err(),
            "the native effect owns the real index guard"
        );
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap(),
            rustix::process::Signal::TERM,
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_ok()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(child.try_wait().unwrap().is_none());
        assert!(
            guard.try_lock().is_err(),
            "shutdown must not release a live native guard"
        );
        if complete {
            std::fs::write(control.path().join("release"), b"finish").unwrap();
        }
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            status.success(),
            complete,
            "only fully drained shutdown may report success"
        );
        guard.try_lock().unwrap();
        guard.unlock().unwrap();
        let sql = rusqlite::Connection::open(&db).unwrap();
        assert_eq!(
            sql.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
        assert_eq!(
            sql.query_row("SELECT count(*) FROM shutdown_fixture", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            i64::from(complete)
        );
        assert_eq!(sql.query_row("SELECT status FROM index_commit_receipts WHERE operation_id='shutdown-native-effect'",[],|r|r.get::<_,i64>(0)).unwrap(),if complete {1}else{0});
        drop(sql);
        // Restart opens the actual same Lance/SQLite binding, then reclaims
        // authority after the crashed lease expires using the public clocked API.
        let source = HttpNotion::start().await;
        let journal = ReconciliationScope::new(vec![ROOT.into()], vec![], "fixture", "1").unwrap();
        let binding = CommitBinding::new("chunks", WORKSPACE, &journal, "1", 1).unwrap();
        let coordinator = IndexCommitCoordinator::open_existing(&index, &db, binding).unwrap();
        let metadata =
            EmbeddingMetadata::new("fixture".into(), "fixture".into(), "1".into(), 3).unwrap();
        let table =
            GuardedChunkTable::bind(&index, "chunks", metadata.clone(), coordinator.clone())
                .unwrap();
        let scope = LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        };
        let handler = AuthoritativePageRefresh::new(
            source.client(),
            table,
            coordinator.clone(),
            scope,
            &journal,
            ChunkConfig {
                target_chars: 1000,
                overlap_chars: 0,
            },
        )
        .unwrap();
        let recovery_now = now_ms() / 1000 + 7200;
        let lease = coordinator
            .state()
            .acquire_lease(recovery_now, 120)
            .unwrap();
        let inbox = InboxScope {
            workspace_id: WORKSPACE.into(),
            subscription_id: SUB.into(),
        };
        let (claim, recovered) = coordinator
            .state()
            .claim(&inbox, recovery_now, 120)
            .unwrap()
            .expect("acknowledged event must remain retryable");
        assert_eq!(recovered.event.id, admitted.id);
        let provider = Provider {
            metadata: metadata.clone(),
            count: Arc::new(AtomicUsize::new(0)),
        };
        handler
            .apply_lifecycle(
                &PageId(PAGE.into()),
                "shutdown-event-recovery",
                &provider,
                lease.clone(),
                Arc::new(move || recovery_now),
            )
            .await
            .unwrap();
        WebhookRecovery::complete(
            coordinator.state().as_ref(),
            &claim,
            recovery_now,
            ProcessingOutcome::Succeeded,
        )
        .unwrap();
        coordinator
            .state()
            .release_lease(&lease, recovery_now)
            .unwrap();
        coordinator.drain_owned().await.unwrap();
        let reopened = LanceChunkTable::open(&index, "chunks", metadata)
            .await
            .unwrap();
        assert!(
            reopened
                .page_chunks(PAGE)
                .await
                .unwrap()
                .iter()
                .any(|c| c.text.contains("Saffron"))
        );
        assert_eq!(
            coordinator
                .state()
                .event(&claim.key)
                .unwrap()
                .unwrap()
                .state,
            EventState::Succeeded
        );
    }
}
