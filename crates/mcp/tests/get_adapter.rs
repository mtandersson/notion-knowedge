use notion_knowledge_core::source::*;
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

#[derive(Clone, Copy)]
enum Behavior {
    Valid,
    Missing,
    OutOfScope,
    UnauthorizedOutput,
    OversizeOutput,
    MismatchedReference,
}

struct Fixture {
    calls: Mutex<Vec<SourceExpandQuery>>,
    behavior: Behavior,
}

impl SourceExpansion for Fixture {
    fn expand(&self, query: SourceExpandQuery) -> SourceExpansionFuture<'_> {
        self.calls.lock().unwrap().push(query.clone());
        let behavior = self.behavior;
        Box::pin(async move {
            match behavior {
                Behavior::Missing => return Err(SourceExpansionError::Missing),
                Behavior::OutOfScope => return Err(SourceExpansionError::OutOfScope),
                _ => {}
            }

            let root_page_id = query.root_page_ids.first().cloned().unwrap_or_default();
            let mut remaining = query.max_chars;
            let mut sources: Vec<_> = query
                .refs
                .iter()
                .map(|reference| {
                    let (page_id, chunk_ids) = match reference {
                        StableSourceRef::Page(page_id) => {
                            (page_id.clone(), vec![format!("{page_id}-chunk")])
                        }
                        StableSourceRef::Chunk(chunk_id) => {
                            ("page-for-chunk".into(), vec![chunk_id.clone()])
                        }
                    };
                    let full = format!("Expanded content for {page_id}");
                    let text: String = full.chars().take(remaining).collect();
                    let truncated = full.chars().count() > remaining;
                    remaining = remaining.saturating_sub(text.chars().count());
                    ExpandedSource {
                        reference: reference.clone(),
                        text,
                        truncated,
                        content_scope: SourceContentScope::Indexed,
                        provenance: SourceProvenance {
                            indexed_last_edited_time: "2026-10-07T11:00:00Z".into(),
                            refreshed_last_edited_time: None,
                            index_stale: None,
                            page_id,
                            root_page_id: root_page_id.clone(),
                            url: "https://example.invalid/source".into(),
                            title: "Fixture".into(),
                            heading_path: vec!["Section".into()],
                            block_id: Some("block-1".into()),
                            chunk_ids,
                        },
                    }
                })
                .collect();

            match behavior {
                Behavior::UnauthorizedOutput => {
                    sources[0].provenance.root_page_id = "other-root".into();
                }
                Behavior::OversizeOutput => {
                    sources[0].text = "x".repeat(query.max_chars + 1);
                }
                Behavior::MismatchedReference => {
                    sources[0].reference = StableSourceRef::Page("wrong-page".into());
                }
                Behavior::Valid | Behavior::Missing | Behavior::OutOfScope => {}
            }
            Ok(sources)
        })
    }
}

async fn exchange(adapter: Arc<Fixture>, arguments: Value) -> Value {
    exchange_with_backend(adapter, arguments, None).await
}

async fn exchange_with_backend(
    adapter: Arc<Fixture>,
    arguments: Value,
    backend: Option<Arc<dyn notion_knowledge_core::backend::NotionRead>>,
) -> Value {
    let (client, server) = tokio::io::duplex(65_536);
    let task = tokio::spawn(async move {
        let mut handler =
            KnowledgeServer::with_source_expansion(adapter, vec!["root-1".into()]).unwrap();
        if let Some(backend) = backend {
            handler = handler.and_fresh_source(backend);
        }
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
    for request in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"contract","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"knowledge_get","arguments":arguments}}),
    ] {
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        read.read_line(&mut line).await.unwrap();
        if request["id"] == 2 {
            let response = serde_json::from_str(&line).unwrap();
            drop(write);
            task.abort();
            return response;
        }
        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
    }
    unreachable!()
}

#[tokio::test]
async fn expands_page_and_chunk_refs_with_server_owned_scope_and_provenance() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        behavior: Behavior::Valid,
    });
    let response = exchange(
        adapter.clone(),
        json!({
            "refs":[
                {"kind":"chunk","id":"stable-chunk"},
                {"kind":"page","id":"page-2"}
            ],
            "max_chars":256
        }),
    )
    .await;

    assert!(response.get("error").is_none());
    let sources = response["result"]["structuredContent"]["sources"]
        .as_array()
        .unwrap();
    assert_eq!(sources.len(), 2);
    assert_eq!(
        sources[0]["reference"],
        json!({"kind":"chunk","id":"stable-chunk"})
    );
    assert_eq!(
        sources[0]["provenance"]["chunk_ids"],
        json!(["stable-chunk"])
    );
    assert_eq!(sources[0]["provenance"]["root_page_id"], "root-1");
    assert_eq!(
        sources[1]["reference"],
        json!({"kind":"page","id":"page-2"})
    );
    assert_eq!(sources[1]["provenance"]["page_id"], "page-2");
    assert!(
        response["result"]["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|block| block["text"]
                .as_str()
                .is_some_and(|text| text.contains("untrusted")))
    );

    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].root_page_ids, vec!["root-1".to_owned()]);
    assert_eq!(calls[0].max_chars, 256);
    assert_eq!(calls[0].refs.len(), 2);
}

#[tokio::test]
async fn missing_and_out_of_scope_refs_share_the_same_safe_public_error() {
    let mut errors = Vec::new();
    for behavior in [Behavior::Missing, Behavior::OutOfScope] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            behavior,
        });
        let response = exchange(
            adapter,
            json!({"refs":[{"kind":"page","id":"page-1"}],"max_chars":128}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
        errors.push(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    assert_eq!(errors[0], errors[1]);
    assert!(errors[0].starts_with("source_not_accessible"));
}

#[tokio::test]
async fn invalid_arguments_never_execute_source_expansion() {
    for arguments in [
        json!({"refs":[],"max_chars":128}),
        json!({"refs":[{"kind":"page","id":"page-1"}],"max_chars":128,"freshness":"automatic"}),
        json!({"refs":[{"kind":"page","id":"page-1"}],"max_chars":0}),
        json!({"refs":[{"kind":"page","id":"   "}],"max_chars":128}),
        json!({"refs":[{"kind":"page","id":"same"},{"kind":"page","id":"same"}],"max_chars":128}),
    ] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            behavior: Behavior::Valid,
        });
        let response = exchange(adapter.clone(), arguments).await;
        assert_eq!(response["error"]["code"], -32602);
        assert!(adapter.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn scope_oversize_and_reference_mismatch_outputs_fail_closed() {
    for behavior in [
        Behavior::UnauthorizedOutput,
        Behavior::OversizeOutput,
        Behavior::MismatchedReference,
    ] {
        let adapter = Arc::new(Fixture {
            calls: Mutex::new(vec![]),
            behavior,
        });
        let response = exchange(
            adapter,
            json!({"refs":[{"kind":"chunk","id":"stable-chunk"}],"max_chars":64}),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with("retrieval_unavailable")
        );
    }
}

#[test]
fn source_expansion_requires_explicit_nonempty_root_scope() {
    let adapter = Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        behavior: Behavior::Valid,
    });
    assert!(KnowledgeServer::with_source_expansion(adapter.clone(), vec![]).is_err());
    assert!(KnowledgeServer::with_source_expansion(adapter, vec![" ".into()]).is_err());
}

use notion_knowledge_core::backend::{
    BackendError, BackendErrorKind, BackendFuture, NotionRead, Page, PageContent, PageId,
};

struct FreshFixture {
    calls: Mutex<Vec<(String, String)>>,
    edited: &'static str,
    behavior: FreshBehavior,
}

#[derive(Clone, Copy)]
enum FreshBehavior {
    Valid,
    Failed(BackendErrorKind),
    WrongPage,
    ConcurrentEdit,
    Archived,
    FailedSecondPage,
}

impl FreshFixture {
    fn page(&self, id: &PageId) -> Page {
        Page {
            id: if matches!(self.behavior, FreshBehavior::WrongPage) {
                PageId("wrong-page".into())
            } else {
                id.clone()
            },
            url: "https://example.invalid/fresh".into(),
            title: "Fresh authoritative title".into(),
            last_edited_time: self.edited.into(),
            archived: matches!(self.behavior, FreshBehavior::Archived),
            properties: Default::default(),
        }
    }
}

impl NotionRead for FreshFixture {
    fn read_content<'a>(&'a self, id: &'a PageId) -> BackendFuture<'a, PageContent> {
        self.calls
            .lock()
            .unwrap()
            .push(("content".into(), id.0.clone()));
        Box::pin(async move {
            if matches!(self.behavior, FreshBehavior::FailedSecondPage) && id.0 == "page-2" {
                return Err(BackendError {
                    kind: BackendErrorKind::Unavailable,
                    operation: "fixture",
                    retry_after: None,
                    committed_page_id: None,
                });
            }
            if let FreshBehavior::Failed(kind) = self.behavior {
                return Err(BackendError {
                    kind,
                    operation: "fixture",
                    retry_after: None,
                    committed_page_id: None,
                });
            }
            Ok(PageContent {
                page: self.page(id),
                markdown: "Färsk 🥖 authoritative page".into(),
            })
        })
    }

    fn fetch_page<'a>(&'a self, id: &'a PageId) -> BackendFuture<'a, Page> {
        self.calls
            .lock()
            .unwrap()
            .push(("metadata".into(), id.0.clone()));
        Box::pin(async move {
            let mut page = self.page(id);
            if matches!(self.behavior, FreshBehavior::ConcurrentEdit) {
                page.last_edited_time = "2026-10-07T12:00:00Z".into();
            }
            Ok(page)
        })
    }
}

fn fresh_fixture(edited: &'static str, behavior: FreshBehavior) -> Arc<FreshFixture> {
    Arc::new(FreshFixture {
        calls: Mutex::new(vec![]),
        edited,
        behavior,
    })
}

fn indexed_fixture(behavior: Behavior) -> Arc<Fixture> {
    Arc::new(Fixture {
        calls: Mutex::new(vec![]),
        behavior,
    })
}

#[tokio::test]
async fn fresh_reads_by_resolved_stable_page_id_and_reports_stale_or_unchanged_index() {
    for (edited, stale) in [
        ("2026-10-07T11:00:00Z", false),
        ("2026-10-07T11:30:00Z", true),
    ] {
        let indexed = indexed_fixture(Behavior::Valid);
        let fresh = fresh_fixture(edited, FreshBehavior::Valid);
        let response = exchange_with_backend(
            indexed.clone(),
            json!({
                "refs":[{"kind":"chunk","id":"stable-chunk"}], "max_chars":128, "freshness":"fresh"
            }),
            Some(fresh.clone()),
        )
        .await;
        let source = &response["result"]["structuredContent"]["sources"][0];
        assert_eq!(source["text"], "Färsk 🥖 authoritative page");
        assert_eq!(source["content_scope"], "page");
        assert_eq!(
            source["reference"],
            json!({"kind":"chunk","id":"stable-chunk"})
        );
        assert_eq!(source["provenance"]["page_id"], "page-for-chunk");
        assert_eq!(
            source["provenance"]["indexed_last_edited_time"],
            "2026-10-07T11:00:00Z"
        );
        assert_eq!(source["provenance"]["refreshed_last_edited_time"], edited);
        assert_eq!(source["provenance"]["index_stale"], stale);
        assert_eq!(source["provenance"]["title"], "Fresh authoritative title");
        assert_eq!(source["provenance"]["url"], "https://example.invalid/fresh");
        assert_eq!(source["provenance"]["heading_path"], json!([]));
        assert!(source["provenance"].get("block_id").is_none());
        assert_eq!(
            *fresh.calls.lock().unwrap(),
            vec![
                ("content".into(), "page-for-chunk".into()),
                ("metadata".into(), "page-for-chunk".into())
            ]
        );
        // Only the read-only expansion capability is called, exactly once.
        assert_eq!(indexed.calls.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn default_and_explicit_indexed_reads_do_not_call_notion_or_claim_refresh() {
    for freshness in [None, Some("indexed")] {
        let fresh = fresh_fixture("2026-10-07T11:30:00Z", FreshBehavior::Valid);
        let mut args = json!({"refs":[{"kind":"page","id":"page-1"}], "max_chars":128});
        if let Some(mode) = freshness {
            args["freshness"] = json!(mode);
        }
        let response =
            exchange_with_backend(indexed_fixture(Behavior::Valid), args, Some(fresh.clone()))
                .await;
        let source = &response["result"]["structuredContent"]["sources"][0];
        assert_eq!(source["text"], "Expanded content for page-1");
        assert_eq!(source["content_scope"], "indexed");
        assert!(
            source["provenance"]
                .get("refreshed_last_edited_time")
                .is_none()
        );
        assert!(source["provenance"].get("index_stale").is_none());
        assert!(fresh.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn fresh_shared_page_reads_are_consistent_and_honor_total_unicode_budget() {
    let fresh = fresh_fixture("2026-10-07T11:30:00Z", FreshBehavior::Valid);
    let response = exchange_with_backend(indexed_fixture(Behavior::Valid), json!({
        "refs":[{"kind":"chunk","id":"a"},{"kind":"chunk","id":"b"}], "max_chars":8, "freshness":"fresh"
    }), Some(fresh.clone())).await;
    let sources = response["result"]["structuredContent"]["sources"]
        .as_array()
        .unwrap();
    assert_eq!(sources[0]["text"], "Färsk 🥖 ");
    assert_eq!(sources[1]["text"], "");
    assert!(sources.iter().all(|s| s["truncated"] == true));
    assert_eq!(fresh.calls.lock().unwrap().len(), 2);
    assert_eq!(
        sources[0]["provenance"]["refreshed_last_edited_time"],
        sources[1]["provenance"]["refreshed_last_edited_time"]
    );
}

#[tokio::test]
async fn fresh_failures_are_explicit_and_never_fall_back_to_indexed_content() {
    for (behavior, prefix) in [
        (
            FreshBehavior::Failed(BackendErrorKind::Unavailable),
            "notion_unavailable",
        ),
        (
            FreshBehavior::Failed(BackendErrorKind::PermissionDenied),
            "source_not_accessible",
        ),
        (FreshBehavior::WrongPage, "notion_unavailable"),
        (FreshBehavior::Archived, "source_not_accessible"),
        (FreshBehavior::ConcurrentEdit, "notion_conflict"),
    ] {
        let response = exchange_with_backend(
            indexed_fixture(Behavior::Valid),
            json!({
                "refs":[{"kind":"page","id":"page-1"}], "max_chars":128, "freshness":"fresh"
            }),
            Some(fresh_fixture("2026-10-07T11:30:00Z", behavior)),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(response["result"]["structuredContent"].is_null());
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .starts_with(prefix)
        );
    }
    let response = exchange(
        indexed_fixture(Behavior::Valid),
        json!({
            "refs":[{"kind":"page","id":"page-1"}], "max_chars":128, "freshness":"fresh"
        }),
    )
    .await;
    assert_eq!(response["result"]["isError"], true);
    assert!(
        response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("notion_unavailable")
    );
}

#[tokio::test]
async fn fresh_cannot_bypass_indexed_identity_and_root_authorization() {
    for behavior in [
        Behavior::Missing,
        Behavior::OutOfScope,
        Behavior::UnauthorizedOutput,
        Behavior::MismatchedReference,
    ] {
        let fresh = fresh_fixture("2026-10-07T11:30:00Z", FreshBehavior::Valid);
        let response = exchange_with_backend(
            indexed_fixture(behavior),
            json!({
                "refs":[{"kind":"page","id":"arbitrary-page"}], "max_chars":128, "freshness":"fresh"
            }),
            Some(fresh.clone()),
        )
        .await;
        assert_eq!(response["result"]["isError"], true);
        assert!(fresh.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn a_later_page_failure_returns_no_partial_content_and_preserves_indexed_reads() {
    let indexed = indexed_fixture(Behavior::Valid);
    let fresh = fresh_fixture("2026-10-07T11:30:00Z", FreshBehavior::FailedSecondPage);
    let args = json!({"refs":[{"kind":"page","id":"page-1"},{"kind":"page","id":"page-2"}], "max_chars":256});
    let before = exchange(indexed.clone(), args.clone()).await;
    let mut fresh_args = args.clone();
    fresh_args["freshness"] = json!("fresh");
    let response = exchange_with_backend(indexed.clone(), fresh_args, Some(fresh.clone())).await;
    assert_eq!(response["result"]["isError"], true);
    assert!(response["result"]["structuredContent"].is_null());
    assert_eq!(
        *fresh.calls.lock().unwrap(),
        vec![
            ("content".into(), "page-1".into()),
            ("metadata".into(), "page-1".into()),
            ("content".into(), "page-2".into())
        ]
    );
    let after = exchange(indexed, args).await;
    assert_eq!(
        before["result"]["structuredContent"],
        after["result"]["structuredContent"]
    );
}

#[tokio::test]
async fn successful_fresh_reads_leave_the_subsequent_indexed_snapshot_unchanged() {
    let indexed = indexed_fixture(Behavior::Valid);
    let args = json!({"refs":[{"kind":"page","id":"page-1"}], "max_chars":256});
    let before = exchange(indexed.clone(), args.clone()).await;
    let mut fresh_args = args.clone();
    fresh_args["freshness"] = json!("fresh");
    let fresh = exchange_with_backend(
        indexed.clone(),
        fresh_args,
        Some(fresh_fixture("2026-10-07T11:30:00Z", FreshBehavior::Valid)),
    )
    .await;
    assert_eq!(
        fresh["result"]["structuredContent"]["sources"][0]["provenance"]["index_stale"],
        true
    );
    let after = exchange(indexed, args).await;
    assert_eq!(
        before["result"]["structuredContent"],
        after["result"]["structuredContent"]
    );
}
