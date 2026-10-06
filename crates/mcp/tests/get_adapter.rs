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

            let root_page_id = query
                .root_page_ids
                .first()
                .cloned()
                .unwrap_or_default();
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
                    ExpandedSource {
                        reference: reference.clone(),
                        text: format!("Expanded content for {page_id}"),
                        truncated: false,
                        provenance: SourceProvenance {
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
    let (client, server) = tokio::io::duplex(65_536);
    let task = tokio::spawn(async move {
        KnowledgeServer::with_source_expansion(adapter, vec!["root-1".into()])
            .unwrap()
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
        response["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("untrusted")
    );

    let calls = adapter.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].root_page_ids, vec!["root-1"]);
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
    assert!(
        KnowledgeServer::with_source_expansion(adapter, vec![" ".into()])
            .is_err()
    );
}
