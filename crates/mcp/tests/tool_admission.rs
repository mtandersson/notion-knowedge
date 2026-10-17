//! Exercise shared work admission through cloned newline-delimited MCP services.
use notion_knowledge_core::sync_status::{SyncStatus, SyncStatusSource};
use notion_knowledge_mcp::KnowledgeServer;
use rmcp::ServiceExt;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

struct Status {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    calls: AtomicUsize,
}
impl SyncStatusSource for Status {
    fn status(&self) -> Pin<Box<dyn Future<Output = SyncStatus> + Send + '_>> {
        Box::pin(async move {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered.notify_one();
                self.release.notified().await;
            }
            SyncStatus::default()
        })
    }
}

async fn exchange(handler: KnowledgeServer, name: &str) -> Value {
    let (client, server) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move { handler.serve(server).await.unwrap().waiting().await });
    let (read, mut write) = tokio::io::split(client);
    let mut read = BufReader::new(read);
    for request in [
        json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"admission","version":"1"}}}),
        json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":name,"arguments":{}}}),
    ] {
        write
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        assert_ne!(read.read_line(&mut line).await.unwrap(), 0);
        if request["id"] == 2 {
            task.abort();
            return serde_json::from_str(&line).unwrap();
        }
        write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
            .await
            .unwrap();
    }
    unreachable!()
}

#[tokio::test]
async fn cloned_mcp_services_reject_mixed_tools_before_arguments_or_adapters_and_recover() {
    let source = Arc::new(Status {
        entered: Default::default(),
        release: Default::default(),
        calls: AtomicUsize::new(0),
    });
    let handler = KnowledgeServer::default()
        .with_read_only(false)
        .with_tool_capacity(1)
        .and_sync_status(source.clone());
    let first_handler = handler.clone();
    let first = tokio::spawn(async move { exchange(first_handler, "knowledge_sync_status").await });
    source.entered.notified().await;
    for name in [
        "knowledge_search",
        "knowledge_get",
        "knowledge_upload_file",
        "knowledge_sync_status",
    ] {
        let result = exchange(handler.clone(), name).await;
        assert_eq!(result["result"]["isError"], true, "{result}");
        assert_eq!(
            result["result"]["content"][0]["text"],
            "tool_busy: retry later"
        );
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    source.release.notify_one();
    assert!(first.await.unwrap()["error"].is_null());
    assert!(exchange(handler, "knowledge_sync_status").await["error"].is_null());
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
}
