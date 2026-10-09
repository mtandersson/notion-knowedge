mod common;
use std::io;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::time::timeout;

#[tokio::test]
async fn stdio_client_initializes_discovers_tools_and_exits_on_disconnect() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .env_clear()
        .env("NK_NOTION_AUTH", "integration")
        .env("NOTION_TOKEN", "secret-sentinel")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = child.stderr.take().unwrap();

    // Exercise the wire format directly so the test is independent of the
    // server SDK's client implementation. Bound the entire conversation,
    // including EOF and process exit, to detect lifecycle regressions.
    let exchange = async {
        let mut frames = Vec::new();
        for request in [
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "stdio-smoke", "version": "1.0.0"}
            }}),
            json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
            json!({"jsonrpc": "2.0", "id": 3, "method": "ping"}),
            json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"knowledge_search","arguments":{"query":"find notes","limit":5,"mode":"hybrid","filters":{"page_ids":["page-1"]}}}}),
            json!({"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"knowledge_upload_file","arguments":{"file":{"download_url":"https://files.example.test/input?token=private-sentinel","file_id":"file_test"}}}}),
        ] {
            stdin.write_all(request.to_string().as_bytes()).await?;
            stdin.write_all(b"\n").await?;
            stdin.flush().await?;
            let mut line = String::new();
            if stdout.read_line(&mut line).await? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "missing response",
                ));
            }
            frames.push(line);
            if request["id"] == 1 {
                stdin
                    .write_all(b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n")
                    .await?;
                stdin.flush().await?;
            }
        }
        drop(stdin);
        let mut trailing = String::new();
        stdout.read_to_string(&mut trailing).await?;
        let status = child.wait().await?;
        let mut diagnostics = String::new();
        stderr.read_to_string(&mut diagnostics).await?;
        Ok::<_, io::Error>((frames, trailing, status, diagnostics))
    };

    let result = timeout(Duration::from_secs(10), exchange).await;
    if !matches!(&result, Ok(Ok(_))) {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    let (frames, trailing, status, diagnostics) = result
        .expect("stdio conversation must finish within ten seconds")
        .expect("stdio conversation must succeed");
    assert!(status.success());
    assert!(trailing.is_empty(), "unexpected stdout: {trailing}");
    assert!(diagnostics.contains("bootstrap ready"));
    assert!(diagnostics.contains("Serving MCP over stdio"));
    assert!(!diagnostics.contains("secret-sentinel"));
    assert!(!frames.join("").contains("secret-sentinel"));

    let responses: Vec<Value> = frames
        .iter()
        .map(|line| serde_json::from_str(line).expect("stdout contains only JSON protocol frames"))
        .collect();
    for (index, response) in responses.iter().enumerate() {
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], index + 1);
        assert!(response.get("error").is_none(), "{response}");
    }
    let initialized = &responses[0]["result"];
    assert_eq!(initialized["protocolVersion"], "2025-03-26");
    assert!(initialized["capabilities"]["tools"].is_object());
    assert_eq!(initialized["serverInfo"]["name"], "notion-knowledge");
    assert_eq!(
        initialized["serverInfo"]["version"],
        env!("CARGO_PKG_VERSION")
    );
    common::assert_search_catalog(&responses[1]["result"]["tools"]);
    assert_eq!(responses[2]["result"], json!({}));
    assert_eq!(responses[3]["result"]["isError"], true);
    assert!(
        responses[3]["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("retrieval_unavailable:")
    );
    assert!(responses[3]["result"].get("structuredContent").is_none());
    assert_eq!(responses[4]["result"]["isError"], true);
    assert!(responses[4]["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .starts_with("file_upload_unavailable:"));
    assert!(!frames.join("").contains("private-sentinel"));
    assert!(!diagnostics.contains("private-sentinel"));
}
