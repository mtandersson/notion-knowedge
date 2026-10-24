#![cfg(unix)]
#[path = "support/listener_shutdown.rs"]
mod listener_shutdown;
use std::{process::Stdio, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    time::timeout,
};

async fn server(seconds: u64) -> (Child, BufReader<tokio::process::ChildStderr>, u16) {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = socket.local_addr().unwrap().port();
    drop(socket);
    let mut child = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"))
        .arg("--http")
        .env_clear()
        .env("NK_HTTP_PORT", port.to_string())
        .env("NK_SHUTDOWN_TIMEOUT_SECONDS", seconds.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    timeout(Duration::from_secs(10), async {
        loop {
            let mut line = String::new();
            assert_ne!(
                stderr.read_line(&mut line).await.unwrap(),
                0,
                "server exited before listening"
            );
            if line.contains("Serving MCP over Streamable HTTP") {
                break;
            }
        }
        reqwest::get(format!("http://127.0.0.1:{port}/livez"))
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
    })
    .await
    .unwrap();
    (child, stderr, port)
}
fn signal(child: &Child, signal: rustix::process::Signal) {
    rustix::process::kill_process(
        rustix::process::Pid::from_raw(child.id().unwrap() as i32).unwrap(),
        signal,
    )
    .unwrap();
}

#[tokio::test]
async fn sigint_and_sigterm_stop_the_shipping_http_process_cleanly() {
    for sig in [rustix::process::Signal::INT, rustix::process::Signal::TERM] {
        let (mut child, _, port) = server(2).await;
        signal(&child, sig);
        let status = timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            status.success(),
            "ordinary signal must perform clean shutdown: {status}"
        );
        assert!(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn shutdown_closes_listener_before_deadline_and_reports_incomplete_request_cleanup() {
    let (mut child, mut stderr, port) = server(1).await;
    let mut held = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    held.write_all(format!("POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\nContent-Length: 4000\r\nExpect: 100-continue\r\n\r\n").as_bytes()).await.unwrap();
    // HTTP100 confirms the actual router is polling this incomplete request.
    let mut response = Vec::new();
    timeout(Duration::from_secs(3), async {
        while !response.ends_with(b"\r\n\r\n") {
            response.push(held.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    assert!(
        String::from_utf8(response)
            .unwrap()
            .contains("100 Continue")
    );
    held.write_all(b"{").await.unwrap();
    signal(&child, rustix::process::Signal::TERM);
    listener_shutdown::wait_listener_closed(port).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "listener must close before waiting for unfinished work"
    );
    let status = timeout(Duration::from_secs(4), child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !status.success(),
        "deadline expiry must not claim clean completion"
    );
    let mut output = String::new();
    stderr.read_to_string(&mut output).await.unwrap();
    assert!(output.contains("shutdown deadline exceeded"));
}
