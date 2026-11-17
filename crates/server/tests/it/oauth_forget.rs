//! Installed offline reset: real processes, private synthetic sealed state,
//! and loopback HTTP only. No provider request is needed by these tests.
#![cfg(unix)]

use ring::aead::{AES_256_GCM, Aad, LessSafeKey, Nonce, UnboundKey};
use serde_json::json;
use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    process::{Output, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
    time::timeout,
};

struct Fixture {
    dir: tempfile::TempDir,
}
impl Fixture {
    fn new() -> Self {
        let this = Self {
            dir: tempfile::tempdir().unwrap(),
        };
        fs::write(this.path("key"), [42u8; 32]).unwrap();
        fs::set_permissions(this.path("key"), fs::Permissions::from_mode(0o600)).unwrap();
        this.write_grant("workspace-private", "user-private");
        this
    }
    fn path(&self, name: &str) -> std::path::PathBuf {
        self.dir.path().join(name)
    }
    fn write_grant(&self, workspace: &str, user: &str) {
        // Version-1 compatibility fixture, as in startup.rs. This fixed key
        // and nonce encrypt only synthetic test credentials.
        let key = LessSafeKey::new(UnboundKey::new(&AES_256_GCM, &[42u8; 32]).unwrap());
        let mut payload = serde_json::to_vec(&json!({
            "schema":1, "grant_id":"0123456789abcdef0123456789abcdef", "epoch":1,
            "notion_client_id":"notion-client", "workspace_id":workspace,
            "owner_user_id":user, "bot_id":"bot-private", "issued_at_unix":1,
            "expires_at_unix":null, "access_token":"access-private-sentinel",
            "refresh_token":"refresh-private-sentinel"
        }))
        .unwrap();
        key.seal_in_place_append_tag(
            Nonce::assume_unique_for_key([7; 12]),
            Aad::from(b"notion-knowledge/notion-oauth-grant/v1"),
            &mut payload,
        )
        .unwrap();
        let mut envelope = b"NKGRANT1".to_vec();
        envelope.extend_from_slice(&[7; 12]);
        envelope.extend_from_slice(&payload);
        fs::write(self.path("state"), envelope).unwrap();
        fs::set_permissions(self.path("state"), fs::Permissions::from_mode(0o600)).unwrap();
    }
    fn command(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_notion-knowledge-server"));
        c.env_clear()
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .env("NK_HTTP_HOST", "127.0.0.1")
            .env("NK_OAUTH_ISSUER", "https://auth.example.com")
            .env("NK_OAUTH_RESOURCE", "https://knowledge.example.com/mcp")
            .env("NK_OAUTH_CLIENT_ID", "chatgpt")
            .env(
                "NK_OAUTH_CLIENT_REDIRECT_URI",
                "https://chatgpt.example.com/callback",
            )
            .env("NK_NOTION_OAUTH_CLIENT_ID", "notion-client")
            .env(
                "NK_NOTION_OAUTH_REDIRECT_URI",
                "https://auth.example.com/oauth/notion/callback",
            )
            .env("NK_NOTION_OAUTH_CLIENT_SECRET", "client-private-sentinel")
            .env("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-private")
            .env("NK_NOTION_ALLOWED_USER_ID", "user-private")
            .env("NK_NOTION_GRANT_KEY_FILE", self.path("key"))
            .env("NK_NOTION_GRANT_STATE_FILE", self.path("state"));
        c
    }
    async fn run(&self, args: &[&str]) -> Output {
        let output = timeout(Duration::from_secs(15), self.command().args(args).output())
            .await
            .unwrap()
            .unwrap();
        let printed = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "client-private-sentinel",
            "access-private-sentinel",
            "refresh-private-sentinel",
        ] {
            assert!(!printed.contains(secret), "secret in command output");
        }
        assert!(
            !printed.contains(self.dir.path().to_str().unwrap()),
            "private path in command output"
        );
        output
    }
    async fn server(&self) -> (Child, String) {
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = socket.local_addr().unwrap().port();
        drop(socket);
        let mut child = self
            .command()
            .arg("--http")
            .env("NK_HTTP_PORT", port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        timeout(Duration::from_secs(15), async {
            let mut stderr = BufReader::new(child.stderr.take().unwrap());
            loop {
                let mut line = String::new();
                assert_ne!(
                    stderr.read_line(&mut line).await.unwrap(),
                    0,
                    "server exited before readiness"
                );
                if line.contains("Serving MCP over Streamable HTTP") {
                    break;
                }
            }
        })
        .await
        .unwrap();
        (child, format!("http://127.0.0.1:{port}"))
    }
}

#[tokio::test]
async fn offline_reset_excludes_live_server_and_preserves_key_and_lock_across_restart() {
    let f = Fixture::new();
    let original = fs::read(f.path("state")).unwrap();
    let key = fs::read(f.path("key")).unwrap();
    let (mut server, _) = f.server().await;
    let lock_inode = fs::metadata(f.path("state.lock")).unwrap().ino();
    assert!(!f.run(&["--notion-oauth-forget"]).await.status.success());
    assert_eq!(fs::read(f.path("state")).unwrap(), original);
    // A second process cannot become another callback/refresh coordinator.
    let competing = f.run(&["--http"]).await;
    assert!(!competing.status.success());
    assert!(
        String::from_utf8_lossy(&competing.stderr)
            .contains("Notion OAuth credential runtime unavailable")
    );
    server.kill().await.unwrap();
    server.wait().await.unwrap();

    assert!(f.run(&["--notion-oauth-forget"]).await.status.success());
    assert!(!f.path("state").exists());
    assert_eq!(fs::read(f.path("key")).unwrap(), key);
    assert_eq!(
        fs::metadata(f.path("state.lock")).unwrap().ino(),
        lock_inode
    );
    assert!(f.run(&["--notion-oauth-forget"]).await.status.success());
    assert!(f.run(&["--check"]).await.status.success());

    let (mut restarted, base) = f.server().await;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    // No grant after reset: no bearer can establish an approved binding.
    // Actual issued-token logout is tested through the joined HTTP fixture.
    for bearer in ["access-private-sentinel".to_owned(), "A".repeat(43)] {
        let response = client
            .post(format!("{base}/mcp"))
            .bearer_auth(bearer)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"tools/list"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::UNAUTHORIZED);
    }
    assert!(!f.path("state").exists());
    restarted.kill().await.unwrap();
    restarted.wait().await.unwrap();
}

#[tokio::test]
async fn offline_reset_refuses_foreign_corrupt_or_unsafe_state_without_removing_it() {
    for case in [
        "workspace",
        "user",
        "corrupt",
        "state-symlink",
        "lock-symlink",
        "lock-hardlink",
        "lock-mode",
    ] {
        let f = Fixture::new();
        match case {
            "workspace" => f.write_grant("foreign-workspace", "user-private"),
            "user" => f.write_grant("workspace-private", "foreign-user"),
            "corrupt" => fs::write(f.path("state"), b"corrupt-private-sentinel").unwrap(),
            "state-symlink" => {
                fs::rename(f.path("state"), f.path("other")).unwrap();
                symlink(f.path("other"), f.path("state")).unwrap();
            }
            "lock-symlink" => {
                fs::write(f.path("other"), b"").unwrap();
                symlink(f.path("other"), f.path("state.lock")).unwrap();
            }
            "lock-hardlink" => {
                fs::write(f.path("other"), b"").unwrap();
                fs::set_permissions(f.path("other"), fs::Permissions::from_mode(0o600)).unwrap();
                fs::hard_link(f.path("other"), f.path("state.lock")).unwrap();
            }
            "lock-mode" => {
                fs::write(f.path("state.lock"), b"").unwrap();
                fs::set_permissions(f.path("state.lock"), fs::Permissions::from_mode(0o644))
                    .unwrap();
            }
            _ => unreachable!(),
        }
        let before = fs::read(f.path("state")).unwrap();
        let key = fs::read(f.path("key")).unwrap();
        assert!(
            !f.run(&["--notion-oauth-forget"]).await.status.success(),
            "{case}"
        );
        assert_eq!(fs::read(f.path("state")).unwrap(), before, "{case}");
        assert_eq!(fs::read(f.path("key")).unwrap(), key, "{case}");
    }
}

#[tokio::test]
async fn offline_reset_requires_its_exact_standalone_command() {
    let f = Fixture::new();
    let before = fs::read(f.path("state")).unwrap();
    for args in [
        vec!["--notion-oauth-forget", "--check"],
        vec!["--notion-oauth-forget=all"],
    ] {
        assert!(!f.run(&args).await.status.success());
        assert_eq!(fs::read(f.path("state")).unwrap(), before);
    }
}
