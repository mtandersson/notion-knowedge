//! Headless integration identity client. Credentials never appear in diagnostics.
use std::time::Duration;

use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use reqwest::header::{AUTHORIZATION, HeaderValue};
use serde::Deserialize;

/// Explicit version for the stable get-self endpoint.
pub const NOTION_VERSION: &str = "2022-06-28";

#[derive(Clone)]
pub struct NotionClient {
    pub(crate) transport: std::sync::Arc<crate::transport::Transport>,
    pub(crate) http: reqwest::Client,
    pub(crate) authorization: HeaderValue,
    identity_url: String,
    pub(crate) api_root: String,
    pub(crate) replacement_enabled: bool,
}

impl std::fmt::Debug for NotionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NotionClient { credentials: [REDACTED] }")
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct IntegrationIdentity {
    pub id: String,
}

#[derive(Deserialize)]
struct User {
    object: String,
    id: String,
    #[serde(rename = "type")]
    kind: String,
}

fn failure(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.identity",
        retry_after: None,
    }
}

impl NotionClient {
    /// The composition root supplies a validated credential from secret configuration.
    /// No token discovery, file loading or logging occurs in the adapter.
    pub fn integration(token: &str) -> Result<Self, BackendError> {
        Self::with_identity_url(token, "https://api.notion.com/v1/users/me")
    }

    fn with_identity_url(token: &str, identity_url: &str) -> Result<Self, BackendError> {
        if token.is_empty() || token.chars().any(|c| c.is_whitespace() || c.is_control()) {
            return Err(failure(BackendErrorKind::InvalidInput));
        }
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| failure(BackendErrorKind::InvalidInput))?;
        authorization.set_sensitive(true);
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|_| failure(BackendErrorKind::Internal))?;
        Ok(Self {
            transport: Default::default(),
            http,
            authorization,
            identity_url: identity_url.to_owned(),
            api_root: "https://api.notion.com/v1".to_owned(),
            replacement_enabled: false,
        })
    }

    /// Verify the integration with GET /v1/users/me. Raw upstream errors are discarded.
    pub async fn identity(&self) -> Result<IntegrationIdentity, BackendError> {
        let response = self
            .http
            .get(&self.identity_url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", NOTION_VERSION);
        let mut response = self.send(response, true, "notion.identity").await?;
        let status = response.status();
        if !status.is_success() {
            let kind = match status.as_u16() {
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            };
            return Err(failure(kind));
        }
        // Identity metadata is small; bound reads even when Content-Length is absent.
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| failure(BackendErrorKind::Unavailable))?
        {
            if body.len() + chunk.len() > 64 * 1024 {
                return Err(failure(BackendErrorKind::Internal));
            }
            body.extend_from_slice(&chunk);
        }
        let user: User =
            serde_json::from_slice(&body).map_err(|_| failure(BackendErrorKind::Internal))?;
        if user.object != "user" || user.kind != "bot" || user.id.is_empty() {
            return Err(failure(BackendErrorKind::Internal));
        }
        Ok(IntegrationIdentity { id: user.id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn probe(status: u16, body: &str) -> (Result<IntegrationIdentity, BackendError>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/users/me", listener.local_addr().unwrap());
        let body = body.to_owned();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0; 1024];
                let n = stream.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&chunk[..n]);
                if request.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let client = NotionClient::with_identity_url("test-credential", &url)
            .unwrap()
            .without_retries();
        assert!(!format!("{client:?}").contains("test-credential"));
        (client.identity().await, server.await.unwrap())
    }

    #[tokio::test]
    async fn integration_probe_authenticates_and_returns_bot_identity() {
        let (result, request) = probe(
            200,
            r#"{"object":"user","id":"bot-id","type":"bot","bot":{}}"#,
        )
        .await;
        assert_eq!(result.unwrap().id, "bot-id");
        let request = request.to_lowercase();
        assert!(request.starts_with("get /v1/users/me http/1.1"));
        assert!(request.contains("authorization: bearer test-credential\r\n"));
        assert!(request.contains(&format!("notion-version: {NOTION_VERSION}\r\n")));
    }

    #[tokio::test]
    async fn remote_failures_are_classified_without_echoing_private_bodies() {
        for (status, kind) in [
            (401, BackendErrorKind::Unauthenticated),
            (403, BackendErrorKind::PermissionDenied),
            (429, BackendErrorKind::RateLimited),
            (503, BackendErrorKind::Unavailable),
            (302, BackendErrorKind::Internal),
        ] {
            let (result, _) = probe(status, "test-credential private upstream error").await;
            let error = result.unwrap_err();
            assert_eq!(error.kind, kind);
            assert!(!format!("{error:?} {error}").contains("test-credential"));
        }
    }

    #[tokio::test]
    async fn interrupted_identity_response_is_transient() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1/users/me", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 2048];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n{}",
                )
                .await
                .unwrap();
        });
        let client = NotionClient::with_identity_url("test-credential", &url)
            .unwrap()
            .without_retries();
        assert_eq!(
            client.identity().await.unwrap_err().kind,
            BackendErrorKind::Unavailable
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn oversized_identity_response_is_rejected() {
        assert_eq!(
            probe(200, &"x".repeat(64 * 1024 + 1))
                .await
                .0
                .unwrap_err()
                .kind,
            BackendErrorKind::Internal
        );
    }

    #[tokio::test]
    async fn identity_requires_a_valid_bot_response() {
        for body in [
            "invalid test-credential",
            r#"{"object":"user","id":"user-id","type":"person"}"#,
            r#"{"object":"user","id":"","type":"bot"}"#,
        ] {
            assert_eq!(
                probe(200, body).await.0.unwrap_err().kind,
                BackendErrorKind::Internal
            );
        }
    }
}
