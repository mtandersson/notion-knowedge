//! Dedicated Notion webhook boundary; never logs request bodies or credentials.
use crate::config::SecretToken;
use axum::{
    Router,
    body::to_bytes,
    extract::{Request, State},
    http::StatusCode,
    routing::post,
};
use hmac::{Hmac, Mac};
use notion_knowledge_core::webhook::{WebhookAdmission, WebhookEvent};
use serde::Deserialize;
use sha2::Sha256;
use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

pub const MAX_BODY_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub enum WebhookConfig {
    Disabled,
    Setup {
        candidate_file: PathBuf,
    },
    Verified {
        token: SecretToken,
        workspace_id: String,
        integration_id: String,
        subscription_id: String,
    },
}

/// Isolate synchronous SQLite commits from the async HTTP executor. A cancelled
/// wait cannot roll back a completed receipt; redelivery observes the same row.
pub struct DurableAdmission(pub Arc<notion_knowledge_retrieval::sync_state::SqliteSyncStateStore>);
impl WebhookAdmission for DurableAdmission {
    fn admit(
        &self,
        event: WebhookEvent,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(), notion_knowledge_core::webhook::AdmissionError>,
                > + Send
                + '_,
        >,
    > {
        use notion_knowledge_core::webhook::{AdmissionError, WebhookInbox};
        let store = self.0.clone();
        Box::pin(async move {
            tokio::task::spawn_blocking(move || store.receive(&event))
                .await
                .map_err(|_| AdmissionError::Unavailable)?
                .map_err(|_| AdmissionError::Unavailable)?;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct Endpoint {
    config: Arc<WebhookConfig>,
    admission: Option<Arc<dyn WebhookAdmission>>,
}

/// No browser-origin middleware: Notion authenticates with a raw-body signature.
pub fn router(config: WebhookConfig, admission: Option<Arc<dyn WebhookAdmission>>) -> Router {
    if matches!(config, WebhookConfig::Disabled) {
        return Router::new();
    }
    Router::new()
        .route("/webhooks/notion", post(receive))
        .with_state(Endpoint {
            config: Arc::new(config),
            admission,
        })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetupPayload {
    verification_token: String,
}

#[derive(Deserialize)]
struct Envelope {
    id: String,
    timestamp: String,
    workspace_id: String,
    subscription_id: String,
    integration_id: String,
    #[serde(rename = "type")]
    event_type: String,
    entity: Entity,
    attempt_number: u32,
}
#[derive(Deserialize)]
struct Entity {
    id: String,
    #[serde(rename = "type")]
    entity_type: String,
}

pub fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
}

async fn receive(State(endpoint): State<Endpoint>, request: Request) -> StatusCode {
    let (parts, body) = request.into_parts();
    if parts.headers.contains_key("content-encoding") {
        return StatusCode::UNSUPPORTED_MEDIA_TYPE;
    }
    let signatures: Vec<_> = parts.headers.get_all("x-notion-signature").iter().collect();
    let body =
        match tokio::time::timeout(Duration::from_secs(10), to_bytes(body, MAX_BODY_BYTES)).await {
            Ok(Ok(body)) => body,
            Ok(Err(_)) => return StatusCode::PAYLOAD_TOO_LARGE,
            Err(_) => return StatusCode::REQUEST_TIMEOUT,
        };
    match endpoint.config.as_ref() {
        WebhookConfig::Disabled => StatusCode::NOT_FOUND,
        WebhookConfig::Setup { candidate_file } => {
            if !signatures.is_empty() {
                return StatusCode::UNAUTHORIZED;
            }
            let Ok(payload) = serde_json::from_slice::<SetupPayload>(&body) else {
                return StatusCode::BAD_REQUEST;
            };
            if !valid_token(&payload.verification_token) {
                return StatusCode::BAD_REQUEST;
            }
            let path = candidate_file.clone();
            let result = tokio::task::spawn_blocking(move || {
                let parent = path
                    .parent()
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
                let mut candidate = tempfile::NamedTempFile::new_in(parent)?;
                candidate.write_all(payload.verification_token.as_bytes())?;
                candidate.as_file().sync_all()?;
                // Atomic no-clobber publication. Temporary failure cleans itself
                // up; an existing regular file, directory or symlink is preserved.
                candidate
                    .persist_noclobber(&path)
                    .map_err(|error| error.error)?;
                Ok::<_, std::io::Error>(())
            })
            .await;
            match result {
                Ok(Ok(())) => StatusCode::OK,
                Ok(Err(error)) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    StatusCode::CONFLICT
                }
                _ => StatusCode::SERVICE_UNAVAILABLE,
            }
        }
        WebhookConfig::Verified {
            token,
            workspace_id,
            integration_id,
            subscription_id,
        } => {
            if signatures.len() != 1
                || !verify(token.expose_secret(), signatures[0].as_bytes(), &body)
            {
                return StatusCode::UNAUTHORIZED;
            }
            let Ok(event) = serde_json::from_slice::<Envelope>(&body) else {
                return StatusCode::BAD_REQUEST;
            };
            if ![
                &event.id,
                &event.workspace_id,
                &event.subscription_id,
                &event.integration_id,
                &event.entity.id,
            ]
            .into_iter()
            .all(|id| valid_id(id))
                || chrono::DateTime::parse_from_rfc3339(&event.timestamp).is_err()
                || event.attempt_number == 0
                || event.event_type.is_empty()
                || event.event_type.len() > 128
                || !event
                    .event_type
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b == b'.' || b == b'_')
                || !matches!(
                    event.entity.entity_type.as_str(),
                    "page" | "block" | "database" | "data_source" | "comment"
                )
            {
                return StatusCode::BAD_REQUEST;
            }
            if !event.workspace_id.eq_ignore_ascii_case(workspace_id)
                || !event.integration_id.eq_ignore_ascii_case(integration_id)
                || !event.subscription_id.eq_ignore_ascii_case(subscription_id)
            {
                return StatusCode::FORBIDDEN;
            }
            let Some(admission) = &endpoint.admission else {
                return StatusCode::SERVICE_UNAVAILABLE;
            };
            let hint = WebhookEvent {
                id: event.id,
                timestamp: event.timestamp,
                workspace_id: event.workspace_id,
                subscription_id: event.subscription_id,
                integration_id: event.integration_id,
                event_type: event.event_type,
                entity_id: event.entity.id,
                entity_type: event.entity.entity_type,
                attempt_number: event.attempt_number,
            };
            match tokio::time::timeout(Duration::from_secs(10), admission.admit(hint)).await {
                Ok(Ok(())) => StatusCode::OK,
                _ => StatusCode::SERVICE_UNAVAILABLE,
            }
        }
    }
}

pub fn valid_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 512
        && !token.chars().any(|c| c.is_whitespace() || c.is_control())
}

fn verify(token: &str, signature: &[u8], body: &[u8]) -> bool {
    let Some(hex) = signature.strip_prefix(b"sha256=") else {
        return false;
    };
    if hex.len() != 64 {
        return false;
    }
    let mut expected = [0u8; 32];
    for (i, pair) in hex.as_chunks::<2>().0.iter().enumerate() {
        let digit = |b: u8| (b as char).to_digit(16).map(|d| d as u8);
        let (Some(a), Some(b)) = (digit(pair[0]), digit(pair[1])) else {
            return false;
        };
        expected[i] = a * 16 + b;
    }
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token.as_bytes()).expect("HMAC accepts every key length");
    mac.update(body);
    mac.verify_slice(&expected).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::webhook::AdmissionError;
    use std::{ffi::OsString, sync::Mutex};
    const ID: &str = "13950b26-c203-4f3b-b97d-93ec06319565";
    const TOKEN: &str = "fixture-webhook-signing-key";
    #[derive(Default)]
    struct Sink(Mutex<Vec<WebhookEvent>>);
    impl WebhookAdmission for Sink {
        fn admit(
            &self,
            event: WebhookEvent,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = Result<(), AdmissionError>> + Send + '_>,
        > {
            Box::pin(async move {
                self.0.lock().unwrap().push(event);
                Ok(())
            })
        }
    }
    fn verified() -> WebhookConfig {
        crate::config::Config::from_lookup(|key| match key {
            "NK_WEBHOOK_MODE" => Some(OsString::from("verified")),
            "NK_WEBHOOK_VERIFICATION_TOKEN" => Some(TOKEN.into()),
            "NK_WEBHOOK_STATE_FILE" => Some("/tmp/webhook-unit-unused.sqlite".into()),
            "NK_WEBHOOK_WORKSPACE_ID"
            | "NK_WEBHOOK_INTEGRATION_ID"
            | "NK_WEBHOOK_SUBSCRIPTION_ID" => Some(ID.into()),
            _ => None,
        })
        .unwrap()
        .webhook
    }
    fn payload() -> Vec<u8> {
        format!(r#"{{ "id":"{ID}","timestamp":"2026-10-08T00:00:00Z","workspace_id":"{ID}","integration_id":"{ID}","subscription_id":"{ID}","type":"page.content_updated","entity":{{"id":"{ID}","type":"page"}},"attempt_number":1,"data":{{"private":"must never be retained"}} }}"#).into_bytes()
    }
    fn signature(body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(TOKEN.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={:x}", mac.finalize().into_bytes())
    }
    async fn server(
        config: WebhookConfig,
        sink: Option<Arc<dyn WebhookAdmission>>,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/webhooks/notion", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            axum::serve(listener, router(config, sink)).await.unwrap();
        });
        (url, handle)
    }
    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap()
    }
    #[tokio::test]
    async fn authenticated_raw_bytes_reach_admission_and_tampered_or_invalid_requests_do_not() {
        let sink = Arc::new(Sink::default());
        let (url, handle) = server(verified(), Some(sink.clone())).await;
        let body = payload();
        let sig = signature(&body);
        let client = client();
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", &sig)
                .header("origin", "https://api.notion.com")
                .body(body.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        let mut changed = body.clone();
        changed.push(b' ');
        for (signature, body) in [
            (sig, changed),
            ("sha256=00".into(), body.clone()),
            ("bogus".into(), body.clone()),
        ] {
            assert_eq!(
                client
                    .post(&url)
                    .header("x-notion-signature", signature)
                    .body(body)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::UNAUTHORIZED
            );
        }
        assert_eq!(
            client
                .post(&url)
                .body(body.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        let malformed = b"not-json";
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", signature(malformed))
                .body(malformed.to_vec())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        let mut parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        parsed["workspace_id"] = "367cba44-b6f3-4c92-81e7-6a2e9659efd4".into();
        let wrong = serde_json::to_vec(&parsed).unwrap();
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", signature(&wrong))
                .body(wrong)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        handle.abort();
    }
    #[tokio::test]
    async fn later_retries_and_new_event_names_remain_authenticated_hints() {
        let sink = Arc::new(Sink::default());
        let (url, handle) = server(verified(), Some(sink.clone())).await;
        let mut value: serde_json::Value = serde_json::from_slice(&payload()).unwrap();
        value["attempt_number"] = 9.into();
        value["type"] = "page.future_event".into();
        let body = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            client()
                .post(&url)
                .header("x-notion-signature", signature(&body))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(sink.0.lock().unwrap()[0].attempt_number, 9);
        value["attempt_number"] = 0.into();
        let body = serde_json::to_vec(&value).unwrap();
        assert_eq!(
            client()
                .post(&url)
                .header("x-notion-signature", signature(&body))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        handle.abort();
    }
    #[tokio::test]
    async fn chunked_delivery_enforces_raw_byte_authentication_and_stream_size_limit() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (url, handle) = server(verified(), None).await;
        let authority = url
            .strip_prefix("http://")
            .unwrap()
            .strip_suffix("/webhooks/notion")
            .unwrap();
        for (body, expected) in [(payload(), "503"), (vec![b' '; MAX_BODY_BYTES + 1], "413")] {
            let mut stream = tokio::net::TcpStream::connect(authority).await.unwrap();
            let head = format!(
                "POST /webhooks/notion HTTP/1.1\r\nHost: {authority}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nX-Notion-Signature: {}\r\n\r\n",
                signature(&body)
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            for chunk in body.chunks(4096) {
                stream
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .unwrap();
                stream.write_all(chunk).await.unwrap();
                stream.write_all(b"\r\n").await.unwrap();
            }
            stream.write_all(b"0\r\n\r\n").await.unwrap();
            let mut response = Vec::new();
            tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
                .await
                .unwrap()
                .unwrap();
            assert!(
                String::from_utf8(response)
                    .unwrap()
                    .starts_with(&format!("HTTP/1.1 {expected}"))
            );
        }
        handle.abort();
    }
    #[tokio::test]
    async fn missing_admission_does_not_acknowledge_and_oversize_bodies_are_rejected() {
        let (url, handle) = server(verified(), None).await;
        let client = client();
        let body = payload();
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", signature(&body))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        let body = vec![b' '; MAX_BODY_BYTES + 1];
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", signature(&body))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        handle.abort();
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn setup_preserves_existing_symlink_target() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("target");
        std::fs::write(&target, "existing").unwrap();
        let path = directory.path().join("candidate");
        std::os::unix::fs::symlink(&target, &path).unwrap();
        let (url, handle) = server(
            WebhookConfig::Setup {
                candidate_file: path,
            },
            None,
        )
        .await;
        assert_eq!(
            client()
                .post(url)
                .body(format!(r#"{{"verification_token":"{TOKEN}"}}"#))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::CONFLICT
        );
        assert_eq!(std::fs::read_to_string(target).unwrap(), "existing");
        handle.abort();
    }
    #[tokio::test]
    async fn setup_captures_private_candidate_once_without_promoting_it_to_signing_key() {
        let path = std::env::temp_dir().join(format!("nk52-candidate-{}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let (url, handle) = server(
            WebhookConfig::Setup {
                candidate_file: path.clone(),
            },
            None,
        )
        .await;
        let client = client();
        let setup = format!(r#"{{"verification_token":"{TOKEN}"}}"#);
        assert_eq!(
            client
                .post(&url)
                .body(setup.clone())
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), TOKEN);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        assert_eq!(
            client.post(&url).body(setup).send().await.unwrap().status(),
            StatusCode::CONFLICT
        );
        let body = payload();
        assert_eq!(
            client
                .post(&url)
                .header("x-notion-signature", signature(&body))
                .body(body)
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::UNAUTHORIZED
        );
        handle.abort();
        std::fs::remove_file(path).unwrap();
    }
}
