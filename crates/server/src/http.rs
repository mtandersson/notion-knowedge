//! Streamable HTTP wiring; application behavior remains in the MCP crate.

use std::{io, net::SocketAddr, sync::Arc};

/// Maximum HTTP MCP POST body, independent of its declared content type.
/// Stdio input bounds and semantic-tool output budgets are separate contracts.
const MAX_MCP_REQUEST_BYTES: usize = 4 * 1024 * 1024;

use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode, header::CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

/// Serve the shared MCP handler and orchestration probes until Ctrl-C.
pub async fn serve(settings: crate::config::Config) -> io::Result<()> {
    serve_with_handler(settings, notion_knowledge_mcp::KnowledgeServer::default()).await
}

/// Both normal and explicit experimental composition use the same transport.
pub async fn serve_with_handler(
    settings: crate::config::Config,
    handler: notion_knowledge_mcp::KnowledgeServer,
) -> io::Result<()> {
    let bind = settings.http_bind;
    let diagnostics = crate::diagnostics::bootstrap(&settings);
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let mut config = StreamableHttpServerConfig::default();
    // Keep the SDK's DNS rebinding protection, including for configured IPs.
    config.allowed_hosts.push(bind.ip().to_string());
    config.allowed_origins = vec![
        format!("http://{bind}"),
        format!("http://localhost:{}", bind.port()),
    ];
    let cancellation = config.cancellation_token.clone();
    let service = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let diagnostics = diagnostics_router(bind, diagnostics);
    let admission = match settings.webhook_state_file {
        Some(path) => {
            let store = tokio::task::spawn_blocking(move || {
                notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(path)
            })
            .await
            .map_err(|_| io::Error::other("webhook inbox unavailable"))?
            .map_err(|_| io::Error::other("webhook inbox unavailable"))?;
            Some(Arc::new(crate::webhook::DurableAdmission(
                Arc::new(store),
                settings.webhook_debounce,
            ))
                as Arc<dyn notion_knowledge_core::webhook::WebhookAdmission>)
        }
        None => None,
    };
    let webhooks = crate::webhook::router(settings.webhook, admission);
    // Discovery alone does not authenticate anyone. In that explicitly opted-in
    // mode the real MCP SDK service is NOT mounted. The discovery router always
    // challenges requests, including session/SSE and spurious bearer tokens.
    // #120/#127 must supply a verified auth layer before re-enabling it.
    let mcp = match settings.oauth_discovery {
        Some(discovery) => crate::oauth_discovery::router(bind, discovery),
        None => axum::Router::new()
            .nest_service("/mcp", service)
            .layer(middleware::from_fn(validate_json)),
    };
    let router = axum::Router::new()
        .merge(diagnostics)
        .merge(mcp)
        .merge(webhooks);
    eprintln!("Serving MCP over Streamable HTTP at http://{bind}/mcp.");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation.cancel();
        })
        .await
}

/// Docker invokes this one-shot probe from inside the running container.
/// HTTP deployments must answer their liveness endpoint. Stdio deployments
/// have no listener, so Docker process liveness is sufficient there.
pub fn container_healthcheck(settings: &crate::config::Config) -> io::Result<bool> {
    let pid1 = std::fs::read("/proc/1/cmdline")?;
    if !command_line_uses_http(&pid1) {
        return Ok(true);
    }
    probe_liveness(settings.http_bind)
}

fn command_line_uses_http(command_line: &[u8]) -> bool {
    command_line
        .split(|byte| *byte == 0)
        .any(|argument| argument == b"--http")
}

fn probe_liveness(bind: SocketAddr) -> io::Result<bool> {
    use std::io::{Read, Write};
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, TcpStream};
    use std::time::Duration;

    let ip = match bind.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    let target = SocketAddr::new(ip, bind.port());
    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&target, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    write!(
        stream,
        "GET /livez HTTP/1.1\r\nHost: localhost:{}\r\nConnection: close\r\n\r\n",
        bind.port()
    )?;
    stream.flush()?;
    let mut response = [0_u8; 64];
    let read = stream.read(&mut response)?;
    Ok(response[..read].starts_with(b"HTTP/1.1 200"))
}

// The SDK returns plain text when deserializing malformed request bodies.
// Validate JSON at the transport boundary so these protocol failures retain
// JSON-RPC envelopes. Bound buffering; GET/SSE responses pass through untouched.
async fn validate_json(request: Request, next: Next) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    // Enforce the limit *before* handing bodies to the SDK, even for an
    // unsupported media type. Otherwise non-JSON POST bodies bypass the cap.
    let is_json = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_MCP_REQUEST_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return oversized_request(),
    };
    if !is_json {
        return next
            .run(Request::from_parts(parts, Body::from(bytes)))
            .await;
    }
    let parsed = serde_json::from_slice::<Value>(&bytes);
    let request_id = parsed
        .as_ref()
        .ok()
        .and_then(|value| value.get("id"))
        .filter(|id| id.is_string() || id.is_number())
        .cloned()
        .unwrap_or(Value::Null);
    let parse_failure = match parsed {
        Err(_) => Some((json!(null), -32700, "Parse error")),
        Ok(value) => {
            let id = value
                .get("id")
                .filter(|id| id.is_string() || id.is_number())
                .cloned()
                .unwrap_or(Value::Null);
            serde_json::from_value::<rmcp::model::ClientJsonRpcMessage>(value)
                .err()
                .map(|_| (id, -32600, "Invalid Request"))
        }
    };
    let response = next
        .run(Request::from_parts(parts, Body::from(bytes)))
        .await;
    // Let SDK Host/Origin, content negotiation and session validation win.
    // Only replace its plain-text deserialization failure after those gates.
    if response.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE
        && let Some((id, code, message)) = parse_failure
    {
        return protocol_error(id, code, message);
    }
    if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
        return protocol_error(request_id, -32600, "Initialize request required");
    }
    if response.status() == StatusCode::BAD_REQUEST
        && !response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return protocol_error(request_id, -32600, "Invalid MCP protocol request");
    }
    response
}

fn oversized_request() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        axum::Json(json!({
            "jsonrpc": "2.0", "id": null,
            "error": {"code": -32000, "message": "Request body exceeds limit"}
        })),
    )
        .into_response()
}

fn protocol_error(id: Value, code: i32, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        axum::Json(json!({
            "jsonrpc": "2.0", "id": id,
            "error": {"code": code, "message": message}
        })),
    )
        .into_response()
}

/// Dependency health, not liveness. A degraded dependency yields 503 while
/// retaining its individual state in the JSON body.
fn diagnostics_router(
    bind: SocketAddr,
    diagnostics: crate::diagnostics::Diagnostics,
) -> axum::Router {
    axum::Router::new()
        .route("/livez", get(liveness_response))
        .route("/readyz", get(readiness_response))
        .route("/health", get(health_response))
        .with_state(diagnostics)
        .layer(middleware::from_fn_with_state(bind, guard_diagnostics))
}

async fn liveness_response() -> Response {
    (
        StatusCode::OK,
        [("cache-control", "no-store")],
        axum::Json(json!({
            "server": {
                "name": notion_knowledge_core::SERVER_NAME,
                "version": notion_knowledge_core::VERSION
            },
            "transport": "http",
            "status": "alive"
        })),
    )
        .into_response()
}

async fn readiness_response(
    State(diagnostics): State<crate::diagnostics::Diagnostics>,
) -> Response {
    let health = diagnostics.health();
    let ready = health.index == notion_knowledge_core::health::DependencyState::Healthy;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        [("cache-control", "no-store")],
        axum::Json(json!({
            "server": {
                "name": notion_knowledge_core::SERVER_NAME,
                "version": notion_knowledge_core::VERSION
            },
            "transport": "http",
            "status": if ready { "ready" } else { "not_ready" },
            "dependencies": {"index": health.index.as_str()}
        })),
    )
        .into_response()
}

async fn health_response(State(diagnostics): State<crate::diagnostics::Diagnostics>) -> Response {
    let health = diagnostics.health();
    let status = if health.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        [("cache-control", "no-store")],
        axum::Json(crate::diagnostics::report(health, "http")),
    )
        .into_response()
}

// The MCP service's SDK gates do not wrap sibling routes. Keep diagnostics
// private to the same configured authority/loopback deployment boundary.
pub(crate) async fn guard_diagnostics(
    State(bind): State<SocketAddr>,
    request: Request,
    next: Next,
) -> Response {
    let allowed_host = request
        .headers()
        .get("host")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<axum::http::uri::Authority>().ok())
        .is_some_and(|authority| {
            let host = authority
                .host()
                .trim_start_matches('[')
                .trim_end_matches(']');
            host.eq_ignore_ascii_case("localhost")
                || host.parse::<std::net::IpAddr>().ok().is_some_and(|ip| {
                    ip == bind.ip()
                        || ip == std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                        || ip == std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
                })
        });
    let allowed_origin = match request.headers().get("origin") {
        None => true,
        Some(value) => value.to_str().ok().is_some_and(|value| {
            value == format!("http://{bind}")
                || value == format!("http://localhost:{}", bind.port())
        }),
    };
    if !allowed_host || !allowed_origin {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::health::{DependencyState, HealthProbe};
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Probe(AtomicBool);
    impl HealthProbe for Probe {
        fn state(&self) -> DependencyState {
            if self.0.load(Ordering::Relaxed) {
                DependencyState::Healthy
            } else {
                DependencyState::Unavailable
            }
        }
    }

    #[test]
    fn container_healthcheck_identifies_http_pid1_arguments() {
        assert!(command_line_uses_http(
            b"/usr/local/bin/notion-knowledge-server\0--http\0"
        ));
        assert!(!command_line_uses_http(
            b"/usr/local/bin/notion-knowledge-server\0"
        ));
    }

    #[tokio::test]
    async fn orchestration_probes_separate_liveness_local_readiness_and_upstream_health() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        let notion = Arc::new(Probe(AtomicBool::new(false)));
        let index = Arc::new(Probe(AtomicBool::new(false)));
        let router = diagnostics_router(
            bind,
            crate::diagnostics::Diagnostics::new(notion.clone(), index.clone()),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        for (notion_healthy, index_healthy) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            notion.0.store(notion_healthy, Ordering::Relaxed);
            index.0.store(index_healthy, Ordering::Relaxed);

            let live = client
                .get(format!("http://{bind}/livez"))
                .send()
                .await
                .unwrap();
            assert_eq!(live.status(), StatusCode::OK);
            assert_eq!(live.headers()["cache-control"], "no-store");
            let live: Value = live.json().await.unwrap();
            assert_eq!(live["status"], "alive");

            let ready = client
                .get(format!("http://{bind}/readyz"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                ready.status(),
                if index_healthy {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
            assert_eq!(ready.headers()["cache-control"], "no-store");
            let ready: Value = ready.json().await.unwrap();
            assert_eq!(
                ready["status"],
                if index_healthy { "ready" } else { "not_ready" }
            );
            assert_eq!(
                ready["dependencies"]["index"],
                if index_healthy {
                    "healthy"
                } else {
                    "unavailable"
                }
            );

            let health = client
                .get(format!("http://{bind}/health"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                health.status(),
                if notion_healthy && index_healthy {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn orchestration_health_observes_current_dependency_states_and_access_policy() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        let notion = Arc::new(Probe(AtomicBool::new(true)));
        let index = Arc::new(Probe(AtomicBool::new(true)));
        let router = diagnostics_router(
            bind,
            crate::diagnostics::Diagnostics::new(notion.clone(), index.clone()),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let url = format!("http://{bind}/health");
        for (notion_healthy, index_healthy) in
            [(true, true), (false, true), (true, false), (false, false)]
        {
            notion.0.store(notion_healthy, Ordering::Relaxed);
            index.0.store(index_healthy, Ordering::Relaxed);
            let response = client.get(&url).send().await.unwrap();
            assert_eq!(
                response.status(),
                if notion_healthy && index_healthy {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
            assert_eq!(response.headers()["cache-control"], "no-store");
            let body: Value = response.json().await.unwrap();
            assert_eq!(
                body["status"],
                if notion_healthy && index_healthy {
                    "healthy"
                } else {
                    "degraded"
                }
            );
            assert_eq!(
                body["dependencies"]["notion"],
                if notion_healthy {
                    "healthy"
                } else {
                    "unavailable"
                }
            );
            assert_eq!(
                body["dependencies"]["index"],
                if index_healthy {
                    "healthy"
                } else {
                    "unavailable"
                }
            );
        }
        for host in [
            bind.to_string(),
            format!("localhost:{}", bind.port()),
            format!("[::1]:{}", bind.port()),
            "LOCALHOST:1".to_owned(),
            "[0:0:0:0:0:0:0:1]:4400".to_owned(),
        ] {
            assert_eq!(
                client
                    .get(&url)
                    .header("host", host)
                    .header("origin", format!("http://localhost:{}", bind.port()))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
        }
        for (header, value) in [
            ("host", "attacker.example"),
            ("host", "localhost.attacker.example:1"),
            ("origin", "null"),
            ("origin", "https://localhost:1"),
        ] {
            let response = client.get(&url).header(header, value).send().await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            assert_eq!(response.text().await.unwrap(), "Forbidden");
        }
        assert_eq!(
            client.post(&url).send().await.unwrap().status(),
            StatusCode::METHOD_NOT_ALLOWED
        );
        server.abort();
    }
}
