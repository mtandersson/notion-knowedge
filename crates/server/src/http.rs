//! Streamable HTTP wiring; application behavior remains in the MCP crate.

use std::{io, net::SocketAddr, sync::Arc};

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

/// Serve the shared MCP handler at `/mcp` until Ctrl-C.
pub async fn serve(settings: crate::config::Config) -> io::Result<()> {
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
        || Ok(notion_knowledge_mcp::KnowledgeServer),
        Arc::new(LocalSessionManager::default()),
        config,
    );
    let diagnostics = diagnostics_router(bind, diagnostics);
    let router = axum::Router::new()
        .merge(diagnostics)
        .nest_service("/mcp", service)
        .layer(middleware::from_fn(validate_json));
    eprintln!("Serving MCP over Streamable HTTP at http://{bind}/mcp.");
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation.cancel();
        })
        .await
}

// The SDK returns plain text when deserializing malformed request bodies.
// Validate JSON at the transport boundary so these protocol failures retain
// JSON-RPC envelopes. Bound buffering; GET/SSE responses pass through untouched.
async fn validate_json(request: Request, next: Next) -> Response {
    if request.method() != Method::POST
        || !request
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("application/json"))
    {
        return next.run(request).await;
    }
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, 4 * 1024 * 1024).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return (StatusCode::PAYLOAD_TOO_LARGE, "Request body exceeds limit").into_response();
        }
    };
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
        .route("/health", get(health_response))
        .with_state(diagnostics)
        .layer(middleware::from_fn_with_state(bind, guard_diagnostics))
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
async fn guard_diagnostics(
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
