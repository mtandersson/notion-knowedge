//! Streamable HTTP wiring; application behavior remains in the MCP crate.

use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::Semaphore;

use notion_knowledge_core::logging::{self, CorrelationId, Operation, Outcome};

/// Maximum HTTP MCP POST body, independent of its declared content type.
/// Stdio input bounds and semantic-tool output budgets are separate contracts.
const MAX_MCP_REQUEST_BYTES: usize = 4 * 1024 * 1024;

use axum::{
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{
        HeaderValue, Method, StatusCode,
        header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER, WWW_AUTHENTICATE},
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};

use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};

/// Serve the shared MCP handler and orchestration probes until Ctrl-C.
pub async fn serve(settings: crate::config::Config) -> io::Result<()> {
    serve_with_handler(settings, notion_knowledge_mcp::KnowledgeServer::default()).await
}

/// Both normal and explicit experimental composition use the same transport.
pub async fn serve_with_handler(
    settings: crate::config::Config,
    handler: notion_knowledge_mcp::KnowledgeServer,
) -> io::Result<()> {
    #[cfg(all(unix, feature = "webhook-worker"))]
    let worker = crate::webhook_worker::runtime::configured(&settings)
        .await
        .map_err(io::Error::other)?;
    let shutdown = tokio_util::sync::CancellationToken::new();
    serve_composed(
        settings,
        handler,
        #[cfg(all(unix, feature = "webhook-worker"))]
        worker.map(Arc::new),
        shutdown,
    )
    .await
}

/// Trusted composition seam for normal production serving and credential-free
/// integration fixtures. The same router, admission, cancellation and fatal
/// worker supervision run in both cases; no fixture endpoint is configurable.
pub async fn serve_composed(
    settings: crate::config::Config,
    handler: notion_knowledge_mcp::KnowledgeServer,
    #[cfg(all(unix, feature = "webhook-worker"))] worker: Option<
        Arc<crate::webhook_worker::runtime::Worker>,
    >,
    shutdown: tokio_util::sync::CancellationToken,
) -> io::Result<()> {
    let handler =
        crate::root_scope::compose(handler, &settings, crate::root_scope::Transport::Http)
            .await
            .map_err(io::Error::other)?;
    serve_installed(
        settings,
        handler,
        #[cfg(all(unix, feature = "webhook-worker"))]
        worker,
        shutdown,
    )
    .await
}

/// Serve a fully installed trusted handler through the ordinary authentication middleware.
/// Typed integration compositions can supply adapters without environment overrides.
pub async fn serve_installed(
    settings: crate::config::Config,
    handler: notion_knowledge_mcp::KnowledgeServer,
    #[cfg(all(unix, feature = "webhook-worker"))] worker: Option<
        Arc<crate::webhook_worker::runtime::Worker>,
    >,
    shutdown: tokio_util::sync::CancellationToken,
) -> io::Result<()> {
    logging::set_level(settings.log_level);
    // Construct ONE shared refresh coordinator per HTTP server. OAuth discovery
    // remains fail-closed; no MCP bearer or callback routes are enabled here.
    let _notion_oauth = crate::notion_oauth_runtime::NotionOAuthRuntime::from_config(&settings)
        .map_err(|_| io::Error::other("Notion OAuth credential runtime unavailable"))?;
    let bind = settings.http_bind;
    let mut diagnostics = crate::diagnostics::bootstrap(&settings);
    let handler = handler
        .with_tool_capacity(settings.mcp_tool_capacity)
        .with_read_only(settings.read_only)
        .with_destructive_writes_enabled(settings.destructive_writes_enabled)
        .with_capability_policy(settings.capability_policy.clone());
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let mut config = StreamableHttpServerConfig::default();
    // Keep the SDK's DNS rebinding protection, including for configured IPs.
    config.allowed_hosts.push(bind.ip().to_string());
    config.allowed_origins = vec![
        format!("http://{bind}"),
        format!("http://localhost:{}", bind.port()),
    ];
    config.cancellation_token = shutdown;
    let cancellation = config.cancellation_token.clone();
    let session_manager = Arc::new(crate::session_limits::BoundedSessions::new(
        settings.mcp_max_sessions,
        Duration::from_secs(settings.mcp_session_idle_seconds),
    ));
    let sweeper = session_manager.clone();
    let stop_sweep = cancellation.clone();
    let sweep_every = Duration::from_secs(settings.mcp_session_idle_seconds.min(30));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(sweep_every);
        loop {
            tokio::select! {
                _ = stop_sweep.cancelled() => break,
                _ = interval.tick() => sweeper.sweep().await,
            }
        }
    });
    let service =
        StreamableHttpService::new(move || Ok(handler.clone()), session_manager.clone(), config);
    let bearer = settings.bearer_fallback.map(Arc::new);
    // Operational state supplied for read activation is not webhook admission.
    // Only explicitly verified webhook mode can create or migrate its inbox.
    let admission = match (&settings.webhook, settings.webhook_state_file) {
        (crate::webhook::WebhookConfig::Verified { .. }, Some(path)) => {
            let store = tokio::task::spawn_blocking(move || {
                notion_knowledge_retrieval::sync_state::SqliteSyncStateStore::open(path)
            })
            .await
            .map_err(|_| io::Error::other("webhook inbox unavailable"))?
            .map_err(|_| io::Error::other("webhook inbox unavailable"))?;
            let store = Arc::new(store);
            diagnostics = diagnostics.with_operational_store(store.clone());
            Some(Arc::new(crate::webhook::DurableAdmission(
                store,
                settings.webhook_debounce,
            ))
                as Arc<dyn notion_knowledge_core::webhook::WebhookAdmission>)
        }
        _ => None,
    };
    let diagnostics = diagnostics_router(bind, diagnostics);
    let diagnostics = if settings.health_requires_bearer {
        // The health policy is independent of the MCP auth policy.
        diagnostics.layer(middleware::from_fn_with_state(
            bearer.clone().expect("validated health authentication"),
            require_development_bearer,
        ))
    } else {
        diagnostics
    };
    let webhooks = crate::webhook::router_with_limit(
        settings.webhook,
        admission,
        settings.webhook_max_in_flight,
    );
    // Discovery alone does not authenticate anyone. In that explicitly opted-in
    // mode the real MCP SDK service is NOT mounted. The discovery router always
    // challenges requests, including session/SSE and spurious bearer tokens.
    // #120/#127 must supply a verified auth layer before re-enabling it.
    let mcp = match settings.oauth_discovery {
        Some(discovery) => crate::oauth_discovery::router(bind, discovery),
        None => {
            let mcp = axum::Router::new()
                .nest_service("/mcp", service)
                // The session admission lock is inside JSON validation;
                // request and SSE permits are acquired before SDK dispatch.
                .layer(middleware::from_fn_with_state(
                    session_manager.clone(),
                    limit_session_initializations,
                ))
                .layer(middleware::from_fn(validate_json))
                .layer(middleware::from_fn_with_state(
                    crate::session_limits::StreamBudget::new(
                        settings.mcp_max_streams,
                        Duration::from_secs(settings.mcp_stream_max_seconds),
                    ),
                    crate::session_limits::limit_stream_responses,
                ))
                // Try-acquire happens before JSON buffering or SDK dispatch.
                // Authentication remains the outermost MCP middleware.
                .layer(middleware::from_fn_with_state(
                    Arc::new(Semaphore::new(settings.mcp_max_in_flight)),
                    limit_mcp_requests,
                ));
            if let Some(token) = bearer {
                // Authentication is the outermost MCP gate, before body parsing,
                // initialization, session lookup and all tool dispatch.
                mcp.layer(middleware::from_fn_with_state(
                    token,
                    require_development_bearer,
                ))
            } else {
                mcp
            }
        }
    };
    let router = axum::Router::new()
        .merge(diagnostics)
        .merge(mcp)
        .merge(webhooks)
        .layer(middleware::from_fn(correlate_request));
    eprintln!("Serving MCP over Streamable HTTP at http://{bind}/mcp.");
    #[cfg(all(unix, feature = "webhook-worker"))]
    let owned_coordinator = worker.as_ref().map(|worker| worker.coordinator.clone());
    #[cfg(all(unix, feature = "webhook-worker"))]
    let worker_task = worker.map(|worker| {
        let stop = cancellation.clone();
        tokio::spawn(async move {
            // Cancellation also propagates if the worker panics.
            let _cancel_on_exit = stop.clone().drop_guard();
            let result = worker.run(stop.clone()).await;
            stop.cancel();
            result
        })
    });
    let shutdown = cancellation.clone();
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = cancellation.cancelled() => {},
            }
            cancellation.cancel();
        })
        .await;
    shutdown.cancel();
    #[cfg(all(unix, feature = "webhook-worker"))]
    if let Some(task) = worker_task {
        let worker_result = task
            .await
            .map_err(|_| io::Error::other("index worker terminated"))
            .and_then(|result| result.map_err(io::Error::other));
        // The supervisor also owns cleanup after panic: a dropped observer
        // cannot release a submitted effect's guard before its thread finishes.
        if let Some(coordinator) = owned_coordinator {
            coordinator
                .drain_owned()
                .await
                .map_err(|_| io::Error::other("index drain failed"))?;
        }
        worker_result?;
    }
    result
}

/// Generated server-side IDs cannot be spoofed using client-supplied headers.
/// Instrument the outer router, including rejected requests and health probes.
async fn correlate_request(request: Request, next: Next) -> Response {
    let id = CorrelationId::new();
    let started = Instant::now();
    let mut response = logging::scope(id.clone(), next.run(request)).await;
    let status = response.status();
    response.headers_mut().insert(
        "x-correlation-id",
        HeaderValue::from_str(id.as_str()).expect("generated correlation ID is ASCII"),
    );
    let outcome = if status.is_success() {
        Outcome::Success
    } else if status.is_client_error() {
        Outcome::Rejected
    } else {
        Outcome::Failed
    };
    logging::emit(
        &id,
        Operation::HttpRequest,
        outcome,
        started.elapsed(),
        Some(status.as_u16()),
        outcome.level(),
    );
    response
}

/// Docker invokes this one-shot probe from inside the running container.
/// HTTP deployments must answer their liveness endpoint. Stdio deployments
/// have no listener, so Docker process liveness is sufficient there.
pub fn container_healthcheck(settings: &crate::config::Config) -> io::Result<bool> {
    let pid1 = std::fs::read("/proc/1/cmdline")?;
    if !command_line_uses_http(&pid1) {
        return Ok(true);
    }
    probe_liveness(
        settings.http_bind,
        if settings.health_requires_bearer {
            settings
                .bearer_fallback
                .as_ref()
                .map(|token| token.expose_secret())
        } else {
            None
        },
    )
}

fn command_line_uses_http(command_line: &[u8]) -> bool {
    command_line
        .split(|byte| *byte == 0)
        .any(|argument| argument == b"--http")
}

fn probe_liveness(bind: SocketAddr, bearer: Option<&str>) -> io::Result<bool> {
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
    // Only sent over the local loopback health probe; never printed.
    let auth_header = bearer
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "GET /livez HTTP/1.1\r\nHost: localhost:{}\r\n{auth_header}Connection: close\r\n\r\n",
        bind.port()
    )?;
    stream.flush()?;
    let mut response = [0_u8; 64];
    let read = stream.read(&mut response)?;
    Ok(response[..read].starts_with(b"HTTP/1.1 200"))
}

// Check the static developer token on *every* MCP HTTP request, including
// GET/SSE, DELETE and subsequent calls with an existing MCP session ID.
// Never include the supplied header or configured credential in responses.
async fn require_development_bearer(
    State(token): State<Arc<crate::config::SecretToken>>,
    request: Request,
    next: Next,
) -> Response {
    let mut headers = request.headers().get_all(AUTHORIZATION).iter();
    let accepted = if let (Some(header), None) = (headers.next(), headers.next()) {
        header
            .to_str()
            .ok()
            .and_then(|value| value.split_once(' '))
            .filter(|(scheme, credential)| {
                scheme.eq_ignore_ascii_case("Bearer")
                    && !credential.is_empty()
                    && !credential.bytes().any(|byte| byte.is_ascii_whitespace())
            })
            .is_some_and(|(_, presented)| {
                // HMAC verifies a fixed challenge in constant time without
                // comparing strings or exposing either credential.
                let challenge = b"notion-knowledge-developer-bearer-v1";
                let expected_key =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, token.expose_secret().as_bytes());
                let received_key =
                    ring::hmac::Key::new(ring::hmac::HMAC_SHA256, presented.as_bytes());
                let proof = ring::hmac::sign(&received_key, challenge);
                ring::hmac::verify(&expected_key, challenge, proof.as_ref()).is_ok()
            })
    } else {
        false
    };
    if !accepted {
        return (
            StatusCode::UNAUTHORIZED,
            [
                (CACHE_CONTROL, "no-store"),
                (WWW_AUTHENTICATE, "Bearer realm=\"notion-knowledge-dev\""),
            ],
            "Unauthorized",
        )
            .into_response();
    }
    next.run(request).await
}

/// Finite, zero-queue admission for potentially expensive MCP dispatch.
///
/// This permit covers request parsing/dispatch, not the lifetime of an SSE
/// response body or a protocol session. #398 owns those separate budgets.
async fn limit_mcp_requests(
    State(pool): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST && request.method() != Method::DELETE {
        // GET/SSE streams require a body-lifetime limit (#398).
        return next.run(request).await;
    }
    let Ok(_permit) = pool.try_acquire_owned() else {
        notion_knowledge_core::metrics::Resource::McpRequest.reject();
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(CACHE_CONTROL, "no-store"), (RETRY_AFTER, "1")],
            axum::Json(json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32000, "message": "MCP server busy"}
            })),
        )
            .into_response();
    };
    let _metric = notion_knowledge_core::metrics::Resource::McpRequest.enter(1);
    next.run(request).await
}

/// Set by the transport JSON validator after a syntactically valid MCP
/// initialize message is detected. It is not a client-supplied header.
#[derive(Clone)]
struct McpInitialize;

/// Serialize initialization quota checks and successful SDK allocation so
/// concurrent initialize calls cannot outrun the session manager's bound.
async fn limit_session_initializations(
    State(manager): State<Arc<crate::session_limits::BoundedSessions>>,
    request: Request,
    next: Next,
) -> Response {
    if request.extensions().get::<McpInitialize>().is_none() {
        return next.run(request).await;
    }
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    let _guard = LOCK.lock().await;
    manager.sweep().await;
    if !manager.has_capacity() {
        notion_knowledge_core::metrics::Resource::Session.reject();
        return crate::session_limits::overload();
    }
    next.run(request).await
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
    let (mut parts, body) = request.into_parts();
    // An incomplete client body must not reserve one of the finite slots
    // indefinitely. Do not time out an already-dispatched mutation here.
    let bytes = match tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(body, MAX_MCP_REQUEST_BYTES),
    )
    .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(_)) => return oversized_request(),
        Err(_) => {
            return (
                StatusCode::REQUEST_TIMEOUT,
                axum::Json(json!({
                    "jsonrpc":"2.0", "id":null,
                    "error":{"code":-32000, "message":"Request body timed out"}
                })),
            )
                .into_response();
        }
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
    if parsed.as_ref().ok().is_some_and(|value| {
        value.get("method").and_then(Value::as_str) == Some("initialize")
            && value.get("id").is_some()
    }) {
        parts.extensions.insert(McpInitialize);
    }
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
        .route("/metrics", get(metrics_response))
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
        axum::Json(crate::diagnostics::report(
            health,
            "http",
            diagnostics.read_only(),
        )),
    )
        .into_response()
}

/// Prometheus metrics share the health endpoint's host/origin and optional bearer gates.
async fn metrics_response(State(diagnostics): State<crate::diagnostics::Diagnostics>) -> Response {
    let mut output = notion_knowledge_core::metrics::render();
    if let Some(store) = diagnostics.operational_store() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|value| i64::try_from(value.as_secs()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let snapshot = tokio::task::spawn_blocking(move || store.operational_metrics(now)).await;
        let Ok(Ok(snapshot)) = snapshot else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Operational state unavailable",
            )
                .into_response();
        };
        use std::fmt::Write as _;
        output.push_str("# TYPE nk_webhook_queue_outstanding gauge\n");
        let _ = writeln!(
            output,
            "nk_webhook_queue_outstanding {}",
            snapshot.webhook_outstanding
        );
        if let Some(age) = snapshot.oldest_webhook_age_seconds {
            output.push_str("# TYPE nk_webhook_oldest_outstanding_age_seconds gauge\n");
            let _ = writeln!(output, "nk_webhook_oldest_outstanding_age_seconds {age}");
        }
        if let Some(age) = snapshot.index_change_age_seconds {
            output.push_str("# TYPE nk_index_last_change_age_seconds gauge\n");
            let _ = writeln!(output, "nk_index_last_change_age_seconds {age}");
        }
    }
    (
        StatusCode::OK,
        [
            ("cache-control", "no-store"),
            ("content-type", "text/plain; version=0.0.4; charset=utf-8"),
        ],
        output,
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

    #[tokio::test]
    async fn bounded_mcp_dispatch_rejects_before_handler_and_recovers_without_blocking_health() {
        use std::sync::atomic::AtomicUsize;
        use tokio::sync::Notify;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bind = listener.local_addr().unwrap();
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let (entered, unlock, seen) = (started.clone(), release.clone(), calls.clone());
        let router = axum::Router::new()
            .route(
                "/mcp",
                axum::routing::post(move || {
                    let (entered, unlock, seen) = (entered.clone(), unlock.clone(), seen.clone());
                    async move {
                        if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                            entered.notify_one();
                            unlock.notified().await;
                        }
                        StatusCode::ACCEPTED
                    }
                }),
            )
            .route("/livez", get(|| async { StatusCode::OK }))
            .layer(middleware::from_fn_with_state(
                Arc::new(Semaphore::new(1)),
                limit_mcp_requests,
            ));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let url = format!("http://{bind}/mcp");
        let waiting = started.notified();
        let first_client = client.clone();
        let first_url = url.clone();
        let first = tokio::spawn(async move {
            first_client
                .post(first_url)
                .body("first")
                .send()
                .await
                .unwrap()
        });
        waiting.await;
        for _ in 0..3 {
            let response = client
                .post(&url)
                .body("sensitive-request-data")
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(response.headers()[RETRY_AFTER], "1");
            assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
            let error: Value = response.json().await.unwrap();
            assert_eq!(error["error"]["message"], "MCP server busy");
            assert!(!error.to_string().contains("sensitive-request-data"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            client
                .get(format!("http://{bind}/livez"))
                .send()
                .await
                .unwrap()
                .status(),
            StatusCode::OK
        );
        release.notify_one();
        assert_eq!(first.await.unwrap().status(), StatusCode::ACCEPTED);
        assert_eq!(
            client.post(&url).send().await.unwrap().status(),
            StatusCode::ACCEPTED
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
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
        let metrics = client
            .get(format!("http://{bind}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);
        assert_eq!(metrics.headers()["cache-control"], "no-store");
        assert_eq!(
            metrics.headers()["content-type"],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        let text = metrics.text().await.unwrap();
        assert!(text.contains("nk_http_requests_total{outcome=\"success\"}"));
        assert!(text.contains("nk_search_requests_total{mode=\"hybrid\""));
        assert!(!text.contains("page_id"));
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

#[cfg(test)]
mod ingestion_tests;
