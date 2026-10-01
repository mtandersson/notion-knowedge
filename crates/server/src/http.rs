//! Streamable HTTP wiring; application behavior remains in the MCP crate.

use std::{io, net::SocketAddr, sync::Arc};

use axum::{
    body::{Body, to_bytes},
    extract::Request,
    http::{Method, StatusCode, header::CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};

/// Serve the shared MCP handler at `/mcp` until Ctrl-C.
pub async fn serve(bind: SocketAddr) -> io::Result<()> {
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
    let router = axum::Router::new()
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
