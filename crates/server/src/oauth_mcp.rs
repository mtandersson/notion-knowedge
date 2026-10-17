//! OAuth MCP admission. Session IDs never authenticate requests. Durable
//! logout/revocation and request-bound content adapters are separate gates.
use crate::{oauth_authorize::Login, session_limits::BoundedSessions};
use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use rmcp::transport::streamable_http_server::session::SessionId;
use std::{sync::Arc, time::Instant};
use tokio::sync::Semaphore;

#[derive(Clone)]
pub(crate) struct ResourceGate {
    pub login: Option<Arc<Login>>,
    pub sessions: Arc<BoundedSessions>,
    pub admission: Arc<Semaphore>,
    pub metadata_url: String,
}
impl ResourceGate {
    fn denied(&self) -> Response {
        let challenge = format!("Bearer resource_metadata=\"{}\"", self.metadata_url);
        (
            StatusCode::UNAUTHORIZED,
            [
                (header::CACHE_CONTROL, "no-store"),
                (header::WWW_AUTHENTICATE, challenge.as_str()),
            ],
            "MCP authorization required",
        )
            .into_response()
    }
}

pub(crate) async fn require_authorization(
    State(gate): State<ResourceGate>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(login) = &gate.login else {
        return gate.denied();
    };
    let mut headers = request.headers().get_all(header::AUTHORIZATION).iter();
    let Some(value) = headers.next().and_then(|v| v.to_str().ok()) else {
        return gate.denied();
    };
    if headers.next().is_some() {
        return gate.denied();
    }
    let Some((scheme, bearer)) = value.split_once(' ') else {
        return gate.denied();
    };
    if !scheme.eq_ignore_ascii_case("bearer")
        || bearer.len() != 43
        || !bearer
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
    {
        return gate.denied();
    }
    let Some(local) = login.local_resource_authorization(bearer) else {
        return gate.denied();
    };
    let mut session_headers = request.headers().get_all("mcp-session-id").iter();
    let session = session_headers
        .next()
        .map(|value| value.to_str().map(|s| SessionId::from(s.to_owned())));
    if session_headers.next().is_some() || session.as_ref().is_some_and(|value| value.is_err()) {
        return gate.denied();
    }
    let session = session.and_then(Result::ok);
    if session
        .as_ref()
        .is_some_and(|id| !gate.sessions.authorize_session(id, &local))
    {
        return gate.denied();
    }
    // Includes GET and DELETE. No unbounded queue can accumulate behind the
    // shared refresh mutex, even when provider introspection is stalled.
    let Ok(_permit) = gate.admission.clone().try_acquire_owned() else {
        return crate::session_limits::overload();
    };
    let auth = match login.active_resource_authorization(bearer).await {
        Ok(auth) if auth.same_session(&local) && auth.expires_at() > Instant::now() => auth,
        Err(
            crate::notion_oauth_runtime::CredentialError::Unavailable
            | crate::notion_oauth_runtime::CredentialError::Configuration,
        ) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::CACHE_CONTROL, "no-store")],
                "MCP authorization unavailable",
            )
                .into_response();
        }
        _ => return gate.denied(),
    };
    request.extensions_mut().insert(auth.clone());
    let response = next.run(request).await;
    // The SDK returns a session ID only after successful initialization. Bind
    // it before the response exposes that ID to the caller, never on client input.
    if session.is_none()
        && let Some(id) = response
            .headers()
            .get("mcp-session-id")
            .and_then(|v| v.to_str().ok())
    {
        let id = SessionId::from(id.to_owned());
        if !gate.sessions.bind_session(&id, &auth) {
            return gate.denied();
        }
    }
    response
}
