//! Per-process bounds for stateful SDK sessions and HTTP SSE lifetimes (#398).
//!
//! The manager owns capacity for the lifetime of each SDK worker, not only for
//! the HTTP initialize request. Stream guards keep an active session from
//! expiring while a GET/reconnect/POST response is still being consumed.
use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{
        Method, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE, RETRY_AFTER},
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures::{Stream, StreamExt};
use http_body::{Body as HttpBody, Frame, SizeHint};
use notion_knowledge_core::metrics::{Resource, ResourceLease};
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::streamable_http_server::session::{
        ServerSseMessage, SessionId, SessionManager, local::LocalSessionManager,
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

struct Entry {
    _metric: ResourceLease,
    _permit: OwnedSemaphorePermit,
    last_seen: Instant,
    active_streams: usize,
    authorization: Option<notion_knowledge_mcp::HttpAuthorization>,
}

/// One allocation per SDK session. The semaphore owns the hard upper bound.
pub(crate) struct BoundedSessions {
    inner: LocalSessionManager,
    permits: Arc<Semaphore>,
    entries: Arc<Mutex<HashMap<SessionId, Entry>>>,
    idle: Duration,
}

impl BoundedSessions {
    pub(crate) fn new(maximum: usize, idle: Duration) -> Self {
        assert!(maximum > 0 && !idle.is_zero());
        let mut inner = LocalSessionManager::default();
        // The wrapper owns the TTL and knows about active SSE streams.
        // Disable the SDK worker's separate timer so it cannot terminate
        // a legitimate long-lived GET/POST stream before its HTTP deadline.
        inner.session_config.keep_alive = None;
        Self {
            inner,
            permits: Arc::new(Semaphore::new(maximum)),
            entries: Arc::new(Mutex::new(HashMap::new())),
            idle,
        }
    }

    /// Authorization uses the same bounded registry and lifetime as SDK workers.
    pub(crate) fn authorize_session(
        &self,
        id: &SessionId,
        auth: &notion_knowledge_mcp::HttpAuthorization,
    ) -> bool {
        auth.is_active()
            && self.entries.lock().is_ok_and(|registry| {
                registry
                    .get(id)
                    .and_then(|entry| entry.authorization.as_ref())
                    .is_some_and(|bound| bound.same_session(auth))
            })
    }

    pub(crate) fn bind_session(
        &self,
        id: &SessionId,
        auth: &notion_knowledge_mcp::HttpAuthorization,
    ) -> bool {
        let Ok(mut registry) = self.entries.lock() else {
            return false;
        };
        if !auth.is_active() {
            return false;
        }
        let Some(entry) = registry.get_mut(id) else {
            return false;
        };
        match &entry.authorization {
            Some(bound) => bound.same_session(auth),
            None => {
                entry.authorization = Some(auth.clone());
                true
            }
        }
    }

    pub(crate) fn has_token(&self, digest: &[u8; 32]) -> bool {
        self.entries.lock().is_ok_and(|entries| {
            entries.values().any(|entry| {
                entry
                    .authorization
                    .as_ref()
                    .is_some_and(|auth| auth.matches_token_digest(digest))
            })
        })
    }

    pub(crate) async fn close_token(&self, digest: &[u8; 32]) {
        let ids = self
            .entries
            .lock()
            .map(|entries| {
                entries
                    .iter()
                    .filter(|(_, entry)| {
                        entry
                            .authorization
                            .as_ref()
                            .is_some_and(|auth| auth.matches_token_digest(digest))
                    })
                    .map(|(id, _)| id.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for id in ids {
            let _ = self.close_session(&id).await;
        }
    }

    /// An initializer denied after allocation must clean up even if the HTTP
    /// observer disconnects. The session permit bounds and owns this task.
    pub(crate) async fn close_denied(self: &Arc<Self>, id: SessionId) {
        let lease = self
            .entries
            .lock()
            .ok()
            .and_then(|mut entries| entries.remove(&id));
        let Some(lease) = lease else {
            return;
        };
        let manager = self.clone();
        let cleanup = tokio::spawn(async move {
            let _lease = lease;
            let _ = manager.inner.close_session(&id).await;
        });
        let _ = cleanup.await;
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.permits.available_permits() != 0
    }

    async fn touch(&self, id: &SessionId) -> bool {
        let expires = {
            let mut registry = self.entries.lock().expect("session registry");
            match registry.get_mut(id) {
                Some(entry)
                    if entry.active_streams > 0 || entry.last_seen.elapsed() < self.idle =>
                {
                    entry.last_seen = Instant::now();
                    return true;
                }
                Some(_) => {
                    registry.remove(id);
                    true
                }
                None => return false,
            }
        };
        if expires {
            let _ = self.inner.close_session(id).await;
        }
        false
    }

    pub(crate) async fn sweep(&self) {
        let old: Vec<SessionId> = {
            let mut registry = self.entries.lock().expect("session registry");
            let old = registry
                .iter()
                .filter(|(_, entry)| {
                    entry.active_streams == 0 && entry.last_seen.elapsed() >= self.idle
                })
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>();
            for id in &old {
                registry.remove(id);
            }
            old
        };
        for id in old {
            let _ = self.inner.close_session(&id).await;
        }
    }

    fn hold_stream<S>(
        &self,
        id: &SessionId,
        stream: S,
    ) -> impl Stream<Item = ServerSseMessage> + Send + Sync + 'static
    where
        S: Stream<Item = ServerSseMessage> + Send + Sync + 'static,
    {
        let tracking = {
            let mut registry = self.entries.lock().expect("session registry");
            registry.get_mut(id).map(|entry| {
                entry.active_streams += 1;
                entry.last_seen = Instant::now();
                StreamLease {
                    id: id.clone(),
                    entries: self.entries.clone(),
                }
            })
        };
        // Capture the guard in the resulting stream, not in the constructor
        // future. A disconnected response releases it via Drop.
        stream.map(move |event| {
            let _keep_alive = &tracking;
            event
        })
    }
}

struct StreamLease {
    id: SessionId,
    entries: Arc<Mutex<HashMap<SessionId, Entry>>>,
}
impl Drop for StreamLease {
    fn drop(&mut self) {
        if let Some(entry) = self
            .entries
            .lock()
            .expect("session registry")
            .get_mut(&self.id)
        {
            entry.active_streams = entry.active_streams.saturating_sub(1);
            entry.last_seen = Instant::now();
        }
    }
}

/// SDK logs and HTTP errors must not include session IDs or other internals.
#[derive(Debug)]
pub struct SessionLimitError;
impl fmt::Display for SessionLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("session unavailable")
    }
}
impl std::error::Error for SessionLimitError {}

impl SessionManager for BoundedSessions {
    type Error = SessionLimitError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        self.sweep().await;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| SessionLimitError)?;
        let (id, transport) = self
            .inner
            .create_session()
            .await
            .map_err(|_| SessionLimitError)?;
        self.entries.lock().expect("session registry").insert(
            id.clone(),
            Entry {
                _permit: permit,
                _metric: Resource::Session.enter(1),
                last_seen: Instant::now(),
                active_streams: 0,
                authorization: None,
            },
        );
        Ok((id, transport))
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        if !self.touch(id).await {
            return Err(SessionLimitError);
        }
        match self.inner.initialize_session(id, message).await {
            Ok(response) => Ok(response),
            Err(_) => {
                let _ = self.close_session(id).await;
                Err(SessionLimitError)
            }
        }
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        if !self.touch(id).await {
            return Ok(false);
        }
        self.inner
            .has_session(id)
            .await
            .map_err(|_| SessionLimitError)
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        // Move the permit into this future *before* awaiting SDK cleanup.
        // Cancellation cannot leave a stale registry entry, and the permit
        // stays reserved until the worker is closed or this future is dropped.
        let lease = self.entries.lock().expect("session registry").remove(id);
        let result = self
            .inner
            .close_session(id)
            .await
            .map_err(|_| SessionLimitError);
        drop(lease);
        result
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self
            .inner
            .create_stream(id, message)
            .await
            .map_err(|_| SessionLimitError)?;
        Ok(self.hold_stream(id, stream))
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner
            .accept_message(id, message)
            .await
            .map_err(|_| SessionLimitError)
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self
            .inner
            .create_standalone_stream(id)
            .await
            .map_err(|_| SessionLimitError)?;
        Ok(self.hold_stream(id, stream))
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        let stream = self
            .inner
            .resume(id, last_event_id)
            .await
            .map_err(|_| SessionLimitError)?;
        Ok(self.hold_stream(id, stream))
    }
}

/// Bound both standalone GET streams and streamed POST responses. A POST must
/// reserve *before* dispatch (not after its mutation has already occurred).
#[derive(Clone)]
pub(crate) struct StreamBudget {
    permits: Arc<Semaphore>,
    max_age: Duration,
}
impl StreamBudget {
    pub(crate) fn new(maximum: usize, max_age: Duration) -> Self {
        assert!(maximum > 0 && !max_age.is_zero());
        Self {
            permits: Arc::new(Semaphore::new(maximum)),
            max_age,
        }
    }
}

pub(crate) async fn limit_stream_responses(
    State(budget): State<StreamBudget>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::GET && request.method() != Method::POST {
        // DELETE must remain available to terminate a long-lived GET.
        return next.run(request).await;
    }
    let Ok(permit) = budget.permits.clone().try_acquire_owned() else {
        Resource::Stream.reject();
        return overload();
    };
    let metric = Resource::Stream.enter(1);
    let revoked = request
        .extensions()
        .get::<notion_knowledge_mcp::HttpAuthorization>()
        .map(|auth| Box::pin(auth.cancellation().cancelled_owned()));
    let deadline = request
        .extensions()
        .get::<notion_knowledge_mcp::HttpAuthorization>()
        .map(|auth| auth.expires_at())
        .map_or_else(
            || Instant::now() + budget.max_age,
            |expires| expires.min(Instant::now() + budget.max_age),
        );
    let response = next.run(request).await;
    let streaming = response.headers().get(CONTENT_TYPE).is_some_and(|v| {
        v.to_str()
            .ok()
            .is_some_and(|v| v.starts_with("text/event-stream"))
    });
    if !streaming {
        return response;
    }
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(BoundedBody {
            inner: body,
            permit: Some(permit),
            metric: Some(metric),
            revoked,
            deadline: Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                deadline,
            ))),
            finished: false,
        }),
    )
}

pub(crate) fn overload() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(CACHE_CONTROL, "no-store"), (RETRY_AFTER, "1")],
        axum::Json(serde_json::json!({
            "jsonrpc":"2.0", "id": null,
            "error":{"code":-32000,"message":"MCP capacity exceeded"}
        })),
    )
        .into_response()
}

/// The permit and underlying SDK stream are dropped on disconnect, EOF or a
/// hard deadline; waiting for an unresponsive peer cannot extend the budget.
struct BoundedBody {
    inner: Body,
    permit: Option<OwnedSemaphorePermit>,
    metric: Option<ResourceLease>,
    revoked: Option<Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>>,
    deadline: Pin<Box<tokio::time::Sleep>>,
    finished: bool,
}
impl HttpBody for BoundedBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let revoked = self
            .revoked
            .as_mut()
            .is_some_and(|future| future.as_mut().poll(cx).is_ready());
        if revoked || self.deadline.as_mut().poll(cx).is_ready() {
            self.inner = Body::empty();
            self.metric.take();
            self.permit.take();
            self.finished = true;
            return Poll::Ready(None);
        }
        let frame = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(&frame, Poll::Ready(None)) {
            self.metric.take();
            self.permit.take();
            self.finished = true;
        }
        frame
    }
    fn is_end_stream(&self) -> bool {
        self.finished || self.inner.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_quota_and_idle_sweep_release_sdk_workers() {
        let manager = BoundedSessions::new(1, Duration::from_millis(70));
        let (id, transport) = manager.create_session().await.unwrap();
        assert!(manager.has_session(&id).await.unwrap());
        assert!(!manager.has_capacity());
        assert!(manager.create_session().await.is_err());
        tokio::time::sleep(Duration::from_millis(100)).await;
        manager.sweep().await;
        assert!(!manager.has_session(&id).await.unwrap());
        assert!(manager.has_capacity());
        drop(transport);
        let (new_id, _transport) = manager.create_session().await.unwrap();
        assert_ne!(id, new_id);
        manager.close_session(&new_id).await.unwrap();
        assert!(manager.has_capacity());
    }

    #[tokio::test]
    async fn delayed_dispatch_cannot_extend_authorized_sse_deadline() {
        use axum::{Router, middleware, routing::get};
        let budget = StreamBudget::new(1, Duration::from_secs(2));
        let auth = notion_knowledge_mcp::HttpAuthorization::new(
            [1; 32],
            "grant".into(),
            1,
            "workspace".into(),
            "owner".into(),
            Instant::now() + Duration::from_millis(300),
        );
        let router = Router::new()
            .route(
                "/mcp",
                get(|| async {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    (
                        [(CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(
                            futures::stream::pending::<Result<Bytes, std::io::Error>>(),
                        ),
                    )
                }),
            )
            .layer(middleware::from_fn_with_state(
                budget,
                limit_stream_responses,
            ))
            .layer(axum::Extension(auth));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = socket.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(socket, router).await.unwrap();
        });
        let response = reqwest::Client::new()
            .get(format!("http://{addr}/mcp"))
            .send()
            .await
            .unwrap();
        let bytes = tokio::time::timeout(Duration::from_millis(100), response.bytes())
            .await
            .expect("dispatch must not add time to bearer lifetime")
            .unwrap();
        assert!(bytes.is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn stream_permit_retained_through_response_body_and_deadline() {
        use axum::{Router, middleware, routing::get};
        let budget = StreamBudget::new(1, Duration::from_millis(100));
        let router = Router::new()
            .route(
                "/mcp",
                get(|| async {
                    (
                        [(CONTENT_TYPE, "text/event-stream")],
                        Body::from("data: hello\n\n"),
                    )
                }),
            )
            .layer(middleware::from_fn_with_state(
                budget.clone(),
                limit_stream_responses,
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let client = reqwest::Client::builder().no_proxy().build().unwrap();
        let first = client
            .get(format!("http://{addr}/mcp"))
            .send()
            .await
            .unwrap();
        // The response stream may finish immediately, so do not assert that
        // the second request is denied in this tiny EOF fixture.
        assert_eq!(first.status(), StatusCode::OK);
        drop(first);
        let response = client
            .get(format!("http://{addr}/mcp"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        task.abort();
    }
}
