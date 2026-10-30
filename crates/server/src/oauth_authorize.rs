//! Opt-in browser entry point. This starts upstream consent and
//! can mount a browser-bound callback when sealed grant storage is installed.
//! Tokens are issued only from persisted allowlisted grants and S256 codes.
//! Resource access remains disabled until the session authorization gate.
use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{RawQuery, State, rejection::BytesRejection},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    config::Config,
    notion_grant_store::GrantStore,
    notion_oauth_callback::{NotionCallback, NotionOwnerPolicy, NotionTokenClient},
    notion_oauth_redirect::NotionOAuthRedirect,
    notion_oauth_runtime::NotionOAuthRuntime,
    oauth_code::{
        ApprovedNotionGrant, AuthorizationFlow, AuthorizationRequest, Client, Error, TokenRequest,
    },
};

#[derive(Clone)]
pub struct Settings {
    client: Client,
    local_http: bool,
}
impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OAuthClientSettings([REDACTED])")
    }
}
impl Settings {
    pub fn new(client_id: &str, redirect_uri: &str, resource: &str) -> Result<Self, Error> {
        Ok(Self {
            client: Client::new(client_id, redirect_uri, resource)?,
            local_http: false,
        })
    }
    pub(crate) fn for_discovery(
        client_id: &str,
        redirect_uri: &str,
        discovery: &crate::oauth_discovery::OAuthDiscovery,
    ) -> Result<Self, Error> {
        if discovery.is_local_http() {
            Ok(Self {
                client: Client::local_http(client_id, redirect_uri, discovery.resource())?,
                local_http: true,
            })
        } else {
            Self::new(client_id, redirect_uri, discovery.resource())
        }
    }
}

pub struct Login {
    local_http: bool,
    flow: AuthorizationFlow,
    redirect: NotionOAuthRedirect,
    callback: Option<CallbackRuntime>,
    sessions: std::sync::Mutex<Option<std::sync::Weak<crate::session_limits::BoundedSessions>>>,
    revocation_admission: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    cleanup_gate: std::sync::Mutex<Option<Arc<CleanupGate>>>,
}
#[cfg(test)]
struct CleanupGate {
    started: tokio::sync::Notify,
    proceed: tokio::sync::Semaphore,
}
struct CallbackRuntime {
    store: Arc<GrantStore>,
    credentials: NotionOAuthRuntime,
    client: NotionTokenClient,
    allowed: NotionOwnerPolicy,
    admission: Arc<tokio::sync::Semaphore>,
}
impl Login {
    pub(crate) fn attach_sessions(&self, sessions: &Arc<crate::session_limits::BoundedSessions>) {
        *self.sessions.lock().expect("session registry") = Some(Arc::downgrade(sessions));
    }
    async fn revoke_token(
        &self,
        client_id: &str,
        token: &str,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<(), Error> {
        let digest = self.flow.revoke(client_id, token)?;
        let sessions = self
            .sessions
            .lock()
            .map_err(|_| Error::TemporarilyUnavailable)?
            .as_ref()
            .and_then(std::sync::Weak::upgrade);
        if let Some(sessions) = sessions
            && sessions.has_token(&digest)
        {
            #[cfg(test)]
            let gate = self.cleanup_gate.lock().unwrap().clone();
            let cleanup = tokio::spawn(async move {
                // A disconnected HTTP observer cannot abandon remaining SDK
                // workers. Finite revocation admission remains held to completion.
                let _permit = permit;
                #[cfg(test)]
                if let Some(gate) = gate {
                    gate.started.notify_one();
                    let _release = gate.proceed.acquire().await.unwrap();
                }
                sessions.close_token(&digest).await;
            });
            cleanup.await.map_err(|_| Error::TemporarilyUnavailable)?;
        }
        Ok(())
    }

    fn cookie_name(&self) -> &'static str {
        if self.local_http {
            "nk-oauth-local"
        } else {
            "__Host-nk-oauth"
        }
    }
    fn cookie(&self, value: &str, max_age: u32) -> String {
        let secure = if self.local_http { "" } else { " Secure;" };
        format!(
            "{}={value}; Path=/;{secure} HttpOnly; SameSite=Lax; Max-Age={max_age}",
            self.cookie_name()
        )
    }

    pub(crate) fn content_factory(
        &self,
        scope: notion_knowledge_core::lifecycle::LifecycleScope,
    ) -> Option<crate::oauth_content::ContentFactory> {
        Some(crate::oauth_content::ContentFactory {
            runtime: self.callback.as_ref()?.credentials.clone(),
            scope,
        })
    }

    /// Reject invalid local credentials before any confidential provider work.
    pub(crate) fn local_resource_authorization(
        &self,
        bearer: &str,
    ) -> Option<notion_knowledge_mcp::HttpAuthorization> {
        let runtime = self.callback.as_ref()?;
        let grant = runtime.store.load_for_refresh().ok()??;
        self.flow.authorize_resource(
            bearer,
            &ApprovedNotionGrant {
                grant_id: grant.grant_id(),
                epoch: grant.epoch(),
                workspace_id: grant.workspace_id(),
                owner_type: "user",
                owner_user_id: grant.owner_user_id(),
            },
        )
    }

    pub(crate) async fn active_resource_authorization(
        &self,
        bearer: &str,
    ) -> Result<notion_knowledge_mcp::HttpAuthorization, crate::notion_oauth_runtime::CredentialError>
    {
        use crate::notion_oauth_runtime::CredentialError;
        let runtime = self.callback.as_ref().ok_or(CredentialError::NoGrant)?;
        let grant = runtime.credentials.ensure_active().await?;
        if !runtime
            .store
            .still_current(&grant)
            .map_err(|_| CredentialError::Unavailable)?
        {
            return Err(CredentialError::IdentityMismatch);
        }
        self.flow
            .authorize_resource(
                bearer,
                &ApprovedNotionGrant {
                    grant_id: grant.grant_id(),
                    epoch: grant.epoch(),
                    workspace_id: grant.workspace_id(),
                    owner_type: "user",
                    owner_user_id: grant.owner_user_id(),
                },
            )
            .ok_or(CredentialError::ReauthorizationRequired)
    }

    pub fn from_config(
        config: &Config,
        runtime: Option<&NotionOAuthRuntime>,
    ) -> Result<Option<Arc<Self>>, &'static str> {
        let Some(settings) = config.oauth_client.as_ref() else {
            return Ok(None);
        };
        let policy = config
            .notion_oauth_callback
            .as_ref()
            .ok_or("Notion OAuth callback policy unavailable")?;
        let registration = config
            .notion_oauth_redirect
            .as_ref()
            .ok_or("Notion OAuth registration unavailable")?;
        let callback = runtime
            .map(|runtime| {
                Ok::<_, &'static str>(CallbackRuntime {
                    store: runtime.grant_store(),
                    credentials: runtime.clone(),
                    client: NotionTokenClient::new(
                        registration,
                        policy.client_secret.expose_secret(),
                    )?,
                    allowed: policy.allowed.clone(),
                    admission: Arc::new(tokio::sync::Semaphore::new(4)),
                })
            })
            .transpose()?;
        Ok(Some(Arc::new(Self {
            local_http: settings.local_http,
            flow: AuthorizationFlow::new(
                settings.client.clone(),
                policy.allowed.authorization_identity(),
            )
            .with_write_policy(config.oauth_writes && !config.read_only),
            redirect: NotionOAuthRedirect::new(registration.clone()),
            callback,
            sessions: std::sync::Mutex::new(None),
            revocation_admission: Arc::new(tokio::sync::Semaphore::new(4)),
            #[cfg(test)]
            cleanup_gate: std::sync::Mutex::new(None),
        })))
    }
}

/// Strictly bounded query parsing. Reject unknown and repeated parameters,
/// including percent-encoded duplicates, rather than choosing one value.
fn parse(raw: &str, fields: &[&str]) -> Result<std::collections::HashMap<String, String>, Error> {
    if raw.is_empty()
        || raw.len() > 8192
        || !raw.is_ascii()
        || raw.bytes().any(|b| b.is_ascii_control())
        || raw.contains('#')
    {
        return Err(Error::InvalidRequest);
    }
    let bytes = raw.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        if *b == b'%'
            && (i + 2 >= bytes.len()
                || !bytes[i + 1].is_ascii_hexdigit()
                || !bytes[i + 2].is_ascii_hexdigit())
        {
            return Err(Error::InvalidRequest);
        }
    }
    let url = reqwest::Url::parse(&format!("https://query.invalid/?{raw}"))
        .map_err(|_| Error::InvalidRequest)?;
    let mut values = std::collections::HashMap::new();
    for (key, value) in url.query_pairs() {
        if !fields.contains(&key.as_ref())
            || values
                .insert(key.into_owned(), value.into_owned())
                .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    if values.len() != fields.len() {
        return Err(Error::InvalidRequest);
    }
    Ok(values)
}

pub async fn authorize(
    State(login): State<Option<Arc<Login>>>,
    RawQuery(query): RawQuery,
) -> Response {
    let Some(login) = login else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CACHE_CONTROL, "no-store")],
            "OAuth authorization not available",
        )
            .into_response();
    };
    let result = (|| {
        let params = parse(
            query.as_deref().unwrap_or_default(),
            &[
                "response_type",
                "client_id",
                "redirect_uri",
                "resource",
                "state",
                "code_challenge",
                "code_challenge_method",
                "scope",
            ],
        )?;
        let request = AuthorizationRequest {
            response_type: &params["response_type"],
            client_id: &params["client_id"],
            redirect_uri: &params["redirect_uri"],
            resource: &params["resource"],
            state: &params["state"],
            code_challenge: &params["code_challenge"],
            code_challenge_method: &params["code_challenge_method"],
            scope: &params["scope"],
        };
        login.redirect.begin_browser(&login.flow, &request)
    })();
    match result {
        Ok((url, browser)) => {
            // One pending login per browser. Starting another replaces the
            // cookie and safely invalidates that browser's earlier attempt.
            let cookie = login.cookie(browser.expose(), 120);
            (
                StatusCode::FOUND,
                [
                    (header::LOCATION, url.expose()),
                    (header::SET_COOKIE, cookie.as_str()),
                    (header::CACHE_CONTROL, "no-store"),
                    (header::REFERRER_POLICY, "no-referrer"),
                ],
            )
                .into_response()
        }
        Err(error) => (
            if error == Error::TemporarilyUnavailable {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            },
            [
                (header::CACHE_CONTROL, "no-store"),
                (header::REFERRER_POLICY, "no-referrer"),
            ],
            axum::Json(serde_json::json!({"error": error.oauth_code()})),
        )
            .into_response(),
    }
}

/// Reject ambiguous cookie headers before consuming state or contacting Notion.
fn browser_cookie<'a>(headers: &'a HeaderMap, expected_name: &str) -> Option<&'a str> {
    let mut found = None;
    let mut size = 0usize;
    for header in headers.get_all(header::COOKIE) {
        let value = header.to_str().ok()?;
        size = size.checked_add(value.len())?;
        if size > 8192 {
            return None;
        }
        for part in value.split(';') {
            let Some((name, value)) = part.trim().split_once('=') else {
                continue;
            };
            if name == expected_name {
                if found.is_some() {
                    return None;
                }
                found = Some(value);
            }
        }
    }
    found
}

pub async fn callback(
    State(login): State<Option<Arc<Login>>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
) -> Response {
    let Some(login) = login else {
        return callback_unavailable();
    };
    let Some(runtime) = login.callback.as_ref() else {
        return callback_unavailable();
    };
    // Bound provider network work separately; no queue of secret callback
    // requests, and no state consumption on overload.
    let Ok(_permit) = runtime.admission.clone().try_acquire_owned() else {
        return callback_unavailable();
    };
    let result = match browser_cookie(&headers, login.cookie_name()) {
        Some(cookie) => {
            NotionCallback::new(&login.redirect, &runtime.client, &runtime.allowed)
                .complete_browser_persist_and_approve(
                    query.as_deref().unwrap_or_default(),
                    cookie,
                    &runtime.store,
                    &login.flow,
                )
                .await
        }
        None => return callback_denied(),
    };
    match result {
        Ok(approved) => {
            // Only an operator-registered, exactly validated URI can reach
            // this point. OAuth authorization codes are PKCE-bound and never
            // contain upstream Notion credentials.
            let Ok(mut destination) = reqwest::Url::parse(&approved.redirect_uri) else {
                return callback_denied();
            };
            destination
                .query_pairs_mut()
                .append_pair("code", approved.code.expose())
                .append_pair("state", &approved.state);
            let cleared_cookie = login.cookie("", 0);
            (
                StatusCode::FOUND,
                [
                    (header::LOCATION, destination.as_str()),
                    (header::SET_COOKIE, cleared_cookie.as_str()),
                    (header::CACHE_CONTROL, "no-store"),
                    (header::REFERRER_POLICY, "no-referrer"),
                ],
            )
                .into_response()
        }
        Err(_) => callback_denied(),
    }
}

fn callback_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        "Notion authorization is not available. Please start again later.",
    )
        .into_response()
}
fn callback_denied() -> Response {
    (
        StatusCode::BAD_REQUEST,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        "Notion authorization could not be completed. Please start again.",
    )
        .into_response()
}

#[cfg(test)]
mod tests;

fn token_error(error: Error, status: StatusCode) -> Response {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        axum::Json(serde_json::json!({"error": error.oauth_code()})),
    )
        .into_response()
}

fn token_content_type(headers: &HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next().and_then(|v| v.to_str().ok()) else {
        return false;
    };
    if values.next().is_some() {
        return false;
    }
    let mut parts = value.split(';');
    if !parts
        .next()
        .unwrap_or_default()
        .trim()
        .eq_ignore_ascii_case("application/x-www-form-urlencoded")
    {
        return false;
    }
    match parts.next() {
        None => true,
        Some(charset) => {
            parts.next().is_none()
                && (charset.trim().eq_ignore_ascii_case("charset=utf-8")
                    || charset.trim().eq_ignore_ascii_case("charset=\"utf-8\""))
        }
    }
}

pub async fn token(
    State(login): State<Option<Arc<Login>>>,
    RawQuery(query): RawQuery,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let Some(login) = login else {
        return token_error(
            Error::TemporarilyUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        );
    };
    let Some(runtime) = login.callback.as_ref() else {
        return token_error(
            Error::TemporarilyUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        );
    };
    let Ok(_permit) = runtime.admission.clone().try_acquire_owned() else {
        return token_error(
            Error::TemporarilyUnavailable,
            StatusCode::SERVICE_UNAVAILABLE,
        );
    };
    if query.is_some()
        || !token_content_type(&headers)
        || headers.contains_key(header::AUTHORIZATION)
        || headers.get_all(header::CONTENT_ENCODING).iter().count() > 1
        || headers
            .get(header::CONTENT_ENCODING)
            .is_some_and(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return token_error(Error::InvalidRequest, StatusCode::BAD_REQUEST);
    }
    let bytes = match body {
        Ok(bytes) => bytes,
        Err(error) => return token_error(Error::InvalidRequest, error.status()),
    };
    let result = async {
        let raw = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidRequest)?;
        let params = parse(
            raw,
            &[
                "grant_type",
                "code",
                "client_id",
                "redirect_uri",
                "resource",
                "code_verifier",
            ],
        )?;
        let stored = runtime
            .store
            .load_for_refresh()
            .map_err(|_| Error::TemporarilyUnavailable)?
            .ok_or(Error::InvalidGrant)?;
        if !runtime
            .store
            .still_current(&stored)
            .map_err(|_| Error::TemporarilyUnavailable)?
        {
            return Err(Error::InvalidGrant);
        }
        let binding = ApprovedNotionGrant {
            grant_id: stored.grant_id(),
            epoch: stored.epoch(),
            workspace_id: stored.workspace_id(),
            owner_type: "user",
            owner_user_id: stored.owner_user_id(),
        };
        let prepared = login.flow.prepare_redemption(
            &TokenRequest {
                grant_type: &params["grant_type"],
                code: &params["code"],
                client_id: &params["client_id"],
                redirect_uri: &params["redirect_uri"],
                resource: &params["resource"],
                code_verifier: &params["code_verifier"],
            },
            &binding,
        )?;
        // Only a valid claimed code can trigger confidential provider work.
        // No bearer exists until the exact grant is actively verified.
        let active = runtime
            .credentials
            .ensure_active()
            .await
            .map_err(|error| match error {
                crate::notion_oauth_runtime::CredentialError::NoGrant
                | crate::notion_oauth_runtime::CredentialError::ReauthorizationRequired
                | crate::notion_oauth_runtime::CredentialError::IdentityMismatch => {
                    Error::InvalidGrant
                }
                crate::notion_oauth_runtime::CredentialError::Configuration
                | crate::notion_oauth_runtime::CredentialError::Unavailable => {
                    Error::TemporarilyUnavailable
                }
            })?;
        if !runtime
            .store
            .still_current(&active)
            .map_err(|_| Error::TemporarilyUnavailable)?
        {
            return Err(Error::InvalidGrant);
        }
        let active_binding = ApprovedNotionGrant {
            grant_id: active.grant_id(),
            epoch: active.epoch(),
            workspace_id: active.workspace_id(),
            owner_type: "user",
            owner_user_id: active.owner_user_id(),
        };
        login.flow.issue_redemption(prepared, &active_binding)
    }
    .await;
    match result {
        Ok(issued) => (StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store"), (header::PRAGMA, "no-cache")],
            axum::Json(serde_json::json!({"access_token": issued.access_token.expose(), "token_type":issued.token_type,
                "expires_in":issued.expires_in, "scope":issued.scope}))).into_response(),
        Err(error) => token_error(error, if error == Error::TemporarilyUnavailable { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::BAD_REQUEST }),
    }
}

/// Bounded RFC7009 public-client token revocation. No provider call or grant
/// mutation occurs; unknown and already-revoked tokens produce the same reply.
pub async fn revoke(
    State(login): State<Option<Arc<Login>>>,
    request: axum::extract::Request,
) -> Response {
    let Some(login) = login.filter(|login| login.callback.is_some()) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::CACHE_CONTROL, "no-store")],
            "OAuth revocation unavailable",
        )
            .into_response();
    };
    let Ok(permit) = login.revocation_admission.clone().try_acquire_owned() else {
        return crate::session_limits::overload();
    };
    let (parts, body) = request.into_parts();
    let result = async {
        if parts.uri.query().is_some()
            || !token_content_type(&parts.headers)
            || parts.headers.contains_key(header::AUTHORIZATION)
            || parts
                .headers
                .get_all(header::CONTENT_ENCODING)
                .iter()
                .count()
                > 1
            || parts
                .headers
                .get(header::CONTENT_ENCODING)
                .is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(Error::InvalidRequest);
        }
        let bytes = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            axum::body::to_bytes(body, 8192),
        )
        .await
        .map_err(|_| Error::InvalidRequest)?
        .map_err(|_| Error::InvalidRequest)?;
        let raw = std::str::from_utf8(&bytes).map_err(|_| Error::InvalidRequest)?;
        let params = parse(raw, &["token", "client_id"])
            .or_else(|_| parse(raw, &["token", "client_id", "token_type_hint"]))?;
        if params["token"].is_empty()
            || params["token"].len() > 256
            || params["token"]
                .chars()
                .any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(Error::InvalidRequest);
        }
        login
            .revoke_token(&params["client_id"], &params["token"], permit)
            .await
    }
    .await;
    match result {
        Ok(()) => (
            StatusCode::OK,
            [
                (header::CACHE_CONTROL, "no-store"),
                (header::PRAGMA, "no-cache"),
            ],
            "",
        )
            .into_response(),
        Err(error) => (
            if error == Error::TemporarilyUnavailable {
                StatusCode::SERVICE_UNAVAILABLE
            } else {
                StatusCode::BAD_REQUEST
            },
            [(header::CACHE_CONTROL, "no-store")],
            axum::Json(serde_json::json!({"error":error.oauth_code()})),
        )
            .into_response(),
    }
}
