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
        })
    }
}

pub struct Login {
    flow: AuthorizationFlow,
    redirect: NotionOAuthRedirect,
    callback: Option<CallbackRuntime>,
}
struct CallbackRuntime {
    store: Arc<GrantStore>,
    client: NotionTokenClient,
    allowed: NotionOwnerPolicy,
    admission: Arc<tokio::sync::Semaphore>,
}
impl Login {
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
            flow: AuthorizationFlow::new(
                settings.client.clone(),
                policy.allowed.authorization_identity(),
            ),
            redirect: NotionOAuthRedirect::new(registration.clone()),
            callback,
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
            let cookie = format!(
                "__Host-nk-oauth={}; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=120",
                browser.expose()
            );
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
fn browser_cookie(headers: &HeaderMap) -> Option<&str> {
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
            if name == "__Host-nk-oauth" {
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
    let result = match browser_cookie(&headers) {
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
            (
                StatusCode::FOUND,
                [
                    (header::LOCATION, destination.as_str()),
                    (
                        header::SET_COOKIE,
                        "__Host-nk-oauth=; Path=/; Secure; HttpOnly; SameSite=Lax; Max-Age=0",
                    ),
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
        || headers
            .get(header::CONTENT_ENCODING)
            .is_some_and(|v| v.as_bytes() != b"identity")
    {
        return token_error(Error::InvalidRequest, StatusCode::BAD_REQUEST);
    }
    let bytes = match body {
        Ok(bytes) => bytes,
        Err(error) => return token_error(Error::InvalidRequest, error.status()),
    };
    let result = (|| {
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
            .load()
            .map_err(|error| {
                if error == crate::notion_grant_store::StoreError::Expired {
                    Error::InvalidGrant
                } else {
                    Error::TemporarilyUnavailable
                }
            })?
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
        login.flow.redeem(
            &TokenRequest {
                grant_type: &params["grant_type"],
                code: &params["code"],
                client_id: &params["client_id"],
                redirect_uri: &params["redirect_uri"],
                resource: &params["resource"],
                code_verifier: &params["code_verifier"],
            },
            &binding,
        )
    })();
    match result {
        Ok(issued) => (StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store"), (header::PRAGMA, "no-cache")],
            axum::Json(serde_json::json!({"access_token": issued.access_token.expose(), "token_type":issued.token_type,
                "expires_in":issued.expires_in, "scope":issued.scope}))).into_response(),
        Err(error) => token_error(error, if error == Error::TemporarilyUnavailable { StatusCode::SERVICE_UNAVAILABLE } else { StatusCode::BAD_REQUEST }),
    }
}
