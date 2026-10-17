//! Opt-in browser entry point. This starts upstream consent only; it does
//! not mount a callback, issue credentials, or enable any MCP operation.
use std::sync::Arc;

use axum::{
    extract::{RawQuery, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

use crate::{
    config::Config,
    notion_oauth_redirect::NotionOAuthRedirect,
    oauth_code::{AuthorizationFlow, AuthorizationRequest, Client, Error},
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
}
impl Login {
    pub fn from_config(config: &Config) -> Option<Arc<Self>> {
        let settings = config.oauth_client.as_ref()?;
        let allowed = &config.notion_oauth_callback.as_ref()?.allowed;
        Some(Arc::new(Self {
            flow: AuthorizationFlow::new(settings.client.clone(), allowed.authorization_identity()),
            redirect: NotionOAuthRedirect::new(config.notion_oauth_redirect.as_ref()?.clone()),
        }))
    }
}

/// Strictly bounded query parsing. Reject unknown and repeated parameters,
/// including percent-encoded duplicates, rather than choosing one value.
fn parse(raw: &str) -> Result<std::collections::HashMap<String, String>, Error> {
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
        if !matches!(
            key.as_ref(),
            "response_type"
                | "client_id"
                | "redirect_uri"
                | "resource"
                | "state"
                | "code_challenge"
                | "code_challenge_method"
                | "scope"
        ) || values
            .insert(key.into_owned(), value.into_owned())
            .is_some()
        {
            return Err(Error::InvalidRequest);
        }
    }
    if values.len() != 8 {
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
        let params = parse(query.as_deref().unwrap_or_default())?;
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
