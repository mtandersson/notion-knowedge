//! Explicit, fail-closed OAuth discovery while the authorization flow is unfinished.
//!
//! Enabling discovery does NOT enable MCP access. The authorization/token/
//! revocation endpoints only become functional in the follow-up OAuth issues.
use std::net::SocketAddr;

use axum::{
    Router,
    extract::State,
    http::{StatusCode, Uri, header},
    middleware::{self},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::json;

/// Canonical, operator-managed HTTPS endpoints. Never inferred from Host or
/// Forwarded headers, and never populated from a Notion OAuth response.
#[derive(Clone, Debug)]
pub struct OAuthDiscovery {
    issuer: String,
    resource: String,
}

impl OAuthDiscovery {
    /// The current service implements only a root issuer and /mcp resource.
    /// Restricting these shapes prevents announcing URLs which this server
    /// cannot actually answer through the well-known routes.
    pub fn new(issuer: &str, resource: &str) -> Result<Self, &'static str> {
        if !canonical_https(issuer, "") {
            return Err("NK_OAUTH_ISSUER");
        }
        if !canonical_https(resource, "/mcp") {
            return Err("NK_OAUTH_RESOURCE");
        }
        Ok(Self {
            issuer: issuer.to_owned(),
            resource: resource.to_owned(),
        })
    }

    fn protected_metadata_url(&self) -> String {
        // The root well-known route is also exposed; the suffixed URL is the
        // RFC 9728 canonical well-known location for a /mcp resource.
        format!(
            "{}/.well-known/oauth-protected-resource/mcp",
            self.resource
                .strip_suffix("/mcp")
                .expect("validated resource")
        )
    }
}

fn canonical_https(value: &str, required_path: &str) -> bool {
    if value.len() > 2048 || !value.is_ascii() {
        return false;
    }
    let Ok(uri) = value.parse::<Uri>() else {
        return false;
    };
    if uri.scheme_str() != Some("https") {
        return false;
    }
    let Some(authority) = uri.authority() else {
        return false;
    };
    // Prevent userinfo, encoded delimiters and ambiguously interpreted
    // authorities; this also rejects URL fragments and query strings by the
    // final exact-string comparison.
    let host = authority.as_str();
    // Production discovery serves canonical DNS origins on HTTPS/443 only.
    // No ambiguous explicit port, IPv6 zone, IP literal, userinfo or
    // noncanonical hostname is allowed.
    if host != host.to_ascii_lowercase()
        || !host.contains('.')
        || host.starts_with('.')
        || host.ends_with('.')
        || host.contains("..")
        || host.parse::<std::net::IpAddr>().is_ok()
        || !host.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
    {
        return false;
    }
    value == format!("https://{authority}{required_path}")
}

/// Mount only for explicitly configured HTTPS discovery. The MCP entrypoint
/// is deliberately denied even if a caller presents an arbitrary bearer.
/// Do not mount a real MCP handler in this mode until #120/#127 can validate
/// issuer, audience, grant identity/epoch and revocation on every request.
pub fn router(bind: SocketAddr, discovery: OAuthDiscovery) -> Router {
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(protected_resource),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server),
        )
        .route("/authorize", get(not_implemented))
        .route("/token", axum::routing::post(not_implemented))
        .route("/revoke", axum::routing::post(not_implemented))
        .route("/mcp", get(deny_mcp).post(deny_mcp).delete(deny_mcp))
        .with_state(discovery)
        // The SDK Host/Origin checks do not cover sibling routes. The trusted
        // reverse proxy must rewrite Host to the backend authority, as it does
        // for the existing MCP and private health routes.
        .layer(middleware::from_fn_with_state(
            bind,
            crate::http::guard_diagnostics,
        ))
}

async fn protected_resource(State(discovery): State<OAuthDiscovery>) -> Response {
    metadata_response(json!({
        "resource": discovery.resource,
        "authorization_servers": [discovery.issuer],
    }))
}

async fn authorization_server(State(discovery): State<OAuthDiscovery>) -> Response {
    let issuer = &discovery.issuer;
    metadata_response(json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "revocation_endpoint": format!("{issuer}/revoke"),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "revocation_endpoint_auth_methods_supported": ["none"],
        "protected_resources": [discovery.resource],
    }))
}

fn metadata_response(document: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::ACCESS_CONTROL_ALLOW_ORIGIN, "*"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        axum::Json(document),
    )
        .into_response()
}

async fn deny_mcp(State(discovery): State<OAuthDiscovery>) -> Response {
    let challenge = format!(
        "Bearer resource_metadata=\"{}\"",
        discovery.protected_metadata_url()
    );
    (
        StatusCode::UNAUTHORIZED,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::WWW_AUTHENTICATE, challenge.as_str()),
        ],
        "MCP OAuth authorization is not yet available",
    )
        .into_response()
}

/// The three advertised URLs exist but must fail closed until the production
/// authorization handler, grant storage and revocation are implemented.
async fn not_implemented() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::CACHE_CONTROL, "no-store")],
        "OAuth authorization not available",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_https_metadata_config_rejects_ambiguous_or_unsafe_origins() {
        assert!(
            OAuthDiscovery::new("https://login.example.com", "https://mcp.example.com/mcp").is_ok()
        );
        for issuer in [
            "http://login.example.com",
            "https://login.example.com/",
            "https://login.example.com/auth",
            "https://login.example.com?x=1",
            "https://admin@login.example.com",
            "https://login.example.com#fragment",
            "https://login.example.com%2f.evil.test",
            "https://login.example.com:bad",
            "",
        ] {
            assert!(OAuthDiscovery::new(issuer, "https://mcp.example.com/mcp").is_err());
        }
        for resource in [
            "http://mcp.example.com/mcp",
            "https://mcp.example.com",
            "https://mcp.example.com/mcp/",
            "https://mcp.example.com/mcp?token=private",
            "https://mcp.example.com:443/other",
            "https://owner@mcp.example.com/mcp",
            "https://mcp.example.com/mcp#fragment",
            "",
        ] {
            assert!(OAuthDiscovery::new("https://login.example.com", resource).is_err());
        }
    }
}
