//! Typed startup configuration. Errors identify settings without exposing values.

use std::{env, ffi::OsString, fmt, net::SocketAddr};

#[derive(Debug)]
pub struct Config {
    pub http_bind: SocketAddr,
    /// Fail-closed MCP access mode; enabled unless explicitly opted out.
    pub read_only: bool,
    /// Opt-in discovery mode; all MCP calls are denied pending real OAuth.
    pub oauth_discovery: Option<crate::oauth_discovery::OAuthDiscovery>,
    /// Optional, trusted Notion authorization redirect config; NOT a live HTTP login.
    pub notion_oauth_redirect: Option<crate::notion_oauth_redirect::NotionOAuthConfig>,
    /// Confidential Notion code exchange config, never a live MCP auth grant.
    pub notion_oauth_callback: Option<NotionCallbackSettings>,
    /// Explicit opt-in sealed Notion grant state, never activated as MCP auth.
    pub notion_grant_store: Option<GrantStoreSettings>,
    pub notion_auth: NotionAuth,
    pub webhook: crate::webhook::WebhookConfig,
    pub webhook_debounce: notion_knowledge_core::webhook::DebounceWindow,
    pub webhook_state_file: Option<std::path::PathBuf>,
}

/// Secrets are excluded from Debug; allowed identifiers are provisioned,
/// never enrolled from a Notion token response.
#[derive(Debug)]
pub struct NotionCallbackSettings {
    pub client_secret: SecretToken,
    pub allowed: crate::notion_oauth_callback::NotionOwnerPolicy,
}

/// Paths refer to operator-managed secret key and private encrypted state.
pub struct GrantStoreSettings {
    key_file: std::path::PathBuf,
    state_file: std::path::PathBuf,
}
impl fmt::Debug for GrantStoreSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GrantStoreSettings([REDACTED])")
    }
}

#[derive(Debug)]
pub enum NotionAuth {
    None,
    Integration(SecretToken),
}

/// A credential whose debug representation never contains its value.
pub struct SecretToken(String);

impl SecretToken {
    /// Access the credential explicitly when wiring the Notion adapter.
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ConfigError {
    pub setting: &'static str,
    pub reason: &'static str,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.setting, self.reason)
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Validate the encrypted state before serving any HTTP or stdio MCP.
    /// Missing, corrupt, wrong-key, expired, and foreign grants are never
    /// interpreted as an authenticated principal.
    pub fn validate_grant_store(&self) -> Result<(), crate::notion_grant_store::StoreError> {
        let Some(settings) = &self.notion_grant_store else {
            return Ok(());
        };
        let callback = self
            .notion_oauth_callback
            .as_ref()
            .ok_or(crate::notion_grant_store::StoreError::Configuration)?;
        let registration = self
            .notion_oauth_redirect
            .as_ref()
            .ok_or(crate::notion_grant_store::StoreError::Configuration)?;
        let store = crate::notion_grant_store::GrantStore::open(
            &settings.key_file,
            &settings.state_file,
            registration.client_id(),
            callback.allowed.clone(),
        )?;
        let _ = store.load()?;
        Ok(())
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| env::var_os(key))
    }

    pub(crate) fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let host = optional(&mut lookup, "NK_HTTP_HOST", "127.0.0.1")?
            .parse()
            .map_err(|_| invalid("NK_HTTP_HOST", "must be an IPv4 or IPv6 address"))?;
        let port = optional(&mut lookup, "NK_HTTP_PORT", "3000")?
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| invalid("NK_HTTP_PORT", "must be an integer from 1 to 65535"))?;
        let read_only = match optional(&mut lookup, "NK_READ_ONLY", "true")?.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(invalid("NK_READ_ONLY", "must be true or false")),
        };
        let oauth_issuer = lookup("NK_OAUTH_ISSUER")
            .map(|value| text(value, "NK_OAUTH_ISSUER"))
            .transpose()?;
        let oauth_resource = lookup("NK_OAUTH_RESOURCE")
            .map(|value| text(value, "NK_OAUTH_RESOURCE"))
            .transpose()?;
        let oauth_discovery = match (oauth_issuer, oauth_resource) {
            (None, None) => None,
            (Some(issuer), Some(resource)) => Some(
                crate::oauth_discovery::OAuthDiscovery::new(&issuer, &resource)
                    .map_err(|key| invalid(key, "must be a canonical HTTPS origin/resource"))?,
            ),
            (None, Some(_)) => {
                return Err(invalid(
                    "NK_OAUTH_ISSUER",
                    "required with NK_OAUTH_RESOURCE",
                ));
            }
            (Some(_), None) => {
                return Err(invalid(
                    "NK_OAUTH_RESOURCE",
                    "required with NK_OAUTH_ISSUER",
                ));
            }
        };
        let notion_oauth_client_id = lookup("NK_NOTION_OAUTH_CLIENT_ID")
            .map(|value| text(value, "NK_NOTION_OAUTH_CLIENT_ID"))
            .transpose()?;
        let notion_oauth_redirect_uri = lookup("NK_NOTION_OAUTH_REDIRECT_URI")
            .map(|value| text(value, "NK_NOTION_OAUTH_REDIRECT_URI"))
            .transpose()?;
        let notion_oauth_redirect = match (notion_oauth_client_id, notion_oauth_redirect_uri) {
            (None, None) => None,
            (Some(client_id), Some(redirect_uri)) => {
                let issuer = oauth_discovery.as_ref().ok_or_else(|| {
                    invalid("NK_OAUTH_ISSUER", "required before Notion OAuth redirect")
                })?;
                Some(
                    crate::notion_oauth_redirect::NotionOAuthConfig::new(
                        &client_id,
                        &redirect_uri,
                        issuer.issuer(),
                    )
                    .map_err(|setting| {
                        invalid(setting, "invalid fixed Notion OAuth configuration")
                    })?,
                )
            }
            (None, Some(_)) => {
                return Err(invalid(
                    "NK_NOTION_OAUTH_CLIENT_ID",
                    "required with NK_NOTION_OAUTH_REDIRECT_URI",
                ));
            }
            (Some(_), None) => {
                return Err(invalid(
                    "NK_NOTION_OAUTH_REDIRECT_URI",
                    "required with NK_NOTION_OAUTH_CLIENT_ID",
                ));
            }
        };
        let notion_secret = lookup("NK_NOTION_OAUTH_CLIENT_SECRET")
            .map(|value| text(value, "NK_NOTION_OAUTH_CLIENT_SECRET"))
            .transpose()?;
        let allowed_workspace = lookup("NK_NOTION_ALLOWED_WORKSPACE_ID")
            .map(|value| text(value, "NK_NOTION_ALLOWED_WORKSPACE_ID"))
            .transpose()?;
        let allowed_user = lookup("NK_NOTION_ALLOWED_USER_ID")
            .map(|value| text(value, "NK_NOTION_ALLOWED_USER_ID"))
            .transpose()?;
        let notion_oauth_callback = match (notion_secret, allowed_workspace, allowed_user) {
            (None, None, None) => None,
            (Some(secret), Some(workspace), Some(user)) => {
                if notion_oauth_redirect.is_none() {
                    return Err(invalid(
                        "NK_NOTION_OAUTH_CLIENT_ID",
                        "Notion OAuth client and callback registration required",
                    ));
                }
                // Validate secret without ever putting its value in errors.
                if secret.is_empty()
                    || secret.len() > 4096
                    || !secret.is_ascii()
                    || secret
                        .bytes()
                        .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
                {
                    return Err(invalid(
                        "NK_NOTION_OAUTH_CLIENT_SECRET",
                        "must be a nonempty server-only credential",
                    ));
                }
                Some(NotionCallbackSettings {
                    client_secret: SecretToken(secret),
                    allowed: crate::notion_oauth_callback::NotionOwnerPolicy::new(
                        &workspace, &user,
                    )
                    .map_err(|setting| {
                        invalid(setting, "must be a canonical nonempty owner identifier")
                    })?,
                })
            }
            (None, _, _) => {
                return Err(invalid(
                    "NK_NOTION_OAUTH_CLIENT_SECRET",
                    "required with Notion OAuth callback policy",
                ));
            }
            (_, None, _) => {
                return Err(invalid(
                    "NK_NOTION_ALLOWED_WORKSPACE_ID",
                    "required with Notion OAuth callback policy",
                ));
            }
            (_, _, None) => {
                return Err(invalid(
                    "NK_NOTION_ALLOWED_USER_ID",
                    "required with Notion OAuth callback policy",
                ));
            }
        };
        let state_file = lookup("NK_NOTION_GRANT_STATE_FILE")
            .map(|value| text(value, "NK_NOTION_GRANT_STATE_FILE"))
            .transpose()?;
        let key_file = lookup("NK_NOTION_GRANT_KEY_FILE")
            .map(|value| text(value, "NK_NOTION_GRANT_KEY_FILE"))
            .transpose()?;
        let notion_grant_store = match (state_file, key_file) {
            (None, None) => None,
            (Some(state), Some(key)) => {
                if notion_oauth_callback.is_none() {
                    return Err(invalid(
                        "NK_NOTION_OAUTH_CLIENT_SECRET",
                        "complete Notion OAuth callback configuration required for grant state",
                    ));
                }
                let valid_path = |value: &str| {
                    let path = std::path::Path::new(value);
                    path.is_absolute()
                        && path.file_name().is_some()
                        && !path
                            .components()
                            .any(|part| matches!(part, std::path::Component::ParentDir))
                        && !value.contains(char::is_control)
                };
                if !valid_path(&state) {
                    return Err(invalid(
                        "NK_NOTION_GRANT_STATE_FILE",
                        "must be a safe absolute path",
                    ));
                }
                if !valid_path(&key) || state == key {
                    return Err(invalid(
                        "NK_NOTION_GRANT_KEY_FILE",
                        "must be a separate safe absolute path",
                    ));
                }
                Some(GrantStoreSettings {
                    state_file: state.into(),
                    key_file: key.into(),
                })
            }
            (None, Some(_)) => {
                return Err(invalid(
                    "NK_NOTION_GRANT_STATE_FILE",
                    "required with grant key file",
                ));
            }
            (Some(_), None) => {
                return Err(invalid(
                    "NK_NOTION_GRANT_KEY_FILE",
                    "required with grant state file",
                ));
            }
        };
        let notion_auth = match optional(&mut lookup, "NK_NOTION_AUTH", "none")?.as_str() {
            "none" => NotionAuth::None,
            "integration" => {
                let token = lookup("NOTION_TOKEN").ok_or_else(|| {
                    invalid("NOTION_TOKEN", "required for integration authentication")
                })?;
                let token = text(token, "NOTION_TOKEN")?;
                if token.is_empty() || token.chars().any(|c| c.is_whitespace() || c.is_control()) {
                    return Err(invalid(
                        "NOTION_TOKEN",
                        "must be nonempty and contain no whitespace or control characters",
                    ));
                }
                NotionAuth::Integration(SecretToken(token))
            }
            _ => return Err(invalid("NK_NOTION_AUTH", "must be none or integration")),
        };
        let mode = optional(&mut lookup, "NK_WEBHOOK_MODE", "disabled")?;
        let webhook = match mode.as_str() {
            "disabled" => crate::webhook::WebhookConfig::Disabled,
            "setup" => {
                let path = text(
                    lookup("NK_WEBHOOK_CANDIDATE_FILE").ok_or_else(|| {
                        invalid("NK_WEBHOOK_CANDIDATE_FILE", "required in setup mode")
                    })?,
                    "NK_WEBHOOK_CANDIDATE_FILE",
                )?;
                if path.is_empty() || !std::path::Path::new(&path).is_absolute() {
                    return Err(invalid(
                        "NK_WEBHOOK_CANDIDATE_FILE",
                        "must be an absolute nonempty path",
                    ));
                }
                crate::webhook::WebhookConfig::Setup {
                    candidate_file: path.into(),
                }
            }
            "verified" => {
                let token = text(
                    lookup("NK_WEBHOOK_VERIFICATION_TOKEN").ok_or_else(|| {
                        invalid("NK_WEBHOOK_VERIFICATION_TOKEN", "required in verified mode")
                    })?,
                    "NK_WEBHOOK_VERIFICATION_TOKEN",
                )?;
                if !crate::webhook::valid_token(&token) {
                    return Err(invalid(
                        "NK_WEBHOOK_VERIFICATION_TOKEN",
                        "must be nonempty, at most 512 bytes and contain no whitespace or controls",
                    ));
                }
                let mut id = |key| -> Result<String, ConfigError> {
                    let value = text(
                        lookup(key).ok_or_else(|| invalid(key, "required in verified mode"))?,
                        key,
                    )?;
                    if !crate::webhook::valid_id(&value) {
                        return Err(invalid(key, "must be a hyphenated UUID"));
                    }
                    Ok(value)
                };
                crate::webhook::WebhookConfig::Verified {
                    token: SecretToken(token),
                    workspace_id: id("NK_WEBHOOK_WORKSPACE_ID")?,
                    integration_id: id("NK_WEBHOOK_INTEGRATION_ID")?,
                    subscription_id: id("NK_WEBHOOK_SUBSCRIPTION_ID")?,
                }
            }
            _ => {
                return Err(invalid(
                    "NK_WEBHOOK_MODE",
                    "must be disabled, setup or verified",
                ));
            }
        };
        let webhook_state_file =
            if matches!(webhook, crate::webhook::WebhookConfig::Verified { .. }) {
                let path = text(
                    lookup("NK_WEBHOOK_STATE_FILE").ok_or_else(|| {
                        invalid("NK_WEBHOOK_STATE_FILE", "required in verified mode")
                    })?,
                    "NK_WEBHOOK_STATE_FILE",
                )?;
                if path.is_empty()
                    || !std::path::Path::new(&path).is_absolute()
                    || path.chars().any(char::is_control)
                {
                    return Err(invalid(
                        "NK_WEBHOOK_STATE_FILE",
                        "must be an absolute nonempty path without controls",
                    ));
                }
                Some(path.into())
            } else {
                None
            };
        let mut webhook_debounce = notion_knowledge_core::webhook::DebounceWindow::default();
        for (name, target, maximum) in [
            (
                "NK_WEBHOOK_DEBOUNCE_MS",
                &mut webhook_debounce.quiet_ms,
                60000,
            ),
            (
                "NK_WEBHOOK_MAX_DELAY_MS",
                &mut webhook_debounce.max_delay_ms,
                300000,
            ),
        ] {
            if let Some(value) = lookup(name) {
                let raw = text(value, name)?;
                *target = raw
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (1..=maximum).contains(n))
                    .ok_or_else(|| {
                        invalid(name, "must be an integer within the documented range")
                    })?;
            }
        }
        webhook_debounce.validate().map_err(|_| {
            invalid(
                "NK_WEBHOOK_MAX_DELAY_MS",
                "must be at least NK_WEBHOOK_DEBOUNCE_MS",
            )
        })?;
        Ok(Self {
            webhook_state_file,
            webhook_debounce,
            http_bind: SocketAddr::new(host, port),
            read_only,
            oauth_discovery,
            notion_oauth_redirect,
            notion_oauth_callback,
            notion_grant_store,
            notion_auth,
            webhook,
        })
    }
}

fn invalid(setting: &'static str, reason: &'static str) -> ConfigError {
    ConfigError { setting, reason }
}

fn text(value: OsString, setting: &'static str) -> Result<String, ConfigError> {
    value
        .into_string()
        .map_err(|_| invalid(setting, "must be valid Unicode"))
}

fn optional(
    lookup: &mut impl FnMut(&str) -> Option<OsString>,
    key: &'static str,
    default: &str,
) -> Result<String, ConfigError> {
    match lookup(key) {
        Some(value) => text(value, key),
        None => Ok(default.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(values: &[(&str, &str)]) -> Result<Config, ConfigError> {
        Config::from_lookup(|key| {
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| OsString::from(value))
        })
    }

    #[test]
    fn debounce_windows_are_bounded_and_cap_cannot_be_shorter_than_quiet() {
        let defaults = parse(&[]).unwrap().webhook_debounce;
        assert_eq!(defaults.quiet_ms, 5000);
        assert_eq!(defaults.max_delay_ms, 30000);
        for bad in ["", "0", "60001", "-1", "private-invalid-value"] {
            let error = parse(&[("NK_WEBHOOK_DEBOUNCE_MS", bad)])
                .unwrap_err()
                .to_string();
            assert!(error.contains("NK_WEBHOOK_DEBOUNCE_MS"));
            if !bad.is_empty() {
                assert!(!error.contains(bad));
            }
        }
        assert!(
            parse(&[
                ("NK_WEBHOOK_DEBOUNCE_MS", "60000"),
                ("NK_WEBHOOK_MAX_DELAY_MS", "59999")
            ])
            .is_err()
        );
        assert!(parse(&[("NK_WEBHOOK_MAX_DELAY_MS", "300001")]).is_err());
        let custom = parse(&[
            ("NK_WEBHOOK_DEBOUNCE_MS", "10"),
            ("NK_WEBHOOK_MAX_DELAY_MS", "20"),
        ])
        .unwrap();
        assert_eq!(custom.webhook_debounce.quiet_ms, 10);
        assert_eq!(custom.webhook_debounce.max_delay_ms, 20);
    }

    #[test]
    fn oauth_discovery_requires_both_canonical_https_uris() {
        let valid = [
            ("NK_OAUTH_ISSUER", "https://auth.example.com"),
            ("NK_OAUTH_RESOURCE", "https://mcp.example.com/mcp"),
        ];
        assert!(parse(&valid).unwrap().oauth_discovery.is_some());
        assert!(parse(&[]).unwrap().oauth_discovery.is_none());
        assert_eq!(parse(&valid[..1]).unwrap_err().setting, "NK_OAUTH_RESOURCE");
        assert_eq!(parse(&valid[1..]).unwrap_err().setting, "NK_OAUTH_ISSUER");
        for (index, value) in [
            (0, "http://auth.example.com"),
            (0, "https://user@auth.example.com"),
            (1, "https://mcp.example.com/mcp?key=private"),
            (1, "http://mcp.example.com/mcp"),
        ] {
            let mut bad = valid;
            bad[index].1 = value;
            let error = parse(&bad).unwrap_err();
            assert_eq!(error.setting, valid[index].0);
            assert!(!error.to_string().contains(value));
        }
    }

    #[test]
    fn notion_oauth_redirect_requires_paired_registration_and_canonical_issuer_callback() {
        let base = [
            ("NK_OAUTH_ISSUER", "https://auth.example.com"),
            ("NK_OAUTH_RESOURCE", "https://mcp.example.com/mcp"),
        ];
        let client = ("NK_NOTION_OAUTH_CLIENT_ID", "client-id");
        let callback = (
            "NK_NOTION_OAUTH_REDIRECT_URI",
            "https://auth.example.com/oauth/notion/callback",
        );
        assert!(
            parse(&[base[0], base[1], client, callback])
                .unwrap()
                .notion_oauth_redirect
                .is_some()
        );
        assert!(parse(&[]).unwrap().notion_oauth_redirect.is_none());
        assert_eq!(
            parse(&[base[0], base[1], client]).unwrap_err().setting,
            callback.0
        );
        assert_eq!(
            parse(&[base[0], base[1], callback]).unwrap_err().setting,
            client.0
        );
        assert_eq!(
            parse(&[client, callback]).unwrap_err().setting,
            "NK_OAUTH_ISSUER"
        );
        for invalid_uri in [
            "http://auth.example.com/oauth/notion/callback",
            "https://evil.example.com/oauth/notion/callback",
            "https://auth.example.com/oauth/notion/callback?next=evil",
        ] {
            let invalid = ("NK_NOTION_OAUTH_REDIRECT_URI", invalid_uri);
            let error = parse(&[base[0], base[1], client, invalid]).unwrap_err();
            assert_eq!(error.setting, invalid.0);
            assert!(!error.to_string().contains(invalid_uri));
        }
    }

    #[test]
    fn notion_callback_credentials_and_immutable_owner_must_be_complete() {
        let base = [
            ("NK_OAUTH_ISSUER", "https://auth.example.com"),
            ("NK_OAUTH_RESOURCE", "https://mcp.example.com/mcp"),
            ("NK_NOTION_OAUTH_CLIENT_ID", "client-123"),
            (
                "NK_NOTION_OAUTH_REDIRECT_URI",
                "https://auth.example.com/oauth/notion/callback",
            ),
        ];
        let secret = ("NK_NOTION_OAUTH_CLIENT_SECRET", "private-credential");
        let workspace = ("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-123");
        let owner = ("NK_NOTION_ALLOWED_USER_ID", "user-456");
        assert!(parse(&base).unwrap().notion_oauth_callback.is_none());
        let correct = [base[0], base[1], base[2], base[3], secret, workspace, owner];
        assert!(parse(&correct).unwrap().notion_oauth_callback.is_some());
        assert_eq!(
            parse(&[base[0], base[1], base[2], base[3], secret, owner])
                .unwrap_err()
                .setting,
            "NK_NOTION_ALLOWED_WORKSPACE_ID"
        );
        assert_eq!(
            parse(&[base[0], base[1], base[2], base[3], secret, workspace])
                .unwrap_err()
                .setting,
            "NK_NOTION_ALLOWED_USER_ID"
        );
        assert_eq!(
            parse(&[base[0], base[1], base[2], base[3], workspace, owner])
                .unwrap_err()
                .setting,
            "NK_NOTION_OAUTH_CLIENT_SECRET"
        );
        assert_eq!(
            parse(&[secret, workspace, owner]).unwrap_err().setting,
            "NK_NOTION_OAUTH_CLIENT_ID"
        );
        for bad in ["", "a secret", "one\nsecret"] {
            let error = parse(&[
                base[0],
                base[1],
                base[2],
                base[3],
                ("NK_NOTION_OAUTH_CLIENT_SECRET", bad),
                workspace,
                owner,
            ])
            .unwrap_err();
            assert_eq!(error.setting, "NK_NOTION_OAUTH_CLIENT_SECRET");
            if !bad.is_empty() {
                assert!(!error.to_string().contains(bad));
            }
        }
    }

    #[test]
    fn sealed_grant_configuration_requires_key_state_pair_and_callback() {
        let core = [
            ("NK_OAUTH_ISSUER", "https://auth.example.com"),
            ("NK_OAUTH_RESOURCE", "https://mcp.example.com/mcp"),
            ("NK_NOTION_OAUTH_CLIENT_ID", "client-id"),
            (
                "NK_NOTION_OAUTH_REDIRECT_URI",
                "https://auth.example.com/oauth/notion/callback",
            ),
            ("NK_NOTION_OAUTH_CLIENT_SECRET", "private-secret"),
            ("NK_NOTION_ALLOWED_WORKSPACE_ID", "workspace-123"),
            ("NK_NOTION_ALLOWED_USER_ID", "user-456"),
        ];
        let key = ("NK_NOTION_GRANT_KEY_FILE", "/run/notion/key");
        let state = ("NK_NOTION_GRANT_STATE_FILE", "/var/lib/notion/grant");
        let valid = [
            core[0], core[1], core[2], core[3], core[4], core[5], core[6], key, state,
        ];
        assert!(parse(&valid).unwrap().notion_grant_store.is_some());
        assert!(parse(&core).unwrap().notion_grant_store.is_none());
        assert_eq!(
            parse(&[
                core[0], core[1], core[2], core[3], core[4], core[5], core[6], key
            ])
            .unwrap_err()
            .setting,
            "NK_NOTION_GRANT_STATE_FILE"
        );
        assert_eq!(
            parse(&[
                core[0], core[1], core[2], core[3], core[4], core[5], core[6], state
            ])
            .unwrap_err()
            .setting,
            "NK_NOTION_GRANT_KEY_FILE"
        );
        assert_eq!(
            parse(&[key, state]).unwrap_err().setting,
            "NK_NOTION_OAUTH_CLIENT_SECRET"
        );
        for malformed in ["relative/grant", "/tmp/../grant", "/var/lib/\ngrant"] {
            let bad = ("NK_NOTION_GRANT_STATE_FILE", malformed);
            assert_eq!(
                parse(&[
                    core[0], core[1], core[2], core[3], core[4], core[5], core[6], key, bad
                ])
                .unwrap_err()
                .setting,
                "NK_NOTION_GRANT_STATE_FILE"
            );
        }
    }

    #[test]
    fn read_only_mode_defaults_to_safe_and_requires_explicit_boolean() {
        assert!(parse(&[]).unwrap().read_only);
        assert!(parse(&[("NK_READ_ONLY", "true")]).unwrap().read_only);
        assert!(!parse(&[("NK_READ_ONLY", "false")]).unwrap().read_only);
        for invalid_value in ["", "0", "TRUE", "private-secret"] {
            let error = parse(&[("NK_READ_ONLY", invalid_value)]).unwrap_err();
            assert_eq!(error.setting, "NK_READ_ONLY");
            assert!(!error.to_string().contains(invalid_value) || invalid_value.is_empty());
        }
    }

    #[test]
    fn bootstrap_defaults_need_no_credentials() {
        let config = parse(&[]).unwrap();
        assert_eq!(config.http_bind, "127.0.0.1:3000".parse().unwrap());
        assert!(matches!(config.notion_auth, NotionAuth::None));
    }

    #[test]
    fn explicit_bind_supports_ipv4_ipv6_and_port_boundaries() {
        for host in ["127.0.0.2", "::1"] {
            for port in ["1", "65535"] {
                let config = parse(&[("NK_HTTP_HOST", host), ("NK_HTTP_PORT", port)]).unwrap();
                assert_eq!(
                    config.http_bind.ip(),
                    host.parse::<std::net::IpAddr>().unwrap()
                );
                assert_eq!(config.http_bind.port(), port.parse::<u16>().unwrap());
            }
        }
    }

    #[test]
    fn invalid_optional_settings_are_rejected_instead_of_defaulted() {
        for (key, values) in [
            (
                "NK_HTTP_HOST",
                vec!["", "localhost", "127.0.0.1:3000", " ::1 "],
            ),
            (
                "NK_HTTP_PORT",
                vec!["", "0", "65536", "-1", "1.5", " 3000 "],
            ),
            ("NK_NOTION_AUTH", vec!["", "oauth", " integration "]),
        ] {
            for value in values {
                assert_eq!(parse(&[(key, value)]).unwrap_err().setting, key);
            }
        }
    }

    #[test]
    fn integration_authentication_requires_a_nonempty_token() {
        assert_eq!(
            parse(&[("NK_NOTION_AUTH", "integration")])
                .unwrap_err()
                .setting,
            "NOTION_TOKEN"
        );
        for token in [
            "",
            " ",
            "secret sentinel",
            "secret\nsentinel",
            "secret\u{7f}",
        ] {
            let error =
                parse(&[("NK_NOTION_AUTH", "integration"), ("NOTION_TOKEN", token)]).unwrap_err();
            assert_eq!(error.setting, "NOTION_TOKEN");
            assert!(!error.to_string().contains("secret"));
            assert!(!format!("{error:?}").contains("secret"));
        }
    }

    #[test]
    fn integration_credentials_are_preserved_but_redacted_in_debug() {
        let config = parse(&[
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "secret-sentinel"),
        ])
        .unwrap();
        assert!(!format!("{config:?}").contains("secret-sentinel"));
        match config.notion_auth {
            NotionAuth::Integration(token) => assert_eq!(token.expose_secret(), "secret-sentinel"),
            NotionAuth::None => panic!("integration authentication was not selected"),
        }
    }

    #[test]
    fn webhook_configuration_requires_explicit_trust_and_redacts_token() {
        assert!(matches!(
            parse(&[]).unwrap().webhook,
            crate::webhook::WebhookConfig::Disabled
        ));
        assert_eq!(
            parse(&[("NK_WEBHOOK_MODE", "setup")]).unwrap_err().setting,
            "NK_WEBHOOK_CANDIDATE_FILE"
        );
        assert_eq!(
            parse(&[
                ("NK_WEBHOOK_MODE", "setup"),
                ("NK_WEBHOOK_CANDIDATE_FILE", "relative")
            ])
            .unwrap_err()
            .setting,
            "NK_WEBHOOK_CANDIDATE_FILE"
        );
        assert_eq!(
            parse(&[("NK_WEBHOOK_MODE", "verified")])
                .unwrap_err()
                .setting,
            "NK_WEBHOOK_VERIFICATION_TOKEN"
        );
        let id = "13950b26-c203-4f3b-b97d-93ec06319565";
        let values = [
            ("NK_WEBHOOK_MODE", "verified"),
            ("NK_WEBHOOK_VERIFICATION_TOKEN", "fixture-private-key"),
            ("NK_WEBHOOK_WORKSPACE_ID", id),
            ("NK_WEBHOOK_INTEGRATION_ID", id),
            ("NK_WEBHOOK_SUBSCRIPTION_ID", id),
            ("NK_WEBHOOK_STATE_FILE", "/tmp/webhook-test-state.sqlite"),
        ];
        assert_eq!(
            parse(&values[..5]).unwrap_err().setting,
            "NK_WEBHOOK_STATE_FILE"
        );
        for path in ["", "relative", "/tmp/bad\npath"] {
            let mut invalid = values;
            invalid[5].1 = path;
            assert_eq!(
                parse(&invalid).unwrap_err().setting,
                "NK_WEBHOOK_STATE_FILE"
            );
        }
        let config = parse(&values).unwrap();
        assert!(!format!("{config:?}").contains("fixture-private-key"));
        for index in 2..5 {
            let mut invalid = values;
            invalid[index].1 = "not-a-uuid";
            assert_eq!(parse(&invalid).unwrap_err().setting, values[index].0);
        }
        let mut invalid = values;
        invalid[1].1 = "secret invalid";
        let error = parse(&invalid).unwrap_err();
        assert!(!format!("{error:?}").contains("secret invalid"));
    }
    #[cfg(unix)]
    #[test]
    fn non_unicode_settings_report_only_the_key() {
        use std::os::unix::ffi::OsStringExt;
        for key in [
            "NK_HTTP_HOST",
            "NK_HTTP_PORT",
            "NK_NOTION_AUTH",
            "NOTION_TOKEN",
        ] {
            let error = Config::from_lookup(|name| {
                if name == key {
                    Some(OsString::from_vec(vec![0xff]))
                } else if name == "NK_NOTION_AUTH" {
                    Some("integration".into())
                } else {
                    None
                }
            })
            .unwrap_err();
            assert_eq!(error, invalid(key, "must be valid Unicode"));
        }
    }
}
