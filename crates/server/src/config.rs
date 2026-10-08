//! Typed startup configuration. Errors identify settings without exposing values.

use std::{env, ffi::OsString, fmt, net::SocketAddr};

#[derive(Debug)]
pub struct Config {
    pub http_bind: SocketAddr,
    pub notion_auth: NotionAuth,
    pub webhook: crate::webhook::WebhookConfig,
    pub webhook_state_file: Option<std::path::PathBuf>,
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
        Ok(Self {
            webhook_state_file,
            http_bind: SocketAddr::new(host, port),
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
