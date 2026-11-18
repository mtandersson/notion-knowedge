//! Typed startup configuration. Errors identify settings without exposing values.

use std::{env, ffi::OsString, fmt, net::SocketAddr};

/// Settings of removed multi-user/OAuth features (#730): present values are
/// ignored with one startup warning, never fatal and never read.
const REMOVED_SETTINGS: [&str; 15] = [
    "NK_OAUTH_ISSUER",
    "NK_OAUTH_RESOURCE",
    "NK_OAUTH_CLIENT_ID",
    "NK_OAUTH_CLIENT_REDIRECT_URI",
    "NK_OAUTH_LOCAL_HTTP",
    "NK_OAUTH_WRITES",
    "NK_OAUTH_WRITE_LEDGER_FILE",
    "NK_OAUTH_WRITE_AUDIT_FILE",
    "NK_NOTION_OAUTH_CLIENT_ID",
    "NK_NOTION_OAUTH_CLIENT_SECRET",
    "NK_NOTION_OAUTH_REDIRECT_URI",
    "NK_NOTION_GRANT_STATE_FILE",
    "NK_NOTION_GRANT_KEY_FILE",
    "NK_NOTION_ALLOWED_WORKSPACE_ID",
    "NK_NOTION_ALLOWED_USER_ID",
];

#[derive(Debug)]
pub struct Config {
    pub http_bind: SocketAddr,
    /// One shared deadline for HTTP, workers and owned native effects after stop.
    pub shutdown_timeout_seconds: u64,
    /// Upper bound on simultaneously dispatched MCP HTTP calls (not session count).
    pub mcp_max_in_flight: usize,
    /// Shared weighted MCP tool admission across stdio and HTTP.
    pub mcp_tool_capacity: usize,
    /// SDK stateful worker sessions and long-lived transport response caps.
    pub mcp_max_sessions: usize,
    pub mcp_session_idle_seconds: u64,
    pub mcp_max_streams: usize,
    pub mcp_stream_max_seconds: u64,
    /// Separate ingress pool so verified Notion receipts are not starved by MCP.
    pub webhook_max_in_flight: usize,
    pub max_upload_files: usize,
    pub file_ingestion: Option<crate::file_ingestion::Settings>,
    /// Highest level emitted to stderr as redacted, structured JSON.
    pub log_level: notion_knowledge_core::logging::Level,
    /// Fail-closed MCP access mode; enabled unless explicitly opted out.
    pub read_only: bool,
    /// Explicit guarded writes for the private integration-backed HTTP transport.
    pub integration_write_stores: Option<crate::write_stores::Settings>,
    /// Explicit second gate for destructive operations; defaults disabled.
    pub destructive_writes_enabled: bool,
    pub capability_policy: notion_knowledge_mcp::capabilities::CapabilityPolicy,
    /// Loopback HTTP bearer; the only HTTP MCP authentication.
    pub bearer_fallback: Option<SecretToken>,
    /// Diagnostics authentication is independent from the MCP route.
    pub health_requires_bearer: bool,
    ignored_settings: Vec<&'static str>,
    pub notion_auth: NotionAuth,
    /// Trusted process-level root policy; no MCP request can broaden it.
    pub notion_scope: Option<notion_knowledge_core::lifecycle::LifecycleScope>,
    pub webhook: crate::webhook::WebhookConfig,
    pub webhook_debounce: notion_knowledge_core::webhook::DebounceWindow,
    pub webhook_state_file: Option<std::path::PathBuf>,
    pub webhook_worker: Option<crate::webhook_worker::WorkerSettings>,
    pub reconciliation_schedule: Option<crate::scheduled_reconciliation::Settings>,
    pub graph_index: Option<std::path::PathBuf>,
    pub graph_boost: notion_knowledge_core::graph_boost::GraphBoostConfig,
    pub retrieval: Option<crate::webhook_worker::WorkerSettings>,
    pub reconciliation_index: Option<crate::webhook_worker::WorkerSettings>,
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
    /// Removed settings that were present and ignored (#730).
    pub fn ignored_settings(&self) -> &[&'static str] {
        &self.ignored_settings
    }

    /// One startup warning naming (never echoing values of) ignored settings.
    pub fn ignored_settings_warning(&self) -> Option<String> {
        if self.ignored_settings.is_empty() {
            return None;
        }
        let grant_files = self
            .ignored_settings
            .iter()
            .any(|key| key.starts_with("NK_NOTION_GRANT_"));
        Some(format!(
            "Warning: ignoring removed settings {}; OAuth and per-user Notion grants are no longer supported (single-user integration token and loopback bearer only).{}",
            self.ignored_settings.join(", "),
            if grant_files {
                " The Notion grant state and key files they name are never read and may be deleted."
            } else {
                ""
            }
        ))
    }

    /// Emit the ignored-settings warning to stderr; call once per process start.
    pub fn warn_ignored_settings(&self) {
        if let Some(warning) = self.ignored_settings_warning() {
            eprintln!("{warning}");
        }
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_lookup(|key| env::var_os(key))
    }

    /// Parse a trusted startup configuration source; never pass webhook hints.
    pub fn from_lookup(
        mut lookup: impl FnMut(&str) -> Option<OsString>,
    ) -> Result<Self, ConfigError> {
        let log_level = notion_knowledge_core::logging::Level::parse(&optional(
            &mut lookup,
            "NK_LOG_LEVEL",
            "info",
        )?)
        .ok_or_else(|| invalid("NK_LOG_LEVEL", "must be off, error, warn, info or debug"))?;
        let host = optional(&mut lookup, "NK_HTTP_HOST", "127.0.0.1")?
            .parse()
            .map_err(|_| invalid("NK_HTTP_HOST", "must be an IPv4 or IPv6 address"))?;
        let port = optional(&mut lookup, "NK_HTTP_PORT", "3000")?
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or_else(|| invalid("NK_HTTP_PORT", "must be an integer from 1 to 65535"))?;
        let concurrency = |lookup: &mut dyn FnMut(&str) -> Option<OsString>,
                           key: &'static str,
                           default: &str,
                           maximum: usize| {
            optional(lookup, key, default)?
                .parse::<usize>()
                .ok()
                .filter(|limit| (1..=maximum).contains(limit))
                .ok_or_else(|| {
                    invalid(
                        key,
                        "must be a positive integer within the documented range",
                    )
                })
        };
        let mcp_max_in_flight = concurrency(&mut lookup, "NK_MCP_MAX_IN_FLIGHT", "8", 256)?;
        let shutdown_timeout_seconds =
            concurrency(&mut lookup, "NK_SHUTDOWN_TIMEOUT_SECONDS", "30", 3600)? as u64;
        let mcp_tool_capacity = concurrency(&mut lookup, "NK_MCP_TOOL_CAPACITY", "16", 128)?;
        let webhook_max_in_flight = concurrency(&mut lookup, "NK_WEBHOOK_MAX_IN_FLIGHT", "4", 128)?;
        let mcp_max_sessions = concurrency(&mut lookup, "NK_MCP_MAX_SESSIONS", "32", 512)?;
        let mcp_session_idle_seconds =
            concurrency(&mut lookup, "NK_MCP_SESSION_IDLE_SECONDS", "300", 3600)? as u64;
        let mcp_max_streams = concurrency(&mut lookup, "NK_MCP_MAX_STREAMS", "16", 256)?;
        let mcp_stream_max_seconds =
            concurrency(&mut lookup, "NK_MCP_STREAM_MAX_SECONDS", "300", 3600)? as u64;
        // Settings of removed multi-user/OAuth features are ignored, never
        // fatal; startup warns once with their names (#730).
        let ignored_settings: Vec<&'static str> = REMOVED_SETTINGS
            .iter()
            .copied()
            .filter(|key| lookup(key).is_some())
            .collect();
        let read_only = match optional(&mut lookup, "NK_READ_ONLY", "true")?.as_str() {
            "true" => true,
            "false" => false,
            _ => return Err(invalid("NK_READ_ONLY", "must be true or false")),
        };
        let destructive_writes_enabled =
            match optional(&mut lookup, "NK_DESTRUCTIVE_WRITES", "false")?.as_str() {
                "false" => false,
                "true" if !read_only => true,
                "true" => {
                    return Err(invalid(
                        "NK_DESTRUCTIVE_WRITES",
                        "requires NK_READ_ONLY=false",
                    ));
                }
                _ => return Err(invalid("NK_DESTRUCTIVE_WRITES", "must be true or false")),
            };
        let allowlist = lookup("NK_TOOL_ALLOWLIST")
            .map(|value| text(value, "NK_TOOL_ALLOWLIST"))
            .transpose()?;
        let blocklist = lookup("NK_TOOL_BLOCKLIST")
            .map(|value| text(value, "NK_TOOL_BLOCKLIST"))
            .transpose()?;
        let capability_policy = notion_knowledge_mcp::capabilities::CapabilityPolicy::from_lists(
            allowlist.as_deref(),
            blocklist.as_deref(),
        )
        .map_err(|error| match error {
            notion_knowledge_mcp::capabilities::PolicyListError::Allowlist => {
                invalid("NK_TOOL_ALLOWLIST", "unknown or malformed capability")
            }
            notion_knowledge_mcp::capabilities::PolicyListError::Blocklist => {
                invalid("NK_TOOL_BLOCKLIST", "unknown or malformed capability")
            }
        })?;
        // A static bearer is deliberately never a production default.
        let bearer_mode = optional(&mut lookup, "NK_HTTP_AUTH", "none")?;
        let bearer_value = lookup("NK_HTTP_BEARER_TOKEN");
        let bearer_fallback = match (bearer_mode.as_str(), bearer_value) {
            ("none", None) => None,
            ("none", Some(_)) => {
                return Err(invalid(
                    "NK_HTTP_BEARER_TOKEN",
                    "requires explicit bearer mode",
                ));
            }
            ("bearer", Some(value)) => {
                if !SocketAddr::new(host, port).ip().is_loopback() {
                    return Err(invalid(
                        "NK_HTTP_HOST",
                        "bearer fallback requires loopback binding",
                    ));
                }
                let token = text(value, "NK_HTTP_BEARER_TOKEN")?;
                if !(32..=256).contains(&token.len())
                    || !token
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b))
                {
                    return Err(invalid(
                        "NK_HTTP_BEARER_TOKEN",
                        "requires 32-256 printable URL-safe ASCII characters",
                    ));
                }
                Some(SecretToken(token))
            }
            ("bearer", None) => {
                return Err(invalid("NK_HTTP_BEARER_TOKEN", "required in bearer mode"));
            }
            _ => return Err(invalid("NK_HTTP_AUTH", "must be none or bearer")),
        };
        let health_requires_bearer =
            match optional(&mut lookup, "NK_HTTP_HEALTH_AUTH", "none")?.as_str() {
                "none" => false,
                "bearer" if bearer_fallback.is_some() => true,
                "bearer" => {
                    return Err(invalid(
                        "NK_HTTP_HEALTH_AUTH",
                        "bearer health authentication requires bearer HTTP mode",
                    ));
                }
                _ => return Err(invalid("NK_HTTP_HEALTH_AUTH", "must be none or bearer")),
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
        let roots = lookup("NK_NOTION_SCOPE_ROOTS")
            .map(|value| text(value, "NK_NOTION_SCOPE_ROOTS"))
            .transpose()?;
        let workspace = lookup("NK_NOTION_SCOPE_WORKSPACE_ID")
            .map(|value| text(value, "NK_NOTION_SCOPE_WORKSPACE_ID"))
            .transpose()?;
        let notion_scope = match (roots, workspace) {
            (None, None) => None,
            (Some(roots), Some(workspace)) => {
                if !matches!(notion_auth, NotionAuth::Integration(_)) {
                    return Err(invalid(
                        "NK_NOTION_AUTH",
                        "integration required for authoritative scope",
                    ));
                }
                let parse = |value: &str| {
                    notion_knowledge_notion::pages::page_id(value)
                        .ok()
                        .filter(|id| id.0 == value)
                        .map(|id| id.0)
                };
                let workspace_id = parse(&workspace).ok_or_else(|| {
                    invalid(
                        "NK_NOTION_SCOPE_WORKSPACE_ID",
                        "must be canonical page UUID",
                    )
                })?;
                let root_ids: Option<Vec<_>> = roots
                    .split(',')
                    .map(|part| parse(part).map(notion_knowledge_core::backend::PageId))
                    .collect();
                let root_ids = root_ids.ok_or_else(|| {
                    invalid(
                        "NK_NOTION_SCOPE_ROOTS",
                        "must be comma-separated canonical UUIDs",
                    )
                })?;
                if root_ids.is_empty() || root_ids.len() > 100 {
                    return Err(invalid(
                        "NK_NOTION_SCOPE_ROOTS",
                        "must contain 1 to 100 roots",
                    ));
                }
                let generation = optional(&mut lookup, "NK_NOTION_SCOPE_GENERATION", "1")?
                    .parse::<u64>()
                    .ok()
                    .filter(|number| *number > 0)
                    .ok_or_else(|| {
                        invalid("NK_NOTION_SCOPE_GENERATION", "must be a positive integer")
                    })?;
                let mut exclusions = notion_knowledge_core::discovery::ExclusionRules::default();
                for (name, target) in [
                    ("NK_NOTION_SCOPE_EXCLUDED_PAGES", &mut exclusions.page_ids),
                    (
                        "NK_NOTION_SCOPE_EXCLUDED_DESCENDANTS",
                        &mut exclusions.descendants_of,
                    ),
                ] {
                    if let Some(value) = lookup(name) {
                        let value = text(value, name)?;
                        let ids: Option<Vec<_>> = value.split(',').map(parse).collect();
                        let ids = ids
                            .filter(|ids| !ids.is_empty() && ids.len() <= 100)
                            .ok_or_else(|| {
                                invalid(
                                    name,
                                    "must contain 1 to 100 comma-separated canonical UUIDs",
                                )
                            })?;
                        target.extend(ids);
                    }
                }
                Some(notion_knowledge_core::lifecycle::LifecycleScope {
                    workspace_id,
                    generation,
                    roots: root_ids,
                    exclusions,
                })
            }
            _ => {
                return Err(invalid(
                    "NK_NOTION_SCOPE_ROOTS",
                    "workspace and root IDs must be configured together",
                ));
            }
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
        let graph_enabled = match lookup("NK_GRAPH").as_deref() {
            None => false,
            Some(value) if value == "false" => false,
            Some(value) if value == "true" => true,
            _ => return Err(invalid("NK_GRAPH", "must be true or false")),
        };
        let graph_index = if graph_enabled {
            if !cfg!(all(unix, feature = "webhook-worker")) || notion_scope.is_none() {
                return Err(invalid(
                    "NK_GRAPH",
                    "requires Unix webhook-worker feature and trusted scope",
                ));
            }
            let raw = text(
                lookup("NK_WEBHOOK_INDEX_DIR")
                    .ok_or_else(|| invalid("NK_WEBHOOK_INDEX_DIR", "required for graph binding"))?,
                "NK_WEBHOOK_INDEX_DIR",
            )?;
            if !std::path::Path::new(&raw).is_absolute() || raw.chars().any(char::is_control) {
                return Err(invalid(
                    "NK_WEBHOOK_INDEX_DIR",
                    "requires an absolute local directory",
                ));
            }
            Some(raw.into())
        } else {
            None
        };
        let retrieval_enabled = match lookup("NK_RETRIEVAL").as_deref() {
            None => false,
            Some(value) if value == "false" => false,
            Some(value) if value == "true" => true,
            _ => return Err(invalid("NK_RETRIEVAL", "must be true or false")),
        };
        let enabled = match lookup("NK_GRAPH_BOOST").as_deref() {
            None => false,
            Some(v) if v == "false" => false,
            Some(v) if v == "true" => true,
            _ => return Err(invalid("NK_GRAPH_BOOST", "must be true or false")),
        };
        let mut graph_boost = notion_knowledge_core::graph_boost::GraphBoostConfig {
            enabled,
            ..Default::default()
        };
        for (name, target, maximum) in [
            ("NK_GRAPH_BOOST_MAX_HOPS", &mut graph_boost.max_hops, 2),
            (
                "NK_GRAPH_BOOST_CANDIDATE_LIMIT",
                &mut graph_boost.candidate_limit,
                100,
            ),
        ] {
            if let Some(raw) = lookup(name) {
                *target = text(raw, name)?
                    .parse::<usize>()
                    .ok()
                    .filter(|n| (1..=maximum).contains(n))
                    .ok_or_else(|| invalid(name, "outside documented integer range"))?;
            }
        }
        if let Some(raw) = lookup("NK_GRAPH_BOOST_WEIGHT") {
            graph_boost.weight = text(raw, "NK_GRAPH_BOOST_WEIGHT")?
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite() && *n > 0.0 && *n <= 0.5)
                .ok_or_else(|| {
                    invalid("NK_GRAPH_BOOST_WEIGHT", "must be finite and in (0, 0.5]")
                })?;
        }
        if graph_boost.enabled && (!graph_enabled || !retrieval_enabled) {
            return Err(invalid(
                "NK_GRAPH_BOOST",
                "requires NK_GRAPH=true and NK_RETRIEVAL=true",
            ));
        }
        let webhook_state_file =
            if matches!(webhook, crate::webhook::WebhookConfig::Verified { .. })
                || lookup("NK_RECONCILIATION_INTERVAL_SECONDS").is_some()
                || graph_enabled
                || retrieval_enabled
            {
                let path = text(
                    lookup("NK_WEBHOOK_STATE_FILE").ok_or_else(|| {
                        invalid(
                            "NK_WEBHOOK_STATE_FILE",
                            "required for configured index state",
                        )
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
        let lexical = crate::lexical_expansion::Settings::parse(&mut lookup)?;
        let mut webhook_worker = crate::webhook_worker::WorkerSettings::parse(
            &mut lookup,
            &webhook,
            notion_scope.as_ref(),
        )?;
        let reconciliation_schedule = crate::scheduled_reconciliation::Settings::parse(
            &mut lookup,
            cfg!(all(unix, feature = "webhook-worker")) && notion_scope.is_some(),
        )?;
        if reconciliation_schedule.is_some() && !matches!(notion_auth, NotionAuth::Integration(_)) {
            return Err(invalid(
                "NK_RECONCILIATION_INTERVAL_SECONDS",
                "requires authenticated Notion integration",
            ));
        }
        let mut reconciliation_index = if reconciliation_schedule.is_some() {
            Some(crate::webhook_worker::WorkerSettings::index_settings(
                &mut lookup,
            )?)
        } else {
            None
        };
        let mut retrieval = if retrieval_enabled {
            if !cfg!(all(unix, feature = "webhook-worker")) || notion_scope.is_none() {
                return Err(invalid(
                    "NK_RETRIEVAL",
                    "requires Unix webhook-worker feature and trusted scope",
                ));
            }
            Some(crate::webhook_worker::WorkerSettings::index_settings(
                &mut lookup,
            )?)
        } else {
            None
        };
        // All installed views retain the same verified immutable dictionary.
        for settings in [
            &mut webhook_worker,
            &mut reconciliation_index,
            &mut retrieval,
        ]
        .into_iter()
        .flatten()
        {
            settings.lexical = lexical.clone();
            settings.projection_mode = if graph_enabled {
                notion_knowledge_notion::relation_proposal::ProjectionMode::ResolveTargets
            } else {
                notion_knowledge_notion::relation_proposal::ProjectionMode::UnresolvedTargets
            };
        }
        let max_upload_files = optional(&mut lookup, "NK_MAX_UPLOAD_FILES", "4")?
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=16).contains(n))
            .ok_or_else(|| invalid("NK_MAX_UPLOAD_FILES", "must be an integer from 1 to 16"))?;
        let file_ingestion = crate::file_ingestion::Settings::parse(
            &mut lookup,
            read_only,
            matches!(notion_auth, NotionAuth::Integration(_)),
            notion_scope.is_some(),
        )?;
        let integration_writes =
            match optional(&mut lookup, "NK_INTEGRATION_WRITES", "false")?.as_str() {
                "false" => false,
                "true" => true,
                _ => return Err(invalid("NK_INTEGRATION_WRITES", "must be true or false")),
            };
        if integration_writes
            && (read_only
                || !matches!(notion_auth, NotionAuth::Integration(_))
                || notion_scope.is_none()
                || bearer_fallback.is_none()
                || retrieval.is_none()
                || webhook_state_file.is_none()
                || (webhook_worker.is_none() && reconciliation_schedule.is_none()))
        {
            return Err(invalid(
                "NK_INTEGRATION_WRITES",
                "requires writable integration scope, loopback bearer, retrieval and the shared sync worker",
            ));
        }
        let integration_write_stores = crate::write_stores::Settings::parse_keys(
            &mut lookup,
            integration_writes,
            "NK_INTEGRATION_WRITES",
            "NK_INTEGRATION_WRITE_LEDGER_FILE",
            "NK_INTEGRATION_WRITE_AUDIT_FILE",
        )?;
        Ok(Self {
            ignored_settings,
            shutdown_timeout_seconds,
            mcp_max_in_flight,
            mcp_tool_capacity,
            mcp_max_sessions,
            mcp_session_idle_seconds,
            mcp_max_streams,
            mcp_stream_max_seconds,
            webhook_max_in_flight,
            max_upload_files,
            file_ingestion,
            reconciliation_index,
            graph_index,
            graph_boost,
            retrieval,
            reconciliation_schedule,
            webhook_worker,
            webhook_state_file,
            webhook_debounce,
            http_bind: SocketAddr::new(host, port),
            log_level,
            read_only,
            integration_write_stores,
            destructive_writes_enabled,
            capability_policy,
            bearer_fallback,
            health_requires_bearer,
            notion_auth,
            notion_scope,
            webhook,
        })
    }
}

pub(crate) fn invalid(setting: &'static str, reason: &'static str) -> ConfigError {
    ConfigError { setting, reason }
}

pub(crate) fn text(value: OsString, setting: &'static str) -> Result<String, ConfigError> {
    value
        .into_string()
        .map_err(|_| invalid(setting, "must be valid Unicode"))
}

pub(crate) fn optional(
    lookup: &mut (impl FnMut(&str) -> Option<OsString> + ?Sized),
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
    fn shutdown_timeout_is_bounded_and_explicitly_configurable() {
        assert_eq!(parse(&[]).unwrap().shutdown_timeout_seconds, 30);
        for value in ["1", "3600"] {
            assert_eq!(
                parse(&[("NK_SHUTDOWN_TIMEOUT_SECONDS", value)])
                    .unwrap()
                    .shutdown_timeout_seconds
                    .to_string(),
                value
            );
        }
        for value in ["", "0", "3601", "-1", "1.5", "secret-sentinel"] {
            let error = parse(&[("NK_SHUTDOWN_TIMEOUT_SECONDS", value)]).unwrap_err();
            assert_eq!(error.setting, "NK_SHUTDOWN_TIMEOUT_SECONDS");
            assert!(!error.to_string().contains("secret-sentinel"));
        }
    }

    #[test]
    fn graph_activation_is_explicit_and_needs_trusted_existing_state_settings() {
        assert!(parse(&[]).unwrap().graph_index.is_none());
        for value in ["", "TRUE", "yes"] {
            assert_eq!(
                parse(&[("NK_GRAPH", value)]).unwrap_err().setting,
                "NK_GRAPH"
            );
        }
        assert!(parse(&[("NK_GRAPH", "true")]).is_err());
        assert!(
            parse(&[("NK_GRAPH", "false")])
                .unwrap()
                .graph_index
                .is_none()
        );
    }
    #[test]
    fn graph_boost_is_opt_in_and_rejects_unbounded_or_unsupported_settings() {
        let defaults = parse(&[]).unwrap();
        assert_eq!(
            defaults.graph_boost,
            notion_knowledge_core::graph_boost::GraphBoostConfig::default()
        );
        assert_eq!(
            parse(&[("NK_GRAPH_BOOST", "true")]).unwrap_err().setting,
            "NK_GRAPH_BOOST"
        );
        for (name, invalid_values) in [
            ("NK_GRAPH_BOOST", vec!["", "TRUE", "yes"]),
            ("NK_GRAPH_BOOST_MAX_HOPS", vec!["0", "3", "-1", ""]),
            ("NK_GRAPH_BOOST_CANDIDATE_LIMIT", vec!["0", "101", "-1", ""]),
            (
                "NK_GRAPH_BOOST_WEIGHT",
                vec!["0", "0.51", "NaN", "inf", "-0.1", ""],
            ),
        ] {
            for value in invalid_values {
                assert_eq!(parse(&[(name, value)]).unwrap_err().setting, name);
            }
        }
        let tuned = parse(&[
            ("NK_GRAPH_BOOST_MAX_HOPS", "2"),
            ("NK_GRAPH_BOOST_WEIGHT", "0.5"),
            ("NK_GRAPH_BOOST_CANDIDATE_LIMIT", "100"),
        ])
        .unwrap();
        assert!(!tuned.graph_boost.enabled);
        assert_eq!(tuned.graph_boost.max_hops, 2);
        assert_eq!(tuned.graph_boost.weight, 0.5);
        assert_eq!(tuned.graph_boost.candidate_limit, 100);
    }
    #[test]
    fn http_admission_limits_are_finite_and_validated() {
        let defaults = parse(&[]).unwrap();
        assert_eq!(defaults.mcp_max_in_flight, 8);
        assert_eq!(defaults.mcp_tool_capacity, 16);
        assert_eq!(defaults.mcp_max_sessions, 32);
        assert_eq!(defaults.mcp_max_streams, 16);
        assert_eq!(defaults.mcp_session_idle_seconds, 300);
        assert_eq!(defaults.mcp_stream_max_seconds, 300);
        assert_eq!(defaults.webhook_max_in_flight, 4);
        let tuned = parse(&[
            ("NK_MCP_MAX_IN_FLIGHT", "2"),
            ("NK_WEBHOOK_MAX_IN_FLIGHT", "1"),
        ])
        .unwrap();
        assert_eq!(tuned.mcp_max_in_flight, 2);
        assert_eq!(tuned.webhook_max_in_flight, 1);
        for (name, maximum) in [
            ("NK_MCP_MAX_IN_FLIGHT", 256),
            ("NK_MCP_TOOL_CAPACITY", 128),
            ("NK_WEBHOOK_MAX_IN_FLIGHT", 128),
            ("NK_MCP_MAX_SESSIONS", 512),
            ("NK_MCP_SESSION_IDLE_SECONDS", 3600),
            ("NK_MCP_MAX_STREAMS", 256),
            ("NK_MCP_STREAM_MAX_SECONDS", 3600),
        ] {
            for bad in ["0", "-1", "hello", "9999999999999999999999"] {
                assert_eq!(parse(&[(name, bad)]).unwrap_err().setting, name);
            }
            assert_eq!(
                parse(&[(name, &(maximum + 1).to_string())])
                    .unwrap_err()
                    .setting,
                name
            );
        }
    }

    #[test]
    fn removed_oauth_and_grant_settings_are_ignored_with_one_warning() {
        const TOKEN: &str = "abcdefghijklmnopqrstuvwxyz0123456789";
        let single_user = vec![
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "static-private-token"),
            (
                "NK_NOTION_SCOPE_WORKSPACE_ID",
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ),
            (
                "NK_NOTION_SCOPE_ROOTS",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            ),
            ("NK_HTTP_AUTH", "bearer"),
            ("NK_HTTP_BEARER_TOKEN", TOKEN),
        ];
        let baseline = parse(&single_user).unwrap();
        assert!(baseline.ignored_settings().is_empty());
        assert!(baseline.ignored_settings_warning().is_none());
        for removed in REMOVED_SETTINGS {
            // Any value, even a malformed one, is ignored rather than validated.
            for value in [
                "https://auth.example.com",
                "/var/lib/notion/private",
                "",
                "TRUE",
            ] {
                let mut stale = single_user.clone();
                stale.push((removed, value));
                let config = parse(&stale).unwrap();
                assert_eq!(config.ignored_settings(), [removed]);
                assert!(config.bearer_fallback.is_some());
                assert!(matches!(config.notion_auth, NotionAuth::Integration(_)));
            }
        }
        let mut all = single_user.clone();
        all.extend(REMOVED_SETTINGS.map(|key| (key, "/var/lib/notion/private-value")));
        let all = parse(&all).unwrap();
        assert_eq!(all.ignored_settings(), REMOVED_SETTINGS);
        assert_eq!(
            all.notion_scope.as_ref().unwrap().workspace_id,
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        );
        let warning = all.ignored_settings_warning().unwrap();
        assert_eq!(warning.lines().count(), 1);
        for key in REMOVED_SETTINGS {
            assert!(warning.contains(key), "{key}");
        }
        assert!(!warning.contains("private-value"));
        assert!(warning.contains("may be deleted"));
        let oauth_only = parse(&[("NK_OAUTH_ISSUER", "https://auth.example.com")]).unwrap();
        assert!(
            !oauth_only
                .ignored_settings_warning()
                .unwrap()
                .contains("may be deleted")
        );
    }

    #[test]
    fn configured_notion_scope_requires_trusted_pair_and_integration() {
        const WORKSPACE: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        const ROOT: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        assert!(parse(&[]).unwrap().notion_scope.is_none());
        assert_eq!(
            parse(&[("NK_NOTION_SCOPE_ROOTS", ROOT)])
                .unwrap_err()
                .setting,
            "NK_NOTION_SCOPE_ROOTS"
        );
        assert_eq!(
            parse(&[("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE)])
                .unwrap_err()
                .setting,
            "NK_NOTION_SCOPE_ROOTS"
        );
        assert_eq!(
            parse(&[
                ("NK_NOTION_SCOPE_ROOTS", ROOT),
                ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE)
            ])
            .unwrap_err()
            .setting,
            "NK_NOTION_AUTH"
        );
        let good = parse(&[
            ("NK_NOTION_AUTH", "integration"),
            ("NOTION_TOKEN", "private-fixture-credential"),
            ("NK_NOTION_SCOPE_ROOTS", ROOT),
            ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
            ("NK_NOTION_SCOPE_GENERATION", "3"),
        ])
        .unwrap();
        let scope = good.notion_scope.unwrap();
        assert_eq!(scope.generation, 3);
        assert_eq!(scope.workspace_id, WORKSPACE);
        assert_eq!(scope.roots[0].0, ROOT);

        for (key, value) in [
            ("NK_NOTION_SCOPE_ROOTS", "untrusted-root"),
            ("NK_NOTION_SCOPE_WORKSPACE_ID", "untrusted-workspace"),
            ("NK_NOTION_SCOPE_GENERATION", "0"),
        ] {
            let mut inputs = vec![
                ("NK_NOTION_AUTH", "integration"),
                ("NOTION_TOKEN", "private-fixture-credential"),
                ("NK_NOTION_SCOPE_ROOTS", ROOT),
                ("NK_NOTION_SCOPE_WORKSPACE_ID", WORKSPACE),
                ("NK_NOTION_SCOPE_GENERATION", "3"),
            ];
            inputs.iter_mut().find(|item| item.0 == key).unwrap().1 = value;
            let err = parse(&inputs).unwrap_err();
            assert_eq!(err.setting, key);
            assert!(!err.to_string().contains("untrusted-"));
        }
    }

    #[test]
    fn development_bearer_requires_explicit_mode_and_strong_private_configuration() {
        const TOKEN: &str = "local-dev-0123456789-abcdefghijklmnopqrstuvwxyz";
        assert!(parse(&[]).unwrap().bearer_fallback.is_none());
        assert!(!parse(&[]).unwrap().health_requires_bearer);
        assert_eq!(
            parse(&[("NK_HTTP_BEARER_TOKEN", TOKEN)])
                .unwrap_err()
                .setting,
            "NK_HTTP_BEARER_TOKEN"
        );
        assert_eq!(
            parse(&[("NK_HTTP_AUTH", "bearer")]).unwrap_err().setting,
            "NK_HTTP_BEARER_TOKEN"
        );
        for bad in ["", "short", "private bad credential", "a\\nsecret"] {
            let error =
                parse(&[("NK_HTTP_AUTH", "bearer"), ("NK_HTTP_BEARER_TOKEN", bad)]).unwrap_err();
            assert_eq!(error.setting, "NK_HTTP_BEARER_TOKEN");
            if !bad.is_empty() {
                assert!(!format!("{error:?}").contains(bad));
            }
        }
        for bad_mode in ["Bearer", "auto", ""] {
            assert_eq!(
                parse(&[("NK_HTTP_AUTH", bad_mode)]).unwrap_err().setting,
                "NK_HTTP_AUTH"
            );
        }
        let config = parse(&[("NK_HTTP_AUTH", "bearer"), ("NK_HTTP_BEARER_TOKEN", TOKEN)]).unwrap();
        assert_eq!(
            config.bearer_fallback.as_ref().unwrap().expose_secret(),
            TOKEN
        );
        assert!(!format!("{config:?}").contains(TOKEN));
        assert!(!config.health_requires_bearer);
        let protected = parse(&[
            ("NK_HTTP_AUTH", "bearer"),
            ("NK_HTTP_BEARER_TOKEN", TOKEN),
            ("NK_HTTP_HEALTH_AUTH", "bearer"),
        ])
        .unwrap();
        assert!(protected.health_requires_bearer);
        assert_eq!(
            parse(&[("NK_HTTP_HEALTH_AUTH", "bearer")])
                .unwrap_err()
                .setting,
            "NK_HTTP_HEALTH_AUTH"
        );
        assert_eq!(
            parse(&[("NK_HTTP_HEALTH_AUTH", "public")])
                .unwrap_err()
                .setting,
            "NK_HTTP_HEALTH_AUTH"
        );
        assert_eq!(
            parse(&[
                ("NK_HTTP_AUTH", "bearer"),
                ("NK_HTTP_BEARER_TOKEN", TOKEN),
                ("NK_HTTP_HOST", "0.0.0.0"),
            ])
            .unwrap_err()
            .setting,
            "NK_HTTP_HOST"
        );
    }

    #[test]
    fn structured_log_level_is_operator_controlled_without_reflecting_secrets() {
        use notion_knowledge_core::logging::Level;
        assert_eq!(parse(&[]).unwrap().log_level, Level::Info);
        assert_eq!(
            parse(&[("NK_LOG_LEVEL", "DEBUG")]).unwrap().log_level,
            Level::Debug
        );
        assert_eq!(
            parse(&[("NK_LOG_LEVEL", "off")]).unwrap().log_level,
            Level::Off
        );
        let bad = parse(&[("NK_LOG_LEVEL", "authorization=private-sentinel")])
            .unwrap_err()
            .to_string();
        assert!(bad.contains("NK_LOG_LEVEL"));
        assert!(!bad.contains("private-sentinel"));
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
    fn destructive_writes_require_separate_operator_opt_in() {
        assert!(!parse(&[]).unwrap().destructive_writes_enabled);
        assert!(
            !parse(&[("NK_READ_ONLY", "false")])
                .unwrap()
                .destructive_writes_enabled
        );
        assert!(
            parse(&[("NK_READ_ONLY", "false"), ("NK_DESTRUCTIVE_WRITES", "true")])
                .unwrap()
                .destructive_writes_enabled
        );
        assert_eq!(
            parse(&[("NK_DESTRUCTIVE_WRITES", "true")])
                .unwrap_err()
                .setting,
            "NK_DESTRUCTIVE_WRITES"
        );
        for invalid_value in ["", "1", "TRUE", " true ", "private-sentinel"] {
            let error = parse(&[
                ("NK_READ_ONLY", "false"),
                ("NK_DESTRUCTIVE_WRITES", invalid_value),
            ])
            .unwrap_err();
            assert_eq!(error.setting, "NK_DESTRUCTIVE_WRITES");
            assert!(!error.to_string().contains("private-sentinel"));
        }
    }

    #[test]
    fn operation_lists_validate_exact_identifiers_and_report_only_setting_names() {
        let configured = parse(&[
            ("NK_TOOL_ALLOWLIST", "read,knowledge_append"),
            ("NK_TOOL_BLOCKLIST", "knowledge_get"),
        ])
        .unwrap();
        assert!(configured.capability_policy.permits("knowledge_search"));
        assert!(!configured.capability_policy.permits("knowledge_get"));
        assert!(configured.capability_policy.permits("knowledge_append"));
        for key in ["NK_TOOL_ALLOWLIST", "NK_TOOL_BLOCKLIST"] {
            let err = parse(&[(key, "invalid_unknown_operation")]).unwrap_err();
            assert_eq!(err.setting, key);
            assert_eq!(err.reason, "unknown or malformed capability");
            assert!(!err.to_string().contains("invalid_unknown_operation"));
        }
        assert!(
            !parse(&[("NK_TOOL_ALLOWLIST", "")])
                .unwrap()
                .capability_policy
                .permits("knowledge_get")
        );
    }

    #[test]
    fn bootstrap_defaults_need_no_credentials() {
        let config = parse(&[]).unwrap();
        assert_eq!(
            config.http_bind,
            "127.0.0.1:3000".parse::<SocketAddr>().unwrap()
        );
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
