//! Secret-free identity and dependency diagnostics for this process.

use std::sync::Arc;

use crate::config::{Config, NotionAuth};
use notion_knowledge_core::{
    SERVER_NAME, VERSION,
    health::{DependencyState, Health, HealthProbe},
};
use serde_json::{Value, json};

struct BootstrapProbe(DependencyState);
impl HealthProbe for BootstrapProbe {
    fn state(&self) -> DependencyState {
        self.0
    }
}

/// Credentials are configuration, not evidence of a working upstream adapter.
/// Replace these probes with concrete adapter snapshots when wiring the adapters.
#[derive(Clone)]
pub struct Diagnostics {
    notion: Arc<dyn HealthProbe>,
    index: Arc<dyn HealthProbe>,
    read_only: bool,
}

impl Diagnostics {
    pub fn new(notion: Arc<dyn HealthProbe>, index: Arc<dyn HealthProbe>) -> Self {
        Self {
            notion,
            index,
            read_only: true,
        }
    }

    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub fn health(&self) -> Health {
        Health::observe(self.notion.as_ref(), self.index.as_ref())
    }
}

pub fn bootstrap(config: &Config) -> Diagnostics {
    let notion = match config.notion_auth {
        NotionAuth::None => DependencyState::Unconfigured,
        NotionAuth::Integration(_) => DependencyState::Unavailable,
    };
    Diagnostics::new(
        Arc::new(BootstrapProbe(notion)),
        Arc::new(BootstrapProbe(DependencyState::Unavailable)),
    )
    .with_read_only(config.read_only)
}

/// This allowlisted projection cannot carry adapter errors, tokens or signed URLs.
pub fn report(health: Health, transport: &'static str, read_only: bool) -> Value {
    json!({
        "server": {"name": SERVER_NAME, "version": VERSION},
        "transport": transport,
        "access": {"read_only": read_only},
        "status": if health.is_healthy() { "healthy" } else { "degraded" },
        "dependencies": {"notion": health.notion.as_str(), "index": health.index.as_str()}
    })
}
