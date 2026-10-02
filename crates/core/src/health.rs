//! Adapter-neutral health snapshots. No credentials, URLs, or error text belong here.

/// An adapter's most recent dependency assessment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DependencyState {
    Healthy,
    Unconfigured,
    Unavailable,
    Unknown,
}

impl DependencyState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Unconfigured => "unconfigured",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

/// Adapters expose a bounded snapshot, without doing I/O in a diagnostic request.
/// Implementations must return Unknown until checked and when their observation
/// is stale; configuration alone must never produce Healthy.
pub trait HealthProbe: Send + Sync {
    fn state(&self) -> DependencyState;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Health {
    pub notion: DependencyState,
    pub index: DependencyState,
}

impl Health {
    pub fn observe(notion: &dyn HealthProbe, index: &dyn HealthProbe) -> Self {
        Self {
            notion: notion.state(),
            index: index.state(),
        }
    }

    pub fn is_healthy(self) -> bool {
        self.notion == DependencyState::Healthy && self.index == DependencyState::Healthy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Probe(DependencyState);
    impl HealthProbe for Probe {
        fn state(&self) -> DependencyState {
            self.0
        }
    }

    #[test]
    fn health_requires_both_dependencies_and_preserves_individual_states() {
        let states = [
            DependencyState::Healthy,
            DependencyState::Unconfigured,
            DependencyState::Unavailable,
            DependencyState::Unknown,
        ];
        for notion in states {
            for index in states {
                let report = Health::observe(&Probe(notion), &Probe(index));
                assert_eq!(report.notion, notion);
                assert_eq!(report.index, index);
                assert_eq!(
                    report.is_healthy(),
                    notion == DependencyState::Healthy && index == DependencyState::Healthy
                );
            }
        }
    }
}
