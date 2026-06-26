use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeBoundaryStatus {
    pub(crate) protocol_version: u32,
    pub(crate) runtime_host: &'static str,
    pub(crate) active_sessions: usize,
    pub(crate) uptime_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RuntimeBoundarySnapshot {
    pub(crate) protocol_version: u32,
    pub(crate) runtime_host: &'static str,
    pub(crate) active_sessions: usize,
    pub(crate) uptime_secs: u64,
    pub(crate) sessions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeBoundaryClock {
    uptime: Duration,
}

impl RuntimeBoundaryClock {
    #[must_use]
    pub(crate) fn from_uptime(uptime: Duration) -> Self {
        Self { uptime }
    }

    #[must_use]
    pub(crate) fn uptime_secs(&self) -> u64 {
        self.uptime.as_secs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_boundary_clock_reports_seconds_without_runtime_bootstrap() {
        let clock = RuntimeBoundaryClock::from_uptime(Duration::from_secs(12));
        assert_eq!(clock.uptime_secs(), 12);
    }
}
