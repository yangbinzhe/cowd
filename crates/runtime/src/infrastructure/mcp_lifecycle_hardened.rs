#![allow(clippy::unnested_or_patterns, clippy::map_unwrap_or)]
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpLifecyclePhase {
    ConfigLoad,
    ServerRegistration,
    SpawnConnect,
    InitializeHandshake,
    ToolDiscovery,
    ResourceDiscovery,
    Ready,
    Invocation,
    ErrorSurfacing,
    Shutdown,
    Cleanup,
}

impl McpLifecyclePhase {
    #[must_use]
    pub fn all() -> [Self; 11] {
        [
            Self::ConfigLoad,
            Self::ServerRegistration,
            Self::SpawnConnect,
            Self::InitializeHandshake,
            Self::ToolDiscovery,
            Self::ResourceDiscovery,
            Self::Ready,
            Self::Invocation,
            Self::ErrorSurfacing,
            Self::Shutdown,
            Self::Cleanup,
        ]
    }
}

impl std::fmt::Display for McpLifecyclePhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConfigLoad => write!(f, "config_load"),
            Self::ServerRegistration => write!(f, "server_registration"),
            Self::SpawnConnect => write!(f, "spawn_connect"),
            Self::InitializeHandshake => write!(f, "initialize_handshake"),
            Self::ToolDiscovery => write!(f, "tool_discovery"),
            Self::ResourceDiscovery => write!(f, "resource_discovery"),
            Self::Ready => write!(f, "ready"),
            Self::Invocation => write!(f, "invocation"),
            Self::ErrorSurfacing => write!(f, "error_surfacing"),
            Self::Shutdown => write!(f, "shutdown"),
            Self::Cleanup => write!(f, "cleanup"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpErrorSurface {
    pub phase: McpLifecyclePhase,
    pub server_name: Option<String>,
    pub message: String,
    pub context: BTreeMap<String, String>,
    pub recoverable: bool,
    pub timestamp: u64,
}

impl McpErrorSurface {
    #[must_use]
    pub fn new(
        phase: McpLifecyclePhase,
        server_name: Option<String>,
        message: impl Into<String>,
        context: BTreeMap<String, String>,
        recoverable: bool,
    ) -> Self {
        Self {
            phase,
            server_name,
            message: message.into(),
            context,
            recoverable,
            timestamp: now_secs(),
        }
    }
}

impl std::fmt::Display for McpErrorSurface {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MCP lifecycle error during {}: {}",
            self.phase, self.message
        )?;
        if let Some(server_name) = &self.server_name {
            write!(f, " (server: {server_name})")?;
        }
        if !self.context.is_empty() {
            write!(f, " with context {:?}", self.context)?;
        }
        if self.recoverable {
            write!(f, " [recoverable]")?;
        }
        Ok(())
    }
}

impl std::error::Error for McpErrorSurface {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpFailedServer {
    pub server_name: String,
    pub phase: McpLifecyclePhase,
    pub error: McpErrorSurface,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpDegradedReport {
    pub working_servers: Vec<String>,
    pub failed_servers: Vec<McpFailedServer>,
    pub available_tools: Vec<String>,
    pub missing_tools: Vec<String>,
}

impl McpDegradedReport {
    #[must_use]
    pub fn new(
        working_servers: Vec<String>,
        failed_servers: Vec<McpFailedServer>,
        available_tools: Vec<String>,
        expected_tools: Vec<String>,
    ) -> Self {
        let working_servers = dedupe_sorted(working_servers);
        let available_tools = dedupe_sorted(available_tools);
        let available_tool_set: BTreeSet<_> = available_tools.iter().cloned().collect();
        let expected_tools = dedupe_sorted(expected_tools);
        let missing_tools = expected_tools
            .into_iter()
            .filter(|tool| !available_tool_set.contains(tool))
            .collect();

        Self {
            working_servers,
            failed_servers,
            available_tools,
            missing_tools,
        }
    }
}

fn dedupe_sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values.dedup();
    values
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    #[test]
    fn phase_display_matches_serde_name() {
        // given
        let phases = McpLifecyclePhase::all();

        // when
        let serialized = phases
            .into_iter()
            .map(|phase| {
                (
                    phase.to_string(),
                    serde_json::to_value(phase).expect("serialize phase"),
                )
            })
            .collect::<Vec<_>>();

        // then
        for (display, json_value) in serialized {
            assert_eq!(json_value, json!(display));
        }
    }

    #[test]
    fn given_partial_server_health_when_building_degraded_report_then_missing_tools_are_reported() {
        // given
        let failed = vec![McpFailedServer {
            server_name: "broken".to_string(),
            phase: McpLifecyclePhase::InitializeHandshake,
            error: McpErrorSurface::new(
                McpLifecyclePhase::InitializeHandshake,
                Some("broken".to_string()),
                "initialize failed",
                BTreeMap::from([("reason".to_string(), "broken pipe".to_string())]),
                false,
            ),
        }];

        // when
        let report = McpDegradedReport::new(
            vec!["alpha".to_string(), "beta".to_string(), "alpha".to_string()],
            failed,
            vec![
                "alpha.echo".to_string(),
                "beta.search".to_string(),
                "alpha.echo".to_string(),
            ],
            vec![
                "alpha.echo".to_string(),
                "beta.search".to_string(),
                "broken.fetch".to_string(),
            ],
        );

        // then
        assert_eq!(
            report.working_servers,
            vec!["alpha".to_string(), "beta".to_string()]
        );
        assert_eq!(report.failed_servers.len(), 1);
        assert_eq!(report.failed_servers[0].server_name, "broken");
        assert_eq!(
            report.available_tools,
            vec!["alpha.echo".to_string(), "beta.search".to_string()]
        );
        assert_eq!(report.missing_tools, vec!["broken.fetch".to_string()]);
    }

    #[test]
    fn error_surface_display_includes_phase_server_and_recoverable_flag() {
        // given
        let error = McpErrorSurface::new(
            McpLifecyclePhase::SpawnConnect,
            Some("alpha".to_string()),
            "process exited early",
            BTreeMap::from([("exit_code".to_string(), "1".to_string())]),
            true,
        );

        // when
        let rendered = error.to_string();

        // then
        assert!(rendered.contains("spawn_connect"));
        assert!(rendered.contains("process exited early"));
        assert!(rendered.contains("server: alpha"));
        assert!(rendered.contains("recoverable"));
        let trait_object: &dyn std::error::Error = &error;
        assert_eq!(trait_object.to_string(), rendered);
    }
}
