//! Canonical execution outcome contracts.
//!
//! Outcomes are durable observations about an execution. They are not policy
//! decisions and cannot promote a provider, strategy, Agent, Team, Tool, or
//! Skill on their own.

use serde::{Deserialize, Serialize};

use crate::{
    reality::{EvidenceCompleteness, EvidenceRef},
    strategy::ExecutionCandidateKind,
};

pub const OUTCOME_SCHEMA_REVISION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    pub identity: OutcomeIdentity,
    pub runtime: RuntimeIdentity,
    pub provider: Option<ProviderIdentity>,
    pub strategy: StrategyIdentity,
    pub timing: OutcomeTiming,
    pub usage: OutcomeUsage,
    pub terminal: OutcomeTerminalClass,
    pub quality: OutcomeQuality,
    pub observation: OutcomeObservation,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceRef>,
    pub evidence_completeness: EvidenceCompleteness,
    pub schema_revision: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeIdentity {
    pub execution_id: String,
    pub session_id: String,
    pub turn_id: String,
    pub terminal_generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paired_sample_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_graph_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIdentity {
    pub workspace_key: String,
    pub runtime_revision: String,
    pub config_revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderIdentity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_revision: Option<u64>,
    pub provider_name: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyIdentity {
    pub decision_id: String,
    pub policy_revision: String,
    pub decision_source: String,
    pub selected_candidate: ExecutionCandidateKind,
    pub selected_pattern: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeTiming {
    pub started_at_ms: u64,
    pub completed_at_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeUsage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub evaluation_tokens: Option<u64>,
    pub tool_calls: u64,
    pub duplicate_tool_calls: u64,
    pub retries: u64,
    pub max_observed_concurrency: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "class", content = "reason")]
pub enum OutcomeTerminalClass {
    Succeeded(String),
    Failed(String),
    Cancelled(String),
    Blocked(String),
    PartialFailure(String),
}

impl OutcomeTerminalClass {
    #[must_use]
    pub const fn is_success(&self) -> bool {
        matches!(self, Self::Succeeded(_))
    }

    #[must_use]
    pub const fn class_name(&self) -> &'static str {
        match self {
            Self::Succeeded(_) => "succeeded",
            Self::Failed(_) => "failed",
            Self::Cancelled(_) => "cancelled",
            Self::Blocked(_) => "blocked",
            Self::PartialFailure(_) => "partial_failure",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum OutcomeQuality {
    Unknown,
    Estimate {
        value_bp: u16,
        basis: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        calibration_ref: Option<String>,
    },
}

impl OutcomeQuality {
    #[must_use]
    pub fn estimate(
        value_bp: u16,
        basis: impl Into<String>,
        calibration_ref: Option<String>,
    ) -> Self {
        Self::Estimate {
            value_bp: value_bp.min(10_000),
            basis: basis.into(),
            calibration_ref,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutcomeObservation {
    pub source: String,
    pub observed_at_ms: u64,
    pub freshness_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OutcomeSegmentKey {
    pub provider: String,
    pub model: String,
    pub profile: String,
    pub protocol: String,
    pub config_revision: String,
    pub policy_revision: String,
    pub candidate: ExecutionCandidateKind,
}

impl OutcomeSegmentKey {
    #[must_use]
    pub fn from_outcome(outcome: &ExecutionOutcome) -> Self {
        let provider = outcome.provider.as_ref();
        Self {
            provider: provider
                .map(|identity| identity.provider_name.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            model: provider
                .map(|identity| identity.model.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            profile: provider
                .and_then(|identity| identity.profile.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            protocol: provider
                .and_then(|identity| identity.protocol.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            config_revision: outcome.runtime.config_revision.clone(),
            policy_revision: outcome.strategy.policy_revision.clone(),
            candidate: outcome.strategy.selected_candidate,
        }
    }
}
