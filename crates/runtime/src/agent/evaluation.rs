//! Immutable terminal Agent-run evaluation evidence and read-only self-models.
//!
//! An evaluation is written only by `AgentRuntime` with the terminal packet in
//! the same Runtime event transaction. It is not a mutable score table and it
//! never authorizes an evolution release by itself.

use std::collections::BTreeMap;

use harness_contract::agent::{
    AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus, ReleaseChannel,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunEvaluation {
    pub evaluation_id: String,
    pub run_id: String,
    pub agent_instance_id: String,
    pub definition_id: String,
    pub definition_revision: u64,
    pub binding_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_assignment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release_channel: Option<ReleaseChannel>,
    pub task_id: String,
    pub task_domain: String,
    pub complexity: String,
    pub role_slot_id: String,
    pub model: String,
    pub provider: String,
    pub granted_capabilities: Vec<String>,
    pub allowed_tools: Vec<String>,
    pub allowed_skills: Vec<String>,
    pub memory_reality_fingerprint: String,
    pub team_id: Option<String>,
    pub environment_fingerprint: String,
    pub terminal_status: AgentTerminalStatus,
    pub acceptance: Vec<String>,
    pub outcome: String,
    pub failure: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub tool_calls: u64,
    pub evidence_refs: Vec<String>,
    pub created_at_ms: u64,
}

impl AgentRunEvaluation {
    #[must_use]
    pub fn from_terminal(
        packet: &AgentTaskPacket,
        returned: &AgentReturnPacket,
        created_at_ms: u64,
    ) -> Option<Self> {
        let binding = packet.binding.as_ref()?;
        let mut granted_capabilities = binding
            .effective_capabilities
            .iter()
            .map(|capability| capability.as_str().to_string())
            .collect::<Vec<_>>();
        granted_capabilities.sort();
        let mut allowed_tools = packet.allowed_tools.clone();
        allowed_tools.sort();
        allowed_tools.dedup();
        let mut allowed_skills = packet.allowed_skills.clone();
        allowed_skills.sort();
        allowed_skills.dedup();
        let memory_reality_fingerprint = digest_json(&serde_json::json!({
            "read_scopes": binding.data_lease.read_scopes,
            "fact_boundaries": binding.data_lease.fact_boundaries,
            "fact_refs": binding.data_lease.fact_refs,
            "matrix_snapshot_refs": binding.data_lease.matrix_snapshot_refs,
            "team_working_state_visible": binding.data_lease.team_working_state_visible,
        }));
        let environment_fingerprint = digest_json(&serde_json::json!({
            "provider": returned.provider,
            "model": returned.model,
            "team_id": packet.team_id(),
            "permission_lease": packet.permission_lease,
            "tool_contract_refs": binding.tool_contract_refs,
            "skill_refs": binding.skill_refs,
        }));
        Some(Self {
            evaluation_id: format!(
                "agent-run-evaluation:{}:{}",
                packet.run_id(),
                binding.binding_digest
            ),
            run_id: packet.run_id().to_string(),
            agent_instance_id: packet.agent_id().to_string(),
            definition_id: binding.definition_ref.definition_id.as_str().to_string(),
            definition_revision: binding.definition_ref.revision,
            binding_digest: binding.binding_digest.clone(),
            release_assignment_id: binding
                .release
                .as_ref()
                .map(|release| release.assignment_id.clone()),
            release_generation: binding.release.as_ref().map(|release| release.generation),
            release_channel: binding.release.as_ref().map(|release| release.channel),
            task_id: packet.task_id().to_string(),
            task_domain: task_domain(packet),
            complexity: task_complexity(packet),
            role_slot_id: binding.instance.role_slot_id.clone().unwrap_or_default(),
            model: returned.model.clone(),
            provider: returned.provider.clone(),
            granted_capabilities,
            allowed_tools,
            allowed_skills,
            memory_reality_fingerprint,
            team_id: packet.team_id().map(str::to_owned),
            environment_fingerprint,
            terminal_status: returned.status,
            acceptance: packet.acceptance.clone(),
            outcome: returned.outcome.clone(),
            failure: returned.failure.clone(),
            input_tokens: returned.input_tokens,
            output_tokens: returned.output_tokens,
            tool_calls: returned.tool_calls,
            evidence_refs: returned
                .evidence_refs
                .iter()
                .map(|reference| reference.evidence_ref.0.id.clone())
                .collect(),
            created_at_ms,
        })
    }

    #[must_use]
    pub fn is_success(&self) -> bool {
        self.terminal_status == AgentTerminalStatus::Completed && self.failure.is_none()
    }
}

/// Read-only aggregation of immutable terminal evaluations. A definition's
/// model is separated by environment fingerprint so a stronger provider, tool
/// grant, or team context cannot inflate another runtime setting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSelfModel {
    pub definition_id: String,
    pub definition_revision: u64,
    pub environment_fingerprint: String,
    pub run_count: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_tool_calls: u64,
    pub task_domains: Vec<String>,
    pub successful_evidence_refs: Vec<String>,
    pub failed_evidence_refs: Vec<String>,
}

impl AgentSelfModel {
    #[must_use]
    pub fn success_rate_millis(&self) -> u64 {
        if self.run_count == 0 {
            0
        } else {
            self.success_count.saturating_mul(1_000) / self.run_count
        }
    }
}

#[must_use]
pub fn project_self_models(
    evaluations: impl IntoIterator<Item = AgentRunEvaluation>,
) -> Vec<AgentSelfModel> {
    let mut groups = BTreeMap::<(String, u64, String), AgentSelfModel>::new();
    for evaluation in evaluations {
        let key = (
            evaluation.definition_id.clone(),
            evaluation.definition_revision,
            evaluation.environment_fingerprint.clone(),
        );
        let model = groups.entry(key).or_insert_with(|| AgentSelfModel {
            definition_id: evaluation.definition_id.clone(),
            definition_revision: evaluation.definition_revision,
            environment_fingerprint: evaluation.environment_fingerprint.clone(),
            run_count: 0,
            success_count: 0,
            failure_count: 0,
            total_input_tokens: 0,
            total_output_tokens: 0,
            total_tool_calls: 0,
            task_domains: Vec::new(),
            successful_evidence_refs: Vec::new(),
            failed_evidence_refs: Vec::new(),
        });
        model.run_count = model.run_count.saturating_add(1);
        model.total_input_tokens = model
            .total_input_tokens
            .saturating_add(evaluation.input_tokens);
        model.total_output_tokens = model
            .total_output_tokens
            .saturating_add(evaluation.output_tokens);
        model.total_tool_calls = model.total_tool_calls.saturating_add(evaluation.tool_calls);
        if !model.task_domains.contains(&evaluation.task_domain) {
            model.task_domains.push(evaluation.task_domain.clone());
        }
        let target = if evaluation.is_success() {
            model.success_count = model.success_count.saturating_add(1);
            &mut model.successful_evidence_refs
        } else {
            model.failure_count = model.failure_count.saturating_add(1);
            &mut model.failed_evidence_refs
        };
        for evidence in evaluation.evidence_refs {
            if !target.contains(&evidence) {
                target.push(evidence);
            }
        }
    }
    let mut models = groups.into_values().collect::<Vec<_>>();
    for model in &mut models {
        model.task_domains.sort();
        model.successful_evidence_refs.sort();
        model.failed_evidence_refs.sort();
    }
    models.sort_by(|left, right| {
        left.definition_id
            .cmp(&right.definition_id)
            .then_with(|| left.definition_revision.cmp(&right.definition_revision))
            .then_with(|| {
                left.environment_fingerprint
                    .cmp(&right.environment_fingerprint)
            })
    });
    models
}

fn task_domain(packet: &AgentTaskPacket) -> String {
    packet
        .task_id()
        .split([':', '/', '-'])
        .find(|part| !part.trim().is_empty())
        .unwrap_or("general")
        .to_ascii_lowercase()
}

fn task_complexity(packet: &AgentTaskPacket) -> String {
    let score = packet.acceptance.len()
        + packet.constraints.len()
        + packet.context_refs.len()
        + usize::from(packet.team_id().is_some());
    match score {
        0..=2 => "low",
        3..=6 => "medium",
        _ => "high",
    }
    .to_string()
}

fn digest_json(value: &serde_json::Value) -> String {
    let encoded = serde_json::to_vec(value).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(encoded))
}
