//! Declarative agent contracts for Cowd AI execution.
//!
//! This crate describes what an agent is allowed and expected to do. It does
//! not execute tools, spawn processes, or own runtime orchestration.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum AgentSpecError {
    #[error("missing field: {0}")]
    MissingField(String),
    #[error("invalid contract: {0}")]
    InvalidContract(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentExecutorKind {
    CowdNative,
    ExternalCli,
    McpBacked,
    ManualReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentToolPermission {
    ReadOnly,
    WriteWorkspace,
    ConnectorAction,
    MatrixWrite,
    MemoryCandidateOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPolicyRequirement {
    RequiresApproval,
    RequiresMatrixEvidence,
    RequiresVerification,
    RequiresWorktreeIsolation,
    RequiresHumanReview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentMemoryScope {
    None,
    CandidateOnly,
    Session,
    Team,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentOutputContract {
    pub required_fields: Vec<String>,
    pub evidence_required: bool,
}

impl AgentOutputContract {
    #[must_use]
    pub fn reviewable() -> Self {
        Self {
            required_fields: vec![
                "summary".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            evidence_required: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSpec {
    pub id: String,
    pub name: String,
    pub description: String,
    pub instructions: String,
    pub executor: AgentExecutorKind,
    pub model: Option<String>,
    pub tools: Vec<AgentToolPermission>,
    pub policies: Vec<AgentPolicyRequirement>,
    pub os_env: Vec<String>,
    pub context_profile: String,
    pub memory_scope: AgentMemoryScope,
    pub matrix_requirements: Vec<String>,
    pub subagents: Vec<String>,
    pub output_contract: AgentOutputContract,
}

impl AgentSpec {
    #[must_use]
    pub fn cowd_native(
        id: impl Into<String>,
        name: impl Into<String>,
        instructions: impl Into<String>,
    ) -> Self {
        let name = name.into();
        Self {
            id: id.into(),
            description: format!("{name} agent"),
            name,
            instructions: instructions.into(),
            executor: AgentExecutorKind::CowdNative,
            model: None,
            tools: vec![AgentToolPermission::ReadOnly],
            policies: vec![AgentPolicyRequirement::RequiresVerification],
            os_env: Vec::new(),
            context_profile: "main_turn".to_string(),
            memory_scope: AgentMemoryScope::CandidateOnly,
            matrix_requirements: Vec::new(),
            subagents: Vec::new(),
            output_contract: AgentOutputContract::reviewable(),
        }
    }

    #[must_use]
    pub fn reviewer() -> Self {
        Self::cowd_native(
            "agent-spec-reviewer",
            "reviewer",
            "Review implementation evidence, risks, and regressions.",
        )
        .with_policy(AgentPolicyRequirement::RequiresHumanReview)
        .with_matrix_requirement("review_evidence")
    }

    #[must_use]
    pub fn worker() -> Self {
        Self::cowd_native(
            "agent-spec-worker",
            "worker",
            "Execute a bounded task and return evidence-backed output.",
        )
    }

    #[must_use]
    pub fn with_tool(mut self, permission: AgentToolPermission) -> Self {
        if !self.tools.contains(&permission) {
            self.tools.push(permission);
        }
        self
    }

    #[must_use]
    pub fn with_policy(mut self, requirement: AgentPolicyRequirement) -> Self {
        if !self.policies.contains(&requirement) {
            self.policies.push(requirement);
        }
        self
    }

    #[must_use]
    pub fn with_matrix_requirement(mut self, requirement: impl Into<String>) -> Self {
        let requirement = requirement.into();
        if !self.matrix_requirements.contains(&requirement) {
            self.matrix_requirements.push(requirement);
        }
        self
    }

    pub fn validate(&self) -> Result<(), AgentSpecError> {
        if self.id.trim().is_empty() {
            return Err(AgentSpecError::MissingField("id".to_string()));
        }
        if self.name.trim().is_empty() {
            return Err(AgentSpecError::MissingField("name".to_string()));
        }
        if self.instructions.trim().is_empty() {
            return Err(AgentSpecError::MissingField("instructions".to_string()));
        }
        if self.tools.is_empty() {
            return Err(AgentSpecError::InvalidContract(
                "agent must declare at least one tool permission".to_string(),
            ));
        }
        if self.output_contract.evidence_required
            && !self
                .output_contract
                .required_fields
                .iter()
                .any(|field| field == "evidence")
        {
            return Err(AgentSpecError::InvalidContract(
                "evidence-required contract must include evidence field".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_worker_spec_validates() {
        AgentSpec::worker().validate().expect("valid worker spec");
    }

    #[test]
    fn empty_instructions_are_rejected() {
        let mut spec = AgentSpec::worker();
        spec.instructions.clear();
        assert!(matches!(
            spec.validate(),
            Err(AgentSpecError::MissingField(field)) if field == "instructions"
        ));
    }
}
