//! Goal, observation, and intervention contracts for governed execution.
//!
//! These types are pure data contracts. Runtime owns persistence, policy, and
//! graph application; Gateway and surfaces only consume their projections.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceStatus {
    Open,
    Satisfied,
    Blocked,
    Waived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceCriterion {
    pub id: String,
    pub statement: String,
    #[serde(default)]
    pub required_evidence: Vec<String>,
    pub status: AcceptanceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiver: Option<CriterionWaiver>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CriterionWaiver {
    pub actor: String,
    pub reason: String,
    pub permission_receipt: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GoalCompletion {
    Open,
    Satisfied,
    Blocked,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalContract {
    pub id: String,
    pub session_id: String,
    pub objective: String,
    pub criteria: Vec<AcceptanceCriterion>,
    #[serde(default)]
    pub constraints: Vec<String>,
    pub phase: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub unresolved: Vec<String>,
    #[serde(default)]
    pub blockers: Vec<String>,
    pub completion: GoalCompletion,
    pub revision: u64,
    pub user_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalRevision {
    pub goal_id: String,
    pub previous_revision: u64,
    pub revision: u64,
    pub reason: String,
    pub user_sequence: u64,
    pub changed_fields: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeObservationKind {
    ToolProgress,
    GraphProgress,
    ContextPressure,
    ProviderProgress,
    UserInput,
    StrategyHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeObservation {
    pub goal_id: String,
    pub kind: RuntimeObservationKind,
    pub source: String,
    pub summary: String,
    /// Stable identity for an observation pattern. Runtime uses it to
    /// distinguish a repeated failed action from unrelated low-progress work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint: Option<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    /// Bounded, machine-readable measurements for Runtime policy. Control
    /// decisions must not parse the human-facing `summary` string.
    #[serde(default)]
    pub metrics: BTreeMap<String, i64>,
    pub progress_delta: i32,
    pub novelty: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeInterventionKind {
    Continue,
    Parallelize,
    Retrieve,
    Replan,
    Switch,
    Synthesize,
    Block,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeIntervention {
    pub goal_id: String,
    pub kind: RuntimeInterventionKind,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub expected_graph_revision: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn goal_contract_roundtrips_revisioned_acceptance() {
        let contract = GoalContract {
            id: "goal-1".into(),
            session_id: "session-1".into(),
            objective: "finish the governed task".into(),
            criteria: vec![AcceptanceCriterion {
                id: "criterion-1".into(),
                statement: "produce checked result".into(),
                required_evidence: vec!["evidence:1".into()],
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: vec!["read_only".into()],
            phase: "execution".into(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
            blockers: Vec::new(),
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        };
        assert_eq!(
            serde_json::from_str::<GoalContract>(&serde_json::to_string(&contract).unwrap())
                .unwrap(),
            contract
        );
    }
}
