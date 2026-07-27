use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::capability_goal::EvolutionCapabilityGoal;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvolutionMissionStatus {
    Open,
    CandidateReady,
    Running,
    Evaluating,
    Promoted,
    Rejected,
    RolledBack,
    Archived,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionMission {
    pub mission_id: String,
    pub status: EvolutionMissionStatus,
    pub owner: String,
    pub scope: Vec<String>,
    pub goal_ids: Vec<String>,
    pub goals: Vec<EvolutionCapabilityGoal>,
    pub signal_ids: Vec<String>,
    pub diagnosis_id: String,
    pub proposal_ids: Vec<String>,
    pub candidate_ids: Vec<String>,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
}

impl EvolutionMission {
    #[must_use]
    pub fn new(
        owner: impl Into<String>,
        scope: Vec<String>,
        signal_ids: Vec<String>,
        diagnosis_id: impl Into<String>,
        goals: Vec<EvolutionCapabilityGoal>,
    ) -> Self {
        let now = now_ms();
        let goal_ids = goals.iter().map(|goal| goal.goal_id.clone()).collect();
        Self {
            mission_id: format!("evo-mission-{}", Uuid::new_v4()),
            status: EvolutionMissionStatus::Open,
            owner: owner.into(),
            scope,
            goal_ids,
            goals,
            signal_ids,
            diagnosis_id: diagnosis_id.into(),
            proposal_ids: Vec::new(),
            candidate_ids: Vec::new(),
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    pub fn attach_proposal(&mut self, proposal_id: impl Into<String>) {
        let proposal_id = proposal_id.into();
        if !self.proposal_ids.contains(&proposal_id) {
            self.proposal_ids.push(proposal_id);
        }
        self.updated_at_ms = now_ms();
    }

    pub fn attach_candidate(&mut self, candidate_id: impl Into<String>) {
        let candidate_id = candidate_id.into();
        if !self.candidate_ids.contains(&candidate_id) {
            self.candidate_ids.push(candidate_id);
        }
        self.status = EvolutionMissionStatus::CandidateReady;
        self.updated_at_ms = now_ms();
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
