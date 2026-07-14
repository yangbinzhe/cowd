use std::{
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

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

#[derive(Debug, Clone)]
pub struct EvolutionMissionStore {
    path: PathBuf,
}

impl EvolutionMissionStore {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("missions.jsonl"),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, mission: &EvolutionMission) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| error.to_string())?;
        writeln!(
            file,
            "{}",
            serde_json::to_string(mission).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())
    }

    pub fn list(&self) -> Result<Vec<EvolutionMission>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(&self.path).map_err(|error| error.to_string())?;
        let mut missions = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| error.to_string())?;
            if line.trim().is_empty() {
                continue;
            }
            missions.push(
                serde_json::from_str::<EvolutionMission>(&line)
                    .map_err(|error| error.to_string())?,
            );
        }
        missions.sort_by(|left, right| right.updated_at_ms.cmp(&left.updated_at_ms));
        Ok(missions)
    }

    pub fn update(
        &self,
        mission_id: &str,
        update: impl FnOnce(&mut EvolutionMission),
    ) -> Result<EvolutionMission, String> {
        let mut missions = self.list()?;
        let Some(mission) = missions
            .iter_mut()
            .find(|mission| mission.mission_id == mission_id)
        else {
            return Err("evolution mission not found".to_string());
        };
        update(mission);
        mission.updated_at_ms = now_ms();
        let updated = mission.clone();
        missions.sort_by(|left, right| left.created_at_ms.cmp(&right.created_at_ms));
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = fs::File::create(&self.path).map_err(|error| error.to_string())?;
        for mission in &missions {
            writeln!(
                file,
                "{}",
                serde_json::to_string(mission).map_err(|error| error.to_string())?
            )
            .map_err(|error| error.to_string())?;
        }
        Ok(updated)
    }

    pub fn find(&self, mission_id: &str) -> Result<Option<EvolutionMission>, String> {
        Ok(self
            .list()?
            .into_iter()
            .find(|mission| mission.mission_id == mission_id))
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
