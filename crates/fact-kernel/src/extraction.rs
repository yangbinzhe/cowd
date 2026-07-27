use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::candidate::FactCandidate;
use crate::core::FactEvidenceId;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FactExtractionBatchId(String);

impl FactExtractionBatchId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("fact-extraction-batch-{}", Uuid::new_v4()))
    }

    #[must_use]
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for FactExtractionBatchId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactExtractionTrigger {
    TurnEnd,
    SessionCompaction,
    Handoff,
    DeepInvestigation,
    Import,
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FactExtractionTokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactExtractionBatch {
    pub batch_id: FactExtractionBatchId,
    pub trigger: FactExtractionTrigger,
    pub session_id: Option<String>,
    pub project_id: Option<String>,
    pub task_id: Option<String>,
    pub team_id: Option<String>,
    pub candidates: Vec<FactCandidate>,
    pub source_evidence: Vec<FactEvidenceId>,
    pub token_usage: FactExtractionTokenUsage,
    pub created_at: DateTime<Utc>,
}

impl FactExtractionBatch {
    #[must_use]
    pub fn new(trigger: FactExtractionTrigger, candidates: Vec<FactCandidate>) -> Self {
        Self {
            batch_id: FactExtractionBatchId::new(),
            trigger,
            session_id: None,
            project_id: None,
            task_id: None,
            team_id: None,
            candidates,
            source_evidence: Vec::new(),
            token_usage: FactExtractionTokenUsage::default(),
            created_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn with_session_id(mut self, session_id: Option<String>) -> Self {
        self.session_id = session_id;
        self
    }

    #[must_use]
    pub fn with_project_id(mut self, project_id: Option<String>) -> Self {
        self.project_id = project_id;
        self
    }

    #[must_use]
    pub fn with_task_id(mut self, task_id: Option<String>) -> Self {
        self.task_id = task_id;
        self
    }

    #[must_use]
    pub fn with_team_id(mut self, team_id: Option<String>) -> Self {
        self.team_id = team_id;
        self
    }

    #[must_use]
    pub fn with_source_evidence(mut self, source_evidence: Vec<FactEvidenceId>) -> Self {
        self.source_evidence = source_evidence;
        self
    }

    #[must_use]
    pub fn with_token_usage(mut self, token_usage: FactExtractionTokenUsage) -> Self {
        self.token_usage = token_usage;
        self
    }
}
