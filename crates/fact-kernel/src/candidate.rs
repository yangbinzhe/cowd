use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::core::{Confidence, EvidenceId, FactSource};
use crate::hypothesis::FactReality;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FactCandidateId(String);

impl FactCandidateId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("fact-candidate-{}", Uuid::new_v4()))
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

impl Default for FactCandidateId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum FactScope {
    Global,
    Project(String),
    Session(String),
    Task(String),
    Team(String),
    Agent(String),
}

impl FactScope {
    #[must_use]
    pub fn key(&self) -> String {
        match self {
            Self::Global => "global".to_string(),
            Self::Project(value) => format!("project:{value}"),
            Self::Session(value) => format!("session:{value}"),
            Self::Task(value) => format!("task:{value}"),
            Self::Team(value) => format!("team:{value}"),
            Self::Agent(value) => format!("agent:{value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactStatus {
    Candidate,
    Active,
    Held,
    Rejected,
    Superseded,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtractionMethod {
    Rule,
    Model,
    Imported,
    Checkpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FactCandidateRelationKind {
    Supersedes,
    ConflictsWith,
    DerivedFrom,
    Duplicates,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactCandidateRelation {
    pub kind: FactCandidateRelationKind,
    pub target: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactCandidate {
    pub candidate_id: FactCandidateId,
    pub fact_type: String,
    pub statement: String,
    pub structured_payload: Option<Value>,
    pub scope: FactScope,
    pub reality: FactReality,
    pub source: FactSource,
    pub evidence: Vec<EvidenceId>,
    pub confidence: Confidence,
    pub extraction_method: ExtractionMethod,
    pub extractor_version: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub tags: Vec<String>,
    pub status: FactStatus,
    pub relations: Vec<FactCandidateRelation>,
}

impl FactCandidate {
    #[must_use]
    pub fn observed(
        fact_type: impl Into<String>,
        statement: impl Into<String>,
        scope: FactScope,
        source: FactSource,
    ) -> Self {
        Self {
            candidate_id: FactCandidateId::new(),
            fact_type: fact_type.into(),
            statement: statement.into(),
            structured_payload: None,
            scope,
            reality: FactReality::Observed,
            source,
            evidence: Vec::new(),
            confidence: Confidence::default(),
            extraction_method: ExtractionMethod::Rule,
            extractor_version: "fact-kernel:rule:v1".to_string(),
            expires_at: None,
            tags: Vec::new(),
            status: FactStatus::Candidate,
            relations: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_evidence(mut self, evidence: Vec<EvidenceId>) -> Self {
        self.evidence = evidence;
        self
    }

    #[must_use]
    pub fn with_confidence(mut self, confidence: Confidence) -> Self {
        self.confidence = confidence;
        self
    }

    #[must_use]
    pub fn with_method(mut self, method: ExtractionMethod, version: impl Into<String>) -> Self {
        self.extraction_method = method;
        self.extractor_version = version.into();
        self
    }

    #[must_use]
    pub fn with_reality(mut self, reality: FactReality) -> Self {
        self.reality = reality;
        self
    }

    #[must_use]
    pub fn with_payload(mut self, payload: Value) -> Self {
        self.structured_payload = Some(payload);
        self
    }

    #[must_use]
    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }
}
