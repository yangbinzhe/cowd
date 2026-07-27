use chrono::{DateTime, Utc};
use harness_contract::reality::{
    ClaimSupportState, EvidenceCompleteness, EvidenceRef, RealityBoundary,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FactId(String);

impl FactId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("fact-{}", Uuid::new_v4()))
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

impl Default for FactId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FactEvidenceId(String);

impl FactEvidenceId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("evidence-{}", Uuid::new_v4()))
    }

    #[must_use]
    pub fn from_string(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn as_evidence_ref(&self, boundary: RealityBoundary) -> EvidenceRef {
        EvidenceRef::unknown("fact_evidence", self.0.clone())
            .with_boundary(boundary)
            .with_source("fact_kernel")
    }
}

impl Default for FactEvidenceId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    User,
    Runtime,
    Tool,
    Provider,
    Connector,
    Channel,
    Memory,
    Matrix,
    Audit,
    Growth,
    Simulation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactSource {
    pub kind: SourceKind,
    pub id: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct Confidence(Option<u16>);

impl Confidence {
    #[must_use]
    pub fn from_basis_points(value: u16) -> Self {
        Self(Some(value.min(10_000)))
    }

    #[must_use]
    pub const fn unknown() -> Self {
        Self(None)
    }

    #[must_use]
    pub const fn basis_points(self) -> Option<u16> {
        self.0
    }

    #[must_use]
    pub const fn is_unknown(self) -> bool {
        self.0.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Provenance {
    pub source: FactSource,
    pub observed_at: DateTime<Utc>,
    pub trace_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidencePacket {
    pub id: FactEvidenceId,
    pub source: FactSource,
    pub payload: Value,
    pub confidence: Confidence,
    pub collected_at: DateTime<Utc>,
}

impl EvidencePacket {
    #[must_use]
    pub fn new(source: FactSource, payload: Value) -> Self {
        Self {
            id: FactEvidenceId::new(),
            source,
            payload,
            confidence: Confidence::default(),
            collected_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FactRecord {
    pub id: FactId,
    pub fact_type: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_key: Option<String>,
    #[serde(default = "default_fact_status")]
    pub status: String,
    #[serde(default = "unknown_boundary")]
    pub boundary: RealityBoundary,
    #[serde(default)]
    pub support: ClaimSupportState,
    #[serde(default)]
    pub evidence_completeness: EvidenceCompleteness,
    #[serde(default)]
    pub relations: Vec<String>,
    pub confidence: Confidence,
    pub provenance: Vec<Provenance>,
    pub evidence: Vec<FactEvidenceId>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl FactRecord {
    #[must_use]
    pub fn new(fact_type: impl Into<String>, statement: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: FactId::new(),
            fact_type: fact_type.into(),
            statement: statement.into(),
            scope_key: None,
            status: "active".to_string(),
            boundary: RealityBoundary::Unknown,
            support: ClaimSupportState::Unknown,
            evidence_completeness: EvidenceCompleteness::None,
            relations: Vec::new(),
            confidence: Confidence::default(),
            provenance: Vec::new(),
            evidence: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}

const fn unknown_boundary() -> RealityBoundary {
    RealityBoundary::Unknown
}

fn default_fact_status() -> String {
    "active".to_string()
}
