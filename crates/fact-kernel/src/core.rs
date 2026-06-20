use chrono::{DateTime, Utc};
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
pub struct EvidenceId(String);

impl EvidenceId {
    #[must_use]
    pub fn new() -> Self {
        Self(format!("evidence-{}", Uuid::new_v4()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for EvidenceId {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Confidence(u16);

impl Confidence {
    #[must_use]
    pub fn from_basis_points(value: u16) -> Self {
        Self(value.min(10_000))
    }

    #[must_use]
    pub fn basis_points(self) -> u16 {
        self.0
    }
}

impl Default for Confidence {
    fn default() -> Self {
        Self(5_000)
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
    pub id: EvidenceId,
    pub source: FactSource,
    pub payload: Value,
    pub confidence: Confidence,
    pub collected_at: DateTime<Utc>,
}

impl EvidencePacket {
    #[must_use]
    pub fn new(source: FactSource, payload: Value) -> Self {
        Self {
            id: EvidenceId::new(),
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
    pub confidence: Confidence,
    pub provenance: Vec<Provenance>,
    pub evidence: Vec<EvidenceId>,
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
            confidence: Confidence::default(),
            provenance: Vec::new(),
            evidence: Vec::new(),
            created_at: now,
            updated_at: now,
        }
    }
}
