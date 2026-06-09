use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IaccSourceKind {
    Api,
    Db,
    File,
    Rpa,
    Manual,
    Connector,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccSourceSnapshot {
    pub snapshot_id: String,
    pub source_system: String,
    pub source_kind: IaccSourceKind,
    #[serde(default)]
    pub resource_ref: Option<String>,
    #[serde(default)]
    pub business_period: Option<String>,
    pub captured_at: DateTime<Utc>,
    pub schema_version: String,
    pub row_count: u64,
    #[serde(default)]
    pub checksum: Option<String>,
    pub confidence: f32,
    #[serde(default)]
    pub metadata: Value,
}

impl IaccSourceSnapshot {
    #[must_use]
    pub fn new(
        source_system: impl Into<String>,
        source_kind: IaccSourceKind,
        schema_version: impl Into<String>,
    ) -> Self {
        Self {
            snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
            source_system: source_system.into(),
            source_kind,
            resource_ref: None,
            business_period: None,
            captured_at: Utc::now(),
            schema_version: schema_version.into(),
            row_count: 0,
            checksum: None,
            confidence: 1.0,
            metadata: Value::Null,
        }
    }
}
