use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::version::MfgContractVersion;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct MfgLiveSnapshotStateV1 {
    #[serde(default)]
    pub cockpit: serde_json::Value,
    #[serde(default)]
    pub alerts: serde_json::Value,
    #[serde(default)]
    pub assignments: serde_json::Value,
    #[serde(default)]
    pub incidents: serde_json::Value,
    #[serde(default)]
    pub executions: serde_json::Value,
    #[serde(default)]
    pub reports: serde_json::Value,
    #[serde(default)]
    pub reviews: serde_json::Value,
    #[serde(default)]
    pub receipts: serde_json::Value,
    #[serde(default)]
    pub data_compute: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveEventV1 {
    pub event_type: String,
    pub subject_ref: String,
    pub revision: u64,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveSnapshotV1 {
    pub view_epoch: String,
    pub cursor: String,
    pub generated_at: DateTime<Utc>,
    pub contract_version: MfgContractVersion,
    pub state: MfgLiveSnapshotStateV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveDeltaV1 {
    pub view_epoch: String,
    pub base_cursor: String,
    pub target_cursor: String,
    #[serde(default)]
    pub events: Vec<MfgLiveEventV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveResyncV1 {
    pub previous_view_epoch: String,
    pub reason: String,
    pub snapshot_url: String,
    pub latest_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveHeartbeatV1 {
    pub view_epoch: String,
    pub cursor: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MfgLiveEnvelopeV1 {
    Snapshot(MfgLiveSnapshotV1),
    Delta(MfgLiveDeltaV1),
    Resync(MfgLiveResyncV1),
    Heartbeat(MfgLiveHeartbeatV1),
}
