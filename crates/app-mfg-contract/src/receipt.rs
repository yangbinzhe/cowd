use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{mutation::MfgActionId, version::MfgContractVersion};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgReceiptStatus {
    Preview,
    Accepted,
    Replayed,
    Completed,
    Conflict,
    Rejected,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgReceiptV1 {
    pub receipt_id: String,
    pub idempotency_key: String,
    pub actor_principal: String,
    pub action_id: MfgActionId,
    pub resource_ref: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
    #[serde(default)]
    pub result_revision: Option<u64>,
    pub payload_digest: String,
    pub status: MfgReceiptStatus,
    #[serde(default)]
    pub response: serde_json::Value,
    pub contract_version: MfgContractVersion,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
