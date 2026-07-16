use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{capability::MfgEntitlementProjectionV2, version::MfgContractVersion};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgContractFreshnessV1 {
    pub generated_at: DateTime<Utc>,
    pub stale_after_ms: u64,
    pub is_stale: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgSurfaceStatusV1 {
    pub contract_version: MfgContractVersion,
    pub freshness: MfgContractFreshnessV1,
    pub entitlement: MfgEntitlementProjectionV2,
    #[serde(default)]
    pub degraded_domains: Vec<String>,
}
