use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MFG_CONTRACT_VERSION: &str = "mfg.frontend.v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct MfgContractVersion(pub String);

impl Default for MfgContractVersion {
    fn default() -> Self {
        Self(MFG_CONTRACT_VERSION.to_string())
    }
}
