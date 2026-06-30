use harness_contract::reality::RecallSourceKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecallSourceStatus {
    EnabledAndWired,
    ConfiguredButUnwired,
    Degraded,
    Disabled,
}

impl RecallSourceStatus {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::EnabledAndWired => "enabled_and_wired",
            Self::ConfiguredButUnwired => "configured_but_unwired",
            Self::Degraded => "degraded",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecallSource {
    pub kind: RecallSourceKind,
    pub status: RecallSourceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
