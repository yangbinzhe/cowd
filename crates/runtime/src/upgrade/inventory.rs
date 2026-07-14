use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpgradeCarrierStatus {
    Ready,
    Running,
    Waiting,
    Paused,
    Completed,
    Failed,
    Cancelled,
    Blocked,
}

impl UpgradeCarrierStatus {
    #[must_use]
    pub fn is_active(self) -> bool {
        matches!(
            self,
            Self::Ready | Self::Running | Self::Waiting | Self::Paused
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeCarrierRecord {
    pub carrier_kind: String,
    pub carrier_id: String,
    pub status: UpgradeCarrierStatus,
    pub revision: u64,
    pub result_ref: Option<String>,
    pub state_ref: Option<String>,
    pub state_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeDispositionReceipt {
    pub carrier_kind: String,
    pub carrier_id: String,
    pub action: String,
    pub actor: String,
    pub reason: String,
    pub result_refs: Vec<String>,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeInventory {
    pub schema_version: u32,
    pub source_binary_version: String,
    pub workspace_id: String,
    pub workspace_root: PathBuf,
    pub generated_at_ms: u64,
    pub carriers: Vec<UpgradeCarrierRecord>,
    pub dispositions: Vec<UpgradeDispositionReceipt>,
    pub carrier_count: usize,
    pub active_count: usize,
    pub content_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeCleanShutdownReceipt {
    pub schema_version: u32,
    pub source_binary_version: String,
    pub workspace_id: String,
    pub manifest_path: PathBuf,
    pub manifest_hash: String,
    pub active_count: usize,
    pub clean_shutdown_at_ms: u64,
}
