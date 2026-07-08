use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::versioning::EvolutionVersionRecord;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionRollbackReceipt {
    pub rollback_id: String,
    pub version_id: String,
    pub candidate_id: String,
    pub rollback_artifact: String,
    pub accepted: bool,
    pub reason: String,
    pub created_at_ms: u128,
}

#[derive(Debug, Clone, Default)]
pub struct EvolutionRollbackManager;

impl EvolutionRollbackManager {
    #[must_use]
    pub fn rollback(version: &EvolutionVersionRecord) -> EvolutionRollbackReceipt {
        EvolutionRollbackReceipt {
            rollback_id: format!("evo-rollback-{}", Uuid::new_v4()),
            version_id: version.version_id.clone(),
            candidate_id: version.candidate_id.clone(),
            rollback_artifact: version.rollback_artifact.clone(),
            accepted: !version.rollback_artifact.trim().is_empty(),
            reason: "rollback receipt generated; application remains approval controlled"
                .to_string(),
            created_at_ms: now_ms(),
        }
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
