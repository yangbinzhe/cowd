use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{
    candidate::EvolutionCandidate, promotion::EvolutionPromotionReceipt,
    rollback::EvolutionRollbackReceipt,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvolutionAppliedCapabilityRecord {
    pub record_id: String,
    pub version_id: String,
    pub candidate_id: String,
    pub kind: String,
    pub adapter: String,
    pub owner: String,
    pub status: String,
    pub enabled_scope: Vec<String>,
    pub goal_ids: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub policy_effects: Vec<String>,
    pub created_at_ms: u128,
    pub updated_at_ms: u128,
    pub disabled_by_rollback_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EvolutionAppliedCapabilityRegistry {
    path: PathBuf,
}

impl EvolutionAppliedCapabilityRegistry {
    #[must_use]
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("applied-capabilities.json"),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn list(&self) -> Result<Vec<EvolutionAppliedCapabilityRecord>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let bytes = fs::read(&self.path).map_err(|error| error.to_string())?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok(Vec::new());
        }
        serde_json::from_slice(&bytes).map_err(|error| error.to_string())
    }

    pub fn active(&self) -> Result<Vec<EvolutionAppliedCapabilityRecord>, String> {
        Ok(self
            .list()?
            .into_iter()
            .filter(|record| record.status == "active")
            .collect())
    }

    pub fn apply_promotion(
        &self,
        candidate: &EvolutionCandidate,
        receipt: &EvolutionPromotionReceipt,
    ) -> Result<Option<EvolutionAppliedCapabilityRecord>, String> {
        if !receipt.accepted {
            return Ok(None);
        }
        let Some(version) = receipt.version_record.as_ref() else {
            return Ok(None);
        };

        let now = now_ms();
        let mut records = self.list()?;
        let artifact_refs = artifact_refs(candidate);
        let policy_effects = policy_effects(candidate);
        let record = if let Some(existing) = records
            .iter_mut()
            .find(|record| record.version_id == version.version_id)
        {
            existing.status = "active".to_string();
            existing.updated_at_ms = now;
            existing.disabled_by_rollback_id = None;
            existing.policy_effects = policy_effects;
            existing.artifact_refs = artifact_refs;
            existing.clone()
        } else {
            let record = EvolutionAppliedCapabilityRecord {
                record_id: format!("evo-applied-{}", Uuid::new_v4()),
                version_id: version.version_id.clone(),
                candidate_id: candidate.candidate_id.clone(),
                kind: candidate.kind.as_str().to_string(),
                adapter: candidate.promotion_adapter.clone(),
                owner: if candidate.owner.trim().is_empty() {
                    candidate.target_owner.clone()
                } else {
                    candidate.owner.clone()
                },
                status: "active".to_string(),
                enabled_scope: candidate.scope.clone(),
                goal_ids: candidate.goal_ids.clone(),
                artifact_refs,
                policy_effects,
                created_at_ms: now,
                updated_at_ms: now,
                disabled_by_rollback_id: None,
            };
            records.push(record.clone());
            record
        };

        self.write(&records)?;
        Ok(Some(record))
    }

    pub fn rollback_version(
        &self,
        receipt: &EvolutionRollbackReceipt,
    ) -> Result<Option<EvolutionAppliedCapabilityRecord>, String> {
        let mut records = self.list()?;
        let now = now_ms();
        let mut updated = None;
        for record in records
            .iter_mut()
            .filter(|record| record.version_id == receipt.version_id)
        {
            record.status = "rolled_back".to_string();
            record.updated_at_ms = now;
            record.disabled_by_rollback_id = Some(receipt.rollback_id.clone());
            updated = Some(record.clone());
        }
        self.write(&records)?;
        Ok(updated)
    }

    fn write(&self, records: &[EvolutionAppliedCapabilityRecord]) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        fs::write(
            &self.path,
            serde_json::to_vec_pretty(records).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())
    }
}

fn artifact_refs(candidate: &EvolutionCandidate) -> Vec<String> {
    let mut refs = candidate
        .generated_artifacts
        .iter()
        .map(|artifact| artifact.path.clone())
        .chain(candidate.artifact_path.clone())
        .chain(candidate.comparison_report_ref.clone())
        .filter(|value| !value.trim().is_empty())
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    refs
}

fn policy_effects(candidate: &EvolutionCandidate) -> Vec<String> {
    let mut effects = vec![
        format!("adapter={}", candidate.promotion_adapter),
        format!("candidate_kind={}", candidate.kind.as_str()),
        "eligible_for_context_activation".to_string(),
        "model_visible_via_runtime_capabilities".to_string(),
    ];
    effects.extend(candidate.goal_ids.iter().map(|goal| format!("goal={goal}")));
    effects.extend(
        candidate
            .adoption_gate
            .iter()
            .map(|gate| format!("gate={gate}")),
    );
    effects.sort();
    effects.dedup();
    effects
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EvolutionCandidateGenerator, EvolutionProposal, EvolutionSignal};

    fn candidate() -> EvolutionCandidate {
        let signal = EvolutionSignal::memory_noise("runtime", "session-1", vec!["mem".into()]);
        let proposal = EvolutionProposal::from_signals(&[signal]);
        let mut candidate =
            EvolutionCandidateGenerator::generate(&proposal, "baseline", "candidate");
        candidate.comparison_report_ref = Some("comparison:ok".to_string());
        candidate
    }

    #[test]
    fn promotion_installs_and_rollback_disables_active_capability() {
        let root =
            std::env::temp_dir().join(format!("cowd-applied-registry-{}", uuid::Uuid::new_v4()));
        let registry = EvolutionAppliedCapabilityRegistry::new(&root);
        let candidate = candidate();
        let receipt = crate::EvolutionPromotionManager::promote(&candidate);

        let applied = registry
            .apply_promotion(&candidate, &receipt)
            .expect("promotion applies")
            .expect("accepted receipt creates record");
        assert_eq!(applied.status, "active");
        assert_eq!(registry.active().expect("active records").len(), 1);

        let version = receipt.version_record.expect("version record");
        let rollback = crate::EvolutionRollbackManager::rollback(&version);
        let disabled = registry
            .rollback_version(&rollback)
            .expect("rollback updates registry")
            .expect("matching record");
        assert_eq!(disabled.status, "rolled_back");
        assert_eq!(disabled.disabled_by_rollback_id, Some(rollback.rollback_id));
        assert!(registry.active().expect("active records").is_empty());

        let _ = fs::remove_dir_all(root);
    }
}
