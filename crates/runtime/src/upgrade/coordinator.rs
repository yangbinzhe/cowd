use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::inventory::{
    UpgradeCarrierRecord, UpgradeCleanShutdownReceipt, UpgradeDispositionReceipt, UpgradeInventory,
};

pub trait UpgradeInventoryCollector: Send + Sync {
    fn name(&self) -> &str;
    fn collect(&self) -> Result<Vec<UpgradeCarrierRecord>, UpgradeError>;
}

#[derive(Clone)]
pub struct ClosureUpgradeInventoryCollector {
    name: String,
    collect: Arc<dyn Fn() -> Result<Vec<UpgradeCarrierRecord>, UpgradeError> + Send + Sync>,
}

impl ClosureUpgradeInventoryCollector {
    pub fn new(
        name: impl Into<String>,
        collect: impl Fn() -> Result<Vec<UpgradeCarrierRecord>, UpgradeError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            collect: Arc::new(collect),
        }
    }
}

impl UpgradeInventoryCollector for ClosureUpgradeInventoryCollector {
    fn name(&self) -> &str {
        &self.name
    }

    fn collect(&self) -> Result<Vec<UpgradeCarrierRecord>, UpgradeError> {
        (self.collect)()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpgradeError {
    #[error("runtime is not in maintenance mode")]
    MaintenanceRequired,
    #[error("runtime is already in maintenance mode")]
    AlreadyInMaintenance,
    #[error("active carrier `{carrier_kind}:{carrier_id}` has no disposition")]
    ActiveCarrierUndisposed {
        carrier_kind: String,
        carrier_id: String,
    },
    #[error("inventory collector `{collector}` failed: {message}")]
    Collector { collector: String, message: String },
    #[error("upgrade disposition is invalid: {0}")]
    InvalidDisposition(String),
    #[error("upgrade manifest IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("upgrade manifest serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpgradeMaintenanceSnapshot {
    pub maintenance: bool,
    pub entered_by: Option<String>,
    pub entered_at_ms: Option<u64>,
    pub collector_names: Vec<String>,
    pub dispositions: Vec<UpgradeDispositionReceipt>,
}

#[derive(Default)]
struct UpgradeCoordinatorState {
    maintenance: bool,
    entered_by: Option<String>,
    entered_at_ms: Option<u64>,
    collectors: Vec<Arc<dyn UpgradeInventoryCollector>>,
    dispositions: Vec<UpgradeDispositionReceipt>,
}

#[derive(Clone, Default)]
pub struct UpgradeCoordinator {
    state: Arc<RwLock<UpgradeCoordinatorState>>,
}

impl UpgradeCoordinator {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_collector(&self, collector: Arc<dyn UpgradeInventoryCollector>) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        state
            .collectors
            .retain(|item| item.name() != collector.name());
        state.collectors.push(collector);
        state
            .collectors
            .sort_by(|left, right| left.name().cmp(right.name()));
    }

    pub fn enter_maintenance(&self, actor: impl Into<String>) -> Result<(), UpgradeError> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if state.maintenance {
            return Err(UpgradeError::AlreadyInMaintenance);
        }
        state.maintenance = true;
        state.entered_by = Some(actor.into());
        state.entered_at_ms = Some(now_ms());
        Ok(())
    }

    #[must_use]
    pub fn accepts_new_work(&self) -> bool {
        !self
            .state
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .maintenance
    }

    pub fn record_disposition(
        &self,
        receipt: UpgradeDispositionReceipt,
    ) -> Result<(), UpgradeError> {
        if receipt.actor.trim().is_empty()
            || receipt.reason.trim().is_empty()
            || !matches!(receipt.action.as_str(), "cancel" | "drain")
            || receipt
                .result_refs
                .iter()
                .any(|reference| reference.trim().is_empty())
            || (receipt.action == "drain" && receipt.result_refs.is_empty())
        {
            return Err(UpgradeError::InvalidDisposition(
                "actor/reason/action/result_refs failed validation".to_string(),
            ));
        }
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if !state.maintenance {
            return Err(UpgradeError::MaintenanceRequired);
        }
        state.dispositions.retain(|existing| {
            existing.carrier_kind != receipt.carrier_kind
                || existing.carrier_id != receipt.carrier_id
        });
        state.dispositions.push(receipt);
        Ok(())
    }

    #[must_use]
    pub fn snapshot(&self) -> UpgradeMaintenanceSnapshot {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        UpgradeMaintenanceSnapshot {
            maintenance: state.maintenance,
            entered_by: state.entered_by.clone(),
            entered_at_ms: state.entered_at_ms,
            collector_names: state
                .collectors
                .iter()
                .map(|collector| collector.name().to_string())
                .collect(),
            dispositions: state.dispositions.clone(),
        }
    }

    pub fn collect_inventory(
        &self,
        source_binary_version: impl Into<String>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
    ) -> Result<UpgradeInventory, UpgradeError> {
        let state = self.state.read().unwrap_or_else(|error| error.into_inner());
        if !state.maintenance {
            return Err(UpgradeError::MaintenanceRequired);
        }
        let mut carriers = Vec::new();
        for collector in &state.collectors {
            carriers.extend(
                collector
                    .collect()
                    .map_err(|error| UpgradeError::Collector {
                        collector: collector.name().to_string(),
                        message: error.to_string(),
                    })?,
            );
        }
        carriers.sort_by(|left, right| {
            (&left.carrier_kind, &left.carrier_id).cmp(&(&right.carrier_kind, &right.carrier_id))
        });
        let dispositions = state.dispositions.clone();
        let source_binary_version = source_binary_version.into();
        let workspace_id = workspace_id.into();
        let workspace_root = workspace_root.into();
        let generated_at_ms = now_ms();
        let active_count = carriers
            .iter()
            .filter(|carrier| carrier.status.is_active())
            .count();
        let hash_payload = serde_json::to_vec(&(
            1u32,
            &source_binary_version,
            &workspace_id,
            &workspace_root,
            generated_at_ms,
            &carriers,
            &dispositions,
        ))?;
        let content_hash = format!(
            "{:016x}",
            model_protocol::prompt_cache::stable_hash_bytes(&hash_payload)
        );
        Ok(UpgradeInventory {
            schema_version: 1,
            source_binary_version,
            workspace_id,
            workspace_root,
            generated_at_ms,
            carrier_count: carriers.len(),
            active_count,
            carriers,
            dispositions,
            content_hash,
        })
    }

    pub fn export_clean_shutdown_manifest(
        &self,
        inventory: &UpgradeInventory,
        path: impl AsRef<Path>,
    ) -> Result<UpgradeCleanShutdownReceipt, UpgradeError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(inventory)?;
        let temporary = path.with_extension("json.tmp");
        fs::write(&temporary, &bytes)?;
        fs::rename(&temporary, path)?;
        Ok(UpgradeCleanShutdownReceipt {
            schema_version: 1,
            source_binary_version: inventory.source_binary_version.clone(),
            workspace_id: inventory.workspace_id.clone(),
            manifest_path: path.to_path_buf(),
            manifest_hash: inventory.content_hash.clone(),
            active_count: inventory.active_count,
            clean_shutdown_at_ms: now_ms(),
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{ClosureUpgradeInventoryCollector, UpgradeCoordinator};
    use crate::upgrade::inventory::{
        UpgradeCarrierRecord, UpgradeCarrierStatus, UpgradeDispositionReceipt,
    };

    #[test]
    fn active_carrier_can_be_exported_for_canonical_import() {
        let coordinator = UpgradeCoordinator::new();
        coordinator.register_collector(Arc::new(ClosureUpgradeInventoryCollector::new(
            "turns",
            || {
                Ok(vec![UpgradeCarrierRecord {
                    carrier_kind: "active_turn".to_string(),
                    carrier_id: "turn-1".to_string(),
                    status: UpgradeCarrierStatus::Running,
                    revision: 3,
                    result_ref: None,
                    state_ref: None,
                    state_hash: "abc".to_string(),
                }])
            },
        )));
        coordinator.enter_maintenance("operator").unwrap();
        let importable = coordinator
            .collect_inventory("0.9.472", "workspace", "/tmp/workspace")
            .unwrap();
        assert_eq!(importable.active_count, 1);
        coordinator
            .record_disposition(UpgradeDispositionReceipt {
                carrier_kind: "active_turn".to_string(),
                carrier_id: "turn-1".to_string(),
                action: "cancel".to_string(),
                actor: "operator".to_string(),
                reason: "upgrade".to_string(),
                result_refs: vec![],
                created_at_ms: 1,
            })
            .unwrap();
        let inventory = coordinator
            .collect_inventory("0.9.472", "workspace", "/tmp/workspace")
            .unwrap();
        assert_eq!(inventory.active_count, 1);
        assert!(!inventory.content_hash.is_empty());
    }
}
