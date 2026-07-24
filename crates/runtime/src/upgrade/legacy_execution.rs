use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
};

use crate::execution_core::graph::ExecutionGraphEvent;
use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
    RuntimeEventStore, RuntimeEventStoreError, RuntimeTransactionEventInput,
};

use super::{
    UpgradeCarrierRecord, UpgradeCleanShutdownReceipt, UpgradeDispositionReceipt, UpgradeInventory,
};

pub const LEGACY_EXECUTION_IMPORTED: &str = "legacy_execution_imported";
pub const UPGRADE_RECOVERY_REQUIRED: &str = "upgrade_recovery_required";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyExecutionImportReceipt {
    pub transaction_id: String,
    pub duplicate: bool,
    pub active_carrier_count: usize,
    pub imported_count: usize,
    pub disposition_count: usize,
    pub pre_hash: String,
    pub post_hash: String,
}

#[derive(Debug, thiserror::Error)]
pub enum LegacyExecutionImportError {
    #[error("legacy clean shutdown manifest is missing: {0}")]
    MissingManifest(PathBuf),
    #[error("legacy clean shutdown manifest I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("legacy clean shutdown manifest is invalid: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("legacy clean shutdown manifest validation failed: {0}")]
    Validation(String),
    #[error(transparent)]
    EventStore(#[from] RuntimeEventStoreError),
}

pub struct LegacyExecutionImporter {
    event_store: Arc<RuntimeEventStore>,
    workspace_id: String,
    workspace_root: PathBuf,
    source_binary_version: String,
}

impl LegacyExecutionImporter {
    pub fn new(
        event_store: Arc<RuntimeEventStore>,
        workspace_id: impl Into<String>,
        workspace_root: impl Into<PathBuf>,
        source_binary_version: impl Into<String>,
    ) -> Self {
        Self {
            event_store,
            workspace_id: workspace_id.into(),
            workspace_root: workspace_root.into(),
            source_binary_version: source_binary_version.into(),
        }
    }

    pub fn import_manifest(
        &self,
        manifest_path: impl AsRef<Path>,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        let manifest_path = manifest_path.as_ref();
        let result = self.import_manifest_inner(manifest_path);
        if let Err(error) = &result {
            self.mark_recovery_required(manifest_path, &error.to_string());
        }
        result
    }

    pub fn import_clean_shutdown(
        &self,
        receipt: &UpgradeCleanShutdownReceipt,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        let result = self.import_clean_shutdown_inner(receipt);
        if let Err(error) = &result {
            self.mark_recovery_required(&receipt.manifest_path, &error.to_string());
        }
        result
    }

    pub fn import_receipt_file(
        &self,
        receipt_path: impl AsRef<Path>,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        let receipt_path = receipt_path.as_ref();
        let result = (|| {
            if !receipt_path.is_file() {
                return Err(LegacyExecutionImportError::MissingManifest(
                    receipt_path.to_path_buf(),
                ));
            }
            let receipt: UpgradeCleanShutdownReceipt =
                serde_json::from_slice(&fs::read(receipt_path)?)?;
            self.import_clean_shutdown_inner(&receipt)
        })();
        if let Err(error) = &result {
            self.mark_recovery_required(receipt_path, &error.to_string());
        }
        result
    }

    pub fn mutation_allowed(&self) -> Result<bool, LegacyExecutionImportError> {
        let events = self
            .event_store
            .list_stream(&upgrade_stream_id(&self.workspace_id))
            .map_err(|error| LegacyExecutionImportError::Validation(error.to_string()))?;
        Ok(events
            .last()
            .is_none_or(|event| event.kind != UPGRADE_RECOVERY_REQUIRED))
    }

    fn import_manifest_inner(
        &self,
        manifest_path: &Path,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        if !manifest_path.is_file() {
            return Err(LegacyExecutionImportError::MissingManifest(
                manifest_path.to_path_buf(),
            ));
        }
        let inventory: UpgradeInventory = serde_json::from_slice(&fs::read(manifest_path)?)?;
        validate_inventory(
            &inventory,
            &self.source_binary_version,
            &self.workspace_id,
            &self.workspace_root,
        )?;

        if let Some(marker) = self
            .event_store
            .list_stream(&upgrade_stream_id(&self.workspace_id))
            .map_err(|error| LegacyExecutionImportError::Validation(error.to_string()))?
            .into_iter()
            .find(|event| {
                event.kind == LEGACY_EXECUTION_IMPORTED
                    && event
                        .payload
                        .get("manifest_hash")
                        .and_then(serde_json::Value::as_str)
                        == Some(inventory.content_hash.as_str())
            })
        {
            return Ok(LegacyExecutionImportReceipt {
                transaction_id: marker.transaction_id,
                duplicate: true,
                active_carrier_count: inventory.active_count,
                imported_count: marker.payload["imported_count"].as_u64().unwrap_or(0) as usize,
                disposition_count: marker.payload["disposition_count"].as_u64().unwrap_or(0)
                    as usize,
                pre_hash: marker.payload["pre_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                post_hash: marker.payload["post_hash"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
            });
        }

        let active = inventory
            .carriers
            .iter()
            .filter(|carrier| carrier.status.is_active())
            .collect::<Vec<_>>();
        let pre_hash = carrier_identity_hash(active.iter().copied())?;
        let stream_id = upgrade_stream_id(&self.workspace_id);
        let expected_revision = self.event_store.stream_revision(&stream_id)?;
        let transaction_id = format!("legacy-execution-import:{}", inventory.content_hash);
        let mut imported_count = 0;
        let mut disposition_count = 0;
        let mut handled = Vec::with_capacity(active.len());
        let mut imported_graph_ids = Vec::new();
        let mut events = Vec::with_capacity(active.len() + 1);
        let mut expected_streams = vec![ExpectedStreamRevision {
            stream_id: stream_id.clone(),
            expected_revision,
        }];

        for carrier in &active {
            let disposition = disposition_for(&inventory.dispositions, carrier);
            if let Some(disposition) = disposition {
                validate_disposition(disposition)?;
                disposition_count += 1;
                events.push(transaction_event(
                    &stream_id,
                    RuntimeEventScope::Recovery,
                    "legacy_execution_disposition_recorded",
                    format!(
                        "legacy-disposition:{}:{}",
                        carrier.carrier_kind, carrier.carrier_id
                    ),
                    serde_json::json!({
                        "carrier_identity": {
                            "carrier_kind": carrier.carrier_kind,
                            "carrier_id": carrier.carrier_id,
                            "revision": carrier.revision,
                            "state_hash": carrier.state_hash,
                        },
                        "disposition": disposition,
                        "manifest_hash": inventory.content_hash,
                    }),
                ));
            } else {
                imported_count += 1;
                let graph = materialize_graph(carrier, &inventory.content_hash);
                let graph_id = graph.id.clone();
                imported_graph_ids.push(graph_id.clone());
                expected_streams.push(ExpectedStreamRevision {
                    stream_id: graph_id.clone(),
                    expected_revision: 0,
                });
                events.push(transaction_event(
                    &graph_id,
                    RuntimeEventScope::ExecutionGraph,
                    "execution_graph.planned",
                    format!("legacy-graph:{graph_id}"),
                    serde_json::to_value(ExecutionGraphEvent::Planned { graph })?,
                ));
            }
            handled.push(*carrier);
        }

        let post_hash = carrier_identity_hash(handled)?;
        let handled_count = imported_count + disposition_count;
        if handled_count != inventory.active_count || pre_hash != post_hash {
            return Err(LegacyExecutionImportError::Validation(
                "legacy execution pre/post parity failed".to_string(),
            ));
        }
        events.push(transaction_event(
            &stream_id,
            RuntimeEventScope::Recovery,
            LEGACY_EXECUTION_IMPORTED,
            format!("legacy-marker:{}", inventory.content_hash),
            serde_json::json!({
                "manifest_hash": inventory.content_hash,
                "carrier_count": inventory.carrier_count,
                "active_carrier_count": inventory.active_count,
                "pre_count": inventory.active_count,
                "post_count": handled_count,
                "pre_hash": pre_hash,
                "post_hash": post_hash,
                "imported_count": imported_count,
                "disposition_count": disposition_count,
                "imported_graph_ids": imported_graph_ids,
            }),
        ));
        let receipt = self
            .event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: transaction_id.clone(),
                expected_streams,
                events,
            })?;
        Ok(LegacyExecutionImportReceipt {
            transaction_id,
            duplicate: receipt.duplicate,
            active_carrier_count: inventory.active_count,
            imported_count,
            disposition_count,
            pre_hash,
            post_hash,
        })
    }

    fn import_clean_shutdown_inner(
        &self,
        receipt: &UpgradeCleanShutdownReceipt,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        if receipt.schema_version != 1 {
            return Err(validation("unsupported clean shutdown receipt schema"));
        }
        if receipt.workspace_id != self.workspace_id {
            return Err(validation("clean shutdown workspace mismatch"));
        }
        if !receipt.manifest_path.is_file() {
            return Err(LegacyExecutionImportError::MissingManifest(
                receipt.manifest_path.clone(),
            ));
        }
        let inventory: UpgradeInventory =
            serde_json::from_slice(&fs::read(&receipt.manifest_path)?)?;
        if receipt.manifest_hash != inventory.content_hash {
            return Err(validation("clean shutdown manifest hash mismatch"));
        }
        if receipt.active_count != inventory.active_count {
            return Err(validation("clean shutdown active count mismatch"));
        }
        let source_importer = Self::new(
            Arc::clone(&self.event_store),
            &self.workspace_id,
            &self.workspace_root,
            &receipt.source_binary_version,
        );
        source_importer.import_manifest_inner(&receipt.manifest_path)
    }

    fn mark_recovery_required(&self, manifest_path: &Path, reason: &str) {
        let stream_id = upgrade_stream_id(&self.workspace_id);
        let Ok(expected_revision) = self.event_store.stream_revision(&stream_id) else {
            return;
        };
        let reason_hash = stable_hash(reason.as_bytes());
        let _ = self
            .event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("upgrade-recovery-required:{reason_hash}"),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: stream_id.clone(),
                    expected_revision,
                }],
                events: vec![transaction_event(
                    &stream_id,
                    RuntimeEventScope::Recovery,
                    UPGRADE_RECOVERY_REQUIRED,
                    format!("upgrade-recovery:{reason_hash}"),
                    serde_json::json!({
                        "workspace_id": self.workspace_id,
                        "manifest_path": manifest_path,
                        "reason": reason,
                    }),
                )],
            });
    }
}

fn materialize_graph(carrier: &UpgradeCarrierRecord, manifest_hash: &str) -> ExecutionGraph {
    let identity = format!(
        "{}:{}:{}:{}",
        carrier.carrier_kind, carrier.carrier_id, carrier.revision, carrier.state_hash
    );
    let identity_hash = stable_hash(identity.as_bytes());
    let mut graph = ExecutionGraph::new(format!(
        "Imported legacy {} {}",
        carrier.carrier_kind, carrier.carrier_id
    ));
    graph.id = format!("legacy-execution-graph-{identity_hash}");
    graph.revision = 1;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        "legacy_execution",
        carrier
            .state_ref
            .clone()
            .unwrap_or_else(|| format!("legacy-state:{manifest_hash}:{}", carrier.carrier_id)),
    );
    node.id = format!("legacy-execution-node-{identity_hash}");
    node.idempotency_key = format!("legacy-execution:{identity_hash}");
    graph.node_statuses.insert(
        node.id.clone(),
        match carrier.status {
            super::UpgradeCarrierStatus::Ready => ExecutionNodeStatus::Ready,
            super::UpgradeCarrierStatus::Running => ExecutionNodeStatus::Running,
            super::UpgradeCarrierStatus::Waiting => ExecutionNodeStatus::WaitingExternal,
            super::UpgradeCarrierStatus::Paused => ExecutionNodeStatus::Paused,
            super::UpgradeCarrierStatus::Completed => ExecutionNodeStatus::Completed,
            super::UpgradeCarrierStatus::Failed => ExecutionNodeStatus::Failed,
            super::UpgradeCarrierStatus::Cancelled => ExecutionNodeStatus::Cancelled,
            super::UpgradeCarrierStatus::Blocked => ExecutionNodeStatus::Blocked,
        },
    );
    graph.nodes.push(node);
    graph
}

fn validate_disposition(
    disposition: &UpgradeDispositionReceipt,
) -> Result<(), LegacyExecutionImportError> {
    if disposition.actor.trim().is_empty() {
        return Err(validation("disposition actor is empty"));
    }
    if !matches!(disposition.action.as_str(), "cancel" | "drain") {
        return Err(validation("disposition action is unsupported"));
    }
    if disposition.reason.trim().is_empty()
        || disposition
            .result_refs
            .iter()
            .any(|reference| reference.trim().is_empty())
        || (disposition.action == "drain" && disposition.result_refs.is_empty())
    {
        return Err(validation("disposition reason/result refs are invalid"));
    }
    Ok(())
}

fn validate_inventory(
    inventory: &UpgradeInventory,
    source_binary_version: &str,
    workspace_id: &str,
    workspace_root: &Path,
) -> Result<(), LegacyExecutionImportError> {
    if inventory.schema_version != 1 {
        return Err(validation("unsupported manifest schema version"));
    }
    if inventory.source_binary_version != source_binary_version {
        return Err(validation("source binary version mismatch"));
    }
    if inventory.workspace_id != workspace_id {
        return Err(validation("workspace id mismatch"));
    }
    let manifest_root = fs::canonicalize(&inventory.workspace_root)
        .map_err(|_| validation("manifest workspace root cannot be canonicalized"))?;
    let expected_root = fs::canonicalize(workspace_root)
        .map_err(|_| validation("expected workspace root cannot be canonicalized"))?;
    if manifest_root != expected_root {
        return Err(validation("workspace root mismatch"));
    }
    if inventory.carrier_count != inventory.carriers.len() {
        return Err(validation("carrier count mismatch"));
    }
    let active_count = inventory
        .carriers
        .iter()
        .filter(|carrier| carrier.status.is_active())
        .count();
    if inventory.active_count != active_count {
        return Err(validation("active carrier count mismatch"));
    }
    if inventory.content_hash != inventory_content_hash(inventory)? {
        return Err(validation("manifest content hash mismatch"));
    }
    Ok(())
}

fn inventory_content_hash(
    inventory: &UpgradeInventory,
) -> Result<String, LegacyExecutionImportError> {
    let payload = serde_json::to_vec(&(
        inventory.schema_version,
        &inventory.source_binary_version,
        &inventory.workspace_id,
        &inventory.workspace_root,
        inventory.generated_at_ms,
        &inventory.carriers,
        &inventory.dispositions,
    ))?;
    Ok(stable_hash(&payload))
}

fn carrier_identity_hash<'a>(
    carriers: impl IntoIterator<Item = &'a UpgradeCarrierRecord>,
) -> Result<String, LegacyExecutionImportError> {
    let identities = carriers
        .into_iter()
        .map(|carrier| {
            (
                &carrier.carrier_kind,
                &carrier.carrier_id,
                carrier.revision,
                &carrier.state_hash,
            )
        })
        .collect::<Vec<_>>();
    Ok(stable_hash(&serde_json::to_vec(&identities)?))
}

fn disposition_for<'a>(
    dispositions: &'a [UpgradeDispositionReceipt],
    carrier: &UpgradeCarrierRecord,
) -> Option<&'a UpgradeDispositionReceipt> {
    dispositions.iter().find(|disposition| {
        disposition.carrier_kind == carrier.carrier_kind
            && disposition.carrier_id == carrier.carrier_id
    })
}

fn transaction_event(
    stream_id: &str,
    scope: RuntimeEventScope,
    kind: &str,
    idempotency_key: String,
    payload: serde_json::Value,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: stream_id.to_string(),
            scope,
            kind: kind.to_string(),
            status: Some("completed".to_string()),
            actor: Some("runtime_upgrade".to_string()),
            refs: Vec::new(),
            payload,
        },
        idempotency_key: Some(idempotency_key),
        schema_version: 1,
    }
}

fn upgrade_stream_id(workspace_id: &str) -> String {
    format!("upgrade:{workspace_id}")
}

fn stable_hash(bytes: &[u8]) -> String {
    format!(
        "{:016x}",
        model_protocol::fingerprint::stable_hash_bytes(bytes)
    )
}

fn validation(message: &str) -> LegacyExecutionImportError {
    LegacyExecutionImportError::Validation(message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::upgrade::UpgradeCarrierStatus;

    fn inventory(root: &Path) -> UpgradeInventory {
        let mut inventory = UpgradeInventory {
            schema_version: 1,
            source_binary_version: "0.9.472".to_string(),
            workspace_id: "workspace".to_string(),
            workspace_root: root.to_path_buf(),
            generated_at_ms: 42,
            carriers: vec![UpgradeCarrierRecord {
                carrier_kind: "active_turn".to_string(),
                carrier_id: "turn-1".to_string(),
                status: UpgradeCarrierStatus::Running,
                revision: 3,
                result_ref: None,
                state_ref: Some("legacy/turn-1.json".to_string()),
                state_hash: "state-hash".to_string(),
            }],
            dispositions: Vec::new(),
            carrier_count: 1,
            active_count: 1,
            content_hash: String::new(),
        };
        inventory.content_hash = inventory_content_hash(&inventory).unwrap();
        inventory
    }

    #[test]
    fn imports_active_carriers_and_marker_in_one_idempotent_transaction() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let inventory = inventory(temp.path());
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let importer =
            LegacyExecutionImporter::new(Arc::clone(&store), "workspace", temp.path(), "0.9.472");

        let first = importer.import_manifest(&manifest).unwrap();
        let second = importer.import_manifest(&manifest).unwrap();

        assert!(!first.duplicate);
        assert!(second.duplicate);
        assert_eq!(first.pre_hash, first.post_hash);
        assert_eq!(first.imported_count, 1);
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, LEGACY_EXECUTION_IMPORTED);
        let graph_id = materialize_graph(&inventory.carriers[0], &inventory.content_hash).id;
        let graph = crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(&store))
            .load(&graph_id)
            .unwrap();
        assert_eq!(graph.id, graph_id);
        assert_eq!(
            graph.node_statuses.values().next(),
            Some(&ExecutionNodeStatus::Running)
        );
        assert!(importer.mutation_allowed().unwrap());
    }

    #[test]
    fn valid_disposition_is_recorded_without_creating_import_graph() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let mut inventory = inventory(temp.path());
        inventory.dispositions.push(UpgradeDispositionReceipt {
            carrier_kind: "active_turn".to_string(),
            carrier_id: "turn-1".to_string(),
            action: "cancel".to_string(),
            actor: "operator".to_string(),
            reason: "upgrade".to_string(),
            result_refs: vec!["cancellation:turn-1".to_string()],
            created_at_ms: 43,
        });
        inventory.content_hash = inventory_content_hash(&inventory).unwrap();
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let importer =
            LegacyExecutionImporter::new(Arc::clone(&store), "workspace", temp.path(), "0.9.472");

        let receipt = importer.import_manifest(&manifest).unwrap();

        assert_eq!(receipt.imported_count, 0);
        assert_eq!(receipt.disposition_count, 1);
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events[0].kind, "legacy_execution_disposition_recorded");
        assert_eq!(events[1].kind, LEGACY_EXECUTION_IMPORTED);
    }

    #[test]
    fn invalid_manifest_marks_recovery_required_and_blocks_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let mut inventory = inventory(temp.path());
        inventory.active_count = 2;
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let importer =
            LegacyExecutionImporter::new(Arc::clone(&store), "workspace", temp.path(), "0.9.472");

        assert!(matches!(
            importer.import_manifest(&manifest),
            Err(LegacyExecutionImportError::Validation(_))
        ));
        assert!(!importer.mutation_allowed().unwrap());
        assert_eq!(
            store.list_stream("upgrade:workspace").unwrap()[0].kind,
            UPGRADE_RECOVERY_REQUIRED
        );
    }
}
