use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

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
    #[error("legacy import failed: {import_error}; recovery marker persistence failed: {source}")]
    RecoveryMarkerPersistence {
        import_error: Box<LegacyExecutionImportError>,
        #[source]
        source: RuntimeEventStoreError,
    },
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
        self.import_manifest_inner(manifest_path)
            .map_err(|error| self.record_recovery_error(manifest_path, error))
    }

    pub fn import_clean_shutdown(
        &self,
        receipt: &UpgradeCleanShutdownReceipt,
    ) -> Result<LegacyExecutionImportReceipt, LegacyExecutionImportError> {
        self.import_clean_shutdown_inner(receipt)
            .map_err(|error| self.record_recovery_error(&receipt.manifest_path, error))
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
        result.map_err(|error| self.record_recovery_error(receipt_path, error))
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

        let active = inventory
            .carriers
            .iter()
            .filter(|carrier| carrier.status.is_active())
            .collect::<Vec<_>>();
        // A legacy selector and snapshot hash do not authenticate a live
        // Session, Agent attempt, or physical owner. Only an explicit
        // disposition can settle these carriers; never manufacture a Graph.
        // Validate before replay so an old import marker cannot revive that
        // retired capability.
        let dispositions = active
            .iter()
            .map(|carrier| {
                let disposition = disposition_for(&inventory.dispositions, carrier)?;
                validate_disposition(disposition)?;
                Ok(disposition)
            })
            .collect::<Result<Vec<_>, LegacyExecutionImportError>>()?;

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

        let pre_hash = carrier_identity_hash(active.iter().copied())?;
        let stream_id = upgrade_stream_id(&self.workspace_id);
        let expected_revision = self.event_store.stream_revision(&stream_id)?;
        let transaction_id = format!("legacy-execution-import:{}", inventory.content_hash);
        let imported_count = 0;
        let mut disposition_count = 0;
        let mut handled = Vec::with_capacity(active.len());
        let imported_graph_ids: Vec<String> = Vec::new();
        let mut events = Vec::with_capacity(active.len() + 1);
        let expected_streams = vec![ExpectedStreamRevision {
            stream_id: stream_id.clone(),
            expected_revision,
        }];

        for (carrier, disposition) in active.iter().zip(dispositions) {
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

    fn record_recovery_error(
        &self,
        manifest_path: &Path,
        error: LegacyExecutionImportError,
    ) -> LegacyExecutionImportError {
        match self.mark_recovery_required(manifest_path, &error.to_string()) {
            Ok(()) => error,
            Err(source) => LegacyExecutionImportError::RecoveryMarkerPersistence {
                import_error: Box::new(error),
                source,
            },
        }
    }

    fn mark_recovery_required(
        &self,
        manifest_path: &Path,
        reason: &str,
    ) -> Result<(), RuntimeEventStoreError> {
        let stream_id = upgrade_stream_id(&self.workspace_id);
        let expected_revision = self.event_store.stream_revision(&stream_id)?;
        let reason_hash = stable_hash(reason.as_bytes());
        self.event_store
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
            })?;
        Ok(())
    }
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
) -> Result<&'a UpgradeDispositionReceipt, LegacyExecutionImportError> {
    dispositions.iter().find(|disposition| {
        disposition.carrier_kind == carrier.carrier_kind
            && disposition.carrier_id == carrier.carrier_id
    }).ok_or_else(|| validation(&format!(
        "active legacy carrier {}:{} requires an explicit cancel or drain disposition; original execution identity is unavailable",
        carrier.carrier_kind, carrier.carrier_id,
    )))
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
    fn active_carriers_require_disposition_without_creating_execution_graphs() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let inventory = inventory(temp.path());
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
        let importer =
            LegacyExecutionImporter::new(Arc::clone(&store), "workspace", temp.path(), "0.9.472");

        let original = fs::read(&manifest).unwrap();
        for _ in 0..2 {
            let error = importer.import_manifest(&manifest).unwrap_err();
            assert!(error
                .to_string()
                .contains("explicit cancel or drain disposition"));
        }
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, UPGRADE_RECOVERY_REQUIRED);
        assert!(
            crate::execution_core::graph::ExecutionGraphStateStore::new(Arc::clone(&store))
                .graph_ids()
                .unwrap()
                .is_empty()
        );
        assert!(!importer.mutation_allowed().unwrap());
        assert_eq!(fs::read(&manifest).unwrap(), original);
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
        let store = Arc::new(RuntimeEventStore::for_test());
        let importer =
            LegacyExecutionImporter::new(Arc::clone(&store), "workspace", temp.path(), "0.9.472");

        let receipt = importer.import_manifest(&manifest).unwrap();

        assert_eq!(receipt.imported_count, 0);
        assert_eq!(receipt.disposition_count, 1);
        assert_eq!(receipt.active_carrier_count, 1);
        assert_eq!(receipt.pre_hash, receipt.post_hash);
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "legacy_execution_disposition_recorded");
        assert_eq!(events[1].kind, LEGACY_EXECUTION_IMPORTED);
        assert_eq!(events[0].transaction_id, events[1].transaction_id);
        assert_eq!(events[0].commit_cursor, events[1].commit_cursor);
        let replay = importer.import_manifest(&manifest).unwrap();
        assert!(replay.duplicate);
        assert_eq!(replay.transaction_id, receipt.transaction_id);
        assert_eq!(replay.pre_hash, receipt.pre_hash);
        assert_eq!(replay.post_hash, receipt.post_hash);
        assert_eq!(store.list_stream("upgrade:workspace").unwrap(), events);
        assert!(importer.mutation_allowed().unwrap());

        assert!(importer
            .import_receipt_file(temp.path().join("missing-later-receipt.json"))
            .is_err());
        let blocked = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(blocked.last().unwrap().kind, UPGRADE_RECOVERY_REQUIRED);
        assert!(importer.import_manifest(&manifest).unwrap().duplicate);
        assert!(!importer.mutation_allowed().unwrap());
        assert_eq!(store.list_stream("upgrade:workspace").unwrap(), blocked);
    }

    #[test]
    fn an_undisposed_carrier_prevents_partial_disposition_publication() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let mut inventory = inventory(temp.path());
        inventory.dispositions.push(UpgradeDispositionReceipt {
            carrier_kind: "active_turn".into(),
            carrier_id: "turn-1".into(),
            action: "cancel".into(),
            actor: "operator".into(),
            reason: "upgrade".into(),
            result_refs: vec!["cancellation:turn-1".into()],
            created_at_ms: 43,
        });
        let mut undisposed = inventory.carriers[0].clone();
        undisposed.carrier_id = "turn-2".into();
        inventory.carriers.push(undisposed);
        inventory.active_count = 2;
        inventory.carrier_count = 2;
        inventory.content_hash = inventory_content_hash(&inventory).unwrap();
        let original = serde_json::to_vec_pretty(&inventory).unwrap();
        fs::write(&manifest, &original).unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
        let importer =
            LegacyExecutionImporter::new(store.clone(), "workspace", temp.path(), "0.9.472");

        assert!(importer.import_manifest(&manifest).is_err());
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, UPGRADE_RECOVERY_REQUIRED);
        assert!(!importer.mutation_allowed().unwrap());
        assert!(
            crate::execution_core::graph::ExecutionGraphStateStore::new(store)
                .graph_ids()
                .unwrap()
                .is_empty()
        );
        assert_eq!(fs::read(&manifest).unwrap(), original);
    }

    #[test]
    fn drain_disposition_retries_atomically_after_a_failed_import_append() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let mut inventory = inventory(temp.path());
        inventory.dispositions.push(UpgradeDispositionReceipt {
            carrier_kind: "active_turn".into(),
            carrier_id: "turn-1".into(),
            action: "drain".into(),
            actor: "operator".into(),
            reason: "upgrade after physical drain".into(),
            result_refs: vec!["drain:turn-1".into()],
            created_at_ms: 43,
        });
        inventory.content_hash = inventory_content_hash(&inventory).unwrap();
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
        let importer =
            LegacyExecutionImporter::new(store.clone(), "workspace", temp.path(), "0.9.472");
        let transaction = format!("legacy-execution-import:{}", inventory.content_hash);
        store.inject_append_fault_once(transaction.clone(), false);

        assert!(importer.import_manifest(&manifest).is_err());
        assert!(!store.append_fault_is_pending(&transaction));
        let failed = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].kind, UPGRADE_RECOVERY_REQUIRED);
        assert!(!importer.mutation_allowed().unwrap());

        let recovered = importer.import_manifest(&manifest).unwrap();
        assert_eq!(recovered.imported_count, 0);
        assert_eq!(recovered.disposition_count, 1);
        assert_eq!(recovered.pre_hash, recovered.post_hash);
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[1].kind, "legacy_execution_disposition_recorded");
        assert_eq!(events[2].kind, LEGACY_EXECUTION_IMPORTED);
        assert_eq!(events[1].transaction_id, events[2].transaction_id);
        assert_eq!(events[1].commit_cursor, events[2].commit_cursor);
        assert_eq!(events[1].payload["disposition"]["action"], "drain");
        assert!(importer.mutation_allowed().unwrap());
        assert!(importer.import_manifest(&manifest).unwrap().duplicate);
        assert_eq!(store.list_stream("upgrade:workspace").unwrap(), events);
        assert!(
            crate::execution_core::graph::ExecutionGraphStateStore::new(store)
                .graph_ids()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn recovery_marker_write_failures_are_reported_by_every_import_entry() {
        for entry in 0..3 {
            for after_commit in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let missing = temp.path().join("missing.json");
                let store = Arc::new(RuntimeEventStore::for_test());
                let importer = LegacyExecutionImporter::new(
                    store.clone(),
                    "workspace",
                    temp.path(),
                    "0.9.472",
                );
                let original = LegacyExecutionImportError::MissingManifest(missing.clone());
                let transaction = format!(
                    "upgrade-recovery-required:{}",
                    stable_hash(original.to_string().as_bytes()),
                );
                store.inject_append_fault_once(transaction.clone(), after_commit);
                let error = match entry {
                    0 => importer.import_manifest(&missing),
                    1 => importer.import_receipt_file(&missing),
                    _ => importer.import_clean_shutdown(&UpgradeCleanShutdownReceipt {
                        schema_version: 1,
                        source_binary_version: "0.9.472".into(),
                        workspace_id: "workspace".into(),
                        manifest_path: missing.clone(),
                        manifest_hash: "missing".into(),
                        active_count: 1,
                        clean_shutdown_at_ms: 43,
                    }),
                }
                .unwrap_err();
                assert!(error
                    .to_string()
                    .contains("recovery marker persistence failed"));
                assert!(error.to_string().contains(&original.to_string()));
                assert!(error
                    .to_string()
                    .contains("injected transaction reply failure"));
                assert!(std::error::Error::source(&error)
                    .unwrap()
                    .to_string()
                    .contains("injected transaction reply failure"));
                assert!(!store.append_fault_is_pending(&transaction));
                assert_eq!(
                    store.list_stream("upgrade:workspace").unwrap().len(),
                    usize::from(after_commit)
                );
                assert_eq!(importer.mutation_allowed().unwrap(), !after_commit);
            }
        }
    }

    #[test]
    fn an_old_import_marker_cannot_restore_undisposed_carriers() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let inventory = inventory(temp.path());
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
        let original = store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("legacy-execution-import:{}", inventory.content_hash),
                expected_streams: vec![ExpectedStreamRevision {
                    stream_id: "upgrade:workspace".into(),
                    expected_revision: 0,
                }],
                events: vec![transaction_event(
                    "upgrade:workspace",
                    RuntimeEventScope::Recovery,
                    LEGACY_EXECUTION_IMPORTED,
                    format!("legacy-marker:{}", inventory.content_hash),
                    serde_json::json!({"manifest_hash":inventory.content_hash,"imported_count":1,
                    "imported_graph_ids":["legacy-unverified-graph"]}),
                )],
            })
            .unwrap();
        let importer =
            LegacyExecutionImporter::new(store.clone(), "workspace", temp.path(), "0.9.472");

        assert!(importer
            .import_manifest(&manifest)
            .unwrap_err()
            .to_string()
            .contains("explicit cancel or drain disposition"));
        let events = store.list_stream("upgrade:workspace").unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].transaction_id, original.transaction_id);
        assert_eq!(
            events[0].payload["imported_graph_ids"][0],
            "legacy-unverified-graph"
        );
        assert_eq!(events[1].kind, UPGRADE_RECOVERY_REQUIRED);
        assert!(!importer.mutation_allowed().unwrap());
        assert!(
            crate::execution_core::graph::ExecutionGraphStateStore::new(store)
                .graph_ids()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn invalid_manifest_marks_recovery_required_and_blocks_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let manifest = temp.path().join("clean-shutdown.json");
        let mut inventory = inventory(temp.path());
        inventory.active_count = 2;
        fs::write(&manifest, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
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
