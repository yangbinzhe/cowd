//! Atomic ownership-cutover publication contract.
//!
//! This module validates the immutable evidence consumed by the offline
//! ownership-cutover coordinator. It does not connect to either database, acquire the maintenance
//! lock, stop Gateway/APP workers, or make staged generations visible. Those
//! actions remain with the offline ownership-cutover coordinator and are represented here only by
//! durable receipt digests.

use std::{
    collections::BTreeSet,
    fs,
    path::{Component, Path, PathBuf},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

const CONTRACT_ID: &str = "cowd.ownership-cutover/v1";
const OWNERSHIP_CONTRACT_DIGEST: &str =
    "sha256:61ed3c6becf145fcf1029b4ee39b2ac4d0aa39177ae2e195fe7ec2b052f270e5";
const MANIFEST_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.manifest.v1\0";
const EXECUTION_DIGEST_DOMAIN: &[u8] = b"cowd.ownership-cutover.execution.v1\0";
const ACTIVE_FILE: &str = "active.json";
const PUBLICATION_DIRECTORY: &str = "publication";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OwnershipCutoverManifestV1 {
    pub schema_version: u16,
    pub contract_id: String,
    pub ownership_contract_digest: String,
    pub execution_contract_digest: String,
    pub publication_generation: String,
    pub activation_fence_id: String,
    pub source: CutoverSourceV1,
    pub core: CutoverTargetV1,
    pub mfg: CutoverTargetV1,
    pub execution_receipts: CutoverExecutionReceiptsV1,
    pub state_receipts: CutoverStateReceiptsV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous: Option<PreviousCutoverV1>,
    pub created_at: String,
    pub activation_kind: CutoverActivationKindV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_target_manifest_digest: Option<String>,
    pub manifest_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutoverSourceV1 {
    pub backend: CutoverBackendV1,
    pub namespace: String,
    pub source_version: String,
    pub schema_version: u64,
    pub maintenance_fence_id: String,
    pub snapshot_relative_path: String,
    pub snapshot_whole_digest: String,
    pub snapshot_file_digest: String,
    pub export_receipt_relative_path: String,
    pub export_receipt_digest: String,
    pub core_section_digest: String,
    pub mfg_section_digest: String,
    pub counts: CutoverCountsV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutoverCountsV1 {
    pub tables: u64,
    pub mfg_objects: u64,
    pub core_objects: u64,
    pub reconciliation: u64,
    pub excluded: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutoverTargetV1 {
    pub backend: CutoverBackendV1,
    pub namespace: String,
    pub generation: String,
    pub relative_path: String,
    pub section_digest: String,
    pub durable_import_receipt_relative_path: String,
    pub durable_import_receipt_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutoverExecutionReceiptsV1 {
    pub maintenance_lock_receipt_digest: String,
    pub gateway_stopped_receipt_digest: String,
    pub apps_stopped_receipt_digest: String,
    pub legacy_readonly_receipt_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutoverStateReceiptsV1 {
    pub staged_receipt_digest: String,
    pub verified_receipt_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PreviousCutoverV1 {
    pub publication_generation: String,
    pub manifest_digest: String,
    pub core_generation: String,
    pub core_relative_path: String,
    pub mfg_generation: String,
    pub mfg_relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CutoverBackendV1 {
    Sqlite,
    Postgres,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CutoverActivationKindV1 {
    Active,
    Rollback,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OwnershipCutoverError {
    #[error("ownership cutover contract is invalid: {0}")]
    Invalid(String),
    #[error("ownership cutover I/O failed at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

impl OwnershipCutoverManifestV1 {
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, OwnershipCutoverError> {
        serde_json::from_slice(bytes)
            .map_err(|error| OwnershipCutoverError::Invalid(error.to_string()))
    }

    pub(crate) fn canonical_manifest_digest(&self) -> Result<String, OwnershipCutoverError> {
        let mut value = serde_json::to_value(self)
            .map_err(|error| OwnershipCutoverError::Invalid(error.to_string()))?;
        value
            .as_object_mut()
            .ok_or_else(|| invalid("manifest must serialize as an object"))?
            .remove("manifest_digest");
        canonical_digest(MANIFEST_DIGEST_DOMAIN, &value)
    }

    fn summary(&self) -> PreviousCutoverV1 {
        PreviousCutoverV1 {
            publication_generation: self.publication_generation.clone(),
            manifest_digest: self.manifest_digest.clone(),
            core_generation: self.core.generation.clone(),
            core_relative_path: self.core.relative_path.clone(),
            mfg_generation: self.mfg.generation.clone(),
            mfg_relative_path: self.mfg.relative_path.clone(),
        }
    }

    pub(crate) fn validate(
        &self,
        root: &Path,
        current: Option<&Self>,
        history: &[Self],
    ) -> Result<(), OwnershipCutoverError> {
        self.validate_shape(root)?;
        self.validate_transition(root, current, history)
    }

    fn validate_shape(&self, root: &Path) -> Result<(), OwnershipCutoverError> {
        if self.schema_version != 1 || self.contract_id != CONTRACT_ID {
            return Err(invalid("schema_version or contract_id is not V1"));
        }
        if self.ownership_contract_digest != OWNERSHIP_CONTRACT_DIGEST {
            return Err(invalid(
                "cowd.ownership-split/v1.2-final contract digest mismatch",
            ));
        }
        let execution: Value = serde_json::from_str(include_str!(
            "../../../../contracts/ownership-cutover/v1/execution-contract.json"
        ))
        .map_err(|error| invalid(error.to_string()))?;
        let expected_execution = canonical_digest(EXECUTION_DIGEST_DOMAIN, &execution)?;
        if self.execution_contract_digest != expected_execution {
            return Err(invalid("execution contract digest mismatch"));
        }
        validate_identifier("publication_generation", &self.publication_generation, 128)?;
        validate_identifier("activation_fence_id", &self.activation_fence_id, 256)?;
        validate_identifier("source.namespace", &self.source.namespace, 128)?;
        validate_identifier("source.source_version", &self.source.source_version, 256)?;
        validate_identifier(
            "source.maintenance_fence_id",
            &self.source.maintenance_fence_id,
            256,
        )?;
        if self.source.schema_version == 0 {
            return Err(invalid("source.schema_version must be positive"));
        }
        validate_target("core", &self.core)?;
        validate_target("mfg", &self.mfg)?;
        if self.core.generation == self.mfg.generation
            || self.core.relative_path == self.mfg.relative_path
            || (self.core.backend == self.mfg.backend && self.core.namespace == self.mfg.namespace)
        {
            return Err(invalid(
                "Core and MFG generations must have distinct ownership",
            ));
        }
        if (self.source.backend == self.core.backend
            && self.source.namespace == self.core.namespace)
            || (self.source.backend == self.mfg.backend
                && self.source.namespace == self.mfg.namespace)
        {
            return Err(invalid(
                "legacy source and target namespaces must be distinct",
            ));
        }
        for digest in self.all_required_digests() {
            validate_digest(digest)?;
        }
        if self.source.counts.tables != 46 || self.source.counts.excluded != 3 {
            return Err(invalid(
                "cowd.ownership-split/v1.2-final table/excluded counts do not match",
            ));
        }
        let created_at = parse_utc(&self.created_at, "created_at")?;
        if created_at.timestamp_millis() <= 0 {
            return Err(invalid("created_at must be after the Unix epoch"));
        }
        match self.activation_kind {
            CutoverActivationKindV1::Active => {
                if self.rollback_target_manifest_digest.is_some() {
                    return Err(invalid("active publication cannot carry rollback evidence"));
                }
            }
            CutoverActivationKindV1::Rollback => {
                let target = self
                    .rollback_target_manifest_digest
                    .as_deref()
                    .ok_or_else(|| invalid("rollback target digest is required"))?;
                validate_digest(target)?;
            }
        }
        if let Some(previous) = &self.previous {
            validate_identifier(
                "previous.publication_generation",
                &previous.publication_generation,
                128,
            )?;
            validate_identifier("previous.core_generation", &previous.core_generation, 128)?;
            validate_identifier("previous.mfg_generation", &previous.mfg_generation, 128)?;
            validate_digest(&previous.manifest_digest)?;
            validate_relative(&previous.core_relative_path)?;
            validate_relative(&previous.mfg_relative_path)?;
        }
        if self.manifest_digest != self.canonical_manifest_digest()? {
            return Err(invalid("manifest digest mismatch"));
        }
        validate_source_evidence(root, &self.source)?;
        validate_target_evidence(root, &self.core, &self.source, "core")?;
        validate_target_evidence(root, &self.mfg, &self.source, "mfg")
    }

    fn validate_transition(
        &self,
        root: &Path,
        current: Option<&Self>,
        history: &[Self],
    ) -> Result<(), OwnershipCutoverError> {
        for manifest in history.iter().chain(current) {
            manifest.validate_shape(root)?;
        }
        let mut publications = BTreeSet::new();
        let mut fences = BTreeSet::new();
        let mut active_core_generations = BTreeSet::new();
        let mut active_mfg_generations = BTreeSet::new();
        let mut manifests = Vec::new();
        for manifest in history.iter().chain(current) {
            if !publications.insert(manifest.publication_generation.as_str())
                || !fences.insert(manifest.activation_fence_id.as_str())
            {
                return Err(invalid("history contains a reused generation or fence"));
            }
            if matches!(manifest.activation_kind, CutoverActivationKindV1::Active) {
                active_core_generations.insert(manifest.core.generation.as_str());
                active_mfg_generations.insert(manifest.mfg.generation.as_str());
            }
            manifests.push(manifest);
        }
        if publications.contains(self.publication_generation.as_str())
            || fences.contains(self.activation_fence_id.as_str())
            || manifests
                .iter()
                .any(|manifest| manifest.manifest_digest == self.manifest_digest)
        {
            return Err(invalid(
                "publication generation, fence, or manifest was replayed",
            ));
        }
        match current {
            Some(current) => {
                if self.previous.as_ref() != Some(&current.summary()) {
                    return Err(invalid(
                        "previous pointer does not name the current active manifest",
                    ));
                }
                if parse_utc(&self.created_at, "created_at")?
                    <= parse_utc(&current.created_at, "current.created_at")?
                {
                    return Err(invalid("publication time must advance"));
                }
            }
            None if self.previous.is_some() => {
                return Err(invalid(
                    "initial publication cannot claim a previous generation",
                ));
            }
            None => {}
        }
        match self.activation_kind {
            CutoverActivationKindV1::Active => {
                if active_core_generations.contains(self.core.generation.as_str())
                    || active_mfg_generations.contains(self.mfg.generation.as_str())
                {
                    return Err(invalid("active database generation cannot be reused"));
                }
            }
            CutoverActivationKindV1::Rollback => {
                let target_digest = self
                    .rollback_target_manifest_digest
                    .as_deref()
                    .ok_or_else(|| invalid("rollback target digest is required"))?;
                let target = manifests
                    .iter()
                    .copied()
                    .find(|manifest| manifest.manifest_digest == target_digest)
                    .ok_or_else(|| invalid("rollback target is not present in verified history"))?;
                if current.is_some_and(|active| active.manifest_digest == target_digest)
                    || self.core.generation != target.core.generation
                    || self.core.relative_path != target.core.relative_path
                    || self.mfg.generation != target.mfg.generation
                    || self.mfg.relative_path != target.mfg.relative_path
                {
                    return Err(invalid(
                        "rollback pointer does not exactly select a historical pair",
                    ));
                }
            }
        }
        Ok(())
    }

    fn all_required_digests(&self) -> [&str; 17] {
        [
            &self.ownership_contract_digest,
            &self.execution_contract_digest,
            &self.source.snapshot_whole_digest,
            &self.source.snapshot_file_digest,
            &self.source.export_receipt_digest,
            &self.source.core_section_digest,
            &self.source.mfg_section_digest,
            &self.core.section_digest,
            &self.core.durable_import_receipt_digest,
            &self.mfg.section_digest,
            &self.mfg.durable_import_receipt_digest,
            &self.execution_receipts.maintenance_lock_receipt_digest,
            &self.execution_receipts.gateway_stopped_receipt_digest,
            &self.execution_receipts.apps_stopped_receipt_digest,
            &self.execution_receipts.legacy_readonly_receipt_digest,
            &self.state_receipts.staged_receipt_digest,
            &self.state_receipts.verified_receipt_digest,
        ]
    }
}

fn validate_source_evidence(
    root: &Path,
    source: &CutoverSourceV1,
) -> Result<(), OwnershipCutoverError> {
    let snapshot = read_regular_relative(root, &source.snapshot_relative_path, false)?;
    if digest_bytes(&snapshot) != source.snapshot_file_digest {
        return Err(invalid("snapshot file digest mismatch"));
    }
    matrix_core::MfgOwnershipSplitSnapshotV1::decode_strict(&snapshot)
        .map_err(|error| invalid(error.to_string()))?;
    let snapshot: Value = serde_json::from_slice(&snapshot)
        .map_err(|error| invalid(format!("snapshot JSON: {error}")))?;
    verify_embedded_digest(
        &snapshot,
        "whole_snapshot_digest",
        "cowd.ownership.snapshot.v1",
    )?;
    verify_embedded_digest(
        field(&snapshot, "core_matrix_domain")?,
        "section_digest",
        "cowd.ownership.section.v1",
    )?;
    verify_embedded_digest(
        field(&snapshot, "mfg_domain")?,
        "section_digest",
        "cowd.ownership.section.v1",
    )?;
    let snapshot_source = field(&snapshot, "source")?;
    require_json_string(snapshot_source, "source_version", &source.source_version)?;
    require_json_u64(snapshot_source, "schema_version", source.schema_version)?;
    require_json_string(
        snapshot_source,
        "maintenance_fence_id",
        &source.maintenance_fence_id,
    )?;
    require_json_string(
        snapshot_source,
        "ownership_contract_digest",
        OWNERSHIP_CONTRACT_DIGEST,
    )?;
    require_json_string(
        &snapshot,
        "whole_snapshot_digest",
        &source.snapshot_whole_digest,
    )?;
    require_json_string(
        field(&snapshot, "core_matrix_domain")?,
        "section_digest",
        &source.core_section_digest,
    )?;
    require_json_string(
        field(&snapshot, "mfg_domain")?,
        "section_digest",
        &source.mfg_section_digest,
    )?;
    require_json_u64(
        field(&snapshot, "core_matrix_domain")?,
        "object_count",
        source.counts.core_objects,
    )?;
    require_json_u64(
        field(&snapshot, "mfg_domain")?,
        "object_count",
        source.counts.mfg_objects,
    )?;
    let reconciliation = field(&snapshot, "reconciliation")?;
    let reconciliation_count = [
        "pending_outbox",
        "command_receipts",
        "mutation_receipts",
        "mutation_receipt_aliases",
        "mutation_receipt_repairs",
    ]
    .into_iter()
    .try_fold(0_u64, |total, name| {
        reconciliation
            .get(name)
            .and_then(Value::as_array)
            .map(|records| total + records.len() as u64)
            .ok_or_else(|| invalid(format!("snapshot reconciliation.{name} is not an array")))
    })?;
    if reconciliation_count != source.counts.reconciliation
        || field(&snapshot, "excluded")?
            .as_array()
            .map(|records| records.len() as u64)
            != Some(source.counts.excluded)
    {
        return Err(invalid(
            "snapshot array counts differ from the cutover manifest",
        ));
    }
    let receipt = read_regular_relative(root, &source.export_receipt_relative_path, false)?;
    let receipt: Value = serde_json::from_slice(&receipt)
        .map_err(|error| invalid(format!("export receipt JSON: {error}")))?;
    require_exact_keys(
        &receipt,
        "export receipt",
        &[
            "schema",
            "generation",
            "snapshot_file_digest",
            "contract_digest",
            "schema_digest",
            "external_catalog_digest",
            "revision_baseline_digest",
            "execution_profile_digest",
            "source",
            "counts",
            "excluded_actions",
            "receipt_digest",
        ],
    )?;
    verify_embedded_digest(
        &receipt,
        "receipt_digest",
        "cowd.ownership.export-receipt.v1",
    )?;
    require_json_string(&receipt, "schema", "OwnershipExportReceiptV1")?;
    require_json_string(&receipt, "contract_digest", OWNERSHIP_CONTRACT_DIGEST)?;
    require_json_string(
        &receipt,
        "snapshot_file_digest",
        &source.snapshot_file_digest,
    )?;
    require_json_string(&receipt, "receipt_digest", &source.export_receipt_digest)?;
    let receipt_source = field(&receipt, "source")?;
    require_exact_keys(
        receipt_source,
        "export receipt source",
        &[
            "backend",
            "namespace",
            "source_version",
            "schema_version",
            "maintenance_fence_id",
            "exported_at",
        ],
    )?;
    require_json_string(receipt_source, "backend", backend_name(source.backend))?;
    require_json_string(receipt_source, "namespace", &source.namespace)?;
    require_json_string(receipt_source, "source_version", &source.source_version)?;
    require_json_u64(receipt_source, "schema_version", source.schema_version)?;
    require_json_string(
        receipt_source,
        "maintenance_fence_id",
        &source.maintenance_fence_id,
    )?;
    parse_utc(
        receipt_source
            .get("exported_at")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("export receipt source.exported_at is missing"))?,
        "export receipt source.exported_at",
    )?;
    let counts = field(&receipt, "counts")?;
    require_exact_keys(
        counts,
        "export receipt counts",
        &[
            "tables",
            "mfg_objects",
            "core_objects",
            "reconciliation",
            "excluded",
        ],
    )?;
    for (name, expected) in [
        ("tables", source.counts.tables),
        ("mfg_objects", source.counts.mfg_objects),
        ("core_objects", source.counts.core_objects),
        ("reconciliation", source.counts.reconciliation),
        ("excluded", source.counts.excluded),
    ] {
        require_json_u64(counts, name, expected)?;
    }
    if receipt
        .get("excluded_actions")
        .and_then(Value::as_array)
        .map(Vec::len)
        != Some(source.counts.excluded as usize)
    {
        return Err(invalid(
            "export receipt excluded actions differ from the source split",
        ));
    }
    Ok(())
}

fn validate_target(owner: &str, target: &CutoverTargetV1) -> Result<(), OwnershipCutoverError> {
    validate_identifier(&format!("{owner}.namespace"), &target.namespace, 128)?;
    validate_identifier(&format!("{owner}.generation"), &target.generation, 128)?;
    validate_relative(&target.relative_path)?;
    validate_relative(&target.durable_import_receipt_relative_path)?;
    validate_digest(&target.section_digest)?;
    validate_digest(&target.durable_import_receipt_digest)
}

fn validate_target_evidence(
    root: &Path,
    target: &CutoverTargetV1,
    source: &CutoverSourceV1,
    owner: &str,
) -> Result<(), OwnershipCutoverError> {
    let source_section_digest = if owner == "core" {
        &source.core_section_digest
    } else {
        &source.mfg_section_digest
    };
    if target.section_digest != source_section_digest.as_str() {
        return Err(invalid(
            "target section digest differs from the source split",
        ));
    }
    let generation = resolve_relative(root, &target.relative_path)?;
    if !fs::metadata(&generation)
        .map_err(io_error(&generation))?
        .is_dir()
    {
        return Err(invalid("target generation path is not a directory"));
    }
    let receipt = read_regular_relative(root, &target.durable_import_receipt_relative_path, false)?;
    if digest_bytes(&receipt) != target.durable_import_receipt_digest {
        return Err(invalid("durable import receipt file digest mismatch"));
    }
    let value: Value = serde_json::from_slice(&receipt)
        .map_err(|error| invalid(format!("import receipt JSON: {error}")))?;
    require_exact_keys(
        &value,
        "import receipt",
        &[
            "schema_version",
            "owner",
            "backend",
            "namespace",
            "generation",
            "ownership_contract_digest",
            "section_digest",
            "source_snapshot_whole_digest",
            "source_version",
            "source_schema_version",
            "maintenance_fence_id",
            "counts",
            "target_checkpoint",
            "durable",
            "completed_at",
        ],
    )?;
    require_json_u64(&value, "schema_version", 1)?;
    require_json_string(&value, "owner", owner)?;
    require_json_string(&value, "generation", &target.generation)?;
    require_json_string(&value, "section_digest", &target.section_digest)?;
    require_json_string(&value, "backend", backend_name(target.backend))?;
    require_json_string(&value, "namespace", &target.namespace)?;
    require_json_string(
        &value,
        "ownership_contract_digest",
        OWNERSHIP_CONTRACT_DIGEST,
    )?;
    require_json_string(
        &value,
        "source_snapshot_whole_digest",
        &source.snapshot_whole_digest,
    )?;
    require_json_string(&value, "source_version", &source.source_version)?;
    require_json_u64(&value, "source_schema_version", source.schema_version)?;
    require_json_string(&value, "maintenance_fence_id", &source.maintenance_fence_id)?;
    let counts = field(&value, "counts")?;
    require_exact_keys(
        counts,
        "import receipt counts",
        &[
            "tables",
            "mfg_objects",
            "core_objects",
            "reconciliation",
            "excluded",
        ],
    )?;
    for (name, expected) in [
        ("tables", source.counts.tables),
        ("mfg_objects", source.counts.mfg_objects),
        ("core_objects", source.counts.core_objects),
        ("reconciliation", source.counts.reconciliation),
        ("excluded", source.counts.excluded),
    ] {
        require_json_u64(counts, name, expected)?;
    }
    let checkpoint = field(&value, "target_checkpoint")?;
    require_exact_keys(
        checkpoint,
        "target checkpoint",
        &[
            "source_generation",
            "imported_object_count",
            "reconciliation_count",
            "journal_digest",
        ],
    )?;
    require_json_string(checkpoint, "source_generation", &source.source_version)?;
    require_json_u64(
        checkpoint,
        "imported_object_count",
        if owner == "core" {
            source.counts.core_objects
        } else {
            source.counts.mfg_objects
        },
    )?;
    require_json_u64(
        checkpoint,
        "reconciliation_count",
        source.counts.reconciliation,
    )?;
    validate_digest(
        checkpoint
            .get("journal_digest")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("target checkpoint journal_digest is missing"))?,
    )?;
    if value.get("durable").and_then(Value::as_bool) != Some(true) {
        return Err(invalid("import receipt must attest durable completion"));
    }
    parse_utc(
        value
            .get("completed_at")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("import receipt completed_at is missing"))?,
        "import receipt completed_at",
    )?;
    Ok(())
}

pub(crate) fn validate_active_publication(
    root: &Path,
    current: Option<&OwnershipCutoverManifestV1>,
    history: &[OwnershipCutoverManifestV1],
) -> Result<OwnershipCutoverManifestV1, OwnershipCutoverError> {
    validate_publication_directory(root, true)?;
    let active = root.join(PUBLICATION_DIRECTORY).join(ACTIVE_FILE);
    let bytes = fs::read(&active).map_err(io_error(&active))?;
    let manifest = OwnershipCutoverManifestV1::decode(&bytes)?;
    manifest.validate(root, current, history)?;
    Ok(manifest)
}

fn validate_publication_directory(
    root: &Path,
    require_active: bool,
) -> Result<(), OwnershipCutoverError> {
    let publication = root.join(PUBLICATION_DIRECTORY);
    if !publication.exists() {
        return if require_active {
            Err(invalid("publication directory or active.json is missing"))
        } else {
            Ok(())
        };
    }
    let metadata = fs::symlink_metadata(&publication).map_err(io_error(&publication))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(invalid("publication directory must be a real directory"));
    }
    let mut names = fs::read_dir(&publication)
        .map_err(io_error(&publication))?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(io_error(&publication))?;
    names.sort();
    let active_name = std::ffi::OsString::from(ACTIVE_FILE);
    let expected = if require_active {
        vec![active_name.clone()]
    } else {
        Vec::new()
    };
    if names != expected && names != vec![active_name.clone()] {
        return Err(invalid(
            "publication directory may contain only the final active.json pointer",
        ));
    }
    if require_active && names.is_empty() {
        return Err(invalid("active.json is missing"));
    }
    if names == vec![active_name] {
        let active = publication.join(ACTIVE_FILE);
        let metadata = fs::symlink_metadata(&active).map_err(io_error(&active))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(invalid("active.json must be a regular file"));
        }
    }
    Ok(())
}

fn read_regular_relative(
    root: &Path,
    relative: &str,
    allow_directory: bool,
) -> Result<Vec<u8>, OwnershipCutoverError> {
    let path = resolve_relative(root, relative)?;
    let metadata = fs::metadata(&path).map_err(io_error(&path))?;
    if (!allow_directory && !metadata.is_file()) || (allow_directory && !metadata.is_dir()) {
        return Err(invalid("evidence path has the wrong file type"));
    }
    fs::read(&path).map_err(io_error(&path))
}

fn resolve_relative(root: &Path, relative: &str) -> Result<PathBuf, OwnershipCutoverError> {
    validate_relative(relative)?;
    let root = fs::canonicalize(root).map_err(io_error(root))?;
    let mut current = root.clone();
    for component in Path::new(relative).components() {
        let Component::Normal(component) = component else {
            return Err(invalid("path is not normalized and relative"));
        };
        current.push(component);
        let metadata = fs::symlink_metadata(&current).map_err(io_error(&current))?;
        if metadata.file_type().is_symlink() {
            return Err(invalid("path traverses a symbolic link"));
        }
    }
    if !current.starts_with(&root) {
        return Err(invalid("path escapes the cutover root"));
    }
    Ok(current)
}

fn validate_relative(value: &str) -> Result<(), OwnershipCutoverError> {
    let path = Path::new(value);
    if value.is_empty()
        || value.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(invalid(
            "path must be normalized, relative, and separator-safe",
        ));
    }
    Ok(())
}

fn validate_identifier(
    field: &str,
    value: &str,
    maximum: usize,
) -> Result<(), OwnershipCutoverError> {
    if value.is_empty()
        || value.len() > maximum
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b':'))
    {
        return Err(invalid(format!("{field} is not a bounded identifier")));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), OwnershipCutoverError> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(invalid("digest must use sha256"));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("digest must be canonical lowercase SHA-256"));
    }
    Ok(())
}

fn canonical_digest(domain: &[u8], value: &Value) -> Result<String, OwnershipCutoverError> {
    let canonical = canonicalize(value);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|error| OwnershipCutoverError::Invalid(error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(bytes);
    Ok(format!("sha256:{:x}", digest.finalize()))
}

fn verify_embedded_digest(
    value: &Value,
    field: &str,
    domain: &str,
) -> Result<(), OwnershipCutoverError> {
    let expected = value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("embedded digest {field} is missing")))?;
    validate_digest(expected)?;
    let mut body = value.clone();
    body.as_object_mut()
        .ok_or_else(|| invalid("embedded digest owner is not an object"))?
        .remove(field);
    let mut separator = domain.as_bytes().to_vec();
    separator.push(0);
    if canonical_digest(&separator, &body)? != expected {
        return Err(invalid(format!("embedded digest {field} mismatch")));
    }
    Ok(())
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key.clone(), canonicalize(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.iter().map(canonicalize).collect()),
        _ => value.clone(),
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn parse_utc(value: &str, field: &str) -> Result<DateTime<Utc>, OwnershipCutoverError> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| invalid(format!("{field} must be RFC3339")))?;
    if parsed.offset().local_minus_utc() != 0 || !value.ends_with('Z') {
        return Err(invalid(format!("{field} must be canonical UTC")));
    }
    Ok(parsed.with_timezone(&Utc))
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a Value, OwnershipCutoverError> {
    value
        .get(name)
        .ok_or_else(|| invalid(format!("evidence is missing {name}")))
}

fn require_json_string(
    value: &Value,
    name: &str,
    expected: &str,
) -> Result<(), OwnershipCutoverError> {
    if value.get(name).and_then(Value::as_str) != Some(expected) {
        return Err(invalid(format!("evidence field {name} does not match")));
    }
    Ok(())
}

fn require_json_u64(value: &Value, name: &str, expected: u64) -> Result<(), OwnershipCutoverError> {
    if value.get(name).and_then(Value::as_u64) != Some(expected) {
        return Err(invalid(format!("evidence field {name} does not match")));
    }
    Ok(())
}

fn require_exact_keys(
    value: &Value,
    owner: &str,
    expected: &[&str],
) -> Result<(), OwnershipCutoverError> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid(format!("{owner} must be an object")))?;
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(invalid(format!("{owner} has a missing or unknown field")));
    }
    Ok(())
}

fn backend_name(backend: CutoverBackendV1) -> &'static str {
    match backend {
        CutoverBackendV1::Sqlite => "sqlite",
        CutoverBackendV1::Postgres => "postgres",
    }
}

fn invalid(detail: impl Into<String>) -> OwnershipCutoverError {
    OwnershipCutoverError::Invalid(detail.into())
}

fn io_error(path: &Path) -> impl FnOnce(std::io::Error) -> OwnershipCutoverError + '_ {
    move |source| OwnershipCutoverError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONTRACT_ROOT: &str = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../contracts/ownership-cutover/v1"
    );

    fn root() -> PathBuf {
        PathBuf::from(CONTRACT_ROOT)
    }

    fn read_manifest(path: &str) -> OwnershipCutoverManifestV1 {
        OwnershipCutoverManifestV1::decode(&fs::read(root().join(path)).expect("fixture read"))
            .expect("fixture decode")
    }

    #[test]
    fn active_golden_binds_source_targets_receipts_and_frozen_contracts() {
        let manifest = validate_active_publication(&root(), None, &[]).expect("active golden");
        assert_eq!(manifest.source.counts.tables, 46);
        assert!(manifest.source.counts.core_objects > 0);
        assert!(manifest.source.counts.mfg_objects > 0);
        assert!(manifest.source.counts.reconciliation > 0);
        assert_ne!(manifest.core.generation, manifest.mfg.generation);
        assert_eq!(
            manifest.manifest_digest,
            manifest.canonical_manifest_digest().unwrap()
        );
    }

    #[test]
    fn schema_and_execution_contract_freeze_closed_publication_semantics() {
        let schema: Value = serde_json::from_slice(
            &fs::read(root().join("active.schema.json")).expect("schema read"),
        )
        .expect("schema JSON");
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["ownership_contract_digest"]["const"],
            OWNERSHIP_CONTRACT_DIGEST
        );
        let execution: Value = serde_json::from_slice(
            &fs::read(root().join("execution-contract.json")).expect("execution read"),
        )
        .expect("execution JSON");
        assert_eq!(execution["atomicity"]["cross_database_acid_claimed"], false);
        assert_eq!(
            execution["rollback"]["deletes_database_or_generation"],
            false
        );
        assert_eq!(
            execution["state_machine"]["publication_directory_final_entries"],
            serde_json::json!(["active.json"])
        );
        assert_eq!(
            canonical_digest(EXECUTION_DIGEST_DOMAIN, &execution).unwrap(),
            validate_active_publication(&root(), None, &[])
                .unwrap()
                .execution_contract_digest
        );
    }

    #[test]
    fn rollback_is_a_new_pointer_to_an_exact_historical_pair() {
        let first = read_manifest("golden/history/active-v0.json");
        let current = read_manifest("golden/history/active-v1.json");
        let rollback = read_manifest("golden/history/rollback-v2.json");
        first.validate_shape(&root()).expect("first");
        current
            .validate(&root(), Some(&first), &[])
            .expect("current");
        rollback
            .validate(&root(), Some(&current), &[first.clone()])
            .expect("rollback");
        assert_eq!(rollback.core.relative_path, first.core.relative_path);
        assert_eq!(rollback.mfg.relative_path, first.mfg.relative_path);
    }

    #[test]
    fn every_semantic_tamper_fixture_is_rejected() {
        let directory = root().join("golden/tamper");
        let mut seen = BTreeSet::new();
        for entry in fs::read_dir(directory).expect("tamper directory") {
            let path = entry.expect("entry").path();
            if path.extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            seen.insert(path.file_name().unwrap().to_owned());
            let bytes = fs::read(&path).expect("tamper read");
            match OwnershipCutoverManifestV1::decode(&bytes) {
                Ok(manifest) => assert!(
                    manifest.validate(&root(), None, &[]).is_err(),
                    "{} unexpectedly validated",
                    path.display()
                ),
                Err(_) => {}
            }
        }
        assert_eq!(seen.len(), 15, "the complete frozen tamper matrix must run");
    }

    #[test]
    fn replay_and_half_stage_are_rejected_against_history_and_disk() {
        let first = read_manifest("golden/history/active-v0.json");
        assert!(first
            .validate(&root(), Some(&first), &[first.clone()])
            .is_err());
        let mut missing = first.clone();
        missing.source.snapshot_relative_path = "golden/source/missing.json".to_owned();
        missing.manifest_digest = missing.canonical_manifest_digest().unwrap();
        assert!(missing.validate(&root(), None, &[]).is_err());
    }

    #[test]
    fn evidence_receipts_reject_unknown_fields_after_digest_recomputation() {
        let scratch = copy_contract_tree("receipt-unknown-fields");
        let mut manifest = OwnershipCutoverManifestV1::decode(
            &fs::read(scratch.join("publication/active.json")).unwrap(),
        )
        .unwrap();

        let export_path = scratch.join(&manifest.source.export_receipt_relative_path);
        let mut export: Value = serde_json::from_slice(&fs::read(&export_path).unwrap()).unwrap();
        export["unexpected"] = serde_json::json!(true);
        export["receipt_digest"] = Value::Null;
        export.as_object_mut().unwrap().remove("receipt_digest");
        let receipt_digest =
            canonical_digest(b"cowd.ownership.export-receipt.v1\0", &export).unwrap();
        export["receipt_digest"] = Value::String(receipt_digest.clone());
        fs::write(&export_path, serde_json::to_vec_pretty(&export).unwrap()).unwrap();
        manifest.source.export_receipt_digest = receipt_digest;
        manifest.manifest_digest = manifest.canonical_manifest_digest().unwrap();
        assert!(manifest.validate(&scratch, None, &[]).is_err());

        let mut manifest = OwnershipCutoverManifestV1::decode(
            &fs::read(scratch.join("publication/active.json")).unwrap(),
        )
        .unwrap();
        let import_path = scratch.join(&manifest.core.durable_import_receipt_relative_path);
        let mut import: Value = serde_json::from_slice(&fs::read(&import_path).unwrap()).unwrap();
        import["unexpected"] = serde_json::json!(true);
        let import_bytes = serde_json::to_vec_pretty(&import).unwrap();
        fs::write(&import_path, &import_bytes).unwrap();
        manifest.core.durable_import_receipt_digest = digest_bytes(&import_bytes);
        manifest.manifest_digest = manifest.canonical_manifest_digest().unwrap();
        assert!(manifest.validate(&scratch, None, &[]).is_err());

        fs::remove_dir_all(scratch).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_evidence_is_rejected() {
        use std::os::unix::fs::symlink;
        let scratch = unique_scratch("symlink");
        fs::create_dir_all(scratch.join("safe")).unwrap();
        fs::write(scratch.join("outside.json"), b"{}").unwrap();
        symlink(scratch.join("outside.json"), scratch.join("safe/link.json")).unwrap();
        assert!(resolve_relative(&scratch, "safe/link.json").is_err());
        fs::remove_dir_all(scratch).unwrap();
    }

    #[test]
    fn publication_directory_rejects_intermediate_state_files() {
        let scratch = copy_contract_tree("intermediate-state");
        fs::write(scratch.join("publication/staged.json"), b"{}").unwrap();
        assert!(validate_publication_directory(&scratch, true).is_err());
        fs::remove_dir_all(scratch).unwrap();
    }

    fn unique_scratch(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "cowd-ownership-cutover-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ))
    }

    fn copy_contract_tree(label: &str) -> PathBuf {
        let target = unique_scratch(label);
        copy_directory(&root(), &target);
        target
    }

    fn copy_directory(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let destination = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_directory(&entry.path(), &destination);
            } else {
                fs::copy(entry.path(), destination).unwrap();
            }
        }
    }
}
