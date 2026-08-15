use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const OWNERSHIP_CONTRACT_DIGEST_V1: &str =
    "sha256:61ed3c6becf145fcf1029b4ee39b2ac4d0aa39177ae2e195fe7ec2b052f270e5";
pub const OWNERSHIP_CONTRACT_VERSION_V1: &str = "cowd.ownership-split/v1.2-final";
pub const OWNERSHIP_EXECUTION_PROFILE_DIGEST_V1: &str =
    "sha256:93e47823acdfbd15289a4792486c84e136a3b121a7c985fb519b2db30279cc78";

const FIELD_MAPPING: &str = include_str!("../../../../contracts/ownership/v1/field-mapping.json");
const IDENTITY: &str = include_str!("../../../../contracts/ownership/v1/identity.json");
const REVISION_PROJECTION: &str =
    include_str!("../../../../contracts/ownership/v1/revision-projection.json");
const REFERENCE_ENCODING: &str =
    include_str!("../../../../contracts/ownership/v1/reference-encoding.json");
const EXECUTION_PROFILE: &str =
    include_str!("../../../../contracts/ownership/v1/execution-profile.json");

const CORE_TABLES: &[(&str, &[&str], &[&str])] = &[
    (
        "matrix_entity",
        &["entity_id"],
        &[
            "attributes_json",
            "canonical_key",
            "confidence",
            "created_at",
            "display_name",
            "entity_id",
            "entity_json",
            "entity_type",
            "source_keys_json",
            "updated_at",
        ],
    ),
    (
        "matrix_entity_source_key",
        &["source_system", "source_key"],
        &[
            "created_at",
            "entity_id",
            "source_key",
            "source_ref",
            "source_system",
        ],
    ),
    (
        "matrix_relation",
        &["relation_id"],
        &[
            "attributes_json",
            "confidence",
            "created_at",
            "from_entity_id",
            "relation_id",
            "relation_json",
            "relation_type",
            "to_entity_id",
            "updated_at",
        ],
    ),
    (
        "matrix_fact",
        &["fact_id"],
        &[
            "confidence",
            "created_at",
            "dimensions_json",
            "entity_refs_json",
            "event_time",
            "fact_id",
            "fact_type",
            "measures_json",
            "metric_key",
            "raw_hash",
            "snapshot_id",
            "source_ref",
            "valid_from",
            "valid_to",
        ],
    ),
    (
        "matrix_attention_item",
        &["attention_id"],
        &[
            "attention_id",
            "attention_json",
            "created_at",
            "priority_score",
            "status",
            "updated_at",
        ],
    ),
    (
        "matrix_evidence_packet",
        &["packet_id"],
        &["attention_id", "created_at", "packet_id", "packet_json"],
    ),
    (
        "matrix_quality_gate",
        &["gate_id"],
        &[
            "created_at",
            "decision",
            "gate_id",
            "gate_json",
            "gate_type",
            "score",
            "target_ref",
        ],
    ),
    (
        "matrix_metric_definition",
        &["metric_id"],
        &["created_at", "definition_json", "metric_id", "updated_at"],
    ),
    (
        "matrix_metric_state",
        &["state_id"],
        &[
            "computed_at",
            "delta",
            "entity_scope",
            "metric_id",
            "period",
            "previous_value",
            "state_id",
            "state_json",
            "status",
            "value",
        ],
    ),
    (
        "matrix_metric_dependency",
        &["dependency_id"],
        &[
            "confidence",
            "created_at",
            "dependency_id",
            "dependency_json",
            "dependency_type",
            "downstream_metric_id",
            "updated_at",
            "upstream_metric_id",
        ],
    ),
    (
        "matrix_compute_job",
        &["job_id"],
        &[
            "created_at",
            "job_id",
            "job_json",
            "priority",
            "status",
            "trigger_fact_type",
            "updated_at",
        ],
    ),
    (
        "matrix_change_event",
        &["change_id"],
        &[
            "change_id",
            "change_json",
            "delta",
            "detected_at",
            "entity_ref",
            "metric_id",
            "period",
            "severity_hint",
        ],
    ),
    (
        "matrix_source_pack",
        &["source_pack_id"],
        &[
            "access_mode",
            "created_at",
            "refresh_mode",
            "source_name",
            "source_pack_id",
            "source_pack_json",
            "updated_at",
        ],
    ),
    (
        "matrix_data_plane_watermark",
        &["source_ref", "fact_type", "partition_ref"],
        &[
            "fact_type",
            "high_watermark",
            "last_batch_id",
            "partition_ref",
            "source_ref",
            "updated_at",
            "watermark_json",
        ],
    ),
    (
        "matrix_connector_run",
        &["run_id"],
        &[
            "connector_kind",
            "created_at",
            "run_id",
            "run_json",
            "source_pack_id",
            "status",
            "updated_at",
        ],
    ),
    (
        "matrix_ontology_pack",
        &["ontology_id"],
        &[
            "domain",
            "ontology_id",
            "pack_json",
            "updated_at",
            "version",
        ],
    ),
    (
        "matrix_entity_match_candidate",
        &["candidate_id"],
        &[
            "candidate_id",
            "candidate_json",
            "confidence",
            "created_at",
            "left_entity_id",
            "right_entity_id",
            "status",
        ],
    ),
    (
        "matrix_entity_conflict_decision",
        &["decision_id"],
        &[
            "candidate_id",
            "decided_at",
            "decision_id",
            "decision_json",
            "retired_entity_id",
            "survivor_entity_id",
        ],
    ),
    (
        "matrix_metric_snapshot",
        &["snapshot_id"],
        &[
            "created_at",
            "metric_ids_json",
            "scope_ref",
            "snapshot_id",
            "snapshot_json",
        ],
    ),
];

const MFG_TABLES: &[&str] = &[
    "mfg_cockpit_profile",
    "mfg_cockpit_view_draft",
    "mfg_cockpit_view_proposal",
    "mfg_cockpit_view_version",
    "mfg_cockpit_view_active",
    "mfg_cockpit_report",
    "mfg_report_delivery_review",
    "mfg_report_delivery_review_transition",
    "mfg_report_delivery_review_effect_outbox",
    "mfg_alert_rule",
    "mfg_alert_occurrence",
    "mfg_alert_subscription",
    "mfg_assignment",
    "mfg_command_receipt",
    "mfg_mutation_receipt",
    "mfg_mutation_receipt_alias",
    "mfg_mutation_receipt_repair_report",
    "mfg_incident",
    "mfg_operational_analysis",
    "mfg_action_execution",
    "mfg_memory_case",
    "mfg_playbook",
    "mfg_skill_execution",
    "mfg_workflow_graph",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipImportError {
    Decode(String),
    Invalid(String),
}

impl std::fmt::Display for OwnershipImportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Decode(message) => {
                write!(formatter, "ownership snapshot decode failed: {message}")
            }
            Self::Invalid(message) => write!(formatter, "ownership snapshot rejected: {message}"),
        }
    }
}

impl std::error::Error for OwnershipImportError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MfgOwnershipSplitSnapshotV1 {
    pub contract_version: String,
    pub source: OwnershipImportSource,
    pub mfg_domain: OwnershipImportSection,
    pub core_matrix_domain: OwnershipImportSection,
    pub reconciliation: OwnershipReconciliation,
    pub excluded: Vec<OwnershipExcludedRecord>,
    pub whole_snapshot_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipImportSource {
    pub app_id: String,
    pub source_version: String,
    pub schema_version: u64,
    pub exported_at: String,
    pub maintenance_fence_id: String,
    pub expected_legacy_schema_version: u64,
    pub ownership_contract_digest: String,
    pub external_catalog_digest: String,
    pub revision_baseline_digest: String,
    pub execution_profile_digest: String,
    pub legacy_schema: OwnershipLegacySchema,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipLegacySchema {
    pub namespace: String,
    pub id: u64,
    pub schema_version: u64,
    pub updated_at: String,
    pub disposition: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipImportSection {
    pub owner: String,
    pub object_count: u64,
    pub section_digest: String,
    pub objects: Vec<OwnershipImportObject>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipImportObject {
    pub source_table: String,
    pub stable_id: String,
    pub revision: OwnershipImportRevision,
    pub source_references: Vec<OwnershipReference>,
    pub evidence_references: Vec<OwnershipReference>,
    pub payload: BTreeMap<String, Value>,
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipImportRevision {
    pub projection_key: String,
    pub axis: Vec<Value>,
    pub context: BTreeMap<String, Value>,
    pub context_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReference {
    pub aggregate_type: String,
    pub stable_id: String,
    pub revision: Option<Value>,
    pub payload_digest: Option<String>,
    pub source: OwnershipReferenceSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReferenceSource {
    pub table: String,
    pub field: String,
    pub json_pointer: Option<String>,
    pub extractor_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReconciliation {
    pub pending_outbox: Vec<PendingOutboxRecord>,
    pub command_receipts: Vec<CommandReceiptRecord>,
    pub mutation_receipts: Vec<MutationReceiptRecord>,
    pub mutation_receipt_aliases: Vec<MutationAliasRecord>,
    pub mutation_receipt_repairs: Vec<MutationRepairRecord>,
    pub set_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingOutboxRecord {
    pub stable_ref: String,
    pub status: String,
    pub action: String,
    pub effect_key: String,
    pub attempt_count: u64,
    pub next_attempt_at: Option<String>,
    pub last_error: Option<String>,
    pub receipt_ref: Option<String>,
    pub payload: Value,
    pub payload_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandReceiptRecord {
    pub stable_ref: String,
    pub status: String,
    pub domain: String,
    pub idempotency_key: String,
    pub subject_ref: String,
    pub receipt: Value,
    pub created_at: String,
    pub payload_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationReceiptRecord {
    pub stable_ref: String,
    pub status: String,
    pub receipt_id: String,
    pub idempotency_key: String,
    pub actor_principal: String,
    pub action_id: String,
    pub resource_ref: String,
    pub expected_revision: Value,
    pub result_revision: Value,
    pub mutation_payload_digest: String,
    pub lease_token: String,
    pub response: Value,
    pub contract_version: String,
    pub created_at: String,
    pub updated_at: String,
    pub payload_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationAliasRecord {
    pub stable_ref: String,
    pub status: String,
    pub legacy_idempotency_key: String,
    pub canonical_receipt_stable_id: String,
    pub canonical_receipt_payload_digest: String,
    pub created_at: String,
    pub payload_digest: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRepairRecord {
    pub stable_ref: String,
    pub status: String,
    pub report_id: String,
    pub idempotency_key: String,
    pub existing_receipt: Value,
    pub incoming_receipt: Value,
    pub existing_digest: String,
    pub incoming_digest: String,
    pub conflict_fields: Vec<String>,
    pub created_at: String,
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipExcludedRecord {
    pub source_table: String,
    pub reason: String,
    pub regeneration: String,
}

#[derive(Debug, Clone)]
pub struct OwnershipImportContext {
    pub external_reference_catalog: Vec<u8>,
    pub revision_baseline: Vec<u8>,
    pub execution_profile: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalReferenceCatalog {
    schema: String,
    digest: String,
    owner: String,
    exported_at: String,
    entries: Vec<ExternalReferenceEntry>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalReferenceEntry {
    aggregate_type: String,
    stable_id: String,
    revision: Option<Value>,
    payload_digest: Option<String>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevisionBaseline {
    schema: String,
    digest: String,
    owner: String,
    exported_at: String,
    initial: bool,
    entries: Vec<RevisionBaselineEntry>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RevisionBaselineEntry {
    aggregate_type: String,
    projection_key: String,
    axis_max: Vec<Value>,
    context_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedCoreMatrixRecord {
    table: String,
}

impl ImportedCoreMatrixRecord {
    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMatrixImportPlan {
    source: OwnershipImportSource,
    whole_snapshot_digest: String,
    section_digest: String,
    records: Vec<ImportedCoreMatrixRecord>,
    objects: Vec<OwnershipImportObject>,
}

impl CoreMatrixImportPlan {
    #[must_use]
    pub fn source(&self) -> &OwnershipImportSource {
        &self.source
    }
    #[must_use]
    pub fn whole_snapshot_digest(&self) -> &str {
        &self.whole_snapshot_digest
    }
    #[must_use]
    pub fn section_digest(&self) -> &str {
        &self.section_digest
    }
    #[must_use]
    pub fn records(&self) -> &[ImportedCoreMatrixRecord] {
        &self.records
    }
    #[must_use]
    pub fn objects(&self) -> &[OwnershipImportObject] {
        &self.objects
    }
}

impl MfgOwnershipSplitSnapshotV1 {
    pub fn decode_strict(bytes: &[u8]) -> Result<Self, OwnershipImportError> {
        serde_json::from_slice(bytes)
            .map_err(|error| OwnershipImportError::Decode(error.to_string()))
    }

    pub fn dry_run(
        &self,
        context: &OwnershipImportContext,
    ) -> Result<CoreMatrixImportPlan, OwnershipImportError> {
        self.validate_envelope()?;
        let catalog: ExternalReferenceCatalog = decode_context(
            &context.external_reference_catalog,
            "external reference catalog",
        )?;
        let baseline: RevisionBaseline =
            decode_context(&context.revision_baseline, "revision baseline")?;
        validate_bound_inputs(
            &self.source,
            &catalog,
            &baseline,
            &context.execution_profile,
        )?;
        validate_section(&self.mfg_domain, "mfg", MFG_TABLES)?;
        validate_section(&self.core_matrix_domain, "core", &core_table_names())?;
        validate_reconciliation(&self.reconciliation)?;
        validate_reconciliation_source_projection(&self.mfg_domain, &self.reconciliation)?;
        validate_excluded(&self.excluded)?;
        verify_embedded(
            &self.mfg_domain,
            "section_digest",
            "cowd.ownership.section.v1",
        )?;
        verify_embedded(
            &self.core_matrix_domain,
            "section_digest",
            "cowd.ownership.section.v1",
        )?;
        verify_embedded(self, "whole_snapshot_digest", "cowd.ownership.snapshot.v1")?;
        let all_objects = self
            .mfg_domain
            .objects
            .iter()
            .chain(&self.core_matrix_domain.objects)
            .collect::<Vec<_>>();
        validate_identity_revision_and_references(&all_objects, &catalog, &baseline)?;
        let mut seen = BTreeMap::<String, (String, OwnershipImportRevision)>::new();
        let mut records = Vec::with_capacity(self.core_matrix_domain.objects.len());
        for object in &self.core_matrix_domain.objects {
            validate_core_object(object)?;
            let stable_ref = stable_ref(object)?;
            if let Some((previous_digest, previous_revision)) = seen.insert(
                stable_ref.clone(),
                (object.payload_digest.clone(), object.revision.clone()),
            ) {
                if previous_digest != object.payload_digest || previous_revision != object.revision
                {
                    return Err(invalid(format!(
                        "duplicate stable id `{stable_ref}` has divergent payload or revision"
                    )));
                }
                continue;
            }
            records.push(ImportedCoreMatrixRecord {
                table: object.source_table.clone(),
            });
        }
        Ok(CoreMatrixImportPlan {
            source: self.source.clone(),
            whole_snapshot_digest: self.whole_snapshot_digest.clone(),
            section_digest: self.core_matrix_domain.section_digest.clone(),
            records,
            objects: self.core_matrix_domain.objects.clone(),
        })
    }

    fn validate_envelope(&self) -> Result<(), OwnershipImportError> {
        if self.contract_version != OWNERSHIP_CONTRACT_VERSION_V1 || self.source.app_id != "mfg" {
            return Err(invalid("contract_version/app_id mismatch"));
        }
        if self.source.source_version.trim().is_empty()
            || self.source.schema_version == 0
            || self.source.maintenance_fence_id.trim().is_empty()
            || self.source.expected_legacy_schema_version == 0
        {
            return Err(invalid(
                "source metadata contains an empty or zero required value",
            ));
        }
        if self.source.ownership_contract_digest != OWNERSHIP_CONTRACT_DIGEST_V1 {
            return Err(invalid("ownership contract digest mismatch"));
        }
        if self.source.execution_profile_digest != OWNERSHIP_EXECUTION_PROFILE_DIGEST_V1 {
            return Err(invalid("execution profile digest mismatch"));
        }
        if self.source.legacy_schema.id != 1
            || self.source.schema_version != self.source.expected_legacy_schema_version
            || self.source.legacy_schema.schema_version
                != self.source.expected_legacy_schema_version
            || self.source.legacy_schema.disposition != "validate_and_record_never_copy"
        {
            return Err(invalid("legacy schema binding mismatch"));
        }
        validate_utc(&self.source.exported_at, "source.exported_at")?;
        validate_utc(
            &self.source.legacy_schema.updated_at,
            "source.legacy_schema.updated_at",
        )?;
        validate_digest(&self.source.external_catalog_digest)?;
        validate_digest(&self.source.revision_baseline_digest)?;
        validate_digest(&self.whole_snapshot_digest)
    }
}

fn validate_section(
    section: &OwnershipImportSection,
    owner: &str,
    tables: &[&str],
) -> Result<(), OwnershipImportError> {
    if section.owner != owner || section.object_count != section.objects.len() as u64 {
        return Err(invalid(format!(
            "{owner} section owner/object_count mismatch"
        )));
    }
    validate_digest(&section.section_digest)?;
    for object in &section.objects {
        if !tables.contains(&object.source_table.as_str()) {
            return Err(invalid(format!(
                "table `{}` is not owned by {owner}",
                object.source_table
            )));
        }
        validate_digest(&object.payload_digest)?;
        require_domain_digest(
            "payload",
            "cowd.ownership.payload.v1",
            &object.payload,
            &object.payload_digest,
        )?;
        validate_reference_order(&object.source_references, "source_references")?;
        validate_reference_order(&object.evidence_references, "evidence_references")?;
        validate_payload_fields(object)?;
    }
    Ok(())
}

fn remove_digest_field(value: &mut Value, field: &str) -> Result<(), OwnershipImportError> {
    value
        .as_object_mut()
        .ok_or_else(|| invalid("canonical digest input must be an object"))?
        .remove(field)
        .ok_or_else(|| invalid(format!("canonical digest input is missing `{field}`")))?;
    Ok(())
}

fn validate_excluded(records: &[OwnershipExcludedRecord]) -> Result<(), OwnershipImportError> {
    let expected = BTreeSet::from(["mfg_projection_event", "mfg_live_epoch", "mfg_live_secret"]);
    let actual = records
        .iter()
        .map(|record| record.source_table.as_str())
        .collect::<BTreeSet<_>>();
    if records.len() != 3
        || actual != expected
        || records
            .iter()
            .any(|r| r.reason.trim().is_empty() || r.regeneration.trim().is_empty())
    {
        return Err(invalid(
            "excluded records must be exactly the three regenerable runtime tables",
        ));
    }
    Ok(())
}

fn validate_core_object(object: &OwnershipImportObject) -> Result<(), OwnershipImportError> {
    let (_, stable_fields, fields) = table_spec(&object.source_table)
        .ok_or_else(|| invalid(format!("unknown Core table `{}`", object.source_table)))?;
    exact_keys(&object.payload, fields, "payload")?;
    for field in *stable_fields {
        if !object.payload.contains_key(*field) {
            return Err(invalid(format!("stable id field `{field}` is missing")));
        }
    }
    Ok(())
}

fn decode_context<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    name: &str,
) -> Result<T, OwnershipImportError> {
    if bytes.is_empty() {
        return Err(invalid(format!("{name} is required")));
    }
    serde_json::from_slice(bytes).map_err(|error| invalid(format!("{name}: {error}")))
}

fn validate_bound_inputs(
    source: &OwnershipImportSource,
    catalog: &ExternalReferenceCatalog,
    baseline: &RevisionBaseline,
    profile_bytes: &[u8],
) -> Result<(), OwnershipImportError> {
    if catalog.schema != "ExternalReferenceCatalogV1" || catalog.owner.trim().is_empty() {
        return Err(invalid("invalid external catalog envelope"));
    }
    validate_utc(&catalog.exported_at, "external catalog exported_at")?;
    let catalog_value: Value =
        serde_json::from_slice(&source_bytes(catalog)?).map_err(|e| invalid(e.to_string()))?;
    verify_value_embedded(
        &catalog_value,
        "digest",
        "cowd.ownership.external-catalog.v1",
    )?;
    if source.external_catalog_digest != catalog.digest {
        return Err(invalid("external catalog digest mismatch"));
    }
    if baseline.schema != "RevisionBaselineCatalogV1"
        || baseline.owner.trim().is_empty()
        || baseline.initial != baseline.entries.is_empty()
    {
        return Err(invalid("invalid revision baseline envelope"));
    }
    validate_utc(&baseline.exported_at, "revision baseline exported_at")?;
    let baseline_value: Value =
        serde_json::from_slice(&source_bytes(baseline)?).map_err(|e| invalid(e.to_string()))?;
    verify_value_embedded(
        &baseline_value,
        "digest",
        "cowd.ownership.revision-baseline.v1",
    )?;
    if source.revision_baseline_digest != baseline.digest {
        return Err(invalid("revision baseline digest mismatch"));
    }
    let supplied: Value = decode_context(profile_bytes, "execution profile")?;
    let frozen: Value =
        serde_json::from_str(EXECUTION_PROFILE).map_err(|e| invalid(e.to_string()))?;
    if supplied != frozen {
        return Err(invalid("execution profile does not equal frozen profile"));
    }
    verify_value_embedded(&supplied, "digest", "cowd.ownership.execution-profile.v1")?;
    Ok(())
}

fn source_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, OwnershipImportError> {
    serde_json::to_vec(value).map_err(|e| invalid(e.to_string()))
}

fn validate_payload_fields(object: &OwnershipImportObject) -> Result<(), OwnershipImportError> {
    let mapping: Value = serde_json::from_str(FIELD_MAPPING).map_err(|e| invalid(e.to_string()))?;
    let fields = mapping
        .pointer(&format!("/tables/{}/fields", object.source_table))
        .and_then(Value::as_object)
        .ok_or_else(|| invalid(format!("no field mapping for {}", object.source_table)))?;
    let expected = fields.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let actual = object
        .payload
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if expected != actual {
        return Err(invalid(format!(
            "{} payload has unknown/missing fields",
            object.source_table
        )));
    }
    for (field, rule) in fields {
        let value = &object.payload[field];
        if rule["nullable"] == false && value.is_null() {
            return Err(invalid(format!(
                "{}.{} cannot be null",
                object.source_table, field
            )));
        }
        if !value.is_null() {
            let valid_type = match rule["source_type"].as_str().unwrap_or_default() {
                "TEXT" => value.is_string(),
                "INTEGER" => value.as_i64().is_some(),
                "REAL" => value.as_f64().is_some_and(f64::is_finite),
                _ => false,
            };
            if !valid_type {
                return Err(invalid(format!(
                    "{}.{} has wrong lossless source type",
                    object.source_table, field
                )));
            }
        }
        if field.ends_with("_json") && !value.is_null() {
            let text = value.as_str().ok_or_else(|| {
                invalid(format!(
                    "{}.{} must be JSON text",
                    object.source_table, field
                ))
            })?;
            let _: Value = serde_json::from_str(text).map_err(|e| {
                invalid(format!(
                    "{}.{} invalid JSON: {e}",
                    object.source_table, field
                ))
            })?;
        }
        if field.ends_with("_at") && !value.is_null() {
            validate_utc(
                value
                    .as_str()
                    .ok_or_else(|| invalid("timestamp must be text"))?,
                &format!("{}.{}", object.source_table, field),
            )?;
        }
    }
    Ok(())
}

fn validate_identity_revision_and_references(
    objects: &[&OwnershipImportObject],
    catalog: &ExternalReferenceCatalog,
    baseline: &RevisionBaseline,
) -> Result<(), OwnershipImportError> {
    let identity: Value = serde_json::from_str(IDENTITY).map_err(|e| invalid(e.to_string()))?;
    let revisions: Value =
        serde_json::from_str(REVISION_PROJECTION).map_err(|e| invalid(e.to_string()))?;
    let encoding: Value =
        serde_json::from_str(REFERENCE_ENCODING).map_err(|e| invalid(e.to_string()))?;
    let extractors = encoding["column_reference_edges"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(
            encoding["json_reference_edges"]
                .as_array()
                .into_iter()
                .flatten(),
        )
        .filter_map(|entry| entry["extractor_id"].as_str().map(|id| (id, entry)))
        .collect::<BTreeMap<_, _>>();
    let mut internal = BTreeMap::new();
    for object in objects {
        if let Some(previous) = internal.insert(object.stable_id.as_str(), *object) {
            if previous.payload_digest != object.payload_digest
                || previous.revision != object.revision
            {
                return Err(invalid(
                    "duplicate stable_id has divergent payload/revision",
                ));
            }
        }
    }
    let external = catalog
        .entries
        .iter()
        .map(|entry| (entry.stable_id.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    if external.len() != catalog.entries.len() {
        return Err(invalid("duplicate external stable_id"));
    }
    for entry in &catalog.entries {
        if !entry
            .stable_id
            .starts_with(&format!("{}:", entry.aggregate_type))
        {
            return Err(invalid("external catalog aggregate/stable_id mismatch"));
        }
        if let Some(digest) = &entry.payload_digest {
            validate_digest(digest)?;
        }
    }
    for object in objects {
        let key_fields = identity
            .pointer(&format!("/tables/{}/key_fields", object.source_table))
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("identity rule missing"))?;
        let mut key = serde_json::Map::new();
        for field in key_fields {
            let field = field
                .as_str()
                .ok_or_else(|| invalid("identity key field invalid"))?;
            let value = object
                .payload
                .get(field)
                .ok_or_else(|| invalid(format!("identity field {field} missing")))?;
            if value.is_null() {
                return Err(invalid("identity field cannot be null"));
            }
            key.insert(field.to_string(), value.clone());
        }
        let expected_id = format!(
            "{}:{}",
            object.source_table,
            base64url_no_pad(canonical_json(&Value::Object(key))?.as_bytes())
        );
        if object.stable_id != expected_id {
            return Err(invalid(format!(
                "{} stable_id mismatch",
                object.source_table
            )));
        }
        let rule = revisions
            .pointer(&format!("/tables/{}", object.source_table))
            .ok_or_else(|| invalid("revision rule missing"))?;
        let projection_fields = rule["projection_key_fields"]
            .as_array()
            .ok_or_else(|| invalid("projection fields missing"))?;
        let mut projection_key = serde_json::Map::new();
        for field in projection_fields {
            let field = field
                .as_str()
                .ok_or_else(|| invalid("projection field invalid"))?;
            projection_key.insert(
                field.into(),
                object
                    .payload
                    .get(field)
                    .ok_or_else(|| invalid("projection value missing"))?
                    .clone(),
            );
        }
        let expected_projection = format!(
            "{}.projection:{}",
            object.source_table,
            base64url_no_pad(canonical_json(&Value::Object(projection_key))?.as_bytes())
        );
        if object.revision.projection_key != expected_projection {
            return Err(invalid("revision projection_key mismatch"));
        }
        let axis_fields = rule["ordered_revision_axis"]
            .as_array()
            .ok_or_else(|| invalid("revision axis rule missing"))?;
        let expected_axis = axis_fields
            .iter()
            .map(|entry| {
                object
                    .payload
                    .get(entry["field"].as_str().unwrap_or_default())
                    .cloned()
                    .ok_or_else(|| invalid("revision axis value missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if object.revision.axis != expected_axis {
            return Err(invalid("revision axis mismatch"));
        }
        let context_fields = rule["immutable_context_fields"]
            .as_array()
            .ok_or_else(|| invalid("revision context rule missing"))?;
        let expected_context = context_fields
            .iter()
            .map(|entry| {
                let field = entry["field"]
                    .as_str()
                    .ok_or_else(|| invalid("context field invalid"))?;
                Ok((
                    field.to_string(),
                    object
                        .payload
                        .get(field)
                        .cloned()
                        .ok_or_else(|| invalid("context value missing"))?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, OwnershipImportError>>()?;
        if object.revision.context != expected_context {
            return Err(invalid("revision context mismatch"));
        }
        require_domain_digest(
            "revision context",
            "cowd.ownership.revision-context.v1",
            &object.revision.context,
            &object.revision.context_digest,
        )?;
        for (references, destination) in [
            (&object.source_references, "source_references"),
            (&object.evidence_references, "evidence_references"),
        ] {
            for reference in references {
                if !reference
                    .stable_id
                    .starts_with(&format!("{}:", reference.aggregate_type))
                {
                    return Err(invalid("reference aggregate/stable_id mismatch"));
                }
                let extractor = extractors
                    .get(reference.source.extractor_id.as_str())
                    .ok_or_else(|| invalid("unknown reference extractor"))?;
                if extractor["destination_field"] != destination
                    || extractor["source"]["table"] != object.source_table
                    || extractor["source"]["field"] != reference.source.field
                    || reference.source.table != object.source_table
                    || extractor["extractor_id"] != reference.source.extractor_id
                    || extractor["aggregate_type"] != reference.aggregate_type
                {
                    return Err(invalid("reference extractor/source mismatch"));
                }
                let (revision, digest) =
                    if let Some(target) = internal.get(reference.stable_id.as_str()) {
                        (
                            Some(
                                serde_json::to_value(&target.revision)
                                    .map_err(|e| invalid(e.to_string()))?,
                            ),
                            Some(target.payload_digest.clone()),
                        )
                    } else if let Some(target) = external.get(reference.stable_id.as_str()) {
                        (target.revision.clone(), target.payload_digest.clone())
                    } else {
                        return Err(invalid(format!(
                            "dangling reference {}",
                            reference.stable_id
                        )));
                    };
                if reference.revision != revision || reference.payload_digest != digest {
                    return Err(invalid("reference copied revision/payload digest mismatch"));
                }
            }
        }
    }
    validate_revision_baseline(objects, baseline)
}

fn validate_revision_baseline(
    objects: &[&OwnershipImportObject],
    baseline: &RevisionBaseline,
) -> Result<(), OwnershipImportError> {
    let mut seen = BTreeSet::new();
    for entry in &baseline.entries {
        if !seen.insert((&entry.aggregate_type, &entry.projection_key)) {
            return Err(invalid("duplicate revision baseline entry"));
        }
        validate_digest(&entry.context_digest)?;
        let matching = objects
            .iter()
            .filter(|object| {
                object.source_table == entry.aggregate_type
                    && object.revision.projection_key == entry.projection_key
            })
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Err(invalid("revision baseline target missing"));
        }
        for object in matching {
            if object.revision.context_digest != entry.context_digest
                || compare_axis(&object.revision.axis, &entry.axis_max)? == std::cmp::Ordering::Less
            {
                return Err(invalid("revision rollback/context mismatch"));
            }
        }
    }
    Ok(())
}

fn compare_axis(
    left: &[Value],
    right: &[Value],
) -> Result<std::cmp::Ordering, OwnershipImportError> {
    if left.len() != right.len() {
        return Err(invalid("revision axes incomparable"));
    }
    for (left, right) in left.iter().zip(right) {
        let order = match (left, right) {
            (Value::Number(a), Value::Number(b)) => {
                a.as_i64().zip(b.as_i64()).map(|(a, b)| a.cmp(&b))
            }
            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
            _ => None,
        }
        .ok_or_else(|| invalid("revision axes incomparable"))?;
        if order != std::cmp::Ordering::Equal {
            return Ok(order);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

fn validate_reference_order(
    references: &[OwnershipReference],
    name: &str,
) -> Result<(), OwnershipImportError> {
    let encoded = references
        .iter()
        .map(|reference| {
            serde_json::to_value(reference)
                .map_err(|e| invalid(e.to_string()))
                .and_then(|value| canonical_json(&value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if encoded
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        return Err(invalid(format!(
            "{name} must use unique canonical UTF-8 byte order"
        )));
    }
    Ok(())
}

fn validate_reconciliation(value: &OwnershipReconciliation) -> Result<(), OwnershipImportError> {
    macro_rules! records {
        ($records:expr,$domain:literal) => {{
            let mut previous: Option<String> = None;
            for record in $records {
                let serialized =
                    serde_json::to_value(record).map_err(|e| invalid(e.to_string()))?;
                let stable = serialized["stable_ref"]
                    .as_str()
                    .ok_or_else(|| invalid("reconciliation stable_ref missing"))?
                    .to_string();
                if previous.as_ref().is_some_and(|value| value >= &stable) {
                    return Err(invalid("reconciliation array order/duplicate"));
                }
                previous = Some(stable);
                verify_value_embedded(&serialized, "payload_digest", $domain)?;
            }
        }};
    }
    records!(&value.pending_outbox, "cowd.ownership.reconcile.outbox.v1");
    records!(
        &value.command_receipts,
        "cowd.ownership.reconcile.command-receipt.v1"
    );
    records!(
        &value.mutation_receipts,
        "cowd.ownership.reconcile.mutation-receipt.v1"
    );
    records!(
        &value.mutation_receipt_aliases,
        "cowd.ownership.reconcile.mutation-alias.v1"
    );
    records!(
        &value.mutation_receipt_repairs,
        "cowd.ownership.reconcile.mutation-repair.v1"
    );
    if value.pending_outbox.iter().any(|record| {
        !matches!(
            record.status.as_str(),
            "pending" | "retry_wait" | "processing"
        )
    }) {
        return Err(invalid("invalid pending outbox status"));
    }
    if value.command_receipts.iter().any(|record| {
        record.status != "recorded"
            || record.stable_ref != format!("{}\u{1f}{}", record.domain, record.idempotency_key)
    }) {
        return Err(invalid("invalid command receipt mapping"));
    }
    if value.mutation_receipts.iter().any(|record| {
        record.stable_ref != record.receipt_id
            || validate_digest(&record.mutation_payload_digest).is_err()
            || !matches!(
                record.status.as_str(),
                "accepted"
                    | "effect_started"
                    | "effect_retryable"
                    | "business_completed"
                    | "preview"
                    | "completed"
            )
    }) {
        return Err(invalid("invalid mutation receipt mapping"));
    }
    let receipts = value
        .mutation_receipts
        .iter()
        .map(|record| (&record.receipt_id, &record.payload_digest))
        .collect::<BTreeMap<_, _>>();
    if value.mutation_receipt_aliases.iter().any(|record| {
        record.status != "bound"
            || record.stable_ref != record.legacy_idempotency_key
            || receipts.get(&record.canonical_receipt_stable_id)
                != Some(&&record.canonical_receipt_payload_digest)
    }) {
        return Err(invalid("invalid mutation alias mapping"));
    }
    if value.mutation_receipt_repairs.iter().any(|record| {
        record.status != "conflict_preserved" || record.stable_ref != record.report_id
    }) {
        return Err(invalid("invalid mutation repair mapping"));
    }
    for timestamp in value
        .command_receipts
        .iter()
        .map(|record| record.created_at.as_str())
        .chain(
            value
                .mutation_receipts
                .iter()
                .flat_map(|record| [record.created_at.as_str(), record.updated_at.as_str()]),
        )
        .chain(
            value
                .mutation_receipt_aliases
                .iter()
                .map(|record| record.created_at.as_str()),
        )
        .chain(
            value
                .mutation_receipt_repairs
                .iter()
                .map(|record| record.created_at.as_str()),
        )
    {
        validate_utc(timestamp, "reconciliation timestamp")?;
    }
    verify_embedded(value, "set_digest", "cowd.ownership.reconciliation.v1")
}

fn validate_reconciliation_source_projection(
    section: &OwnershipImportSection,
    reconciliation: &OwnershipReconciliation,
) -> Result<(), OwnershipImportError> {
    let receipts = reconciliation
        .mutation_receipts
        .iter()
        .map(|record| (record.receipt_id.as_str(), record.payload_digest.as_str()))
        .collect::<BTreeMap<_, _>>();
    let actual = [
        (
            "mfg_report_delivery_review_effect_outbox",
            records_without_digest(&reconciliation.pending_outbox)?,
        ),
        (
            "mfg_command_receipt",
            records_without_digest(&reconciliation.command_receipts)?,
        ),
        (
            "mfg_mutation_receipt",
            records_without_digest(&reconciliation.mutation_receipts)?,
        ),
        (
            "mfg_mutation_receipt_alias",
            records_without_digest(&reconciliation.mutation_receipt_aliases)?,
        ),
        (
            "mfg_mutation_receipt_repair_report",
            records_without_digest(&reconciliation.mutation_receipt_repairs)?,
        ),
    ]
    .into_iter()
    .collect::<BTreeMap<_, _>>();
    let mut projected = BTreeMap::<&str, BTreeMap<String, Value>>::new();
    for object in &section.objects {
        let table = object.source_table.as_str();
        if !actual.contains_key(table) {
            continue;
        }
        let payload = &object.payload;
        let projection = match table {
            "mfg_report_delivery_review_effect_outbox" => {
                let status = source_string(payload, "status")?;
                if status == "completed" {
                    None
                } else {
                    if !matches!(status, "pending" | "retry_wait" | "processing") {
                        return Err(invalid("unclassified outbox reconciliation status"));
                    }
                    Some(serde_json::json!({
                        "stable_ref": source_string(payload, "effect_id")?,
                        "status": status,
                        "action": source_string(payload, "action")?,
                        "effect_key": source_string(payload, "effect_key")?,
                        "attempt_count": source_u64(payload, "attempt_count")?,
                        "next_attempt_at": source_optional_string(payload, "next_attempt_at")?,
                        "last_error": source_optional_string(payload, "last_error")?,
                        "receipt_ref": source_optional_string(payload, "receipt_ref")?,
                        "payload": source_json(payload, "payload_json")?,
                    }))
                }
            }
            "mfg_command_receipt" => {
                let domain = source_string(payload, "domain")?;
                let idempotency_key = source_string(payload, "idempotency_key")?;
                Some(serde_json::json!({
                    "stable_ref": format!("{domain}\u{1f}{idempotency_key}"),
                    "status": "recorded",
                    "domain": domain,
                    "idempotency_key": idempotency_key,
                    "subject_ref": source_string(payload, "subject_ref")?,
                    "receipt": source_json(payload, "receipt_json")?,
                    "created_at": source_string(payload, "created_at")?,
                }))
            }
            "mfg_mutation_receipt" => Some(serde_json::json!({
                "stable_ref": source_string(payload, "receipt_id")?,
                "status": source_string(payload, "status")?,
                "receipt_id": source_string(payload, "receipt_id")?,
                "idempotency_key": source_string(payload, "idempotency_key")?,
                "actor_principal": source_string(payload, "actor_principal")?,
                "action_id": source_string(payload, "action_id")?,
                "resource_ref": source_string(payload, "resource_ref")?,
                "expected_revision": source_value(payload, "expected_revision")?,
                "result_revision": source_value(payload, "result_revision")?,
                "mutation_payload_digest": source_string(payload, "payload_digest")?,
                "lease_token": source_string(payload, "lease_token")?,
                "response": source_json(payload, "response_json")?,
                "contract_version": source_string(payload, "contract_version")?,
                "created_at": source_string(payload, "created_at")?,
                "updated_at": source_string(payload, "updated_at")?,
            })),
            "mfg_mutation_receipt_alias" => {
                let legacy = source_string(payload, "legacy_idempotency_key")?;
                let receipt_id = source_string(payload, "receipt_id")?;
                let digest = receipts.get(receipt_id).ok_or_else(|| {
                    invalid("mutation alias source references missing canonical receipt")
                })?;
                Some(serde_json::json!({
                    "stable_ref": legacy,
                    "status": "bound",
                    "legacy_idempotency_key": legacy,
                    "canonical_receipt_stable_id": receipt_id,
                    "canonical_receipt_payload_digest": digest,
                    "created_at": source_string(payload, "created_at")?,
                }))
            }
            "mfg_mutation_receipt_repair_report" => {
                let existing = source_json(payload, "existing_receipt_json")?;
                let incoming = source_json(payload, "incoming_receipt_json")?;
                let mut conflict_fields = Vec::new();
                collect_json_diff_paths("", &existing, &incoming, &mut conflict_fields);
                conflict_fields.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
                let report_id = source_string(payload, "report_id")?;
                Some(serde_json::json!({
                    "stable_ref": report_id,
                    "status": "conflict_preserved",
                    "report_id": report_id,
                    "idempotency_key": source_string(payload, "idempotency_key")?,
                    "existing_receipt": existing,
                    "incoming_receipt": incoming,
                    "existing_digest": domain_digest("cowd.ownership.repair-side.v1", &existing)?,
                    "incoming_digest": domain_digest("cowd.ownership.repair-side.v1", &incoming)?,
                    "conflict_fields": conflict_fields,
                    "created_at": source_string(payload, "created_at")?,
                }))
            }
            _ => unreachable!(),
        };
        if let Some(projection) = projection {
            let stable = projection["stable_ref"]
                .as_str()
                .ok_or_else(|| invalid("projected reconciliation stable_ref missing"))?
                .to_owned();
            if projected
                .entry(table)
                .or_default()
                .insert(stable, projection)
                .is_some()
            {
                return Err(invalid("duplicate reconciliation source projection"));
            }
        }
    }
    for (table, actual_records) in actual {
        if projected.remove(table).unwrap_or_default() != actual_records {
            return Err(invalid(format!(
                "reconciliation records do not exactly project source table {table}"
            )));
        }
    }
    Ok(())
}

fn records_without_digest<T: Serialize>(
    records: &[T],
) -> Result<BTreeMap<String, Value>, OwnershipImportError> {
    let mut values = BTreeMap::new();
    for record in records {
        let mut value = serde_json::to_value(record).map_err(|error| invalid(error.to_string()))?;
        let stable_ref = value["stable_ref"]
            .as_str()
            .ok_or_else(|| invalid("reconciliation stable_ref missing"))?
            .to_owned();
        remove_digest_field(&mut value, "payload_digest")?;
        if values.insert(stable_ref, value).is_some() {
            return Err(invalid("duplicate reconciliation stable_ref"));
        }
    }
    Ok(values)
}

fn source_value(
    payload: &BTreeMap<String, Value>,
    field: &str,
) -> Result<Value, OwnershipImportError> {
    payload
        .get(field)
        .cloned()
        .ok_or_else(|| invalid(format!("reconciliation source field {field} missing")))
}

fn source_string<'a>(
    payload: &'a BTreeMap<String, Value>,
    field: &str,
) -> Result<&'a str, OwnershipImportError> {
    payload.get(field).and_then(Value::as_str).ok_or_else(|| {
        invalid(format!(
            "reconciliation source field {field} is not a string"
        ))
    })
}

fn source_optional_string<'a>(
    payload: &'a BTreeMap<String, Value>,
    field: &str,
) -> Result<Option<&'a str>, OwnershipImportError> {
    match payload.get(field) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value)),
        _ => Err(invalid(format!(
            "reconciliation source field {field} is not a nullable string"
        ))),
    }
}

fn source_u64(payload: &BTreeMap<String, Value>, field: &str) -> Result<u64, OwnershipImportError> {
    payload
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("reconciliation source field {field} is not a u64")))
}

fn source_json(
    payload: &BTreeMap<String, Value>,
    field: &str,
) -> Result<Value, OwnershipImportError> {
    let encoded = source_string(payload, field)?;
    serde_json::from_str(encoded)
        .map_err(|error| invalid(format!("reconciliation source field {field}: {error}")))
}

fn collect_json_diff_paths(prefix: &str, left: &Value, right: &Value, output: &mut Vec<String>) {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => {
            let keys = left.keys().chain(right.keys()).collect::<BTreeSet<_>>();
            for key in keys {
                let escaped = key.replace('~', "~0").replace('/', "~1");
                let path = format!("{prefix}/{escaped}");
                match (left.get(key), right.get(key)) {
                    (Some(left), Some(right)) => {
                        collect_json_diff_paths(&path, left, right, output);
                    }
                    _ => output.push(path),
                }
            }
        }
        (Value::Array(left), Value::Array(right)) => {
            for index in 0..left.len().max(right.len()) {
                let path = format!("{prefix}/{index}");
                match (left.get(index), right.get(index)) {
                    (Some(left), Some(right)) => {
                        collect_json_diff_paths(&path, left, right, output);
                    }
                    _ => output.push(path),
                }
            }
        }
        _ if left != right => output.push(if prefix.is_empty() {
            "/".to_owned()
        } else {
            prefix.to_owned()
        }),
        _ => {}
    }
}

fn verify_embedded(
    value: &impl Serialize,
    field: &str,
    domain: &str,
) -> Result<(), OwnershipImportError> {
    let value = serde_json::to_value(value).map_err(|e| invalid(e.to_string()))?;
    verify_value_embedded(&value, field, domain)
}
fn verify_value_embedded(
    value: &Value,
    field: &str,
    domain: &str,
) -> Result<(), OwnershipImportError> {
    let expected = value[field]
        .as_str()
        .ok_or_else(|| invalid(format!("{field} missing")))?;
    let mut body = value.clone();
    remove_digest_field(&mut body, field)?;
    require_domain_digest(field, domain, &body, expected)
}
fn require_domain_digest(
    name: &str,
    domain: &str,
    value: &impl Serialize,
    expected: &str,
) -> Result<(), OwnershipImportError> {
    validate_digest(expected)?;
    let actual = domain_digest(domain, value)?;
    if actual == expected {
        Ok(())
    } else {
        Err(invalid(format!("{name} digest mismatch")))
    }
}
fn domain_digest(domain: &str, value: &impl Serialize) -> Result<String, OwnershipImportError> {
    let value = serde_json::to_value(value).map_err(|e| invalid(e.to_string()))?;
    let mut hash = Sha256::new();
    hash.update(domain.as_bytes());
    hash.update([0]);
    hash.update(canonical_json(&value)?.as_bytes());
    Ok(format!("sha256:{:x}", hash.finalize()))
}
fn canonical_json(value: &Value) -> Result<String, OwnershipImportError> {
    Ok(match value {
        Value::Null => "null".into(),
        Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).map_err(|e| invalid(e.to_string()))?
        }
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>, _>>()?
                .join(",")
        ),
        Value::Object(values) => {
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key).map_err(|e| invalid(e.to_string()))?,
                        canonical_json(value)?
                    )))
                    .collect::<Result<Vec<_>, OwnershipImportError>>()?
                    .join(",")
            )
        }
    })
}
fn base64url_no_pad(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let bits = ((chunk[0] as u32) << 16)
            | ((chunk.get(1).copied().unwrap_or_default() as u32) << 8)
            | chunk.get(2).copied().unwrap_or_default() as u32;
        out.push(A[((bits >> 18) & 63) as usize] as char);
        out.push(A[((bits >> 12) & 63) as usize] as char);
        if chunk.len() > 1 {
            out.push(A[((bits >> 6) & 63) as usize] as char)
        }
        if chunk.len() > 2 {
            out.push(A[(bits & 63) as usize] as char)
        }
    }
    out
}
fn validate_utc(value: &str, name: &str) -> Result<(), OwnershipImportError> {
    if !value.ends_with('Z') || value.parse::<DateTime<Utc>>().is_err() {
        Err(invalid(format!("{name} must be RFC3339 UTC Z")))
    } else {
        Ok(())
    }
}

fn stable_ref(object: &OwnershipImportObject) -> Result<String, OwnershipImportError> {
    if object
        .stable_id
        .starts_with(&format!("{}:", object.source_table))
    {
        Ok(object.stable_id.clone())
    } else {
        Err(invalid("stable_id aggregate type mismatch"))
    }
}

fn table_spec(
    table: &str,
) -> Option<&'static (
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
)> {
    CORE_TABLES.iter().find(|spec| spec.0 == table)
}
fn core_table_names() -> Vec<&'static str> {
    CORE_TABLES.iter().map(|spec| spec.0).collect()
}

fn exact_keys(
    map: &BTreeMap<String, Value>,
    expected: &[&str],
    kind: &str,
) -> Result<(), OwnershipImportError> {
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(invalid(format!(
            "{kind} fields differ: expected {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn validate_digest(value: &str) -> Result<(), OwnershipImportError> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid(format!("invalid digest `{value}`")));
    }
    Ok(())
}
pub fn ownership_import_digest(value: &impl Serialize) -> Result<String, OwnershipImportError> {
    let value = serde_json::to_value(value).map_err(|e| invalid(e.to_string()))?;
    let bytes = serde_json::to_vec(&canonical_value(&value)).map_err(|e| invalid(e.to_string()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}
fn canonical_value(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonical_value).collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(k, v)| (k.clone(), canonical_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}
fn invalid(message: impl Into<String>) -> OwnershipImportError {
    OwnershipImportError::Invalid(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(comprehensive: bool) -> OwnershipImportContext {
        OwnershipImportContext {
            external_reference_catalog: include_bytes!(
                "../../../../contracts/ownership/v1/golden/external-reference-catalog.json"
            )
            .to_vec(),
            revision_baseline: if comprehensive {
                include_bytes!("../../../../contracts/ownership/v1/golden/revision-baseline-comprehensive.json").to_vec()
            } else {
                include_bytes!(
                    "../../../../contracts/ownership/v1/golden/revision-baseline-empty.json"
                )
                .to_vec()
            },
            execution_profile: include_bytes!(
                "../../../../contracts/ownership/v1/execution-profile.json"
            )
            .to_vec(),
        }
    }

    #[test]
    fn final_minimal_and_comprehensive_goldens_validate() {
        let minimal = MfgOwnershipSplitSnapshotV1::decode_strict(include_bytes!(
            "../../../../contracts/ownership/v1/golden/minimal-snapshot.json"
        ))
        .unwrap();
        assert!(minimal
            .dry_run(&context(false))
            .unwrap()
            .records()
            .is_empty());
        let comprehensive = MfgOwnershipSplitSnapshotV1::decode_strict(include_bytes!(
            "../../../../contracts/ownership/v1/golden/comprehensive-snapshot.json"
        ))
        .unwrap();
        let plan = comprehensive.dry_run(&context(true)).unwrap();
        assert_eq!(plan.records().len(), 19);
        assert_eq!(
            plan.records()
                .iter()
                .map(ImportedCoreMatrixRecord::table)
                .collect::<BTreeSet<_>>()
                .len(),
            19
        );
        let revision_siblings = comprehensive
            .mfg_domain
            .objects
            .iter()
            .filter(|object| object.source_table == "mfg_cockpit_view_version")
            .collect::<Vec<_>>();
        assert_eq!(revision_siblings.len(), 2);
        assert_eq!(revision_siblings[0].source_references.len(), 1);
        assert_eq!(revision_siblings[1].source_references.len(), 1);
        assert_eq!(revision_siblings[0].revision.axis, [Value::from(1)]);
        assert_eq!(revision_siblings[1].revision.axis, [Value::from(2)]);
    }

    #[test]
    fn conflict_references_require_canonical_utf8_byte_order() {
        let snapshot = MfgOwnershipSplitSnapshotV1::decode_strict(include_bytes!(
            "../../../../contracts/ownership/v1/golden/comprehensive-snapshot.json"
        ))
        .unwrap();
        let conflict = snapshot
            .core_matrix_domain
            .objects
            .iter()
            .find(|object| object.source_table == "matrix_entity_conflict_decision")
            .unwrap();
        validate_reference_order(&conflict.source_references, "source_references").unwrap();
        let mut reversed = conflict.source_references.clone();
        reversed.reverse();
        assert!(validate_reference_order(&reversed, "source_references").is_err());
    }

    #[test]
    fn ten_frozen_tamper_classes_fail_closed() {
        for bytes in [
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/catalog-digest-mismatch.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/execution-profile.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/matrix-schema.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/reconciliation.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/reconciliation-object-projection.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/reference-class.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/revision-baseline.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/unknown-contract-version.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/unknown-reconciliation-array.json").as_slice(),
            include_bytes!("../../../../contracts/ownership/v1/golden/tamper/whole-digest-tamper.json").as_slice(),
        ] {
            let wrapper: Value = serde_json::from_slice(bytes).unwrap();
            let snapshot_bytes = serde_json::to_vec(&wrapper["snapshot"]).unwrap();
            if let Ok(snapshot) = MfgOwnershipSplitSnapshotV1::decode_strict(&snapshot_bytes) {
                assert!(snapshot.dry_run(&context(true)).is_err());
            }
        }
    }
}
