use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixConnectorRun,
    MatrixDataPlaneWatermark, MatrixEntity, MatrixEntityConflictDecision,
    MatrixEntityMatchCandidate, MatrixEvidencePacket, MatrixFact, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricSnapshot, MatrixMetricState, MatrixOntologyPack,
    MatrixQualityGateDecision, MatrixRelation, MatrixSourcePack,
};

pub const OWNERSHIP_CONTRACT_DIGEST_V1: &str =
    "sha256:84bd9bb410cb413a7603954af21dbe809b9308ecab86684445c71274de9486e8";
pub const OWNERSHIP_CONTRACT_VERSION_V1: &str = "cowd.ownership-split/v1.1-final";

const FIELD_MAPPING: &str = include_str!("../../../../contracts/ownership/v1/field-mapping.json");
const REVISION_PROJECTION: &str =
    include_str!("../../../../contracts/ownership/v1/revision-projection.json");
const REFERENCE_GRAPH: &str = include_str!("../../../../contracts/ownership/v1/reference-graph.json");
const REFERENCE_ENCODING: &str =
    include_str!("../../../../contracts/ownership/v1/reference-encoding.json");
const JSON_SCHEMA_REGISTRY: &str =
    include_str!("../../../../contracts/ownership/v1/json-schema-registry.json");

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
    pub exported_at: DateTime<Utc>,
    pub maintenance_fence_id: String,
    pub ownership_contract_digest: String,
    pub external_reference_catalog_digest: String,
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
    pub stable_id: BTreeMap<String, Value>,
    pub revision: OwnershipImportRevision,
    pub source_references: Vec<OwnershipReference>,
    pub evidence_references: Vec<OwnershipReference>,
    pub payload: BTreeMap<String, Value>,
    pub payload_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipImportRevision {
    pub strategy: String,
    pub authority: Option<OwnershipRevisionAuthority>,
    pub context: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipRevisionAuthority {
    pub field: String,
    #[serde(rename = "type")]
    pub value_type: String,
    pub comparison: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReference {
    pub namespace: String,
    pub aggregate: String,
    pub stable_id: BTreeMap<String, Value>,
    pub revision: Option<Value>,
    pub digest: Option<String>,
    pub source: OwnershipReferenceSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReferenceSource {
    pub table: String,
    pub field: String,
    pub json_pointer: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReconciliation {
    pub pending_outbox: Vec<OwnershipReconcileRecord>,
    pub command_receipts: Vec<OwnershipReconcileRecord>,
    pub mutation_receipts: Vec<OwnershipReconcileRecord>,
    pub mutation_receipt_aliases: Vec<OwnershipReconcileRecord>,
    pub mutation_receipt_repairs: Vec<OwnershipReconcileRecord>,
    pub set_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipReconcileRecord {
    pub stable_ref: String,
    pub payload_digest: String,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipExcludedRecord {
    pub source_table: String,
    pub reason: String,
    pub regeneration: String,
}

#[derive(Debug, Clone, Default)]
pub struct OwnershipImportContext {
    /// Strict `ExternalReferenceCatalogV1` bytes bound by the source envelope.
    pub external_reference_catalog: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalReferenceCatalogV1 {
    schema: String,
    digest: String,
    owner: String,
    exported_at: DateTime<Utc>,
    entries: Vec<ExternalReferenceCatalogEntryV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalReferenceCatalogEntryV1 {
    namespace: String,
    aggregate: String,
    stable_id: BTreeMap<String, Value>,
    revision: Option<Value>,
    digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedMatrixSourceKey {
    pub source_system: String,
    pub source_key: String,
    pub entity_id: String,
    pub source_ref: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedMatrixFact {
    pub fact: MatrixFact,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImportedMatrixOntologyPack {
    pub pack: MatrixOntologyPack,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ImportedCoreMatrixRecord {
    Entity(MatrixEntity),
    EntitySourceKey(ImportedMatrixSourceKey),
    Relation(MatrixRelation),
    Fact(ImportedMatrixFact),
    Attention(MatrixAttentionItem),
    Evidence(MatrixEvidencePacket),
    QualityGate(MatrixQualityGateDecision),
    MetricDefinition(MatrixMetricDefinition),
    MetricState(MatrixMetricState),
    MetricDependency(MatrixMetricDependency),
    ComputeJob(MatrixComputeJob),
    ChangeEvent(MatrixChangeEvent),
    SourcePack(MatrixSourcePack),
    Watermark(MatrixDataPlaneWatermark),
    ConnectorRun(MatrixConnectorRun),
    OntologyPack(ImportedMatrixOntologyPack),
    MatchCandidate(MatrixEntityMatchCandidate),
    ConflictDecision(MatrixEntityConflictDecision),
    MetricSnapshot(MatrixMetricSnapshot),
}

impl ImportedCoreMatrixRecord {
    #[must_use]
    pub fn table(&self) -> &'static str {
        match self {
            Self::Entity(_) => "matrix_entity",
            Self::EntitySourceKey(_) => "matrix_entity_source_key",
            Self::Relation(_) => "matrix_relation",
            Self::Fact(_) => "matrix_fact",
            Self::Attention(_) => "matrix_attention_item",
            Self::Evidence(_) => "matrix_evidence_packet",
            Self::QualityGate(_) => "matrix_quality_gate",
            Self::MetricDefinition(_) => "matrix_metric_definition",
            Self::MetricState(_) => "matrix_metric_state",
            Self::MetricDependency(_) => "matrix_metric_dependency",
            Self::ComputeJob(_) => "matrix_compute_job",
            Self::ChangeEvent(_) => "matrix_change_event",
            Self::SourcePack(_) => "matrix_source_pack",
            Self::Watermark(_) => "matrix_data_plane_watermark",
            Self::ConnectorRun(_) => "matrix_connector_run",
            Self::OntologyPack(_) => "matrix_ontology_pack",
            Self::MatchCandidate(_) => "matrix_entity_match_candidate",
            Self::ConflictDecision(_) => "matrix_entity_conflict_decision",
            Self::MetricSnapshot(_) => "matrix_metric_snapshot",
        }
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
        let catalog = decode_external_catalog(context, &self.source)?;
        validate_section(&self.mfg_domain, "mfg", MFG_TABLES)?;
        validate_section(&self.core_matrix_domain, "core", &core_table_names())?;
        validate_digest(&self.reconciliation.set_digest)?;
        for record in self
            .reconciliation
            .pending_outbox
            .iter()
            .chain(&self.reconciliation.command_receipts)
            .chain(&self.reconciliation.mutation_receipts)
            .chain(&self.reconciliation.mutation_receipt_aliases)
            .chain(&self.reconciliation.mutation_receipt_repairs)
        {
            if record.stable_ref.trim().is_empty() || record.status.trim().is_empty() {
                return Err(invalid(
                    "reconciliation record contains an empty required value",
                ));
            }
            validate_digest(&record.payload_digest)?;
        }
        let reconciliation_value = serde_json::json!({
            "pending_outbox": self.reconciliation.pending_outbox,
            "command_receipts": self.reconciliation.command_receipts,
            "mutation_receipts": self.reconciliation.mutation_receipts,
            "mutation_receipt_aliases": self.reconciliation.mutation_receipt_aliases,
            "mutation_receipt_repairs": self.reconciliation.mutation_receipt_repairs,
        });
        require_domain_digest(
            "reconciliation",
            "cowd.ownership.reconciliation.v1",
            &reconciliation_value,
            &self.reconciliation.set_digest,
        )?;
        validate_excluded(&self.excluded)?;

        let mut mfg_section_value =
            serde_json::to_value(&self.mfg_domain).map_err(|error| invalid(error.to_string()))?;
        remove_digest_field(&mut mfg_section_value, "section_digest")?;
        require_domain_digest(
            "mfg section",
            "cowd.ownership.section.v1",
            &mfg_section_value,
            &self.mfg_domain.section_digest,
        )?;

        let mut section_value = serde_json::to_value(&self.core_matrix_domain)
            .map_err(|error| invalid(error.to_string()))?;
        remove_digest_field(&mut section_value, "section_digest")?;
        require_domain_digest(
            "core section",
            "cowd.ownership.section.v1",
            &section_value,
            &self.core_matrix_domain.section_digest,
        )?;

        let mut whole_value =
            serde_json::to_value(self).map_err(|error| invalid(error.to_string()))?;
        remove_digest_field(&mut whole_value, "whole_snapshot_digest")?;
        require_domain_digest(
            "whole snapshot",
            "cowd.ownership.snapshot.v1",
            &whole_value,
            &self.whole_snapshot_digest,
        )?;

        validate_contract_projection(self)?;
        validate_typed_references(self, &catalog)?;

        let mut seen = BTreeMap::<String, (String, OwnershipImportRevision)>::new();
        let mut records = Vec::with_capacity(self.core_matrix_domain.objects.len());
        for object in &self.core_matrix_domain.objects {
            let record = validate_core_object(object)?;
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
            records.push(record);
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
        {
            return Err(invalid(
                "source metadata contains an empty or zero required value",
            ));
        }
        if self.source.ownership_contract_digest != OWNERSHIP_CONTRACT_DIGEST_V1 {
            return Err(invalid("ownership contract digest mismatch"));
        }
        validate_digest(&self.source.external_reference_catalog_digest)?;
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

fn validate_core_object(
    object: &OwnershipImportObject,
) -> Result<ImportedCoreMatrixRecord, OwnershipImportError> {
    let (_, stable_fields, fields) = table_spec(&object.source_table)
        .ok_or_else(|| invalid(format!("unknown Core table `{}`", object.source_table)))?;
    exact_keys(&object.payload, fields, "payload")?;
    exact_keys(&object.stable_id, stable_fields, "stable_id")?;
    for field in *stable_fields {
        if object.stable_id.get(*field) != object.payload.get(*field) {
            return Err(invalid(format!(
                "stable id field `{field}` differs from payload"
            )));
        }
    }
    typed_record(object)
}

fn typed_record(
    object: &OwnershipImportObject,
) -> Result<ImportedCoreMatrixRecord, OwnershipImportError> {
    macro_rules! json_record {
        ($column:literal, $type:ty, $variant:ident) => {{
            let value = parse_json_column(object, $column)?;
            let typed: $type = serde_json::from_value(value.clone())
                .map_err(|e| invalid(format!("{}/{}: {e}", object.source_table, $column)))?;
            ensure_typed_canonical(&value, &typed, &object.source_table, $column)?;
            validate_projection(object, &value, $column)?;
            ImportedCoreMatrixRecord::$variant(typed)
        }};
    }
    Ok(match object.source_table.as_str() {
        "matrix_entity" => json_record!("entity_json", MatrixEntity, Entity),
        "matrix_entity_source_key" => {
            ImportedCoreMatrixRecord::EntitySourceKey(ImportedMatrixSourceKey {
                source_system: text(object, "source_system")?,
                source_key: text(object, "source_key")?,
                entity_id: text(object, "entity_id")?,
                source_ref: optional_text(object, "source_ref")?,
                created_at: timestamp(object, "created_at")?,
            })
        }
        "matrix_relation" => json_record!("relation_json", MatrixRelation, Relation),
        "matrix_fact" => ImportedCoreMatrixRecord::Fact(ImportedMatrixFact {
            fact: MatrixFact {
                fact_id: text(object, "fact_id")?,
                snapshot_id: text(object, "snapshot_id")?,
                fact_type: text(object, "fact_type")?,
                entity_refs: parse_json_column_as(object, "entity_refs_json")?,
                metric_key: optional_text(object, "metric_key")?,
                dimensions: parse_json_column(object, "dimensions_json")?,
                measures: parse_json_column(object, "measures_json")?,
                event_time: timestamp(object, "event_time")?,
                valid_from: optional_timestamp(object, "valid_from")?,
                valid_to: optional_timestamp(object, "valid_to")?,
                source_ref: optional_text(object, "source_ref")?,
                confidence: number(object, "confidence")? as f32,
                raw_hash: text(object, "raw_hash")?,
            },
            created_at: timestamp(object, "created_at")?,
        }),
        "matrix_attention_item" => json_record!("attention_json", MatrixAttentionItem, Attention),
        "matrix_evidence_packet" => json_record!("packet_json", MatrixEvidencePacket, Evidence),
        "matrix_quality_gate" => json_record!("gate_json", MatrixQualityGateDecision, QualityGate),
        "matrix_metric_definition" => {
            json_record!("definition_json", MatrixMetricDefinition, MetricDefinition)
        }
        "matrix_metric_state" => json_record!("state_json", MatrixMetricState, MetricState),
        "matrix_metric_dependency" => {
            json_record!("dependency_json", MatrixMetricDependency, MetricDependency)
        }
        "matrix_compute_job" => json_record!("job_json", MatrixComputeJob, ComputeJob),
        "matrix_change_event" => json_record!("change_json", MatrixChangeEvent, ChangeEvent),
        "matrix_source_pack" => json_record!("source_pack_json", MatrixSourcePack, SourcePack),
        "matrix_data_plane_watermark" => {
            json_record!("watermark_json", MatrixDataPlaneWatermark, Watermark)
        }
        "matrix_connector_run" => json_record!("run_json", MatrixConnectorRun, ConnectorRun),
        "matrix_ontology_pack" => {
            let value = parse_json_column(object, "pack_json")?;
            let pack: MatrixOntologyPack =
                serde_json::from_value(value.clone()).map_err(|e| invalid(e.to_string()))?;
            ensure_typed_canonical(&value, &pack, &object.source_table, "pack_json")?;
            validate_projection(object, &value, "pack_json")?;
            ImportedCoreMatrixRecord::OntologyPack(ImportedMatrixOntologyPack {
                pack,
                updated_at: timestamp(object, "updated_at")?,
            })
        }
        "matrix_entity_match_candidate" => {
            json_record!("candidate_json", MatrixEntityMatchCandidate, MatchCandidate)
        }
        "matrix_entity_conflict_decision" => json_record!(
            "decision_json",
            MatrixEntityConflictDecision,
            ConflictDecision
        ),
        "matrix_metric_snapshot" => {
            json_record!("snapshot_json", MatrixMetricSnapshot, MetricSnapshot)
        }
        _ => return Err(invalid("unsupported Core table")),
    })
}

fn validate_projection(
    object: &OwnershipImportObject,
    typed: &Value,
    typed_column: &str,
) -> Result<(), OwnershipImportError> {
    let Some(typed) = typed.as_object() else {
        return Err(invalid(format!(
            "{}.{} must decode to an object",
            object.source_table, typed_column
        )));
    };
    for (column, physical) in &object.payload {
        if column == typed_column {
            continue;
        }
        let logical = match column.as_str() {
            "attributes_json" => "attributes",
            "source_keys_json" => "source_keys",
            "metric_ids_json" => "metric_ids",
            "watermark_json" | "entity_json" | "relation_json" | "attention_json"
            | "packet_json" | "gate_json" | "definition_json" | "state_json"
            | "dependency_json" | "job_json" | "change_json" | "source_pack_json" | "run_json"
            | "pack_json" | "candidate_json" | "decision_json" | "snapshot_json" => continue,
            other if other.ends_with("_json") => continue,
            other => other,
        };
        let Some(expected) = typed.get(logical) else {
            continue;
        };
        let decoded_physical = if column.ends_with("_json") {
            let raw = physical.as_str().ok_or_else(|| {
                invalid(format!(
                    "{}.{} must be encoded JSON text",
                    object.source_table, column
                ))
            })?;
            serde_json::from_str(raw).map_err(|e| {
                invalid(format!(
                    "{}.{} invalid JSON: {e}",
                    object.source_table, column
                ))
            })?
        } else {
            physical.clone()
        };
        if !projection_values_equal(logical, expected, &decoded_physical) {
            return Err(invalid(format!(
                "{}.{} differs from typed {}.{}",
                object.source_table, column, typed_column, logical
            )));
        }
    }
    Ok(())
}

fn projection_values_equal(field: &str, expected: &Value, physical: &Value) -> bool {
    if canonical_value(expected) == canonical_value(physical) {
        return true;
    }
    if matches!(
        field,
        "created_at"
            | "updated_at"
            | "computed_at"
            | "detected_at"
            | "decided_at"
            | "event_time"
            | "valid_from"
            | "valid_to"
    ) {
        return match (expected.as_str(), physical.as_str()) {
            (Some(expected), Some(physical)) => {
                expected.parse::<DateTime<Utc>>().ok() == physical.parse::<DateTime<Utc>>().ok()
            }
            _ => false,
        };
    }
    false
}

fn ensure_typed_canonical<T: Serialize>(
    value: &Value,
    typed: &T,
    table: &str,
    column: &str,
) -> Result<(), OwnershipImportError> {
    let emitted = serde_json::to_value(typed).map_err(|e| invalid(e.to_string()))?;
    if canonical_value(value) != canonical_value(&emitted) {
        return Err(invalid(format!(
            "{table}.{column} contains unknown/missing/defaulted typed fields"
        )));
    }
    Ok(())
}

fn validate_reference_graph(
    objects: &[OwnershipImportObject],
    context: &OwnershipImportContext,
) -> Result<(), OwnershipImportError> {
    let ids = objects
        .iter()
        .filter_map(|o| stable_ref(o).ok())
        .collect::<BTreeSet<_>>();
    let edges = [
        (
            "matrix_entity_source_key",
            "entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_relation",
            "from_entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_relation",
            "to_entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_fact",
            "metric_key",
            "matrix_metric_definition",
            "metric_id",
        ),
        (
            "matrix_evidence_packet",
            "attention_id",
            "matrix_attention_item",
            "attention_id",
        ),
        (
            "matrix_metric_state",
            "metric_id",
            "matrix_metric_definition",
            "metric_id",
        ),
        (
            "matrix_metric_dependency",
            "upstream_metric_id",
            "matrix_metric_definition",
            "metric_id",
        ),
        (
            "matrix_metric_dependency",
            "downstream_metric_id",
            "matrix_metric_definition",
            "metric_id",
        ),
        (
            "matrix_change_event",
            "metric_id",
            "matrix_metric_definition",
            "metric_id",
        ),
        (
            "matrix_connector_run",
            "source_pack_id",
            "matrix_source_pack",
            "source_pack_id",
        ),
        (
            "matrix_entity_match_candidate",
            "left_entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_entity_match_candidate",
            "right_entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_entity_conflict_decision",
            "candidate_id",
            "matrix_entity_match_candidate",
            "candidate_id",
        ),
        (
            "matrix_entity_conflict_decision",
            "survivor_entity_id",
            "matrix_entity",
            "entity_id",
        ),
        (
            "matrix_entity_conflict_decision",
            "retired_entity_id",
            "matrix_entity",
            "entity_id",
        ),
    ];
    for object in objects {
        for (source_table, source_field, target_table, target_field) in edges {
            if object.source_table != source_table {
                continue;
            }
            let Some(value) = object.payload.get(source_field) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            let Some(value) = value.as_str() else {
                return Err(invalid(format!(
                    "reference {source_table}.{source_field} is not text"
                )));
            };
            let target = format!(
                "{target_table}:{}={}",
                target_field,
                canonical_scalar(&Value::String(value.to_string()))?
            );
            if !ids.contains(&target) {
                return Err(invalid(format!(
                    "dangling reference {source_table}.{source_field} -> `{value}`"
                )));
            }
        }
    }
    for object in objects {
        match object.source_table.as_str() {
            "matrix_fact" => {
                for reference in parse_json_column(object, "entity_refs_json")?
                    .as_array()
                    .ok_or_else(|| invalid("matrix_fact.entity_refs_json must be an array"))?
                    .iter()
                    .filter_map(Value::as_str)
                {
                    require_external_or_internal(
                        reference,
                        "matrix_entity",
                        "entity_id",
                        &ids,
                        context,
                    )?;
                }
            }
            "matrix_attention_item" => {
                let value = parse_json_column(object, "attention_json")?;
                if let Some(reference) = value.get("entity_ref").and_then(Value::as_str) {
                    require_external_or_internal(
                        reference,
                        "matrix_entity",
                        "entity_id",
                        &ids,
                        context,
                    )?;
                }
                for reference in string_array_at(&value, "metric_refs")? {
                    require_external_or_internal(
                        reference,
                        "matrix_metric_definition",
                        "metric_id",
                        &ids,
                        context,
                    )?;
                }
                for reference in string_array_at(&value, "linked_changes")? {
                    let id = reference
                        .strip_prefix("matrix:change:")
                        .unwrap_or(reference);
                    require_internal(id, "matrix_change_event", "change_id", &ids)?;
                }
            }
            "matrix_evidence_packet" => {
                let value = parse_json_column(object, "packet_json")?;
                if let Some(reference) = value
                    .pointer("/business_context/entity_ref")
                    .and_then(Value::as_str)
                {
                    require_external_or_internal(
                        reference,
                        "matrix_entity",
                        "entity_id",
                        &ids,
                        context,
                    )?;
                }
                if let Some(source_refs) = value.get("source_refs").and_then(Value::as_array) {
                    for reference in source_refs
                        .iter()
                        .filter_map(|item| item.get("reference").and_then(Value::as_str))
                    {
                        if !context.evidence_references.contains(reference) {
                            return Err(invalid(format!(
                                "unresolved evidence reference `{reference}`"
                            )));
                        }
                    }
                }
            }
            "matrix_metric_state" => {
                require_external_or_internal(
                    &text(object, "entity_scope")?,
                    "matrix_entity",
                    "entity_id",
                    &ids,
                    context,
                )?;
            }
            "matrix_change_event" => {
                require_external_or_internal(
                    &text(object, "entity_ref")?,
                    "matrix_entity",
                    "entity_id",
                    &ids,
                    context,
                )?;
            }
            "matrix_metric_snapshot" => {
                for reference in parse_json_column(object, "metric_ids_json")?
                    .as_array()
                    .ok_or_else(|| {
                        invalid("matrix_metric_snapshot.metric_ids_json must be an array")
                    })?
                    .iter()
                    .filter_map(Value::as_str)
                {
                    require_internal(reference, "matrix_metric_definition", "metric_id", &ids)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn string_array_at<'a>(
    value: &'a Value,
    field: &str,
) -> Result<Vec<&'a str>, OwnershipImportError> {
    value
        .get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid(format!("typed field `{field}` must be an array")))?
        .iter()
        .map(|item| {
            item.as_str()
                .ok_or_else(|| invalid(format!("typed field `{field}` contains a non-string")))
        })
        .collect()
}

fn require_external_or_internal(
    reference: &str,
    table: &str,
    field: &str,
    ids: &BTreeSet<String>,
    context: &OwnershipImportContext,
) -> Result<(), OwnershipImportError> {
    if context.external_references.contains(reference) {
        return Ok(());
    }
    require_internal(reference, table, field, ids)
}

fn require_internal(
    reference: &str,
    table: &str,
    field: &str,
    ids: &BTreeSet<String>,
) -> Result<(), OwnershipImportError> {
    let target = format!(
        "{table}:{field}={}",
        canonical_scalar(&Value::String(reference.to_string()))?
    );
    if ids.contains(&target) {
        Ok(())
    } else {
        Err(invalid(format!(
            "dangling reference to {table}.{field} `{reference}`"
        )))
    }
}

fn stable_ref(object: &OwnershipImportObject) -> Result<String, OwnershipImportError> {
    let mut parts = Vec::new();
    for (field, value) in &object.stable_id {
        parts.push(format!("{field}={}", canonical_scalar(value)?));
    }
    Ok(format!("{}:{}", object.source_table, parts.join("+")))
}

fn canonical_scalar(value: &Value) -> Result<String, OwnershipImportError> {
    if value.is_array() || value.is_object() {
        return Err(invalid("stable id must contain scalar values"));
    }
    serde_json::to_string(value).map_err(|e| invalid(e.to_string()))
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

fn parse_json_column(
    object: &OwnershipImportObject,
    field: &str,
) -> Result<Value, OwnershipImportError> {
    let raw = text(object, field)?;
    serde_json::from_str(&raw).map_err(|e| {
        invalid(format!(
            "{}.{} is invalid JSON: {e}",
            object.source_table, field
        ))
    })
}
fn parse_json_column_as<T: for<'de> Deserialize<'de>>(
    object: &OwnershipImportObject,
    field: &str,
) -> Result<T, OwnershipImportError> {
    serde_json::from_value(parse_json_column(object, field)?).map_err(|e| invalid(e.to_string()))
}
fn text(object: &OwnershipImportObject, field: &str) -> Result<String, OwnershipImportError> {
    object
        .payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            invalid(format!(
                "{}.{} must be non-empty text",
                object.source_table, field
            ))
        })
}
fn optional_text(
    object: &OwnershipImportObject,
    field: &str,
) -> Result<Option<String>, OwnershipImportError> {
    match object.payload.get(field) {
        Some(Value::Null) => Ok(None),
        Some(Value::String(v)) => Ok(Some(v.clone())),
        _ => Err(invalid(format!(
            "{}.{} must be text or null",
            object.source_table, field
        ))),
    }
}
fn number(object: &OwnershipImportObject, field: &str) -> Result<f64, OwnershipImportError> {
    object
        .payload
        .get(field)
        .and_then(Value::as_f64)
        .filter(|v| v.is_finite())
        .ok_or_else(|| {
            invalid(format!(
                "{}.{} must be a finite number",
                object.source_table, field
            ))
        })
}
fn timestamp(
    object: &OwnershipImportObject,
    field: &str,
) -> Result<DateTime<Utc>, OwnershipImportError> {
    text(object, field)?.parse().map_err(|e| {
        invalid(format!(
            "{}.{} invalid timestamp: {e}",
            object.source_table, field
        ))
    })
}
fn optional_timestamp(
    object: &OwnershipImportObject,
    field: &str,
) -> Result<Option<DateTime<Utc>>, OwnershipImportError> {
    optional_text(object, field)?
        .map(|v| {
            v.parse().map_err(|e| {
                invalid(format!(
                    "{}.{} invalid timestamp: {e}",
                    object.source_table, field
                ))
            })
        })
        .transpose()
}

fn unique_non_empty(values: &[String], name: &str) -> Result<(), OwnershipImportError> {
    if values.iter().any(|v| v.trim().is_empty())
        || values.iter().collect::<BTreeSet<_>>().len() != values.len()
    {
        return Err(invalid(format!(
            "{name} contains empty or duplicate values"
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
fn require_digest(
    label: &str,
    value: &impl Serialize,
    expected: &str,
) -> Result<(), OwnershipImportError> {
    let actual = ownership_import_digest(value)?;
    if actual != expected {
        return Err(invalid(format!(
            "{label} digest mismatch: expected {expected}, got {actual}"
        )));
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

    fn empty_snapshot() -> MfgOwnershipSplitSnapshotV1 {
        let mut reconciliation = OwnershipReconciliation {
            pending_outbox: Vec::new(),
            command_receipts: Vec::new(),
            mutation_receipts: Vec::new(),
            set_digest: String::new(),
        };
        reconciliation.set_digest = ownership_import_digest(&serde_json::json!({
            "pending_outbox": [], "command_receipts": [], "mutation_receipts": []
        }))
        .unwrap();
        let mut snapshot = MfgOwnershipSplitSnapshotV1 {
            contract_version: OWNERSHIP_CONTRACT_VERSION_V1.to_string(),
            source: OwnershipImportSource {
                app_id: "mfg".to_string(),
                source_version: "1.0.0".to_string(),
                schema_version: 1,
                exported_at: "2026-08-15T00:00:00Z".parse().unwrap(),
                maintenance_fence_id: "fence-1".to_string(),
                ownership_contract_digest: OWNERSHIP_CONTRACT_DIGEST_V1.to_string(),
            },
            mfg_domain: OwnershipImportSection {
                owner: "mfg".to_string(),
                object_count: 0,
                section_digest: String::new(),
                objects: Vec::new(),
            },
            core_matrix_domain: OwnershipImportSection {
                owner: "core".to_string(),
                object_count: 0,
                section_digest: String::new(),
                objects: Vec::new(),
            },
            reconciliation,
            excluded: vec![
                OwnershipExcludedRecord {
                    source_table: "mfg_projection_event".to_string(),
                    reason: "projection".to_string(),
                    regeneration: "rebuild".to_string(),
                },
                OwnershipExcludedRecord {
                    source_table: "mfg_live_epoch".to_string(),
                    reason: "runtime".to_string(),
                    regeneration: "new epoch".to_string(),
                },
                OwnershipExcludedRecord {
                    source_table: "mfg_live_secret".to_string(),
                    reason: "secret".to_string(),
                    regeneration: "rotate".to_string(),
                },
            ],
            whole_snapshot_digest: String::new(),
        };
        seal(&mut snapshot);
        snapshot
    }

    fn seal(snapshot: &mut MfgOwnershipSplitSnapshotV1) {
        snapshot.mfg_domain.object_count = snapshot.mfg_domain.objects.len() as u64;
        snapshot.core_matrix_domain.object_count = snapshot.core_matrix_domain.objects.len() as u64;
        for section in [&mut snapshot.mfg_domain, &mut snapshot.core_matrix_domain] {
            section.section_digest = ownership_import_digest(&serde_json::json!({
                "owner": section.owner, "object_count": section.object_count, "objects": section.objects
            })).unwrap();
        }
        let mut value = serde_json::to_value(&snapshot).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("whole_snapshot_digest");
        snapshot.whole_snapshot_digest = ownership_import_digest(&value).unwrap();
    }

    fn object(
        table: &str,
        stable_id: BTreeMap<String, Value>,
        payload: BTreeMap<String, Value>,
    ) -> OwnershipImportObject {
        OwnershipImportObject {
            source_table: table.to_string(),
            stable_id,
            revision: OwnershipImportRevision {
                mapping: "none".to_string(),
                value: Value::Null,
            },
            source_references: Vec::new(),
            evidence_references: Vec::new(),
            payload_digest: ownership_import_digest(&payload).unwrap(),
            payload,
        }
    }

    fn entity_object(entity_id: &str) -> OwnershipImportObject {
        let created: DateTime<Utc> = "2025-01-01T00:00:00Z".parse().unwrap();
        let entity = MatrixEntity {
            entity_id: entity_id.to_string(),
            entity_type: "machine".to_string(),
            canonical_key: entity_id.to_string(),
            display_name: entity_id.to_string(),
            source_keys: Vec::new(),
            attributes: serde_json::json!({}),
            confidence: 1.0,
            created_at: created,
            updated_at: created,
        };
        object(
            "matrix_entity",
            BTreeMap::from([("entity_id".into(), Value::String(entity_id.into()))]),
            BTreeMap::from([
                ("entity_id".into(), Value::String(entity.entity_id.clone())),
                (
                    "entity_type".into(),
                    Value::String(entity.entity_type.clone()),
                ),
                (
                    "canonical_key".into(),
                    Value::String(entity.canonical_key.clone()),
                ),
                (
                    "display_name".into(),
                    Value::String(entity.display_name.clone()),
                ),
                ("source_keys_json".into(), Value::String("[]".into())),
                ("attributes_json".into(), Value::String("{}".into())),
                ("confidence".into(), Value::from(1.0)),
                (
                    "entity_json".into(),
                    Value::String(serde_json::to_string(&entity).unwrap()),
                ),
                ("created_at".into(), Value::String(created.to_rfc3339())),
                ("updated_at".into(), Value::String(created.to_rfc3339())),
            ]),
        )
    }

    #[test]
    fn empty_snapshot_is_strict_and_valid() {
        assert_eq!(CORE_TABLES.len(), 19);
        assert_eq!(
            core_table_names()
                .into_iter()
                .collect::<BTreeSet<_>>()
                .len(),
            19
        );
        let snapshot = empty_snapshot();
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let decoded = MfgOwnershipSplitSnapshotV1::decode_strict(&bytes).unwrap();
        let plan = decoded.dry_run(&OwnershipImportContext::default()).unwrap();
        assert!(plan.records.is_empty());
    }

    #[test]
    fn unknown_field_and_digest_tampering_fail_closed() {
        let snapshot = empty_snapshot();
        let mut value = serde_json::to_value(&snapshot).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unknown".to_string(), Value::Bool(true));
        assert!(
            MfgOwnershipSplitSnapshotV1::decode_strict(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );

        let mut snapshot = snapshot;
        snapshot.core_matrix_domain.object_count = 1;
        assert!(snapshot
            .dry_run(&OwnershipImportContext::default())
            .is_err());
    }

    #[test]
    fn unknown_payload_dangling_relation_and_unresolved_evidence_fail_closed() {
        let mut snapshot = empty_snapshot();
        let mut entity = entity_object("entity-1");
        entity
            .payload
            .insert("implicit_default".into(), Value::Null);
        entity.payload_digest = ownership_import_digest(&entity.payload).unwrap();
        snapshot.core_matrix_domain.objects = vec![entity];
        seal(&mut snapshot);
        assert!(snapshot
            .dry_run(&OwnershipImportContext::default())
            .is_err());

        let mut snapshot = empty_snapshot();
        let created: DateTime<Utc> = "2025-01-01T00:00:00Z".parse().unwrap();
        let relation = MatrixRelation {
            relation_id: "relation-1".into(),
            relation_type: "feeds".into(),
            from_entity_id: "entity-1".into(),
            to_entity_id: "missing".into(),
            attributes: serde_json::json!({}),
            confidence: 1.0,
            created_at: created,
            updated_at: created,
        };
        let relation_object = object(
            "matrix_relation",
            BTreeMap::from([("relation_id".into(), Value::String("relation-1".into()))]),
            BTreeMap::from([
                (
                    "relation_id".into(),
                    Value::String(relation.relation_id.clone()),
                ),
                (
                    "relation_type".into(),
                    Value::String(relation.relation_type.clone()),
                ),
                (
                    "from_entity_id".into(),
                    Value::String(relation.from_entity_id.clone()),
                ),
                (
                    "to_entity_id".into(),
                    Value::String(relation.to_entity_id.clone()),
                ),
                ("attributes_json".into(), Value::String("{}".into())),
                ("confidence".into(), Value::from(1.0)),
                (
                    "relation_json".into(),
                    Value::String(serde_json::to_string(&relation).unwrap()),
                ),
                ("created_at".into(), Value::String(created.to_rfc3339())),
                ("updated_at".into(), Value::String(created.to_rfc3339())),
            ]),
        );
        snapshot.core_matrix_domain.objects = vec![entity_object("entity-1"), relation_object];
        seal(&mut snapshot);
        assert!(snapshot
            .dry_run(&OwnershipImportContext::default())
            .unwrap_err()
            .to_string()
            .contains("dangling"));

        let mut snapshot = empty_snapshot();
        let mut entity = entity_object("entity-1");
        entity.evidence_references.push("evidence://missing".into());
        snapshot.core_matrix_domain.objects = vec![entity];
        seal(&mut snapshot);
        assert!(snapshot
            .dry_run(&OwnershipImportContext::default())
            .unwrap_err()
            .to_string()
            .contains("unresolved evidence"));
    }
}
