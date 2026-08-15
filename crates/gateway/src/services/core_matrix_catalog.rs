//! Cowd-owned authority for the closed Matrix Core operation vocabulary.
//!
//! The schemas are generated from the same typed inputs accepted by the
//! dispatcher and from the concrete Matrix result DTOs it serializes.  APP
//! manifests can only select and rename capabilities from this authority;
//! they cannot define Core schemas or executable operations.

use std::collections::{BTreeMap, BTreeSet};

use cowd_app_protocol::{
    AppManifestV1, CoreOperationCatalogV1, GenerationId, IdempotencySemanticsV1,
    OperationDelegationV1, OperationDescriptorV1, OperationKindV1, ProtocolValidate, Sha256Digest,
    PROTOCOL_REVISION_V1,
};
use matrix_core::{
    MatrixAttentionItem, MatrixChangeEvent, MatrixComputeJob, MatrixComputeJobInput,
    MatrixComputePlan, MatrixConnectorRun, MatrixConnectorRunInput, MatrixDataPlaneHealth,
    MatrixDataPlaneIngestPlan, MatrixDataPlaneIngestPlanInput, MatrixEntity,
    MatrixEntityConflictDecision, MatrixEntityInput, MatrixEntityMatchCandidate,
    MatrixEvidencePacket, MatrixFact, MatrixFactInput, MatrixImpactTrace,
    MatrixMetricAttentionPlan, MatrixMetricDefinition, MatrixMetricDependency,
    MatrixMetricDependencyInput, MatrixMetricLineage, MatrixMetricSnapshot, MatrixMetricState,
    MatrixQualityGateDecision, MatrixRelation, MatrixRelationInput, MatrixSourceDeltaPlan,
    MatrixSourcePack, MatrixSourcePackValidation,
};
use matrix_repository::{MatrixHealth, MatrixMetricRecomputeResult, MatrixRevisioned};
use schemars::{schema_for, JsonSchema};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::core_platform_operations::{
    CrossPlaneActionPlanInput, CrossPlaneActionPlanOutput, SurfaceOutboxListInput,
    SurfaceOutboxListOutput, ACTION_PLAN_OPERATION_ID, SURFACE_OUTBOX_LIST_OPERATION_ID,
};
use super::{matrix_app_reality::MatrixAppRealityError, ContextService};
use matrix_repository::MatrixStore;

const CORE_PREFIX: &str = "core.matrix.";
const READ_CAPABILITY: &str = "core.matrix.read";
const WRITE_CAPABILITY: &str = "core.matrix.write";

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CoreMatrixOperationDefinition {
    pub(crate) short_id: &'static str,
    pub(crate) descriptor: OperationDescriptorV1,
    pub(crate) input_schema: Value,
    pub(crate) output_schema: Value,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum CoreMatrixCatalogError {
    #[error("Core Matrix operation catalog is invalid: {0}")]
    Invalid(String),
    #[error("signed APP requirement references unknown Core operation `{0}`")]
    UnknownRequirement(String),
    #[error("signed APP requirement does not match Core operation `{0}`")]
    RequirementMismatch(String),
}

#[derive(Debug, thiserror::Error)]
#[error("Core Matrix operation `{operation_id}` failed ({code}): {detail}")]
pub(crate) struct CoreMatrixDispatchError {
    pub(crate) operation_id: String,
    pub(crate) code: &'static str,
    pub(crate) detail: String,
}

pub(crate) fn dispatch_operation(
    store: &dyn MatrixStore,
    context: &ContextService,
    operation_id: &str,
    payload: &Value,
) -> Result<Value, CoreMatrixDispatchError> {
    let short_id =
        operation_id
            .strip_prefix(CORE_PREFIX)
            .ok_or_else(|| CoreMatrixDispatchError {
                operation_id: operation_id.to_owned(),
                code: "validation_failed",
                detail: "operation is outside the Core Matrix authority".to_owned(),
            })?;
    if !super::matrix_app_reality::supports(short_id) {
        return Err(CoreMatrixDispatchError {
            operation_id: operation_id.to_owned(),
            code: "not_found",
            detail: "operation is not implemented by the Core Matrix dispatcher".to_owned(),
        });
    }
    super::matrix_app_reality::dispatch(store, context, short_id, payload).map_err(|error| {
        CoreMatrixDispatchError {
            operation_id: operation_id.to_owned(),
            code: error.code(),
            detail: error.to_string(),
        }
    })
}

pub(crate) fn definitions() -> Result<Vec<CoreMatrixOperationDefinition>, CoreMatrixCatalogError> {
    let mut values = vec![
        definition::<Empty, MatrixHealth>("health", OperationKindV1::Query),
        definition::<Empty, MatrixDataPlaneHealth>("data_plane.health", OperationKindV1::Query),
        definition::<DataPlanePlanInput, MatrixDataPlaneIngestPlan>(
            "data_plane.plan_ingest",
            OperationKindV1::Query,
        ),
        definition::<SourcePackUpsertInput, MatrixRevisioned<MatrixSourcePack>>(
            "source_pack.upsert_checked",
            OperationKindV1::Command,
        ),
        definition::<SourcePackIdInput, SourcePackWithRevision>(
            "source_pack.get_with_revision",
            OperationKindV1::Query,
        ),
        definition::<SourcePackIdInput, MatrixSourcePackValidation>(
            "source_pack.validate",
            OperationKindV1::Query,
        ),
        definition::<SourcePackIdInput, MatrixSourceDeltaPlan>(
            "source_pack.delta_plan",
            OperationKindV1::Query,
        ),
        definition::<SourcePackIngestFactsInput, Vec<MatrixAttentionItem>>(
            "source_pack.ingest_facts",
            OperationKindV1::Command,
        ),
        definition::<ConnectorRunInput, MatrixConnectorRun>(
            "connector_run.plan",
            OperationKindV1::Command,
        ),
        definition::<ConnectorExecuteInput, MatrixConnectorRun>(
            "connector_run.execute",
            OperationKindV1::Command,
        ),
        definition::<RunIdInput, Option<MatrixConnectorRun>>(
            "connector_run.get",
            OperationKindV1::Query,
        ),
        definition::<Empty, Vec<MatrixMetricDefinition>>(
            "metric.list_definitions",
            OperationKindV1::Query,
        ),
        definition::<MetricIdInput, Vec<MatrixMetricState>>(
            "metric.states",
            OperationKindV1::Query,
        ),
        definition::<MetricLineageInput, MatrixMetricLineage>(
            "metric.lineage_with_revisions",
            OperationKindV1::Query,
        ),
        definition::<MetricAttentionInput, MatrixMetricAttentionPlan>(
            "metric.plan_attention",
            OperationKindV1::Query,
        ),
        definition::<MetricSnapshotInput, MatrixMetricSnapshot>(
            "metric.materialize_snapshot",
            OperationKindV1::Command,
        ),
        definition::<MetricDependencyInput, MatrixRevisioned<MatrixMetricDependency>>(
            "metric_dependency.upsert_checked",
            OperationKindV1::Command,
        ),
        definition::<FactTypeInput, Vec<String>>(
            "metric.affected_by_fact_type",
            OperationKindV1::Query,
        ),
        definition::<ComputePlanInput, MatrixComputePlan>(
            "compute_job.plan",
            OperationKindV1::Command,
        ),
        definition::<JobIdInput, Option<MatrixComputeJob>>(
            "compute_job.get",
            OperationKindV1::Query,
        ),
        definition::<JobIdInput, MatrixComputeJob>("compute_job.execute", OperationKindV1::Command),
        definition::<Empty, MatrixMetricRecomputeResult>(
            "metric.recompute",
            OperationKindV1::Command,
        ),
        definition::<EntityUpsertInput, MatrixRevisioned<MatrixEntity>>(
            "entity.upsert_checked",
            OperationKindV1::Command,
        ),
        definition::<EntitySourceKeyInput, EntitySourceKeyResult>(
            "entity.resolve_source_key",
            OperationKindV1::Query,
        ),
        definition::<EntityMatchInput, MatrixEntityMatchCandidate>(
            "entity.propose_match",
            OperationKindV1::Query,
        ),
        definition::<EntityConflictInput, MatrixEntityConflictDecision>(
            "entity.decide_conflict",
            OperationKindV1::Command,
        ),
        definition::<LimitInput, EntityListWithRevisions>(
            "entity.list_with_revisions",
            OperationKindV1::Query,
        ),
        definition::<EntityIdInput, EntityWithRevision>(
            "entity.get_with_revision",
            OperationKindV1::Query,
        ),
        definition::<RelationUpsertInput, MatrixRevisioned<MatrixRelation>>(
            "relation.upsert_checked",
            OperationKindV1::Command,
        ),
        definition::<EntityRelationsInput, RelationsWithRevisions>(
            "relation.list_for_entity_with_revisions",
            OperationKindV1::Query,
        ),
        definition::<EntityImpactInput, MatrixImpactTrace>(
            "entity.impact_trace",
            OperationKindV1::Query,
        ),
        definition::<LimitInput, Vec<MatrixChangeEvent>>("change.list", OperationKindV1::Query),
        definition::<LimitInput, Vec<MatrixAttentionItem>>(
            "attention.list",
            OperationKindV1::Query,
        ),
        definition::<FactIngestInput, Vec<(MatrixFact, MatrixAttentionItem)>>(
            "fact.ingest",
            OperationKindV1::Command,
        ),
        definition::<EvidenceBuildInput, MatrixEvidencePacket>(
            "evidence.build",
            OperationKindV1::Command,
        ),
        definition::<PacketIdInput, MatrixEvidencePacket>("evidence.get", OperationKindV1::Query),
        definition::<PacketIdInput, Option<MatrixEvidencePacket>>(
            "evidence_packet.get",
            OperationKindV1::Query,
        ),
        definition::<EvidenceQualityInput, MatrixQualityGateDecision>(
            "evidence.evaluate_quality",
            OperationKindV1::Command,
        ),
        definition::<EvidenceContextInput, ContextItemSchema>(
            "evidence.context.get",
            OperationKindV1::Query,
        ),
        definition::<GateIdInput, MatrixQualityGateDecision>(
            "quality_gate.get",
            OperationKindV1::Query,
        ),
        definition::<MetricLineageBatchInput, MetricLineageBatchOutput>(
            "skill.metric_lineage_batch",
            OperationKindV1::Query,
        ),
        definition::<EntityImpactBatchInput, EntityImpactBatchOutput>(
            "skill.entity_impact_batch",
            OperationKindV1::Query,
        ),
        platform_definition::<CrossPlaneActionPlanInput, CrossPlaneActionPlanOutput>(
            ACTION_PLAN_OPERATION_ID,
            "core.cross_plane.read",
            "cross_plane.plan.governed",
        ),
        platform_definition::<SurfaceOutboxListInput, SurfaceOutboxListOutput>(
            SURFACE_OUTBOX_LIST_OPERATION_ID,
            "core.surface.outbox.read",
            "surface.outbox.query.governed",
        ),
    ]
    .into_iter()
    .collect::<Result<Vec<_>, _>>()?;
    values.sort_by(|left, right| {
        left.descriptor
            .operation_id
            .cmp(&right.descriptor.operation_id)
    });
    let ids = values
        .iter()
        .map(|value| value.descriptor.operation_id.as_str())
        .collect::<BTreeSet<_>>();
    if values.len() != 44 || ids.len() != 44 {
        return Err(CoreMatrixCatalogError::Invalid(
            "authority must contain exactly 44 unique operations".to_string(),
        ));
    }
    Ok(values)
}

fn platform_definition<I: JsonSchema, O: JsonSchema>(
    operation_id: &'static str,
    core_capability: &'static str,
    audit_classification: &'static str,
) -> Result<CoreMatrixOperationDefinition, CoreMatrixCatalogError> {
    let input_schema = canonicalize(
        serde_json::to_value(schema_for!(I))
            .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?,
    );
    let output_schema = canonicalize(
        serde_json::to_value(schema_for!(O))
            .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?,
    );
    let descriptor = OperationDescriptorV1 {
        operation_id: operation_id.to_owned(),
        kind: OperationKindV1::Query,
        input_schema_digest: schema_digest(&input_schema)?,
        output_schema_digest: schema_digest(&output_schema)?,
        required_capabilities: vec![core_capability.to_owned()],
        delegation: OperationDelegationV1::Either,
        tenant_scoped: false,
        workspace_scoped: true,
        read_only: true,
        idempotency: IdempotencySemanticsV1::ReadOnly,
        default_deadline_ms: 10_000,
        maximum_deadline_ms: 30_000,
        maximum_request_bytes: 1024 * 1024,
        maximum_response_bytes: 4 * 1024 * 1024,
        maximum_frame_bytes: 1024 * 1024,
        streaming: false,
        replay_window_seconds: None,
        degraded_read_allowed: false,
        audit_classification: audit_classification.to_owned(),
    };
    descriptor
        .validate()
        .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
    Ok(CoreMatrixOperationDefinition {
        short_id: operation_id,
        descriptor,
        input_schema,
        output_schema,
    })
}

pub(crate) fn projected_catalog(
    manifest: &AppManifestV1,
    generation: &GenerationId,
) -> Result<CoreOperationCatalogV1, CoreMatrixCatalogError> {
    let authority = definitions()?
        .into_iter()
        .map(|definition| (definition.descriptor.operation_id.clone(), definition))
        .collect::<BTreeMap<_, _>>();
    let mut operations = Vec::with_capacity(manifest.core_bridge_requirements.len());
    for requirement in &manifest.core_bridge_requirements {
        let definition = authority
            .get(&requirement.core_operation_id)
            .ok_or_else(|| {
                CoreMatrixCatalogError::UnknownRequirement(requirement.core_operation_id.clone())
            })?;
        let mut descriptor = definition.descriptor.clone();
        if descriptor.input_schema_digest != requirement.accepted_input_schema_digest
            || descriptor.output_schema_digest != requirement.accepted_output_schema_digest
            || descriptor.kind != requirement.kind
            || descriptor.streaming != requirement.streaming
        {
            return Err(CoreMatrixCatalogError::RequirementMismatch(
                requirement.core_operation_id.clone(),
            ));
        }
        let insertion = match descriptor
            .required_capabilities
            .binary_search(&requirement.required_app_capability)
        {
            Ok(_) => {
                return Err(CoreMatrixCatalogError::RequirementMismatch(
                    requirement.core_operation_id.clone(),
                ));
            }
            Err(index) => index,
        };
        descriptor
            .required_capabilities
            .insert(insertion, requirement.required_app_capability.clone());
        descriptor
            .validate()
            .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
        validate_projected_capabilities(manifest, &descriptor)?;
        operations.push(descriptor);
    }
    operations.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    let mut catalog = CoreOperationCatalogV1 {
        schema_version: 1,
        protocol_revision: PROTOCOL_REVISION_V1,
        app_id: manifest.app_id.clone(),
        generation: generation.clone(),
        catalog_digest: Sha256Digest(
            "sha256:0000000000000000000000000000000000000000000000000000000000000000".to_string(),
        ),
        operations,
    };
    catalog
        .bind_canonical_catalog_digest()
        .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
    catalog
        .validate_for_manifest(manifest, generation)
        .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
    Ok(catalog)
}

pub(crate) fn validate_projected_capabilities(
    manifest: &AppManifestV1,
    descriptor: &OperationDescriptorV1,
) -> Result<(), CoreMatrixCatalogError> {
    let authority = definitions()?
        .into_iter()
        .find(|definition| definition.descriptor.operation_id == descriptor.operation_id)
        .ok_or_else(|| {
            CoreMatrixCatalogError::UnknownRequirement(descriptor.operation_id.clone())
        })?;
    let requirement = manifest
        .core_bridge_requirements
        .iter()
        .find(|requirement| requirement.core_operation_id == descriptor.operation_id)
        .ok_or_else(|| {
            CoreMatrixCatalogError::UnknownRequirement(descriptor.operation_id.clone())
        })?;
    let mut expected = authority.descriptor.required_capabilities;
    let insertion = match expected.binary_search(&requirement.required_app_capability) {
        Ok(_) => {
            return Err(CoreMatrixCatalogError::RequirementMismatch(
                descriptor.operation_id.clone(),
            ));
        }
        Err(index) => index,
    };
    expected.insert(insertion, requirement.required_app_capability.clone());
    if descriptor.required_capabilities != expected {
        return Err(CoreMatrixCatalogError::RequirementMismatch(
            descriptor.operation_id.clone(),
        ));
    }
    Ok(())
}

fn definition<I: JsonSchema, O: JsonSchema>(
    short_id: &'static str,
    kind: OperationKindV1,
) -> Result<CoreMatrixOperationDefinition, CoreMatrixCatalogError> {
    let input_schema = canonicalize(
        serde_json::to_value(schema_for!(I))
            .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?,
    );
    let output_schema = canonicalize(
        serde_json::to_value(schema_for!(O))
            .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?,
    );
    let read_only = kind == OperationKindV1::Query;
    let descriptor = OperationDescriptorV1 {
        operation_id: format!("{CORE_PREFIX}{short_id}"),
        kind,
        input_schema_digest: schema_digest(&input_schema)?,
        output_schema_digest: schema_digest(&output_schema)?,
        required_capabilities: vec![if read_only {
            READ_CAPABILITY.to_string()
        } else {
            WRITE_CAPABILITY.to_string()
        }],
        delegation: OperationDelegationV1::Either,
        tenant_scoped: false,
        workspace_scoped: true,
        read_only,
        idempotency: if read_only {
            IdempotencySemanticsV1::ReadOnly
        } else {
            IdempotencySemanticsV1::Required
        },
        default_deadline_ms: if read_only { 10_000 } else { 30_000 },
        maximum_deadline_ms: if read_only { 30_000 } else { 120_000 },
        maximum_request_bytes: 4 * 1024 * 1024,
        maximum_response_bytes: 4 * 1024 * 1024,
        maximum_frame_bytes: 1024 * 1024,
        streaming: false,
        replay_window_seconds: None,
        degraded_read_allowed: false,
        audit_classification: if read_only {
            "matrix.query.governed".to_string()
        } else {
            "matrix.command.governed".to_string()
        },
    };
    descriptor
        .validate()
        .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
    Ok(CoreMatrixOperationDefinition {
        short_id,
        descriptor,
        input_schema,
        output_schema,
    })
}

fn schema_digest(schema: &Value) -> Result<Sha256Digest, CoreMatrixCatalogError> {
    let bytes = serde_json::to_vec(schema)
        .map_err(|error| CoreMatrixCatalogError::Invalid(error.to_string()))?;
    Ok(Sha256Digest(format!("sha256:{:x}", Sha256::digest(bytes))))
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, value)| (key, canonicalize(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        other => other,
    }
}

pub(super) fn validate_input(operation: &str, value: &Value) -> Result<(), MatrixAppRealityError> {
    macro_rules! decode {
        ($type:ty) => {{
            serde_json::from_value::<$type>(value.clone())?;
        }};
    }
    match operation {
        "health" | "data_plane.health" | "metric.list_definitions" | "metric.recompute" => {
            decode!(Empty)
        }
        "data_plane.plan_ingest" => decode!(DataPlanePlanInput),
        "source_pack.upsert_checked" => decode!(SourcePackUpsertInput),
        "source_pack.get_with_revision" | "source_pack.validate" | "source_pack.delta_plan" => {
            decode!(SourcePackIdInput)
        }
        "source_pack.ingest_facts" => decode!(SourcePackIngestFactsInput),
        "connector_run.plan" => decode!(ConnectorRunInput),
        "connector_run.execute" => decode!(ConnectorExecuteInput),
        "connector_run.get" => decode!(RunIdInput),
        "metric.states" => decode!(MetricIdInput),
        "metric.lineage_with_revisions" => decode!(MetricLineageInput),
        "metric.plan_attention" => decode!(MetricAttentionInput),
        "metric.materialize_snapshot" => decode!(MetricSnapshotInput),
        "metric_dependency.upsert_checked" => decode!(MetricDependencyInput),
        "metric.affected_by_fact_type" => decode!(FactTypeInput),
        "compute_job.plan" => decode!(ComputePlanInput),
        "compute_job.get" | "compute_job.execute" => decode!(JobIdInput),
        "entity.upsert_checked" => decode!(EntityUpsertInput),
        "entity.resolve_source_key" => decode!(EntitySourceKeyInput),
        "entity.propose_match" => decode!(EntityMatchInput),
        "entity.decide_conflict" => decode!(EntityConflictInput),
        "entity.list_with_revisions" | "change.list" | "attention.list" => decode!(LimitInput),
        "entity.get_with_revision" => decode!(EntityIdInput),
        "relation.upsert_checked" => decode!(RelationUpsertInput),
        "relation.list_for_entity_with_revisions" => decode!(EntityRelationsInput),
        "entity.impact_trace" => decode!(EntityImpactInput),
        "fact.ingest" => decode!(FactIngestInput),
        "evidence.build" => decode!(EvidenceBuildInput),
        "evidence.get" | "evidence_packet.get" => decode!(PacketIdInput),
        "evidence.evaluate_quality" => decode!(EvidenceQualityInput),
        "evidence.context.get" => decode!(EvidenceContextInput),
        "quality_gate.get" => decode!(GateIdInput),
        "skill.metric_lineage_batch" => decode!(MetricLineageBatchInput),
        "skill.entity_impact_batch" => decode!(EntityImpactBatchInput),
        _ => {}
    }
    Ok(())
}

macro_rules! strict_input {
    ($name:ident { $($field:ident : $type:ty),* $(,)? }) => {
        #[derive(Debug, Deserialize, JsonSchema)]
        #[serde(deny_unknown_fields)]
        struct $name { $(pub(super) $field: $type),* }
    };
}

strict_input!(Empty {});
strict_input!(LimitInput { limit: usize });
strict_input!(DataPlanePlanInput {
    ingest: MatrixDataPlaneIngestPlanInput
});
strict_input!(SourcePackUpsertInput { source_pack: MatrixSourcePack, expected_revision: Option<u64> });
strict_input!(SourcePackIdInput {
    source_pack_id: String
});
strict_input!(SourcePackIngestFactsInput { source_pack_id: String, facts: Vec<MatrixFactInput> });
strict_input!(ConnectorRunInput {
    source_pack_id: String,
    input: MatrixConnectorRunInput
});
strict_input!(RunIdInput { run_id: String });
strict_input!(MetricIdInput { metric_id: String });
strict_input!(MetricLineageInput {
    metric_id: String,
    max_depth: usize
});
strict_input!(MetricAttentionInput { trigger_fact_type: String, entity_scope: Option<String>, period: Option<String>, limit: usize });
strict_input!(MetricSnapshotInput { metric_ids: Vec<String>, scope_ref: Option<String> });
strict_input!(MetricDependencyInput { dependency: MatrixMetricDependencyInput, expected_revision: Option<u64> });
strict_input!(FactTypeInput { fact_type: String });
strict_input!(ComputePlanInput {
    job: MatrixComputeJobInput
});
strict_input!(JobIdInput { job_id: String });
strict_input!(EntityUpsertInput { entity: MatrixEntityInput, expected_revision: Option<u64> });
strict_input!(EntitySourceKeyInput {
    source_system: String,
    source_key: String
});
strict_input!(EntityMatchInput {
    left_entity_id: String,
    right_entity_id: String
});
strict_input!(EntityConflictInput { candidate_id: String, survivor_entity_id: String, retired_entity_id: String, survivorship_rule: String, notes: Option<String> });
strict_input!(EntityIdInput { entity_id: String });
strict_input!(RelationUpsertInput { relation: MatrixRelationInput, expected_revision: Option<u64> });
strict_input!(EntityRelationsInput {
    entity_id: String,
    limit: usize
});
strict_input!(EntityImpactInput {
    entity_id: String,
    max_depth: usize
});
strict_input!(FactIngestInput { facts: Vec<MatrixFactInput> });
strict_input!(EvidenceBuildInput { packet_id: Option<String>, attention_id: Option<String>, problem_statement: Option<String> });
strict_input!(PacketIdInput { packet_id: String });
strict_input!(EvidenceQualityInput {
    packet_id: String,
    gate_id: String
});
strict_input!(GateIdInput { gate_id: String });
strict_input!(MetricLineageBatchInput { metric_ids: Vec<String>, max_depth: usize });
strict_input!(EntityImpactBatchInput { entity_ids: Vec<String>, max_depth: usize });

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ConnectorExecuteInput {
    pub(super) source_pack_id: String,
    pub(super) input: StrictConnectorRunInput,
}

impl ConnectorExecuteInput {
    pub(super) fn validate(&self) -> Result<(), MatrixAppRealityError> {
        if self.source_pack_id.trim().is_empty()
            || self.input.run_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(matrix_repository::MatrixStoreError::InvalidScenario(
                "connector_run.execute requires a non-empty source_pack_id and optional mode=run"
                    .to_string(),
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct StrictConnectorRunInput {
    pub(super) run_id: Option<String>,
    pub(super) mode: Option<ExecuteMode>,
    pub(super) resource_ref: Option<String>,
    pub(super) partition_ref: Option<String>,
    pub(super) credential_ref: Option<String>,
    pub(super) expected_rows: Option<u64>,
    pub(super) checksum: Option<String>,
}

impl StrictConnectorRunInput {
    pub(super) fn into_matrix_input(self) -> MatrixConnectorRunInput {
        MatrixConnectorRunInput {
            run_id: self.run_id,
            mode: self.mode.map(|ExecuteMode::Run| "run".to_string()),
            resource_ref: self.resource_ref,
            partition_ref: self.partition_ref,
            credential_ref: self.credential_ref,
            expected_rows: self.expected_rows,
            checksum: self.checksum,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub(super) enum ExecuteMode {
    Run,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "PacketIdInput")]
pub(super) struct EvidenceContextInput {
    pub(super) packet_id: String,
}

impl EvidenceContextInput {
    pub(super) fn validate(&self) -> Result<(), MatrixAppRealityError> {
        if self.packet_id.trim().is_empty() {
            return Err(matrix_repository::MatrixStoreError::InvalidScenario(
                "evidence.context.get requires a non-empty packet_id".to_string(),
            )
            .into());
        }
        Ok(())
    }
}

#[derive(JsonSchema)]
struct SourcePackWithRevision {
    source_pack: MatrixSourcePack,
    revision: u64,
}

#[derive(JsonSchema)]
struct EntitySourceKeyResult {
    entity: MatrixEntity,
    revision: u64,
}

#[derive(JsonSchema)]
struct EntityListWithRevisions {
    entities: Vec<MatrixEntity>,
    revisions: BTreeMap<String, u64>,
}

#[derive(JsonSchema)]
struct EntityWithRevision {
    entity: MatrixEntity,
    revision: u64,
}

#[derive(JsonSchema)]
struct RelationsWithRevisions {
    relations: Vec<MatrixRelation>,
    revisions: BTreeMap<String, u64>,
}

#[derive(JsonSchema)]
struct ContextItemSchema {
    id: String,
    source: ContextSourceKindSchema,
    authority: ContextAuthoritySchema,
    visibility: ContextVisibilitySchema,
    role: ContextRoleSchema,
    content: String,
    token_estimate: u64,
    score: f32,
    evidence: Vec<String>,
    source_id: Option<String>,
    source_version: Option<String>,
    source_lifecycle: ContextSourceLifecycleSchema,
    source_reason: Option<String>,
    conflict_with: Vec<String>,
}

#[derive(JsonSchema)]
enum ContextSourceKindSchema {
    StableHead,
    RuntimeHeader,
    Conversation,
    Memory,
    Knowledge,
    Fact,
    Matrix,
    Task,
    ToolTrace,
    Workspace,
    AgentPeer,
    Handoff,
}

#[derive(JsonSchema)]
enum ContextAuthoritySchema {
    System,
    User,
    Project,
    Session,
    Agent,
    Tool,
    Derived,
}

#[derive(JsonSchema)]
enum ContextVisibilitySchema {
    Private,
    Shared,
    Team,
}

#[derive(JsonSchema)]
enum ContextRoleSchema {
    Instruction,
    Identity,
    Orientation,
    Evidence,
    Warning,
    TaskState,
    RecentTurn,
    ToolSummary,
}

#[derive(JsonSchema)]
enum ContextSourceLifecycleSchema {
    Static,
    Runtime,
    Ephemeral,
    Session,
    Durable,
    External,
    SuppressedForCurrentTurn,
    Conflict,
}

#[derive(JsonSchema)]
struct MetricLineageBatchOutput {
    items: Vec<MetricLineageBatchItem>,
}

#[derive(JsonSchema)]
struct MetricLineageBatchItem {
    metric_id: String,
    status: String,
    lineage: Option<MatrixMetricLineage>,
    error: Option<String>,
}

#[derive(JsonSchema)]
struct EntityImpactBatchOutput {
    items: Vec<EntityImpactBatchItem>,
}

#[derive(JsonSchema)]
struct EntityImpactBatchItem {
    entity_id: String,
    status: String,
    impact_trace: Option<MatrixImpactTrace>,
    error: Option<String>,
}

#[cfg(test)]
mod tests {
    use cowd_app_protocol::{
        AppId, AppSurfacesV1, AuthorizationProfileV1, BundleIntegrityV1, BundleSignatureV1,
        CoreBridgeRequirementV1, FilesystemPolicyV1, IntegrityAlgorithmV1, NetworkPolicyV1,
        ProtocolRangeV1, SandboxProfileV1, SignatureAlgorithmV1,
    };

    use super::*;

    #[test]
    fn authority_and_dispatcher_are_bijective() {
        let authority = definitions().expect("authority");
        let authority_ids = authority
            .iter()
            .filter(|definition| definition.descriptor.operation_id.starts_with(CORE_PREFIX))
            .map(|definition| definition.short_id)
            .collect::<BTreeSet<_>>();
        let dispatcher_ids = super::super::matrix_app_reality::MATRIX_APP_OPERATIONS
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        assert_eq!(authority.len(), 44);
        assert_eq!(dispatcher_ids.len(), 42);
        assert_eq!(authority_ids, dispatcher_ids);
        let platform_authority_ids = authority
            .iter()
            .filter(|definition| !definition.descriptor.operation_id.starts_with(CORE_PREFIX))
            .map(|definition| definition.descriptor.operation_id.as_str())
            .collect::<BTreeSet<_>>();
        let platform_dispatcher_ids =
            super::super::core_platform_operations::PLATFORM_OPERATION_IDS
                .into_iter()
                .collect::<BTreeSet<_>>();
        assert_eq!(platform_dispatcher_ids.len(), 2);
        assert_eq!(platform_authority_ids, platform_dispatcher_ids);
        assert!(platform_dispatcher_ids
            .iter()
            .all(|operation_id| super::super::core_platform_operations::supports(operation_id)));
        for definition in authority {
            definition.descriptor.validate().expect("descriptor");
            assert!(definition.input_schema.is_object());
            assert!(definition.output_schema.is_object());
            if definition.descriptor.kind == OperationKindV1::Command {
                assert_eq!(
                    definition.descriptor.idempotency,
                    IdempotencySemanticsV1::Required
                );
                assert!(!definition.descriptor.read_only);
            }
        }
    }

    #[test]
    fn frozen_mfg_39_schema_subset_and_projection_are_interoperable() {
        let expected = serde_json::from_str::<BTreeMap<String, String>>(include_str!(
            "fixtures/mfg39-schema-digests.json"
        ))
        .expect("MFG digest fixture");
        assert_eq!(expected.len(), 78);
        let authority = definitions().expect("authority");
        let by_id = authority
            .iter()
            .map(|definition| (definition.descriptor.operation_id.clone(), definition))
            .collect::<BTreeMap<_, _>>();
        for (key, expected_digest) in &expected {
            let (operation_id, direction) = key
                .strip_suffix(".v1")
                .and_then(|key| key.rsplit_once('.'))
                .expect("schema fixture key");
            let definition = by_id.get(operation_id).expect("Core operation exists");
            let actual = match direction {
                "input" => &definition.descriptor.input_schema_digest.0,
                "output" => &definition.descriptor.output_schema_digest.0,
                _ => panic!("unknown schema direction"),
            };
            assert_eq!(actual, expected_digest, "{key}");
        }

        let mfg_operation_ids = expected
            .keys()
            .filter_map(|key| key.strip_suffix(".input.v1"))
            .collect::<BTreeSet<_>>();
        assert_eq!(mfg_operation_ids.len(), 39);
        assert!(!mfg_operation_ids.contains("core.matrix.evidence_packet.get"));
        assert!(!mfg_operation_ids.contains("core.matrix.skill.metric_lineage_batch"));
        assert!(!mfg_operation_ids.contains("core.matrix.skill.entity_impact_batch"));

        let mut requirements = mfg_operation_ids
            .into_iter()
            .map(|operation_id| {
                let definition = by_id.get(operation_id).expect("definition");
                let capability = if definition.descriptor.read_only {
                    "mfg.read"
                } else {
                    "mfg.write"
                };
                CoreBridgeRequirementV1 {
                    app_operation_id: operation_id.replacen("core.matrix.", "mfg.reality.", 1),
                    core_operation_id: operation_id.to_string(),
                    accepted_input_schema_digest: definition.descriptor.input_schema_digest.clone(),
                    accepted_output_schema_digest: definition
                        .descriptor
                        .output_schema_digest
                        .clone(),
                    required_app_capability: capability.to_string(),
                    kind: definition.descriptor.kind,
                    streaming: false,
                }
            })
            .collect::<Vec<_>>();
        requirements.sort_by(|left, right| left.app_operation_id.cmp(&right.app_operation_id));
        let mut manifest = fixture_manifest(requirements);
        manifest
            .bind_canonical_signed_digest()
            .expect("manifest digest");
        manifest.validate().expect("manifest");
        let generation = GenerationId(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        );
        let catalog = projected_catalog(&manifest, &generation).expect("projected catalog");
        assert_eq!(catalog.operations.len(), 39);
        for descriptor in &catalog.operations {
            let core_capability = if descriptor.read_only {
                "core.matrix.read"
            } else {
                "core.matrix.write"
            };
            let app_capability = if descriptor.read_only {
                "mfg.read"
            } else {
                "mfg.write"
            };
            assert_eq!(
                descriptor.required_capabilities,
                vec![core_capability.to_string(), app_capability.to_string()]
            );
            validate_projected_capabilities(&manifest, descriptor)
                .expect("projection retains Core authority and signed APP capability");
        }
        catalog
            .validate_for_manifest(&manifest, &generation)
            .expect("catalog validates against signed requirements");
    }

    #[test]
    fn projected_capabilities_reject_forged_core_authority() {
        let definition = definitions()
            .expect("authority")
            .into_iter()
            .find(|definition| definition.short_id == "health")
            .expect("health");
        let mut manifest = fixture_manifest(vec![CoreBridgeRequirementV1 {
            app_operation_id: "mfg.health".to_string(),
            core_operation_id: definition.descriptor.operation_id.clone(),
            accepted_input_schema_digest: definition.descriptor.input_schema_digest.clone(),
            accepted_output_schema_digest: definition.descriptor.output_schema_digest.clone(),
            required_app_capability: "mfg.read".to_string(),
            kind: definition.descriptor.kind,
            streaming: false,
        }]);
        manifest
            .bind_canonical_signed_digest()
            .expect("manifest digest");
        let generation = GenerationId(
            "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
        );
        let catalog = projected_catalog(&manifest, &generation).expect("projected catalog");
        let descriptor = &catalog.operations[0];
        validate_projected_capabilities(&manifest, descriptor).expect("authority projection");

        let mut forged = descriptor.clone();
        forged
            .required_capabilities
            .insert(1, "core.matrix.write".to_string());
        assert!(validate_projected_capabilities(&manifest, &forged).is_err());

        let mut missing_core = descriptor.clone();
        missing_core.required_capabilities.remove(0);
        assert!(validate_projected_capabilities(&manifest, &missing_core).is_err());
    }

    #[test]
    fn strict_input_rejects_unknown_fields_and_schema_tamper_fails_closed() {
        assert!(validate_input("health", &serde_json::json!({"extra": true})).is_err());
        let definition = definitions()
            .expect("authority")
            .into_iter()
            .find(|definition| definition.short_id == "compute_job.execute")
            .expect("execute operation");
        let mut requirements = vec![CoreBridgeRequirementV1 {
            app_operation_id: "mfg.compute.execute".to_string(),
            core_operation_id: definition.descriptor.operation_id.clone(),
            accepted_input_schema_digest: Sha256Digest(
                "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            ),
            accepted_output_schema_digest: definition.descriptor.output_schema_digest,
            required_app_capability: "mfg.write".to_string(),
            kind: OperationKindV1::Command,
            streaming: false,
        }];
        requirements.sort_by(|left, right| left.app_operation_id.cmp(&right.app_operation_id));
        let mut manifest = fixture_manifest(requirements);
        manifest
            .bind_canonical_signed_digest()
            .expect("manifest digest");
        assert!(projected_catalog(
            &manifest,
            &GenerationId(
                "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    .to_string()
            )
        )
        .is_err());
    }

    fn fixture_manifest(requirements: Vec<CoreBridgeRequirementV1>) -> AppManifestV1 {
        let placeholder = Sha256Digest(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        );
        AppManifestV1 {
            schema_version: 1,
            app_id: AppId("mfg".to_string()),
            display_name: "MFG fixture".to_string(),
            artifact_version: "1.0.0".to_string(),
            required_protocol: ProtocolRangeV1::exact_v1(),
            executable: "bin/mfg-worker".to_string(),
            web_root: None,
            capabilities: vec!["mfg.read".to_string(), "mfg.write".to_string()],
            authorization_profiles: vec![AuthorizationProfileV1 {
                profile_id: "operator".to_string(),
                display_name: "Operator".to_string(),
                capabilities: vec!["mfg.read".to_string(), "mfg.write".to_string()],
                surface_capabilities: BTreeMap::new(),
                is_default: true,
            }],
            core_bridge_requirements: requirements,
            surfaces: AppSurfacesV1 {
                web: false,
                tui_view: false,
            },
            integrity: BundleIntegrityV1 {
                algorithm: IntegrityAlgorithmV1::Sha256,
                files: BTreeMap::from([("bin/mfg-worker".to_string(), placeholder.clone())]),
                manifest_digest: placeholder.clone(),
            },
            signature: BundleSignatureV1 {
                algorithm: SignatureAlgorithmV1::Ed25519,
                key_id: "fixture-key".to_string(),
                signature: "fixture-signature".to_string(),
                signed_digest: placeholder.clone(),
                expires_unix_ms: None,
                provenance_digest: Some(placeholder),
            },
            sandbox: SandboxProfileV1 {
                filesystem: FilesystemPolicyV1::BundleReadOnlyDataReadWrite,
                network: NetworkPolicyV1::Deny,
                max_processes: 8,
                max_open_files: 256,
                max_memory_bytes: 256 * 1024 * 1024,
                cpu_quota_millis_per_second: 1_000,
            },
            presentation: None,
        }
    }
}
