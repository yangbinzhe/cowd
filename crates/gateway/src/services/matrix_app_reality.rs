//! Closed Matrix operation dispatcher for compile-time linked applications.
//!
//! The operation vocabulary is deliberately smaller than `MatrixStore` and
//! contains no arbitrary SQL, path, backend or configuration access. Gateway
//! remains the sole owner of backend selection and storage authority.

use std::collections::BTreeMap;

use matrix_repository::{MatrixStore, MatrixStoreError};
use serde::de::DeserializeOwned;

pub(super) const MATRIX_OPERATION_INTENT_V1: &str = "cowd.reality.matrix_operation.v1";

pub(super) const MATRIX_APP_OPERATIONS: &[&str] = &[
    "health",
    "data_plane.health",
    "data_plane.plan_ingest",
    "source_pack.upsert_checked",
    "source_pack.get_with_revision",
    "source_pack.validate",
    "source_pack.delta_plan",
    "source_pack.ingest_facts",
    "connector_run.plan",
    "connector_run.get",
    "metric.list_definitions",
    "metric.states",
    "metric.lineage_with_revisions",
    "metric.plan_attention",
    "metric.materialize_snapshot",
    "metric_dependency.upsert_checked",
    "metric.affected_by_fact_type",
    "compute_job.plan",
    "compute_job.get",
    "entity.upsert_checked",
    "entity.resolve_source_key",
    "entity.propose_match",
    "entity.decide_conflict",
    "entity.list_with_revisions",
    "entity.get_with_revision",
    "relation.upsert_checked",
    "relation.list_for_entity_with_revisions",
    "entity.impact_trace",
    "change.list",
    "attention.list",
    "fact.ingest",
    "evidence.build",
    "evidence.get",
    "evidence_packet.get",
    "evidence.evaluate_quality",
    "quality_gate.get",
    "skill.metric_lineage_batch",
    "skill.entity_impact_batch",
];

#[derive(Debug, thiserror::Error)]
pub(super) enum MatrixAppRealityError {
    #[error(transparent)]
    Matrix(#[from] MatrixStoreError),
    #[error("invalid Matrix application operation input: {0}")]
    Input(#[from] serde_json::Error),
}

impl MatrixAppRealityError {
    #[must_use]
    pub(super) fn code(&self) -> &'static str {
        match self {
            Self::Matrix(MatrixStoreError::NotFound(_)) => "not_found",
            Self::Matrix(MatrixStoreError::RevisionConflict { .. }) => "revision_conflict",
            Self::Matrix(
                MatrixStoreError::InvalidScenario(_) | MatrixStoreError::ScenarioState(_),
            )
            | Self::Input(_) => "validation_failed",
            Self::Matrix(MatrixStoreError::Backend(_)) => "host_failed",
        }
    }
}

#[must_use]
pub(super) fn supports(operation: &str) -> bool {
    MATRIX_APP_OPERATIONS.contains(&operation)
}

fn input<T: DeserializeOwned>(
    value: &serde_json::Value,
    field: &str,
) -> Result<T, MatrixAppRealityError> {
    serde_json::from_value(
        value
            .as_object()
            .and_then(|value| value.get(field))
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )
    .map_err(MatrixAppRealityError::from)
}

fn bounded_limit(
    value: &serde_json::Value,
    field: &str,
    maximum: usize,
) -> Result<usize, MatrixAppRealityError> {
    let limit: usize = input(value, field)?;
    if (1..=maximum).contains(&limit) {
        Ok(limit)
    } else {
        Err(
            MatrixStoreError::InvalidScenario(format!("{field} must be between 1 and {maximum}"))
                .into(),
        )
    }
}

fn bounded_depth(value: &serde_json::Value, field: &str) -> Result<usize, MatrixAppRealityError> {
    bounded_limit(value, field, 32)
}

pub(super) fn dispatch(
    store: &dyn MatrixStore,
    operation: &str,
    input_value: &serde_json::Value,
) -> Result<serde_json::Value, MatrixAppRealityError> {
    let value = match operation {
        "health" => serde_json::to_value(store.health()?)?,
        "data_plane.health" => serde_json::to_value(store.data_plane_health()?)?,
        "data_plane.plan_ingest" => {
            serde_json::to_value(store.plan_data_plane_ingest(input(input_value, "ingest")?)?)?
        }
        "source_pack.upsert_checked" => serde_json::to_value(store.upsert_source_pack_checked(
            input(input_value, "source_pack")?,
            input(input_value, "expected_revision")?,
        )?)?,
        "source_pack.get_with_revision" => {
            let id: String = input(input_value, "source_pack_id")?;
            let source_pack = store
                .get_source_pack(&id)?
                .ok_or_else(|| MatrixStoreError::NotFound(format!("source pack {id}")))?;
            serde_json::json!({
                "source_pack": source_pack,
                "revision": store.resource_revision_for_existing("source_pack", &id)?,
            })
        }
        "source_pack.validate" => {
            let id: String = input(input_value, "source_pack_id")?;
            serde_json::to_value(store.validate_source_pack(&id)?)?
        }
        "source_pack.delta_plan" => {
            let id: String = input(input_value, "source_pack_id")?;
            serde_json::to_value(store.source_pack_delta_plan(&id)?)?
        }
        "source_pack.ingest_facts" => {
            let id: String = input(input_value, "source_pack_id")?;
            store.validate_source_pack(&id)?;
            let facts: Vec<matrix_core::MatrixFactInput> = input(input_value, "facts")?;
            if facts.len() > 10_000 {
                return Err(MatrixStoreError::InvalidScenario(
                    "facts must contain at most 10000 items".to_string(),
                )
                .into());
            }
            let mut attention = Vec::with_capacity(facts.len());
            for fact in facts {
                attention.push(store.ingest_fact(&matrix_core::MatrixFact::from_input(fact))?);
            }
            serde_json::to_value(attention)?
        }
        "connector_run.plan" => {
            let id: String = input(input_value, "source_pack_id")?;
            serde_json::to_value(store.plan_connector_run(&id, input(input_value, "input")?)?)?
        }
        "connector_run.get" => {
            let id: String = input(input_value, "run_id")?;
            serde_json::to_value(store.get_connector_run(&id)?)?
        }
        "metric.list_definitions" => serde_json::to_value(store.list_metric_definitions()?)?,
        "metric.states" => {
            let id: String = input(input_value, "metric_id")?;
            serde_json::to_value(store.metric_states(&id)?)?
        }
        "metric.lineage_with_revisions" => {
            let id: String = input(input_value, "metric_id")?;
            serde_json::to_value(
                store.metric_lineage(&id, bounded_depth(input_value, "max_depth")?)?,
            )?
        }
        "metric.plan_attention" => serde_json::to_value(store.plan_metric_attention(
            &input::<String>(input_value, "trigger_fact_type")?,
            input(input_value, "entity_scope")?,
            input(input_value, "period")?,
            bounded_limit(input_value, "limit", 1_000)?,
        )?)?,
        "metric.materialize_snapshot" => {
            let metric_ids: Vec<String> = input(input_value, "metric_ids")?;
            if metric_ids.is_empty() || metric_ids.len() > 1_000 {
                return Err(MatrixStoreError::InvalidScenario(
                    "metric_ids must contain between 1 and 1000 items".to_string(),
                )
                .into());
            }
            serde_json::to_value(
                store.materialize_metric_snapshot(metric_ids, input(input_value, "scope_ref")?)?,
            )?
        }
        "metric_dependency.upsert_checked" => {
            let dependency =
                matrix_core::MatrixMetricDependency::from_input(input(input_value, "dependency")?);
            serde_json::to_value(store.upsert_metric_dependency_checked(
                &dependency,
                input(input_value, "expected_revision")?,
            )?)?
        }
        "metric.affected_by_fact_type" => {
            let fact_type: String = input(input_value, "fact_type")?;
            serde_json::to_value(store.metrics_affected_by_fact_type(&fact_type)?)?
        }
        "compute_job.plan" => {
            serde_json::to_value(store.plan_compute_job_for_fact_type(input(input_value, "job")?)?)?
        }
        "compute_job.get" => {
            let id: String = input(input_value, "job_id")?;
            serde_json::to_value(store.get_compute_job(&id)?)?
        }
        "entity.upsert_checked" => {
            let entity = matrix_core::MatrixEntity::from_input(input(input_value, "entity")?);
            serde_json::to_value(
                store.upsert_entity_checked(&entity, input(input_value, "expected_revision")?)?,
            )?
        }
        "entity.resolve_source_key" => {
            let source_system: String = input(input_value, "source_system")?;
            let source_key: String = input(input_value, "source_key")?;
            let entity = store
                .resolve_entity_by_source_key(&source_system, &source_key)?
                .ok_or_else(|| {
                    MatrixStoreError::NotFound(format!(
                        "entity source key {source_system}:{source_key}"
                    ))
                })?;
            let revision = store.resource_revision_for_existing("entity", &entity.entity_id)?;
            serde_json::json!({"entity": entity, "revision": revision})
        }
        "entity.propose_match" => serde_json::to_value(store.propose_entity_match(
            &input::<String>(input_value, "left_entity_id")?,
            &input::<String>(input_value, "right_entity_id")?,
        )?)?,
        "entity.decide_conflict" => serde_json::to_value(store.decide_entity_conflict(
            &input::<String>(input_value, "candidate_id")?,
            &input::<String>(input_value, "survivor_entity_id")?,
            &input::<String>(input_value, "retired_entity_id")?,
            &input::<String>(input_value, "survivorship_rule")?,
            input(input_value, "notes")?,
        )?)?,
        "entity.list_with_revisions" => {
            let entities = store.list_entities(bounded_limit(input_value, "limit", 1_000)?)?;
            let mut revisions = BTreeMap::new();
            for entity in &entities {
                revisions.insert(
                    entity.entity_id.clone(),
                    store.resource_revision_for_existing("entity", &entity.entity_id)?,
                );
            }
            serde_json::json!({"entities": entities, "revisions": revisions})
        }
        "entity.get_with_revision" => {
            let id: String = input(input_value, "entity_id")?;
            let entity = store
                .get_entity(&id)?
                .ok_or_else(|| MatrixStoreError::NotFound(format!("entity {id}")))?;
            serde_json::json!({
                "entity": entity,
                "revision": store.resource_revision_for_existing("entity", &id)?,
            })
        }
        "relation.upsert_checked" => {
            let relation = matrix_core::MatrixRelation::from_input(input(input_value, "relation")?);
            serde_json::to_value(
                store
                    .upsert_relation_checked(&relation, input(input_value, "expected_revision")?)?,
            )?
        }
        "relation.list_for_entity_with_revisions" => {
            let id: String = input(input_value, "entity_id")?;
            let relations =
                store.list_entity_relations(&id, bounded_limit(input_value, "limit", 1_000)?)?;
            let mut revisions = BTreeMap::new();
            for relation in &relations {
                revisions.insert(
                    relation.relation_id.clone(),
                    store.resource_revision_for_existing("relation", &relation.relation_id)?,
                );
            }
            serde_json::json!({"relations": relations, "revisions": revisions})
        }
        "entity.impact_trace" => serde_json::to_value(store.impact_trace(
            &input::<String>(input_value, "entity_id")?,
            bounded_depth(input_value, "max_depth")?,
        )?)?,
        "change.list" => serde_json::to_value(store.list_changes(bounded_limit(
            input_value,
            "limit",
            1_000,
        )?)?)?,
        "attention.list" => serde_json::to_value(store.list_attention(bounded_limit(
            input_value,
            "limit",
            1_000,
        )?)?)?,
        "fact.ingest" => {
            let inputs: Vec<matrix_core::MatrixFactInput> = input(input_value, "facts")?;
            if inputs.is_empty() || inputs.len() > 10_000 {
                return Err(MatrixStoreError::InvalidScenario(
                    "facts must contain between 1 and 10000 items".to_string(),
                )
                .into());
            }
            let mut ingested = Vec::with_capacity(inputs.len());
            for input in inputs {
                let fact = matrix_core::MatrixFact::from_input(input);
                let attention = store.ingest_fact(&fact)?;
                ingested.push((fact, attention));
            }
            serde_json::to_value(ingested)?
        }
        "evidence.build" => serde_json::to_value(store.build_evidence_packet(
            input::<Option<String>>(input_value, "packet_id")?.as_deref(),
            input::<Option<String>>(input_value, "attention_id")?.as_deref(),
            input::<Option<String>>(input_value, "problem_statement")?.as_deref(),
        )?)?,
        "evidence.get" => {
            let id: String = input(input_value, "packet_id")?;
            serde_json::to_value(
                store
                    .get_evidence_packet(&id)?
                    .ok_or_else(|| MatrixStoreError::NotFound(format!("evidence packet {id}")))?,
            )?
        }
        "evidence_packet.get" => {
            let id: String = input(input_value, "packet_id")?;
            serde_json::to_value(store.get_evidence_packet(&id)?)?
        }
        "evidence.evaluate_quality" => {
            serde_json::to_value(store.evaluate_evidence_quality_with_gate_id(
                &input::<String>(input_value, "packet_id")?,
                &input::<String>(input_value, "gate_id")?,
            )?)?
        }
        "quality_gate.get" => {
            let id: String = input(input_value, "gate_id")?;
            serde_json::to_value(
                store
                    .get_quality_gate(&id)?
                    .ok_or_else(|| MatrixStoreError::NotFound(format!("quality gate {id}")))?,
            )?
        }
        "skill.metric_lineage_batch" => {
            let metric_ids: Vec<String> = input(input_value, "metric_ids")?;
            if metric_ids.is_empty() || metric_ids.len() > 1_000 {
                return Err(MatrixStoreError::InvalidScenario(
                    "metric_ids must contain between 1 and 1000 items".to_string(),
                )
                .into());
            }
            let max_depth = bounded_depth(input_value, "max_depth")?;
            let items = metric_ids
                .into_iter()
                .map(
                    |metric_id| match store.metric_lineage(&metric_id, max_depth) {
                        Ok(lineage) => serde_json::json!({
                            "metric_id": metric_id,
                            "status": "completed",
                            "lineage": lineage,
                        }),
                        Err(error) => serde_json::json!({
                            "metric_id": metric_id,
                            "status": "unavailable",
                            "error": error.to_string(),
                        }),
                    },
                )
                .collect::<Vec<_>>();
            serde_json::json!({"items": items})
        }
        "skill.entity_impact_batch" => {
            let entity_ids: Vec<String> = input(input_value, "entity_ids")?;
            if entity_ids.is_empty() || entity_ids.len() > 1_000 {
                return Err(MatrixStoreError::InvalidScenario(
                    "entity_ids must contain between 1 and 1000 items".to_string(),
                )
                .into());
            }
            let max_depth = bounded_depth(input_value, "max_depth")?;
            let items = entity_ids
                .into_iter()
                .map(
                    |entity_id| match store.impact_trace(&entity_id, max_depth) {
                        Ok(trace) => serde_json::json!({
                            "entity_id": entity_id,
                            "status": "completed",
                            "impact_trace": trace,
                        }),
                        Err(error) => serde_json::json!({
                            "entity_id": entity_id,
                            "status": "unavailable",
                            "error": error.to_string(),
                        }),
                    },
                )
                .collect::<Vec<_>>();
            serde_json::json!({"items": items})
        }
        _ => {
            return Err(MatrixStoreError::InvalidScenario(format!(
                "application Matrix operation {operation} is not allowed"
            ))
            .into());
        }
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_store() -> (tempfile::TempDir, std::sync::Arc<dyn MatrixStore>) {
        let config_home = tempfile::tempdir().expect("temporary config home");
        let registry = storage::StorageRegistry::default_for_config_home(config_home.path());
        registry.ensure_directories().expect("storage directories");
        let handle = registry
            .endpoint(&storage::StorageDomainId::Matrix)
            .expect("Matrix endpoint")
            .as_handle();
        let store =
            matrix_repository::open_matrix_sqlite_repository_handle(&handle).expect("Matrix store");
        (config_home, std::sync::Arc::new(store))
    }

    #[test]
    fn allowlist_dispatches_typed_operations_and_preserves_revision_conflicts() {
        let (_config_home, store) = fixture_store();
        assert!(supports("entity.upsert_checked"));
        assert!(!supports("sql.execute"));
        assert_eq!(MATRIX_APP_OPERATIONS.len(), 38);

        let entity = serde_json::json!({
            "entity": {
                "entity_id": "entity-app-reality",
                "entity_type": "component",
                "canonical_key": "app-reality",
                "display_name": "APP Reality",
                "source_keys": [],
                "attributes": {},
                "confidence": 0.95
            },
            "expected_revision": null
        });
        let created =
            dispatch(store.as_ref(), "entity.upsert_checked", &entity).expect("entity creates");
        assert_eq!(created["created"], true);
        assert_eq!(created["revision"], 1);

        let stale = dispatch(store.as_ref(), "entity.upsert_checked", &entity)
            .expect_err("stale create must conflict");
        assert_eq!(stale.code(), "revision_conflict");

        let listed = dispatch(
            store.as_ref(),
            "entity.list_with_revisions",
            &serde_json::json!({"limit": 10}),
        )
        .expect("entities list");
        assert_eq!(listed["entities"].as_array().map(Vec::len), Some(1));
        assert_eq!(listed["revisions"]["entity-app-reality"], 1);
    }

    #[test]
    fn dispatcher_rejects_unbounded_and_unknown_operations() {
        let (_config_home, store) = fixture_store();
        let unbounded = dispatch(
            store.as_ref(),
            "attention.list",
            &serde_json::json!({"limit": 1001}),
        )
        .expect_err("unbounded limit rejected");
        assert_eq!(unbounded.code(), "validation_failed");
        assert!(dispatch(store.as_ref(), "sql.execute", &serde_json::json!({})).is_err());
    }

    #[test]
    fn evidence_build_replays_the_same_canonical_packet_identity() {
        let (_config_home, store) = fixture_store();
        let packet_id = "evidence-mfg-idempotent-fixture";
        let first = dispatch(
            store.as_ref(),
            "evidence.build",
            &serde_json::json!({
                "packet_id": packet_id,
                "problem_statement": "first canonical problem"
            }),
        )
        .expect("first evidence build");
        let replay = dispatch(
            store.as_ref(),
            "evidence.build",
            &serde_json::json!({
                "packet_id": packet_id,
                "problem_statement": "a retried request cannot replace canonical content"
            }),
        )
        .expect("idempotent evidence replay");
        let fetched = dispatch(
            store.as_ref(),
            "evidence.get",
            &serde_json::json!({"packet_id": packet_id}),
        )
        .expect("canonical evidence fetch");

        assert_eq!(first, replay);
        assert_eq!(replay, fetched);
        assert_eq!(first["packet_id"], packet_id);
        assert_eq!(first["problem_statement"], "first canonical problem");
    }
}
