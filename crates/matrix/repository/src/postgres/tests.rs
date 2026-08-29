use std::env;

use matrix_core::{
    MatrixComputeJobInput, MatrixDataPlaneIngestPlanInput, MatrixEntityInput, MatrixFactInput,
    MatrixMetricDependencyInput, MatrixSourceEntityMapping, MatrixSourceKey,
    MatrixSourceRelationMapping,
};
use storage::{StaticSecretRefResolver, StorageDomainId, StorageEndpoint, StorageScope};

use super::*;
use crate::{copy_quiesced_matrix_store, MatrixRecallQuery, MatrixSqliteRepository};

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_bounded_recall_matches_authorization_order_and_limit_contract() {
    let url = env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let resolver = StaticSecretRefResolver::new([("matrix.pg.recall".to_string(), url)]);
    let repository = PostgresMatrixRepository::connect(
        PostgresConnectionConfig::new(
            "matrix-postgres-recall-test",
            "matrix.pg.recall",
            "cowd-matrix-postgres-recall-contract",
        ),
        &resolver,
    )
    .expect("postgres repository opens");
    for (id, snapshot, confidence) in [
        ("matrix-pg-recall-low", "snapshot-pg-authorized", 0.8),
        ("matrix-pg-recall-high", "snapshot-pg-authorized", 0.95),
        ("matrix-pg-recall-cross", "snapshot-pg-forbidden", 1.0),
    ] {
        repository
            .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
                fact_id: Some(id.to_string()),
                snapshot_id: Some(snapshot.to_string()),
                fact_type: "supply.recall-needle".to_string(),
                entity_refs: Vec::new(),
                metric_key: None,
                dimensions: Value::Null,
                measures: serde_json::json!({"value": confidence}),
                event_time: Some(Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: None,
                confidence: Some(confidence),
                raw_hash: None,
            }))
            .expect("fact saves");
    }
    let recalled = repository
        .recall_facts(&MatrixRecallQuery::new(
            vec!["snapshot-pg-authorized".to_string()],
            "recall-needle",
            1,
        ))
        .expect("bounded recall succeeds");
    assert_eq!(recalled.len(), 1);
    assert_eq!(recalled[0].fact_id, "matrix-pg-recall-high");
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn real_postgres_adapter_preserves_matrix_snapshot_and_metric_semantics() {
    let url = env::var("COWD_TEST_POSTGRES_URL").expect("COWD_TEST_POSTGRES_URL is required");
    let source = MatrixSqliteRepository::in_memory().expect("sqlite source opens");
    let entity = source
        .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
            entity_id: Some("matrix-pg-migration-entity".to_string()),
            entity_type: "part".to_string(),
            canonical_key: "PG-MIGRATION-PART".to_string(),
            display_name: Some("PostgreSQL migration part".to_string()),
            source_keys: vec![MatrixSourceKey {
                source_system: "erp".to_string(),
                source_key: "pg-migration-part".to_string(),
                source_ref: Some("erp://part/pg-migration-part".to_string()),
            }],
            attributes: serde_json::json!({"tier": "integration-test"}),
            confidence: Some(0.99),
        }))
        .expect("entity saves");
    source
        .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("matrix-pg-migration-fact".to_string()),
            snapshot_id: Some("matrix-pg-migration-snapshot".to_string()),
            fact_type: "inventory.level".to_string(),
            entity_refs: vec![entity.reference()],
            metric_key: Some("inventory_level".to_string()),
            dimensions: serde_json::json!({"warehouse": "test"}),
            measures: serde_json::json!({"quantity": 42}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("erp://inventory/test".to_string()),
            confidence: Some(0.98),
            raw_hash: Some("sha256:matrix-pg-migration".to_string()),
        }))
        .expect("fact saves");
    let mut load_definition = MatrixMetricDefinition::inferred_for_measure(
        "work_center_load",
        "manufacturing.work_center_load",
        "load_hours",
    );
    load_definition.formula_ref = matrix_core::MATRIX_FORMULA_RATIO_PERCENT_V1.to_string();
    load_definition.denominator_measure = Some("capacity_hours".to_string());
    source
        .register_metric_definition(&load_definition)
        .expect("ratio definition saves");
    source
        .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("matrix-pg-load-fact".to_string()),
            snapshot_id: Some("matrix-pg-load-snapshot".to_string()),
            fact_type: "manufacturing.work_center_load".to_string(),
            entity_refs: vec!["work-center:pg".to_string()],
            metric_key: Some("work_center_load".to_string()),
            dimensions: serde_json::json!({"week": "2026-W30"}),
            measures: serde_json::json!({"load_hours": 188, "capacity_hours": 160}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("mes://work-center/pg".to_string()),
            confidence: Some(0.9),
            raw_hash: Some("sha256:matrix-pg-load".to_string()),
        }))
        .expect("ratio fact saves");
    source
        .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
            fact_id: Some("matrix-pg-load-other-period".to_string()),
            snapshot_id: Some("matrix-pg-load-snapshot".to_string()),
            fact_type: "manufacturing.work_center_load".to_string(),
            entity_refs: vec!["work-center:other".to_string()],
            metric_key: Some("work_center_load".to_string()),
            dimensions: serde_json::json!({"week": "2026-W31"}),
            measures: serde_json::json!({"load_hours": 50, "capacity_hours": 100}),
            event_time: Some(Utc::now()),
            valid_from: None,
            valid_to: None,
            source_ref: Some("mes://work-center/other".to_string()),
            confidence: Some(0.85),
            raw_hash: Some("sha256:matrix-pg-load-other".to_string()),
        }))
        .expect("second ratio fact saves");
    let output_definition = MatrixMetricDefinition::inferred_for_measure(
        "good_units",
        "manufacturing.good_units",
        "units",
    );
    source
        .register_metric_definition(&output_definition)
        .expect("sum definition saves");
    for (id, units) in [("good-units-a", 3), ("good-units-b", 4)] {
        source
            .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
                fact_id: Some(id.to_string()),
                snapshot_id: Some("good-units-snapshot".to_string()),
                fact_type: "manufacturing.good_units".to_string(),
                entity_refs: vec!["line:one".to_string()],
                metric_key: Some("good_units".to_string()),
                dimensions: serde_json::json!({"period": "2026-W30"}),
                measures: serde_json::json!({"units": units}),
                event_time: Some(Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: Some("mes://line/one".to_string()),
                confidence: Some(0.95),
                raw_hash: Some(format!("sha256:{id}")),
            }))
            .expect("sum fact saves");
    }
    let dependency = MatrixMetricDependency::from_input(MatrixMetricDependencyInput {
        dependency_id: Some("good-units-to-load".to_string()),
        upstream_metric_id: "good_units".to_string(),
        downstream_metric_id: "work_center_load".to_string(),
        dependency_type: "operational_input".to_string(),
        entity_relation_type: None,
        required_fact_types: vec!["manufacturing.good_units".to_string()],
        transformation_ref: None,
        confidence: Some(0.9),
        notes: None,
    });
    source
        .upsert_metric_dependency(&dependency)
        .expect("metric dependency saves");
    let source_lineage = source
        .metric_lineage("good_units", 6)
        .expect("sqlite lineage computes");
    let plan = source
        .plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
            source_ref: "erp://inventory/test".to_string(),
            fact_type: "inventory.level".to_string(),
            partition_ref: Some("2026-07-23".to_string()),
            high_watermark: Some("42".to_string()),
            estimated_rows: Some(1),
            raw_checksum: Some("sha256:matrix-pg-migration".to_string()),
            expected_revision: None,
            adapter_id: Some("integration-test".to_string()),
            strategy: Some("full_snapshot".to_string()),
            table: Some("inventory".to_string()),
            cursor: Some("42".to_string()),
            offset: Some(1),
            metric_ids: Vec::new(),
        })
        .expect("watermark plans");
    source
        .commit_data_plane_ingest(&plan)
        .expect("watermark commits");

    let resolver = StaticSecretRefResolver::new([("matrix.pg.test".to_string(), url)]);
    let target = PostgresMatrixRepository::connect(
        PostgresConnectionConfig::new(
            "matrix-postgres-integration-test",
            "matrix.pg.test",
            "cowd-matrix-postgres-contract",
        ),
        &resolver,
    )
    .expect("postgres target opens");
    let mut index_client = target
        .executor()
        .checkout_critical()
        .expect("PostgreSQL index audit checkout succeeds");
    let metric_index: String = index_client
            .query_one(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'idx_matrix_fact_metric_v2'",
                &[],
            )
            .expect("metric index exists")
            .get(0);
    assert!(!metric_index.contains(" WHERE "));
    let scoped_metric_index: String = index_client
            .query_one(
                "SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'idx_matrix_fact_metric_scope_period_time'",
                &[],
            )
            .expect("scoped metric index exists")
            .get(0);
    assert!(scoped_metric_index.contains("metric_key"));
    assert!(scoped_metric_index.contains("entity_refs"));
    assert!(scoped_metric_index.contains("dimensions"));
    let obsolete_index_count: i64 = index_client
            .query_one(
                "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'idx_matrix_fact_metric'",
                &[],
            )
            .expect("obsolete metric index audit succeeds")
            .get(0);
    assert_eq!(obsolete_index_count, 0);
    drop(index_client);
    let manifest_root = tempfile::tempdir().expect("manifest root");
    let manifest =
        copy_quiesced_matrix_store(&source, &target, manifest_root.path().join("matrix.json"))
            .expect("quiesced migration succeeds");
    let selected = crate::MatrixStoreHandle::new(StorageEndpoint::postgres(
        StorageDomainId::Matrix,
        StorageScope::Global,
        "matrix",
        "matrix.0001",
    ))
    .open_with_postgres_executor(target.executor().clone())
    .expect("injected PostgreSQL Matrix selection succeeds");

    assert_eq!(manifest.source_digest, manifest.target_digest);
    assert!(manifest.record_count >= 7);
    assert_eq!(
        MatrixStore::health(&*selected)
            .expect("selected store health")
            .fact_count,
        5
    );
    assert!(MatrixStore::get_entity(&target, &entity.entity_id)
        .expect("entity reads")
        .is_some());
    let recompute = MatrixStore::recompute_metrics(&target)
        .expect("PostgreSQL executes the normalized query plan");
    let load = recompute
        .metric_states
        .iter()
        .find(|state| {
            state.metric_id == "work_center_load"
                && state.entity_scope == "work-center:pg"
                && state.period == "2026-W30"
        })
        .expect("ratio metric state");
    assert!((load.value - 117.5).abs() < f64::EPSILON);
    let other_load = recompute
        .metric_states
        .iter()
        .find(|state| {
            state.metric_id == "work_center_load"
                && state.entity_scope == "work-center:other"
                && state.period == "2026-W31"
        })
        .expect("second period ratio state");
    assert!((other_load.value - 50.0).abs() < f64::EPSILON);
    let output = recompute
        .metric_states
        .iter()
        .find(|state| state.metric_id == "good_units")
        .expect("sum metric state");
    assert!((output.value - 7.0).abs() < f64::EPSILON);
    let target_lineage =
        MatrixStore::metric_lineage(&target, "good_units", 6).expect("postgres lineage computes");
    assert_eq!(
        target_lineage.impacted_metric_ids,
        source_lineage.impacted_metric_ids
    );
    assert_eq!(
        target_lineage
            .downstream_dependencies
            .iter()
            .map(|item| item.dependency_id.as_str())
            .collect::<Vec<_>>(),
        source_lineage
            .downstream_dependencies
            .iter()
            .map(|item| item.dependency_id.as_str())
            .collect::<Vec<_>>()
    );

    let scoped_job = MatrixStore::plan_compute_job_for_fact_type(
        &target,
        MatrixComputeJobInput {
            job_id: Some("pg-scoped-load".to_string()),
            trigger_fact_type: "manufacturing.work_center_load".to_string(),
            trigger_fact_refs: Vec::new(),
            entity_scope: Some("work-center:pg".to_string()),
            period: Some("2026-W30".to_string()),
            metric_ids: vec!["work_center_load".to_string()],
            priority: None,
        },
    )
    .expect("PostgreSQL scoped job plans");
    let scoped_job = MatrixStore::run_compute_job(&target, &scoped_job.job.job_id)
        .expect("PostgreSQL scoped job runs");
    assert_eq!(scoped_job.result_summary["metric_state_count"], 1);
    assert!(
        MatrixStore::resolve_entity_by_source_key(&target, "erp", "pg-migration-part")
            .expect("source key resolves")
            .is_some()
    );
    assert_eq!(
        MatrixStore::list_facts(&target, 10)
            .expect("facts list")
            .len(),
        5
    );
    assert!(MatrixStore::get_data_plane_watermark(
        &target,
        "erp://inventory/test",
        "inventory.level",
        "2026-07-23",
    )
    .expect("watermark reads")
    .is_some());
    let replay_plan = MatrixStore::plan_data_plane_ingest(
        &target,
        MatrixDataPlaneIngestPlanInput {
            source_ref: "erp://inventory/test".to_string(),
            fact_type: "inventory.level".to_string(),
            partition_ref: Some("2026-07-23".to_string()),
            high_watermark: Some("42".to_string()),
            estimated_rows: Some(1),
            raw_checksum: Some("sha256:matrix-pg-migration".to_string()),
            expected_revision: None,
            adapter_id: Some("integration-test".to_string()),
            strategy: Some("full_snapshot".to_string()),
            table: Some("inventory".to_string()),
            cursor: Some("42".to_string()),
            offset: Some(1),
            metric_ids: Vec::new(),
        },
    )
    .expect("PostgreSQL replay plan");
    assert_eq!(
        MatrixStore::commit_data_plane_ingest(&target, &replay_plan)
            .expect("PostgreSQL same-batch replay")
            .revision,
        1
    );

    let source_pack = MatrixSourcePack {
        source_pack_id: "pg-transactional-source".to_string(),
        source_name: "pg_transaction_fixture".to_string(),
        owner: "test".to_string(),
        access_mode: "database".to_string(),
        refresh_mode: "incremental".to_string(),
        entity_mappings: vec![
            MatrixSourceEntityMapping {
                source_entity: "work_center".to_string(),
                matrix_entity_type: "work_center".to_string(),
                source_key_field: "from_id".to_string(),
            },
            MatrixSourceEntityMapping {
                source_entity: "operation".to_string(),
                matrix_entity_type: "operation".to_string(),
                source_key_field: "to_id".to_string(),
            },
        ],
        fact_mappings: vec![matrix_core::MatrixSourceFactMapping {
            source_table: "events".to_string(),
            fact_type: "manufacturing.transaction_fixture".to_string(),
            metric_key: "transaction_fixture".to_string(),
            entity_ref_fields: vec!["from_id".to_string(), "to_id".to_string()],
            measure_fields: vec!["value".to_string()],
            event_time_field: None,
            dedup_key: "event_id".to_string(),
            delta_signature: "event_id".to_string(),
        }],
        relation_mappings: vec![MatrixSourceRelationMapping {
            source_table: "events".to_string(),
            relation_type: "executes".to_string(),
            from_source_key_field: "from_id".to_string(),
            to_source_key_field: "to_id".to_string(),
            attribute_fields: vec!["value".to_string()],
            dedup_key: "event_id".to_string(),
        }],
        reconciliation_rules: Vec::new(),
        quality_rules: Vec::new(),
        freshness_sla: None,
        security_policy: None,
        metadata: Value::Null,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    MatrixStore::upsert_source_pack(&target, source_pack)
        .expect("PostgreSQL transaction source pack saves");
    let snapshot = MatrixSourceSnapshot::from_input(matrix_core::MatrixSourceSnapshotInput {
        snapshot_id: Some("pg-transactional-snapshot".to_string()),
        source_pack_id: Some("pg-transactional-source".to_string()),
        source_system: "pg_transaction_fixture".to_string(),
        source_kind: matrix_core::MatrixSourceKind::Db,
        resource_ref: Some("postgres://fixture/events".to_string()),
        business_period: Some("2026-W30".to_string()),
        captured_at: None,
        schema_version: Some("fixture/v1".to_string()),
        row_count: Some(1),
        checksum: Some("sha256:pg-transaction".to_string()),
        confidence: Some(0.9),
        metadata: Value::Null,
    });
    let mut failure_client = target
        .executor()
        .checkout_critical()
        .expect("PostgreSQL failure injection checkout");
    failure_client
            .batch_execute(
                "CREATE OR REPLACE FUNCTION r5_reject_matrix_receipt() RETURNS trigger
                 LANGUAGE plpgsql AS $$ BEGIN
                   IF NEW.payload->'metadata' ? 'chunk_receipt' THEN
                     RAISE EXCEPTION 'injected source apply failure';
                   END IF;
                   RETURN NEW;
                 END $$;
                 DROP TRIGGER IF EXISTS r5_reject_matrix_receipt ON matrix_source_snapshot;
                 CREATE TRIGGER r5_reject_matrix_receipt BEFORE INSERT OR UPDATE ON matrix_source_snapshot
                 FOR EACH ROW EXECUTE FUNCTION r5_reject_matrix_receipt();",
            )
            .expect("PostgreSQL failure injection installs");
    drop(failure_client);
    let fixture_rows = vec![serde_json::json!({
        "event_id": "E-1",
        "from_id": "WC-1",
        "to_id": "OP-1",
        "value": 9
    })];
    let health_before_failure = MatrixStore::health(&target).expect("health before rollback");
    assert!(MatrixStore::apply_source_snapshot_rows(
        &target,
        "pg-transactional-source",
        snapshot.clone(),
        &fixture_rows,
    )
    .is_err());
    assert!(
        MatrixStore::get_source_snapshot(&target, "pg-transactional-snapshot")
            .expect("failed PostgreSQL snapshot reads")
            .is_none()
    );
    assert_eq!(
        MatrixStore::health(&target).expect("health after rollback"),
        health_before_failure
    );
    let mut cleanup_client = target
        .executor()
        .checkout_critical()
        .expect("PostgreSQL failure cleanup checkout");
    cleanup_client
        .batch_execute(
            "DROP TRIGGER IF EXISTS r5_reject_matrix_receipt ON matrix_source_snapshot;
                 DROP FUNCTION IF EXISTS r5_reject_matrix_receipt();",
        )
        .expect("PostgreSQL failure injection cleans up");
    drop(cleanup_client);
    let apply_report = MatrixStore::apply_source_snapshot_rows(
        &target,
        "pg-transactional-source",
        snapshot,
        &fixture_rows,
    )
    .expect("PostgreSQL source snapshot commits atomically");
    assert_eq!(apply_report.fact_count, 1);
    assert_eq!(apply_report.relation_count, 1);
    let durable_snapshot = MatrixStore::get_source_snapshot(&target, "pg-transactional-snapshot")
        .expect("committed PostgreSQL snapshot reads")
        .expect("committed PostgreSQL snapshot exists");
    assert!(durable_snapshot.metadata.get("chunk_receipt").is_some());

    let invalid_definition = MatrixMetricDefinition::inferred_for_measure(
        "invalid_units",
        "manufacturing.invalid_units",
        "units",
    );
    source
        .register_metric_definition(&invalid_definition)
        .expect("sqlite invalid definition saves");
    MatrixStore::register_metric_definition(&target, &invalid_definition)
        .expect("postgres invalid definition saves");
    let invalid_fact = MatrixFact::from_input(MatrixFactInput {
        fact_id: Some("invalid-units-null".to_string()),
        snapshot_id: Some("invalid-units-snapshot".to_string()),
        fact_type: "manufacturing.invalid_units".to_string(),
        entity_refs: vec!["line:null".to_string()],
        metric_key: Some("invalid_units".to_string()),
        dimensions: serde_json::json!({"period": "2026-W30"}),
        measures: serde_json::json!({"units": null}),
        event_time: Some(Utc::now()),
        valid_from: None,
        valid_to: None,
        source_ref: Some("mes://line/null".to_string()),
        confidence: Some(0.8),
        raw_hash: Some("sha256:invalid-units-null".to_string()),
    });
    source
        .ingest_fact(&invalid_fact)
        .expect("sqlite invalid fact saves");
    MatrixStore::ingest_fact(&target, &invalid_fact).expect("postgres invalid fact saves");
    let only_invalid = vec!["invalid_units".to_string()];
    assert!(source
        .recompute_metrics_for_metric_ids(&only_invalid)
        .is_err());
    assert!(MatrixStore::recompute_metrics_for_metric_ids(&target, &only_invalid).is_err());
    assert!(
        copy_quiesced_matrix_store(&source, &target, manifest_root.path().join("again.json"))
            .is_err()
    );
}
