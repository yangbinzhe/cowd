use super::*;
use matrix_core::{
    MatrixEntityInput, MatrixMetricDependencyInput, MatrixRelationInput,
    MatrixScenarioOutputContract, MatrixSnapshotRef, MatrixSourceEntityMapping,
    MatrixSourceFactMapping, MatrixSourceKind, MatrixSourceRelationMapping,
};

fn minimal_source_pack(id: &str) -> MatrixSourcePack {
    MatrixSourcePack {
        source_pack_id: id.to_string(),
        source_name: "revision-fixture".to_string(),
        owner: "test".to_string(),
        access_mode: "manual".to_string(),
        refresh_mode: "snapshot".to_string(),
        entity_mappings: Vec::new(),
        fact_mappings: Vec::new(),
        relation_mappings: Vec::new(),
        reconciliation_rules: Vec::new(),
        quality_rules: Vec::new(),
        freshness_sla: None,
        security_policy: None,
        metadata: Value::Null,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    }
}

#[test]
fn bounded_recall_finds_authorized_snapshot_beyond_global_latest_window() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let base = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    repository
        .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
            fact_id: Some("matrix-authorized-old".to_string()),
            snapshot_id: Some("snapshot-authorized".to_string()),
            fact_type: "supply.recall-needle".to_string(),
            entity_refs: Vec::new(),
            metric_key: Some("authorized_metric".to_string()),
            dimensions: serde_json::json!({"region":"east"}),
            measures: serde_json::json!({"value":1}),
            event_time: Some(base),
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.9),
            raw_hash: None,
        }))
        .unwrap();
    repository
        .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
            fact_id: Some("matrix-authorized-lower".to_string()),
            snapshot_id: Some("snapshot-authorized".to_string()),
            fact_type: "supply.recall-needle".to_string(),
            entity_refs: Vec::new(),
            metric_key: Some("authorized_metric".to_string()),
            dimensions: Value::Null,
            measures: serde_json::json!({"value":0}),
            event_time: Some(base + chrono::Duration::seconds(1)),
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.8),
            raw_hash: None,
        }))
        .unwrap();
    for index in 0..600 {
        repository
            .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
                fact_id: Some(format!("matrix-new-{index:03}")),
                snapshot_id: Some("snapshot-forbidden".to_string()),
                fact_type: "supply.recall-needle".to_string(),
                entity_refs: Vec::new(),
                metric_key: None,
                dimensions: Value::Null,
                measures: serde_json::json!({"value": index}),
                event_time: Some(base + chrono::Duration::seconds(i64::from(index) + 1)),
                valid_from: None,
                valid_to: None,
                source_ref: None,
                confidence: Some(1.0),
                raw_hash: None,
            }))
            .unwrap();
    }

    let result = repository
        .recall_facts(&MatrixRecallQuery::new(
            vec!["snapshot-authorized".to_string()],
            "recall-needle",
            1,
        ))
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].fact_id, "matrix-authorized-old");
    assert!(repository
        .recall_facts(&MatrixRecallQuery::new(Vec::new(), "", 8))
        .unwrap()
        .is_empty());
}

#[test]
fn sqlite_metric_query_uses_the_registered_formula_and_explicit_operands() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let mut definition = MatrixMetricDefinition::inferred_for_measure(
        "work_center_load",
        "manufacturing.work_center_load",
        "load_hours",
    );
    definition.formula_ref = matrix_core::MATRIX_FORMULA_RATIO_PERCENT_V1.to_string();
    definition.denominator_measure = Some("capacity_hours".to_string());
    repository
        .register_metric_definition(&definition)
        .expect("definition saves");
    repository
        .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
            fact_id: Some("load-fact".to_string()),
            snapshot_id: Some("load-snapshot".to_string()),
            fact_type: "manufacturing.work_center_load".to_string(),
            entity_refs: vec!["work-center:one".to_string()],
            metric_key: Some("work_center_load".to_string()),
            dimensions: serde_json::json!({"week": "2026-W30"}),
            measures: serde_json::json!({"load_hours": 188, "capacity_hours": 160}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.9),
            raw_hash: None,
        }))
        .expect("fact saves");

    let result = repository.recompute_metrics().expect("query executes");

    assert_eq!(result.metric_states.len(), 1);
    assert!((result.metric_states[0].value - 117.5).abs() < f64::EPSILON);
}

#[test]
fn compute_job_applies_metric_entity_and_period_scope() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let definition = MatrixMetricDefinition::inferred_for_measure(
        "scoped_output",
        "manufacturing.output",
        "value",
    );
    repository
        .register_metric_definition(&definition)
        .expect("definition saves");
    for (id, entity, period, value) in [
        ("scope-hit", "line-a", "2026-W30", 7),
        ("wrong-entity", "line-b", "2026-W30", 11),
        ("wrong-period", "line-a", "2026-W31", 13),
    ] {
        repository
            .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
                fact_id: Some(id.to_string()),
                snapshot_id: Some("scope-snapshot".to_string()),
                fact_type: "manufacturing.output".to_string(),
                entity_refs: vec![entity.to_string()],
                metric_key: Some("scoped_output".to_string()),
                dimensions: serde_json::json!({"period": period}),
                measures: serde_json::json!({"value": value}),
                event_time: None,
                valid_from: None,
                valid_to: None,
                source_ref: None,
                confidence: Some(0.9),
                raw_hash: None,
            }))
            .expect("fact saves");
    }
    let plan = repository
        .plan_compute_job_for_fact_type(MatrixComputeJobInput {
            job_id: Some("scoped-compute-job".to_string()),
            trigger_fact_type: "manufacturing.output".to_string(),
            trigger_fact_refs: Vec::new(),
            entity_scope: Some("line-a".to_string()),
            period: Some("2026-W30".to_string()),
            metric_ids: vec!["scoped_output".to_string()],
            priority: None,
        })
        .expect("job plans");
    let completed = repository
        .run_compute_job(&plan.job.job_id)
        .expect("job runs");
    assert_eq!(completed.result_summary["metric_state_count"], 1);
    let states = repository.metric_states("scoped_output").unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].entity_scope, "line-a");
    assert_eq!(states[0].period, "2026-W30");
    assert!((states[0].value - 7.0).abs() < f64::EPSILON);
}

#[test]
fn sqlite_metric_query_pushes_target_period_and_entity_into_100k_scan() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let definition = MatrixMetricDefinition::inferred_for_measure(
        "metric-042",
        "manufacturing.performance",
        "value",
    );
    repository
        .register_metric_definition(&definition)
        .expect("definition saves");

    let mut connection = repository.executor.checkout().unwrap();
    let transaction = connection.transaction().unwrap();
    {
        let mut insert = transaction
            .prepare(
                "INSERT INTO matrix_fact (
                        fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
                        dimensions_json, measures_json, event_time, valid_from, valid_to,
                        source_ref, confidence, raw_hash, created_at
                     ) VALUES (?1, 'perf-snapshot', 'manufacturing.performance', ?2, ?3,
                               ?4, ?5, ?6, NULL, NULL, 'perf-fixture', 0.9, ?7, ?6)",
            )
            .unwrap();
        for index in 0..100_000usize {
            let metric_id = format!("metric-{:03}", index % 100);
            let entity_refs = format!(r#"["entity-{}"]"#, index % 10);
            let period = format!(r#"{{"period":"2026-W{}"}}"#, 30 + index % 3);
            let measures = format!(r#"{{"value":{}}}"#, index % 17);
            let fact_id = format!("perf-fact-{index:06}");
            insert
                .execute(params![
                    fact_id,
                    entity_refs,
                    metric_id,
                    period,
                    measures,
                    "2026-08-01T00:00:00+00:00",
                    format!("perf-hash-{index:06}"),
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();

    let filter = BTreeSet::from(["metric-042".to_string()]);
    let full_started = std::time::Instant::now();
    let all_rows = metric_source_rows(&connection, None, None, None).unwrap();
    let full_elapsed = full_started.elapsed();
    let expected_ids = all_rows
        .iter()
        .filter(|row| {
            row.metric_id == "metric-042"
                && row.entity_scope == "entity-2"
                && row.period == "2026-W31"
        })
        .map(|row| row.fact_id.clone())
        .collect::<Vec<_>>();

    let mut samples = Vec::new();
    let mut targeted_rows = Vec::new();
    for _ in 0..21 {
        let started = std::time::Instant::now();
        targeted_rows = metric_source_rows(
            &connection,
            Some(&filter),
            Some("entity-2"),
            Some("2026-W31"),
        )
        .unwrap();
        samples.push(started.elapsed());
    }
    let actual_ids = targeted_rows
        .iter()
        .map(|row| row.fact_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(actual_ids, expected_ids);
    assert_eq!(all_rows.len(), 100_000);
    assert!(targeted_rows.len() < all_rows.len() / 100);

    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p95 = samples[(samples.len() - 1) * 95 / 100];
    let p99 = samples[(samples.len() - 1) * 99 / 100];
    assert!(p95 < full_elapsed);

    let targeted_results = metric_query_results(
        &connection,
        Some(&filter),
        Some("entity-2"),
        Some("2026-W31"),
    )
    .unwrap();
    assert_eq!(targeted_results.len(), 1);
    let expected_sum = targeted_rows
        .iter()
        .map(|row| row.measures["value"].as_f64().unwrap())
        .sum::<f64>();
    assert!((targeted_results[0].value - expected_sum).abs() < f64::EPSILON);

    let query_plan = connection
        .prepare(
            "EXPLAIN QUERY PLAN
                 SELECT fact_id FROM matrix_fact
                 WHERE metric_key = ?1
                   AND COALESCE(json_extract(entity_refs_json, '$[0]'), 'enterprise') = ?2
                   AND COALESCE(
                        json_extract(dimensions_json, '$.period'),
                        json_extract(dimensions_json, '$.week'),
                        'current'
                   ) = ?3
                 ORDER BY metric_key, event_time, fact_id",
        )
        .unwrap()
        .query_map(params!["metric-042", "entity-2", "2026-W31"], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(query_plan
        .iter()
        .any(|step| step.contains("idx_matrix_fact_metric_scope_period_time")));
    eprintln!(
            "matrix_100k all_rows={} targeted_rows={} full={full_elapsed:?} p50={p50:?} p95={p95:?} p99={p99:?}",
            all_rows.len(),
            targeted_rows.len(),
        );
}

#[test]
fn unregistered_multi_measure_metric_fails_closed() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    repository
        .ingest_fact(&MatrixFact::from_input(matrix_core::MatrixFactInput {
            fact_id: Some("ambiguous-fact".to_string()),
            snapshot_id: Some("ambiguous-snapshot".to_string()),
            fact_type: "manufacturing.ambiguous".to_string(),
            entity_refs: vec!["work-center:one".to_string()],
            metric_key: Some("ambiguous_metric".to_string()),
            dimensions: serde_json::json!({"week": "2026-W30"}),
            measures: serde_json::json!({"load": 188, "capacity": 160}),
            event_time: None,
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: Some(0.9),
            raw_hash: None,
        }))
        .expect("fact saves");

    assert!(matches!(
        repository.recompute_metrics(),
        Err(MatrixSqliteRepositoryError::InvalidMetricQuery(message))
            if message.contains("register one explicit measure")
    ));
}

#[test]
fn data_plane_ingest_plan_includes_metric_declared_by_source_pack() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let source_pack_id = "source-pack-ingest-metric";
    let mut source_pack = minimal_source_pack(source_pack_id);
    source_pack.fact_mappings = vec![MatrixSourceFactMapping {
        source_table: "manufacturing_events".to_string(),
        fact_type: "manufacturing.event".to_string(),
        metric_key: "manufacturing_event_count".to_string(),
        entity_ref_fields: vec!["asset_id".to_string()],
        measure_fields: Vec::new(),
        event_time_field: None,
        dedup_key: "event_id".to_string(),
        delta_signature: "updated_at".to_string(),
    }];
    repository
        .upsert_source_pack(source_pack)
        .expect("source pack saves");

    let plan = repository
        .plan_data_plane_ingest(MatrixDataPlaneIngestPlanInput {
            source_ref: format!("source-pack://{source_pack_id}"),
            fact_type: "manufacturing.event".to_string(),
            partition_ref: None,
            high_watermark: None,
            estimated_rows: None,
            raw_checksum: None,
            expected_revision: None,
            adapter_id: None,
            strategy: None,
            table: None,
            cursor: None,
            offset: None,
            metric_ids: Vec::new(),
        })
        .expect("ingest plan builds");

    assert!(plan
        .affected_metric_ids
        .iter()
        .any(|metric_id| metric_id == "manufacturing_event_count"));
    assert!(plan.compute_jobs.iter().any(|job| {
        job.metric_ids
            .iter()
            .any(|metric_id| metric_id == "manufacturing_event_count")
    }));
}

#[test]
fn data_plane_watermark_commit_uses_revision_cas() {
    let repository = MatrixSqliteRepository::in_memory().expect("repository opens");
    let input = |expected_revision, cursor: &str| MatrixDataPlaneIngestPlanInput {
        source_ref: "bitable://app/orders".to_string(),
        fact_type: "source.feishu_bitable.row".to_string(),
        partition_ref: Some("orders".to_string()),
        high_watermark: Some(cursor.to_string()),
        estimated_rows: Some(2),
        raw_checksum: Some(format!("sha256:rows:{cursor}")),
        expected_revision,
        adapter_id: Some("feishu_bitable".to_string()),
        strategy: Some("cursor_field".to_string()),
        table: Some("orders".to_string()),
        cursor: Some(cursor.to_string()),
        offset: Some(2),
        metric_ids: Vec::new(),
    };

    let first = repository
        .plan_data_plane_ingest(input(None, "cursor-1"))
        .expect("first plan");
    let committed = repository
        .commit_data_plane_ingest(&first)
        .expect("first commit");
    assert_eq!(committed.revision, 1);

    let replay = repository
        .plan_data_plane_ingest(input(None, "cursor-1"))
        .expect("replay plan");
    assert_eq!(
        repository
            .commit_data_plane_ingest(&replay)
            .expect("same batch replay")
            .revision,
        1
    );

    let stale = repository
        .plan_data_plane_ingest(input(None, "cursor-2"))
        .expect("stale plan");
    assert!(matches!(
        repository.commit_data_plane_ingest(&stale),
        Err(MatrixSqliteRepositoryError::RevisionConflict {
            expected: None,
            actual: Some(1),
            ..
        })
    ));

    let second = repository
        .plan_data_plane_ingest(input(Some(1), "cursor-2"))
        .expect("second plan");
    let committed = repository
        .commit_data_plane_ingest(&second)
        .expect("second commit");
    assert_eq!(committed.revision, 2);
    let loaded = repository
        .get_data_plane_watermark(
            "bitable://app/orders",
            "source.feishu_bitable.row",
            "orders",
        )
        .expect("load")
        .expect("watermark");
    assert_eq!(loaded.revision, 2);
    assert_eq!(loaded.cursor.as_deref(), Some("cursor-2"));
}

#[test]
fn checked_matrix_upserts_require_exact_revision_for_all_four_resources() {
    let repository = MatrixSqliteRepository::in_memory().unwrap();

    let source = repository
        .upsert_source_pack_checked(minimal_source_pack("source-revision"), None)
        .unwrap();
    assert!(source.created);
    assert_eq!(source.revision, 1);
    assert!(matches!(
        repository.upsert_source_pack_checked(minimal_source_pack("source-revision"), None),
        Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
    ));
    assert_eq!(
        repository
            .upsert_source_pack_checked(
                minimal_source_pack("source-revision"),
                Some(source.revision),
            )
            .unwrap()
            .revision,
        2
    );

    let left = MatrixEntity::from_input(MatrixEntityInput {
        entity_id: Some("entity-left".to_string()),
        entity_type: "part".to_string(),
        canonical_key: "left".to_string(),
        display_name: None,
        source_keys: Vec::new(),
        attributes: Value::Null,
        confidence: None,
    });
    let entity = repository.upsert_entity_checked(&left, None).unwrap();
    assert_eq!(entity.revision, 1);
    assert!(matches!(
        repository.upsert_entity_checked(&left, None),
        Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
    ));
    assert_eq!(
        repository
            .upsert_entity_checked(&left, Some(entity.revision))
            .unwrap()
            .revision,
        2
    );

    let right = MatrixEntity::from_input(MatrixEntityInput {
        entity_id: Some("entity-right".to_string()),
        entity_type: "part".to_string(),
        canonical_key: "right".to_string(),
        display_name: None,
        source_keys: Vec::new(),
        attributes: Value::Null,
        confidence: None,
    });
    repository.upsert_entity(&right).unwrap();
    let relation = MatrixRelation::from_input(MatrixRelationInput {
        relation_id: Some("relation-revision".to_string()),
        relation_type: "depends_on".to_string(),
        from_entity_id: left.entity_id.clone(),
        to_entity_id: right.entity_id.clone(),
        attributes: Value::Null,
        confidence: None,
    });
    let relation = repository.upsert_relation_checked(&relation, None).unwrap();
    assert_eq!(relation.revision, 1);
    assert!(matches!(
        repository.upsert_relation_checked(&relation.resource, None),
        Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
    ));
    assert_eq!(
        repository
            .upsert_relation_checked(&relation.resource, Some(relation.revision))
            .unwrap()
            .revision,
        2
    );

    let dependency = MatrixMetricDependency::from_input(MatrixMetricDependencyInput {
        dependency_id: Some("dependency-revision".to_string()),
        upstream_metric_id: "metric-a".to_string(),
        downstream_metric_id: "metric-b".to_string(),
        dependency_type: "derived_from".to_string(),
        entity_relation_type: None,
        required_fact_types: Vec::new(),
        transformation_ref: None,
        confidence: None,
        notes: None,
    });
    let dependency = repository
        .upsert_metric_dependency_checked(&dependency, None)
        .unwrap();
    assert_eq!(dependency.revision, 1);
    assert!(matches!(
        repository.upsert_metric_dependency_checked(&dependency.resource, None),
        Err(MatrixSqliteRepositoryError::RevisionConflict { .. })
    ));
    assert_eq!(
        repository
            .upsert_metric_dependency_checked(&dependency.resource, Some(dependency.revision),)
            .unwrap()
            .revision,
        2
    );
}

#[test]
fn entity_match_preview_is_pure_and_decision_materializes_the_stable_candidate() {
    let repository = MatrixSqliteRepository::in_memory().unwrap();
    for entity_id in ["entity-preview-left", "entity-preview-right"] {
        repository
            .upsert_entity(&MatrixEntity::from_input(MatrixEntityInput {
                entity_id: Some(entity_id.to_string()),
                entity_type: "part".to_string(),
                canonical_key: entity_id.to_string(),
                display_name: Some("Shared preview identity".to_string()),
                source_keys: Vec::new(),
                attributes: Value::Null,
                confidence: None,
            }))
            .unwrap();
    }

    let first = repository
        .propose_entity_match("entity-preview-left", "entity-preview-right")
        .unwrap();
    let second = repository
        .propose_entity_match("entity-preview-left", "entity-preview-right")
        .unwrap();
    assert_eq!(first.candidate_id, second.candidate_id);
    let preview_health = repository.health().unwrap();
    assert_eq!(preview_health.entity_match_candidate_count, 0);
    assert_eq!(preview_health.entity_conflict_decision_count, 0);

    let decision = repository
        .decide_entity_conflict(
            &first.candidate_id,
            "entity-preview-left",
            "entity-preview-right",
            "prefer_verified_source",
            Some("governed commit".to_string()),
        )
        .unwrap();
    assert_eq!(decision.candidate_id, first.candidate_id);
    let committed_health = repository.health().unwrap();
    assert_eq!(committed_health.entity_match_candidate_count, 1);
    assert_eq!(committed_health.entity_conflict_decision_count, 1);
}

#[test]
fn source_snapshot_apply_maps_rows_to_matrix_records_idempotently() {
    let repository = MatrixSqliteRepository::in_memory().unwrap();
    let source_pack = MatrixSourcePack {
        source_pack_id: "source-pack-supply-orders".to_string(),
        source_name: "supply_fixture".to_string(),
        owner: "test".to_string(),
        access_mode: "file".to_string(),
        refresh_mode: "snapshot".to_string(),
        entity_mappings: vec![
            MatrixSourceEntityMapping {
                source_entity: "supplier".to_string(),
                matrix_entity_type: "supplier".to_string(),
                source_key_field: "supplier_id".to_string(),
            },
            MatrixSourceEntityMapping {
                source_entity: "part".to_string(),
                matrix_entity_type: "part".to_string(),
                source_key_field: "part_id".to_string(),
            },
        ],
        fact_mappings: vec![MatrixSourceFactMapping {
            source_table: "orders".to_string(),
            fact_type: "supply.order".to_string(),
            metric_key: "supply_qty".to_string(),
            entity_ref_fields: vec!["supplier_id".to_string(), "part_id".to_string()],
            measure_fields: vec!["qty".to_string()],
            event_time_field: Some("event_time".to_string()),
            dedup_key: "order_id".to_string(),
            delta_signature: "order_id".to_string(),
        }],
        relation_mappings: vec![MatrixSourceRelationMapping {
            source_table: "orders".to_string(),
            relation_type: "supplies".to_string(),
            from_source_key_field: "supplier_id".to_string(),
            to_source_key_field: "part_id".to_string(),
            attribute_fields: vec!["qty".to_string()],
            dedup_key: "order_id".to_string(),
        }],
        reconciliation_rules: vec!["source_snapshot_is_idempotent".to_string()],
        quality_rules: vec!["dedup_key_required".to_string()],
        freshness_sla: Some("manual".to_string()),
        security_policy: Some("test_fixture".to_string()),
        metadata: serde_json::json!({"fixture": true}),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    repository.upsert_source_pack(source_pack).unwrap();

    let rows = vec![
        serde_json::json!({
            "order_id": "O1",
            "supplier_id": "S1",
            "part_id": "P1",
            "qty": 12,
            "event_time": "2026-07-02T00:00:00Z"
        }),
        serde_json::json!({
            "order_id": "O2",
            "supplier_id": "S2",
            "part_id": "P2",
            "qty": 4,
            "event_time": "2026-07-02T01:00:00Z"
        }),
    ];
    let snapshot = repository
        .create_source_snapshot(MatrixSourceSnapshotInput {
            snapshot_id: Some("snapshot-source-orders-1".to_string()),
            source_pack_id: Some("source-pack-supply-orders".to_string()),
            source_system: "supply_fixture".to_string(),
            source_kind: MatrixSourceKind::File,
            resource_ref: Some("file://orders.csv".to_string()),
            business_period: None,
            captured_at: None,
            schema_version: Some("source:csv:orders".to_string()),
            row_count: Some(rows.len() as u64),
            checksum: Some("sha256:test".to_string()),
            confidence: Some(0.96),
            metadata: Value::Null,
        })
        .unwrap();

    let report = repository
        .apply_source_snapshot_rows("source-pack-supply-orders", snapshot.clone(), &rows)
        .unwrap();
    assert_eq!(report.fact_count, 2);
    assert_eq!(report.relation_count, 2);
    assert!(report.warnings.is_empty());

    let supplier = repository
        .resolve_entity_by_source_key("supply_fixture", "S1")
        .unwrap()
        .expect("supplier entity should be indexed by source key");
    let relations = repository
        .list_entity_relations(&supplier.entity_id, 10)
        .unwrap();
    assert_eq!(relations.len(), 1);
    assert_eq!(relations[0].relation_type, "supplies");

    let facts = repository.list_facts(10).unwrap();
    assert_eq!(facts.len(), 2);
    assert!(facts.iter().all(|fact| {
        fact.entity_refs
            .iter()
            .any(|reference| reference.starts_with("matrix:entity:entity-"))
    }));

    let snapshots = repository
        .list_source_snapshots(Some("source-pack-supply-orders"), 10)
        .unwrap();
    assert_eq!(snapshots.len(), 1);
    let durable_receipt: MatrixSourceSnapshotApplyReport = serde_json::from_value(
        snapshots[0]
            .metadata
            .get("chunk_receipt")
            .cloned()
            .expect("transactional chunk receipt"),
    )
    .unwrap();
    assert_eq!(durable_receipt.fact_count, 2);
    assert_eq!(durable_receipt.snapshot_id, snapshots[0].snapshot_id);

    repository
        .apply_source_snapshot_rows("source-pack-supply-orders", snapshot, &rows)
        .unwrap();
    let health = repository.health().unwrap();
    assert_eq!(health.source_snapshot_count, 1);
    assert_eq!(health.fact_count, 2);
    assert_eq!(health.relation_count, 2);
    assert_eq!(health.attention_count, 2);

    let connection = repository.executor.checkout().unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_source_receipt
                 BEFORE INSERT ON matrix_source_snapshot
                 WHEN instr(NEW.snapshot_json, '\"chunk_receipt\"') > 0
                 BEGIN SELECT RAISE(ABORT, 'injected source apply failure'); END;",
        )
        .unwrap();
    drop(connection);
    let failed_snapshot = MatrixSourceSnapshot::from_input(MatrixSourceSnapshotInput {
        snapshot_id: Some("snapshot-source-orders-failed".to_string()),
        source_pack_id: Some("source-pack-supply-orders".to_string()),
        source_system: "supply_fixture".to_string(),
        source_kind: MatrixSourceKind::File,
        resource_ref: Some("file://orders-failed.csv".to_string()),
        business_period: None,
        captured_at: None,
        schema_version: Some("source:csv:orders".to_string()),
        row_count: Some(rows.len() as u64),
        checksum: Some("sha256:failed".to_string()),
        confidence: Some(0.96),
        metadata: Value::Null,
    });
    assert!(repository
        .apply_source_snapshot_rows("source-pack-supply-orders", failed_snapshot, &rows,)
        .is_err());
    assert!(repository
        .get_source_snapshot("snapshot-source-orders-failed")
        .unwrap()
        .is_none());
    let health_after_failure = repository.health().unwrap();
    assert_eq!(health_after_failure.source_snapshot_count, 1);
    assert_eq!(health_after_failure.fact_count, 2);
    assert_eq!(health_after_failure.relation_count, 2);
    assert_eq!(health_after_failure.attention_count, 2);
}

#[test]
fn scenario_runs_are_bound_to_an_immutable_snapshot_and_stay_simulated() {
    let repository = MatrixSqliteRepository::in_memory().unwrap();
    let snapshot = repository
        .create_source_snapshot(MatrixSourceSnapshotInput {
            snapshot_id: Some("scenario-snapshot".to_string()),
            source_pack_id: None,
            source_system: "scenario-fixture".to_string(),
            source_kind: MatrixSourceKind::Manual,
            resource_ref: Some("fixture://scenario-input".to_string()),
            business_period: None,
            captured_at: None,
            schema_version: Some("scenario/v1".to_string()),
            row_count: Some(1),
            checksum: Some("fixture-checksum".to_string()),
            confidence: Some(1.0),
            metadata: serde_json::json!({"fixture": true}),
        })
        .unwrap();
    let spec = repository
        .create_scenario_spec(MatrixScenarioSpec::new(
            MatrixSnapshotRef::from_source_snapshot(&snapshot),
            serde_json::json!({"demand_change": 0.25}),
            "runtime/scenario/supply-risk@1",
            MatrixScenarioOutputContract {
                required_outputs: vec!["shortage_risk".to_string()],
                evidence_required: true,
            },
        ))
        .unwrap();
    let run = repository
        .start_scenario_run(&spec.scenario_id, serde_json::json!({"region": "east"}))
        .unwrap();
    let completed = repository
        .complete_scenario_run(MatrixScenarioResult::simulated(
            &run,
            serde_json::json!({"shortage_risk": "high"}),
            vec![snapshot.reference()],
        ))
        .unwrap();

    assert_eq!(completed.boundary, "simulated");
    assert_eq!(
        repository.get_scenario_result(&run.run_id).unwrap(),
        Some(completed)
    );
    assert_eq!(
        repository
            .get_scenario_run(&run.run_id)
            .unwrap()
            .unwrap()
            .status,
        MatrixScenarioRunStatus::Succeeded
    );
    let health = repository.health().unwrap();
    assert_eq!(health.scenario_spec_count, 1);
    assert_eq!(health.scenario_run_count, 1);
    assert_eq!(health.scenario_result_count, 1);
}
