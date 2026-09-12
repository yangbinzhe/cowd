//! Real PostgreSQL read-model gates; fixed journal seeds are not business E2E.
use std::sync::Arc;
use std::time::Instant;

use harness_contract::agent_action::*;
use runtime::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventScope,
    RuntimeEventStore, RuntimeServices, RuntimeTransactionEventInput,
};
use runtime_postgres::{PostgresArtifactRepository, PostgresRuntimeEventStore, PostgresTaskStore};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn append_messages(store: &RuntimeEventStore, program: &str, count: u64) {
    let stream = format!("agentic-program:{program}");
    let head = store.stream_revision(&stream).unwrap();
    let events = (1..=count)
        .map(|offset| {
            let id = format!("message:{:06}", head + offset);
            let envelope = AgentActionEnvelope {
                action_id: id.clone(),
                expected_revision: None,
                actor: AgentActorBinding {
                    objective_id: "fixed-objective".into(),
                    program_id: program.into(),
                    session_id: "fixed-session".into(),
                    turn_id: "fixed-turn".into(),
                    root_execution_id: None,
                    required_team_count: 0,
                    objective_summary: "fixed source workload".into(),
                    model_lease: "fixture-only".into(),
                    permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                    resource_scopes: vec![],
                    actor_id: "fixture-root".into(),
                    kind: AgentActorKind::Root,
                    execution_id: None,
                    team_id: None,
                    agent_id: None,
                },
                action: AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{program}"),
                    summary: Some(format!("完整来源 {id}")),
                    content_ref: None,
                    refs: vec![],
                    recipients: vec![],
                    intent: None,
                    issue_dispositions: vec![],
                }),
            };
            RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: stream.clone(),
                    scope: RuntimeEventScope::Program,
                    kind: "agentic.action_applied".into(),
                    status: Some("applied".into()),
                    actor: Some("fixture-root".into()),
                    refs: vec![],
                    payload: json!({"envelope":envelope,"entity_ref":id}),
                },
                idempotency_key: Some(id),
                schema_version: 1,
            }
        })
        .collect();
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: format!("{program}:{head}:{count}"),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: stream,
                expected_revision: head,
            }],
            events,
        })
        .unwrap();
}

fn scan_facts(plan: &Value, nodes: &mut Vec<String>, removed: &mut u64) {
    if let Some(node) = plan["Node Type"].as_str() {
        nodes.push(node.into());
    }
    *removed += plan["Rows Removed by Filter"].as_u64().unwrap_or(0);
    if let Some(children) = plan["Plans"].as_array() {
        for child in children {
            scan_facts(child, nodes, removed);
        }
    }
}

#[test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
fn production_program_read_model_has_bounded_warm_queries_and_recovers_corrupt_snapshots() {
    let fixture = PostgresTestScope::new();
    let executor = fixture.reconnect();
    let store = Arc::new(
        PostgresRuntimeEventStore::new(executor.clone())
            .unwrap()
            .into_runtime_event_store(),
    );
    let temp = tempfile::tempdir().unwrap();
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let services = RuntimeServices::builder(temp.path().join("home"), &workspace)
        .runtime_event_store(Arc::clone(&store))
        .artifact_store(Arc::new(
            runtime::ArtifactStore::new(
                temp.path().join("artifacts"),
                Arc::new(PostgresArtifactRepository::new(fixture.reconnect()).unwrap()),
                runtime::ArtifactStoreConfig::default(),
            )
            .unwrap(),
        ))
        .task_aggregate_service(Arc::new(
            PostgresTaskStore::new(fixture.reconnect())
                .unwrap()
                .into_task_service(),
        ))
        .build()
        .unwrap();
    let actions = services.agent_action_service();
    let mut query_counts = std::collections::BTreeMap::new();
    for history in [100u64, 1_000, 10_000] {
        let program = format!("pg-read-{history}");
        let stream = format!("agentic-program:{program}");
        store.append(RuntimeEventInput {stream_id: stream.clone(), scope: RuntimeEventScope::Program,
            kind: "agentic.program_opened".into(), status: Some("open".into()), actor: Some("fixture-root".into()), refs: vec![],
            payload: json!({"objective_id":"fixed-objective","session_id":"fixed-session","turn_id":"fixed-turn",
                "required_team_count":0,"objective_summary":"fixed source workload","model_lease":"fixture-only",
                "permission_ceiling":harness_contract::policy::PermissionMode::ReadOnly,"resource_scopes":[]})}).unwrap();
        append_messages(&store, &program, history - 1);
        let started = Instant::now();
        let initial = actions.project_snapshot(&program).unwrap();
        let cold_us = started.elapsed().as_micros();
        assert_eq!(initial.revision, history);
        let mut samples = vec![];
        for repeat in 0..5 {
            let expected = actions.project_snapshot(&program).unwrap();
            let before = executor.health().metrics.query_count;
            let started = Instant::now();
            let warm = actions.project_snapshot(&program).unwrap();
            let warm_us = started.elapsed().as_micros();
            let warm_queries = executor.health().metrics.query_count - before;
            assert!(Arc::ptr_eq(&expected, &warm));
            assert_eq!(
                warm_queries, 1,
                "warm reads must only validate the durable head"
            );
            let mut deltas = vec![];
            for count in [1u64, 10] {
                let previous = actions.project_snapshot(&program).unwrap();
                append_messages(&store, &program, count);
                let before = executor.health().metrics.query_count;
                let started = Instant::now();
                let current = actions.project_snapshot(&program).unwrap();
                let elapsed_us = started.elapsed().as_micros();
                let queries = executor.health().metrics.query_count - before;
                assert_eq!(current.revision, previous.revision + count);
                assert_eq!(
                    current.topics[&format!("topic:{program}")].len(),
                    (current.revision - 1) as usize
                );
                assert_eq!(
                    previous.topics[&format!("topic:{program}")].len(),
                    (previous.revision - 1) as usize
                );
                let expected_queries = query_counts.entry(count).or_insert(queries);
                assert_eq!(
                    queries, *expected_queries,
                    "PG query count cannot grow with Program history"
                );
                let page = store
                    .list_stream_after(
                        &stream,
                        previous.revision,
                        current.revision,
                        10_000,
                        32 * 1024 * 1024,
                    )
                    .unwrap();
                assert_eq!(page.len(), count as usize);
                let plan: Value = executor.checkout_online_read().unwrap().query_one(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) SELECT * FROM runtime_events WHERE stream_id=$1 AND sequence>$2 AND sequence<=$3 ORDER BY sequence ASC LIMIT 10000",
                    &[&stream, &(previous.revision as i64), &(current.revision as i64)]).unwrap().get(0);
                let mut nodes = vec![];
                let mut removed = 0;
                scan_facts(&plan[0]["Plan"], &mut nodes, &mut removed);
                if history >= 1_000 {
                    assert!(nodes.iter().any(|node| node.contains("Index")), "{plan}");
                    assert_eq!(
                        removed, 0,
                        "delta reads must not filter the historical prefix: {plan}"
                    );
                }
                deltas.push(json!({"delta":count,"elapsed_us":elapsed_us,"queries":queries,
                    "rows_returned":page.len(),"scan_nodes":nodes,"historical_rows_filtered":removed}));
            }
            samples.push(json!({"repeat":repeat,"warm_us":warm_us,"warm_queries":warm_queries,"deltas":deltas}));
        }
        println!(
            "PG_PROGRAM_READ_MATRIX {}",
            json!({"initial_events":history,"cold_us":cold_us,
            "backend":"postgres_production_composition","samples":samples,"snapshot_recovery":"pending"})
        );
        let expected = actions.project_snapshot(&program).unwrap();
        let clean_cold = runtime::AgentActionService::new(Arc::clone(&store));
        let before = executor.health().metrics.query_count;
        assert_eq!(
            clean_cold.project_snapshot(&program).unwrap().as_ref(),
            expected.as_ref()
        );
        assert_eq!(
            executor.health().metrics.query_count - before,
            2,
            "a valid PG snapshot must load without replaying the full journal"
        );
        let checkpoint_id = format!(
            "runtime:agentic-read-model:v1:{:x}",
            Sha256::digest(program.as_bytes())
        );
        for case in ["body", "ahead"] {
            let checkpoint = store
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap();
            let mut damaged = checkpoint.payload;
            let mut cursor = checkpoint.source_cursor;
            if case == "body" {
                damaged["projection"]["objective_summary"] = json!("corrupted valid JSON");
            } else {
                cursor += 10;
                damaged["projection"]["revision"] = json!(cursor);
                damaged["sha256"] = json!(format!(
                    "sha256:{:x}",
                    Sha256::digest(damaged["projection"].to_string().as_bytes())
                ));
            }
            let damaged_checkpoint = store
                .compare_and_put_projection_checkpoint(
                    &checkpoint_id,
                    cursor,
                    checkpoint.revision,
                    &damaged,
                    1,
                )
                .unwrap();
            if case == "ahead" {
                assert!(matches!(
                    store.compare_and_put_projection_checkpoint(
                        &checkpoint_id,
                        expected.revision,
                        damaged_checkpoint.revision,
                        &json!(null),
                        2,
                    ),
                    Err(runtime::RuntimeEventStoreError::StaleRevision { .. })
                ));
            }
            let cold_actions = runtime::AgentActionService::new(Arc::clone(&store));
            assert_eq!(
                cold_actions.project_snapshot(&program).unwrap().as_ref(),
                expected.as_ref()
            );
            assert_ne!(
                store
                    .projection_checkpoint(&checkpoint_id)
                    .unwrap()
                    .unwrap()
                    .payload,
                damaged
            );
            assert_eq!(
                store
                    .projection_checkpoint(&checkpoint_id)
                    .unwrap()
                    .unwrap()
                    .source_cursor,
                expected.revision,
                "{case}"
            );
            let repaired = store
                .projection_checkpoint(&checkpoint_id)
                .unwrap()
                .unwrap();
            assert!(repaired.revision > damaged_checkpoint.revision);
            assert!(matches!(
                store.compare_and_repair_projection_checkpoint(
                    &checkpoint_id,
                    0,
                    damaged_checkpoint.revision,
                    &json!(null),
                    3,
                ),
                Err(runtime::RuntimeEventStoreError::StaleRevision { .. })
            ));
            assert_eq!(
                store
                    .projection_checkpoint(&checkpoint_id)
                    .unwrap()
                    .unwrap(),
                repaired
            );
            assert_eq!(store.stream_revision(&stream).unwrap(), expected.revision);
        }
        println!(
            "PG_PROGRAM_READ_MATRIX {}",
            json!({"initial_events":history,"cold_us":cold_us,
            "backend":"postgres_production_composition","samples":samples,"corrupt_snapshot_rebuilt":true})
        );
    }
}
