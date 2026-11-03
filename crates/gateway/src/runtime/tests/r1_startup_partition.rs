use super::*;
use futures::FutureExt;
use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

struct TranscriptCorruption483 {
    session_id: String,
    message_id: String,
    original: Value,
}

fn require_owned_namespace_483(topology: &SelectedStorageTopology) {
    let namespace: String = topology
        .postgres_executor
        .checkout_online_read()
        .unwrap()
        .query_one("SELECT current_schema()", &[])
        .unwrap()
        .get(0);
    assert!(namespace.starts_with("cowdtest_"));
    assert!(namespace
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'));
}

impl TranscriptCorruption483 {
    fn damage(topology: &SelectedStorageTopology, session_id: &str) -> Self {
        require_owned_namespace_483(topology);
        let mut connection = topology.postgres_executor.checkout_critical().unwrap();
        let mut transaction = connection.transaction().unwrap();
        let original: Value = transaction.query_one(
            "SELECT to_jsonb(m) FROM session_messages m WHERE session_id=$1 ORDER BY sequence DESC LIMIT 1 FOR UPDATE",
            &[&session_id],
        ).unwrap().get(0);
        let message_id = original["stable_message_id"].as_str().unwrap().to_string();
        let content = original["content_json"].as_str().unwrap();
        assert!(serde_json::from_str::<Vec<Value>>(content).is_ok());
        assert_eq!(transaction.execute(
            "UPDATE session_messages SET content_json='[' WHERE session_id=$1 AND stable_message_id=$2 AND content_json=$3",
            &[&session_id, &message_id, &content],
        ).unwrap(), 1);
        let damaged: Value = transaction.query_one(
            "SELECT to_jsonb(m) FROM session_messages m WHERE session_id=$1 AND stable_message_id=$2",
            &[&session_id, &message_id],
        ).unwrap().get(0);
        let mut expected = original.clone();
        expected["content_json"] = json!("[");
        assert_eq!(damaged, expected);
        transaction.commit().unwrap();
        Self {
            session_id: session_id.into(),
            message_id,
            original,
        }
    }

    fn repair(&self, topology: &SelectedStorageTopology) {
        require_owned_namespace_483(topology);
        let mut connection = topology.postgres_executor.checkout_critical().unwrap();
        let mut transaction = connection.transaction().unwrap();
        let content = self.original["content_json"].as_str().unwrap();
        assert_eq!(transaction.execute(
            "UPDATE session_messages SET content_json=$3 WHERE session_id=$1 AND stable_message_id=$2 AND content_json='['",
            &[&self.session_id, &self.message_id, &content],
        ).unwrap(), 1);
        let repaired: Value = transaction.query_one(
            "SELECT to_jsonb(m) FROM session_messages m WHERE session_id=$1 AND stable_message_id=$2",
            &[&self.session_id, &self.message_id],
        ).unwrap().get(0);
        assert_eq!(repaired, self.original);
        transaction.commit().unwrap();
    }
}

fn ingress_snapshot_483(topology: &SelectedStorageTopology, session_id: &str) -> Value {
    let mut connection = topology.postgres_executor.checkout_online_read().unwrap();
    let outbox: Value = connection.query_one(
        "SELECT COALESCE(jsonb_agg(to_jsonb(o) ORDER BY o.sequence,o.request_id),'[]'::jsonb) FROM session_runtime_outbox o WHERE session_id=$1",
        &[&session_id],
    ).unwrap().get(0);
    let history: Value = connection.query_one(
        "SELECT COALESCE(jsonb_agg(to_jsonb(h) ORDER BY h.history_id),'[]'::jsonb) FROM session_runtime_outbox_history h JOIN session_runtime_outbox o USING(request_id) WHERE o.session_id=$1",
        &[&session_id],
    ).unwrap().get(0);
    json!({"outbox":outbox,"history":history})
}

fn claims_483(
    topology: &SelectedStorageTopology,
    requests: &[String],
) -> Vec<(String, String, String, i64)> {
    topology.postgres_executor.checkout_online_read().unwrap().query(
        "SELECT request_id,actor,detail,attempts FROM session_runtime_outbox_history WHERE request_id=ANY($1::text[]) AND action IN ('claim','reclaim') ORDER BY history_id",
        &[&requests],
    ).unwrap().into_iter().map(|row| (row.get(0), row.get(1), row.get(2), row.get(3))).collect()
}

fn assert_hydration_failure_483(services: &RecoveryServices, damaged: &TranscriptCorruption483) {
    let health = services.supervisor.health();
    assert!(health.accepting);
    assert_eq!(health.recovery.global_failures, 0);
    assert_eq!(
        health.recovery.failed_session_ids,
        BTreeSet::from([damaged.session_id.clone()])
    );
    assert!(health
        .recovery
        .failures
        .iter()
        .any(|error| error.contains(&damaged.session_id)
            && error.contains(&damaged.message_id)
            && error.contains("invalid blocks")));
    assert!(!services.runtime.has_active_session(&damaged.session_id));
    assert_eq!(
        services
            .runtime
            .runtime_services()
            .session_recovery_exclusions(),
        vec![damaged.session_id.clone()]
    );
}

async fn seed_ingress_483(
    services: &RecoveryServices,
    session_id: &str,
    text: &str,
    created_at_ms: u64,
) -> session::SessionRuntimeOutboxRecord {
    let input = uuid::Uuid::new_v4().to_string();
    let request = session::SessionRuntimeOutboxRequest {
        input_id: input.clone(),
        request_id: format!("483:{input}"),
        turn_id: uuid::Uuid::new_v4().to_string(),
        message_id: input,
        session_generation: services
            .topology
            .session_store
            .get_session_input_admission(session_id)
            .await
            .unwrap()
            .unwrap()
            .generation,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        created_at_ms,
        runtime_options_json: None,
    };
    let body = json!([{"type":"text","text":text,"cowd_turn_id":request.turn_id,
        "cowd_turn_ingress_message_id":request.message_id}])
    .to_string();
    let record = services
        .topology
        .session_store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(&body),
            created_at_ms,
            &request,
        )
        .await
        .unwrap();
    assert_eq!(record.status, session::SessionRuntimeInputStatus::Queued);
    assert_eq!(record.attempts, 0);
    assert!(record.claim_token.is_none());
    record
}

async fn plain_request_483(
    provider: &mut LoopbackProvider,
    control_count: &mut usize,
    max_controls: usize,
) -> ProviderRequest {
    tokio::time::timeout(Duration::from_secs(90), async {
        let mut cancelled = 0;
        for _ in 0..max_controls + 9 {
            let request = provider
                .requests
                .recv()
                .await
                .expect("483 loopback channel closed");
            if request.reply.is_closed() {
                cancelled += 1;
                assert!(cancelled <= 8, "483 too many original cancelled requests");
                continue;
            }
            match task_route_control_reply(&request.body)
                .expect("483 original TaskRouter wire contract")
            {
                Some(reply) => {
                    *control_count += 1;
                    assert!(
                        *control_count <= max_controls,
                        "483 exceeded fresh Session routing controls"
                    );
                    provider.task_route_controls += 1;
                    request
                        .reply
                        .send(reply)
                        .expect("483 routing consumer disappeared");
                }
                None => return request,
            }
        }
        panic!("483 bounded loopback reads did not reach the actual model request");
    })
    .await
    .expect("483 actual Session model request exceeded original loopback deadline")
}

async fn completed_ingress_483(
    services: &RecoveryServices,
    original: &session::SessionRuntimeOutboxRecord,
) {
    let record = tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            let record = services
                .session
                .runtime_input(&original.request_id)
                .await
                .unwrap()
                .unwrap();
            if record.status == session::SessionRuntimeInputStatus::Completed
                && services
                    .topology
                    .runtime_event_store
                    .session_terminal(&format!("turn-terminal:{}", record.request_id))
                    .unwrap()
                    .is_some_and(|terminal| terminal.status == "materialized")
            {
                break record;
            }
            assert!(!matches!(
                record.status,
                session::SessionRuntimeInputStatus::Failed
                    | session::SessionRuntimeInputStatus::Blocked
                    | session::SessionRuntimeInputStatus::Cancelled
            ));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("483 original ingress worker must actually complete the healthy input");
    assert_eq!(record.input_id, original.input_id);
    assert_eq!(record.turn_id, original.turn_id);
    assert_eq!(record.message_id, original.message_id);
    assert_eq!(record.sequence, original.sequence);
    assert_eq!(record.session_generation, original.session_generation);
    assert_eq!(record.attempts, 1);
    assert!(record.runtime_commit_cursor.is_some());
    assert!(record.claim_fence_epoch.is_some_and(|epoch| epoch > 0));
    assert!(record.claim_owner.is_some());
    assert!(record.claim_token.is_some());
    assert!(record.claim_expires_at_ms.is_none());
    assert!(record.terminal_at_ms.is_some());
    let claims = claims_483(&services.topology, std::slice::from_ref(&record.request_id));
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].3, 1);
    assert_eq!(record.claim_owner.as_deref(), Some(claims[0].1.as_str()));
    assert_eq!(record.claim_token.as_deref(), Some(claims[0].2.as_str()));
    assert!(claims[0].1.starts_with("gateway-session-input:"));
    assert!(claims[0].2.starts_with(&format!("{}:", record.request_id)));
    let terminal = services
        .topology
        .runtime_event_store
        .session_terminal(&format!("turn-terminal:{}", record.request_id))
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.input_claim_token.as_deref(),
        Some(claims[0].2.as_str())
    );
    assert!(terminal
        .input_claim_revision
        .is_some_and(|revision| revision > 0));
    assert_eq!(
        terminal.input_claim_owner.as_deref(),
        Some(claims[0].1.as_str())
    );
    assert_eq!(
        terminal.session_generation,
        Some(original.session_generation)
    );
    assert_eq!(terminal.input_sequence, Some(original.sequence as u64));
    let graph_id =
        runtime::session_ingress_graph_id(&record.session_id, &record.request_id, &record.turn_id);
    let graph = services
        .runtime
        .runtime_services()
        .graph_state_store()
        .load(&graph_id)
        .unwrap();
    assert_eq!(
        graph.lineage.as_ref().unwrap().session_id,
        original.session_id
    );
    assert!(graph.revision > 0);
    assert!(graph.terminal_presentation.is_some());
}

async fn stop_owner_483(
    owner: &mut Option<RecoveryServices>,
) -> crate::selected_storage::DetachedTestPostgresRestartLease {
    let services = owner.take().unwrap();
    services.supervisor.stop_accepting();
    let execution = services
        .runtime
        .runtime_services()
        .shutdown_execution()
        .await;
    let active_drivers = services
        .runtime
        .runtime_services()
        .execution_health()
        .active_drivers;
    if !execution.errors.is_empty() {
        eprintln!(
            "483 original execution shutdown errors: {:?}",
            execution.errors
        );
    }
    let lease = services.stop().await;
    assert_eq!(execution.forced_aborts, 0);
    assert_eq!(active_drivers, 0);
    assert!(
        execution.errors.is_empty(),
        "483 original Runtime cleanup reported errors: {:?}",
        execution.errors
    );
    lease
}

#[tokio::test]
async fn r1_gateway_startup_local_corruption_keeps_healthy_ingress_before_and_after() {
    use crate::services::session_service::activation::{EnsureSessionRequest, SessionSource};
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let providers = provider_registry(&[&provider]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let mut owner = Some(RecoveryServices::start(root.path(), topology, providers.clone()).await);
    let result = AssertUnwindSafe(async {
        let ids = ["before", "damaged", "after"]
            .map(|label| format!("483-{label}:{}", uuid::Uuid::new_v4()));
        for session_id in &ids {
            let mut request = EnsureSessionRequest::new(
                session_id,
                Some(provider.model.clone()),
                SessionSource::Tui,
            );
            request.owner_principal_id = Some("483-recovery-operator".into());
            owner
                .as_ref()
                .unwrap()
                .session
                .create_user_session(request)
                .await
                .unwrap();
        }
        owner.as_ref().unwrap().supervisor.stop_accepting();
        let now = recovery_now_ms().saturating_sub(10);
        let before = seed_ingress_483(
            owner.as_ref().unwrap(),
            &ids[0],
            "Reply exactly HEALTHY_BEFORE_483.",
            now,
        )
        .await;
        let failed = seed_ingress_483(
            owner.as_ref().unwrap(),
            &ids[1],
            "Reply exactly REPAIRED_483.",
            now + 1,
        )
        .await;
        let after = seed_ingress_483(
            owner.as_ref().unwrap(),
            &ids[2],
            "Reply exactly HEALTHY_AFTER_483.",
            now + 2,
        )
        .await;
        assert!(before.next_attempt_at_ms < failed.next_attempt_at_ms);
        assert!(failed.next_attempt_at_ms < after.next_attempt_at_ms);
        let lease = stop_owner_483(&mut owner).await;
        let topology = lease.rebuild().unwrap();
        drop(lease);
        assert!(
            topology
                .session_store
                .get_session_recovery_manifest(&ids[1])
                .await
                .unwrap()
                .unwrap()
                .in_flight_turn
        );
        let damaged = TranscriptCorruption483::damage(&topology, &ids[1]);
        let frozen_ingress = ingress_snapshot_483(&topology, &ids[1]);
        owner = Some(RecoveryServices::start(root.path(), topology, providers.clone()).await);
        let services = owner.as_ref().unwrap();
        assert_hydration_failure_483(services, &damaged);
        assert!(services.supervisor.health().recovery.required >= 3);
        assert!(services.supervisor.health().recovery.recovered >= 2);
        assert!(services.runtime.has_active_session(&ids[0]));
        assert!(services.runtime.has_active_session(&ids[2]));
        let mut seen = BTreeSet::new();
        let mut control_count = 0;
        for _ in 0..2 {
            let request = plain_request_483(&mut provider, &mut control_count, 2).await;
            let body = request.body.to_string();
            let token = if body.contains("HEALTHY_BEFORE_483") {
                "HEALTHY_BEFORE_483"
            } else if body.contains("HEALTHY_AFTER_483") {
                "HEALTHY_AFTER_483"
            } else {
                panic!("483 only healthy original Sessions may reach the provider");
            };
            assert!(seen.insert(token));
            request
                .reply
                .send(json!({"role":"assistant","content":token}))
                .unwrap();
        }
        completed_ingress_483(services, &before).await;
        completed_ingress_483(services, &after).await;
        assert_eq!(seen.len(), 2);
        assert_eq!(
            ingress_snapshot_483(&services.topology, &ids[1]),
            frozen_ingress
        );
        let claims = claims_483(
            &services.topology,
            &[
                before.request_id.clone(),
                failed.request_id.clone(),
                after.request_id.clone(),
            ],
        );
        assert_eq!(
            claims
                .iter()
                .map(|entry| entry.0.clone())
                .collect::<Vec<_>>(),
            vec![before.request_id.clone(), after.request_id.clone()]
        );
        assert!(claims
            .iter()
            .all(|entry| entry.1.starts_with("gateway-session-input:")));
        damaged.repair(&services.topology);
        assert_eq!(
            services
                .runtime
                .runtime_services()
                .session_recovery_exclusions(),
            vec![ids[1].clone()]
        );
        let denied = services
            .session
            .admit_input(harness_contract::turn::SessionInputEnvelope::text(
                &ids[1],
                harness_contract::turn::InputSourceKind::Tui,
                "do not create new repaired ingress",
            ))
            .await
            .err()
            .expect("same epoch must remain quarantined after raw source repair");
        assert!(denied.contains(&format!("session_recovery_quarantined:{}", ids[1])));
        assert_eq!(
            ingress_snapshot_483(&services.topology, &ids[1]),
            frozen_ingress
        );
        let lease = stop_owner_483(&mut owner).await;
        let topology = lease.rebuild().unwrap();
        drop(lease);
        owner = Some(RecoveryServices::start(root.path(), topology, providers.clone()).await);
        let services = owner.as_ref().unwrap();
        assert!(services
            .supervisor
            .health()
            .recovery
            .failed_session_ids
            .is_empty());
        assert!(services
            .runtime
            .runtime_services()
            .session_recovery_exclusions()
            .is_empty());
        let mut repair_controls = 0;
        let request = plain_request_483(&mut provider, &mut repair_controls, 1).await;
        assert!(request.body.to_string().contains("REPAIRED_483"));
        request
            .reply
            .send(json!({"role":"assistant","content":"REPAIRED_483"}))
            .unwrap();
        completed_ingress_483(services, &failed).await;
        let frozen = ingress_snapshot_483(&services.topology, &ids[1]);
        assert_eq!(
            claims_483(&services.topology, std::slice::from_ref(&failed.request_id)).len(),
            1
        );
        let lease = stop_owner_483(&mut owner).await;
        let topology = lease.rebuild().unwrap();
        drop(lease);
        owner = Some(RecoveryServices::start(root.path(), topology, providers).await);
        assert_eq!(
            ingress_snapshot_483(&owner.as_ref().unwrap().topology, &ids[1]),
            frozen
        );
    })
    .catch_unwind()
    .await;
    if owner.is_some() {
        drop(stop_owner_483(&mut owner).await);
    }
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

async fn original_valid_goal_control_483() {
    let root = fixture_root();
    std::fs::create_dir_all(root.path().join("workspace")).unwrap();
    let mut provider = LoopbackProvider::start("root").await;
    let mut producer = LoopbackProvider::start("producer").await;
    let mut reviewer = LoopbackProvider::start("reviewer").await;
    let providers = provider_registry(&[&provider, &producer, &reviewer]);
    let topology = SelectedStorageTopology::compose_for_test(
        &root.path().join("home"),
        &root.path().join("workspace"),
    )
    .unwrap();
    let mut owner = Some(RecoveryServices::start(root.path(), topology, providers).await);
    let result = AssertUnwindSafe(async {
        let services = owner.as_ref().unwrap();
        let mut turn = ReviewedTurn::produce(
            services,
            &mut provider,
            &mut producer,
            &mut reviewer,
            false,
            false,
        )
        .await;
        let (record, mut barrier) = turn
            .delivery_barrier(services, &mut provider, Point::BeforeSessionCommit)
            .await;
        barrier.release(Release::Continue).unwrap();
        turn.assert_materialized(services, &record).await;
    })
    .catch_unwind()
    .await;
    drop(stop_owner_483(&mut owner).await);
    if let Err(error) = result {
        std::panic::resume_unwind(error);
    }
}

#[tokio::test]
async fn r1_gateway_startup_partition_original_valid_goal_control() {
    original_valid_goal_control_483().await;
}
