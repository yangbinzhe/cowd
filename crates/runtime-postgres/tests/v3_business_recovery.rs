use std::sync::Arc;

use harness_contract::{
    agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, MessagePublishInput,
        ObjectiveCompleteRequestInput, program_id_for_objective, root_objective_id,
    },
    execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeResult,
        ExecutionNodeSpec, ExecutionNodeStatus,
    },
    goal::{
        AcceptanceCriterion, AcceptanceStatus, GoalCompletion, GoalContract, GoalExecutionBinding,
        GoalScope, ObjectiveTerminalKind,
    },
    policy::PermissionMode,
    turn::InputRoutingDecision,
};
use runtime::{
    RuntimeEventInput, RuntimeEventScope, RuntimeEventStore, RuntimeServices,
    RuntimeTransactionEventInput, SessionTerminalInput,
};
use runtime_postgres::PostgresRuntimeEventStore;
use session::{
    SessionMessage, SessionRecord, SessionRuntimeInputStatus, SessionRuntimeOutboxRequest,
    SessionTerminalExecutionFence, SessionTerminalTranscriptCommit,
};
use session_postgres::PostgresSessionStore;

#[path = "../../storage/test-support/postgres_scope.rs"]
mod postgres_scope;
use postgres_scope::PostgresTestScope;

fn connect(fixture: &PostgresTestScope) -> Arc<RuntimeEventStore> {
    Arc::new(
        PostgresRuntimeEventStore::new(fixture.reconnect())
            .unwrap()
            .into_runtime_event_store(),
    )
}

fn services(
    fixture: &PostgresTestScope,
    root: &std::path::Path,
) -> (Arc<RuntimeServices>, Arc<RuntimeEventStore>) {
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let events = connect(fixture);
    let services = RuntimeServices::builder(root.join("home"), &workspace)
        .non_durable_backends_for_testing(root)
        .runtime_event_store(Arc::clone(&events))
        .build()
        .unwrap();
    (services, events)
}

#[tokio::test]
#[ignore = "requires an isolated COWD_TEST_POSTGRES_URL"]
async fn real_blocked_a_survives_restart_and_b_binds_exact_source_before_outbox_reclaim() {
    if std::env::var_os("COWD_V3_BUSINESS_RECOVERY_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--include-ignored")
            .arg("--exact")
            .arg("real_blocked_a_survives_restart_and_b_binds_exact_source_before_outbox_reclaim")
            .env("COWD_V3_BUSINESS_RECOVERY_CHILD", "1")
            .env("COWD_COMPLETION_REOPEN_BOUND", "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "isolated business recovery failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let fixture = PostgresTestScope::new();
    let temporary = tempfile::tempdir().unwrap();
    let (first, events) = services(&fixture, temporary.path());
    first.shutdown_maintenance().await;

    let root_id = "root:pg-business-recovery";
    let session_id = "session:pg-business-recovery";
    let turn_id = "turn:pg-business-recovery";
    let session_store = PostgresSessionStore::new(fixture.reconnect()).unwrap();
    session_store
        .create_session(&SessionRecord {
            session_id: session_id.into(),
            platform: "test".into(),
            chat_id: "pg-business-recovery".into(),
            user_id: Some("fixture-user".into()),
            model: None,
            created_at: "2026-10-16T00:00:00Z".into(),
            last_activity: "2026-10-16T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .unwrap();
    let ingress = SessionRuntimeOutboxRequest {
        input_id: "input:pg-business".into(),
        request_id: "request:pg-business".into(),
        turn_id: turn_id.into(),
        message_id: "ingress:pg-business".into(),
        session_generation: 1,
        decision: InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        created_at_ms: 100,
        runtime_options_json: None,
    };
    session_store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"complete only with independent review"}]"#),
            100,
            &ingress,
        )
        .unwrap();
    let claimed = session_store
        .claim_session_runtime_outbox("session-worker", 101, 10_000, 1)
        .unwrap()
        .remove(0);
    let claim_token = claimed.claim_token.clone().unwrap();
    let running = session_store
        .mark_session_runtime_outbox_running(
            &ingress.request_id,
            "session-worker",
            1,
            &claim_token,
            claimed.revision,
            102,
        )
        .unwrap();
    let claim_epoch = running.claim_fence_epoch.unwrap();
    drop(session_store);
    let objective_id = root_objective_id(session_id, turn_id);
    let program_id = program_id_for_objective(&objective_id);
    let mut graph = ExecutionGraph::new("reconcile a blocked Objective after restart");
    graph.id = root_id.into();
    graph = graph.with_lineage(ExecutionGraphLineage {
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        root_task_id: format!("task:{root_id}"),
        task_id: format!("task:{root_id}"),
        generation: 1,
    });
    let mut terminal_node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "test", "payload");
    terminal_node.id = "terminal".into();
    graph.nodes.push(terminal_node);
    let commits = first.commit_service();
    let graph = commits.register_graph(graph).unwrap().graph;
    let graph = commits
        .transition_node(&graph, "terminal", ExecutionNodeStatus::Ready, None, vec![])
        .unwrap()
        .graph;
    commits
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Running,
            None,
            vec![],
        )
        .unwrap();

    let actor = AgentActorBinding {
        objective_id: objective_id.clone(),
        program_id: program_id.clone(),
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        root_execution_id: Some(root_id.into()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "complete only with independent review".into(),
        model_lease: "test".into(),
        permission_ceiling: Some(PermissionMode::ReadOnly),
        resource_scopes: vec![],
        actor_id: "root-actor".into(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let opened = first
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "open-pg-business".into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: format!("topic:{program_id}"),
                summary: Some("work began".into()),
                content_ref: None,
                refs: vec![],
                recipients: vec![],
                intent: None,
                issue_dispositions: vec![],
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        opened.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{opened:?}"
    );
    let rejected = first
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "rejected-pg-business".into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: vec![],
                result_refs: vec!["message:not-a-reviewed-result".into()],
                evidence_refs: vec![],
                unresolved: vec![],
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        rejected.status,
        harness_contract::agent_action::AgentActionStatus::Rejected
    );
    assert!(
        events
            .list_stream(&format!("agentic-program:{program_id}"))
            .unwrap()
            .iter()
            .any(|event| event.kind == "agentic.completion_rejected")
    );
    let result = first
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: format!("session:{session_id}"),
                expected_bytes: None,
                original_name: None,
            },
            b"Unreviewed result must not satisfy the Goal",
        )
        .await
        .unwrap();
    let request = first
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "request-pg-business".into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                blocked: false,
                blocked_reason: None,
                human_review_required: false,
                recommended_next_actions: vec![],
                result_refs: vec![result.selector.clone()],
                evidence_refs: vec![],
                unresolved: vec![],
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        request.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{request:?}"
    );
    let goal_id = format!("goal:{root_id}");
    first
        .goal_store()
        .create(GoalContract {
            id: goal_id.clone(),
            session_id: session_id.into(),
            objective: actor.objective_summary.clone(),
            criteria: vec![AcceptanceCriterion {
                id: "user_intent".into(),
                statement: actor.objective_summary.clone(),
                statement_ref: None,
                source_refs: vec!["session_message:original".into()],
                required_evidence: vec![format!("execution_graph:{root_id}")],
                status: AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: vec![],
            phase: "execution".into(),
            evidence_refs: vec![],
            unresolved: vec![],
            blockers: vec![],
            scope: GoalScope::UserObjective,
            user_intent_criterion_id: Some("user_intent".into()),
            source_intent_ref: Some("session_message:original".into()),
            execution_binding: Some(GoalExecutionBinding {
                objective_id,
                session_id: session_id.into(),
                turn_id: turn_id.into(),
                root_execution_id: root_id.into(),
                agentic_program_id: program_id.clone(),
            }),
            spec_revision: 1,
            spec_digest: "original-spec".into(),
            review_refs: vec![],
            reviews: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
        })
        .unwrap();
    let concluded = first
        .reconcile_agentic_completion_request(&program_id)
        .unwrap()
        .expect("the real Program request must produce A");
    let verdict = concluded.objective_verdict.expect("A must have a verdict");
    assert_eq!(verdict.kind, ObjectiveTerminalKind::Blocked);
    let goal = first.goal_store().get(&goal_id).unwrap().unwrap();
    assert_eq!(goal.completion, GoalCompletion::Blocked);
    let a_cursor = events
        .list_stream(&format!("agentic-program:{program_id}"))
        .unwrap()
        .last()
        .unwrap()
        .commit_cursor;
    let program_a = events
        .list_stream(&format!("agentic-program:{program_id}"))
        .unwrap()
        .pop()
        .unwrap();
    let goal_a = events
        .list_stream(&format!("goal:{goal_id}"))
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(program_a.transaction_id, goal_a.transaction_id);
    assert_eq!(program_a.commit_cursor, goal_a.commit_cursor);
    assert_eq!(events.stream_revision(&graph.id).unwrap(), 3);
    drop(first);
    drop(events);

    let (second, events) = services(&fixture, temporary.path());
    second.shutdown_maintenance().await;
    let graph = second.graph_state_store().load(root_id).unwrap();
    assert_eq!(
        graph.node_statuses["terminal"],
        ExecutionNodeStatus::Running
    );
    let terminal = SessionTerminalInput {
        terminal_id: "terminal:pg-business".into(),
        message_id: "message:pg-business".into(),
        session_id: session_id.into(),
        execution_id: Some(root_id.into()),
        turn_id: Some(turn_id.into()),
        request_id: Some(ingress.request_id.clone()),
        session_generation: Some(1),
        input_sequence: Some(running.sequence as u64),
        input_claim_owner: Some("session-worker".into()),
        input_claim_token: Some(claim_token.clone()),
        input_claim_revision: Some(claim_epoch),
        controlled_recovery_claim_fingerprints: vec![],
        payload_ref: "artifact://terminal".into(),
    };
    let terminal_event = RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: "session-terminal:request:pg-business".into(),
            scope: RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".into(),
            status: Some("pending_delivery".into()),
            actor: Some("SynthesizeNodeExecutor".into()),
            refs: vec![],
            payload: serde_json::to_value(&terminal).unwrap(),
        },
        idempotency_key: Some(terminal.terminal_id.clone()),
        schema_version: 1,
    };
    let completed = ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some("result:terminal".into()),
        wait_class: None,
        summary: Some("blocked Goal reported honestly".into()),
        evidence_refs: vec![],
        failure: None,
        usage: Default::default(),
        finished_at_ms: 1,
    };
    let database = fixture.reconnect();
    database
        .checkout_critical()
        .unwrap()
        .batch_execute(
            "ALTER TABLE runtime_events ADD CONSTRAINT v3_reject_business_terminal CHECK (kind <> 'runtime.session.terminal_requested')",
        )
        .unwrap();
    assert!(
        second
            .commit_service()
            .transition_node(
                &graph,
                "terminal",
                ExecutionNodeStatus::Completed,
                Some(completed.clone()),
                vec![terminal_event.clone()],
            )
            .is_err()
    );
    assert_eq!(events.stream_revision(root_id).unwrap(), graph.revision);
    assert!(
        events
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        events
            .list_stream(&format!("goal:{goal_id}"))
            .unwrap()
            .last()
            .unwrap()
            .commit_cursor,
        a_cursor
    );
    database
        .checkout_critical()
        .unwrap()
        .batch_execute("ALTER TABLE runtime_events DROP CONSTRAINT v3_reject_business_terminal")
        .unwrap();
    let receipt = second
        .commit_service()
        .transition_node(
            &graph,
            "terminal",
            ExecutionNodeStatus::Completed,
            Some(completed),
            vec![terminal_event],
        )
        .unwrap();
    let b_event = events
        .list_stream("session-terminal:request:pg-business")
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(
        b_event.payload["objective_verdict"]["a_commit_cursor"],
        a_cursor
    );
    assert_eq!(
        b_event.payload["objective_verdict"]["verdict"]["kind"],
        "blocked"
    );
    assert!(receipt.transaction.stream_revisions.iter().any(|source| {
        source.stream_id == format!("goal:{goal_id}")
            && source.expected_revision
                == events.stream_revision(&format!("goal:{goal_id}")).unwrap()
    }));
    assert_eq!(
        events
            .list_stream(&format!("agentic-program:{program_id}"))
            .unwrap()
            .last()
            .unwrap()
            .commit_cursor,
        a_cursor
    );
    drop(second);
    drop(events);

    let session_store = PostgresSessionStore::new(fixture.reconnect()).unwrap();
    let transcript = SessionTerminalTranscriptCommit {
        terminal_message_id: terminal.message_id.clone(),
        ingress_message_id: ingress.message_id,
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        messages: vec![SessionMessage {
            stable_message_id: terminal.message_id.clone(),
            session_id: session_id.into(),
            sequence: 0,
            role: "assistant".into(),
            content_json:
                r#"[{"type":"text","text":"The Goal is blocked: independent review is missing."}]"#
                    .into(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 0,
        }],
        runtime_commit_cursor: receipt.transaction.commit_cursor,
        consumed_input_sequence: running.sequence,
        created_at_ms: 103,
        fence: SessionTerminalExecutionFence {
            request_id: terminal.request_id.clone().unwrap(),
            input_sequence: running.sequence,
            session_generation: 1,
            claim_owner: "session-worker".into(),
            claim_token,
            claim_fence_epoch: claim_epoch,
        },
    };
    let mut stale = transcript.clone();
    stale.fence.claim_fence_epoch += 1;
    assert!(matches!(
        session_store.commit_terminal_transcript_if_fenced(&stale),
        Err(session::SessionError::StaleExecutionFence(_))
    ));
    assert_eq!(session_store.get_message_count(session_id).unwrap(), 1);
    let committed = session_store
        .commit_terminal_transcript_if_fenced(&transcript)
        .unwrap();
    assert!(committed.inserted);
    assert_eq!(committed.input.status, SessionRuntimeInputStatus::Completed);
    assert_eq!(session_store.get_message_count(session_id).unwrap(), 2);
    let duplicate = session_store
        .commit_terminal_transcript_if_fenced(&transcript)
        .unwrap();
    assert!(!duplicate.inserted);
    assert_eq!(session_store.get_message_count(session_id).unwrap(), 2);
    drop(session_store);

    let events = connect(&fixture);
    let first_claim = events
        .claim_session_terminals("lost-worker", 100, 10, 1)
        .unwrap()
        .remove(0);
    drop(events);

    let reopened = connect(&fixture);
    let second_claim = reopened
        .claim_session_terminals("recovery-worker", 111, 10, 1)
        .unwrap()
        .remove(0);
    assert_eq!(second_claim.message_id, first_claim.message_id);
    assert_eq!(second_claim.commit_cursor, first_claim.commit_cursor);
    assert!(
        reopened
            .ack_session_terminal(
                &first_claim.terminal_id,
                "lost-worker",
                first_claim.revision,
                112
            )
            .is_err()
    );
    let materialized = reopened
        .ack_session_terminal(
            &second_claim.terminal_id,
            "recovery-worker",
            second_claim.revision,
            112,
        )
        .unwrap();
    assert_eq!(materialized.status, "materialized");
    assert_eq!(
        reopened
            .list_stream(&format!("agentic-program:{program_id}"))
            .unwrap()
            .last()
            .unwrap()
            .commit_cursor,
        a_cursor
    );
}
