//! PLAN W4: a conversation runtime must be reclaimed after every turn.
//!
//! `submit_owned_conversation_turn` used to `panic!` when the turn executors
//! still held the runtime (`Arc::try_unwrap` failure). The fix returns a typed
//! error and poisons the host. This regression runs many turns through a real
//! conversation runtime with a stubbed provider and asserts the poisoned counter
//! never moves: every turn returns its runtime, so the panic path is unreachable
//! in practice.

use std::pin::Pin;
use futures::Stream;
use runtime::{
    ApiClient, ApiRequest, AssistantEvent, ConversationRuntime, PermissionMode, PermissionPolicy,
    RuntimeError, RuntimeServices, Session, SharedPrompter, StaticToolExecutor,
    execution_core::performance, submit_owned_conversation_turn,
};

#[derive(Clone)]
struct EmptyApi;

impl ApiClient for EmptyApi {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        Box::pin(futures::stream::iter(vec![
            Ok(AssistantEvent::TextDelta("reclaimed turn".to_string())),
            Ok(AssistantEvent::MessageStop),
        ]))
    }
}

#[tokio::test]
async fn conversation_runtime_is_reclaimed_every_turn() {
    let read_poisoned = || -> u64 {
        performance::performance_snapshot()
            .into_iter()
            .find(|metric| metric.metric_id == "conversation_runtime_poisoned_total")
            .map(|metric| metric.total)
            .unwrap_or(0)
    };
    let before = read_poisoned();

    for turn in 0..24_u32 {
        let session = Session::new();
        let session_id = session.session_id.clone();
        let bus = runtime::CowdEventBus::new();
        let mut runtime = ConversationRuntime::new(
            session,
            EmptyApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["You are a test runtime.".to_string()],
        )
        .without_memory()
        .with_cowd_event_bus(bus.clone());
        runtime.set_active_model("test-model");

        let services = RuntimeServices::in_memory().expect("runtime services");
        let lineage = harness_contract::execution_graph::ExecutionGraphLineage {
            session_id,
            turn_id: format!("w4-reclaim-{turn}"),
            root_task_id: format!("task-w4-{turn}"),
            task_id: format!("task-w4-{turn}"),
            generation: 1,
        };

        services.publish_session_execution_policy(
            &lineage.session_id,
            runtime.permission_policy().execution_policy_control(),
        );
        let spec = services
            .task_runtime_port()
            .bind_task_spec(
                &lineage.session_id,
                None,
                harness_contract::task::TaskSpec::new("answer directly"),
            )
            .expect("canonical reclaim Task policy");
        services
            .task_runtime_port()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: lineage.task_id.clone(),
                mission_id: services.mission_runtime().default_mission_id().to_string(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: lineage.session_id.clone(),
                origin_turn_id: lineage.turn_id.clone(),
                root_task_id: lineage.root_task_id.clone(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "runtime.reclaim-test".to_string(),
                spec,
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_input",
                    format!("{}:{}", lineage.session_id, lineage.turn_id),
                )],
            })
            .expect("canonical reclaim Task");
        let (runtime, result) = submit_owned_conversation_turn(
            runtime,
            services.clone(),
            "answer directly",
            &SharedPrompter::none(),
            lineage.clone(),
        )
        .await
        .expect("turn result is reclaimable");
        let summary = result.expect("a real terminal, not a reclaimed admission error");
        assert!(summary.final_answer.contains("reclaimed turn"));
        let events = services
            .event_reader()
            .all_events(10_000)
            .expect("terminal evidence");
        let terminal = events
            .iter()
            .find(|event| event.kind == "runtime.strategy.outcome")
            .expect("completed Host strategy");
        let owner = terminal
            .activity_binding()
            .expect("original terminal root scope");
        assert_eq!(owner.session_id, lineage.session_id);
        assert_eq!(owner.turn_id, lineage.turn_id);
        assert_eq!(owner.task_id, lineage.task_id);
        assert_eq!(owner.generation, lineage.generation);
        let graph = services
            .graph_state_store()
            .load(&owner.root_execution_id)
            .expect("original admitted reclaim graph");
        assert_eq!(graph.lineage.as_ref(), Some(&lineage));
        assert!(graph.parent_execution.is_none());
        assert_eq!(runtime.session_id(), lineage.session_id);
        assert!(bus.current_execution_context().is_none());
        assert!(bus.current_activity_binding().is_none());
        assert!(!bus.has_live_telemetry_observer());
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }

    assert_eq!(
        read_poisoned(),
        before,
        "the conversation runtime must be reclaimed on every turn"
    );
}
