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
        Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
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
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            EmptyApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["You are a test runtime.".to_string()],
        )
        .without_memory();
        runtime.set_active_model("test-model");

        let services = RuntimeServices::in_memory().expect("runtime services");
        let lineage = harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "w4-reclaim".to_string(),
            turn_id: format!("w4-reclaim-{turn}"),
            root_task_id: format!("task-w4-{turn}"),
            task_id: format!("task-w4-{turn}"),
            generation: 1,
        };

        let (_runtime, result) = submit_owned_conversation_turn(
            runtime,
            services,
            "run turn",
            &SharedPrompter::none(),
            lineage,
        )
        .await
        .expect("turn result is reclaimable");
        let _ = result;
    }

    assert_eq!(
        read_poisoned(),
        before,
        "the conversation runtime must be reclaimed on every turn"
    );
}
