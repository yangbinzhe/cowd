use super::*;

struct NoTools;

#[async_trait::async_trait]
impl ToolExecutor for NoTools {
    async fn execute_output(
        &self,
        name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        panic!("Host control test must not execute {name}");
    }
}

fn host() -> StandardRuntimeHost<NoTools> {
    let registry = Arc::new(crate::ProviderRegistry::new(crate::config::ProvidersConfig {
        providers: std::collections::HashMap::from([(
            "own834".into(),
            crate::config::ProviderConfig {
                name: "own834".into(),
                base_url: "http://127.0.0.1:9/v1".into(),
                api_key: "test".into(),
                models: vec!["own834-model".into()],
                protocol: Some("completions".into()),
                parallel_tool_calls: Default::default(),
                early_tool_start: Default::default(),
            },
        )]),
    }).unwrap());
    let client = ProviderRuntimeClient::new(registry, "own834-model".into(), Vec::new()).unwrap();
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        client,
        NoTools,
        PermissionPolicy::new(crate::PermissionMode::ReadOnly),
        Vec::new(),
    ).without_memory();
    StandardRuntimeHost {
        runtime: Some(runtime),
        poisoned: false,
        inflight_turn: None,
        services: crate::RuntimeServices::in_memory().unwrap(),
        execution_parent: None,
        execution_lineage: None,
        execution_role: TurnExecutionRole::DelegatedLeaf,
        recovered_tool_receipt_count: 0,
        recovered_tool_receipts_require_text_only: false,
    }
}

#[tokio::test]
async fn own834_required_control_refuses_absent_and_poisoned_owners() {
    let mut host = host();
    host.require_next_model_tool_action(["read_file".into()]).unwrap();
    let runtime = host.runtime.take().unwrap();
    assert!(host.require_next_model_tool_action(["read_file".into()]).is_err());
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    host.inflight_turn = Some(receiver);
    assert!(host.require_next_model_tool_action(["read_file".into()]).is_err());
    host.inflight_turn = None;
    host.runtime = Some(runtime);
    host.poisoned = true;
    assert!(host.require_next_model_tool_action(["read_file".into()]).is_err());
    assert!(host.runtime.is_some());
}

#[tokio::test]
async fn own834_missing_reclaim_preserves_original_error_and_refuses_control() {
    for interrupted in [false, true] {
        let mut host = host();
        drop(host.runtime.take());
        let original = RuntimeError::with_tool_exposure_miss("original business oracle failure")
            .with_repeated_tool_frame_refusal();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        host.inflight_turn = Some(receiver);
        assert!(sender.send((None, Err(original.clone()))).is_ok());
        let returned = if interrupted {
            host.restore_inflight_turn().await.unwrap_err()
        } else {
            host.await_started_turn().await.unwrap_err()
        };
        assert_eq!(returned, original);
        assert!(host.poisoned);
        assert!(host.runtime.is_none());
        assert!(host.inflight_turn.is_none());
        assert!(host.require_next_model_tool_action(["read_file".into()]).is_err());
    }
}

#[test]
fn own834_missing_runtime_never_promotes_success_or_erases_typed_failure() {
    let error = unreclaimed_turn_error(Ok(()), "submitted");
    assert!(error.to_string().contains("without reclaiming"));
    let original = RuntimeError::with_tool_exposure_miss("original settlement failure");
    assert_eq!(
        unreclaimed_turn_error::<()>(Err(original.clone()), "interrupted"),
        original,
    );
}

#[tokio::test]
async fn own834_closed_completion_poisoned_not_successfully_reclaimed() {
    for interrupted in [false, true] {
        let mut host = host();
        drop(host.runtime.take());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        host.inflight_turn = Some(receiver);
        drop(sender);
        let returned = if interrupted {
            host.restore_inflight_turn().await.unwrap_err()
        } else {
            host.await_started_turn().await.unwrap_err()
        };
        assert!(returned.to_string().contains("before recovery"));
        assert!(host.poisoned);
        assert!(host.conversation_control_available().is_err());
    }
}

#[tokio::test]
async fn own834_returned_runtime_preserves_interrupted_recovery_semantics() {
    let mut host = host();
    let runtime = host.runtime.take().unwrap();
    let (sender, receiver) = tokio::sync::oneshot::channel();
    host.inflight_turn = Some(receiver);
    assert!(sender.send((Some(runtime), Err(RuntimeError::new("prior cancelled waiter")))).is_ok());
    host.restore_inflight_turn().await.unwrap();
    assert!(host.runtime.is_some());
    assert!(!host.poisoned);
    host.require_next_model_tool_action(["read_file".into()]).unwrap();
}
