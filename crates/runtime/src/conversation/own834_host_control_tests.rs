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
                reasoning_effort: None,
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
        hook_scope: runtime.hook_execution_scope(),
        runtime: Some(Box::new(runtime)),
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
    let original_scope = host.hook_execution_scope();
    let runtime = host.runtime.take().unwrap();
    assert!(
        original_scope.is_same_owner(&host.hook_execution_scope()),
        "original physical owner remains observable while Graph holds Conversation"
    );
    assert!(host
        .require_next_model_tool_action(["read_file".into()])
        .is_err());
    let (_sender, receiver) = tokio::sync::oneshot::channel();
    host.inflight_turn = Some(receiver);
    assert!(host.require_next_model_tool_action(["read_file".into()]).is_err());
    host.inflight_turn = None;
    host.runtime = Some(runtime);
    assert!(original_scope.is_same_owner(&host.hook_execution_scope()));
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

#[tokio::test]
async fn own834_turn_exit_publishes_its_reason_instead_of_closing_the_channel() {
    let (sender, receiver) =
        tokio::sync::oneshot::channel::<(Option<Box<crate::ConversationRuntime<ProviderRuntimeClient, NoTools>>>, Result<TurnSummary, RuntimeError>)>();
    {
        let mut guard = CompletionSenderGuard::new(sender);
        guard.fail(RuntimeError::new("original turn failure must survive"));
    }
    let (runtime, result) = receiver
        .await
        .expect("the failing exit must publish a value, not drop the sender");
    assert!(runtime.is_none());
    assert_eq!(
        result.unwrap_err().to_string(),
        "original turn failure must survive"
    );
}

#[test]
fn own834_turn_panic_payload_reaches_the_host_instead_of_a_generic_message() {
    let payload = std::panic::catch_unwind(|| panic!("turn body panicked with its own payload"))
        .expect_err("the panic must produce a payload");
    let message = super::panic_runtime_error(payload).to_string();
    assert!(
        message.contains("turn body panicked with its own payload"),
        "the host must receive the panic payload, got: {message}"
    );
    assert!(
        !message.contains("without reporting a result"),
        "the generic drop message must not displace the payload, got: {message}"
    );
}

#[tokio::test]
async fn own834_turn_panic_guard_publishes_the_panic_reason_to_the_host() {
    let (sender, receiver) =
        tokio::sync::oneshot::channel::<(Option<Box<crate::ConversationRuntime<ProviderRuntimeClient, NoTools>>>, Result<TurnSummary, RuntimeError>)>();
    let mut guard = CompletionSenderGuard::new(sender);
    let payload = std::panic::catch_unwind(|| panic!("guarded turn body panicked"))
        .expect_err("the panic must produce a payload");
    guard.fail(super::panic_runtime_error(payload));
    let (runtime, result) = receiver
        .await
        .expect("a panicking turn must publish a value, not drop the sender");
    assert!(runtime.is_none());
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("guarded turn body panicked"),
        "the host must receive the panic payload, got: {message}"
    );
}

#[tokio::test]
async fn own834_turn_panic_still_publishes_a_reason_instead_of_closing_the_channel() {
    let (sender, receiver) =
        tokio::sync::oneshot::channel::<(Option<Box<crate::ConversationRuntime<ProviderRuntimeClient, NoTools>>>, Result<TurnSummary, RuntimeError>)>();
    let unwound = std::panic::AssertUnwindSafe(async move {
        let _guard = CompletionSenderGuard::new(sender);
        panic!("turn body panicked before reporting a result");
    });
    use futures::FutureExt;
    assert!(unwound.catch_unwind().await.is_err());
    let (runtime, result) = receiver
        .await
        .expect("a panicking exit must publish a value, not drop the sender");
    assert!(runtime.is_none());
    let message = result.unwrap_err().to_string();
    assert!(
        message.contains("without reporting a result"),
        "the host must receive an explicit reason, got: {message}"
    );
}
