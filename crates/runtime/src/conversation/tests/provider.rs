use super::{
    apply_explicit_team_requirement, apply_named_e2e_strategy_fixture,
    build_cc_memory_config_with_budget, canonicalize_model_tool_names, classify_model_step_intent,
    consume_provider_stream, consume_provider_stream_with_activity, conversation_message_text,
    current_turn_messages, deterministic_checkpoint_id, enforce_explicit_team_requirement,
    eval_override_selection, image_user_message_from_path, is_agent_collaboration_action,
    is_append_only_projection, memory_project_id_for_session, prepared_vision_payload,
    preview_chars, provider_retry_is_fenced, provider_transport_policy, rate_per_second,
    revalidate_context_binding, turn_strategy_event_kind_allowed, unexposed_model_tool_names,
    vision_tool_model_receipt, vision_user_message, ApiClient, ApiRequest, AssistantEvent,
    AssistantItemKind, CancellationToken, CognitiveContextManager, ConversationRuntime,
    EarlyToolCandidate, EarlyToolDispatchFuture, EarlyToolDispatchResult, EarlyToolDispatcher,
    EarlyToolExecutionReceipt, EvaluationProviderTokenLeaseRegistry, ModelStepIntent,
    ModelStepToolPlan, ModelStreamReducer, ModelToolCall, ProviderContextInventory,
    ProviderTokenReservationSet, RuntimeError, StaticToolExecutor, ToolExposureState,
    ToolModelDeliveryRequirement, TurnStablePrefixMetrics, TurnToolExposureMetrics,
};
use crate::config::RuntimeFeatureConfig;
use crate::context_runtime::{
    ContextAuthority, ContextEnvelopeRequest, ContextIdentity, ContextItem, ContextMode,
    ContextProfile, ContextRole, ContextRuntimeKernel, ContextSourceKind, ResumeContextPacket,
    ResumeContextSource, CONTEXT_RENDER_FORMATTER_VERSION,
    PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION,
};
use crate::execution_core::build_runtime_execution_decision;
use crate::permissions::{PermissionMode, PermissionPolicy};
use crate::runtime_event_store::{RuntimeEventScope, RuntimeEventStore};
use crate::session::{ContentBlock, ConversationMessage, MessageRole, Session};
use crate::{
    resolve_context_budget_tokens, CowdEventBus, PromptAssembly, RealityRecallPort,
    RuntimeBudgetInputs, RuntimeBudgetPlan, SystemPromptBuilder, ToolExecutor,
    COWD_IDENTITY_CONTRACT_VERSION,
};
use futures::{stream::Stream, StreamExt};
use harness_contract::agent::{
    AgentBindingSnapshot, AgentCapability, AgentDataLease, AgentDefinitionId,
    AgentDefinitionRevisionRef, AgentExecutorPolicy, AgentInstanceRef, AgentModelPolicy,
    CognitiveReadScope, CognitiveWriteMode, DefinitionScope,
};
use harness_contract::skill::{
    AgentSkillProfile, SkillAdapterKind, SkillCapabilityProfile, SkillDetectedRuntime,
    SkillEntrypoint, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
};
use harness_contract::strategy::{understand, StrategyInput};
use model_protocol::usage::TokenUsage;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn cache_dimensions_require_a_durable_known_attempt_and_reject_any_unknown() {
    let mut observation = super::TurnCacheDimensionsObservation::default();
    let epoch = super::ProviderAttemptEpoch::new("execution-1", "turn-1").unwrap();
    observation.begin_epoch(Some(epoch.clone()), std::collections::BTreeMap::new());
    assert!(!observation.all_known());
    observation.record_outcome(
        Some(epoch.clone()),
        "orphan-outcome".to_string(),
        Some(TokenUsage::default()),
        true,
    );
    assert_eq!(
        observation.current_attempt_count(),
        0,
        "an outcome without a durable packed request cannot invent a physical attempt"
    );
    observation.record_packed(Some(epoch.clone()), "request-1".to_string());
    assert!(!observation.all_known());
    observation.record_outcome(
        Some(epoch.clone()),
        "request-1".to_string(),
        Some(TokenUsage::default()),
        true,
    );
    assert!(observation.all_known());
    observation.record_packed(Some(epoch.clone()), "request-2".to_string());
    observation.record_outcome(Some(epoch), "request-2".to_string(), None, false);
    assert!(!observation.all_known());
}

#[test]
fn cache_dimensions_multiple_attempts_and_late_old_epoch_are_isolated() {
    let old_epoch = super::ProviderAttemptEpoch::new("execution-old", "turn-old").unwrap();
    let current_epoch =
        super::ProviderAttemptEpoch::new("execution-current", "turn-current").unwrap();
    let mut observation = super::TurnCacheDimensionsObservation::default();
    observation.begin_epoch(Some(old_epoch.clone()), std::collections::BTreeMap::new());
    observation.record_packed(Some(old_epoch.clone()), "old-request".to_string());

    observation.begin_epoch(
        Some(current_epoch.clone()),
        std::collections::BTreeMap::new(),
    );
    observation.record_packed(Some(current_epoch.clone()), "request-1".to_string());
    observation.record_outcome(
        Some(current_epoch.clone()),
        "request-1".to_string(),
        Some(TokenUsage::default()),
        true,
    );
    observation.record_packed(Some(current_epoch.clone()), "request-2".to_string());
    assert!(!observation.all_known(), "a pending retry must fail closed");
    observation.record_outcome(
        Some(current_epoch),
        "request-2".to_string(),
        Some(TokenUsage::default()),
        true,
    );
    assert!(observation.all_known());

    observation.record_outcome(Some(old_epoch), "old-request".to_string(), None, false);
    assert!(
        observation.all_known(),
        "a late outcome from an old epoch must not contaminate the current turn"
    );
}

#[test]
fn failed_epoch_recovery_remains_unknown_after_a_new_known_attempt() {
    let epoch = super::ProviderAttemptEpoch::new("execution-restart", "turn-restart").unwrap();
    let mut durable = std::collections::BTreeMap::new();
    super::TurnCacheDimensionsObservation::apply_durable_payload(
        &mut durable,
        "context.provider_request_packed",
        &serde_json::json!({"request_id": "request-before-crash"}),
    );
    super::TurnCacheDimensionsObservation::apply_durable_payload(
        &mut durable,
        "context.provider_attempt_outcome",
        &serde_json::json!({
            "request_id": "request-before-crash",
            "cache_dimensions_status": "known",
            "usage": {
                "input_tokens": 900,
                "output_tokens": 90,
                "cache_creation_input_tokens": 100,
                "cache_read_input_tokens": 700
            }
        }),
    );
    let mut observation = super::TurnCacheDimensionsObservation::default();
    // The epoch query failed before this recovered map could be trusted. The
    // tracker must never let a later cheap attempt hide the possible durable
    // pre-crash spend.
    observation.begin_epoch_with_recovery_authority(Some(epoch.clone()), durable, false);
    observation.record_packed(Some(epoch.clone()), "request-after-restart".to_string());
    observation.record_outcome(
        Some(epoch),
        "request-after-restart".to_string(),
        Some(TokenUsage {
            input_tokens: 1,
            output_tokens: 1,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        }),
        true,
    );
    assert!(!observation.all_known());
    assert_eq!(observation.current_attempt_count(), 2);
}

#[test]
fn provider_attempt_usage_is_summed_once_per_request_in_current_epoch() {
    let epoch = super::ProviderAttemptEpoch::new("execution-usage", "turn-usage").unwrap();
    let mut observation = super::TurnCacheDimensionsObservation::default();
    observation.begin_epoch(Some(epoch.clone()), std::collections::BTreeMap::new());
    let first = TokenUsage {
        input_tokens: 100,
        output_tokens: 10,
        cache_creation_input_tokens: 20,
        cache_read_input_tokens: 30,
    };
    let retried = TokenUsage {
        input_tokens: 80,
        output_tokens: 8,
        cache_creation_input_tokens: 15,
        cache_read_input_tokens: 25,
    };
    observation.record_packed(Some(epoch.clone()), "request-1".to_string());
    observation.record_outcome(
        Some(epoch.clone()),
        "request-1".to_string(),
        Some(first),
        true,
    );
    // Idempotent replay of the same durable request replaces rather than
    // double-counting the physical attempt.
    observation.record_outcome(
        Some(epoch.clone()),
        "request-1".to_string(),
        Some(first),
        true,
    );
    observation.record_packed(Some(epoch.clone()), "request-2".to_string());
    observation.record_outcome(Some(epoch), "request-2".to_string(), Some(retried), true);
    assert_eq!(
        observation.recovered_usage(),
        super::RecoveredProviderUsage {
            input_tokens: 180,
            output_tokens: 18,
            cache_creation_input_tokens: 35,
            cache_read_input_tokens: 55,
        }
    );
    assert_eq!(observation.current_attempt_count(), 2);
}

#[tokio::test]
async fn outcome_append_failure_leaves_packed_attempt_unknown() {
    struct FailOutcomePort(std::sync::atomic::AtomicUsize);

    #[async_trait::async_trait]
    impl crate::SessionRuntimeJournalPort for FailOutcomePort {
        async fn commit_input_application_receipt(
            &self,
            input_ids: &[String],
            expected_revisions: &[u64],
            receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
            now_ms: u64,
        ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
            let _ = (input_ids, expected_revisions, receipt, now_ms);
            Err(session::SessionError::Store(
                "fixture has no input application owner".into(),
            ))
        }

        async fn authorize_requested_cancellation(
            &self,
            _request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
        ) -> Result<session::SessionDomainEvent, session::SessionError> {
            Err(session::SessionError::Store(
                "outcome fixture has no cancellation authorization owner".into(),
            ))
        }

        async fn append_event_if_claim_current(
            &self,
            _request: &crate::session_runtime_port::RuntimeSessionClaimEventAppend,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            Err(session::SessionError::Store(
                "outcome fixture has no authenticated claim writer".into(),
            ))
        }

        async fn append_event(
            &self,
            event: &crate::RuntimeSessionEvent,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            if event.kind == crate::RuntimeSessionEventKind::ProviderAttemptOutcome {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Err(session::SessionError::Store(
                    "injected outcome append failure".to_string(),
                ))
            } else {
                Ok(crate::RuntimeSessionEventReceipt { sequence: 1 })
            }
        }

        async fn append_context_envelope_if_absent(
            &self,
            _record: &crate::RuntimeContextEnvelopeRecord,
        ) -> Result<Option<crate::RuntimeSessionEventReceipt>, session::SessionError> {
            Ok(None)
        }

        async fn append_compaction_bundle_if_absent(
            &self,
            _events: &[crate::RuntimeSessionEvent],
            _checkpoint_id: &str,
        ) -> Result<bool, session::SessionError> {
            Ok(false)
        }
    }

    let epoch = super::ProviderAttemptEpoch::new("execution-fail", "turn-fail").unwrap();
    let tracker = Arc::new(std::sync::Mutex::new(
        super::TurnCacheDimensionsObservation::default(),
    ));
    {
        let mut tracker = tracker.lock().unwrap();
        tracker.begin_epoch(Some(epoch.clone()), std::collections::BTreeMap::new());
        tracker.record_packed(Some(epoch), "request-fail".to_string());
    }
    let temporary = tempfile::tempdir().unwrap();
    let journal = Arc::new(FailOutcomePort(std::sync::atomic::AtomicUsize::new(0)));
    let writer = super::SessionProviderWireEvidenceWriter {
        artifacts: Arc::new(
            crate::ArtifactStore::for_test(temporary.path(), crate::ArtifactStoreConfig::default())
                .unwrap(),
        ),
        session_port: journal.clone(),
        cache_dimensions: Arc::clone(&tracker),
        native_wire_receipts: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let context = crate::ProviderRequestEvidenceContext {
            graph_owner: None,
        session_id: "session-fail".to_string(),
        execution_id: "execution-fail".to_string(),
        turn_id: "turn-fail".to_string(),
        request_sequence: 1,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "model", 8_192, 1_024, 0, 0, 1,
        ),
        attempt: 1,
    };
    let result = crate::ProviderWireEvidenceWriter::persist_outcome(
        &writer,
        &context,
        crate::ProviderAttemptOutcomeEvidence {
            request_id: "request-fail".to_string(),
            logical_attempt: 1,
            terminal_status: "completed".to_string(),
            usage: Some(model_protocol::usage::TokenUsage::default()),
            cache_dimensions_observed: true,
            protocol_diagnostic: None,
        },
    )
    .await;
    assert!(result.is_err());
    assert!(!tracker.lock().unwrap().all_known());
    assert_eq!(journal.0.load(std::sync::atomic::Ordering::SeqCst), 1);

    // The annotated path must reach the same failing journal, not fail an
    // unrelated identity check or claim a durable diagnostic from a safe log.
    let (evidence, identity) = r304_diagnostic_request_fixture(&context, "request-fail");
    crate::ProviderWireEvidenceWriter::persist(&writer, &context, evidence)
        .await
        .unwrap();
    let opened = crate::provider_runtime_client::ProviderPhysicalObservation::ResponseOpened {
        identity: identity.clone(),
    };
    let ack = crate::ProviderWireEvidenceWriter::persist_physical(&writer, &context, opened, None)
        .await
        .unwrap();
    let error = provider::ApiError::CompatibilityToolProtocol(
        provider::CompatibilityToolProtocolFailure::MalformedFrame,
    )
    .with_compatibility_tool_frame_diagnostic(
        "stream_compat_finish",
        "unterminated",
        b"PRIVATE_R304_FRAME",
    );
    let diagnostic = crate::provider_runtime_client::bind_provider_protocol_diagnostic(
        &error,
        &context,
        &identity.step,
        &identity.provider_attempt,
        "request-fail",
        &identity.resource_lease,
        &ack,
    )
    .unwrap();
    let result = crate::ProviderWireEvidenceWriter::persist_outcome(
        &writer,
        &context,
        crate::ProviderAttemptOutcomeEvidence {
            request_id: "request-fail".into(),
            logical_attempt: context.attempt,
            terminal_status: "failed".into(),
            usage: None,
            cache_dimensions_observed: false,
            protocol_diagnostic: Some(diagnostic),
        },
    )
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("injected outcome append failure")
    );
    assert_eq!(journal.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(!tracker.lock().unwrap().all_known());
    assert_eq!(
        error.compatibility_tool_frame_metadata().unwrap()["persisted"],
        false
    );
}

/// A real original-writer input, with an independently declared Step/attempt
/// and an actual wire digest so the existing native receipt owner accepts it.
fn r304_diagnostic_request_fixture(
    context: &crate::ProviderRequestEvidenceContext,
    request_id: &str,
) -> (
    crate::ProviderWireEvidence,
    crate::provider_runtime_client::ProviderPhysicalIdentity,
) {
    let identity = crate::provider_runtime_client::ProviderPhysicalIdentity {
            graph_owner: None,
        session_id: context.session_id.clone(),
        execution_id: context.execution_id.clone(),
        turn_id: context.turn_id.clone(),
        request_sequence: context.request_sequence,
        request_id: request_id.into(),
        response_id: format!("response-{request_id}"),
        opened_at_ms: 1,
        logical_attempt: context.attempt,
        step: harness_contract::step::StepIdentity {
            step_id: "r304-original-step".into(),
            session_id: context.session_id.clone(),
            turn_id: context.turn_id.clone(),
            execution_id: Some(context.execution_id.clone()),
            actor_ref: None,
            run_ref: None,
            attempt: Some(context.attempt),
            epoch: None,
            goal_ref: None,
            program_ref: None,
            task_ref: None,
            build_ref: "r304-fixture".into(),
        },
        provider_attempt: harness_contract::step::ProviderAttemptIdentity {
            attempt_id: "r304-original-attempt".into(),
            origin_step_id: "r304-original-step".into(),
            model_profile_digest: "r304-profile".into(),
            request_digest: "r304-request-material".into(),
        },
        resource_lease: "r304-original-lease".into(),
    };
    let body = serde_json::json!({"model":"test-model", "messages":[{"role":"user", "content":"checked"}]});
    let evidence = crate::ProviderWireEvidence {
        step_render: Some(harness_contract::step::StepRenderRef {
            step_id: "r304-original-step".into(),
            attempt_id: "r304-original-attempt".into(),
            catalog_digest: "r304-catalog".into(),
            render_digest: "r304-render".into(),
        }),
        request_context: crate::ProviderRequestContext {
            request_id: request_id.into(),
            attempt: context.attempt,
            profile: crate::ResolvedProviderProfile {
                registry_revision: 3,
                provider_name: "openai-compatible".into(),
                model: "test-model".into(),
                base_url: Some("https://provider.invalid/v1".into()),
                protocol: Some("chat_completions".into()),
                parallel_tool_calls_mode:
                    model_protocol::provider_config::ParallelToolCallsMode::Auto,
                effective_parallel_tool_calls: Some(true),
                effective_early_tool_start: false,
                model_capabilities: Vec::new(),
                capabilities:
                    model_protocol::provider_capability::ProviderCapabilityProfile::unknown(),
            },
            transport_fingerprint: crate::TransportProfileFingerprint(42),
            diagnostic_send_reason: None,
        },
        wire_request: provider::ProviderWireRequest {
            method: "POST".into(),
            endpoint: "https://provider.invalid/v1/chat/completions".into(),
            protocol: "openai_chat_completions".into(),
            headers: Vec::new(),
            body_sha256: format!("{:x}", Sha256::digest(serde_json::to_vec(&body).unwrap())),
            body,
            tool_schema_sha256: None,
        },
        prefix_observation: crate::ProviderWirePrefixObservation::default(),
    };
    (evidence, identity)
}

#[tokio::test]
async fn begin_epoch_recovers_packed_and_outcome_after_restart() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    #[derive(Clone)]
    struct NoopApi;

    impl ApiClient for NoopApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::empty())
        }
    }

    let store = Arc::new(crate::test_support::session_store());
    let session_id = "session-provider-recovery";
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "test".to_string(),
            chat_id: "provider-recovery".to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: "2026-09-05T00:00:00Z".to_string(),
            last_activity: "2026-09-05T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    for (kind, payload) in [
        (
            "context.provider_request_packed",
            serde_json::json!({
                "type": "ProviderRequestPacked",
                "execution_id": "execution-recovery",
                "turn_id": "turn-recovery",
                "request_id": "request-recovery"
            }),
        ),
        (
            "context.provider_attempt_outcome",
            serde_json::json!({
                "type": "ProviderAttemptOutcome",
                "execution_id": "execution-recovery",
                "turn_id": "turn-recovery",
                "request_id": "request-recovery",
                "cache_dimensions_status": "known",
                "usage": {
                    "input_tokens": 321,
                    "output_tokens": 45,
                    "cache_creation_input_tokens": 67,
                    "cache_read_input_tokens": 89
                }
            }),
        ),
    ] {
        store
            .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Context,
                kind,
                payload,
                1,
            ))
            .await
            .unwrap();
    }

    let mut session = Session::new();
    session.session_id = session_id.to_string();
    let bus = CowdEventBus::new();
    let _scope = bus.enter_execution(crate::CowdExecutionContext {
        execution_id: "execution-recovery".to_string(),
        session_id: session_id.to_string(),
        turn_id: "turn-recovery".to_string(),
    });
    let runtime = ConversationRuntime::new(
        session,
        NoopApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_session_history_reader(Arc::new(store.history_reader()))
    .with_cowd_event_bus(bus);
    runtime.begin_turn_runtime_epoch().await;
    assert!(runtime.turn_cache_dimensions_known());
    assert_eq!(
        runtime.turn_recovered_provider_usage(),
        super::RecoveredProviderUsage {
            input_tokens: 321,
            output_tokens: 45,
            cache_creation_input_tokens: 67,
            cache_read_input_tokens: 89,
        }
    );
    assert_eq!(runtime.turn_recovered_provider_attempt_count(), 1);
}

#[tokio::test]
async fn transport_heartbeats_cannot_extend_semantic_stream_timeout() {
    let activity = provider::TransportActivity::default();
    let heartbeat_activity = activity.clone();
    let heartbeat = tokio::spawn(async move {
        loop {
            heartbeat_activity.observe();
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    });
    let stream = Box::pin(futures::stream::pending::<
        Result<AssistantEvent, RuntimeError>,
    >());
    let result = consume_provider_stream_with_activity(
        stream,
        CancellationToken::new(),
        Some(super::ProviderStreamTimeoutPolicy {
            idle: Duration::from_millis(20),
            heartbeat_grace: Duration::ZERO,
        }),
        ModelStreamReducer::new(None, None, "semantic-timeout".to_string()),
        None,
        Some(activity),
    )
    .await;
    heartbeat.abort();

    assert_eq!(
        result.resource_result_class,
        crate::execution_core::graph::ResourceResultClass::TimedOut
    );
    assert!(result
        .failure
        .expect("semantic timeout")
        .to_string()
        .contains("without semantic activity"));
}

struct CollaborationAvailableExecutor;

#[async_trait::async_trait]
impl ToolExecutor for CollaborationAvailableExecutor {
    async fn execute_output(
        &self,
        _tool_name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        Err(crate::ToolError::new(
            "provider strategy fixture does not execute tools",
        ))
    }

    fn available_tool_names(&self) -> Vec<String> {
        vec![
            "runtime_capabilities".to_string(),
            harness_contract::agent_action::TEAM_CREATE_TOOL_ID.to_string(),
        ]
    }

    fn collaboration_runtime_available(&self) -> bool {
        true
    }
}

struct CollaborationAvailableStaticExecutor(StaticToolExecutor);

#[async_trait::async_trait]
impl ToolExecutor for CollaborationAvailableStaticExecutor {
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        self.0.execute_output(tool_name, input).await
    }

    fn available_tool_names(&self) -> Vec<String> {
        self.0.available_tool_names()
    }

    fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.0.registered_tool_effect(tool_name, input)
    }

    fn collaboration_runtime_available(&self) -> bool {
        true
    }
}

#[test]
fn provider_retry_fence_activates_for_prior_or_same_attempt_effect_receipt() {
    assert!(!provider_retry_is_fenced(false, 0));
    assert!(provider_retry_is_fenced(true, 0));
    assert!(provider_retry_is_fenced(false, 1));
}
#[tokio::test]
async fn exact_provider_wire_evidence_is_artifact_backed_and_durably_pinned() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let session_store = Arc::new(crate::test_support::session_store());
    let session = Session::new();
    let session_id = session.session_id.clone();
    session_store
        .create_session(&session::SessionRecord {
            session_id: session_id.clone(),
            platform: "test".to_string(),
            chat_id: "provider-evidence".to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: "2026-08-07T00:00:00Z".to_string(),
            last_activity: "2026-08-07T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(
        crate::ArtifactStore::for_test(temporary.path(), crate::ArtifactStoreConfig::default())
            .unwrap(),
    );
    let writer = super::SessionProviderWireEvidenceWriter {
        artifacts: Arc::clone(&artifacts),
        session_port: crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(
            &session_store,
        )),
        cache_dimensions: Arc::new(std::sync::Mutex::new(
            super::TurnCacheDimensionsObservation::default(),
        )),
        native_wire_receipts: Arc::new(std::sync::Mutex::new(Vec::new())),
    };
    let epoch = super::ProviderAttemptEpoch::new("execution-evidence", "turn-evidence").unwrap();
    writer
        .cache_dimensions
        .lock()
        .unwrap()
        .begin_epoch(Some(epoch), std::collections::BTreeMap::new());
    let context = crate::ProviderRequestEvidenceContext {
            graph_owner: None,
        session_id: session_id.clone(),
        execution_id: "execution-evidence".to_string(),
        turn_id: "turn-evidence".to_string(),
        request_sequence: 7,
        request_compiler_cache_hit: true,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "test-model",
            128_000,
            4_096,
            100,
            100,
            1_000,
        ),
        attempt: 7,
    };
    let evidence = crate::ProviderWireEvidence {
        step_render: None,
        request_context: crate::ProviderRequestContext {
            request_id: "request-provider-evidence".to_string(),
            profile: crate::ResolvedProviderProfile {
                registry_revision: 3,
                provider_name: "openai-compatible".to_string(),
                model: "test-model".to_string(),
                base_url: Some("https://provider.example/v1".to_string()),
                protocol: Some("responses".to_string()),
                parallel_tool_calls_mode:
                    model_protocol::provider_config::ParallelToolCallsMode::Auto,
                effective_parallel_tool_calls: Some(true),
                effective_early_tool_start: false,
                model_capabilities: Vec::new(),
                capabilities:
                    model_protocol::provider_capability::ProviderCapabilityProfile::unknown(),
            },
            transport_fingerprint: crate::TransportProfileFingerprint(42),
            diagnostic_send_reason: None,
            attempt: 7,
        },
        wire_request: provider::ProviderWireRequest {
            method: "POST".to_string(),
            endpoint: "https://provider.example/v1/responses".to_string(),
            protocol: "responses".to_string(),
            headers: vec![provider::ProviderWireHeader {
                name: "content-type".to_string(),
                value: "application/json".to_string(),
            }],
            body: serde_json::json!({"model":"test-model","input":"checked"}),
            body_sha256: "sha256-body".to_string(),
            tool_schema_sha256: Some("sha256-tools".to_string()),
        },
        prefix_observation: crate::ProviderWirePrefixObservation::default(),
    };

    crate::ProviderWireEvidenceWriter::persist(&writer, &context, evidence)
        .await
        .unwrap();
    crate::ProviderWireEvidenceWriter::persist_outcome(
        &writer,
        &context,
        crate::ProviderAttemptOutcomeEvidence {
            request_id: "request-provider-evidence".to_string(),
            logical_attempt: 7,
            terminal_status: "completed".to_string(),
            usage: Some(model_protocol::usage::TokenUsage {
                input_tokens: 3,
                output_tokens: 1,
                cache_creation_input_tokens: 2,
                cache_read_input_tokens: 5,
            }),
            cache_dimensions_observed: true,
            protocol_diagnostic: None,
        },
    )
    .await
    .unwrap();
    assert!(writer.cache_dimensions.lock().unwrap().all_known());

    let events = session_store
        .session_domain_events_page(&session_id, 0, 10)
        .await
        .unwrap();
    let packed = events
        .events
        .iter()
        .find(|event| event.kind == "context.provider_request_packed")
        .expect("provider request evidence event");
    assert_eq!(packed.payload["schema_version"], 5);
    // PLAN A1: the per-segment composition and the divergence segment ride along
    // with the aggregate byte counts, so a cache divergence is attributable to a
    // segment from the evidence alone.
    assert!(
        packed.payload.get("prompt_composition").is_some(),
        "the packed request evidence must carry the prompt composition"
    );
    assert_eq!(packed.payload["execution_id"], "execution-evidence");
    assert_eq!(packed.payload["turn_id"], "turn-evidence");
    assert!(packed.payload.get("body").is_none());
    let artifact: harness_contract::context::ArtifactRef =
        serde_json::from_value(packed.payload["artifact"].clone()).unwrap();
    let body = artifacts
        .read(&artifact, &format!("session:{session_id}"), None)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body["provider_request"]["wire_request"]["body"]["input"],
        "checked"
    );
    assert_eq!(body["provider_request"]["request_context"]["attempt"], 7);
    assert_eq!(artifacts.stats().unwrap().pins, 1);
    let outcome = events
        .events
        .iter()
        .find(|event| event.kind == "context.provider_attempt_outcome")
        .expect("provider attempt outcome event");
    assert_eq!(outcome.payload["request_id"], "request-provider-evidence");
    assert_eq!(outcome.payload["usage_status"], "known");
    assert_eq!(outcome.payload["cache_dimensions_status"], "known");
    assert_eq!(outcome.payload["cache_hit_ratio_bp"], 5_000);
    assert!(
        outcome.payload.get("protocol_diagnostic").is_none(),
        "old unannotated outcomes remain valid"
    );

    let (evidence, identity) = r304_diagnostic_request_fixture(&context, "request-r304");
    crate::ProviderWireEvidenceWriter::persist(&writer, &context, evidence)
        .await
        .unwrap();
    let error = provider::ApiError::CompatibilityToolProtocol(
        provider::CompatibilityToolProtocolFailure::MalformedFrame,
    )
    .with_compatibility_tool_frame_diagnostic(
        "stream_compat_finish",
        "unterminated",
        b"PRIVATE_R304_FRAME",
    );
    // Same logical sequence, but another structurally valid Step: its genuine
    // open receipt cannot authorize an outcome for the original packed Step.
    let mut foreign = identity.clone();
    foreign.response_id = "r304-foreign-response".into();
    foreign.step.step_id = "r304-foreign-step".into();
    foreign.provider_attempt.origin_step_id = foreign.step.step_id.clone();
    for opened_identity in [foreign, identity.clone()] {
        let opened = crate::provider_runtime_client::ProviderPhysicalObservation::ResponseOpened {
            identity: opened_identity.clone(),
        };
        let ack =
            crate::ProviderWireEvidenceWriter::persist_physical(&writer, &context, opened, None)
                .await
                .unwrap();
        let diagnostic = crate::provider_runtime_client::bind_provider_protocol_diagnostic(
            &error,
            &context,
            &opened_identity.step,
            &opened_identity.provider_attempt,
            "request-r304",
            &opened_identity.resource_lease,
            &ack,
        )
        .unwrap();
        let mut annotated = crate::ProviderAttemptOutcomeEvidence {
            request_id: "request-r304".into(),
            logical_attempt: context.attempt,
            terminal_status: "failed".into(),
            usage: None,
            cache_dimensions_observed: false,
            protocol_diagnostic: Some(diagnostic),
        };
        if opened_identity.step.step_id == "r304-foreign-step" {
            assert!(
                crate::ProviderWireEvidenceWriter::persist_outcome(&writer, &context, annotated)
                    .await
                    .is_err()
            );
            let read = session_store
                .session_domain_events_page(&session_id, 0, 32)
                .await
                .unwrap();
            assert_eq!(
                read.events
                    .iter()
                    .filter(|e| e.kind == "context.provider_attempt_outcome")
                    .count(),
                1,
                "foreign binding must not partially append an outcome"
            );
            continue;
        }
        let mut foreign_context = context.clone();
        foreign_context.turn_id = "r304-foreign-turn".into();
        assert!(
            crate::ProviderWireEvidenceWriter::persist_outcome(
                &writer,
                &foreign_context,
                annotated.clone()
            )
            .await
            .is_err()
        );
        annotated.logical_attempt = context.attempt + 1;
        assert!(
            crate::ProviderWireEvidenceWriter::persist_outcome(
                &writer,
                &context,
                annotated.clone()
            )
            .await
            .is_err()
        );
        annotated.logical_attempt = context.attempt;
        let read = session_store
            .session_domain_events_page(&session_id, 0, 32)
            .await
            .unwrap();
        assert_eq!(
            read.events
                .iter()
                .filter(|e| e.kind == "context.provider_attempt_outcome")
                .count(),
            1
        );
        crate::ProviderWireEvidenceWriter::persist_outcome(&writer, &context, annotated)
            .await
            .unwrap();
    }
    let read = session_store
        .session_domain_events_page(&session_id, 0, 32)
        .await
        .unwrap();
    let durable = read
        .events
        .iter()
        .find(|event| {
            event.kind == "context.provider_attempt_outcome"
                && event.payload["request_id"] == "request-r304"
        })
        .unwrap();
    let diagnostic = &durable.payload["protocol_diagnostic"];
    assert_eq!(diagnostic["stage"], "stream_compat_finish");
    assert_eq!(diagnostic["class"], "unterminated");
    assert_eq!(
        diagnostic["physical_identity"]["step"]["step_id"],
        "r304-original-step"
    );
    assert_eq!(
        diagnostic["physical_identity"]["provider_attempt"]["attempt_id"],
        "r304-original-attempt"
    );
    assert_eq!(diagnostic["physical_identity"]["logical_attempt"], 7);
    assert_eq!(durable.payload["usage_status"], "unknown");
    assert_eq!(diagnostic["raw_omitted"], true);
    assert_eq!(diagnostic["raw_persisted"], false);
    assert_eq!(diagnostic["raw_replayable"], false);
    assert_eq!(
        diagnostic["raw_omission_reason"],
        "private_encrypted_artifact_unavailable"
    );
    assert!(!durable.payload.to_string().contains("PRIVATE_R304_FRAME"));
    assert_eq!(
        artifacts.stats().unwrap().pins,
        2,
        "request artifacts only; never a plaintext response artifact"
    );
}

#[test]
fn memory_projection_accepts_only_exact_append_revisions() {
    assert!(is_append_only_projection(true, 10, 20, 13, 23));
    assert!(is_append_only_projection(true, 10, 20, 10, 20));
    assert!(!is_append_only_projection(false, 0, 0, 3, 3));
    assert!(!is_append_only_projection(true, 10, 20, 11, 20));
    assert!(!is_append_only_projection(true, 10, 20, 11, 19));
    assert!(!is_append_only_projection(true, 10, 20, 14, 23));
}

#[tokio::test]
async fn memory_projection_converts_only_appended_messages_and_rebuilds_on_replace() {
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    runtime
        .append_external_message(ConversationMessage::user_text("first"))
        .await
        .expect("first message");
    runtime
        .append_external_message(ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "first response".to_string(),
        }]))
        .await
        .expect("first response");

    let first = runtime.memory_context_messages().await;
    assert_eq!(first.len(), 2);
    {
        let projection = runtime.session_memory_projection.lock().await;
        assert_eq!(projection.converted_messages, 2);
        assert_eq!(projection.rebuilds, 1);
    }

    runtime
        .append_external_message(ConversationMessage::user_text("second"))
        .await
        .expect("second message");
    let second = runtime.memory_context_messages().await;
    assert_eq!(second.len(), 3);
    {
        let projection = runtime.session_memory_projection.lock().await;
        assert_eq!(
            projection.converted_messages, 3,
            "the second projection must convert only the appended message"
        );
        assert_eq!(projection.rebuilds, 1);
    }

    runtime
        .session_mut_async()
        .await
        .replace_messages(vec![ConversationMessage::user_text("replacement")]);
    let replaced = runtime.memory_context_messages().await;
    assert_eq!(replaced.len(), 1);
    {
        let projection = runtime.session_memory_projection.lock().await;
        assert_eq!(projection.converted_messages, 4);
        assert_eq!(
            projection.rebuilds, 2,
            "replace/truncate/recovery paths must invalidate the append projection"
        );
    }
}

#[test]
fn small_exact_tool_result_retains_content_after_receipt_envelope() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableExecutor,
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    let output = serde_json::json!({
        "type": "text",
        "path": "fixtures/target.txt",
        "content": "implemented-auto-strategy-0\n",
        "totalLines": 1,
        "truncated": false,
    })
    .to_string();
    let receipt = runtime.tool_model_receipt(
        "read_file",
        &output,
        false,
        &harness_contract::reality::EvidenceRef::observed("tool", "small-exact-read"),
        None,
        &ToolModelDeliveryRequirement::exact(vec!["small-read".to_string()]),
        "{}",
    );

    assert!(!receipt.truncated, "{}", receipt.summary);
    assert!(receipt.summary.contains("implemented-auto-strategy-0"));
    assert!(!receipt.summary.contains("omitted; retrieve"));
}

#[test]
fn exact_evidence_mode_expands_tool_delivery_without_bypassing_context_ceiling() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    let standard = runtime.runtime_budget_plan();
    let exact =
        ConversationRuntime::<MockApi, StaticToolExecutor>::apply_exact_evidence_delivery_budget(
            standard.clone(),
        );
    runtime
        .turn_context_ledger
        .lock()
        .expect("context ledger")
        .reset(
            exact.subsystem_budget_tokens,
            exact.tool_result_budget.max_total_tokens as u64,
        );

    assert!(
        exact.tool_result_budget.max_total_tokens > standard.tool_result_budget.max_total_tokens,
        "exact evidence may raise the turn's TOTAL lease so several whole files fit"
    );
    assert!(
        exact.tool_result_budget.per_tool_max_tokens < exact.tool_result_budget.max_total_tokens,
        "a single result must no longer be granted the whole lease: {} vs {}",
        exact.tool_result_budget.per_tool_max_tokens,
        exact.tool_result_budget.max_total_tokens
    );
    assert!(
        exact.tool_result_budget.per_tool_max_tokens <= 16_000,
        "one result must never dominate the prompt, got {}",
        exact.tool_result_budget.per_tool_max_tokens
    );
    assert!(
        exact.tool_result_budget.max_total_tokens as u64 + exact.max_output_tokens
            < exact.subsystem_budget_tokens
    );

    let target_tokens = (standard.tool_result_budget.per_tool_max_tokens
        + exact.tool_result_budget.per_tool_max_tokens)
        / 2;
    let output = serde_json::json!({
        "type": "text",
        "path": "src/large.rs",
        "content": "x".repeat(target_tokens.saturating_mul(3)),
        "startLine": 1,
        "numLines": 1,
        "totalLines": 1,
        "truncated": false,
    })
    .to_string();
    let receipt = runtime.tool_model_receipt(
        "read_file",
        &output,
        false,
        &harness_contract::reality::EvidenceRef::observed("tool", "large-exact-read"),
        Some(&harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::reality::EvidenceRef::observed("tool", "large-exact-read"),
            "sha256:large-exact-read",
            output.len() as u64,
            "application/json",
            "artifact://art_large_exact_read",
            "session:test",
        )),
        &ToolModelDeliveryRequirement::exact(vec!["large-read".to_string()]),
        "{}",
    );

    assert!(
        !receipt.truncated,
        "raw={} receipt={} omitted={} standard_limit={} exact_limit={}",
        receipt.raw_tokens,
        receipt.receipt_tokens,
        receipt.omitted_tokens,
        standard.tool_result_budget.per_tool_max_tokens,
        exact.tool_result_budget.per_tool_max_tokens,
    );
    assert_eq!(receipt.omitted_tokens, 0);
    assert!(receipt.summary.contains(&"x".repeat(256)));
}

#[test]
fn complete_read_body_survives_receipt_and_serialized_tool_result() {
    let runtime = ConversationRuntime::new(
        Session::new(), MockApi, StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite), vec!["system".into()],
    ).without_memory();
    let plan = runtime.runtime_budget_plan();
    let padding = "a".repeat(plan.tool_result_budget.per_tool_max_tokens as usize * 3);
    let body = format!("{padding}\nR420_UNIQUE_MIDDLE_CONTENT\n{padding}");
    let output = serde_json::json!({"type":"text","truncated":false,"file":{
        "filePath":"src/full.rs", "content":body,"startLine":1,"numLines":3,"totalLines":3,
        "endsWithNewline":false,"byteLength":body.len(),
        "sha256":format!("sha256:{:x}", Sha256::digest(body.as_bytes()))
    }}).to_string();
    let reference = harness_contract::reality::EvidenceRef::observed("tool", "r420-full");
    // Deterministic old bounded path: the distinct middle is actually removed.
    let bounded = runtime.tool_model_receipt("read_file", &output, false, &reference,
        None, &ToolModelDeliveryRequirement::Bounded, "{}");
    assert!(bounded.truncated);
    assert!(!bounded.summary.contains("R420_UNIQUE_MIDDLE_CONTENT"));
    let complete = runtime.tool_model_receipt("read_file", &output, false, &reference,
        None, &ToolModelDeliveryRequirement::Bounded, r#"{"complete":true}"#);
    assert!(!complete.truncated);
    assert_eq!(complete.omitted_tokens, 0);
    assert!(complete.receipt_tokens + plan.max_output_tokens < plan.subsystem_budget_tokens,
        "fixture must fit the original real context capacity");
    let message = ConversationMessage::tool_result("r420-call", "read_file", &complete.summary, false);
    let provider_messages = crate::provider_runtime_client::convert_messages([&message]);
    let serialized = serde_json::to_vec(&provider_messages).expect("original Provider message codec");
    let wire: serde_json::Value = serde_json::from_slice(&serialized).unwrap();
    let sent = wire[0]["content"][0]["content"][0]["text"].as_str().unwrap();
    assert_eq!(sent, complete.summary);
    assert!(sent.contains("R420_UNIQUE_MIDDLE_CONTENT"));
    let payload: serde_json::Value = serde_json::from_str(
        &sent[sent.find('{').unwrap()..]).unwrap();
    assert_eq!(payload["file"]["content"], body);
    let seal = crate::context_evidence::read_body_seal("read_file", &output).unwrap();
    assert!(matches!(seal.extent, crate::context_evidence::ReadExtent::FileLines {
        source_omitted:false, start_line:1, line_count:3, total_lines:3, ..
    }));
    let descriptor = serde_json::json!({"type":"text","file":{
        "filePath":"src/full.rs","sha256":payload["file"]["sha256"],"byteLength":body.len()
    }}).to_string();
    assert!(crate::context_evidence::read_body_seal("read_file", &descriptor).is_none());
    // The complete marker must not turn unrelated tools into full-body delivery.
    let other = runtime.tool_model_receipt("grep_search", &output, false, &reference,
        None, &ToolModelDeliveryRequirement::Bounded, r#"{"complete":true}"#);
    assert!(other.truncated);
}

#[test]
fn model_observation_promotes_only_a_matching_packed_provider_result() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    let raw_ref = harness_contract::reality::EvidenceRef::observed("tool", "raw-1");
    let access = harness_contract::context::EvidenceAccessRef::durable(
        raw_ref.clone(),
        format!("sha256:{}", "b".repeat(64)),
        16,
        "application/json",
        "artifact://raw-1",
        "session:test",
    );
    let requirement = ToolModelDeliveryRequirement::exact(vec!["required-read".to_string()]);
    let output = r#"{"content":"complete","truncated":false}"#;
    let receipt = runtime.tool_model_receipt(
        "read_file",
        output,
        false,
        &raw_ref,
        Some(&access),
        &requirement,
        "{}",
    );
    runtime
        .record_generated_model_receipt(
            "tool-call-1",
            "read_file",
            &requirement,
            &raw_ref,
            &receipt,
            false,
            output,
        )
        .expect("generated receipt");
    assert!(runtime.turn_model_observations().is_empty());

    let request = ApiRequest {
        bound_step: None,
        prompt: PromptAssembly::new(vec!["system".to_string()]),
        messages: vec![ConversationMessage::tool_result(
            "tool-call-1".to_string(),
            "read_file".to_string(),
            receipt.summary.clone(),
            false,
        )]
        .into(),
        model: "qwen3.8-max".to_string(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "qwen3.8-max",
            32_768,
            4_096,
            128,
            256,
            0,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    };
    let candidates = runtime
        .packed_model_observation_candidates(&request, 2, 1)
        .expect("matching packed request");
    assert_eq!(candidates.len(), 1);
    assert!(runtime.turn_model_observations().is_empty());
    runtime
        .confirm_model_observations(
            candidates,
            "qwen3.8-max-effective",
            &crate::ProviderRequestEvidenceContext {
            graph_owner: None,
                session_id: runtime.session_id().into(),
                execution_id: "unobserved-execution".into(),
                turn_id: "unobserved-turn".into(),
                request_sequence: 2,
                request_compiler_cache_hit: false,
                budget: request.budget.clone(),
                attempt: 1,
            },
            None,
        )
        .expect("missing physical wire remains an honest observation gap");
    let promoted = runtime.turn_model_observations();
    assert!(
        promoted.is_empty(),
        "packed bytes and a helper call cannot attest a real provider continuation"
    );

    let mut corrupted = request;
    corrupted.messages = Vec::<ConversationMessage>::new().into();
    assert!(runtime.packed_model_observation_candidates(&corrupted, 3, 2)
        .expect("a dropped ToolResult is not observed").is_empty());
    assert!(runtime.turn_model_observations().is_empty());
    corrupted.messages = vec![ConversationMessage::tool_result(
        "tool-call-1".to_string(),
        "read_file".to_string(),
        "different bytes".to_string(),
        false,
    )]
    .into();
    assert!(runtime
        .packed_model_observation_candidates(&corrupted, 3, 2)
        .is_err());
}

#[tokio::test]
async fn runtime_prefetch_receipt_survives_first_provider_node_and_is_turn_isolated() {
    #[derive(Clone)]
    struct FailThenCommitApi {
        attempts: Arc<AtomicUsize>,
        requests: Arc<std::sync::Mutex<Vec<ApiRequest>>>,
    }

    impl ApiClient for FailThenCommitApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.requests.lock().expect("requests").push(request);
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                Box::pin(futures::stream::iter([Err(RuntimeError::new(
                    "non-retryable provider rejection",
                ))]))
            } else {
                Box::pin(futures::stream::iter([
                    Ok(AssistantEvent::TextDelta(
                        "grounded continuation committed".to_string(),
                    )),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        FailThenCommitApi {
            attempts: Arc::clone(&attempts),
            requests: Arc::clone(&requests),
        },
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    runtime.set_active_model("qwen3.8-max");

    // Host admission starts the turn epoch before graph-owned Runtime
    // prefetch creates this exact receipt and transcript entry.
    runtime.begin_turn_runtime_epoch().await;
    let raw_ref =
        harness_contract::reality::EvidenceRef::observed("tool", "runtime-prefetch-raw-1");
    let requirement = ToolModelDeliveryRequirement::exact(vec!["required-agent-read".to_string()]);
    let output = r#"{"content":"complete runtime-prefetched evidence","truncated":false}"#;
    let receipt =
        runtime.tool_model_receipt("read_file", output, false, &raw_ref, None, &requirement, "{}");
    runtime
        .record_generated_model_receipt(
            "runtime-focus-verify-0-0",
            "read_file",
            &requirement,
            &raw_ref,
            &receipt,
            false,
            output,
        )
        .expect("generated prefetch receipt");
    runtime
        .session
        .write()
        .await
        .push_message(ConversationMessage::tool_result(
            "runtime-focus-verify-0-0",
            "read_file",
            receipt.summary.clone(),
            false,
        ))
        .expect("runtime-prefetched ToolResult transcript");
    runtime
        .begin_turn_strategy("prefetch-lifecycle-turn", "answer from exact evidence")
        .expect("turn strategy admission");

    let first = runtime
        .execute_model_step("answer from exact evidence", true)
        .await;
    assert!(first.is_err(), "first Provider response must fail");
    assert_eq!(
        runtime
            .turn_generated_model_receipts
            .lock()
            .expect("generated receipts")
            .len(),
        1,
        "the first Provider node is not allowed to erase Runtime prefetch receipts"
    );
    assert!(
        runtime.turn_model_observations().is_empty(),
        "a failed Provider response must never promote packed candidates"
    );

    runtime
        .execute_model_step("answer from exact evidence", false)
        .await
        .expect("same-turn valid redelivery must commit");
    let observations = runtime.turn_model_observations();
    assert!(
        observations.is_empty(),
        "mock API has no original physical acquisition or durable wire evidence"
    );
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| request
            .messages
            .iter()
            .any(|message| message.blocks.iter().any(|block| matches!(
                block,
                ContentBlock::ToolResult { tool_use_id, output, is_error: false, .. }
                    if tool_use_id == "runtime-focus-verify-0-0"
                        && output == &receipt.summary
            )))));
    drop(requests);

    runtime.begin_turn_runtime_epoch().await;
    assert!(runtime
        .turn_generated_model_receipts
        .lock()
        .expect("generated receipts")
        .is_empty());
    assert!(runtime.turn_model_observations().is_empty());
    assert!(runtime.turn_tool_observations().is_empty());
    assert!(runtime.turn_evidence_audits().is_empty());
}
use fact_kernel::FactLedger;
use std::sync::atomic::{AtomicUsize, Ordering};

fn rendered_prompt(prompt: &PromptAssembly) -> String {
    let mut segments = prompt.trusted_system.clone();
    segments.extend(prompt.contextual_messages());
    segments.join("\n")
}

#[test]
fn direct_eval_candidate_keeps_policy_complete_graph() {
    use harness_contract::core::ExecutionPattern;
    use harness_contract::strategy::ExecutionCandidateKind;

    let judge = eval_override_selection(
            "direct",
            false,
            "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=judge:temperature_milli=0",
        )
        .expect("judge override")
        .expect("judge selection");
    assert_eq!(
        judge,
        (ExecutionCandidateKind::Direct, ExecutionPattern::Execute)
    );

    let business = eval_override_selection(
        "direct",
        false,
        "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=business",
    )
    .expect("business override")
    .expect("business selection");
    assert_eq!(
        business,
        (ExecutionCandidateKind::Direct, ExecutionPattern::Execute)
    );

    let parallel = eval_override_selection(
        "parallel_tools",
        false,
        "runtime-execution-resource-manager:corpus=auto-strategy-v1:provider_constraint=business",
    )
    .expect("parallel override")
    .expect("parallel selection");
    assert_eq!(
        parallel,
        (
            ExecutionCandidateKind::ParallelTools,
            ExecutionPattern::Execute
        )
    );
}

#[test]
fn first_step_final_answer_remains_model_owned_for_explicit_collaboration() {
    for (case, objective, response) in [
        (
            "explicit_collaboration",
            "这是复杂架构审查，必须实际启动一个多 Agent 协作团队完成分析。",
            "我会开始分析。",
        ),
        (
            "chinese_launch",
            "发起一个团队，生成公开技术标准的全面深度调研报告",
            "无法组队",
        ),
        (
            "sequential_research_then_html",
            "用一个团队调研公开技术标准，然后另一个团队负责生成一套 HTML 研究报告网站",
            "调研结束。",
        ),
        (
            "explicit_non_collaboration_hint_wording",
            "先自主选择并实际启动合适的协作团队，分别完成三个独立审查。",
            "我会开始分析。",
        ),
        (
            "ordinal_groups",
            "第一组研究 Runtime，第二组审查 Gateway，第三组汇总结论",
            "premature",
        ),
    ] {
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: response.to_string(),
            },
        );
        assert!(
            matches!(intent, ModelStepIntent::FinalAnswer { .. }),
            "case {case}: the first-step FinalAnswer must remain model-owned"
        );
    }
}

#[test]
fn model_tool_calls_are_bounded_by_the_current_exposure_lease() {
    let calls = vec![
        ModelToolCall {
            binding: None,
            id: "read".to_string(),
            name: "read_file".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "hidden".to_string(),
            name: "shell".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "missing".to_string(),
            name: "invented_tool".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
    ];

    assert_eq!(
        unexposed_model_tool_names(&calls, &BTreeSet::from(["read_file".to_string()])),
        vec!["invented_tool".to_string(), "shell".to_string()]
    );
}

#[test]
fn provider_tool_name_aliases_only_resolve_inside_the_current_exposure_lease() {
    let mut calls = vec![
        ModelToolCall {
            binding: None,
            id: "search".to_string(),
            name: "web_search".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "hidden".to_string(),
            name: "shell-command".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
    ];
    let executor = StaticToolExecutor::new().register("web_search", |_| Ok(String::new()));
    canonicalize_model_tool_names(&mut calls, &executor);
    assert_eq!(calls[0].name, "web_search");
    assert_eq!(calls[1].name, "shell-command");

    let mut ambiguous = vec![ModelToolCall {
        binding: None,
        id: "ambiguous".to_string(),
        name: "web search".to_string(),
        input: "{}".to_string(),
        depends_on: Vec::new(),
    }];
    let ambiguous_executor = StaticToolExecutor::new()
        .register("web_search", |_| Ok(String::new()))
        .register("web-search", |_| Ok(String::new()));
    canonicalize_model_tool_names(&mut ambiguous, &ambiguous_executor);
    assert_eq!(
        ambiguous[0].name, "web search",
        "ambiguous aliases must fail closed"
    );
}

#[test]
fn e2e_negative_team_fixture_is_marker_scoped_and_produces_real_cost_warning() {
    let prompt = "must start a Team for runtime gateway frontend [cowd-e2e:explicit-team-negative]";
    let mut input = harness_contract::strategy::StrategyInput::from_prompt(prompt);
    apply_named_e2e_strategy_fixture(&mut input, prompt, "explicit-team-negative")
        .expect("known fixture is accepted");
    let decision = harness_contract::strategy::decide_strategy(&input);
    let team = decision
        .candidate_estimates
        .iter()
        .find(|estimate| {
            estimate.candidate == harness_contract::strategy::ExecutionCandidateKind::Team
        })
        .expect("fixture retains Team estimate");

    assert_eq!(
        decision.selected_candidate,
        harness_contract::strategy::ExecutionCandidateKind::Team
    );
    assert!(team.effective_duration_ms() >= team.estimated_serial_ms);
    assert!(decision.reasons.iter().any(|reason| {
        reason.contains("no measured duration advantage or paired quality proof")
    }));

    let mut unmarked = harness_contract::strategy::StrategyInput::from_prompt(
        "must start a Team for runtime gateway frontend",
    );
    let unmarked_prompt = unmarked.prompt.clone();
    apply_named_e2e_strategy_fixture(&mut unmarked, &unmarked_prompt, "explicit-team-negative")
        .expect("known fixture is inert without its marker");
    assert!(unmarked.candidate_costs.is_empty());
}

#[test]
fn explicit_team_requirement_does_not_preempt_first_step_model_tool_calls() {
    let objective = "必须实际启动协作团队，再分析这些模块。";
    let decision = build_runtime_execution_decision(objective, None);
    let classified = classify_model_step_intent(
        String::new(),
        vec![ModelToolCall {
            binding: None,
            id: "provider-agent-helper".to_string(),
            name: "agent_helper".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        }],
    );
    let intent = enforce_explicit_team_requirement(objective, true, &decision, classified);

    let ModelStepIntent::ToolCalls { calls } = intent else {
        panic!("first-step tool calls must stay untouched");
    };
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "agent_helper");
    assert!(!calls.iter().any(is_agent_collaboration_action));
}

#[test]
fn ordinary_tool_names_never_create_runtime_control_intents() {
    for name in ["agent_status", "permission_report", "replan_index"] {
        let intent = classify_model_step_intent(
            String::new(),
            vec![ModelToolCall {
                binding: None,
                id: format!("call-{name}"),
                name: name.to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            }],
        );
        let ModelStepIntent::ToolCalls { calls } = intent else {
            panic!("ordinary tool `{name}` must remain a ToolCall");
        };
        assert_eq!(calls[0].name, name);
    }
}

#[test]
fn ordinary_complex_work_keeps_model_directed_team_choice() {
    let objective = "分析 runtime、memory 和 gateway 的边界。";
    let decision = build_runtime_execution_decision(objective, None);
    let intent = enforce_explicit_team_requirement(
        objective,
        true,
        &decision,
        ModelStepIntent::FinalAnswer {
            text: "普通复杂分析。".to_string(),
        },
    );
    assert!(matches!(intent, ModelStepIntent::FinalAnswer { .. }));
}

#[test]
fn explicit_team_requirement_recognizes_negative_start_constraint() {
    assert_eq!(
        understand(&StrategyInput::from_prompt(
            "请单人完成审查，不要启动团队。"
        ))
        .required_team_count,
        0
    );
}

#[test]
fn delegated_leaf_turn_does_not_force_a_second_team_from_inherited_wording() {
    let objective = "必须实际启动协作团队，再分析这些模块。";
    let decision = build_runtime_execution_decision(objective, None);
    let intent = apply_explicit_team_requirement(
        false,
        objective,
        true,
        &decision,
        ModelStepIntent::FinalAnswer {
            text: "leaf evidence".to_string(),
        },
    );
    assert!(matches!(intent, ModelStepIntent::FinalAnswer { .. }));
}

#[test]
fn prepared_vision_payload_becomes_user_image_message() {
    let output = serde_json::json!({
        "tool": "vision_analyze",
        "status": "prepared",
        "image_path": "/tmp/cowd-test.png",
        "media_type": "image/png",
        "prompt": "describe it",
        "image_base64": "aW1hZ2U=",
        "size_bytes": 5
    })
    .to_string();

    let payload = prepared_vision_payload("vision_analyze", &output, false)
        .expect("prepared vision payload should parse");
    let message = vision_user_message(&payload);

    assert_eq!(message.role, MessageRole::User);
    assert!(matches!(
        message.blocks.get(1),
        Some(ContentBlock::Image {
            media_type,
            data,
            source_path
        }) if media_type == "image/png"
            && data == "aW1hZ2U="
            && source_path.as_deref() == Some("/tmp/cowd-test.png")
    ));

    let receipt = vision_tool_model_receipt(
        &payload,
        &harness_contract::reality::EvidenceRef::new("tool", "vision-evidence"),
    );
    assert!(receipt.starts_with("Tool `vision_analyze` completed."));
    assert!(receipt.contains("tool://vision-evidence"));
    assert!(receipt.contains("image/png"));
}

#[test]
fn prepared_vision_payload_rejects_errors_and_non_vision_tools() {
    let output = serde_json::json!({
        "tool": "vision_analyze",
        "status": "prepared",
        "image_path": "/tmp/cowd-test.png",
        "media_type": "image/png",
        "prompt": "describe it",
        "image_base64": "aW1hZ2U=",
        "size_bytes": 5
    })
    .to_string();
    assert!(prepared_vision_payload("vision_analyze", &output, true).is_none());
    assert!(prepared_vision_payload("read_file", &output, false).is_none());
    let broken = serde_json::json!({
        "tool": "vision_analyze",
        "status": "prepared",
        "image_path": "/tmp/cowd-test.png",
        "media_type": "image/png",
        "prompt": "describe it",
        "image_base64": "",
        "size_bytes": 0
    })
    .to_string();
    assert!(prepared_vision_payload("vision_analyze", &broken, false).is_none());
}

#[test]
fn image_user_message_from_path_reads_image_as_structured_block() {
    let path = std::env::temp_dir().join(format!(
        "cowd-runtime-image-message-{}.jpg",
        uuid::Uuid::new_v4()
    ));
    fs::write(&path, b"fake-jpeg-bytes").expect("test image should write");

    let message = image_user_message_from_path(&path, "image/jpeg", "describe it")
        .expect("image message should be prepared");

    assert_eq!(message.role, MessageRole::User);
    assert!(message.blocks.iter().any(|block| {
        matches!(block, ContentBlock::Image { media_type, data, source_path }
                if media_type == "image/jpeg"
                    && data == "ZmFrZS1qcGVnLWJ5dGVz"
                    && source_path.as_deref() == Some(path.to_string_lossy().as_ref()))
    }));
    let _ = fs::remove_file(path);
}

#[test]
fn provider_transport_policy_scales_with_actual_request_size() {
    let small = ApiRequest {
        bound_step: None,
        prompt: PromptAssembly::new(vec!["system".to_string()]),
        messages: vec![ConversationMessage::user_text("status".to_string())].into(),
        model: "test".to_string(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "test", 32_768, 4_096, 128, 256, 0,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    };
    let large = ApiRequest {
        bound_step: None,
        prompt: PromptAssembly::new(vec!["system".repeat(5_000)]),
        messages: vec![ConversationMessage::user_text("evidence".repeat(10_000))].into(),
        model: "test".to_string(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "test", 1_000_000, 32_000, 128, 256, 0,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    };

    assert!(
        provider_transport_policy(1_000_000, &large).idle_timeout
            > provider_transport_policy(32_768, &small).idle_timeout
    );
}

fn token_reservation_request() -> ApiRequest {
    ApiRequest {
        bound_step: None,
        prompt: PromptAssembly::new(vec!["system".to_string()]),
        messages: vec![ConversationMessage::user_text("bounded".to_string())].into(),
        model: "test".to_string(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "test", 1_000, 100, 20, 5, 10,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    }
}

#[test]
fn provider_reservation_set_records_delegated_admission_instead_of_blocking() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("session-rollback", "eval-rollback", 1_000)
        .expect("install evaluation budget");
    let lease = guard.lease();
    let child = harness_contract::context::ChildExecutionBudgetReservation::single(
        "delegated-small",
        "agent-small",
        "agent",
        30,
        u64::MAX,
        1,
    );
    let ledger = crate::execution_core::budget::ParentExecutionBudgetLedger::new(
        Arc::new(RuntimeEventStore::for_test()),
        child.parent_budget.clone(),
    )
    .unwrap();
    let delegated = (ledger.clone(), child);
    let mut request = token_reservation_request();
    let reservation_set = ProviderTokenReservationSet::acquire(
        Some(&lease),
        Some(&delegated),
        "claude-haiku-4-5-20251001",
        &mut request,
    )
    .expect("record-only delegated budget must admit the provider");
    assert!(reservation_set.delegated.is_some());
    assert_eq!(request.budget.requested_output_tokens, 100);
    assert!(ledger.snapshot().unwrap().reserved_tokens > 0);
}

#[test]
fn provider_reservation_set_leaves_delegated_untouched_when_evaluation_admission_fails() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("session-small", "eval-small", 30)
        .expect("install evaluation budget");
    let lease = guard.lease();
    let child = harness_contract::context::ChildExecutionBudgetReservation::single(
        "delegated-untouched",
        "agent-untouched",
        "agent",
        1_000,
        u64::MAX,
        1,
    );
    let ledger = crate::execution_core::budget::ParentExecutionBudgetLedger::new(
        Arc::new(RuntimeEventStore::for_test()),
        child.parent_budget.clone(),
    )
    .unwrap();
    let delegated = (ledger.clone(), child);
    let mut request = token_reservation_request();
    assert!(ProviderTokenReservationSet::acquire(
        Some(&lease),
        Some(&delegated),
        "claude-haiku-4-5-20251001",
        &mut request
    )
    .is_err());

    let evaluation = guard.snapshot().expect("evaluation snapshot");
    assert_eq!(evaluation.consumed, 0);
    assert_eq!(evaluation.outstanding, 0);
    assert_eq!(ledger.snapshot().unwrap().reserved_tokens, 0);
}

#[test]
fn provider_usage_missing_zero_and_failed_partial_are_distinct_at_original_reducer() {
    let missing = ModelStreamReducer::new(None, None, "missing".into()).finish("completed");
    assert_eq!(missing.observed_usage(true), None);
    assert_eq!(
        RuntimeError::new("missing")
            .with_provider_usage(missing.observed_usage(true))
            .provider_usage(),
        None
    );
    let mut zero = ModelStreamReducer::new(None, None, "zero".into());
    zero.apply(
        AssistantEvent::Usage(TokenUsage::default()),
        std::time::Instant::now(),
    )
    .unwrap();
    let zero = zero.finish("completed");
    assert_eq!(zero.observed_usage(true), Some(TokenUsage::default()));
    assert_eq!(
        RuntimeError::new("refused after known0")
            .with_provider_usage(zero.observed_usage(true))
            .provider_usage(),
        Some(TokenUsage::default())
    );
    let mut partial = ModelStreamReducer::new(None, None, "partial".into());
    partial
        .apply(
            AssistantEvent::Usage(TokenUsage {
                input_tokens: 17,
                ..Default::default()
            }),
            std::time::Instant::now(),
        )
        .unwrap();
    let partial = partial.finish("failed");
    assert_eq!(
        RuntimeError::new("transport failed")
            .with_provider_usage(partial.observed_usage(false))
            .provider_usage(),
        None
    );
    assert_eq!(
        partial.usage.input_tokens, 17,
        "observed partial evidence is retained"
    );
    assert_eq!(
        partial.observed_usage(false),
        None,
        "a failed transport cannot finalize the charge from partial input usage"
    );
}

#[test]
fn evaluation_usage_missing_keeps_charge_but_reported_zero_refunds_once() {
    for observed in [false, true] {
        let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
        let guard = registry
            .install("session-usage-truth", "eval-usage-truth", 1_000)
            .unwrap();
        let lease = guard.lease();
        let mut request = token_reservation_request();
        let mut reservation =
            ProviderTokenReservationSet::acquire(Some(&lease), None, "test", &mut request).unwrap();
        reservation.mark_dispatched();
        let original_charge = guard.snapshot().unwrap().consumed;
        assert!(original_charge > 0);
        let usage = observed.then_some(TokenUsage::default());
        reservation.reconcile(usage).unwrap();
        reservation.reconcile(usage).unwrap();
        drop(reservation);
        let result = guard.snapshot().unwrap();
        assert_eq!(result.consumed, if observed { 0 } else { original_charge });
        assert_eq!(result.outstanding, 0);
        assert!(!result.breached);
    }
}

#[test]
fn advisory_evaluation_lease_never_blocks_provider_admission() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install_advisory("session-advisory", "eval-advisory", 30)
        .expect("advisory evaluation lease");
    let lease = guard.lease();
    let mut request = token_reservation_request();
    let mut reservation =
        ProviderTokenReservationSet::acquire(Some(&lease), None, "test", &mut request)
            .expect("advisory lease must not block an oversized request");
    reservation.mark_dispatched();
    reservation
        .reconcile(model_protocol::usage::TokenUsage {
            input_tokens: 100,
            output_tokens: 100,
            ..Default::default()
        })
        .expect("advisory reconciliation");
    let snapshot = guard.snapshot().expect("advisory snapshot");
    assert_eq!(snapshot.limit, 30);
    assert_eq!(snapshot.consumed, 200);
    assert!(!snapshot.breached);
    assert_eq!(snapshot.outstanding, 0);
}

#[test]
fn evaluation_provider_token_leases_are_isolated_by_session_binding() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let small_guard = registry
        .install("session-small", "eval-small", 30)
        .expect("small lease");
    let large_guard = registry
        .install("session-large", "eval-large", 1_000)
        .expect("large lease");
    assert!(registry
        .install("session-small", "duplicate", 1_000)
        .is_err());

    let small = small_guard.lease();
    let large = large_guard.lease();
    let mut rejected = token_reservation_request();
    assert!(
        ProviderTokenReservationSet::acquire(Some(&small), None, "test", &mut rejected).is_err()
    );

    let mut admitted = token_reservation_request();
    let reservation =
        ProviderTokenReservationSet::acquire(Some(&large), None, "test", &mut admitted)
            .expect("large Session lease must admit independently");
    assert_eq!(small_guard.snapshot().unwrap().consumed, 0);
    assert!(large_guard.snapshot().unwrap().consumed > 0);
    drop(reservation);
    assert_eq!(large_guard.snapshot().unwrap().consumed, 0);

    let mut unbound = token_reservation_request();
    ProviderTokenReservationSet::acquire(None, None, "test", &mut unbound)
        .expect("unbound Conversation must ignore every registered lease");
    assert_eq!(small_guard.snapshot().unwrap().consumed, 0);
    assert_eq!(large_guard.snapshot().unwrap().consumed, 0);
}

#[test]
fn evaluation_lease_charges_provider_estimation_variance_to_real_headroom() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("session-variance", "eval-variance", 1_000)
        .expect("evaluation lease");
    let lease = guard.lease();
    let mut request = token_reservation_request();
    let mut reservation =
        ProviderTokenReservationSet::acquire(Some(&lease), None, "test", &mut request)
            .expect("estimated request fits");
    reservation.mark_dispatched();
    reservation
        .reconcile(model_protocol::usage::TokenUsage {
            input_tokens: 350,
            output_tokens: 100,
            cache_read_input_tokens: 50,
            ..Default::default()
        })
        .expect("measured variance fits the unreserved lease headroom");

    let snapshot = guard.snapshot().expect("settled snapshot");
    assert_eq!(snapshot.consumed, 500);
    assert_eq!(snapshot.input_consumed, 350);
    assert_eq!(snapshot.output_consumed, 100);
    assert_eq!(snapshot.cached_consumed, 50);
    assert_eq!(snapshot.outstanding, 0);
    assert!(!snapshot.breached);
}

#[test]
fn evaluation_lease_still_breaches_when_actual_usage_exceeds_headroom() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("session-overrun", "eval-overrun", 300)
        .expect("evaluation lease");
    let lease = guard.lease();
    let mut request = token_reservation_request();
    let mut reservation =
        ProviderTokenReservationSet::acquire(Some(&lease), None, "test", &mut request)
            .expect("estimated request fits");
    reservation.mark_dispatched();
    reservation
        .reconcile(model_protocol::usage::TokenUsage {
            input_tokens: 350,
            output_tokens: 100,
            cache_read_input_tokens: 50,
            ..Default::default()
        })
        .expect("evaluation reconciliation is infallible");

    let snapshot = guard.snapshot().expect("breached snapshot");
    assert_eq!(snapshot.consumed, snapshot.limit);
    assert_eq!(snapshot.outstanding, 0);
    assert!(snapshot.breached);
}

#[test]
fn explicit_provider_telemetry_lease_accepts_large_operator_supplied_capacity() {
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("session-large-live", "eval-large-live", 8_000_000)
        .expect("large collaboration telemetry value is accepted");
    assert_eq!(guard.snapshot().unwrap().limit, 8_000_000);
    assert!(registry
        .install("session-over-limit", "eval-over-limit", 20_000_001,)
        .is_ok());
}

#[test]
fn candidate_packer_accounts_for_history_schema_and_omits_packet_tail() {
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["builtin policy".to_string()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("test");
    let mut prompt = PromptAssembly::new(vec!["runtime control".repeat(60)]);
    for source_id in (0..64).map(|index| format!("packet-{index}")) {
        prompt.contextual_packets.push(crate::PromptContextPacket {
            memory_source_readset: Vec::new(),
            skill_pin: None,
            authority: ContextAuthority::Project,
            source: ContextSourceKind::Workspace,
            role: ContextRole::Evidence,
            source_id,
            content: "evidence ".repeat(900),
            evidence: Vec::new(),
            utility_score_milli: 0,
        });
    }

    let history: crate::HistoryView =
        vec![ConversationMessage::user_text("history ".repeat(300))].into();
    let request = runtime
        .pack_provider_attempt(
            &prompt,
            &history,
            "test",
            super::ProviderContextInventory {
                tool_count: 2,
                tool_schema_tokens: 1_200,
                ..Default::default()
            },
        )
        .expect("candidate request should fit after contextual packing");

    assert!(request.budget.input_total_tokens() <= request.budget.hard_input_cap_tokens);
    assert_eq!(
        request.budget.target_input_cap_tokens,
        request.budget.hard_input_cap_tokens
    );
    assert!(!request.prompt.contextual_packets.is_empty());
    assert!(!request.budget.omitted_packet_ids.is_empty());
}

#[test]
fn custom_model_output_budget_preserves_production_input_window() {
    let context_window = 16_384;
    let output =
        super::provider_output_budget_hint("custom-model-with-generic-cap", context_window, None);
    assert_eq!(output, 4_000);

    let mut runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["production system policy ".repeat(400)],
    )
    .without_memory();
    runtime.set_active_model("custom-model-with-generic-cap");
    runtime = runtime.with_model_context_window(context_window);

    let history: crate::HistoryView = vec![ConversationMessage::user_text(
        "current durable user turn ".repeat(20),
    )]
    .into();
    let request = runtime
        .pack_provider_attempt(
            &PromptAssembly::new(vec!["production system policy ".repeat(400)]),
            &history,
            "custom-model-with-generic-cap",
            super::ProviderContextInventory {
                tool_count: 3,
                tool_schema_tokens: 1_740,
                ..Default::default()
            },
        )
        .expect("production prompt and bootstrap schemas must reach a 16k custom model");

    assert_eq!(request.budget.requested_output_tokens, u64::from(output));
    assert_eq!(request.budget.provider_max_output_tokens, 64_000);
    assert_eq!(request.budget.max_output_source, "assumed");
    assert_eq!(request.budget.preferred_output_tokens, 4_000);
    assert_eq!(request.budget.output_floor_tokens, 1_024);
    assert!(request.budget.executable);
    assert!(request.budget.fixed_input_tokens <= request.budget.hard_input_cap_tokens);
}

#[derive(Clone)]
struct RouteRecordingApi {
    requests: Arc<std::sync::Mutex<Vec<ApiRequest>>>,
}

impl ApiClient for RouteRecordingApi {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let model = request.model.clone();
        self.requests.lock().expect("requests").push(request);
        let events = if model == "primary" {
            vec![Err(RuntimeError::new("primary unavailable"))]
        } else {
            vec![
                Ok(AssistantEvent::ProviderModel {
                    identity: harness_contract::outcome::ProviderIdentity {
                        registry_revision: Some(1),
                        provider_name: "test".to_string(),
                        model,
                        profile: None,
                        protocol: Some("completions".to_string()),
                        capabilities: std::collections::BTreeMap::new(),
                    },
                }),
                Ok(AssistantEvent::TextDelta("fallback answer".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]
        };
        Box::pin(futures::stream::iter(events))
    }
}

#[derive(Clone)]
struct AccountScopedRouteApi {
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    separate_fallback_account: bool,
}

impl ApiClient for AccountScopedRouteApi {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let model = request.model.clone();
        self.requests.lock().expect("requests").push(model.clone());
        let events = if model == "primary" || !self.separate_fallback_account {
            vec![Err(
                RuntimeError::with_provider_failure_metadata_retry_after_and_scope(
                    "Insufficient Balance",
                    None,
                    false,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                    None,
                    false,
                    model_protocol::provider_failure::ProviderFailureScope::Account,
                ),
            )]
        } else {
            vec![
                Ok(AssistantEvent::TextDelta(
                    "independent account fallback".to_string(),
                )),
                Ok(AssistantEvent::MessageStop),
            ]
        };
        Box::pin(futures::stream::iter(events))
    }

    fn provider_name_for_model(&self, model: &str) -> Option<String> {
        if model == "fallback" && self.separate_fallback_account {
            Some("qwen-tokenplan".to_string())
        } else {
            Some("deepseek".to_string())
        }
    }
}

#[derive(Clone)]
struct TurnAccountCircuitApi {
    requests: Arc<std::sync::Mutex<Vec<String>>>,
    fallback_attempts: Arc<AtomicUsize>,
}

impl ApiClient for TurnAccountCircuitApi {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let model = request.model;
        self.requests.lock().unwrap().push(model.clone());
        let events = if model == "primary" {
            vec![Err(
                RuntimeError::with_provider_failure_metadata_retry_after_and_scope(
                    "Insufficient Balance",
                    None,
                    false,
                    crate::execution_core::graph::ResourceResultClass::Failed,
                    None,
                    false,
                    model_protocol::provider_failure::ProviderFailureScope::Account,
                ),
            )]
        } else if self.fallback_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![Err(RuntimeError::new(
                "independent fallback request failed once",
            ))]
        } else {
            vec![
                Ok(AssistantEvent::TextDelta("fallback recovered".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]
        };
        Box::pin(futures::stream::iter(events))
    }

    fn provider_name_for_model(&self, model: &str) -> Option<String> {
        Some(if model == "primary" {
            "deepseek".to_string()
        } else {
            "qwen-tokenplan".to_string()
        })
    }
}

#[derive(Clone)]
struct CapacityRecordingApi {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl ApiClient for CapacityRecordingApi {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let active_now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active_now, Ordering::SeqCst);
        let active = Arc::clone(&self.active);
        Box::pin(
            futures::stream::once(async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(AssistantEvent::TextDelta("capacity answer".to_string()))
            })
            .chain(futures::stream::iter([Ok(AssistantEvent::MessageStop)])),
        )
    }
}

async fn stale_session_execution_fence(
    session_id: &str,
    request_id: &str,
) -> crate::SessionExecutionFence {
    let store = Arc::new(crate::test_support::session_store());
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "test".to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: Some("test-model".to_string()),
            created_at: "2026-07-26T00:00:00Z".to_string(),
            last_activity: "2026-07-26T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let now = super::now_ms();
    let request = session::SessionRuntimeOutboxRequest {
        input_id: format!("input-{request_id}"),
        request_id: request_id.to_string(),
        turn_id: format!("turn-{request_id}"),
        message_id: format!("message-{request_id}"),
        session_generation: 1,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        created_at_ms: now,
        runtime_options_json: None,
    };
    store
        .append_ingress_with_runtime_outbox(
            session_id,
            "user",
            Some(r#"[{"type":"text","text":"fenced"}]"#),
            now,
            &request,
        )
        .await
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("fence-worker", now, 60_000, 1, &[])
        .await
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().expect("claim token");
    let running = store
        .mark_session_runtime_outbox_running(
            request_id,
            "fence-worker",
            1,
            &token,
            claimed.revision,
            now,
        )
        .await
        .unwrap();
    let fence = crate::SessionExecutionFence::from_claim(
        crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        request_id,
        session_id,
        1,
        running.sequence,
        "fence-worker",
        token,
    )
    .unwrap()
    .with_claim_fence_epoch(running.claim_fence_epoch.expect("original claim epoch"));
    store
        .advance_session_input_generation(
            session_id,
            1,
            true,
            "test",
            "invalidate execution before side effect",
            now + 1,
        )
        .await
        .unwrap();
    fence
}

#[tokio::test]
async fn stale_session_fence_blocks_provider_before_transport_side_effect() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    #[derive(Clone)]
    struct CountingApi(Arc<AtomicUsize>);
    impl ApiClient for CountingApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::iter([Ok(AssistantEvent::MessageStop)]))
        }
    }

    let calls = Arc::new(AtomicUsize::new(0));
    let fence = stale_session_execution_fence("fence-provider", "request-provider").await;
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        CountingApi(Arc::clone(&calls)),
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_session_execution_fence(fence);
    runtime
        .begin_turn_strategy("fence-provider-turn", "answer")
        .unwrap();

    let result = runtime.execute_model_step("answer", true).await;
    assert!(result.is_err(), "stale provider fence result: {result:?}");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "provider transport must not start after durable ownership is lost"
    );
}

#[tokio::test]
async fn stale_session_fence_blocks_tool_before_executor_side_effect() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let fence = stale_session_execution_fence("fence-tool", "request-tool").await;
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new().register("read_file", move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok("should not execute".to_string())
        }),
        PermissionPolicy::new(PermissionMode::DangerFullAccess),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_session_execution_fence(fence);
    runtime
        .begin_turn_strategy("fence-tool-turn", "read")
        .unwrap();
    let result = runtime
        .execute_tool_batch_step(
            &[ModelToolCall {
                binding: None,
                id: "read-fenced".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md"}"#.to_string(),
                depends_on: Vec::new(),
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await;
    let result = result.expect("stale tool fence is returned as a governed tool result");
    assert_eq!(result.failed, 1, "stale tool fence result: {result:?}");
    assert!(result.messages.iter().any(|message| {
        message.blocks.iter().any(|block| {
            matches!(
                block,
                crate::session::ContentBlock::ToolResult {
                    output,
                    is_error: true,
                    ..
                } if output.contains("Session execution fence rejected")
            )
        })
    }));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "tool executor must not start after durable ownership is lost"
    );
}

#[test]
fn automatic_memory_resource_handle_follows_actual_conversation_injection() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota,
    };
    let runtime = ConversationRuntime::new_with_features_and_selected_memory(
        Session::new(),
        CapacityRecordingApi {
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        },
        Arc::new(StaticToolExecutor::new()),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
        &RuntimeFeatureConfig::default(),
        None,
    );
    let source = Arc::clone(&runtime.provider_resources);
    let manager = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Provider,
        ResourceQuota::new(1, 1, 1).unwrap(),
    )]));
    let config = Arc::new(std::sync::RwLock::new(
        crate::ProviderResourceConfig::default(),
    ));
    assert!(source.read().unwrap().manager.is_none());
    let runtime = runtime
        .with_provider_admission(Arc::clone(&manager))
        .with_provider_resource_config(Arc::clone(&config));
    let handles = source.read().unwrap().clone();
    assert!(Arc::ptr_eq(handles.manager.as_ref().unwrap(), &manager));
    assert!(Arc::ptr_eq(&handles.config, &config));
    assert!(Arc::ptr_eq(&runtime.provider_resources, &source));
}

#[tokio::test]
async fn ordinary_conversations_share_one_provider_admission_owner() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota,
    };

    let manager = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Provider,
        ResourceQuota::new(1, 1, 1).unwrap(),
    )]));
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let runtime = |turn: &str| {
        let runtime = ConversationRuntime::new(
            Session::new(),
            CapacityRecordingApi {
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
            },
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            SystemPromptBuilder::new().build(),
        )
        .without_memory()
        .with_model_context_window(128_000)
        .with_provider_admission(Arc::clone(&manager));
        runtime
            .begin_turn_strategy(turn, "answer with current evidence")
            .unwrap();
        runtime
    };
    let mut first = runtime("provider-capacity-1");
    let mut second = runtime("provider-capacity-2");
    let (first, second) = tokio::join!(
        first.execute_model_step("answer with current evidence", true),
        second.execute_model_step("answer with current evidence", true),
    );
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(max_active.load(Ordering::SeqCst), 1);
    let snapshot = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
    assert_eq!(snapshot.active_leases, 0);
    assert_eq!(snapshot.queued_waiters, 0);
}

#[tokio::test]
async fn runtime_owns_fallback_attempts_and_repacks_each_candidate() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = RouteRecordingApi {
        requests: Arc::clone(&requests),
    };
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        api,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("primary");
    *runtime.fallbacks.write().unwrap() = vec!["fallback".to_string()];
    runtime
        .begin_turn_strategy("test-fallback-turn", "summarize the current state")
        .expect("test turn strategy admission");

    let result = runtime
        .execute_model_step("summarize the current state", true)
        .await
        .expect("fallback candidate should complete");
    assert_eq!(result.model.as_deref(), Some("fallback"));
    let requests = requests.lock().expect("requests");
    assert_eq!(
        requests
            .iter()
            .map(|request| request.model.as_str())
            .collect::<Vec<_>>(),
        vec!["primary", "fallback"]
    );
    assert!(requests.iter().all(|request| request.budget.executable));
    assert!(requests.iter().all(|request| {
        request.prompt.trusted_system.first().is_some_and(|head| {
            head.contains("You are Cowd") && head.contains(COWD_IDENTITY_CONTRACT_VERSION)
        })
    }));
}

#[tokio::test]
async fn account_failure_skips_same_account_models_but_preserves_independent_fallback() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota,
    };
    let manager = Arc::new(ExecutionResourceManager::new(
        [
            ExecutionResourceKind::Provider,
            ExecutionResourceKind::ProviderAccount("deepseek".into()),
            ExecutionResourceKind::ProviderAccount("qwen-tokenplan".into()),
            ExecutionResourceKind::ProviderModel("primary".into()),
            ExecutionResourceKind::ProviderModel("fallback".into()),
            ExecutionResourceKind::ProviderTokenPool("deepseek".into()),
            ExecutionResourceKind::ProviderTokenPool("qwen-tokenplan".into()),
        ]
        .into_iter()
        .map(|kind| {
            let quota = if matches!(kind, ExecutionResourceKind::ProviderTokenPool(_)) {
                ResourceQuota::new(1, 256, 256).unwrap()
            } else {
                ResourceQuota::new(1, 8, 16).unwrap()
            };
            (kind, quota)
        }),
    ));
    let same_account_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut same_account = ConversationRuntime::new(
        Session::new(),
        AccountScopedRouteApi {
            requests: Arc::clone(&same_account_requests),
            separate_fallback_account: false,
        },
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000)
    .with_provider_admission(Arc::clone(&manager));
    same_account.set_active_model("primary");
    *same_account.fallbacks.write().unwrap() = vec!["fallback".to_string()];
    same_account
        .begin_turn_strategy("account-failure-same", "answer")
        .unwrap();
    let error = same_account
        .execute_model_step("answer", true)
        .await
        .expect_err("same account must be exhausted after one provider request");
    assert_eq!(
        error.provider_failure_scope(),
        model_protocol::provider_failure::ProviderFailureScope::Account,
        "{error}"
    );
    assert_eq!(
        same_account_requests.lock().unwrap().as_slice(),
        &["primary".to_string()]
    );
    let capacity = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
    assert_eq!(
        capacity.sample_count, 0,
        "balance is not congestion or a success sample"
    );
    assert_eq!(capacity.active_leases, 0);
    assert_eq!(capacity.effective_limit, 8);

    let independent_requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut independent = ConversationRuntime::new(
        Session::new(),
        AccountScopedRouteApi {
            requests: Arc::clone(&independent_requests),
            separate_fallback_account: true,
        },
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000)
    .with_provider_admission(Arc::clone(&manager));
    independent.set_active_model("primary");
    *independent.fallbacks.write().unwrap() = vec!["fallback".to_string()];
    independent
        .begin_turn_strategy("account-failure-independent", "answer")
        .unwrap();
    let result = independent
        .execute_model_step("answer", true)
        .await
        .expect("a separately configured provider account remains a valid fallback");
    assert!(matches!(
        result.intent,
        ModelStepIntent::FinalAnswer { ref text } if text == "independent account fallback"
    ));
    assert_eq!(
        independent_requests.lock().unwrap().as_slice(),
        &["primary".to_string(), "fallback".to_string()]
    );
    let capacity = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
    assert_eq!(
        capacity.sample_count, 1,
        "only the successful independent request is a capacity sample"
    );
    assert_eq!(capacity.failure_rate_basis_points, Some(0));
    assert_eq!(capacity.active_leases, 0);
}

#[tokio::test]
async fn capacity_feedback_excludes_route_failures_but_retains_real_pressure() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ResourceResultClass,
    };
    use model_protocol::provider_failure::ProviderFailureScope;
    let manager = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Provider,
        ResourceQuota::new(1, 8, 16).unwrap(),
    )]));
    let runtime = ConversationRuntime::new(
        Session::new(),
        AccountScopedRouteApi {
            requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            separate_fallback_account: false,
        },
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_provider_admission(Arc::clone(&manager));
    let lease = manager
        .acquire(ExecutionResourceKind::Provider, None)
        .await
        .unwrap();
    for scope in [
        ProviderFailureScope::Account,
        ProviderFailureScope::Configuration,
    ] {
        runtime.record_provider_resource_outcome(
            Some(&lease),
            Duration::ZERO,
            Duration::from_millis(10),
            ResourceResultClass::Failed,
            Some(scope),
        );
    }
    assert_eq!(
        manager
            .snapshot(&ExecutionResourceKind::Provider)
            .unwrap()
            .sample_count,
        0
    );
    for (scope, class) in [
        (ProviderFailureScope::Request, ResourceResultClass::Failed),
        (
            ProviderFailureScope::Account,
            ResourceResultClass::DownstreamOverload,
        ),
        (
            ProviderFailureScope::Configuration,
            ResourceResultClass::TimedOut,
        ),
    ] {
        runtime.record_provider_resource_outcome(
            Some(&lease),
            Duration::ZERO,
            Duration::from_millis(10),
            class,
            Some(scope),
        );
    }
    drop(lease);
    let capacity = manager.snapshot(&ExecutionResourceKind::Provider).unwrap();
    assert_eq!(capacity.sample_count, 3);
    assert!(capacity.failure_rate_basis_points.unwrap() > 0);
    assert!(capacity.overload_rate_basis_points.unwrap() > 0);
    assert!(capacity.timeout_rate_basis_points.unwrap() > 0);
    assert_eq!(capacity.active_leases, 0);
}

#[tokio::test]
async fn account_circuit_survives_multiple_model_nodes_in_the_same_turn() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        TurnAccountCircuitApi {
            requests: Arc::clone(&requests),
            fallback_attempts: Arc::new(AtomicUsize::new(0)),
        },
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("primary");
    *runtime.fallbacks.write().unwrap() = vec!["fallback".to_string()];
    runtime.begin_turn_runtime_epoch().await;
    runtime
        .begin_turn_strategy("account-circuit-turn", "answer")
        .unwrap();

    runtime
        .execute_model_step("answer", true)
        .await
        .expect_err("independent fallback fails its first request");
    let result = runtime
        .execute_model_step("answer", false)
        .await
        .expect("second model node uses only the still-available account");
    assert!(matches!(
        result.intent,
        ModelStepIntent::FinalAnswer { ref text } if text == "fallback recovered"
    ));
    assert_eq!(
        requests.lock().unwrap().as_slice(),
        &[
            "primary".to_string(),
            "fallback".to_string(),
            "fallback".to_string()
        ]
    );
}

#[tokio::test]
async fn runtime_retries_one_typed_transient_failure_without_hidden_wire_retries() {
    #[derive(Clone)]
    struct RetryOnceApi(Arc<AtomicUsize>);

    impl ApiClient for RetryOnceApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let attempt = self.0.fetch_add(1, Ordering::SeqCst);
            let events = if attempt == 0 {
                vec![Err(
                    RuntimeError::with_provider_failure_metadata_and_retry_after(
                        "temporary provider timeout",
                        None,
                        false,
                        crate::execution_core::graph::ResourceResultClass::TimedOut,
                        Some(Duration::from_millis(1)),
                        true,
                    ),
                )]
            } else {
                vec![
                    Ok(AssistantEvent::TextDelta(
                        "recovered after governed retry".to_string(),
                    )),
                    Ok(AssistantEvent::MessageStop),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }

    let attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        RetryOnceApi(Arc::clone(&attempts)),
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime
        .begin_turn_strategy("test-provider-retry", "return a verified answer")
        .unwrap();

    let result = runtime
        .execute_model_step("return a verified answer", true)
        .await
        .expect("one governed retry should recover");
    assert!(matches!(
        result.intent,
        ModelStepIntent::FinalAnswer { ref text }
            if text == "recovered after governed retry"
    ));
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn one_shot_reasoning_effort_is_request_local_and_survives_fallback() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = RouteRecordingApi {
        requests: Arc::clone(&requests),
    };
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        api,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        SystemPromptBuilder::new().build(),
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("primary");
    *runtime.fallbacks.write().unwrap() = vec!["fallback".to_string()];
    runtime
        .begin_turn_strategy("test-reasoning-turn", "reduce verified receipts")
        .expect("test turn strategy admission");
    runtime.require_next_model_reasoning_effort("none");

    runtime
        .execute_model_step("reduce verified receipts", true)
        .await
        .expect("fallback candidate should complete");

    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert!(requests
        .iter()
        .all(|request| { request.reasoning_effort_override.as_deref() == Some("none") }));
    assert!(runtime
        .next_model_reasoning_effort
        .lock()
        .expect("reasoning effort")
        .is_none());
}

struct ContextBudgetFixtureApi;

impl ApiClient for ContextBudgetFixtureApi {
    fn stream(
        &mut self,
        _: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        Box::pin(futures::stream::empty())
    }
}

#[tokio::test]
async fn rendered_capacity_refusal_precedes_reservation_and_stream_dispatch() {
    struct RefusingBudgetApi(Arc<AtomicUsize>);
    impl ApiClient for RefusingBudgetApi {
        fn prepare_request_budget<'a>(
            &'a self,
            _: &'a mut ApiRequest,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<(), RuntimeError>> + Send + 'a>>
        {
            Box::pin(async {
                Err(RuntimeError::context_capacity(
                    crate::context_ledger::ContextCapacityFailure::RenderOverflow,
                    "fixture full wire does not fit",
                ))
            })
        }
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::empty())
        }
    }
    let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
    let guard = registry
        .install("precharge-session", "precharge-fixture", 1_000_000)
        .unwrap();
    let lease = guard.lease();
    let before = lease.snapshot().unwrap();
    let dispatches = Arc::new(AtomicUsize::new(0));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        RefusingBudgetApi(dispatches.clone()),
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["system".into()],
    )
    .without_memory()
    .with_model_context_window(128_000)
    .with_evaluation_provider_token_lease(lease.clone());
    runtime.set_active_model("private-fixture");
    runtime
        .begin_turn_strategy("precharge-turn", "concise answer")
        .unwrap();
    let error = runtime
        .execute_model_step("concise answer", true)
        .await
        .unwrap_err();
    assert_eq!(
        error.context_capacity_failure(),
        Some(crate::context_ledger::ContextCapacityFailure::RenderOverflow)
    );
    assert_eq!(dispatches.load(Ordering::SeqCst), 0);
    assert_eq!(lease.snapshot().unwrap(), before);
}

#[tokio::test]
async fn required_tool_choice_is_reapplied_to_a_calibrated_semantic_attempt() {
    struct ChoiceApi {
        pending: (bool, Option<String>),
        requests: Arc<std::sync::Mutex<Vec<(bool, Option<String>)>>>,
    }
    impl ApiClient for ChoiceApi {
        fn step_tool_definitions(&self) -> Option<Vec<crate::ProviderToolDefinition>> {
            Some(vec![crate::ProviderToolDefinition {
                name: "read_file".into(),
                description: None,
                input_schema: serde_json::json!({"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}),
            }])
        }
        fn configure_tool_choice(&mut self, required: bool, name: Option<String>) {
            self.pending = (required, name);
        }
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let mut requests = self.requests.lock().unwrap();
            requests.push(std::mem::take(&mut self.pending));
            let first = requests.len() == 1;
            drop(requests);
            let events = if first {
                vec![Err(RuntimeError::with_provider_context_window_limit(
                    "fixture limit 32768",
                    Some(32_768),
                ))]
            } else {
                vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "read-after-calibration".into(),
                        name: "read_file".into(),
                        input: r#"{"path":"source.txt"}"#.into(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]
            };
            Box::pin(futures::stream::iter(events))
        }
    }
    struct ChoiceTools(harness_contract::tool::ToolDiscoveryReceipt);
    #[async_trait::async_trait]
    impl ToolExecutor for ChoiceTools {
        fn available_tool_names(&self) -> Vec<String> {
            vec!["read_file".into()]
        }
        fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
            self.0.clone()
        }
        async fn execute_output(
            &self,
            _: &str,
            _: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            panic!("calibration returns a call; it must not dispatch a tool")
        }
    }
    let workspace = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("calibration-choice", workspace.path());
    let mut discovery = host.pin_snapshot().catalog_receipt();
    discovery
        .descriptors
        .retain(|definition| definition.canonical_id == "read_file");
    discovery
        .activation_candidates
        .retain(|name| name == "read_file");
    assert_eq!(discovery.descriptors.len(), 1);
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        ChoiceApi {
            pending: (false, None),
            requests: requests.clone(),
        },
        ChoiceTools(discovery),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["system".into()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("private-fixture");
    runtime
        .begin_turn_strategy("choice-turn", "read the source")
        .unwrap();
    runtime.require_next_model_tool_action(["read_file".to_owned()]);
    runtime
        .execute_model_step("read the source", true)
        .await
        .unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests.iter().all(|(required, _)| *required),
        "{requests:?}"
    );
}

#[test]
fn stale_context_policy_is_refused_instead_of_relabelled_as_current() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        ContextBudgetFixtureApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["system".into()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    let mut prompt = PromptAssembly::new(vec!["private context".into()]);
    prompt.context_policy_revision = Some(
        runtime
            .permission_policy
            .execution_policy_control()
            .snapshot()
            .revision
            .saturating_add(1),
    );
    let error = runtime
        .pack_provider_attempt(
            &prompt,
            &Vec::new().into(),
            "fixture",
            ProviderContextInventory::default(),
        )
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("context_policy_changed_during_compilation"));
    let required = PromptAssembly::new(vec!["must preserve this requirement ".repeat(30_000)]);
    let error = runtime
        .pack_provider_attempt(
            &required,
            &Vec::new().into(),
            "fixture",
            ProviderContextInventory::default(),
        )
        .unwrap_err();
    assert_eq!(
        error.context_capacity_failure(),
        Some(crate::context_ledger::ContextCapacityFailure::RequiredOverflow)
    );
}

#[tokio::test]
async fn a_second_provider_overflow_exits_after_one_calibrated_recompile() {
    struct OverflowApi(Arc<AtomicUsize>);
    impl ApiClient for OverflowApi {
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let first = self.0.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(futures::stream::iter(vec![Err(
                RuntimeError::with_provider_context_window_limit(
                    "fixture still over context",
                    Some(if first { 32_768 } else { 16_384 }),
                ),
            )]))
        }
    }
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        OverflowApi(attempts.clone()),
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["system".into()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("private-fixture");
    runtime
        .begin_turn_strategy("overflow-turn", "concise answer")
        .unwrap();
    assert!(runtime
        .execute_model_step("concise answer", true)
        .await
        .is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn selected_navigation_checks_durable_source_before_injecting_any_summary() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let store = Arc::new(crate::test_support::session_store());
    let session_id = "source-bound-navigation";
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.into(),
            platform: "test".into(),
            chat_id: "navigation-fixture".into(),
            user_id: None,
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
        .await
        .unwrap();
    store
        .insert_message(&session::SessionMessage {
            stable_message_id: "immutable-deployment-result".into(),
            session_id: session_id.into(),
            sequence: 0,
            role: "user".into(),
            content_json: r#"[{"type":"text","text":"deployment failed, retain the obligation"}]"#
                .into(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
            created_at_ms: 1,
        })
        .await
        .unwrap();
    store
        .reconcile_session_context_index(session_id, 16, 4, 1)
        .await
        .unwrap();
    let page = store
        .page_in_session_context(session_id, 512)
        .await
        .unwrap()
        .unwrap();
    let hot = Arc::new(crate::execution_core::hot_state::RuntimeHotStatePlane::new(
        Default::default(),
    ));
    for corruption in ["none", "summary", "source"] {
        hot.sessions().update(session_id, |snapshot| {
            snapshot.context_manifest = Some(page.manifest.clone());
            snapshot.context_cards = page.context_cards.clone();
            if corruption == "summary" {
                snapshot.context_cards[0].summary =
                    "deployment accepted every write without evidence".into();
            }
            if corruption == "source" {
                snapshot.context_cards[0].source_digest = "corrupt-digest".into();
            }
        });
        let mut session = Session::new();
        session.session_id = session_id.into();
        let runtime = ConversationRuntime::new(
            session,
            ContextBudgetFixtureApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::ReadOnly),
            vec!["system".into()],
        )
        .without_memory()
        .with_hot_state(hot.clone())
        .with_session_history_reader(Arc::new(store.history_reader()));
        let items = runtime.runtime_session_context_items("deployment").await;
        if corruption == "none" {
            assert!(!items.is_empty());
            assert!(items
                .iter()
                .any(|item| item.content.contains("deployment failed")));
        } else {
            assert!(
                items.is_empty(),
                "corrupt {corruption} must not enter even an orientation packet: {items:?}"
            );
        }
    }
}

#[derive(Clone)]
struct CalibrationRecordingApi {
    requests: Arc<std::sync::Mutex<Vec<CalibrationRequestRecord>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CalibrationRequestRecord {
    context_window_tokens: u64,
    context_window_source: String,
    reasoning_effort_override: Option<String>,
}

impl ApiClient for CalibrationRecordingApi {
    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        let attempt = {
            let mut requests = self.requests.lock().expect("requests");
            requests.push(CalibrationRequestRecord {
                context_window_tokens: request.budget.context_window_tokens,
                context_window_source: request.budget.context_window_source.clone(),
                reasoning_effort_override: request.reasoning_effort_override.clone(),
            });
            requests.len()
        };
        let events = if attempt == 1 {
            vec![Err(RuntimeError::with_provider_context_window_limit(
                "provider maximum context length is 32768 tokens",
                Some(32_768),
            ))]
        } else {
            vec![
                Ok(AssistantEvent::TextDelta("calibrated answer".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]
        };
        Box::pin(futures::stream::iter(events))
    }
}

#[tokio::test]
async fn explicit_provider_limit_calibrates_once_and_repackages_the_same_model() {
    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = CalibrationRecordingApi {
        requests: Arc::clone(&requests),
    };
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        api,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["builtin policy".to_string()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime.set_active_model("private-model");
    runtime
        .begin_turn_strategy("test-calibration-turn", "give a concise answer")
        .expect("test turn strategy admission");

    let result = runtime
        .execute_model_step("give a concise answer", true)
        .await
        .expect("calibrated retry should complete");
    assert_eq!(result.model.as_deref(), Some("private-model"));
    let requests = requests.lock().expect("requests");
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].context_window_tokens, 128_000);
    assert_eq!(requests[1].context_window_tokens, 32_768);
    assert_eq!(requests[1].context_window_source, "calibrated");
    assert_eq!(requests[1].reasoning_effort_override, None);
}

#[tokio::test]
async fn clean_terminal_does_not_retry_after_its_provider_attempt() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceAdmissionObservationStatus,
        ResourceQuota,
    };

    let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
    let api = CalibrationRecordingApi {
        requests: Arc::clone(&requests),
    };
    let granted = Arc::new(AtomicUsize::new(0));
    let manager = Arc::new(ExecutionResourceManager::new([(
        ExecutionResourceKind::Provider,
        ResourceQuota::new(1, 1, 1).unwrap(),
    )]));
    let observed_grants = Arc::clone(&granted);
    manager
        .install_admission_observer(move |observation| {
            if observation.status == ResourceAdmissionObservationStatus::Granted {
                observed_grants.fetch_add(1, Ordering::SeqCst);
            }
        })
        .unwrap();
    let bus = CowdEventBus::new();
    let _scope = bus.enter_execution(crate::CowdExecutionContext {
        execution_id: "clean-terminal-execution".to_string(),
        session_id: "clean-terminal-session".to_string(),
        turn_id: "clean-terminal-turn".to_string(),
    });
    let mut receiver = bus.subscribe();
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        api,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["builtin policy".to_string()],
    )
    .without_memory()
    .with_cowd_event_bus(bus)
    .with_provider_admission(manager)
    .with_model_context_window(128_000);
    runtime.set_active_model("private-model");

    let error = runtime
        .execute_clean_terminal_synthesis("give a concise answer", "checked evidence", Vec::new())
        .await
        .expect_err("clean terminal must not retry after its provider attempt");
    assert!(error.to_string().contains("maximum context length"));
    let requests = requests.lock().expect("requests");
    assert_eq!(
        requests.as_slice(),
        &[CalibrationRequestRecord {
            context_window_tokens: 128_000,
            context_window_source: "assumed".to_string(),
            reasoning_effort_override: Some("none".to_string()),
        }]
    );
    let mut live_events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        live_events.push(serde_json::to_string(&event).expect("serialize live event"));
    }
    let live_events = live_events.join("\n");
    assert!(!live_events.contains("calibrated answer"));
    assert_eq!(
        granted.load(Ordering::SeqCst),
        1,
        "clean terminal must use exactly one canonical provider admission"
    );
}

#[test]
fn explicit_max_output_override_reaches_provider_budget_policy() {
    assert_eq!(
        super::provider_output_budget_hint("deepseek-v4-pro", 1_000_000, Some(12_000)),
        12_000
    );
}

#[test]
fn switching_models_re_resolves_each_configured_context_window() {
    let mut runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["builtin policy".to_string()],
    )
    .without_memory()
    .with_model_context_window(128_000);
    runtime
        .model_context_windows
        .insert("small-configured-model".to_string(), 32_768);

    runtime.set_active_model("small-configured-model");

    let resolution = runtime.context_window_resolution_for_model("small-configured-model");
    assert_eq!(resolution.tokens, 32_768);
    assert_eq!(resolution.source.as_str(), "configured");
}

#[test]
fn semantic_checkpoint_id_is_stable_and_boundary_specific() {
    let first = deterministic_checkpoint_id("session-a", 2, 8, Some("prior"));
    let retry = deterministic_checkpoint_id("session-a", 2, 8, Some("prior"));
    let different_boundary = deterministic_checkpoint_id("session-a", 3, 8, Some("prior"));

    assert_eq!(first, retry);
    assert_ne!(first, different_boundary);
    assert!(first.starts_with("checkpoint-session-a-"));
}

#[tokio::test]
async fn manual_compaction_uses_one_semantic_checkpoint_and_preserves_recent_turns() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let tmp = tempfile::tempdir().expect("temp memory root");
    let manager = Arc::new(
        CognitiveContextManager::new_ephemeral(memory::config::MemoryConfig {
            store: memory::config::StoreConfig {
                blob_dir: tmp.path().join("blobs"),
                enable_vector_index: false,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .expect("memory manager"),
    );
    let mut session = Session::new();
    session.replace_messages(vec![
        ConversationMessage::user_text("old request ".repeat(200)),
        ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "old response ".repeat(200),
        }]),
        ConversationMessage::user_text("recent user request"),
        ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "recent assistant response".to_string(),
        }]),
    ]);
    let store = Arc::new(crate::test_support::session_store());
    store
        .create_session(&session::SessionRecord {
            session_id: session.session_id.clone(),
            platform: "test".to_string(),
            chat_id: "semantic-compaction".to_string(),
            user_id: None,
            model: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: "2026-01-01T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("session record");
    let original_bytes = crate::JsonValue::Array(
        session
            .messages()
            .map(|m| m.to_persisted_json().unwrap())
            .collect(),
    )
    .render();
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
        tmp.path().join("artifacts"),
    ));
    let session_id = session.session_id.clone();
    let mut runtime = ConversationRuntime::new(
        session,
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_memory_manager(Arc::clone(&manager))
    .with_artifact_store(Arc::clone(&artifacts))
    .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
        Arc::clone(&store),
    ));
    runtime.session_compaction_config.preserve_recent = 2;

    let receipt = runtime
        .compact_active_session()
        .await
        .expect("semantic compaction")
        .expect("a compaction receipt");
    assert!(receipt.removed_message_count > 0);
    let compacted = runtime.session_snapshot().await;
    assert_eq!(
        compacted.message_count(),
        3,
        "configured preserve_recent=2 must win"
    );
    assert!(matches!(
        &compacted.message(0).expect("summary").blocks[0],
        ContentBlock::Text { text }
            if text.contains("Compressed Session Summary")
                && !text.contains("Conversation summary:")
    ));
    assert!(compacted.messages().any(|message| {
        message.blocks.iter().any(|block| {
            matches!(
                block,
                ContentBlock::Text { text } if text == "recent user request"
            )
        })
    }));

    // Reconstruct the Runtime from its persisted-window representation between
    // each compression. No original raw message is carried in this new runtime.
    let mut last_summary = compacted.compaction.as_ref().unwrap().summary.clone();
    for round in 1..3 {
        let mut recovered = runtime.session_snapshot().await;
        recovered
            .push_message(ConversationMessage::user_text(format!(
                "next request {round} {}",
                "detail ".repeat(200)
            )))
            .unwrap();
        recovered
            .push_message(ConversationMessage::assistant(vec![ContentBlock::Text {
                text: format!("next response {round} {}", "analysis ".repeat(200)),
            }]))
            .unwrap();
        runtime = ConversationRuntime::new(
            recovered,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".into()],
        )
        .without_memory()
        .with_memory_manager(Arc::clone(&manager))
        .with_artifact_store(Arc::clone(&artifacts))
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        runtime.session_compaction_config.preserve_recent = 2;
        runtime.compact_active_session().await.unwrap().unwrap();
        last_summary = runtime.session_snapshot().await.compaction.unwrap().summary;
    }
    let port = crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store));
    let mut raw_windows = Vec::new();
    // Follow the exposed read locators through the actual durable receipt
    // resolver and ArtifactStore, validating every byte hash across generations.
    for _ in 0..3 {
        let recovery = last_summary
            .lines()
            .rev()
            .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
            .find(|value| value["name"] == "evidence_retrieve")
            .expect("formatted summary retains an exact recovery request");
        let logical = recovery["input"]["evidence_ref"]
            .as_str()
            .unwrap()
            .strip_prefix("tool://")
            .unwrap();
        let access =
            crate::SessionRuntimeQueryPort::evidence_access(port.as_ref(), &session_id, logical)
                .await
                .unwrap()
                .unwrap();
        let artifact = artifacts.resolve(&access.retrieval_selector).unwrap();
        let bytes = artifacts
            .read(&artifact, &format!("session:{session_id}"), None)
            .await
            .unwrap();
        assert_eq!(
            format!("sha256:{:x}", Sha256::digest(&bytes)),
            access.sha256
        );
        assert_eq!(recovery["sha256"], access.sha256);
        assert!(artifacts
            .read(&artifact, "session:foreign", None)
            .await
            .is_err());
        let raw = String::from_utf8(bytes).unwrap();
        let records: serde_json::Value = serde_json::from_str(&raw).unwrap();
        raw_windows.push(raw);
        let prior = records
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|record| record["blocks"].as_array().unwrap())
            .filter_map(|block| block["text"].as_str())
            .find(|text| text.contains("Raw working-window recovery (read-only;"));
        if let Some(prior) = prior {
            last_summary = prior.to_string();
        }
    }
    assert_eq!(raw_windows.len(), 3);
    assert_eq!(
        raw_windows.last().unwrap(),
        &original_bytes,
        "initial raw window remains byte-exact after three compressions"
    );
    // A missing raw owner must not fall back to positional references.
    runtime.artifact_store = None;
    runtime
        .session
        .write()
        .await
        .push_message(ConversationMessage::user_text(
            "one more request".repeat(200),
        ))
        .unwrap();
    let before = runtime.session_snapshot().await;
    assert!(runtime
        .compact_active_session()
        .await
        .unwrap_err()
        .to_string()
        .contains("Artifact store"));
    assert_eq!(
        runtime.session_snapshot().await.materialize_messages(),
        before.materialize_messages()
    );
}

#[tokio::test]
async fn compaction_without_durable_session_store_retains_the_transcript() {
    let mut session = Session::new();
    session.replace_messages(vec![
        ConversationMessage::user_text("old request"),
        ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "old response".to_string(),
        }]),
        ConversationMessage::user_text("recent request"),
        ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "recent response".to_string(),
        }]),
    ]);
    let before = session.clone();
    let mut runtime = ConversationRuntime::new(
        session,
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();

    let error = runtime
        .compact_active_session()
        .await
        .expect_err("a non-durable runtime must not compact history");

    assert!(error.to_string().contains("durable Session journal port"));
    assert_eq!(runtime.session_snapshot().await, before);
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_session_snapshot_works_from_current_thread_runtime_when_contended() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    let expected_session_id = runtime.session_id().to_string();
    let lock = Arc::clone(&runtime.session);
    let (locked_tx, locked_rx) = std::sync::mpsc::sync_channel(1);
    let holder = std::thread::spawn(move || {
        let _guard = lock.blocking_write();
        let _ = locked_tx.send(());
        std::thread::sleep(std::time::Duration::from_millis(20));
    });
    locked_rx
        .recv()
        .expect("native holder must acquire the session lock before the compatibility read");

    let session = runtime.session_snapshot_blocking();

    holder
        .join()
        .expect("native session-lock holder must finish");
    assert_eq!(session.session_id, expected_session_id);
}

#[tokio::test]
async fn session_head_reads_metadata_without_materializing_a_snapshot() {
    let mut session = Session::new();
    session
        .push_user_text("one")
        .expect("append initial session message");
    let expected_session_id = session.session_id.clone();
    let runtime = ConversationRuntime::new(
        session,
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();

    let head = runtime.session_head().await;

    assert_eq!(runtime.session_id(), expected_session_id);
    assert_eq!(head.message_count, 1);
    assert_eq!(head.history_revision, 1);
    assert!(head.history_bytes > 0);
    assert!(head.history_tokens > 0);
}

#[test]
fn runtime_decision_keeps_all_six_patterns_stable_for_same_input() {
    use harness_contract::core::ExecutionPattern;

    let cases = [
        ("解释一下这个函数有什么用", ExecutionPattern::Direct),
        (
            "调研最新 AI harness 实践并汇总证据",
            ExecutionPattern::Explore,
        ),
        ("实现并修复这个单文件小问题", ExecutionPattern::Execute),
        (
            "权衡两个架构方案并解决冲突方案",
            ExecutionPattern::Deliberate,
        ),
        (
            "使用多 Agent 协同完成复杂架构分析",
            ExecutionPattern::Collaborate,
        ),
        ("后台持续监控这项长期运行任务", ExecutionPattern::Supervise),
    ];

    for (prompt, expected_pattern) in cases {
        let first = crate::execution_core::build_runtime_execution_decision(prompt, None);
        let second = crate::execution_core::build_runtime_execution_decision(prompt, None);
        let wire = serde_json::to_value(&first).expect("runtime decision wire payload");

        assert_eq!(first.pattern(), expected_pattern, "prompt: {prompt}");
        assert_eq!(first.strategy, second.strategy, "prompt: {prompt}");
        assert_eq!(
            first.lease.input_fingerprint, second.lease.input_fingerprint,
            "prompt: {prompt}"
        );
        assert_eq!(first.lease.locked_pattern, expected_pattern);
        assert_eq!(wire["strategy"]["pattern"], expected_pattern.as_str());
    }
}

#[test]
fn strategy_selected_event_failure_restores_unbound_owner_and_blocks_execution() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory();
    runtime
        .begin_turn_strategy("durability-turn", "explain one function")
        .expect("strategy admission");

    let error = runtime
        .bind_turn_strategy_execution("durability-turn", "graph-without-store")
        .expect_err("selected event must be durable before graph execution");
    assert!(error.to_string().contains("event store is unavailable"));
    assert_eq!(
        runtime
            .active_turn_strategy()
            .and_then(|state| state.execution_graph_ref),
        None
    );
}

#[test]
fn admitted_turn_has_one_strategy_identity_through_terminal_outcome() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    let first = runtime
        .begin_turn_strategy("turn-one", "explain this function")
        .expect("admit strategy");
    let replay = runtime
        .begin_turn_strategy("turn-one", "different wording cannot replace identity")
        .expect("same turn reuses strategy");
    assert_eq!(first.decision_id, replay.decision_id);
    assert_eq!(first.decision_lease, replay.decision_lease);

    let bound = runtime
        .bind_turn_strategy_execution("turn-one", "graph-one")
        .expect("bind graph");
    assert_eq!(bound.decision_id, first.decision_id);
    assert_eq!(bound.execution_graph_ref.as_deref(), Some("graph-one"));
    runtime
        .retarget_active_turn_strategy_for_tool_requirements(
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::core::ExecutionPattern::Execute,
            false,
            false,
            false,
            false,
            "provider tool batch retained the admitted decision lease",
        )
        .expect("running ToolBatch retarget is a selected revision");
    runtime
        .finish_turn_strategy(
            "turn-one",
            crate::execution_core::TurnStrategyDecisionStatus::Completed,
            crate::execution_core::TurnStrategyActualOutcome {
                duration_ms: 10,
                terminal_reason: "satisfied".to_string(),
                ..Default::default()
            },
        )
        .expect("finish strategy");
    assert!(runtime.active_turn_strategy().is_none());

    let events = store
        .list_stream(&format!("session:{}", runtime.session_id()))
        .expect("strategy events");
    let strategy_events = events
        .iter()
        .filter(|event| {
            matches!(
                event.kind.as_str(),
                "runtime.strategy.selected"
                    | "runtime.strategy.downgraded"
                    | "runtime.strategy.early_stopped"
                    | "runtime.strategy.outcome"
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        strategy_events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "runtime.strategy.selected",
            "runtime.strategy.selected",
            "runtime.strategy.outcome"
        ]
    );
    assert_eq!(strategy_events[1].status.as_deref(), Some("running"));
    assert_eq!(
        strategy_events[1].payload["selected_pattern"].as_str(),
        Some("execute")
    );
    assert!(turn_strategy_event_kind_allowed(
        "runtime.strategy.selected"
    ));
    assert!(!turn_strategy_event_kind_allowed(
        "runtime.strategy.retargeted"
    ));
    assert!(strategy_events.iter().all(|event| {
        event.scope == RuntimeEventScope::Session
            && event.payload["decision_id"].as_str() == Some(first.decision_id.as_str())
            && event.payload["decision_lease"].as_str() == Some(first.decision_lease.as_str())
            && event.payload["execution_graph_ref"].as_str() == Some("graph-one")
            && event.payload["session_ref"].as_str() == Some(runtime.session_id())
            && event.payload["turn_ref"].as_str() == Some("turn-one")
    }));
}

#[test]
fn tool_requirements_retarget_the_canonical_strategy_state_atomically() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    runtime
        .begin_turn_strategy("turn-network", "继续处理")
        .expect("admit strategy");
    runtime
        .bind_turn_strategy_execution("turn-network", "graph-network")
        .expect("bind graph");

    let decision = runtime
        .retarget_active_turn_strategy_for_tool_requirements(
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
            harness_contract::core::ExecutionPattern::Explore,
            true,
            false,
            true,
            false,
            "provider emitted parallel external research calls",
        )
        .expect("retarget network batch");
    let canonical = runtime
        .active_turn_strategy()
        .expect("canonical strategy remains active");

    assert_eq!(decision, canonical.decision);
    assert_eq!(
        decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::EvidenceGraph
    );
    assert!(decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
    assert!(decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::Parallel));
    assert_eq!(
        canonical.selected_candidate,
        harness_contract::strategy::ExecutionCandidateKind::ParallelTools
    );
}

#[test]
fn model_team_proposal_retargets_within_the_same_strategy_lease() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    let admitted = runtime
        .begin_turn_strategy("turn-team", "分析并解决这个问题")
        .expect("admit strategy");
    runtime
        .bind_turn_strategy_execution("turn-team", "graph-team")
        .expect("bind graph");

    let decision = runtime
        .retarget_active_turn_strategy_for_tool_requirements(
            harness_contract::strategy::ExecutionCandidateKind::Team,
            harness_contract::core::ExecutionPattern::Collaborate,
            false,
            false,
            true,
            false,
            "model proposed a Team after inspecting the task",
        )
        .expect("retarget to model-proposed Team");

    assert_eq!(decision.lease.lease_id, admitted.decision_lease);
    assert!(decision.decision_revision > 1);
    assert_eq!(
        decision.strategy.pattern,
        harness_contract::core::ExecutionPattern::Collaborate
    );
    assert_eq!(
        runtime
            .active_turn_strategy()
            .expect("canonical strategy")
            .selected_candidate,
        harness_contract::strategy::ExecutionCandidateKind::Team
    );
}

#[test]
fn evidence_strategy_revises_to_explicitly_approved_delivery_without_changing_lease() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    let admitted = runtime
        .begin_turn_strategy("turn-research-delivery", "调研外部资料并形成报告")
        .expect("admit strategy");
    runtime
        .bind_turn_strategy_execution("turn-research-delivery", "graph-research-delivery")
        .expect("bind graph");

    let decision = runtime
        .retarget_active_turn_strategy_for_tool_requirements(
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::core::ExecutionPattern::Execute,
            true,
            true,
            false,
            true,
            "research evidence is being delivered to the workspace",
        )
        .expect("retarget approved delivery");

    assert_eq!(decision.lease.lease_id, admitted.decision_lease);
    assert!(decision.decision_revision > 1);
    assert_eq!(
        decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::ExecutionGraph
    );
    assert!(decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
    assert!(decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Approval));
    assert!(decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithGuardrails));
    assert!(decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
}

#[test]
fn governed_plan_retargets_one_strategy_from_research_to_approved_write() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        StaticToolExecutor::new()
            .register("web_search", |_| Ok("external evidence".to_string()))
            .register("edit", |_| Ok("written".to_string())),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    runtime
        .begin_turn_strategy("turn-research-write", "调研外部资料并形成报告")
        .expect("admit strategy");
    runtime
        .bind_turn_strategy_execution("turn-research-write", "graph-research-write")
        .expect("bind graph");

    let compile = |call: &ModelToolCall| {
        let request = crate::tool_dispatch::ToolRequest {
            tool_use_id: call.id.clone(),
            tool_name: call.name.clone(),
            input: call.input.clone(),
            depends_on: Vec::new(),
        };
        let prepared = runtime
            .tool_executor()
            .prepare_governed_invocations(std::slice::from_ref(&request));
        crate::GovernedToolCompiler
            .compile(
                &std::env::current_dir().expect("workspace"),
                std::slice::from_ref(&request),
                |name, input| {
                    prepared.iter().find_map(|invocation| {
                        (invocation.intent.tool_name == name
                            && invocation.intent.normalized_input == *input)
                            .then(|| {
                                (
                                    invocation.effect.clone(),
                                    invocation.catalog_revision,
                                    invocation.descriptor_set_hash.clone(),
                                )
                            })
                    })
                },
            )
            .expect("governed plan")
    };
    let search = ModelToolCall {
        binding: None,
        id: "search".to_string(),
        name: "web_search".to_string(),
        input: r#"{"query":"tokio cancellation token"}"#.to_string(),
        depends_on: Vec::new(),
    };
    let search_decision = runtime
        .retarget_active_turn_strategy_for_governed_plan(
            &compile(&search),
            std::slice::from_ref(&search),
        )
        .expect("research plan retarget");
    assert_eq!(
        search_decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::EvidenceGraph
    );

    let write = ModelToolCall {
        binding: None,
        id: "write".to_string(),
        name: "edit".to_string(),
        input: r#"{"path":"target/report.md","content":"verified"}"#.to_string(),
        depends_on: Vec::new(),
    };
    let write_decision = runtime
        .retarget_active_turn_strategy_for_governed_plan(
            &compile(&write),
            std::slice::from_ref(&write),
        )
        .expect("write plan retarget");

    assert_eq!(
        write_decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::ExecutionGraph
    );
    assert!(write_decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
    assert!(write_decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Approval));
    assert!(write_decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithGuardrails));
    assert!(write_decision.decision_revision > search_decision.decision_revision);
    assert_eq!(
        runtime
            .active_turn_strategy()
            .expect("canonical strategy")
            .decision,
        write_decision
    );
}

#[test]
fn collaboration_strategy_admits_agent_inspection_with_bounded_workspace_work() {
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableStaticExecutor(
            StaticToolExecutor::new()
                .register(harness_contract::agent_action::TEAM_CREATE_TOOL_ID, |_| {
                    Ok("team-created".to_string())
                })
                .register(
                    harness_contract::agent_action::STATE_INSPECT_TOOL_ID,
                    |_| Ok("inspected".to_string()),
                )
                .register("edit", |_| Ok("written".to_string())),
        ),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::new(RuntimeEventStore::for_test()));
    runtime
        .begin_turn_strategy("turn-collaboration-work", "启动两个团队完成实现与复核")
        .expect("admit collaboration strategy");
    runtime
        .bind_turn_strategy_execution("turn-collaboration-work", "graph-collaboration-work")
        .expect("bind graph");

    let calls = vec![
        ModelToolCall {
            binding: None,
            id: "inspect".to_string(),
            name: harness_contract::agent_action::STATE_INSPECT_TOOL_ID.to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "write".to_string(),
            name: "edit".to_string(),
            input: r#"{"path":"target/report.md","content":"verified"}"#.to_string(),
            depends_on: Vec::new(),
        },
    ];
    let requests = calls
        .iter()
        .map(|call| crate::tool_dispatch::ToolRequest {
            tool_use_id: call.id.clone(),
            tool_name: call.name.clone(),
            input: call.input.clone(),
            depends_on: call.depends_on.clone(),
        })
        .collect::<Vec<_>>();
    let prepared = runtime
        .tool_executor()
        .prepare_governed_invocations(&requests);
    let plan = crate::GovernedToolCompiler
        .compile(
            &std::env::current_dir().expect("workspace"),
            &requests,
            |name, input| {
                prepared.iter().find_map(|invocation| {
                    (invocation.intent.tool_name == name
                        && invocation.intent.normalized_input == *input)
                        .then(|| {
                            (
                                invocation.effect.clone(),
                                invocation.catalog_revision,
                                invocation.descriptor_set_hash.clone(),
                            )
                        })
                })
            },
        )
        .expect("governed mixed collaboration plan");
    let decision = runtime
        .retarget_active_turn_strategy_for_governed_plan(&plan, &calls)
        .expect("retarget mixed collaboration work");

    assert_eq!(
        decision.pattern(),
        harness_contract::core::ExecutionPattern::Collaborate
    );
    assert_eq!(
        decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::ExecutionGraph
    );
    assert!(decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
    assert!(decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithGuardrails));
    assert!(plan.validate_against_execution_decision(&decision).allowed);
}

#[test]
fn explicit_team_obligation_survives_a_non_collaboration_setup_batch() {
    // The first root batch is allowed to inspect or perform bounded setup
    // before it emits team_create. Its effect category must enrich the
    // already-admitted Team contract rather than silently choose Direct and
    // make the hard user obligation impossible to fulfill.
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableStaticExecutor(
            StaticToolExecutor::new().register("edit", |_| Ok("written".to_string())),
        ),
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::new(RuntimeEventStore::for_test()));
    runtime
        .begin_turn_strategy("turn-team-setup", "必须启动一个 Team 完成实现与复核")
        .expect("admit explicit Team strategy");
    runtime
        .bind_turn_strategy_execution("turn-team-setup", "graph-team-setup")
        .expect("bind graph");

    let calls = vec![ModelToolCall {
        binding: None,
        id: "setup-write".to_string(),
        name: "edit".to_string(),
        input: r#"{"path":"target/setup.md","content":"prepared"}"#.to_string(),
        depends_on: Vec::new(),
    }];
    let requests = calls
        .iter()
        .map(|call| crate::tool_dispatch::ToolRequest {
            tool_use_id: call.id.clone(),
            tool_name: call.name.clone(),
            input: call.input.clone(),
            depends_on: call.depends_on.clone(),
        })
        .collect::<Vec<_>>();
    let prepared = runtime
        .tool_executor()
        .prepare_governed_invocations(&requests);
    let plan = crate::GovernedToolCompiler
        .compile(
            &std::env::current_dir().expect("workspace"),
            &requests,
            |name, input| {
                prepared.iter().find_map(|invocation| {
                    (invocation.intent.tool_name == name
                        && invocation.intent.normalized_input == *input)
                        .then(|| {
                            (
                                invocation.effect.clone(),
                                invocation.catalog_revision,
                                invocation.descriptor_set_hash.clone(),
                            )
                        })
                })
            },
        )
        .expect("governed setup plan");

    let decision = runtime
        .retarget_active_turn_strategy_for_governed_plan(&plan, &calls)
        .expect("Team contract must admit its setup batch");

    assert_eq!(
        decision.pattern(),
        harness_contract::core::ExecutionPattern::Collaborate
    );
    assert_eq!(
        runtime
            .active_turn_strategy()
            .expect("canonical strategy")
            .selected_candidate,
        harness_contract::strategy::ExecutionCandidateKind::Team
    );
    assert!(decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
    assert!(plan.validate_against_execution_decision(&decision).allowed);
}

#[tokio::test]
async fn parallel_network_tool_batch_is_admitted_by_the_retargeted_strategy_lease() {
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&executions);
    let event_store = Arc::new(RuntimeEventStore::for_test());
    let mut session = Session::new();
    session.session_id = "parallel-network-session".into();
    crate::test_support::register_running_root(
        Arc::clone(&event_store),
        &crate::CowdExecutionContext {
            execution_id: "parallel-network-execution".into(),
            session_id: session.session_id.clone(),
            turn_id: "parallel-network-turn".into(),
        },
    );
    let bus = CowdEventBus::new();
    let _scope = bus.enter_execution(crate::CowdExecutionContext {
        execution_id: "parallel-network-execution".to_string(),
        session_id: "parallel-network-session".to_string(),
        turn_id: "parallel-network-turn".to_string(),
    });
    let mut receiver = bus.subscribe();
    let runtime = ConversationRuntime::new(
        session,
        MockApi,
        StaticToolExecutor::new().register("web_search", move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok("verified external evidence".to_string())
        }),
        PermissionPolicy::new(PermissionMode::DangerFullAccess),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_cowd_event_bus(bus)
    .with_runtime_event_store(event_store);
    runtime
        .begin_turn_strategy("turn-network-batch", "继续")
        .expect("admit direct follow-up");
    let calls = [
        "technical standard official",
        "technical standard maintainers",
    ]
    .into_iter()
    .enumerate()
    .map(|(index, query)| ModelToolCall {
        binding: None,
        id: format!("search-{index}"),
        name: "web_search".to_string(),
        input: serde_json::json!({ "query": query }).to_string(),
        depends_on: Vec::new(),
    })
    .collect::<Vec<_>>();

    let result = runtime
        .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 1)
        .await
        .expect("network batch execution");
    let strategy = runtime
        .active_turn_strategy()
        .expect("canonical strategy remains active");

    assert_eq!(executions.load(Ordering::SeqCst), 2);
    let mut live_events = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        live_events.push(serde_json::to_string(&event).expect("serialize live event"));
    }
    assert_eq!(
        live_events
            .iter()
            .filter(|event| event.contains("\"ToolStart\""))
            .count(),
        2
    );
    assert_eq!(
        live_events
            .iter()
            .filter(|event| event.contains("\"ToolComplete\""))
            .count(),
        2
    );
    assert_eq!(
        live_events
            .iter()
            .filter(|event| event.contains("\"ToolExecuted\""))
            .count(),
        2
    );
    assert!(result.messages.iter().all(|message| {
        message.blocks.iter().all(|block| {
            !matches!(
                block,
                crate::session::ContentBlock::ToolResult { output, .. }
                    if output.contains("network_requires_with_external_research")
                        || output.contains("tool_category_not_allowed_by_compile_target")
            )
        })
    }));
    assert_eq!(
        strategy.decision.compile_target,
        crate::execution_core::RuntimeCompileTarget::EvidenceGraph
    );
    assert!(strategy
        .decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::WithExternalResearch));
    assert!(strategy
        .decision
        .modifiers()
        .contains(&harness_contract::core::ExecutionModifier::Parallel));
}

#[tokio::test]
async fn evidence_index_miss_falls_through_to_the_durable_tool_host() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    let executions = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&executions);
    let session = Session::new();
    let session_store = Arc::new(crate::test_support::session_store());
    session_store
        .create_session(&session::SessionRecord {
            session_id: session.session_id.clone(),
            platform: "test".to_string(),
            chat_id: "durable-evidence-fallback".to_string(),
            user_id: None,
            model: None,
            created_at: "2026-09-04T00:00:00Z".to_string(),
            last_activity: "2026-09-04T00:00:00Z".to_string(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let artifacts = Arc::new(
        crate::ArtifactStore::for_test(temporary.path(), crate::ArtifactStoreConfig::default())
            .unwrap(),
    );
    let runtime = ConversationRuntime::new(
        session,
        MockApi,
        StaticToolExecutor::new().register("evidence_retrieve", move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(r#"{"available":true,"content":"durable evidence"}"#.to_string())
        }),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::new(RuntimeEventStore::for_test()))
    .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
        session_store,
    ))
    .with_artifact_store(artifacts);
    runtime
        .begin_turn_strategy("turn-durable-evidence", "retrieve retained evidence")
        .expect("admit evidence turn");
    let calls = vec![ModelToolCall {
        binding: None,
        id: "retrieve".to_string(),
        name: "evidence_retrieve".to_string(),
        input: r#"{"evidence_ref":"tool://small-output-not-indexed","query":"fact"}"#.to_string(),
        depends_on: Vec::new(),
    }];

    let result = runtime
        .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 1)
        .await
        .expect("durable evidence fallback");

    assert_eq!(result.failed, 0, "messages={:?}", result.messages);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(result.messages.iter().any(|message| {
        message.blocks.iter().any(|block| {
            matches!(
                block,
                crate::session::ContentBlock::ToolResult {
                    output,
                    is_error: false,
                    ..
                } if output.contains("durable evidence")
            )
        })
    }));
}

#[test]
fn canonical_outcome_covers_direct_and_parallel_tool_turns_without_graph_ref() {
    for candidate in [
        harness_contract::strategy::ExecutionCandidateKind::Direct,
        harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
    ] {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        let state = runtime
            .begin_turn_strategy(format!("turn-{candidate:?}"), "give a concise answer")
            .expect("admit strategy");
        runtime
            .retarget_active_turn_strategy_for_tool_requirements(
                candidate,
                harness_contract::core::ExecutionPattern::Execute,
                false,
                false,
                candidate == harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
                false,
                "test binds the canonical execution candidate",
            )
            .expect("retarget");
        runtime
            .finish_turn_strategy(
                &state.turn_ref,
                crate::execution_core::TurnStrategyDecisionStatus::Completed,
                crate::execution_core::TurnStrategyActualOutcome {
                    duration_ms: 10,
                    tool_calls: u64::from(
                        candidate
                            == harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
                    ),
                    terminal_reason: "satisfied".to_string(),
                    ..Default::default()
                },
            )
            .expect("finish");
        let outcomes = store
            .all_events(100)
            .expect("outcomes")
            .into_iter()
            .filter(|event| event.kind == crate::execution_core::OUTCOME_EVENT_KIND)
            .collect::<Vec<_>>();
        assert_eq!(outcomes.len(), 1);
        let outcome: harness_contract::outcome::ExecutionOutcome =
            serde_json::from_value(outcomes[0].payload.clone()).expect("Outcome contract");
        assert_eq!(outcome.strategy.selected_candidate, candidate);
        assert!(outcome.identity.execution_graph_ref.is_none());
    }
}

#[test]
fn canonical_outcome_preserves_failure_cancel_block_and_partial_tool_terminal_classes() {
    // (status, failed tool calls, the Objective's own verdict, expected class)
    let cases = [
        (
            crate::execution_core::TurnStrategyDecisionStatus::Failed,
            0,
            "failed",
            "failed",
        ),
        (
            crate::execution_core::TurnStrategyDecisionStatus::Cancelled,
            0,
            "cancelled",
            "cancelled",
        ),
        (
            crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped,
            0,
            "partial",
            "partial_failure",
        ),
        (
            crate::execution_core::TurnStrategyDecisionStatus::Completed,
            1,
            "partial_failure",
            "partial_failure",
        ),
        // A satisfied Objective is a business success even when tool calls
        // failed and were recovered from. One live run satisfied its Objective,
        // delivered every accepted Task's result and presented the final answer,
        // and was still classified `partial_failure` as
        // "satisfied; 2 tool calls failed before terminal synthesis".
        (
            crate::execution_core::TurnStrategyDecisionStatus::Completed,
            2,
            "satisfied",
            "succeeded",
        ),
    ];
    for (status, failed_tool_calls, objective_verdict, expected_class) in cases {
        let store = Arc::new(RuntimeEventStore::for_test());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        let state = runtime
            .begin_turn_strategy(format!("terminal-{expected_class}"), "test terminal")
            .expect("strategy");
        runtime
            .finish_turn_strategy(
                &state.turn_ref,
                status,
                crate::execution_core::TurnStrategyActualOutcome {
                    failed_tool_calls,
                    terminal_reason: objective_verdict.to_string(),
                    ..Default::default()
                },
            )
            .expect("finish");
        let event = store
            .all_events(10)
            .expect("outcome")
            .into_iter()
            .find(|event| event.kind == crate::execution_core::OUTCOME_EVENT_KIND)
            .expect("canonical Outcome");
        let outcome: harness_contract::outcome::ExecutionOutcome =
            serde_json::from_value(event.payload).expect("Outcome");
        assert_eq!(outcome.terminal.class_name(), expected_class);
    }
}

#[test]
fn explicit_team_obligation_is_durable_and_recoverable() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableExecutor,
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    runtime
        .begin_turn_strategy(
            "provider-turn",
            "必须启动两个 Team 分别审查三个独立域并综合",
        )
        .expect("admit team strategy");
    runtime
        .bind_turn_strategy_execution("provider-turn", "provider-graph")
        .expect("bind strategy graph");
    let frozen_events = store
        .list_stream(&format!("session:{}", runtime.session_id()))
        .expect("frozen strategy events");
    assert!(
        frozen_events.iter().any(|event| {
            event.kind == "runtime.strategy.selected"
                && event.payload["collaboration_obligation"].is_object()
        }),
        "frozen event stream: {frozen_events:#?}"
    );
    let recovered = runtime
        .recover_turn_strategy_identity("provider-turn", "provider-graph")
        .expect("valid durable strategy identity")
        .expect("recover frozen strategy identity");
    assert_eq!(
        recovered
            .collaboration_obligation
            .as_ref()
            .map(|obligation| obligation.minimum_team_count),
        Some(2)
    );
}

#[test]
fn low_novelty_publishes_bounded_early_stop() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableExecutor,
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".to_string()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    runtime
        .begin_turn_strategy("novelty-turn", "必须启动 Team 分别审查三个独立域并综合")
        .expect("admit team strategy");
    runtime
        .bind_turn_strategy_execution("novelty-turn", "novelty-graph")
        .expect("bind strategy graph");
    let selected = runtime.active_turn_strategy().expect("selected state");

    runtime
            .record_turn_strategy_early_stop(
                "low novelty: observed contribution 300 bp is below the 6000 bp target; stop further delegation",
            )
            .expect("early stop must be durable");

    let events = store
        .list_stream(&format!("session:{}", runtime.session_id()))
        .expect("strategy events");
    let early_stops = events
        .iter()
        .filter(|event| event.kind == "runtime.strategy.early_stopped")
        .collect::<Vec<_>>();
    assert_eq!(
        early_stops.len(),
        1,
        "early stop is a single bounded transition"
    );
    let early_stop = early_stops[0];
    assert_eq!(
        early_stop.payload["decision_revision"].as_u64(),
        Some(selected.revision.saturating_add(1))
    );
    assert_eq!(early_stop.status.as_deref(), Some("early_stopped"));
    assert!(early_stop.payload["reason"]
        .as_str()
        .expect("visible early-stop reason")
        .contains("low novelty"));
}

#[tokio::test]
async fn compaction_raw_journal_failure_retains_original_window() {
    struct RejectRawJournal;
    #[async_trait::async_trait]
    impl crate::SessionRuntimeJournalPort for RejectRawJournal {
        async fn commit_input_application_receipt(
            &self,
            input_ids: &[String],
            expected_revisions: &[u64],
            receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
            now_ms: u64,
        ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
            let _ = (input_ids, expected_revisions, receipt, now_ms);
            Err(session::SessionError::Store(
                "fixture has no input application owner".into(),
            ))
        }

        async fn authorize_requested_cancellation(
            &self,
            _request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
        ) -> Result<session::SessionDomainEvent, session::SessionError> {
            Err(session::SessionError::Store(
                "injected raw receipt failure".into(),
            ))
        }

        async fn append_event_if_claim_current(
            &self,
            _request: &crate::session_runtime_port::RuntimeSessionClaimEventAppend,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            Err(session::SessionError::Store(
                "injected raw receipt failure".into(),
            ))
        }

        async fn append_event(
            &self,
            _event: &crate::RuntimeSessionEvent,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            Err(session::SessionError::Store(
                "injected raw receipt failure".into(),
            ))
        }
        async fn append_context_envelope_if_absent(
            &self,
            _record: &crate::RuntimeContextEnvelopeRecord,
        ) -> Result<Option<crate::RuntimeSessionEventReceipt>, session::SessionError> {
            Ok(None)
        }
        async fn append_compaction_bundle_if_absent(
            &self,
            _events: &[crate::RuntimeSessionEvent],
            _checkpoint_id: &str,
        ) -> Result<bool, session::SessionError> {
            panic!("checkpoint must not commit before raw durability")
        }
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut source = Session::new();
    for i in 0..6 {
        source
            .push_message(ConversationMessage::user_text(format!(
                "original {i} {}",
                "原文".repeat(1000)
            )))
            .unwrap();
    }
    let before = source.materialize_messages();
    let mut runtime = ConversationRuntime::new(
        source,
        MockApi,
        StaticToolExecutor::new(),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec![],
    )
    .without_memory()
    .with_artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
        tmp.path().join("raw"),
    )))
    .with_session_journal_port(Arc::new(RejectRawJournal));
    runtime.session_compaction_config.preserve_recent = 2;
    let error = runtime.compact_active_session().await.unwrap_err();
    assert!(
        error.to_string().contains("injected raw receipt failure"),
        "{error}"
    );
    assert_eq!(
        runtime.session_snapshot().await.materialize_messages(),
        before
    );
}

#[test]
fn a_single_exact_evidence_result_cannot_exceed_the_ceiling() {
    let plan = crate::budget_policy::RuntimeBudgetPlan::derive(
        crate::budget_policy::RuntimeBudgetInputs::new(262_144, 32_768),
    );
    let before_total = plan.tool_result_budget.max_total_tokens;
    let raised =
        ConversationRuntime::<MockApi, StaticToolExecutor>::apply_exact_evidence_delivery_budget(
            plan,
        );
    assert!(
        raised.tool_result_budget.max_total_tokens >= before_total,
        "the turn lease may grow so whole files still fit"
    );
    assert!(
        raised.tool_result_budget.per_tool_max_tokens <= 16_000,
        "one result must never dominate the prompt, got {}",
        raised.tool_result_budget.per_tool_max_tokens
    );
    assert!(
        raised.tool_result_budget.per_tool_max_tokens >= 12_000,
        "a whole source file must still fit one call, got {}",
        raised.tool_result_budget.per_tool_max_tokens
    );
}

mod r1_native_real_wire {
    use super::*;
    use crate::context_evidence::{
        decode_read_acquisition, decode_read_delivery, read_payload_digest, read_sha256,
        ReadDeliveryMaterial, ReadDeliveryRecipient, ReadDeliveryTransport, ReadExtent,
        NATIVE_DELIVERY_KIND, READ_ACQUISITION_FIELD,
    };
    use crate::runtime_event_store::{DurableRuntimeEvent, RuntimeEventInput};
    use harness_contract::execution::ExecutionIdentity;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct WireServer {
        endpoint: String,
        first_call: Arc<Mutex<serde_json::Value>>,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
        disconnect: Arc<AtomicBool>,
        reject_output: Arc<AtomicBool>,
        delay_first_completion: Arc<AtomicBool>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for WireServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl WireServer {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured = requests.clone();
            let disconnect = Arc::new(AtomicBool::new(false));
            let reject_output = Arc::new(AtomicBool::new(false));
            let delay_first_completion = Arc::new(AtomicBool::new(false));
            let delay_first_for_server = delay_first_completion.clone();
            let first_call = Arc::new(Mutex::new(serde_json::json!({"name":"read_file","arguments":
                serde_json::json!({"path":"target.txt","complete":true}).to_string()})));
            let first_call_for_server = first_call.clone();
            let disconnected = disconnect.clone();
            let rejected_output = reject_output.clone();
            let task = tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let bytes = tokio::time::timeout(Duration::from_secs(10), async {
                        let mut bytes = Vec::new();
                        loop {
                            let mut buffer = [0_u8; 8192];
                            let count = socket.read(&mut buffer).await.unwrap();
                            assert!(count > 0 && bytes.len() < 4 * 1024 * 1024);
                            bytes.extend_from_slice(&buffer[..count]);
                            if let Some(offset) =
                                bytes.windows(4).position(|part| part == b"\r\n\r\n")
                            {
                                let headers = String::from_utf8_lossy(&bytes[..offset]);
                                let length = headers
                                    .lines()
                                    .find_map(|line| {
                                        let (name, value) = line.split_once(':')?;
                                        name.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse::<usize>().unwrap())
                                    })
                                    .unwrap();
                                if bytes.len() >= offset + 4 + length {
                                    break bytes[offset + 4..offset + 4 + length].to_vec();
                                }
                            }
                        }
                    })
                    .await
                    .unwrap();
                    let ordinal = {
                        let mut requests = captured.lock().unwrap();
                        requests.push(bytes);
                        requests.len()
                    };
                    if disconnected.load(Ordering::SeqCst) {
                        continue;
                    }
                    let delta = if ordinal == 1 {
                        serde_json::json!({"role":"assistant","tool_calls":[{
                            "index":0,"id":"native-read","type":"function",
                            "function":first_call_for_server.lock().unwrap().clone()
                        }]})
                    } else if rejected_output.load(Ordering::SeqCst) {
                        // The output frame names a tool this request never
                        // exposed, so Runtime refuses the whole frame.
                        serde_json::json!({"role":"assistant","tool_calls":[{
                            "index":0,"id":"native-refused-output","type":"function",
                            "function":{"name":"not_an_exposed_tool","arguments":
                                serde_json::json!({"path":"target.txt"}).to_string()}
                        }]})
                    } else {
                        serde_json::json!({"role":"assistant","content":"The supplied read result was received."})
                    };
                    let chunk = |delta, finish_reason| {
                        serde_json::json!({
                            "id":"native-loopback","object":"chat.completion.chunk",
                            "model":"native-wire-model","choices":[{
                                "index":0,"delta":delta,"finish_reason":finish_reason
                            }]
                        })
                    };
                    let response_prefix = format!(
                        "data: {}\n\ndata: {}\n\n",
                        chunk(delta, serde_json::Value::Null),
                        chunk(
                            serde_json::json!({}),
                            serde_json::json!(if ordinal == 1 { "tool_calls" } else { "stop" })
                        )
                    );
                    let done = "data: [DONE]\n\n";
                    socket.write_all(format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        response_prefix.len() + done.len(), response_prefix,
                    ).as_bytes()).await.unwrap();
                    if ordinal == 1 && delay_first_for_server.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                    socket.write_all(done.as_bytes()).await.unwrap();
                }
            });
            Self {
                endpoint,
                first_call,
                requests,
                disconnect,
                reject_output,
                delay_first_completion,
                task,
            }
        }
    }

    struct FallibleSessionJournal {
        inner: Arc<dyn crate::SessionRuntimeJournalPort>,
        reject_packed: AtomicBool,
        reject_outcome: AtomicBool,
        reject_continuation_outcomes: AtomicBool,
        outcomes_seen: AtomicUsize,
        rejected: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::SessionRuntimeJournalPort for FallibleSessionJournal {
        async fn commit_input_application_receipt(
            &self,
            input_ids: &[String],
            expected_revisions: &[u64],
            receipt: &harness_contract::input_disposition::SessionInputApplicationReceipt,
            now_ms: u64,
        ) -> Result<Vec<crate::RuntimeSessionInputRecord>, session::SessionError> {
            self.inner
                .commit_input_application_receipt(input_ids, expected_revisions, receipt, now_ms)
                .await
        }

        async fn authorize_requested_cancellation(
            &self,
            request: &crate::session_runtime_port::RuntimeSessionCancellationAuthorizationRequest,
        ) -> Result<session::SessionDomainEvent, session::SessionError> {
            self.inner.authorize_requested_cancellation(request).await
        }

        async fn append_event_if_claim_current(
            &self,
            request: &crate::session_runtime_port::RuntimeSessionClaimEventAppend,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            self.inner.append_event_if_claim_current(request).await
        }

        async fn append_event(
            &self,
            event: &crate::RuntimeSessionEvent,
        ) -> Result<crate::RuntimeSessionEventReceipt, session::SessionError> {
            let delayed_outcome = event.kind
                == crate::RuntimeSessionEventKind::ProviderAttemptOutcome
                && self.outcomes_seen.fetch_add(1, Ordering::SeqCst) > 0
                && self.reject_continuation_outcomes.load(Ordering::SeqCst);
            if (event.kind == crate::RuntimeSessionEventKind::ProviderRequestPacked
                && self.reject_packed.load(Ordering::SeqCst))
                || (event.kind == crate::RuntimeSessionEventKind::ProviderAttemptOutcome
                    && self.reject_outcome.load(Ordering::SeqCst))
                || delayed_outcome
            {
                self.rejected.fetch_add(1, Ordering::SeqCst);
                return Err(session::SessionError::Store(
                    "native real-wire journal fault".into(),
                ));
            }
            self.inner.append_event(event).await
        }

        async fn append_context_envelope_if_absent(
            &self,
            record: &crate::RuntimeContextEnvelopeRecord,
        ) -> Result<Option<crate::RuntimeSessionEventReceipt>, session::SessionError> {
            self.inner.append_context_envelope_if_absent(record).await
        }

        async fn append_compaction_bundle_if_absent(
            &self,
            events: &[crate::RuntimeSessionEvent],
            checkpoint_id: &str,
        ) -> Result<bool, session::SessionError> {
            self.inner
                .append_compaction_bundle_if_absent(events, checkpoint_id)
                .await
        }
    }

    struct GovernedReadExecutor {
        host: tools::ToolHost,
        expose_grep: bool,
        read_mode: AtomicUsize,
        requests: Arc<Mutex<Vec<crate::RuntimeToolExecutionRequest>>>,
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for GovernedReadExecutor {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.host.pin_snapshot())
        }

        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.host.pin_snapshot().describe_effect(name, input))
        }

        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            self.requests.lock().unwrap().push(request.clone());
            if request.tool_name == "read_file" && self.read_mode.load(Ordering::SeqCst) == 2 {
                return crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(), tool_name: request.tool_name.clone(),
                    status: crate::RuntimeToolExecutionStatus::Unknown, category: request.category,
                    output: None, error: Some("injected original read unknown".into()),
                    evidence_ref: format!("tool-effect:{}", request.idempotency_key),
                    observed_evidence: Vec::new(), pending_agent_batch: None,
                };
            }
            let binding = request.step_binding.as_ref().unwrap();
            let step = binding.resolve(Some(self.host.pin_snapshot())).unwrap();
            let input = serde_json::from_str(&request.input).unwrap();
            let result = if request.tool_name == "read_file" && self.read_mode.load(Ordering::SeqCst) == 1 {
                Err(tools::ToolHostError::Execution("injected original read failure".into()))
            } else if matches!(request.tool_name.as_str(), "tool_orchestrate" | "grep_search") {
                step.lease().execute_with_read_trace(request.authorization.as_ref().unwrap(), &request.tool_name, &input)
                    .and_then(|(output, children)| {
                        request.tool_progress.record_orchestrated_children(&children)
                            .map_err(tools::ToolHostError::Execution)?;
                        Ok(output)
                    })
            } else { step.lease()
                .execute_async(
                    request.authorization.as_ref().unwrap(),
                    &request.tool_name,
                    &input,
                )
                .await };
            let (status, output, error) = match result {
                Ok(output) => (
                    crate::RuntimeToolExecutionStatus::Executed,
                    Some(output),
                    None,
                ),
                Err(error) => (
                    crate::RuntimeToolExecutionStatus::Failed,
                    None,
                    Some(error.to_string()),
                ),
            };
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status,
                category: request.category,
                output,
                error,
                evidence_ref: format!("tool-effect:{}", request.idempotency_key),
                observed_evidence: Vec::new(),
                pending_agent_batch: None,
            }
        }
    }

    #[async_trait::async_trait]
    impl ToolExecutor for GovernedReadExecutor {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.host.pin_snapshot())
        }

        fn available_tool_names(&self) -> Vec<String> {
            let mut names = vec!["read_file".into(), "tool_orchestrate".into()];
            if self.expose_grep { names.push("grep_search".into()); }
            names
        }

        fn registered_tool_effect(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.host.pin_snapshot().describe_effect(name, input))
        }

        async fn execute_output(
            &self,
            _: &str,
            _: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new(
                "real-wire fixture requires original bound authorization",
            ))
        }
    }

    type NativeRuntime = ConversationRuntime<crate::ProviderRuntimeClient, GovernedReadExecutor>;

    struct NativeFixture {
        runtime: NativeRuntime,
        store: Arc<RuntimeEventStore>,
        sessions: Arc<session::UnifiedSessionStore>,
        journal: Arc<FallibleSessionJournal>,
        server: WireServer,
        bus: CowdEventBus,
        owner: harness_contract::projection::RuntimeActivityBinding,
        execution: crate::CowdExecutionContext,
        registry: Arc<crate::ProviderRegistry>,
        services: Option<Arc<crate::RuntimeServices>>,
        live_services: Arc<Mutex<Option<Arc<crate::RuntimeServices>>>>,
        _workspace: Arc<tempfile::TempDir>,
    }

    impl NativeFixture {
        async fn new(body: Option<&str>, recipient: &str) -> Self {
            Self::new_with_early(body, recipient, false).await
        }

        async fn new_with_early(body: Option<&str>, recipient: &str, early: bool) -> Self {
            Self::new_with_exposed_grep(body, recipient, early, false).await
        }

        async fn new_with_exposed_grep(body: Option<&str>, recipient: &str, early: bool, expose_grep: bool) -> Self {
            let workspace = tempfile::tempdir().unwrap();
            if let Some(body) = body {
                fs::write(workspace.path().join("target.txt"), body).unwrap();
            }
            let session = Session::new().with_workspace_root(workspace.path());
            let session_id = session.session_id.clone();
            let turn_id = format!("native-turn-{session_id}");
            let graph_id = format!("native-graph-{session_id}");
            let task_id = format!("native-task-{session_id}");
            let store = Arc::new(RuntimeEventStore::for_test());
            let commits = crate::execution_core::graph::ExecutionCommitService::new(store.clone());
            let mut graph =
                ExecutionGraph::new("Read the supplied current source and report its facts");
            graph.id = graph_id.clone();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
                root_task_id: task_id.clone(),
                task_id: task_id.clone(),
                generation: 1,
            });
            let mut model_node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "native-root-model",
            );
            model_node.id = format!("{graph_id}:model-owner");
            let model_node_id = model_node.id.clone();
            graph.nodes.push(model_node);
            let mut graph = commits.register_graph(graph).unwrap().graph;
            graph = commits
                .transition_node(
                    &graph,
                    &model_node_id,
                    ExecutionNodeStatus::Ready,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            let node = graph
                .nodes
                .iter()
                .find(|node| node.id == model_node_id)
                .unwrap();
            let binding = crate::execution_core::graph::ExecutionNodeBinding {
                executor_kind: node.executor_kind.clone(),
                ticket_idempotency_key: node.idempotency_key.clone(),
                attempt: 1,
                resource_lease_refs: vec![],
                scope_lease_ref: None,
                worktree_lease_ref: None,
            };
            graph = commits
                .bind_and_start_node(&graph, &model_node_id, binding)
                .unwrap()
                .graph;
            assert_eq!(
                graph.node_statuses[&model_node_id],
                ExecutionNodeStatus::Running
            );
            let task_identity = ExecutionIdentity::for_task_graph(
                "native-root-reader",
                "native-wire-workspace",
                "native-mission",
                &task_id,
                &session_id,
                &turn_id,
                &graph_id,
            )
            .unwrap();
            let identity = match recipient {
                "none" => None,
                "task_graph" => Some(task_identity),
                _ => panic!("unknown recipient"),
            };
            let owner = store
                .latest_for_stream(&format!("{graph_id}:node:{model_node_id}"))
                .unwrap()
                .unwrap()
                .activity_binding()
                .unwrap();
            assert_eq!(owner.node_id.as_deref(), Some(model_node_id.as_str()));
            owner.validate().unwrap();
            let execution = crate::CowdExecutionContext {
                execution_id: graph_id.clone(),
                session_id: session_id.clone(),
                turn_id: turn_id.clone(),
            };
            let bus = CowdEventBus::new();
            let sessions = Arc::new(crate::test_support::session_store());
            sessions
                .create_session(&session::SessionRecord {
                    session_id: session_id.clone(),
                    platform: "test".into(),
                    chat_id: "native-loopback".into(),
                    user_id: None,
                    model: Some("native-wire-model".into()),
                    created_at: "2026-10-17T00:00:00Z".into(),
                    last_activity: "2026-10-17T00:00:00Z".into(),
                    message_count: 0,
                    reset_policy: "manual".into(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    status: "active".into(),
                })
                .await
                .unwrap();
            let journal = Arc::new(FallibleSessionJournal {
                inner: crate::session_runtime_port::TestSessionPortAdapter::new(sessions.clone()),
                reject_packed: AtomicBool::new(false),
                reject_outcome: AtomicBool::new(false),
                rejected: AtomicUsize::new(0),
                reject_continuation_outcomes: AtomicBool::new(false),
                outcomes_seen: AtomicUsize::new(0),
            });
            let server = WireServer::start().await;
            server.delay_first_completion.store(early, Ordering::SeqCst);
            let registry = Arc::new(
                crate::ProviderRegistry::new(crate::config::ProvidersConfig {
                    providers: std::collections::HashMap::from([(
                        "native-loopback".into(),
                        crate::config::ProviderConfig {
                            reasoning_effort: None,
                            name: "native-loopback".into(),
                            base_url: server.endpoint.clone(),
                            api_key: "local-fixture-secret".into(),
                            models: vec!["native-wire-model".into()],
                            protocol: Some("completions".into()),
                            parallel_tool_calls: Default::default(),
                            early_tool_start: if early {
                                model_protocol::provider_config::EarlyToolStartMode::Enabled
                            } else { Default::default() },
                        },
                    )]),
                })
                .unwrap(),
            );
            let host = tools::ToolHost::builtin("native-wire-workspace", workspace.path())
                .with_authorization_lease_verifier(Arc::new(
                    crate::AuthorizationNegotiator::verify_lease_signature,
                ));
            let definitions = host
                .pin_snapshot()
                .snapshot()
                .catalog
                .definitions(None)
                .into_iter()
                .filter(|definition| matches!(definition.name.as_str(), "read_file" | "tool_orchestrate")
                    || (expose_grep && definition.name == "grep_search"))
                .map(|definition| crate::ProviderToolDefinition {
                    name: definition.name,
                    description: definition.description,
                    input_schema: definition.input_schema,
                })
                .collect();
            let client = crate::ProviderRuntimeClient::new(
                registry.clone(),
                "native-wire-model".into(),
                definitions,
            )
            .unwrap();
            let executor = GovernedReadExecutor {
                host,
                expose_grep,
                read_mode: AtomicUsize::new(0),
                requests: Arc::new(Mutex::new(Vec::new())),
            };
            let mut runtime = ConversationRuntime::new(
                session,
                client,
                executor,
                PermissionPolicy::new(PermissionMode::ReadOnly),
                SystemPromptBuilder::new().build(),
            )
            .without_memory()
            .with_runtime_event_store(store.clone())
            .with_session_journal_port(journal.clone())
            .with_artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
                workspace.path().join("artifacts"),
            )))
            .with_cowd_event_bus(bus.clone())
            .with_checkpoint_identity("native-wire-workspace", identity);
            runtime.set_active_model("native-wire-model");
            runtime = runtime.with_model_context_window(262_144);
            runtime
                .session_input_stream()
                .set_active_turn(Some(harness_contract::turn::TurnId::from_string(&turn_id)));
            Self {
                runtime,
                store,
                sessions,
                journal,
                server,
                bus,
                owner,
                execution,
                registry,
                services: None,
                live_services: Arc::new(Mutex::new(None)),
                _workspace: Arc::new(workspace),
            }
        }

        async fn run_root_host(mut self, defer_confirmation: bool) -> (Self, ModelToolCall) {
            let fixture_graph_id = self.execution.execution_id.clone();
            assert!(self.bus.current_execution_context().is_none());
            assert!(self.bus.current_activity_binding().is_none());
            self.journal
                .reject_continuation_outcomes
                .store(defer_confirmation, Ordering::SeqCst);
            let ports =
                crate::session_runtime_port::TestSessionPortAdapter::new(self.sessions.clone());
            let services = crate::RuntimeServices::test_builder(
                self._workspace.path().join("runtime-home"),
                self._workspace.path(),
            )
            .runtime_event_store(self.store.clone())
            .artifact_store(self.runtime.artifact_store.as_ref().unwrap().clone())
            .provider_registry(self.registry.clone())
            .tool_execution_host(self.runtime.tool_executor.clone())
            .session_ports(ports.clone(), ports.clone(), self.journal.clone(), ports)
            .build()
            .unwrap();
            *self.live_services.lock().unwrap() = Some(Arc::clone(&services));
            services.publish_session_execution_policy(
                self.runtime.session_id().to_string(),
                self.runtime.permission_policy.execution_policy_control(),
            );
            let spec = services
                .task_runtime_port()
                .bind_task_spec(
                    self.runtime.session_id(),
                    Some(harness_contract::policy::PermissionMode::ReadOnly),
                    harness_contract::task::TaskSpec::new(
                        "Read target.txt and report the current facts",
                    ),
                )
                .unwrap();
            services
                .task_runtime_port()
                .create(harness_contract::task::TaskCreateCommand {
                    task_id: self.owner.task_id.clone(),
                    mission_id: services.mission_runtime().default_mission_id().to_string(),
                    kind: harness_contract::task::TaskKind::Root,
                    origin: harness_contract::task::TaskOrigin::User,
                    origin_session_id: self.execution.session_id.clone(),
                    origin_turn_id: self.execution.turn_id.clone(),
                    root_task_id: self.owner.root_task_id.clone(),
                    parent_task_id: None,
                    predecessor_task_id: None,
                    mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                    mission_assigned_by: "runtime.native-wire-test".into(),
                    spec,
                    evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                        "test_input",
                        self.execution.turn_id.clone(),
                    )],
                })
                .unwrap();
            let identity = self.runtime.execution_identity.as_ref().map(|_| {
                ExecutionIdentity::for_task_graph(
                    "native-root-reader",
                    services.workspace_key(),
                    services.mission_runtime().default_mission_id(),
                    &self.owner.task_id,
                    &self.execution.session_id,
                    &self.execution.turn_id,
                    &self.execution.execution_id,
                )
                .unwrap()
            });
            let required = self.server.first_call.lock().unwrap()["name"].as_str().unwrap().to_string();
            self.runtime.require_next_model_tool_action([required]);
            let runtime = self
                .runtime
                .with_checkpoint_identity(services.workspace_key(), identity);
            let original_turn_cancelled = runtime.cancellation_token().is_cancelled();
            let lineage = ExecutionGraphLineage {
                session_id: self.execution.session_id.clone(),
                turn_id: self.execution.turn_id.clone(),
                root_task_id: self.owner.root_task_id.clone(),
                task_id: self.owner.task_id.clone(),
                generation: 1,
            };
            services
                .release_recovered_producers()
                .await
                .expect("release fresh native-wire Runtime producers");
            let (runtime, result) = tokio::time::timeout(
                Duration::from_secs(30),
                Box::pin(crate::submit_owned_conversation_turn(
                    runtime,
                    services.clone(),
                    "Read target.txt and report the current facts",
                    &crate::SharedPrompter::none(),
                    lineage.clone(),
                )),
            )
            .await
            .unwrap()
            .unwrap();
            self.runtime = runtime;
            assert!(self.runtime.original_turn_control().is_err());
            assert_eq!(self.runtime.cancellation_token().is_cancelled(), original_turn_cancelled);
            if result.is_err() {
                let requests = self.runtime.tool_executor.requests.lock().unwrap();
                eprintln!("early_witness captured_native_requests={}",
                    requests.iter().filter(|request| request.tool_use_id == "native-read").count());
                for request in requests.iter().filter(|request| request.tool_use_id == "native-read") {
                    let key = &request.idempotency_key;
                    let effect = format!("execution-effect:{key}");
                    let events = self.store.list_stream(&effect).unwrap();
                    eprintln!("early_witness call={} binding_key={:?} request_key={} parent={:?} parent_attempt={:?}",
                        request.tool_use_id, request.step_binding.as_ref().map(|binding| binding.effect_idempotency_key()),
                        key, request.parent_execution, request.parent_execution_attempt);
                    for event in events {
                        eprintln!("early_witness effect kind={} seq={} cursor={} tx={} tx_index={}",
                            event.kind, event.sequence, event.commit_cursor, event.transaction_id, event.transaction_index);
                    }
                    if let (Some(parent), Some(attempt)) = (&request.parent_execution, request.parent_execution_attempt) {
                        let stream = format!("execution-agent-receipts:{}:{}:{}", parent.execution_id, parent.node_id, attempt);
                        for event in self.store.list_stream(&stream).unwrap() {
                            eprintln!("early_witness owner_index stream={} kind={} seq={} cursor={} tx={} tx_index={} key={}",
                                stream, event.kind, event.sequence, event.commit_cursor, event.transaction_id,
                                event.transaction_index, event.payload["idempotency_key"]);
                        }
                        if let Ok(graph) = services.graph_state_store().load(&parent.execution_id) {
                            for node in graph.nodes.iter().filter(|node| node.executor_kind == "tool_batch") {
                                for attempt in 1..=node.retry_policy.max_attempts {
                                    let stream = format!("execution-agent-receipts:{}:{}:{}", graph.id, node.id, attempt);
                                    let events = self.store.list_stream(&stream).unwrap();
                                    eprintln!("early_witness batch_index stream={} events={}", stream, events.len());
                                    for event in events {
                                        eprintln!("early_witness batch_index_event kind={} seq={} cursor={} tx={} tx_index={} key={}",
                                            event.kind, event.sequence, event.commit_cursor, event.transaction_id,
                                            event.transaction_index, event.payload["idempotency_key"]);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            result.unwrap();
            let source = {
                let sources = self.runtime.turn_native_tool_sources.lock().unwrap();
                sources.get("native-read").unwrap_or_else(|| {
                    let kinds = self.store.list_stream(&format!("session:{}",self.runtime.session_id()))
                        .unwrap().iter().map(|event| (event.kind.clone(),event.status.clone(),event.payload["tool_name"].clone())).collect::<Vec<_>>();
                    let requests = self.server.requests.lock().unwrap();
                    let tools = requests.first().and_then(|bytes| serde_json::from_slice::<serde_json::Value>(bytes).ok())
                        .and_then(|body| body["tools"].as_array().map(|tools| tools.iter().map(|tool| tool["function"]["name"].clone()).collect::<Vec<_>>()));
                    panic!("native fixture source missing; sources={:?}; wire_requests={}; exposed_tools={tools:?}; session_events={kinds:?}",
                        sources.keys().collect::<Vec<_>>(),requests.len());
                }).clone()
            };
            let call = source.call.clone();
            self.execution = source.execution;
            self.owner = source
                .activity_owner
                .expect("actual Host model ticket source");
            assert_ne!(self.execution.execution_id, fixture_graph_id);
            let graph = services
                .graph_state_store()
                .load(&self.execution.execution_id)
                .unwrap();
            assert_eq!(graph.lineage.as_ref(), Some(&lineage));
            assert!(graph.parent_execution.is_none());
            assert_eq!(self.owner.root_execution_id, graph.id);
            let node_id = self
                .owner
                .node_id
                .as_ref()
                .expect("original Host model node");
            let original = self
                .store
                .event_by_idempotency_key(
                    &format!("{}:node:{node_id}", graph.id),
                    &format!("{}:{node_id}:{}", graph.id, self.owner.revision),
                )
                .unwrap()
                .expect("actual durable model start, not fixture root");
            assert_eq!(original.activity_binding().as_ref(), Some(&self.owner));
            assert_eq!(original.payload["to"], "running");
            assert!(self.bus.current_execution_context().is_none());
            assert!(self.bus.current_activity_binding().is_none());
            assert!(!self.bus.has_live_telemetry_observer());
            assert!(services.active_execution_bus(&graph.id).is_none());
            if let Some(identity) = self.runtime.execution_identity.as_ref() {
                assert_eq!(identity.graph_id(), Some(graph.id.as_str()));
                assert_eq!(identity.task_id(), Some(lineage.task_id.as_str()));
            }
            assert!(call.binding.is_some());
            assert!(self
                .runtime
                .tool_executor
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|request| request.tool_use_id == call.id
                    && request
                        .parent_execution
                        .as_ref()
                        .is_some_and(|parent| parent.execution_id == self.execution.execution_id)));
            services.shutdown_execution().await;
            services.shutdown_maintenance().await;
            self.services = Some(services.clone());
            self.journal
                .reject_continuation_outcomes
                .store(false, Ordering::SeqCst);
            if defer_confirmation {
                assert!(self.materials().is_empty());
                assert!(self.runtime.turn_model_observations().is_empty());
                assert!(self.journal.rejected.load(Ordering::SeqCst) > 0);
            }
            (self, call)
        }

        async fn continue_model(
            &mut self,
        ) -> Result<crate::conversation::ModelStepResult, RuntimeError> {
            let _scope = self
                .bus
                .enter_execution_with_activity(self.execution.clone(), Some(self.owner.clone()));
            if self.runtime.active_turn_strategy().is_none() {
                self.runtime
                    .begin_turn_strategy(
                        &self.execution.turn_id,
                        "Read target.txt and report the current facts",
                    )
                    .unwrap();
            }
            self.runtime.require_next_model_final_response();
            tokio::time::timeout(
                Duration::from_secs(20),
                self.runtime
                    .execute_model_step("Report the current facts", false),
            )
            .await
            .unwrap()
        }

        fn materials(&self) -> Vec<DurableRuntimeEvent> {
            self.store
                .list_stream(&format!("session:{}", self.runtime.session_id()))
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == NATIVE_DELIVERY_KIND)
                .collect()
        }

        fn acquisition(&self, call: &ModelToolCall) -> DurableRuntimeEvent {
            let key = call.binding.as_ref().unwrap().effect_idempotency_key();
            let event = self
                .store
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!("{key}:read-receipt"),
                )
                .unwrap()
                .expect("original governed primary receipt");
            let requests = self.runtime.tool_executor.requests.lock().unwrap();
            let request = requests
                .iter()
                .find(|request| request.tool_use_id == call.id)
                .unwrap();
            assert_eq!(request.idempotency_key, key);
            assert!(request.compiled_invocation.is_some());
            if !event.payload[READ_ACQUISITION_FIELD].is_null() {
                let scope: harness_contract::goal::ProofEffectScope =
                    serde_json::from_value(event.payload["resolved_effect_scope"].clone()).unwrap();
                let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
                assert_eq!(scope.version, 2);
                assert!(scope.complete);
                scope.validate_purpose_mapping(descriptor).unwrap();
                assert!(scope
                    .scope_purposes
                    .iter()
                    .any(|entry| entry.purpose
                        == harness_contract::goal::ProofEffectPurpose::InputRead));
                let mut omitted = scope.clone();
                omitted.scope_purposes.clear();
                assert!(omitted.validate_purpose_mapping(descriptor).is_err());
                for version in [0, 1, 99] {
                    let mut unknown = serde_json::to_value(&scope).unwrap();
                    unknown["version"] = version.into();
                    assert!(
                        serde_json::from_value::<harness_contract::goal::ProofEffectScope>(unknown)
                            .is_err()
                    );
                }
                let mut missing_mapping = serde_json::to_value(&scope).unwrap();
                missing_mapping
                    .as_object_mut()
                    .unwrap()
                    .remove("scope_purposes");
                assert!(
                    serde_json::from_value::<harness_contract::goal::ProofEffectScope>(
                        missing_mapping
                    )
                    .is_err()
                );
                let mut duplicate = scope.clone();
                duplicate
                    .scope_purposes
                    .push(scope.scope_purposes[0].clone());
                assert!(duplicate.validate_purpose_mapping(descriptor).is_err());
                let seal = decode_read_acquisition(&event.payload[READ_ACQUISITION_FIELD]).unwrap();
                let parent = request
                    .parent_execution
                    .as_ref()
                    .expect("parent is issued by the original Host ticket");
                assert_eq!(seal.graph_id, parent.execution_id);
                assert_eq!(seal.node_id, parent.node_id);
                assert_eq!(Some(seal.attempt), request.parent_execution_attempt);
                assert_eq!(seal.step, call.binding.as_ref().unwrap().snapshot.identity);
                let index = self
                    .store
                    .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
                    .unwrap()
                    .unwrap();
                assert_eq!(index.transaction_id, event.transaction_id);
                assert_eq!(
                    index.payload["resolved_effect_scope"],
                    event.payload["resolved_effect_scope"]
                );
            }
            event
        }

        async fn assert_actual_wire(&self, material: &ReadDeliveryMaterial) {
            let ReadDeliveryTransport::NativeProviderContinuation {
                request_id,
                wire_body_sha256,
                wire_artifact,
                packed_sequence,
                outcome_sequence,
                step_render,
                raw_ref,
                ..
            } = &material.transport
            else {
                panic!("must be Native wire transport")
            };
            let bytes = self
                .runtime
                .artifact_store
                .as_ref()
                .unwrap()
                .read(
                    wire_artifact,
                    &format!("session:{}", self.runtime.session_id()),
                    None,
                )
                .await
                .unwrap();
            let artifact: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
            let wire = &artifact["provider_request"]["wire_request"]["body"];
            let canonical = serde_json::to_vec(wire).unwrap();
            assert_eq!(
                format!("{:x}", Sha256::digest(&canonical)),
                *wire_body_sha256
            );
            assert!(self
                .server
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|sent| sent == &canonical));
            assert!(!String::from_utf8(bytes)
                .unwrap()
                .contains("local-fixture-secret"));
            let presentation = wire["messages"]
                .as_array()
                .unwrap()
                .iter()
                .find(|message| {
                    message["role"] == "tool"
                        && message["tool_call_id"] == material.tool_invocation_id
                })
                .expect("actual ToolResult position, not a JSON substring")["content"]
                .as_str()
                .unwrap();
            assert_eq!(
                read_sha256(presentation.as_bytes()),
                material.presentation_sha256
            );
            assert_eq!(presentation.len() as u64, material.presentation_bytes);
            let packed = self
                .sessions
                .session_domain_events_page(self.runtime.session_id(), 0, 4096)
                .await
                .unwrap();
            let packed_event = packed
                .events
                .iter()
                .find(|event| event.sequence == *packed_sequence)
                .unwrap();
            assert_eq!(packed_event.kind, "context.provider_request_packed");
            assert_eq!(packed_event.payload["request_id"], *request_id);
            assert_eq!(packed_event.payload["body_sha256"], *wire_body_sha256);
            assert_eq!(
                packed_event.payload["step_render"]["step_id"],
                step_render.step_id
            );
            assert_ne!(step_render.step_id, material.acquisition.step.step_id);
            assert!(self
                .store
                .list_stream(&format!("session:{}", self.runtime.session_id()))
                .unwrap()
                .iter()
                .any(|event| event.kind == "model.step_bound"
                    && event.payload["identity"]
                        == serde_json::to_value(&material.acquisition.step).unwrap()));
            let outcome = packed
                .events
                .iter()
                .find(|event| event.sequence == *outcome_sequence)
                .unwrap();
            assert_eq!(outcome.kind, "context.provider_attempt_outcome");
            assert_eq!(outcome.payload["request_id"], *request_id);
            assert_eq!(outcome.payload["terminal_status"], "completed");
            assert!(outcome_sequence > packed_sequence);
            let raw = self.runtime.existing_evidence_access(raw_ref).unwrap();
            let original = self
                .runtime
                .artifact_store
                .as_ref()
                .unwrap()
                .resolve(&raw.retrieval_selector)
                .unwrap();
            let original_bytes = self
                .runtime
                .artifact_store
                .as_ref()
                .unwrap()
                .read(&original, &raw.visibility_scope, None)
                .await
                .unwrap();
            assert_eq!(
                read_sha256(&original_bytes),
                material.acquisition.body.output_sha256
            );
            assert_eq!(
                original_bytes.len() as u64,
                material.acquisition.body.output_bytes
            );
            let original_output: serde_json::Value =
                serde_json::from_slice(&original_bytes).unwrap();
            assert_eq!(
                original_output["file"]["sha256"]
                    .as_str()
                    .unwrap()
                    .trim_start_matches("sha256:"),
                material
                    .acquisition
                    .body
                    .source_sha256
                    .trim_start_matches("sha256:")
            );
        }
    }

    #[tokio::test]
    async fn original_early_read_reaches_real_host_with_model_ticket() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let fixture = NativeFixture::new_with_early(Some("alpha\nbeta\n"), "none", true).await;
        let mut host_tasks = tokio::task::JoinSet::new();
        host_tasks.spawn(async move { fixture.run_root_host(false).await });
        let (fixture, call) = host_tasks
            .join_next()
            .await
            .expect("native Host task must exist")
            .expect("native Host task must complete without panic or cancellation");
        let requests = fixture.runtime.tool_executor.requests.lock().unwrap();
        let matching = requests.iter().filter(|request| request.tool_use_id == call.id).collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "the original early read must not be dispatched twice");
        let request = matching[0];
        let parent = request.parent_execution.as_ref().expect("original read parent");
        let graph = fixture.services.as_ref().unwrap().graph_state_store().load(&parent.execution_id).unwrap();
        let node = graph.nodes.iter().find(|node| node.id == parent.node_id).expect("original parent node");
        assert_eq!(node.executor_kind, "inline_model", "a ToolBatch fallback is not an early read");
        assert_eq!(request.step_binding.as_ref(), call.binding.as_ref());
        assert!(request.authorization.is_some());
        drop(requests);
        let receipt = fixture.acquisition(&call);
        assert_eq!(receipt.kind, "execution.read.receipt");
        // A carrier whose outcome drifts from the committed receipt is rejected.
        let request = fixture.runtime.tool_executor.requests.lock().unwrap().iter()
            .find(|request| request.tool_use_id == call.id).unwrap().clone();
        let session_events = fixture.store.list_stream(&format!("session:{}", fixture.runtime.session_id())).unwrap();
        let lifecycle = session_events.iter().filter(|event| event.kind == "tool.invocation.completed"
            && event.payload["tool_call_id"] == call.id && event.payload["tool_name"] == call.name)
            .collect::<Vec<_>>();
        let [lifecycle] = lifecycle.as_slice() else { panic!("one original early lifecycle required") };
        assert_eq!(lifecycle.payload["governed_plan_id"], request.governed_plan_id);
        let plans = session_events.iter().filter(|event| event.kind == "tool.execution_plan.created"
            && event.payload["plan_id"] == request.governed_plan_id).collect::<Vec<_>>();
        let [plan] = plans.as_slice() else { panic!("the original early plan must be durable") };
        assert!(plan.commit_cursor < receipt.commit_cursor && receipt.commit_cursor < lifecycle.commit_cursor);
        let plan_receipt = fixture.store.original_transaction_receipt(&plan.transaction_id).unwrap()
            .expect("original early plan transaction");
        assert_eq!(plan_receipt.commit_cursor, plan.commit_cursor);
        assert_eq!(plan_receipt.event_ids.get(plan.transaction_index as usize), Some(&plan.event_id));
        assert!(plan_receipt.stream_revisions.iter().any(|source| source.stream_id == plan.stream_id
            && source.expected_revision < plan.sequence && plan.sequence <= source.committed_revision));
        let tasks = plan.payload["tasks"].as_array().unwrap().iter().filter(|task|
            task["tool_call_id"] == call.id && task["tool_name"] == call.name).collect::<Vec<_>>();
        let [task] = tasks.as_slice() else { panic!("the original early plan must contain the same call") };
        assert_eq!(task["step_binding"], serde_json::to_value(call.binding.as_ref().unwrap()).unwrap());
        assert_eq!(task["normalized_input"], harness_contract::step::parse_strict_json(&call.input).unwrap());
        let identity = &call.binding.as_ref().unwrap().snapshot.identity;
        for (kind, id) in [("session", identity.session_id.as_str()), ("turn", identity.turn_id.as_str()),
            ("execution", identity.execution_id.as_deref().unwrap())] {
            assert!(plan.refs.iter().any(|reference| reference.kind == kind && reference.id == id));
        }
        let services = fixture.services.as_ref().unwrap();
        let parent = request.parent_execution.as_ref().unwrap();
        let graph = services.graph_state_store().load(&parent.execution_id).unwrap();
        let binding = services.graph_state_store().current_binding(&graph, &parent.node_id,
            request.parent_execution_attempt.unwrap()).unwrap();
        let (ticket, _) = services.graph_state_store().binding_start_ticket(&graph, &parent.node_id, &binding).unwrap();
        let mut unknown: crate::RuntimeToolExecutionOutcome = serde_json::from_value(receipt.payload["outcome"].clone()).unwrap();
        unknown.status = crate::RuntimeToolExecutionStatus::Unknown;
        let candidate = crate::conversation::EarlyToolExecutionReceipt {
            original_source: Some(crate::conversation::EarlyToolOriginalSource {
                ticket: ticket.clone(), request: Arc::new(request), completed_calls: Some(vec![call.clone()]),
            }),
            lifecycle: crate::conversation::ToolLifecycleReuse::Unknown,
            call: call.clone(), outcome: unknown, ready_at_ms: 0, started_at_ms: 0, completed_at_ms: 0,
        };
        assert!(!services.commit_service().original_early_read_is_committed(&ticket, &candidate).unwrap(),
            "a changed carrier outcome must not match the committed terminal receipt");
    }

    #[tokio::test]
    async fn original_early_failed_read_is_not_reexecuted_and_keeps_failed_receipt() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") { return; }
        let fixture = NativeFixture::new_with_early(Some("alpha\nbeta\n"), "none", true).await;
        fixture.runtime.tool_executor.read_mode.store(1, Ordering::SeqCst);
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { fixture.run_root_host(false).await });
        let (fixture, call) = tasks.join_next().await.expect("Host task exists")
            .expect("original failed read must close honestly");
        let requests = fixture.runtime.tool_executor.requests.lock().unwrap();
        assert_eq!(requests.iter().filter(|request| request.tool_use_id == call.id).count(), 1,
            "a completed failed read must not be dispatched again");
        drop(requests);
        let receipt = fixture.acquisition(&call);
        assert_eq!(receipt.payload["outcome"]["status"], "failed");
    }

    #[tokio::test]
    async fn original_early_unknown_read_keeps_custody_without_redispatch() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") { return; }
        let fixture = NativeFixture::new_with_early(Some("alpha\nbeta\n"), "none", true).await;
        fixture.runtime.tool_executor.read_mode.store(2, Ordering::SeqCst);
        let requests = Arc::clone(&fixture.runtime.tool_executor.requests);
        let live_services = Arc::clone(&fixture.live_services);
        let workspace = Arc::clone(&fixture._workspace);
        let cancellation = fixture.runtime.cancellation_token();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { fixture.run_root_host(false).await });
        let (services, ticket, request, worker_nonce) = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let services = live_services.lock().unwrap().clone();
                let request = requests.lock().unwrap().iter()
                    .find(|request| request.tool_use_id == "native-read").cloned();
                if let (Some(services), Some(request)) = (services, request) {
                    if let (Some(parent), Some(attempt)) = (&request.parent_execution, request.parent_execution_attempt) {
                        if let Ok(graph) = services.graph_state_store().load(&parent.execution_id) {
                            if let Ok(binding) = services.graph_state_store().current_binding(&graph, &parent.node_id, attempt) {
                                if let Ok((ticket, _)) = services.graph_state_store().binding_start_ticket(&graph, &parent.node_id, &binding) {
                                    if let Some((failure, worker_nonce)) = services.model_step_executor().original_unknown_worker_for_test(&ticket, "native-read") {
                                        assert!(failure.detail.to_ascii_lowercase().contains("unknown"), "original worker must report Unknown");
                                        assert!(services.execution_supervisor().active_binding_for_ticket_for_test(&ticket).await.is_some(),
                                            "the same original model binding must retain custody");
                                        break (services, ticket, request, worker_nonce);
                                    }
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("original Unknown and retained binding must be observed before the test deadline");
        let before_cancel = services.graph_state_store().load(&ticket.graph_id).unwrap();
        assert_eq!(before_cancel.node_statuses.get(&ticket.node_id), Some(&ExecutionNodeStatus::Running));
        assert!(!before_cancel.node_results.contains_key(&ticket.node_id), "Unknown must not fabricate a terminal outcome");
        let primary = format!("execution-effect:{}", request.idempotency_key);
        assert!(services.event_store().event_by_idempotency_key(&primary,
            &format!("{}:read-receipt", request.idempotency_key)).unwrap().is_none(),
            "Unknown must not fabricate a read receipt");
        assert_eq!(requests.lock().unwrap().iter().filter(|request| request.tool_use_id == "native-read").count(), 1,
            "Unknown original read must not be dispatched again");
        cancellation.cancel();
        assert!(cancellation.is_cancelled(), "the original turn cancellation token must be signalled");
        let shutdown_services = Arc::clone(&services);
        let shutdown_task = tokio::spawn(async move { shutdown_services.shutdown_execution().await });
        let joined = tokio::time::timeout(Duration::from_secs(30), tasks.join_next()).await
            .expect("original cancellation must resolve within the test deadline").expect("Host task exists");
        match joined {
            Ok(_) => panic!("Unknown must not turn into a successful Host result during cancellation"),
            Err(error) if error.is_panic() => {
                let panic = error.into_panic();
                let reason = panic.downcast_ref::<String>().map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied()).unwrap_or("").to_ascii_lowercase();
                assert!(!reason.contains("elapsed"), "timeout is not cancellation evidence: {reason}");
                assert!(reason.contains("cancel") || reason.contains("unknown") || reason.contains("uncertain"),
                    "failure must come from original cancellation or Unknown custody: {reason}");
            }
            Err(error) => assert!(error.is_cancelled(), "unexpected Host join failure: {error}"),
        }
        let shutdown = tokio::time::timeout(Duration::from_secs(30), shutdown_task).await
            .expect("original execution shutdown must complete or report retained custody")
            .expect("original execution shutdown task must join");
        assert_eq!(requests.lock().unwrap().iter().filter(|request| request.tool_use_id == "native-read").count(), 1,
            "cancellation must not redispatch the original Unknown call");
        let graph = services.graph_state_store().load(&ticket.graph_id).unwrap();
        assert!(!graph.node_results.get(&ticket.node_id).is_some_and(|result| result.status == ExecutionNodeStatus::Completed),
            "cancellation must not promote Unknown to completed");
        assert!(services.model_step_executor().retains_original_early_worker_for_test(&ticket, "native-read", &worker_nonce),
            "cleanup must retain the exact original Unknown worker; shutdown report: {shutdown:?}");
        assert!(shutdown.preserved_graphs > 0 || shutdown.errors.iter().any(|error| {
            let error = error.to_ascii_lowercase();
            error.contains("unknown") || error.contains("unconfirmed") || error.contains("unresolved")
        }), "original shutdown must report the preserved or unresolved graph: {shutdown:?}");
        drop(workspace);
    }

    #[tokio::test]
    async fn original_early_deferral_falls_back_to_same_call_in_real_tool_batch() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let fixture = NativeFixture::new_with_early(Some("alpha\nbeta\n"), "none", true).await;
        let program = serde_json::json!({"steps":[{"id":"read","op":"batch","calls":[
            {"tool":"read_file","input":{"path":"target.txt","complete":true}}
        ]}]});
        *fixture.server.first_call.lock().unwrap() = serde_json::json!({
            "name":"tool_orchestrate", "arguments":program.to_string()
        });
        let mut host_tasks = tokio::task::JoinSet::new();
        host_tasks.spawn(async move { fixture.run_root_host(false).await });
        let (fixture, call) = host_tasks
            .join_next()
            .await
            .expect("native Host task must exist")
            .expect("native Host task must complete without panic or cancellation");
        let model_node = fixture.owner.node_id.as_ref().expect("original model node");
        let deferrals = fixture.store.list_stream(&format!("execution-node:{model_node}")).unwrap();
        assert!(deferrals.iter().any(|event| event.kind == "execution.tool_early_deferred"
            && event.payload["tool_call_id"] == call.id
            && event.payload["reason"].as_str().is_some_and(|reason| !reason.is_empty()
                && reason != "provider early_tool_start gate is not performance-certified")),
            "the original dispatcher must defer; the disabled performance gate is not a witness");
        let requests = fixture.runtime.tool_executor.requests.lock().unwrap();
        let matching = requests.iter().filter(|request| request.tool_use_id == call.id).collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "the deferred call must run exactly once in the original batch");
        let request = matching[0];
        let parent = request.parent_execution.as_ref().expect("fallback parent");
        let graph = fixture.services.as_ref().unwrap().graph_state_store().load(&parent.execution_id).unwrap();
        let node = graph.nodes.iter().find(|node| node.id == parent.node_id).expect("fallback node");
        assert_eq!(node.executor_kind, "tool_batch");
        assert_eq!(request.step_binding.as_ref(), call.binding.as_ref());
        drop(requests);
        let receipt = fixture.acquisition(&call);
        assert_eq!(receipt.kind, "execution.read.receipt");
    }

    #[tokio::test]
    async fn grep_batch_same_owner_commits_and_delivers_original_file_window() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") { return; }
        let fixture = NativeFixture::new_with_exposed_grep(Some("alpha\nbeta\n"), "none", false, true).await;
        let input = serde_json::json!({"path":fixture._workspace.path().to_str().unwrap(),
            "searches":[{"pattern":"alpha","glob":"target.txt"}]});
        *fixture.server.first_call.lock().unwrap() = serde_json::json!({"name":"grep_search","arguments":input.to_string()});
        let (fixture, call) = fixture.run_root_host(false).await;
        let primary = fixture.acquisition(&call);
        let children = primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD].as_array().unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0]["batch_index"], 0);
        assert_eq!(children[0]["relative_path"], "target.txt");
        let session = fixture.runtime.session_id();
        let events = fixture.store.list_stream(&format!("session:{session}")).unwrap();
        let delivery = events.iter().find(|event| event.kind == "context.nested_read_model_observed"
            && event.payload["child"]["parent_tool_use_id"] == call.id).unwrap();
        let root = delivery.payload["parent"]["graph_id"].as_str().unwrap();
        let services = fixture.services.as_ref().unwrap();
        let context = harness_contract::projection::ProjectionQueryContext {
            principal:"nested-test".into(), workspace_id:services.workspace_key().into(),
            session_scopes:vec![session.into()], mission_scopes:vec![],
            visibility_grants:vec![format!("workspace:{}", services.workspace_key()),"resource:*".into()],
            detail_scope:harness_contract::projection::ProjectionDetailScope::Full, authorization_revision:1,
        };
        let page = crate::execution_projection::read_evidence(services, root, &context, Default::default()).await.unwrap();
        assert!(!page.incomplete, "{page:?}");
        assert!(page.reads.iter().any(|read| read.body_delivered && read.tool_name == "grep_search"
            && read.path.workspace_relative_path == "target.txt"));
    }

    #[tokio::test]
    async fn nested_read_real_host_wire_emits_only_actual_children() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let fixture = NativeFixture::new(Some("alpha\nbeta\n"), "none").await;
        fs::write(fixture._workspace.path().join("other.txt"), "gamma\ndelta\n").unwrap();
        let program = serde_json::json!({"steps":[{"id":"reads","op":"batch","calls":[
            {"tool":"read_file","input":{"path":"target.txt","complete":true}},
            {"tool":"read_file","input":{"path":"other.txt","complete":true}}
        ]}]});
        *fixture.server.first_call.lock().unwrap() = serde_json::json!({"name":"tool_orchestrate","arguments":program.to_string()});
        let (fixture, call) = fixture.run_root_host(false).await;
        let primary = fixture.acquisition(&call);
        assert!(primary.payload["replay"]["parent_execution"].is_object());
        assert!(primary.payload["root_effect_owner"].is_null());
        let children = primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD].as_array().unwrap_or_else(|| {
            let requests = fixture.runtime.tool_executor.requests.lock().unwrap();
            let request = requests.iter().find(|request| request.tool_use_id == call.id).unwrap();
            let captured = request.tool_progress.child_reads().unwrap();
            let scope = serde_json::from_value::<harness_contract::goal::ProofEffectScope>(primary.payload["resolved_effect_scope"].clone()).ok();
            let validated = captured.as_ref().zip(scope.as_ref()).and_then(|(children, scope)|
                crate::context_evidence::child_read_acquisitions(request, scope, children)).map(|children| children.len());
            let body_seals = captured.as_ref().map(|children| children.iter().map(|child|
                crate::context_evidence::read_body_seal("read_file", child.output()).is_some()).collect::<Vec<_>>());
            let child_bindings = captured.as_ref().map(|children| children.iter().map(|child| {
                let effect = child.effect();
                let auth = request.authorization.as_ref();
                let compiled = request.compiled_invocation.as_ref();
                (effect.tool_id.clone(), format!("{:?}", effect.effect_kind), effect.scopes.len(),
                    auth.is_some_and(|auth| child.parent_authorization() == auth && effect.scopes.iter().all(|s| auth.authorization_lease.scopes.contains(s))),
                    compiled.is_some_and(|parent| effect.scopes.iter().all(|s| parent.effect.scopes.contains(s))),
                    scope.as_ref().is_some_and(|scope| effect.scopes.iter().all(|s| scope.targets.iter().filter(|t| &t.declared_scope == s).count() == 1)))
            }).collect::<Vec<_>>());
            panic!("actual child candidates absent; host_count={:?}; direct_validation={validated:?}; body_seals={body_seals:?}; child_bindings={child_bindings:?}; scope_complete={}; scopes={}; status={}; error={}; request_id_matches={}; descriptor_matches={}",
                captured.as_ref().map(Vec::len), primary.payload["resolved_effect_scope"]["complete"],
                primary.payload["resolved_effect_scope"]["targets"].as_array().map_or(0,Vec::len),
                primary.payload["outcome"]["status"],primary.payload["outcome"]["error"],
                request.authorization.as_ref().is_some_and(|auth| auth.request_id == request.tool_use_id),
                request.authorization.as_ref().zip(request.compiled_invocation.as_ref()).is_some_and(|(auth, invocation)| auth.descriptor_hash == invocation.effect.descriptor_hash));
        });
        assert_eq!(children.len(), 2);
        let request = fixture.runtime.tool_executor.requests.lock().unwrap().iter()
            .find(|request| request.tool_use_id == call.id).unwrap().clone();
        let captured = request.tool_progress.child_reads().unwrap().unwrap();
        assert_eq!(primary.payload["child_read_acquisitions_complete"], true);
        let parent = request.parent_execution.as_ref().unwrap();
        let source = format!("execution-agent-receipts:{}:{}:{}", parent.execution_id,
            parent.node_id, request.parent_execution_attempt.unwrap());
        let index = fixture.store.event_by_idempotency_key(&source,
            &format!("agent-tool-receipt:{}", request.idempotency_key)).unwrap().unwrap();
        assert_eq!(primary.transaction_id, index.transaction_id);
        assert_eq!(primary.commit_cursor, index.commit_cursor);
        assert_eq!(index.payload["child_read_acquisitions_complete"], true);
        assert_eq!(primary.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD],
            index.payload[crate::context_evidence::CHILD_READ_ACQUISITIONS_FIELD]);
        let scope: harness_contract::goal::ProofEffectScope = serde_json::from_value(primary.payload["resolved_effect_scope"].clone()).unwrap();
        let auth = request.authorization.as_ref().unwrap();
        assert_eq!(auth.request_id, request.tool_use_id);
        assert!(captured.iter().all(|child| child.parent_request_id() == auth.request_id
            && child.parent_authorization() == auth));
        assert!(crate::context_evidence::child_read_acquisitions(&request, &scope, &captured).is_some());
        let mut wrong = request.clone();
        wrong.authorization.as_mut().unwrap().request_id = "foreign-request".into();
        assert!(crate::context_evidence::child_read_acquisitions(&wrong, &scope, &captured).is_none());
        let mut wrong = request.clone();
        wrong.authorization.as_mut().unwrap().authorization_lease.signature = "foreign".into();
        assert!(crate::context_evidence::child_read_acquisitions(&wrong, &scope, &captured).is_none());
        let mut wrong = request.clone();
        wrong.tool_use_id = "foreign-call".into();
        assert!(crate::context_evidence::child_read_acquisitions(&wrong, &scope, &captured).is_none());
        let mut wrong = request.clone();
        wrong.session_id = Some("foreign-session".into());
        assert!(crate::context_evidence::child_read_acquisitions(&wrong, &scope, &captured).is_none());
        let events = fixture.store.list_stream(&format!("session:{}",fixture.runtime.session_id())).unwrap();
        let deliveries = events.iter().filter(|event| event.kind == "context.nested_read_model_observed").collect::<Vec<_>>();
        if deliveries.len() != 2 {
            let sources = fixture.runtime.turn_native_tool_sources.lock().unwrap();
            let source = sources.get(&call.id).unwrap();
            let generated = fixture.runtime.turn_generated_model_receipts.lock().unwrap();
            let receipt = generated.iter().find(|receipt| receipt.provider_invocation_id == call.id);
            let parent = fixture.runtime.native_child_parent_acquisition(source);
            let matched = receipt.map(|receipt| children.iter().map(|child|
                crate::context_evidence::child_read_presented_extent(&call.input, &receipt.presentation, child).is_some()).collect::<Vec<_>>());
            let wires = fixture.runtime.native_wire_receipts.lock().unwrap();
            let _owner = fixture.bus.enter_execution_with_activity(fixture.execution.clone(), Some(fixture.owner.clone()));
            let recipient = match (&parent, receipt, wires.last()) {
                (Ok(Some((parent,_))), Some(receipt), Some(wire)) =>
                    fixture.runtime.native_read_recipient(source, parent, &receipt.raw_ref, &wire.context).map(|r| r.is_some()),
                _ => Ok(false),
            };
            let owner_count = fixture.runtime.turn_native_root_owners.lock().unwrap().len();
            panic!("nested wire delivery missing; count={}; generated={}; parent={:?}; matched={matched:?}; wire_count={}; wire_result_counts={:?}; recipient={recipient:?}; root_owners={owner_count}; source_activity={}; source_tool_owner={}",
                deliveries.len(), receipt.is_some(), parent.as_ref().map(|p| p.as_ref().map(|(_,c)|c.len())).map_err(|e|e.to_string()),
                wires.len(), wires.iter().map(|w|(w.results.len(),w.outcome_sequence.is_some(),w.step_render.is_some())).collect::<Vec<_>>(),
                source.activity_owner.is_some(),source.tool_owner.is_some());
        }
        let delivery = *deliveries.iter().find(|event| event.payload["child"]["position"] == 0).unwrap();
        assert_eq!(delivery.payload["recipient"]["kind"], "root");
        assert!(children.iter().any(|child| *child == delivery.payload["child"]));
        assert!(delivery.payload["parent"].get("body").is_none());
        assert_eq!(delivery.payload["delivered_extent"]["line_count"], 2);
        let requests = fixture.server.requests.lock().unwrap();
        assert!(requests.len() >= 2);
        let wire: serde_json::Value = serde_json::from_slice(&requests[1]).unwrap();
        assert!(wire.to_string().contains("alpha"));
        assert_eq!(delivery.payload["transport"]["wire_body_sha256"],
            format!("{:x}", Sha256::digest(serde_json::to_vec(&wire).unwrap())));
        drop(requests);
        let material = crate::agentic::review_evidence::nested_parent_material(&delivery.payload).unwrap();
        let ReadDeliveryTransport::NativeProviderContinuation { wire_artifact, .. } = &material.transport else { panic!("native wire required") };
        let bytes = fixture.runtime.artifact_store.as_ref().unwrap().read(
            wire_artifact, &format!("session:{}", fixture.runtime.session_id()), None).await.unwrap();
        let artifact: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let actual: provider::ProviderWireRequest = serde_json::from_value(artifact["provider_request"]["wire_request"].clone()).unwrap();
        let text = crate::context_evidence::wire_tool_result_text(&actual, &call.id).expect("exact original wire result");
        assert_eq!(read_sha256(text.as_bytes()), material.presentation_sha256);
        let mut missing = actual.clone();
        missing.body["messages"].as_array_mut().unwrap().retain(|m| m["tool_call_id"] != call.id);
        missing.body_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&missing.body).unwrap()));
        assert!(crate::context_evidence::wire_tool_result_text(&missing, &call.id).is_none());
        let mut forged_wire = actual.clone();
        forged_wire.body["messages"].as_array_mut().unwrap().iter_mut()
            .find(|m| m["tool_call_id"] == call.id).unwrap()["content"] = serde_json::json!("forged");
        assert!(crate::context_evidence::wire_tool_result_text(&forged_wire, &call.id).is_none());
        let session = fixture.runtime.session_id();
        let turn = delivery.payload["parent"]["step"]["turn_id"].as_str().unwrap();
        let root = delivery.payload["parent"]["graph_id"].as_str().unwrap();
        let actual_paths = crate::agentic::review_evidence::collect_actual_nested_from_original_host_for_test(
            &fixture.store, &primary, &index, &events, &request.input, root).unwrap();
        assert_eq!(actual_paths, vec!["other.txt".to_string(), "target.txt".to_string()]);
        let mut incomplete = index.clone();
        incomplete.payload["child_read_acquisitions_complete"] = serde_json::json!(false);
        assert!(crate::agentic::review_evidence::collect_actual_nested_from_original_host_for_test(
            &fixture.store, &primary, &incomplete, &events, &request.input, root).is_err());
        let verify = |event: &DurableRuntimeEvent, session: &str| crate::agentic::review_evidence::verify_nested_read_for_test(
            &fixture.store, event, session, turn, Some(root), None);
        assert!(verify(delivery, session).unwrap());
        assert!(!verify(delivery, "foreign-session").unwrap());
        let mut bad = (*delivery).clone();
        bad.payload["parent"]["body"] = children[0]["body"].clone();
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["child"]["position"] = serde_json::json!(1);
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["source_input"] = serde_json::json!("{\"steps\":[]}");
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["child"]["body"] = children.iter().find(|child| child["position"] == 1).unwrap()["body"].clone();
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["child"]["body"]["source_sha256"] = serde_json::json!("sha256:forged");
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["delivered_extent"]["body_sha256"] = serde_json::json!("sha256:forged");
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.payload["transport"]["wire_body_sha256"] = serde_json::json!("");
        assert!(verify(&bad, session).is_err());
        let mut bad = (*delivery).clone();
        bad.actor = Some("foreign".into());
        assert!(!verify(&bad, session).unwrap());
        let services = fixture.services.as_ref().unwrap();
        let context = harness_contract::projection::ProjectionQueryContext {
            principal:"nested-test".into(), workspace_id:services.workspace_key().into(),
            session_scopes:vec![session.into()], mission_scopes:vec![],
            visibility_grants:vec![format!("workspace:{}", services.workspace_key()),"resource:*".into()],
            detail_scope:harness_contract::projection::ProjectionDetailScope::Full, authorization_revision:1,
        };
        let page = crate::execution_projection::read_evidence(services, root, &context, Default::default()).await.unwrap();
        assert!(!page.incomplete, "{page:?}");
        assert_eq!(page.reads.len(), 2);
        assert!(page.reads.iter().all(|read| read.body_delivered && read.tool_name == "read_file"));
    }

    #[tokio::test]
    async fn r1_read_delivery_native_complete_and_preview_keep_original_identity() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let large = (0..12_000)
            .map(|index| format!("source row {index:05}: exact retained native body\n"))
            .collect::<String>();
        for (body, omitted) in [
            ("alpha\r\nbeta\r\n", false),
            ("last line without newline", false),
            ("", false),
            (large.as_str(), true),
        ] {
            let fixture = NativeFixture::new(Some(body), "none").await;
            let (mut fixture, call) = fixture.run_root_host(false).await;
            let primary = fixture.acquisition(&call);
            let seal = decode_read_acquisition(&primary.payload[READ_ACQUISITION_FIELD]).unwrap();
            assert_eq!(seal.body.source_sha256, read_sha256(body.as_bytes()));
            assert_eq!(seal.body.source_bytes, body.len() as u64);
            let index = fixture
                .store
                .event_by_idempotency_key(&seal.index_stream, &seal.index_key)
                .unwrap()
                .unwrap();
            assert_eq!(index.transaction_id, primary.transaction_id);
            assert_eq!(
                index.payload[READ_ACQUISITION_FIELD],
                primary.payload[READ_ACQUISITION_FIELD]
            );
            let events = fixture.materials();
            assert_eq!(events.len(), 1);
            let material = decode_read_delivery(&events[0].payload).unwrap();
            assert!(matches!(
                material.recipient,
                ReadDeliveryRecipient::Root { identity: None, .. }
            ));
            assert_eq!(material.acquisition, seal);
            assert_eq!(material.acquisition_event_id, primary.event_id);
            assert_eq!(material.acquisition_cursor, primary.commit_cursor);
            assert_eq!(material.index_event_id, index.event_id);
            assert_eq!(material.body_omitted, omitted);
            assert_eq!(material.delivered_extent.is_none(), omitted);
            assert!(events[0].commit_cursor > primary.commit_cursor);
            fixture.assert_actual_wire(&material).await;
            assert_eq!(fixture.runtime.turn_model_observations().len(), 1);
            fixture.continue_model().await.unwrap();
            assert_eq!(fixture.runtime.turn_model_observations().len(), 1);
            assert!(fixture
                .materials()
                .iter()
                .all(|event| event.payload["acquisition_cursor"] == primary.commit_cursor));
        }
    }

    /// A read the model really received stays recorded when the frame the
    /// model produced afterwards is refused.
    ///
    /// The delivered read is a fact about what the model saw. A refused
    /// output frame ends the step early, and it must not un-read an input
    /// that was already delivered, or a reviewer that really did read the
    /// result looks like it never read it.
    #[tokio::test]
    async fn r1_observed_input_survives_a_refused_output_frame() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let fixture = NativeFixture::new(Some("original source\n"), "none").await;
        fixture.server.reject_output.store(true, Ordering::SeqCst);
        let (mut fixture, call) = fixture.run_root_host(false).await;

        // The read really executed and produced its own original receipt.
        let primary = fixture.acquisition(&call);
        assert!(!primary.payload[READ_ACQUISITION_FIELD].is_null());

        // The output frame that followed named a tool the request never
        // exposed, so Runtime refused it instead of running it.
        assert!(fixture.server.requests.lock().unwrap().len() >= 2);
        assert!(!fixture
            .runtime
            .tool_executor
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|request| request.tool_use_id == "native-refused-output"));

        // The delivered read is still recorded: one delivery material bound
        // to the original acquisition, and a model observation.
        let materials = fixture.materials();
        assert!(!materials.is_empty(), "the delivered read must stay recorded");
        let mut per_attempt = std::collections::BTreeMap::<
            String,
            std::collections::BTreeSet<String>,
        >::new();
        for event in &materials {
            let material = decode_read_delivery(&event.payload).unwrap();
            // Every delivery is bound to the original acquisition, never to a
            // re-published or a foreign one.
            assert_eq!(material.acquisition_event_id, primary.event_id);
            let ReadDeliveryTransport::NativeProviderContinuation { request_id, .. } =
                &material.transport
            else {
                panic!("unexpected delivery transport {:?}", material.transport);
            };
            // One attempt writes one delivery per observed invocation, so
            // settling the same observation again is idempotent.
            assert!(per_attempt
                .entry(request_id.clone())
                .or_default()
                .insert(material.tool_invocation_id.clone()));
        }
        assert!(!fixture.runtime.turn_model_observations().is_empty());
    }

    #[tokio::test]
    async fn r1_read_delivery_native_failed_wire_or_confirmation_cannot_promote() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        for (recipient, fault) in ["none", "task_graph"].into_iter().flat_map(|recipient| {
            [
                "packed",
                "outcome",
                "disconnect",
                "confirmation",
                "cas",
                "failed_read",
                "cancel",
            ]
            .into_iter()
            .map(move |fault| (recipient, fault))
        }) {
            let fixture = NativeFixture::new(
                (fault != "failed_read").then_some("original source\n"),
                recipient,
            )
            .await;
            let (mut fixture, call) = fixture.run_root_host(true).await;
            let primary = fixture.acquisition(&call);
            if fault != "failed_read" {
                decode_read_acquisition(&primary.payload[READ_ACQUISITION_FIELD]).unwrap();
            }
            let before = fixture.server.requests.lock().unwrap().len();
            let rejected_before = fixture.journal.rejected.load(Ordering::SeqCst);
            match fault {
                "packed" => fixture.journal.reject_packed.store(true, Ordering::SeqCst),
                "outcome" => fixture.journal.reject_outcome.store(true, Ordering::SeqCst),
                "disconnect" => fixture.server.disconnect.store(true, Ordering::SeqCst),
                "cancel" => fixture.runtime.cancellation_token.cancel(),
                _ => {}
            }
            let raced = Arc::new(AtomicUsize::new(0));
            let result = match fault {
                "confirmation" => {
                    super::super::context_plane::with_native_delivery_confirmation_failure(
                        fixture.continue_model(),
                    )
                    .await
                }
                "cas" => {
                    let store = fixture.store.clone();
                    let stream_id = format!("session:{}", fixture.runtime.session_id());
                    let raced = raced.clone();
                    let race: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
                        raced.fetch_add(1, Ordering::SeqCst);
                        store
                            .append(RuntimeEventInput {
                                stream_id: stream_id.clone(),
                                scope: RuntimeEventScope::Session,
                                kind: "test.native_confirmation_competing_append".into(),
                                status: None,
                                actor: None,
                                refs: Vec::new(),
                                payload: serde_json::json!({"competing_append":true}),
                            })
                            .unwrap();
                    });
                    super::super::context_plane::with_native_delivery_confirmation_source_race(
                        race,
                        fixture.continue_model(),
                    )
                    .await
                }
                _ => fixture.continue_model().await,
            };
            if matches!(fault, "failed_read" | "outcome") {
                result.unwrap();
                if fault == "failed_read" {
                    let primary = fixture.acquisition(&call);
                    assert!(primary.payload[READ_ACQUISITION_FIELD].is_null());
                } else {
                    assert!(fixture
                        .runtime
                        .native_wire_receipts
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|wire| !wire.results.is_empty() && wire.outcome_sequence.is_none()));
                    assert!(fixture.server.requests.lock().unwrap().len() > before);
                }
            } else {
                let error = result.expect_err(fault);
                if fault == "confirmation" {
                    assert!(
                        error
                            .to_string()
                            .contains("Runtime confirmation append failure"),
                        "{error}"
                    );
                }
                if fault == "cas" {
                    assert_eq!(raced.load(Ordering::SeqCst), 1);
                    assert!(
                        error
                            .to_string()
                            .contains("Native delivery confirmation unknown"),
                        "{error}"
                    );
                }
            }
            assert!(fixture.materials().is_empty(), "{fault}");
            assert!(
                fixture.runtime.turn_model_observations().is_empty(),
                "{fault}"
            );
            if matches!(fault, "packed" | "cancel") {
                assert_eq!(
                    fixture.server.requests.lock().unwrap().len(),
                    before,
                    "{fault}"
                );
            }
            if matches!(fault, "packed" | "outcome") {
                assert!(fixture.journal.rejected.load(Ordering::SeqCst) > rejected_before);
            }
            if matches!(fault, "confirmation" | "cas") {
                let events = fixture
                    .sessions
                    .session_domain_events_page(fixture.runtime.session_id(), 0, 4096)
                    .await
                    .unwrap();
                assert!(
                    events
                        .events
                        .iter()
                        .filter(|event| event.kind == "context.provider_attempt_outcome"
                            && event.payload["terminal_status"] == "completed")
                        .count()
                        >= 2
                );
                assert!(fixture.server.requests.lock().unwrap().len() >= 2);
                assert_eq!(
                    fixture
                        .runtime
                        .native_wire_receipts
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|wire| !wire.results.is_empty() && wire.outcome_sequence.is_some())
                        .count(),
                    1
                );
            }
        }
    }

    #[tokio::test]
    async fn r1_read_delivery_root_none_and_task_graph_use_original_owner() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let large = (0..12_000)
            .map(|index| format!("root row {index:05}: original unabridged body\n"))
            .collect::<String>();
        let bodies = [
            ("root original content\r\nsecond line\n", false),
            (large.as_str(), true),
        ];
        for (recipient, (body, omitted)) in ["none", "task_graph"]
            .into_iter()
            .flat_map(|recipient| bodies.into_iter().map(move |body| (recipient, body)))
        {
            let fixture = NativeFixture::new(Some(body), recipient).await;
            let (fixture, call) = fixture.run_root_host(false).await;
            let primary = fixture.acquisition(&call);
            let events = fixture.materials();
            assert_eq!(events.len(), 1, "{recipient}");
            let material = decode_read_delivery(&events[0].payload).unwrap();
            let ReadDeliveryRecipient::Root { owner, identity } = &material.recipient else {
                panic!("Root must not acquire an imaginary AgentRun")
            };
            assert_eq!(material.acquisition.graph_id, material.execution_id);
            let index = fixture
                .store
                .event_by_idempotency_key(
                    &material.acquisition.index_stream,
                    &material.acquisition.index_key,
                )
                .unwrap()
                .unwrap();
            assert_eq!(index.transaction_id, primary.transaction_id);
            assert_eq!(
                index.payload[READ_ACQUISITION_FIELD],
                primary.payload[READ_ACQUISITION_FIELD]
            );
            assert_eq!(identity.is_none(), recipient == "none");
            if let Some(identity) = identity {
                assert_eq!(
                    identity.kind(),
                    harness_contract::execution::ExecutionIdentityKind::TaskGraph
                );
                assert!(identity.node_id().is_none());
                assert!(identity.agent_run_id().is_none());
                assert_eq!(
                    identity,
                    fixture.runtime.execution_identity.as_ref().unwrap()
                );
            }
            let original = fixture
                .store
                .list_stream_after(
                    &owner.stream_id,
                    owner.sequence - 1,
                    owner.sequence,
                    1,
                    4 * 1024 * 1024,
                )
                .unwrap();
            assert_eq!(original.len(), 1);
            let original = &original[0];
            assert_eq!(original.kind, "tool.invocation.completed");
            assert_eq!(original.event_id, owner.event_id);
            assert_eq!(read_payload_digest(&original.payload), owner.payload_digest);
            assert_eq!(original.activity_binding().as_ref(), Some(&owner.binding));
            assert_eq!(
                owner.binding.parent_activity_id.as_deref(),
                Some(fixture.owner.activity_id.as_str())
            );
            assert!(owner.binding.agent_run_id.is_none());
            assert!(owner.binding.node_id.is_none());
            assert_eq!(
                owner.binding.tool_call_id.as_deref(),
                Some(call.id.as_str())
            );
            assert!(original.commit_cursor > primary.commit_cursor);
            assert!(events[0].commit_cursor > original.commit_cursor);
            assert_eq!(
                material.acquisition.step,
                call.binding.as_ref().unwrap().snapshot.identity
            );
            assert_eq!(material.acquisition_event_id, primary.event_id);
            assert_eq!(material.body_omitted, omitted);
            assert_eq!(material.delivered_extent.is_none(), omitted);
            assert_eq!(
                material.acquisition.body.source_sha256,
                read_sha256(body.as_bytes())
            );
            assert_eq!(material.acquisition.body.source_bytes, body.len() as u64);
            assert!(matches!(
                material.acquisition.body.extent,
                ReadExtent::FileLines {
                    source_omitted: false,
                    ..
                }
            ));
            fixture.assert_actual_wire(&material).await;
            assert_eq!(fixture.runtime.turn_model_observations().len(), 1);
        }
    }

    #[tokio::test]
    async fn r1_read_delivery_root_foreign_owner_step_or_ticket_cannot_promote() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        for recipient in ["none", "task_graph"] {
            for foreign in [
                "owner_event",
                "owner_digest",
                "owner_root",
                "owner_tool",
                "owner_session",
                "source_step",
                "source_turn",
                "source_tool",
                "source_root",
                "ticket",
                "live_owner",
                "live_revision",
                "live_node",
                "live_activity",
                "missing_source",
            ] {
                let fixture = NativeFixture::new(Some("root target content\n"), recipient).await;
                let (mut fixture, call) = fixture.run_root_host(true).await;
                let primary = fixture.acquisition(&call);
                decode_read_acquisition(&primary.payload[READ_ACQUISITION_FIELD]).unwrap();
                let key = call.binding.as_ref().unwrap().effect_idempotency_key();
                let saved_receipts = fixture
                    .runtime
                    .turn_generated_model_receipts
                    .lock()
                    .unwrap()
                    .clone();
                let saved_sources = fixture
                    .runtime
                    .turn_native_tool_sources
                    .lock()
                    .unwrap()
                    .clone();
                let saved_owners = fixture
                    .runtime
                    .turn_native_root_owners
                    .lock()
                    .unwrap()
                    .clone();
                let saved_live_owner = fixture.owner.clone();
                match foreign {
                    "owner_event" | "owner_digest" | "owner_root" | "owner_tool"
                    | "owner_session" => {
                        let mut owners = fixture.runtime.turn_native_root_owners.lock().unwrap();
                        let owner = owners
                            .get_mut(&key)
                            .expect("original Runtime owner was captured");
                        match foreign {
                            "owner_event" => owner.event_id = primary.event_id.clone(),
                            "owner_digest" => owner.payload_digest = "0".repeat(64),
                            "owner_root" => owner.binding.root_execution_id = "foreign-root".into(),
                            "owner_session" => owner.binding.session_id = "foreign-session".into(),
                            _ => owner.binding.tool_call_id = Some("foreign-read".into()),
                        }
                    }
                    "source_step" | "source_turn" | "source_tool" | "source_root" | "ticket" => {
                        let mut sources = fixture.runtime.turn_native_tool_sources.lock().unwrap();
                        let source = sources.get_mut(&call.id).unwrap();
                        let binding = source.call.binding.as_mut().unwrap();
                        match foreign {
                            "source_step" => {
                                Arc::make_mut(&mut binding.snapshot).identity.step_id =
                                    "foreign-step".into()
                            }
                            "source_turn" => {
                                Arc::make_mut(&mut binding.snapshot).identity.turn_id =
                                    "foreign-turn".into()
                            }
                            "source_tool" => source.call.name = "read_ref".into(),
                            "source_root" => source.execution.execution_id = "foreign-root".into(),
                            _ => binding.origin.call_id = "foreign-ticket-call".into(),
                        }
                        let changed = source.clone();
                        fixture
                            .runtime
                            .turn_generated_model_receipts
                            .lock()
                            .unwrap()[0]
                            .source = Some(changed);
                    }
                    "live_owner" => fixture.owner.fence += 1,
                    "live_revision" => fixture.owner.revision += 1,
                    "live_node" => fixture.owner.node_id = Some("foreign-live-node".into()),
                    "live_activity" => fixture.owner.activity_id = "foreign-live-activity".into(),
                    "missing_source" => {
                        fixture
                            .runtime
                            .turn_generated_model_receipts
                            .lock()
                            .unwrap()[0]
                            .source = None
                    }
                    _ => unreachable!(),
                }
                fixture.continue_model().await.unwrap();
                assert!(fixture.materials().is_empty(), "{recipient}/{foreign}");
                assert!(
                    fixture.runtime.turn_model_observations().is_empty(),
                    "{recipient}/{foreign}"
                );
                assert!(fixture.server.requests.lock().unwrap().len() >= 2);
                assert_eq!(fixture.acquisition(&call).payload, primary.payload);
                assert!(
                    fixture
                        .runtime
                        .native_wire_receipts
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|wire| !wire.results.is_empty() && wire.outcome_sequence.is_some()),
                    "actual continuation must succeed before identity rejection"
                );
                *fixture
                    .runtime
                    .turn_generated_model_receipts
                    .lock()
                    .unwrap() = saved_receipts;
                *fixture.runtime.turn_native_tool_sources.lock().unwrap() = saved_sources;
                *fixture.runtime.turn_native_root_owners.lock().unwrap() = saved_owners;
                fixture.owner = saved_live_owner;
                fixture.continue_model().await.unwrap();
                let accepted = fixture.materials();
                assert_eq!(accepted.len(), 1, "positive control {recipient}/{foreign}");
                let material = decode_read_delivery(&accepted[0].payload).unwrap();
                assert_eq!(material.acquisition_cursor, primary.commit_cursor);
                assert_eq!(fixture.runtime.turn_model_observations().len(), 1);
                fixture.assert_actual_wire(&material).await;
            }
        }
    }

    #[tokio::test]
    async fn own836_native_writer_rejects_foreign_original_node_and_input() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let fixture = NativeFixture::new(Some("actual native source\n"), "task_graph").await;
        let (fixture, call) = fixture.run_root_host(false).await;
        let _scope = fixture
            .bus
            .enter_execution_with_activity(fixture.execution.clone(), Some(fixture.owner.clone()));
        let input_hash = crate::tool_invocation::stable_hash(&call.input);
        let owner = fixture
            .runtime
            .native_tool_activity_owner(&call.id, &call.name, &input_hash)
            .unwrap()
            .expect("real model-node tool source");
        assert_eq!(
            owner.parent_activity_id.as_deref(),
            Some(fixture.owner.activity_id.as_str())
        );
        assert!(fixture
            .runtime
            .native_tool_activity_owner(&call.id, &call.name, "foreign-input")
            .is_err());
        let original = fixture
            .runtime
            .turn_native_tool_sources
            .lock()
            .unwrap()
            .get(&call.id)
            .unwrap()
            .clone();
        fixture
            .runtime
            .turn_native_tool_sources
            .lock()
            .unwrap()
            .get_mut(&call.id)
            .unwrap()
            .activity_owner
            .as_mut()
            .unwrap()
            .node_id = Some("foreign-original-node".into());
        assert!(fixture
            .runtime
            .native_tool_activity_owner(&call.id, &call.name, &input_hash)
            .is_err());
        fixture
            .runtime
            .turn_native_tool_sources
            .lock()
            .unwrap()
            .insert(call.id.clone(), original);
        assert_eq!(
            fixture
                .runtime
                .native_tool_activity_owner(&call.id, &call.name, &input_hash)
                .unwrap(),
            Some(owner)
        );
    }
}

#[test]
fn durable_strategy_quantity_refuses_invalid_latest_instead_of_falling_back() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let runtime = ConversationRuntime::new(
        Session::new(),
        MockApi,
        CollaborationAvailableExecutor,
        PermissionPolicy::new(PermissionMode::WorkspaceWrite),
        vec!["system".into()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(&store));
    runtime
        .begin_turn_strategy("quantity-turn", "启动至少三个团队")
        .unwrap();
    runtime
        .bind_turn_strategy_execution("quantity-turn", "quantity-root")
        .unwrap();
    let recovered = runtime
        .recover_turn_strategy_identity("quantity-turn", "quantity-root")
        .unwrap()
        .unwrap();
    let minimum = recovered.collaboration_obligation.unwrap();
    assert_eq!(
        (
            minimum.minimum_team_count,
            minimum.exact_team_count,
            minimum.team_count_contract_version
        ),
        (3, None, 1)
    );
    let event = store
        .list_stream(&format!("session:{}", runtime.session_id()))
        .unwrap()
        .into_iter()
        .rev()
        .find(|e| e.kind == "runtime.strategy.selected")
        .unwrap();
    let mut payload = event.payload;
    payload["collaboration_obligation"]["team_count_contract_version"] = serde_json::json!(0);
    store
        .append(crate::RuntimeEventInput {
            stream_id: event.stream_id,
            scope: event.scope,
            kind: event.kind,
            status: event.status,
            actor: event.actor,
            refs: event.refs,
            payload,
        })
        .unwrap();
    assert!(runtime
        .recover_turn_strategy_identity("quantity-turn", "quantity-root")
        .is_err());
    assert!(runtime
        .bind_turn_strategy_execution("quantity-turn", "quantity-root")
        .is_err());
}

#[test]
fn empty_final_completion_is_failure_with_original_usage_not_a_goal_candidate() {
    let usage = TokenUsage {
        input_tokens: 19945,
        output_tokens: 0,
        ..Default::default()
    };
    let make =
        |text: &str, blocks: Vec<ContentBlock>, text_only_response: bool| super::ModelStepResult {
            input_interpretation: None,
            intent: ModelStepIntent::FinalAnswer {
                text: text.to_owned(),
            },
            assistant_message: ConversationMessage {
                role: MessageRole::Assistant,
                blocks,
                usage: Some(usage),
            },
            preflight_tool_results: vec![],
            usage,
            model: Some("actual-model".into()),
            models_used: vec!["actual-model".into()],
            first_token_latency_ms: None,
            active_stream_duration_ms: None,
            wall_duration_ms: 1,
            early_tool_receipts: vec![],
            early_tool_deferrals: vec![],
            response_completed_at_ms: 1,
            text_only_response,
        };
    // Original empty carrier and actual known usage, not a reconstructed HTTP response.
    let empty = make(
        "",
        vec![ContentBlock::Text {
            text: String::new(),
        }],
        false,
    );
    let error = empty
        .into_host_result()
        .expect_err("an empty completion is not a final answer");
    assert_eq!(
        error.to_string(),
        "provider completed without a visible final answer or tool calls"
    );
    assert_eq!(error.provider_usage(), Some(usage));
    assert!(!error.is_provider_tool_protocol_failure());
    assert!(error.effect_receipts().is_empty());
    assert!(make("  ", vec![], false).into_host_result().is_err());
    let mut unknown = make("", vec![], false);
    unknown.assistant_message.usage = None;
    assert_eq!(
        unknown.into_host_result().unwrap_err().provider_usage(),
        None
    );
    let call = ModelToolCall {
        binding: None,
        id: "already-receipted".into(),
        name: "read_file".into(),
        input: "{}".into(),
        depends_on: vec![],
    };
    let receipt = EarlyToolExecutionReceipt {
        original_source: None,
        lifecycle: crate::conversation::ToolLifecycleReuse::Unknown,
        call: call.clone(),
        ready_at_ms: 1,
        started_at_ms: 2,
        completed_at_ms: 3,
        outcome: crate::RuntimeToolExecutionOutcome {
            tool_use_id: call.id.clone(),
            tool_name: call.name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: crate::ToolSafetyCategory::ReadOnly,
            output: Some("independent retained body".into()),
            error: None,
            evidence_ref: "original-receipt".into(),
            observed_evidence: vec![],
            pending_agent_batch: None,
        },
    };
    let mut after_effect = make("", vec![], false);
    after_effect.early_tool_receipts.push(receipt.clone());
    assert_eq!(
        after_effect
            .into_host_result()
            .unwrap_err()
            .effect_receipts(),
        &[receipt]
    );

    let visible = make("independent actual answer", vec![], false);
    assert_eq!(visible.clone().into_host_result().unwrap(), visible);
    let reasoning = make(
        "",
        vec![ContentBlock::Thinking {
            thinking: "retained evidence needs another step".into(),
            signature: None,
        }],
        false,
    );
    assert_eq!(reasoning.clone().into_host_result().unwrap(), reasoning);
    let exhausted_terminal = make(
        "",
        vec![ContentBlock::ReasoningSummary {
            text: "still no visible answer".into(),
        }],
        true,
    );
    assert!(
        exhausted_terminal.into_host_result().is_err(),
        "a zero-tool terminal cannot be a reasoning continuation"
    );
    let mut tools = make("", vec![], false);
    tools.intent = ModelStepIntent::ToolCalls {
        calls: vec![ModelToolCall {
            binding: None,
            id: "real-call".into(),
            name: "read_file".into(),
            input: "{}".into(),
            depends_on: vec![],
        }],
    };
    assert_eq!(
        tools.clone().into_host_result().unwrap(),
        tools,
        "empty prose must not discard a tool call"
    );
    let mut interpretation = make("", vec![], false);
    interpretation.input_interpretation = Some(Err("strict interpretation rejected".into()));
    assert_eq!(
        interpretation.clone().into_host_result().unwrap(),
        interpretation,
        "the dedicated control owner retains its own rejection"
    );
}
