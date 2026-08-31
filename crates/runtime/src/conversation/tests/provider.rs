    use super::{
        apply_explicit_team_requirement, apply_named_e2e_strategy_fixture,
        build_cc_memory_config_with_budget, canonicalize_model_tool_names,
        classify_model_step_intent, consume_provider_stream, conversation_message_text,
        current_turn_messages, deterministic_checkpoint_id, enforce_explicit_team_requirement,
        eval_override_selection, image_user_message_from_path, is_append_only_projection,
        is_runtime_team_orchestration_call, memory_project_id_for_session, prepared_vision_payload,
        preview_chars, provider_retry_is_fenced, provider_transport_policy, rate_per_second,
        required_team_orchestration_call, revalidate_context_binding,
        runtime_team_orchestration_count, turn_strategy_event_kind_allowed,
        unexposed_model_tool_names, vision_tool_model_receipt, vision_user_message, ApiClient,
        ApiRequest, AssistantEvent, AssistantItemKind, CancellationToken, CognitiveContextManager,
        ConversationRuntime, EarlyToolCandidate, EarlyToolDispatchFuture, EarlyToolDispatchResult,
        EarlyToolDispatcher, EarlyToolExecutionReceipt, EvaluationProviderTokenLeaseRegistry,
        ModelStepIntent, ModelStepToolPlan, ModelStreamReducer, ModelToolCall,
        ProviderContextInventory, ProviderTokenReservationSet, RuntimeError, StaticToolExecutor,
        ToolExposureState, ToolModelDeliveryRequirement, TurnStablePrefixMetrics,
        TurnToolExposureMetrics,
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
    use harness_contract::team::{FocusPartitionPlan, FocusPartitionSlot};
    use model_protocol::usage::TokenUsage;
    use std::collections::BTreeSet;
    use std::fs;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::time::Duration;

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
                harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID.to_string(),
            ]
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
        let session_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
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
            crate::ArtifactStore::sqlite(temporary.path(), crate::ArtifactStoreConfig::default())
                .unwrap(),
        );
        let writer = super::SessionProviderWireEvidenceWriter {
            artifacts: Arc::clone(&artifacts),
            session_port: crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(
                &session_store,
            )),
        };
        let context = crate::ProviderRequestEvidenceContext {
            session_id: session_id.clone(),
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
        };

        crate::ProviderWireEvidenceWriter::persist(&writer, &context, evidence)
            .await
            .unwrap();

        let events = session_store
            .session_domain_events_page(&session_id, 0, 10)
            .await
            .unwrap();
        let packed = events
            .events
            .iter()
            .find(|event| event.kind == "context.provider_request_packed")
            .expect("provider request evidence event");
        assert_eq!(packed.payload["schema_version"], 2);
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
        let exact = ConversationRuntime::<MockApi, StaticToolExecutor>::apply_exact_evidence_delivery_budget(
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
            exact.tool_result_budget.max_total_tokens
                > standard.tool_result_budget.max_total_tokens
        );
        assert_eq!(
            exact.tool_result_budget.per_tool_max_tokens,
            exact.tool_result_budget.max_total_tokens
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
        );
        runtime
            .record_generated_model_receipt(
                "tool-call-1",
                "read_file",
                &requirement,
                &raw_ref,
                &receipt,
                false,
            )
            .expect("generated receipt");
        assert!(runtime.turn_model_observations().is_empty());

        let request = ApiRequest {
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
            provider_evidence_context: None,
        };
        let candidates = runtime
            .packed_model_observation_candidates(&request, 2, 1)
            .expect("matching packed request");
        assert_eq!(candidates.len(), 1);
        assert!(runtime.turn_model_observations().is_empty());
        runtime
            .confirm_model_observations(candidates, "qwen3.8-max-effective")
            .expect("valid response commit promotion");
        let promoted = runtime.turn_model_observations();
        assert_eq!(promoted.len(), 1);
        assert_eq!(promoted[0].provider_invocation_id, "tool-call-1");
        assert_eq!(promoted[0].model, "qwen3.8-max-effective");
        assert!(promoted[0].complete);

        let mut corrupted = request;
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
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
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
        runtime.begin_turn_runtime_epoch();
        let raw_ref =
            harness_contract::reality::EvidenceRef::observed("tool", "runtime-prefetch-raw-1");
        let requirement =
            ToolModelDeliveryRequirement::exact(vec!["required-agent-read".to_string()]);
        let output = r#"{"content":"complete runtime-prefetched evidence","truncated":false}"#;
        let receipt =
            runtime.tool_model_receipt("read_file", output, false, &raw_ref, None, &requirement);
        runtime
            .record_generated_model_receipt(
                "runtime-focus-verify-0-0",
                "read_file",
                &requirement,
                &raw_ref,
                &receipt,
                false,
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
        assert_eq!(observations.len(), 1);
        assert_eq!(
            observations[0].provider_invocation_id,
            "runtime-focus-verify-0-0"
        );
        assert_eq!(observations[0].model, "qwen3.8-max");
        assert!(observations[0].complete);
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

        runtime.begin_turn_runtime_epoch();
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
    use storage::StorageRegistry;

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
    fn explicit_collaboration_requirement_becomes_a_runtime_team_tool_call() {
        let objective = "这是复杂架构审查，必须实际启动一个多 Agent 协作团队完成分析。";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "我会开始分析。".to_string(),
            },
        );
        let ModelStepIntent::FinalAnswer { .. } = intent else {
            panic!(
                "the first step belongs to the model; the final-answer acceptance gate re-prompts later"
            );
        };
    }

    #[test]
    fn chinese_launch_team_wording_is_enforced_as_an_execution_requirement() {
        let objective = "发起一个团队，生成公开技术标准的全面深度调研报告";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "无法组队".to_string(),
            },
        );

        let ModelStepIntent::FinalAnswer { .. } = intent else {
            panic!("first-step final answers stay model-owned; the gate re-prompts later");
        };
    }

    #[test]
    fn sequential_team_artifact_request_materializes_a_write_capable_followup_team() {
        let objective = "用一个团队调研公开技术标准，然后另一个团队负责生成一套 HTML 研究报告网站";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "调研结束。".to_string(),
            },
        );

        let ModelStepIntent::FinalAnswer { .. } = intent else {
            panic!("first-step final answers stay model-owned; the gate re-prompts later");
        };
    }

    #[test]
    fn research_teams_leave_explicit_agent_report_delivery_to_parent() {
        let objective = "请启动2个研究团队，开展各个层面的今年AI发展趋势调研，然后使用一个智能体进行信息的统一收集、整理，形成一个专业研究报告（html版），放到独立个文件夹下。";
        let call = required_team_orchestration_call(objective);
        let input: serde_json::Value = serde_json::from_str(&call.input).unwrap();

        assert_eq!(input["proposal"]["nodes"].as_array().unwrap().len(), 2);
        assert!(input["proposal"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|node| node["template"] == "cowd/external-research-synthesis"));
        assert_eq!(input["constraints"]["requires_write"], false);
        assert_eq!(
            input["proposal"]["completion"]["required_artifact_kinds"],
            serde_json::json!(["terminal_synthesis"])
        );
        assert!(
            build_runtime_execution_decision(objective, None)
                .strategy
                .understanding
                .requires_write
        );
    }

    #[test]
    fn model_tool_calls_are_bounded_by_the_current_exposure_lease() {
        let calls = vec![
            ModelToolCall {
                id: "read".to_string(),
                name: "read_file".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "hidden".to_string(),
                name: "shell".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
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
                id: "search".to_string(),
                name: "web_search".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
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
        let prompt =
            "must start a Team for runtime gateway frontend [cowd-e2e:explicit-team-negative]";
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
    fn model_team_proposal_is_visible_to_runtime_retargeting() {
        let call = required_team_orchestration_call("review");
        assert!(is_runtime_team_orchestration_call(&call));
    }

    #[test]
    fn explicit_team_requirement_overrides_a_non_collaboration_strategy_hint() {
        let objective = "先自主选择并实际启动合适的协作团队，分别完成三个独立审查。";
        let decision = build_runtime_execution_decision(objective, None);

        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "我会开始分析。".to_string(),
            },
        );

        let ModelStepIntent::FinalAnswer { .. } = intent else {
            panic!("first-step final answers stay model-owned; the gate re-prompts later");
        };
    }

    #[test]
    fn explicit_team_requirement_does_not_preempt_first_step_model_tool_calls() {
        let objective = "必须实际启动协作团队，再分析这些模块。";
        let decision = build_runtime_execution_decision(objective, None);
        let classified = classify_model_step_intent(
            String::new(),
            vec![ModelToolCall {
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
        assert!(!calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn ordinary_tool_names_never_create_runtime_control_intents() {
        for name in [
            "team_board",
            "agent_status",
            "permission_report",
            "replan_index",
        ] {
            let intent = classify_model_step_intent(
                String::new(),
                vec![ModelToolCall {
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
    fn team_orchestration_tool_batch_keeps_the_collaboration_strategy() {
        let calls = vec![required_team_orchestration_call("必须实际启动团队")];
        assert!(calls.iter().any(is_runtime_team_orchestration_call));
    }

    #[test]
    fn required_team_orchestration_uses_a_published_builtin_template() {
        let call = required_team_orchestration_call("必须实际启动团队");
        let replay = required_team_orchestration_call("必须实际启动团队");
        assert_eq!(call.name, "runtime_orchestrate");
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let replay_input = serde_json::from_str::<serde_json::Value>(&replay.input)
            .expect("replayed runtime orchestration input is JSON");
        assert_eq!(
            input["proposal"]["mutation_id"], replay_input["proposal"]["mutation_id"],
            "the same semantic requirement must retry with one mutation identity"
        );
        assert_eq!(
            input["proposal"]["nodes"][0]["template"],
            serde_json::json!("cowd/parallel-research-synthesis")
        );
    }

    #[test]
    fn explicit_two_team_requirement_compiles_two_independent_read_teams() {
        let call = required_team_orchestration_call("启动两个研究团队并行调研本地文件并用中文汇报");
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let nodes = input["proposal"]["nodes"]
            .as_array()
            .expect("semantic Team nodes");
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|node| {
            node["recipe"] == "team" && node["depends_on"].as_array().is_some_and(Vec::is_empty)
        }));
        assert!(nodes
            .iter()
            .all(|node| node["template"] == "cowd/parallel-research-synthesis"));
        assert!(nodes.iter().all(|node| {
            node["evidence_contract"] == serde_json::json!(["summary", "evidence", "unresolved"])
        }));
        assert_eq!(
            input["proposal"]["completion"]["required_node_ids"]
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        assert_eq!(runtime_team_orchestration_count(&call), 2);
    }

    #[test]
    fn two_domain_teams_remain_parallel_when_parent_owns_the_final_html() {
        let call = required_team_orchestration_call(
            "请启动两个团队，一个业务团队和一个技术团队，两个团队研讨后形成统一的 HTML 方案并落盘",
        );
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let nodes = input["proposal"]["nodes"]
            .as_array()
            .expect("semantic Team nodes");
        assert_eq!(nodes.len(), 2);
        assert!(nodes.iter().all(|node| {
            node["template"] == "cowd/parallel-research-synthesis"
                && node["depends_on"].as_array().is_some_and(Vec::is_empty)
        }));
        assert_eq!(input["constraints"]["requires_write"], false);
        assert_eq!(
            input["proposal"]["completion"]["required_artifact_kinds"],
            serde_json::json!(["terminal_synthesis"]),
        );
    }

    #[test]
    fn ordinal_group_constraint_materializes_exactly_three_teams() {
        let objective = "第一组研究 Runtime，第二组审查 Gateway，第三组汇总结论";
        let decision = build_runtime_execution_decision(objective, None);
        let intent = enforce_explicit_team_requirement(
            objective,
            true,
            &decision,
            ModelStepIntent::FinalAnswer {
                text: "premature".to_string(),
            },
        );
        let ModelStepIntent::FinalAnswer { .. } = intent else {
            panic!("first-step final answers stay model-owned; the gate re-prompts later");
        };
    }

    #[test]
    fn mixed_language_three_team_requirement_compiles_parallel_research_then_writer() {
        let call = required_team_orchestration_call(
            "请使用恰好3个Team完成任务，前两个并行研究，第三个生成并写入HTML报告文件",
        );
        let input = serde_json::from_str::<serde_json::Value>(&call.input)
            .expect("runtime orchestration input is JSON");
        let nodes = input["proposal"]["nodes"]
            .as_array()
            .expect("semantic Team nodes");
        assert_eq!(nodes.len(), 3);
        assert!(nodes[..2]
            .iter()
            .all(|node| node["depends_on"].as_array().is_some_and(Vec::is_empty)));
        assert_eq!(
            nodes[2]["depends_on"],
            serde_json::json!(["explicit-team-1", "explicit-team-2"]),
        );
        assert_eq!(
            nodes[2]["output_artifacts"],
            serde_json::json!(["workspace_change", "terminal_synthesis"]),
        );
        assert!(nodes[..2]
            .iter()
            .all(|node| node["template"] == "cowd/parallel-research-synthesis"));
        assert!(nodes[..2].iter().all(|node| {
            node["evidence_contract"] == serde_json::json!(["summary", "evidence", "unresolved"])
        }));
        assert_eq!(nodes[2]["template"], "cowd/execute-review");
        assert_eq!(
            nodes[2]["evidence_contract"],
            serde_json::json!(["implementation", "source_verification", "evidence", "risks"])
        );
        assert!(nodes[2]["evidence_contract"]
            .as_array()
            .is_some_and(|criteria| criteria.iter().all(|criterion| criterion != "plan")));
        assert_eq!(runtime_team_orchestration_count(&call), 3);
    }

    #[test]
    fn sequential_followup_team_language_also_compiles_two_team_entities() {
        let call = required_team_orchestration_call(
            "一个团队负责调研，另一个团队负责独立复核，最后给出结论",
        );
        assert_eq!(runtime_team_orchestration_count(&call), 2);
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
            prompt: PromptAssembly::new(vec!["system".to_string()]),
            messages: vec![ConversationMessage::user_text("status".to_string())].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 32_768, 4_096, 128, 256, 0,
            ),
            provider_evidence_context: None,
        };
        let large = ApiRequest {
            prompt: PromptAssembly::new(vec!["system".repeat(5_000)]),
            messages: vec![ConversationMessage::user_text("evidence".repeat(10_000))].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 1_000_000, 32_000, 128, 256, 0,
            ),
            provider_evidence_context: None,
        };

        assert!(
            provider_transport_policy(1_000_000, &large).idle_timeout
                > provider_transport_policy(32_768, &small).idle_timeout
        );
    }

    fn token_reservation_request() -> ApiRequest {
        ApiRequest {
            prompt: PromptAssembly::new(vec!["system".to_string()]),
            messages: vec![ConversationMessage::user_text("bounded".to_string())].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: false,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 1_000, 100, 20, 5, 10,
            ),
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
            Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()),
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
            Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()),
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
            ProviderTokenReservationSet::acquire(Some(&small), None, "test", &mut rejected)
                .is_err()
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
    fn deep_live_scenario_token_lease_has_a_bounded_non_truncating_capacity() {
        let registry = Arc::new(EvaluationProviderTokenLeaseRegistry::default());
        let guard = registry
            .install("session-large-live", "eval-large-live", 8_000_000)
            .expect("large collaboration budget must fit the governed hard ceiling");
        assert_eq!(guard.snapshot().unwrap().limit, 8_000_000);
        assert!(registry
            .install(
                "session-over-limit",
                "eval-over-limit",
                crate::conversation::MAX_EVALUATION_PROVIDER_TOKEN_LEASE + 1,
            )
            .is_err());
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
        let output = super::provider_output_budget_hint(
            "custom-model-with-generic-cap",
            context_window,
            None,
        );
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
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
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
            .claim_session_runtime_outbox("fence-worker", now, 60_000, 1)
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
        .unwrap();
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
        #[derive(Clone)]
        struct CountingApi(Arc<AtomicUsize>);
        impl ApiClient for CountingApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
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
        .with_model_context_window(128_000);
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
            model_protocol::provider_failure::ProviderFailureScope::Account
        );
        assert_eq!(
            same_account_requests.lock().unwrap().as_slice(),
            &["primary".to_string()]
        );

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
        .with_model_context_window(128_000);
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
        runtime.begin_turn_runtime_epoch();
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
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
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
            .execute_clean_terminal_synthesis("give a concise answer", "checked evidence")
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
        let tmp = tempfile::tempdir().expect("temp memory root");
        let manager = Arc::new(
            CognitiveContextManager::new(memory::config::MemoryConfig {
                store: memory::config::StoreConfig {
                    sqlite_path: tmp.path().join("memory.sqlite"),
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
        let store =
            Arc::new(session::UnifiedSessionStore::open_in_memory().expect("session store"));
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
        let mut runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_memory_manager(manager)
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );
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
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
    fn explicit_root_collaboration_contract_pins_the_admitted_lease_before_control_plane() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            CollaborationAvailableExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        let admitted = runtime
            .begin_turn_strategy(
                "root-collaboration-contract",
                "启动两个协作团队分析并解决这个问题",
            )
            .expect("admit strategy");
        runtime
            .set_turn_strategy_focus_partitions(Vec::new(), 2)
            .expect("freeze explicit collaboration obligation");
        runtime
            .bind_turn_strategy_execution("root-collaboration-contract", "root-graph")
            .expect("bind strategy graph");

        let pinned = runtime
            .require_active_turn_collaboration_control_plane(2)
            .expect("pin root collaboration contract");

        assert_eq!(pinned.lease.lease_id, admitted.decision_lease);
        assert_eq!(
            pinned.pattern(),
            harness_contract::core::ExecutionPattern::Collaborate
        );
        assert_eq!(
            runtime
                .active_turn_strategy()
                .expect("active strategy")
                .selected_candidate,
            harness_contract::strategy::ExecutionCandidateKind::Team
        );
        let selected_events = store
            .list_stream(&format!("session:{}", runtime.session_id()))
            .expect("strategy events")
            .into_iter()
            .filter(|event| event.kind == "runtime.strategy.selected")
            .collect::<Vec<_>>();
        assert!(
            selected_events.iter().any(|event| {
                event
                    .payload
                    .get("reason")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|reason| reason.contains("explicit root collaboration contract"))
            }),
            "the forced runtime invariant must be durable"
        );
    }

    #[test]
    fn evidence_strategy_revises_to_explicitly_approved_delivery_without_changing_lease() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new()
                .register("web_search", |_| Ok("external evidence".to_string()))
                .register("write_file", |_| Ok("written".to_string())),
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
            id: "write".to_string(),
            name: "write_file".to_string(),
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

    #[tokio::test]
    async fn parallel_network_tool_batch_is_admitted_by_the_retargeted_strategy_lease() {
        let executions = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&executions);
        let event_store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let bus = CowdEventBus::new();
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: "parallel-network-execution".to_string(),
            session_id: "parallel-network-session".to_string(),
            turn_id: "parallel-network-turn".to_string(),
        });
        let mut receiver = bus.subscribe();
        let runtime = ConversationRuntime::new(
            Session::new(),
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

    #[test]
    fn canonical_outcome_covers_direct_and_parallel_tool_turns_without_graph_ref() {
        for candidate in [
            harness_contract::strategy::ExecutionCandidateKind::Direct,
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools,
        ] {
            let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
                        tool_calls: u64::from(candidate
                            == harness_contract::strategy::ExecutionCandidateKind::ParallelTools),
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
        let cases = [
            (
                crate::execution_core::TurnStrategyDecisionStatus::Failed,
                0,
                "failed",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::Cancelled,
                0,
                "cancelled",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::EarlyStopped,
                0,
                "partial_failure",
            ),
            (
                crate::execution_core::TurnStrategyDecisionStatus::Completed,
                1,
                "partial_failure",
            ),
        ];
        for (status, failed_tool_calls, expected_class) in cases {
            let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
                        terminal_reason: expected_class.to_string(),
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
    fn high_overlap_publishes_downgrade_with_visible_reason() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
            .begin_turn_strategy("overlap-turn", "必须启动 Team 分别审查三个独立域并综合")
            .expect("admit team strategy");
        runtime
            .bind_turn_strategy_execution("overlap-turn", "overlap-graph")
            .expect("bind strategy graph");
        let selected = runtime.active_turn_strategy().expect("selected state");

        runtime
            .downgrade_turn_strategy(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                "measured evidence overlap 9100 bp exceeded the 800 bp Team budget; continue with one owner",
            )
            .expect("downgrade must be durable");

        let events = store
            .list_stream(&format!("session:{}", runtime.session_id()))
            .expect("strategy events");
        let downgraded = events
            .iter()
            .find(|event| event.kind == "runtime.strategy.downgraded")
            .expect("overlap downgrade event");
        assert!(downgraded.sequence > 0);
        assert_eq!(
            downgraded.payload["decision_id"].as_str(),
            Some(selected.decision_id.as_str())
        );
        assert!(
            downgraded.payload["decision_revision"]
                .as_u64()
                .expect("downgrade revision")
                > selected.revision
        );
        assert_eq!(downgraded.payload["selected_candidate"], "direct");
        assert!(downgraded.payload["collaboration_obligation"].is_null());
        assert!(downgraded.payload["reason"]
            .as_str()
            .expect("visible reason")
            .contains("overlap 9100 bp"));
    }

    #[test]
    fn provider_constraint_publishes_monotonic_downgrade_and_retains_scope() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
            .begin_turn_strategy("provider-turn", "必须启动 Team 分别审查三个独立域并综合")
            .expect("admit team strategy");
        runtime
            .bind_turn_strategy_execution("provider-turn", "provider-graph")
            .expect("bind strategy graph");
        runtime
            .set_turn_strategy_focus_partitions(vec![FocusPartitionPlan {
                role_id: "reviewer".to_string(),
                shared_baseline: vec!["evidence:baseline".to_string()],
                slots: vec![FocusPartitionSlot {
                    focus_id: "runtime".to_string(),
                    boundary: "crates/runtime".to_string(),
                    evidence_responsibility: "Review the runtime boundary".to_string(),
                    capability_cropped_refs: vec!["read:crates/runtime".to_string()],
                    scope_hash: "sha256:provider-constraint-scope".to_string(),
                    overlap_budget_bp: 800,
                    novelty_target_bp: 6_000,
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                }],
            }], 1)
            .expect("set evidence scope");
        let selected = runtime.active_turn_strategy().expect("selected state");
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
            .expect("recover frozen strategy identity");
        assert_eq!(
            recovered
                .collaboration_obligation
                .as_ref()
                .map(|obligation| obligation.minimum_team_count),
            Some(1)
        );
        {
            let mut guard = runtime
                .active_turn_strategy
                .lock()
                .expect("strategy owner lock");
            let state = guard.as_mut().expect("active strategy state");
            state.resource_snapshot.provider_concurrency_penalty_bp = 9_000;
        }

        runtime
            .downgrade_turn_strategy(
                harness_contract::strategy::ExecutionCandidateKind::Direct,
                "provider concurrency constraint 9000 bp removed the Team execution slot",
            )
            .expect("provider downgrade must be durable");

        let events = store
            .list_stream(&format!("session:{}", runtime.session_id()))
            .expect("strategy events");
        let downgraded = events
            .iter()
            .find(|event| event.kind == "runtime.strategy.downgraded")
            .expect("provider downgrade event");
        assert!(
            downgraded.payload["decision_revision"]
                .as_u64()
                .expect("downgrade revision")
                > selected.revision
        );
        assert_eq!(downgraded.payload["selected_candidate"], "direct");
        assert_eq!(
            downgraded.payload["resource_snapshot"]["provider_concurrency_penalty_bp"],
            9_000
        );
        assert_eq!(
            downgraded.payload["evidence_scopes"][0]["slots"][0]["capability_cropped_refs"][0],
            "read:crates/runtime"
        );
    }

    #[test]
    fn low_novelty_publishes_bounded_early_stop() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
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
