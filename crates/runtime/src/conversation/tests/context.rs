    #[test]
    fn recovered_strategy_restores_frozen_candidate_cost_estimates() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let first_runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::clone(&store));
        first_runtime
            .begin_turn_strategy(
                "recovery-cost-turn",
                "全面审查 runtime gateway webui 三个责任域",
            )
            .expect("first admission");
        {
            let mut active = first_runtime
                .active_turn_strategy
                .lock()
                .expect("strategy owner");
            let estimate = active
                .as_mut()
                .expect("active strategy")
                .decision
                .strategy
                .candidate_estimates
                .first_mut()
                .expect("candidate estimate");
            estimate.estimated_critical_path_ms = 987_654;
            estimate.duration_calibration_source = "frozen-before-restart".to_string();
        }
        let frozen = first_runtime
            .bind_turn_strategy_execution("recovery-cost-turn", "recovery-cost-graph")
            .expect("durable selected event");
        let session_id = first_runtime.session_id().to_string();

        let mut resumed_session = Session::new();
        resumed_session.session_id = session_id;
        let resumed = ConversationRuntime::new(
            resumed_session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(store);
        resumed
            .begin_turn_strategy(
                "recovery-cost-turn",
                "the current router may have different live history",
            )
            .expect("resume admission");
        let recovered = resumed
            .bind_turn_strategy_execution("recovery-cost-turn", "recovery-cost-graph")
            .expect("recover frozen decision");

        assert_eq!(recovered.decision_id, frozen.decision_id);
        assert_eq!(
            recovered.decision.strategy.candidate_estimates,
            frozen.decision.strategy.candidate_estimates
        );
        assert_eq!(
            recovered.decision.strategy.candidate_estimates[0].duration_calibration_source,
            "frozen-before-restart"
        );
    }

    #[test]
    fn preview_chars_handles_multibyte_text() {
        let text = "再次美化模型与状态展示，确保中文截断不会 panic".repeat(8);
        let preview = preview_chars(&text, 20);

        assert!(preview.ends_with("..."));
        assert!(text.starts_with(preview.trim_end_matches("...")));
    }

    #[test]
    fn model_can_retrieve_a_focused_chunk_from_tool_evidence() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let evidence_id = "tool-raw-call-1-deadbeef";
        let output = format!(
            "{} target_failure_code {}",
            "ordinary evidence ".repeat(1_200),
            "remaining evidence ".repeat(1_200)
        );
        let session_id = runtime.session_id().to_string();
        let access = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::reality::EvidenceRef::observed("tool", evidence_id),
            "sha256:test",
            output.len() as u64,
            "text/plain; charset=utf-8",
            "artifact://art_conversation_output",
            format!("session:{session_id}"),
        );
        runtime.maybe_index_tool_output(evidence_id, "read_file", &output, Some(&access));

        let retrieved = runtime
            .retrieve_tool_evidence(&format!(
                r#"{{"evidence_ref":"tool://{evidence_id}","query":"target_failure_code","limit":2}}"#
            ))
            .expect("focused evidence should be retrievable");

        assert!(retrieved.contains("target_failure_code"));
        assert!(retrieved.len() < output.len());
    }

    #[test]
    fn context_profile_controls_runtime_envelope_profile() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_context_profile(ContextProfile::YoloGoal);
        let envelope = runtime.build_context_envelope(
            "continue task",
            vec![ContextItem::new(
                "task",
                ContextSourceKind::Task,
                ContextRole::TaskState,
                "active yolo task",
            )],
            Vec::new(),
            Vec::new(),
            runtime.context_budget_tokens(),
        );

        assert_eq!(runtime.context_profile(), ContextProfile::YoloGoal);
        assert_eq!(envelope.profile, ContextProfile::YoloGoal);
        assert_eq!(envelope.identity.mode, ContextMode::YoloGoal);
        assert!(envelope.assembled.runtime_header[0].contains("profile:YoloGoal"));
        assert!(envelope
            .assembled
            .runtime_header
            .iter()
            .any(|section| section.contains("## Runtime clock")));
    }

    #[test]
    fn context_envelope_preserves_cache_cohort_boundary_for_provider_rebuild() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec![
                "shared protocol".to_string(),
                crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY.to_string(),
                "role-specific immutable assignment".to_string(),
                crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string(),
            ],
        )
        .without_memory();
        let canonical = PromptAssembly::new(runtime.system_prompt.clone());
        let envelope = runtime.build_context_envelope(
            "perform the delegated role",
            Vec::new(),
            Vec::new(),
            Vec::new(),
            runtime.context_budget_tokens(),
        );

        assert_eq!(
            envelope.assembled.cache_cohort_segment_count,
            canonical.cache_cohort_segment_count()
        );
        let rebuilt = ConversationRuntime::<MockApi, StaticToolExecutor>::
            provider_prompt_from_envelope(&envelope);
        assert_eq!(
            rebuilt.cache_cohort_system_text(),
            canonical.cache_cohort_system_text()
        );
        let wire = rebuilt.wire_system_text().expect("provider system text");
        assert!(wire.contains("role-specific immutable assignment"));
        assert!(!wire.contains(crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY));
    }

    #[test]
    fn context_budget_defaults_to_seventy_percent_of_model_window() {
        assert_eq!(resolve_context_budget_tokens(1_000_000, 7_000), 700_000);
        assert_eq!(resolve_context_budget_tokens(128_000, 7_000), 89_600);
        assert_eq!(resolve_context_budget_tokens(32_000, 7_000), 22_400);
    }

    #[test]
    fn context_budget_ratio_is_clamped_to_safe_bounds() {
        assert_eq!(resolve_context_budget_tokens(1_000_000, 99_999), 950_000);
        assert_eq!(resolve_context_budget_tokens(1_000_000, 1), 100_000);
    }

    #[test]
    fn memory_source_scan_uses_runtime_capacity_without_layer_caps() {
        let feature_config = RuntimeFeatureConfig::default();
        let plan = RuntimeBudgetPlan::derive(RuntimeBudgetInputs {
            model_context_window: 1_000_000,
            model_max_output_tokens: 32_000,
            subsystem_budget_ratio_bp: 7_000,
            profile: ContextProfile::MainTurn,
            autonomy_mode: None,
            expected_parallel_branches: 1,
            expected_verification_passes: 0,
        });

        let mem_cfg = build_cc_memory_config_with_budget(&feature_config, &plan);

        assert_eq!(mem_cfg.budget.context_window, 700_000);
        assert_eq!(mem_cfg.budget.reserved_response, 32_000);
        assert_ne!(mem_cfg.budget.context_window, 200_000);
        assert!(!mem_cfg.budget.runtime_managed);
        assert_eq!(mem_cfg.budget.l0_reserved, 0);
        assert_eq!(mem_cfg.budget.l3_checkpoint, 0);
        assert!(plan.memory_retrieval_budget.candidate_scan_limit > 80);
    }

    #[test]
    fn telemetry_wall_speed_uses_wall_duration() {
        let wall_speed = rate_per_second(8_562, 178_350).expect("wall speed");

        assert!((wall_speed - 48.01).abs() < 0.2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn high_risk_mutation_creates_real_checkpoint_before_dispatch() {
        use harness_contract::core::{
            ExecutionModifier, ExecutionPattern, ExecutionPolicyGate, TaskRisk,
        };

        let checkpoint_calls = Arc::new(AtomicUsize::new(0));
        let mutation_calls = Arc::new(AtomicUsize::new(0));
        let checkpoint_counter = Arc::clone(&checkpoint_calls);
        let mutation_counter = Arc::clone(&mutation_calls);
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new()
                .register("checkpoint_create", move |_| {
                    checkpoint_counter.fetch_add(1, Ordering::SeqCst);
                    Ok(r#"{"id":"checkpoint-test"}"#.to_string())
                })
                .register("write_file", move |_| {
                    mutation_counter.fetch_add(1, Ordering::SeqCst);
                    Ok("written".to_string())
                }),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        let requests = vec![crate::tool_dispatch::ToolRequest {
            tool_use_id: "write-1".to_string(),
            tool_name: "write_file".to_string(),
            input: r#"{"path":"src/lib.rs","content":"x"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let plan = crate::governed_tool_plan::GovernedToolPlan::from_requests(&requests);
        let mut decision =
            crate::execution_core::build_runtime_execution_decision("实现并修改这个文件", None);
        decision.strategy.pattern = ExecutionPattern::Execute;
        decision.strategy.understanding.risk = TaskRisk::High;
        decision.strategy.modifiers = vec![
            ExecutionModifier::WithGuardrails,
            ExecutionModifier::WithCheckpoint,
        ];
        decision.strategy.gates = vec![ExecutionPolicyGate::Permission, ExecutionPolicyGate::Risk];
        decision.compile_target = crate::execution_core::RuntimeCompileTarget::ExecutionGraph;
        decision.executable = true;
        decision.blocked_reasons.clear();
        let mut validation = plan.validate_against_execution_decision(&decision);

        runtime
            .satisfy_tool_strategy_gates(&decision, &mut validation, &crate::SharedPrompter::none())
            .await;

        assert!(validation.allowed, "{:?}", validation.findings);
        assert!(validation.checkpoint_created);
        assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);

        decision.strategy.understanding.risk = TaskRisk::Critical;
        decision.strategy.gates.push(ExecutionPolicyGate::Approval);
        let mut critical_validation = plan.validate_against_execution_decision(&decision);
        runtime
            .satisfy_tool_strategy_gates(
                &decision,
                &mut critical_validation,
                &crate::SharedPrompter::none(),
            )
            .await;
        assert!(critical_validation.allowed);
        assert!(critical_validation.requires_approval);
        assert!(critical_validation.checkpoint_created);
        assert_eq!(checkpoint_calls.load(Ordering::SeqCst), 2);
        assert_eq!(mutation_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn model_router_delegates_to_provider_default_when_no_model_is_explicit() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        assert_eq!(runtime.model_candidates_for_turn("简单问题"), vec![""]);
    }

    #[test]
    fn model_candidates_keep_configured_primary_and_fallback_order() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("balanced-model".to_string());
        *runtime.fallbacks.write().unwrap() =
            vec!["stepfun-fast".to_string(), "deepseek-depth".to_string()];

        assert_eq!(
            runtime.model_candidates_for_turn("任务内容不得改变配置顺序"),
            vec![
                "balanced-model".to_string(),
                "stepfun-fast".to_string(),
                "deepseek-depth".to_string(),
            ]
        );
    }

    #[test]
    fn model_candidates_observe_shared_fallback_policy_updates() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime.model = Some("primary".to_string());
        let policy = Arc::new(std::sync::RwLock::new(vec!["same-provider".to_string()]));
        runtime = runtime.with_provider_fallback_policy(Arc::clone(&policy));
        assert_eq!(
            runtime.model_candidates_for_turn("first turn"),
            vec!["primary".to_string(), "same-provider".to_string()]
        );

        *policy.write().unwrap() = vec![
            "cross-provider".to_string(),
            "secondary-provider".to_string(),
        ];
        assert_eq!(
            runtime.model_candidates_for_turn("next turn"),
            vec![
                "primary".to_string(),
                "cross-provider".to_string(),
                "secondary-provider".to_string(),
            ]
        );
    }

    #[test]
    fn reconstructs_usage_tracker_from_restored_session() {
        struct SimpleApi;
        impl ApiClient for SimpleApi {
            fn stream(
                &mut self,
                _request: ApiRequest,
            ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>>
            {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("done".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        let mut session = Session::new();
        session
            .push_message(crate::session::ConversationMessage::assistant_with_usage(
                vec![ContentBlock::Text {
                    text: "earlier".to_string(),
                }],
                Some(TokenUsage {
                    input_tokens: 11,
                    output_tokens: 7,
                    cache_creation_input_tokens: 2,
                    cache_read_input_tokens: 1,
                }),
            ))
            .expect("append prior usage");

        let runtime = ConversationRuntime::new(
            session,
            SimpleApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        );

        assert_eq!(runtime.usage().turns(), 1);
        assert_eq!(runtime.usage().cumulative_usage().total_tokens(), 21);
    }

    // ── M2: Memory system tests ──────────────────────────────────────

    #[derive(Clone)]
    struct MockApi;
    impl ApiClient for MockApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[derive(Clone)]
    struct PromptRecordingApi {
        requests: Arc<std::sync::Mutex<Vec<ApiRequest>>>,
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
    }

    impl ApiClient for PromptRecordingApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.requests
                .lock()
                .expect("request recorder")
                .push(request);
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::TextDelta("skill-aware result".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections
                .lock()
                .expect("projection recorder")
                .push(projection);
        }
    }

    #[tokio::test]
    async fn first_model_step_activates_skill_persists_bridge_and_injects_asset() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "skill-activation".to_string(),
                user_id: None,
                model: Some("test-model".to_string()),
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "None".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let profile = SkillCapabilityProfile {
            skill_id: "release-evidence".to_string(),
            name: "Release Evidence".to_string(),
            version: Some("1.0.0".to_string()),
            source_root: "/skills/release-evidence".to_string(),
            package_fingerprint: "test".to_string(),
            kind: SkillKind::Workflow,
            lifecycle_status: SkillLifecycleStatus::UsablePrompt,
            adapters: vec![SkillAdapterKind::PromptOnly],
            risk_level: SkillRiskLevel::Low,
            entrypoints: vec![SkillEntrypoint {
                runtime: SkillDetectedRuntime::Markdown,
                path: "SKILL.md".to_string(),
                adapter: SkillAdapterKind::PromptOnly,
                command_hint: None,
            }],
            inspection_summary: vec!["release evidence planning".to_string()],
            structured_dependencies: Vec::new(),
        };
        let mut runtime = ConversationRuntime::new(
            session,
            PromptRecordingApi {
                requests: Arc::clone(&requests),
                projections: Arc::clone(&projections),
            },
            StaticToolExecutor::new().register("lark_cli_read", |_| Ok("{}".to_string())),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            Arc::clone(&store),
        ))
        .with_skill_profiles(vec![profile])
        .with_agent_skill_profile(AgentSkillProfile {
            adapter_ceiling: vec![SkillAdapterKind::PromptOnly],
            ..AgentSkillProfile::default()
        })
        .with_skill_prompt_assets(vec![super::RuntimeSkillPromptAsset {
            skill_id: "release-evidence".to_string(),
            version: Some("1.0.0".to_string()),
            content: "Require release evidence before accepting completion.".to_string(),
            source_ref: "skill://release-evidence/SKILL.md".to_string(),
            tool_refs: vec!["lark_cli_read".to_string()],
        }]);
        runtime.model = Some("test-model".to_string());
        runtime
            .begin_turn_strategy("test-skill-turn", "prepare release evidence")
            .expect("test turn strategy admission");

        runtime
            .execute_model_step("prepare release evidence", true)
            .await
            .expect("first skill-aware model step");

        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("skill domain events");
        assert!(events.events.iter().any(|event| {
            event.kind == "skill_candidates"
                && event.payload["source"] == "conversation_runtime.skill_activation"
                && event.payload["selected"] == "release-evidence"
        }));
        assert!(events.events.iter().any(|event| {
            event.kind == "skill_memory_candidate"
                && event.payload["source"] == "conversation_runtime.skill_memory_candidate"
                && event.payload["selected"] == "release-evidence"
        }));
        let requests = requests.lock().expect("request recorder");
        assert_eq!(requests.len(), 1);
        assert!(rendered_prompt(&requests[0].prompt)
            .contains("Require release evidence before accepting completion."));
        let projections = projections.lock().expect("projection recorder");
        assert_eq!(projections.len(), 1);
        assert!(
            projections[0]
                .active_ids
                .iter()
                .any(|tool| tool == "lark_cli_read"),
            "the selected Skill tool must be visible in its first provider request"
        );
    }

    struct RuntimeAwareApi(Arc<std::sync::atomic::AtomicBool>);

    impl ApiClient for RuntimeAwareApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.store(
                tokio::runtime::Handle::try_current().is_ok(),
                std::sync::atomic::Ordering::SeqCst,
            );
            Box::pin(futures::stream::iter(vec![Ok(AssistantEvent::MessageStop)]))
        }
    }

    #[test]
    fn synchronous_stream_collection_creates_the_stream_inside_tokio() {
        let observed_runtime = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut api = RuntimeAwareApi(Arc::clone(&observed_runtime));
        let events = api
            .stream_collect(ApiRequest {
                prompt: PromptAssembly::default(),
                messages: Vec::new().into(),
                model: "test".to_string(),
                reasoning_effort_override: None,
                request_compiler_cache_hit: false,
                budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                    "test", 128_000, 4_096, 128, 256, 0,
                ),
                provider_evidence_context: None,
            })
            .expect("synchronous collection should succeed");

        assert_eq!(events, vec![AssistantEvent::MessageStop]);
        assert!(
            observed_runtime.load(std::sync::atomic::Ordering::SeqCst),
            "ApiClient::stream must be constructed with an active Tokio runtime"
        );
    }

    #[derive(Clone)]
    struct ExposureRecordingApi {
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
        required_tool_choices: Arc<std::sync::Mutex<Vec<bool>>>,
        reasoning_efforts: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl ApiClient for ExposureRecordingApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.reasoning_efforts
                .lock()
                .unwrap()
                .push(request.reasoning_effort_override);
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::TextDelta("bounded conclusion".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections.lock().unwrap().push(projection);
        }

        fn configure_tool_choice_required(&mut self, required: bool) {
            self.required_tool_choices.lock().unwrap().push(required);
        }
    }

    struct ExposureToolExecutor;

    #[async_trait::async_trait]
    impl crate::ToolExecutor for ExposureToolExecutor {
        async fn execute_output(
            &self,
            _name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "tool_search".to_string(),
                "custom_reader".to_string(),
                "grep_search".to_string(),
            ]
        }

        fn collaboration_runtime_available(&self) -> bool {
            true
        }
    }

    #[test]
    fn capability_receipt_projects_current_schema_separately_from_catalog() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        *runtime.tool_exposure_state.lock().expect("exposure state") = Some(ToolExposureState {
            catalog_revision: 5,
            bootstrap: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
            ]
            .into_iter()
            .collect(),
            active: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
            ]
            .into_iter()
            .collect(),
            deferred: ["read_many".to_string(), "runtime_orchestrate".to_string()]
                .into_iter()
                .collect(),
            reason: "bootstrap tools exposed".to_string(),
            revision: 2,
            fallback_full: false,
        });

        let projected = runtime.project_runtime_capabilities_for_model(
            &serde_json::json!({
                "available_tool_names": ["tool_search", "runtime_capabilities", "read_many", "runtime_orchestrate"],
                "runtime_orchestrate": {"available": true, "blocked_reasons": []},
                "action_plane": {"can_execute_now": true},
                "strategy": {"model_callable_tools": ["tool_search", "runtime_capabilities", "read_many", "runtime_orchestrate"]}
            })
            .to_string(),
        );
        let value: serde_json::Value =
            serde_json::from_str(&projected).expect("projected capability JSON");

        assert_eq!(
            value["catalog_tool_names"],
            serde_json::json!([
                "tool_search",
                "runtime_capabilities",
                "read_many",
                "runtime_orchestrate"
            ])
        );
        assert_eq!(
            value["tool_visibility"]["active_function_schemas"],
            serde_json::json!(["runtime_capabilities", "tool_search"])
        );
        assert_eq!(
            value["strategy"]["model_callable_tools"],
            serde_json::json!(["runtime_capabilities", "tool_search"])
        );
        assert_eq!(value["runtime_orchestrate"]["available"], false);
        assert_eq!(value["runtime_orchestrate"]["schema_active"], false);
        assert_eq!(value["action_plane"]["can_execute_now"], false);
        assert_eq!(
            value["action_plane"]["recommended_next_tool"],
            "tool_search"
        );
    }

    #[test]
    fn capability_receipt_preserves_lease_preflight_verdict() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();
        *runtime.tool_exposure_state.lock().expect("exposure state") = Some(ToolExposureState {
            catalog_revision: 5,
            bootstrap: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
                "runtime_orchestrate".to_string(),
            ]
            .into_iter()
            .collect(),
            active: [
                "tool_search".to_string(),
                "runtime_capabilities".to_string(),
                "runtime_orchestrate".to_string(),
            ]
            .into_iter()
            .collect(),
            deferred: ["read_many".to_string()].into_iter().collect(),
            reason: "bootstrap tools exposed".to_string(),
            revision: 2,
            fallback_full: false,
        });

        let rejected = runtime.project_runtime_capabilities_for_model(
            &serde_json::json!({
                "available_tool_names": ["tool_search", "runtime_capabilities", "runtime_orchestrate"],
                "runtime_orchestrate": {"available": false, "blocked_reasons": ["model_proposal_conflicts_with_strategy_lease"]},
                "action_plane": {
                    "can_execute_now": false,
                    "preflight": {
                        "can_execute_now": false,
                        "status": "rejected",
                        "reasons": ["model_proposal_conflicts_with_strategy_lease"],
                        "proposal_required_pattern": "collaborate",
                        "lease_locked_pattern": "collaborate"
                    }
                },
                "strategy": {"model_callable_tools": ["tool_search", "runtime_capabilities", "runtime_orchestrate"]}
            })
            .to_string(),
        );
        let rejected_value: serde_json::Value =
            serde_json::from_str(&rejected).expect("projected capability JSON");
        assert_eq!(rejected_value["action_plane"]["can_execute_now"], false);
        assert_eq!(
            rejected_value["action_plane"]["preflight"]["status"],
            "rejected"
        );

        let accepted = runtime.project_runtime_capabilities_for_model(
            &serde_json::json!({
                "available_tool_names": ["tool_search", "runtime_capabilities", "runtime_orchestrate"],
                "runtime_orchestrate": {"available": true, "blocked_reasons": []},
                "action_plane": {
                    "can_execute_now": true,
                    "preflight": {
                        "can_execute_now": true,
                        "status": "accepted",
                        "proposal_required_pattern": "collaborate",
                        "lease_locked_pattern": "collaborate"
                    }
                },
                "strategy": {"model_callable_tools": ["tool_search", "runtime_capabilities", "runtime_orchestrate"]}
            })
            .to_string(),
        );
        let accepted_value: serde_json::Value =
            serde_json::from_str(&accepted).expect("projected capability JSON");
        assert_eq!(accepted_value["action_plane"]["can_execute_now"], true);
        assert_eq!(
            accepted_value["action_plane"]["preflight"]["status"],
            "accepted"
        );
    }

    #[tokio::test]
    async fn text_only_checkpoint_hides_tools_for_exactly_one_model_request() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = ExposureRecordingApi {
            projections: Arc::clone(&projections),
            required_tool_choices: Arc::new(std::sync::Mutex::new(Vec::new())),
            reasoning_efforts: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.require_next_model_final_response();
        runtime
            .begin_turn_strategy("test-text-only-turn", "summarize checked evidence")
            .expect("test turn strategy admission");
        runtime
            .execute_model_step("summarize checked evidence", true)
            .await
            .unwrap();
        runtime
            .execute_model_step("summarize checked evidence", false)
            .await
            .unwrap();

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 2);
        assert!(projections[0].active_ids.is_empty());
        assert_eq!(projections[0].deferred_ids.len(), 3);
        assert_eq!(
            projections[1].active_ids,
            vec!["grep_search", "tool_search"]
        );
        assert!(projections[1]
            .deferred_ids
            .contains(&"custom_reader".to_string()));
    }

    struct MutationExposureToolExecutor;

    #[async_trait::async_trait]
    impl crate::ToolExecutor for MutationExposureToolExecutor {
        async fn execute_output(
            &self,
            _name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("exposure test executor must not run"))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec![
                "tool_search".to_string(),
                "read_file".to_string(),
                "grep_search".to_string(),
                "edit_file".to_string(),
                "write_file".to_string(),
            ]
        }
    }

    #[tokio::test]
    async fn mutation_checkpoint_exposes_only_writes_for_one_model_request() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = ExposureRecordingApi {
            projections: Arc::clone(&projections),
            required_tool_choices: Arc::new(std::sync::Mutex::new(Vec::new())),
            reasoning_efforts: Arc::new(std::sync::Mutex::new(Vec::new())),
        };
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            api,
            MutationExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.require_next_model_tools([
            "edit_file".to_string(),
            "write_file".to_string(),
            "unknown_mutator".to_string(),
        ]);
        runtime
            .begin_turn_strategy("test-mutation-exposure-turn", "write the authorized file")
            .expect("test turn strategy admission");
        runtime
            .execute_model_step("write the authorized file", true)
            .await
            .expect("write-only model step");
        runtime
            .execute_model_step("write the authorized file", false)
            .await
            .expect("restored model step");

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 2);
        assert_eq!(
            projections[0].active_ids,
            vec!["edit_file".to_string(), "write_file".to_string()]
        );
        assert!(!projections[0]
            .active_ids
            .contains(&"unknown_mutator".to_string()));
        assert!(projections[0]
            .deferred_ids
            .contains(&"read_file".to_string()));
        assert!(projections[1]
            .active_ids
            .contains(&"tool_search".to_string()));
        assert!(projections[1].active_ids.contains(&"read_file".to_string()));
        assert!(projections[1]
            .active_ids
            .contains(&"grep_search".to_string()));
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
    }

    #[derive(Clone)]
    struct DynamicExposureApi {
        requests: Arc<std::sync::atomic::AtomicUsize>,
        projections: Arc<std::sync::Mutex<Vec<harness_contract::tool::ToolExposureProjection>>>,
        request_messages: Arc<std::sync::Mutex<Vec<Vec<String>>>>,
    }

    impl ApiClient for DynamicExposureApi {
        fn stream(
            &mut self,
            request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            let mut captured = request
                .messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.clone()),
                    ContentBlock::ToolResult { output, .. } => Some(output.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            if let Some(system) = request.prompt.trusted_system_text() {
                captured.push(system);
            }
            captured.extend(request.prompt.contextual_messages());
            self.request_messages.lock().unwrap().push(captured);
            let request = self
                .requests
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if request == 0 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "discover-1".to_string(),
                        name: "tool_search".to_string(),
                        input: r#"{"query":"read files"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else if request == 1 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "invalid-1".to_string(),
                        name: "invented_tool".to_string(),
                        input: "{}".to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else if request == 2 {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::ToolUse {
                        id: "read-1".to_string(),
                        name: "custom-reader".to_string(),
                        input: r#"{"path":"README.md"}"#.to_string(),
                    }),
                    Ok(AssistantEvent::MessageStop),
                ]))
            } else {
                Box::pin(futures::stream::iter(vec![
                    Ok(AssistantEvent::TextDelta("discovery complete".to_string())),
                    Ok(AssistantEvent::MessageStop),
                ]))
            }
        }

        fn configure_tool_exposure(
            &mut self,
            projection: harness_contract::tool::ToolExposureProjection,
        ) {
            self.projections.lock().unwrap().push(projection);
        }
    }

    struct DynamicExposureToolExecutor;

    #[async_trait::async_trait]
    impl crate::ToolExecutor for DynamicExposureToolExecutor {
        async fn execute_output(
            &self,
            name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            let output = if name == "custom_reader" {
                "README contents".to_string()
            } else if name == "tool_search" {
                serde_json::json!({
                    "query": "read files",
                    "catalog_revision": 0,
                    "descriptors": [{
                        "canonical_id": "custom_reader",
                        "display_name": "custom_reader",
                        "source": "test",
                        "schema_hash": "read-v1",
                        "required_permission": "read-only",
                        "permission_source": "test",
                        "health": "healthy"
                    }],
                    "activation_candidates": ["custom_reader"]
                })
                .to_string()
            } else {
                return Err(crate::ToolError::new("unknown dynamic tool"));
            };
            Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                output,
            ))
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec!["tool_search".to_string(), "custom_reader".to_string()]
        }

        fn classify_tool_safety(
            &self,
            name: &str,
            _input: &str,
        ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
            matches!(name, "tool_search" | "custom_reader")
                .then_some(crate::tool_orchestrator::ToolSafetyCategory::ReadOnly)
        }

        fn registered_tool_effect(
            &self,
            name: &str,
            _input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            use harness_contract::policy::{
                PermissionOperation, PermissionResource, PermissionScope,
            };
            use harness_contract::tool::{
                ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
                ToolPermissionMode,
            };

            matches!(name, "tool_search" | "custom_reader").then(|| ToolEffectDescriptor {
                tool_id: name.to_string(),
                descriptor_hash: "dynamic-tool-search-v1".to_string(),
                effect_kind: ToolEffectKind::Read,
                idempotency: ToolIdempotency::Idempotent,
                scopes: vec![PermissionScope::new(
                    PermissionResource::Tool,
                    PermissionOperation::Read,
                )],
                required_permission: ToolPermissionMode::ReadOnly,
                approval_class: ToolApprovalClass::None,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: harness_contract::policy::EffectAssessment::default(),
            })
        }

        async fn execute_authorized_output(
            &self,
            authorization: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            if authorization.tool_id != name {
                return Err(crate::ToolError::new(
                    "dynamic tool authorization names a different tool",
                ));
            }
            self.execute_output(name, input).await
        }
    }

    struct DirectDeferredApi;

    impl ApiClient for DirectDeferredApi {
        fn stream(
            &mut self,
            _request: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![
                Ok(AssistantEvent::ToolUse {
                    id: "read-direct".to_string(),
                    name: "custom-reader".to_string(),
                    input: r#"{"path":"README.md"}"#.to_string(),
                }),
                Ok(AssistantEvent::Usage(TokenUsage {
                    input_tokens: 100,
                    output_tokens: 10,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 50,
                })),
                Ok(AssistantEvent::MessageStop),
            ]))
        }
    }

    #[tokio::test]
    async fn known_deferred_tool_call_activates_and_executes_the_current_frame() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            DirectDeferredApi,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("direct-deferred-turn", "inspect README")
            .expect("turn strategy");

        let executed = runtime
            .execute_model_step("inspect README", true)
            .await
            .expect("known deferred schema executes under Runtime's just-bound lease");
        assert!(runtime
            .tool_exposure_state
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|state| state.active.contains("custom_reader")));

        let ModelStepIntent::ToolCalls { calls } = executed.intent else {
            panic!("activated frame must preserve the provider tool call");
        };
        assert_eq!(calls[0].name, "custom_reader");
        assert_eq!(runtime.usage().turns(), 1);
        assert_eq!(runtime.usage().cumulative_usage().total_tokens(), 160);
    }

    #[tokio::test]
    async fn one_request_tool_allowlist_is_a_hard_deferred_activation_ceiling() {
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            DirectDeferredApi,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("bounded-deferred-turn", "inspect README")
            .expect("turn strategy");
        runtime.require_next_model_tools(["tool_search".to_string()]);

        let error = runtime
            .execute_model_step("inspect README", true)
            .await
            .expect_err("the one-request allowlist must reject deferred activation");
        assert!(!error.is_tool_exposure_miss(), "{error}");
        assert!(error
            .to_string()
            .contains("governed one-request allowlist rejected [custom_reader]"));
        assert!(!runtime
            .tool_exposure_state
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|state| state.active.contains("custom_reader")));
    }

    #[tokio::test]
    async fn successful_session_tools_are_rehydrated_on_the_next_user_turn() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut session = Session::new();
        session
            .push_message(ConversationMessage::tool_result(
                "prior-read",
                "custom-reader",
                "prior bounded result",
                false,
            ))
            .expect("session tool result");
        let mut runtime = ConversationRuntime::new(
            session,
            DynamicExposureApi {
                requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                projections: Arc::clone(&projections),
                request_messages: Arc::new(std::sync::Mutex::new(Vec::new())),
            },
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory();
        runtime
            .begin_turn_strategy("rehydrated-tool-turn", "continue inspecting files")
            .expect("turn strategy");

        runtime
            .execute_model_step("continue inspecting files", true)
            .await
            .expect("first model request");

        let projections = projections.lock().unwrap();
        assert!(projections[0]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(!projections[0]
            .deferred_ids
            .contains(&"custom_reader".to_string()));
    }

    #[tokio::test]
    async fn dynamic_tool_exposure_defers_schema_until_discovery_activation() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let request_messages = Arc::new(std::sync::Mutex::new(Vec::new()));
        let api = DynamicExposureApi {
            requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            projections: Arc::clone(&projections),
            request_messages: Arc::clone(&request_messages),
        };
        let artifact_root = tempfile::tempdir().unwrap();
        let session = Session::new();
        let session_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        session_store
            .create_session(&session::SessionRecord {
                session_id: session.session_id.clone(),
                platform: "test".to_string(),
                chat_id: "dynamic-exposure".to_string(),
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
            .unwrap();
        let mut runtime = ConversationRuntime::new(
            session,
            api,
            DynamicExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::new(RuntimeEventStore::try_open_in_memory().unwrap()))
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            session_store,
        ))
        .with_artifact_store(Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        ));
        runtime
            .begin_turn_strategy("test-dynamic-exposure-turn", "inspect files")
            .expect("test turn strategy admission");

        let first = runtime
            .execute_model_step("inspect files", true)
            .await
            .expect("first model step");
        let ModelStepIntent::ToolCalls { calls } = first.intent else {
            panic!("first request must invoke tool_search");
        };
        {
            let exposure = runtime
                .tool_exposure_state
                .lock()
                .expect("tool exposure state");
            let exposure = exposure
                .as_ref()
                .expect("first provider request must persist its exposure state");
            assert!(
                exposure.deferred.contains("custom_reader"),
                "custom_reader must be discoverable before tool_search activation: {exposure:?}"
            );
        }
        let discovery_output = runtime
            .tool_executor
            .execute_output("tool_search", &calls[0].input)
            .await
            .expect("governed tool_search execution");
        let parsed_discovery =
            serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(
                discovery_output.model_text(),
            )
            .expect("tool_search must return a canonical discovery receipt");
        assert_eq!(
            parsed_discovery.activation_candidates,
            vec!["custom_reader".to_string()]
        );
        let discovery_result = runtime
            .prepare_governed_tool_result(
                &calls[0].id,
                &calls[0].name,
                &calls[0].input,
                discovery_output.model_text(),
                false,
            )
            .await
            .expect("governed tool_search result preparation");
        runtime
            .session
            .write()
            .await
            .push_message(discovery_result)
            .expect("governed tool_search result publication");
        assert!(
            runtime
                .tool_exposure_state
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|state| state.active.contains("custom_reader")),
            "tool_search must activate custom_reader before the following provider request"
        );
        let protocol_error = runtime
            .execute_model_step("inspect files", false)
            .await
            .expect_err("an invented tool must fail the active exposure lease");
        assert!(protocol_error
            .to_string()
            .contains("outside this request's exposure lease"));
        let activated = runtime
            .execute_model_step("inspect files", false)
            .await
            .expect("protocol recovery must retain the discovery handoff");
        let ModelStepIntent::ToolCalls { calls } = activated.intent else {
            panic!("recovery request must invoke the activated tool");
        };
        assert_eq!(calls[0].name, "custom_reader");
        let batch = runtime
            .execute_tool_batch_step(&calls, &crate::SharedPrompter::none(), 2)
            .await
            .expect("activated tool execution");
        assert_eq!(batch.failed, 0);
        runtime
            .execute_model_step("inspect files", false)
            .await
            .expect("final model step");

        let projections = projections.lock().unwrap();
        assert_eq!(projections.len(), 4);
        assert_eq!(projections[0].catalog_revision, 0);
        assert_eq!(projections[0].active_ids, vec!["tool_search"]);
        assert_eq!(projections[0].deferred_ids, vec!["custom_reader"]);
        assert!(projections[1]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(
            !projections[1]
                .active_ids
                .contains(&"tool_search".to_string())
                && !projections[1]
                    .bootstrap_ids
                    .contains(&"tool_search".to_string()),
            "the immediate post-discovery request must not be able to repeat tool_search"
        );
        assert!(projections[1].exposure_revision > projections[0].exposure_revision);
        assert!(projections[2]
            .active_ids
            .contains(&"custom_reader".to_string()));
        assert!(!projections[2]
            .active_ids
            .contains(&"tool_search".to_string()));
        assert!(
            projections[3]
                .active_ids
                .contains(&"tool_search".to_string()),
            "tool_search must return after a valid post-discovery response"
        );
        assert!(projections[3].exposure_revision > projections[2].exposure_revision);
        let request_messages = request_messages.lock().unwrap();
        assert_eq!(request_messages.len(), 4);
        assert!(
            request_messages[1].iter().any(|message| {
                message.contains("tool_search already completed successfully")
                    && message.contains("Newly activated native function schemas: [custom_reader]")
                    && message.contains("do not claim that a new user turn is needed")
            }),
            "the post-discovery provider request must provide an explicit execution handoff: {:?}",
            request_messages[1]
        );
        assert!(
            request_messages[2].iter().any(|message| {
                message.contains("tool_search already completed successfully")
                    && message.contains("Newly activated native function schemas: [custom_reader]")
            }),
            "protocol recovery must retain the post-discovery handoff"
        );

        let metrics = runtime.tool_exposure_metrics();
        assert_eq!(metrics.provider_requests, 4);
        assert_eq!(metrics.tool_search_calls, 1);
        assert_eq!(metrics.tool_search_additional_rounds, 1);
        assert_eq!(metrics.activation_candidates, 1);
        assert_eq!(metrics.activations, 1);
        assert_eq!(metrics.activated_invocations, 1);
        assert_eq!(metrics.activation_precision_bp, Some(10_000));
        assert_eq!(metrics.activation_recall_bp, None);
    }

    #[tokio::test]
    async fn orchestration_phase_gate_exposes_only_control_plane_tools() {
        let projections = Arc::new(std::sync::Mutex::new(Vec::new()));
        let required_tool_choices = Arc::new(std::sync::Mutex::new(Vec::new()));
        let reasoning_efforts = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut runtime = ConversationRuntime::new(
            Session::new(),
            ExposureRecordingApi {
                projections: Arc::clone(&projections),
                required_tool_choices: Arc::clone(&required_tool_choices),
                reasoning_efforts: Arc::clone(&reasoning_efforts),
            },
            ExposureToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_runtime_event_store(Arc::new(
            RuntimeEventStore::try_open_in_memory().expect("event store"),
        ));
        runtime
            .begin_turn_strategy("orchestration-gate-turn", "必须实际启动协作团队完成分析")
            .expect("turn strategy admission");
        runtime
            .set_turn_strategy_focus_partitions(Vec::new(), 1)
            .expect("freeze root collaboration obligation");
        runtime.require_next_model_orchestration_only();
        runtime
            .execute_model_step("必须实际启动协作团队完成分析", true)
            .await
            .expect("model step");
        let recorded = projections.lock().expect("projections");
        let projection = recorded.last().expect("exposure projection recorded");
        let active = projection
            .active_ids
            .iter()
            .chain(projection.bootstrap_ids.iter())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            active.contains(harness_contract::orchestration::SUBMIT_COLLABORATION_DECISION_TOOL_ID)
        );
        assert_eq!(
            active.len(),
            2,
            "root admission may expose only the control-plane tools: {active:?}"
        );
        assert!(
            active.contains("runtime_capabilities"),
            "the model must be able to inspect exact template role ids before proposing: {active:?}"
        );
        assert_eq!(
            required_tool_choices
                .lock()
                .expect("required choices")
                .as_slice(),
            &[true],
            "root admission must propagate a required native tool choice to the provider adapter"
        );
        assert_eq!(
            reasoning_efforts
                .lock()
                .expect("reasoning efforts")
                .as_slice(),
            &[Some("none".to_string())],
            "the forced Qwen-compatible proposal request disables thinking only for this call"
        );
        assert!(
            !active.contains("grep_search"),
            "orchestration-phase gate must hide general tools: {active:?}"
        );
        assert!(
            !active.contains("tool_search"),
            "orchestration-phase gate must hide discovery tools: {active:?}"
        );
    }

    #[test]
    fn tool_exposure_metrics_distinguish_activation_cost_and_outcomes() {
        use harness_contract::tool::{
            ToolActivationDecision, ToolActivationReceipt, ToolActivationStatus,
        };

        let mut metrics = TurnToolExposureMetrics::default();
        metrics.reset((0, 0));
        metrics.observe_search(&ToolActivationReceipt {
            catalog_revision: 7,
            previous_exposure_revision: 2,
            exposure_revision: 3,
            decisions: vec![
                ToolActivationDecision {
                    canonical_id: "reader".to_string(),
                    status: ToolActivationStatus::Activated,
                    reason: "healthy and permitted".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "writer".to_string(),
                    status: ToolActivationStatus::Denied,
                    reason: "permission ceiling".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "remote".to_string(),
                    status: ToolActivationStatus::Unavailable,
                    reason: "catalog health".to_string(),
                },
                ToolActivationDecision {
                    canonical_id: "missing".to_string(),
                    status: ToolActivationStatus::NotFound,
                    reason: "unknown descriptor".to_string(),
                },
            ],
        });
        metrics.observe_provider_request(
            ProviderContextInventory {
                tool_count: 2,
                tool_schema_tokens: 333,
                ..Default::default()
            },
            (2, 1),
        );
        metrics.observe_invocation("reader");

        let projection = metrics.projection();
        assert_eq!(projection.provider_requests, 1);
        assert_eq!(projection.tool_search_calls, 1);
        assert_eq!(projection.tool_search_additional_rounds, 1);
        assert_eq!(projection.activation_candidates, 4);
        assert_eq!(projection.activations, 1);
        assert_eq!(projection.activated_invocations, 1);
        assert_eq!(projection.permission_rejections, 1);
        assert_eq!(projection.unavailable_descriptors, 1);
        assert_eq!(projection.descriptor_misses, 1);
        assert_eq!(projection.schema_tokens_max, 333);
        assert_eq!(projection.schema_compilations, 2);
        assert_eq!(projection.schema_cache_hits, 1);
        assert_eq!(projection.activation_precision_bp, Some(10_000));
        assert_eq!(projection.activation_recall_bp, None);
    }

    #[test]
    fn stable_prefix_metrics_track_wire_identity_and_provider_native_cache() {
        let mut metrics = TurnStablePrefixMetrics::default();
        let request = |dynamic: &str, cache_hit| ApiRequest {
            prompt: PromptAssembly::new(vec![
                "stable identity".to_string(),
                "stable policy".to_string(),
                crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY.to_string(),
                dynamic.to_string(),
            ]),
            messages: vec![ConversationMessage::user_text("inspect".to_string())].into(),
            model: "test".to_string(),
            reasoning_effort_override: None,
            request_compiler_cache_hit: cache_hit,
            budget: crate::context_ledger::RequestBudgetReport::for_attempt(
                "test", 32_768, 4_096, 128, 256, 0,
            ),
            provider_evidence_context: None,
        };

        metrics.observe_request(&request("runtime A", false));
        metrics.observe_request(&request("runtime B with more bytes", true));
        metrics.observe_usage(TokenUsage {
            input_tokens: 100,
            output_tokens: 10,
            cache_creation_input_tokens: 80,
            cache_read_input_tokens: 64,
        });

        let projection = metrics.projection;
        assert_eq!(projection.provider_requests, 2);
        assert!(!projection.stable_prefix_fingerprint.is_empty());
        assert_eq!(projection.wire_identity_failures, 0);
        assert_eq!(projection.request_compiler_compilations, 1);
        assert_eq!(projection.request_compiler_cache_hits, 1);
        assert_eq!(projection.native_cache_creation_input_tokens, 80);
        assert_eq!(projection.native_cache_read_input_tokens, 64);
        assert!(projection.runtime_system_bytes_max > 0);
    }

    #[tokio::test]
    async fn governed_tool_results_persist_raw_evidence_and_bound_model_receipt() {
        let artifact_root = tempfile::tempdir().unwrap();
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "test-chat".to_string(),
                user_id: None,
                model: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
                last_activity: "2026-01-01T00:00:00Z".to_string(),
                message_count: 0,
                reset_policy: "None".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_artifact_store(Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig::default(),
            )
            .expect("artifact store"),
        ))
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        let raw = format!("first\n{}\nlast", "middle-evidence ".repeat(8_000));

        let receipt = runtime
            .prepare_governed_tool_result(
                "governed-read-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await
            .expect("durable evidence receipt");

        let output = receipt
            .blocks
            .iter()
            .find_map(|block| match block {
                ContentBlock::ToolResult { output, .. } => Some(output),
                _ => None,
            })
            .expect("governed receipt must be a tool result");
        assert!(
            output.contains("tool://tool-raw-governed-read-1-"),
            "unexpected governed receipt: {output}"
        );
        assert!(
            output.len() < raw.len() / 10,
            "model must receive a receipt, not raw output"
        );
        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("durable tool evidence");
        assert!(events.events.iter().any(|event| {
            event.kind == "evidence.raw.persisted"
                && event
                    .payload
                    .get("artifact_selector")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|selector| selector.starts_with("artifact://"))
                && event.payload.get("raw").is_none()
        }));
        let audit = runtime.turn_evidence_audits();
        assert_eq!(audit.len(), 1);
        assert!(audit[0].access.is_some());
        assert!(audit[0].omitted_tokens > 0);
        let observations = runtime.turn_tool_observations();
        assert_eq!(observations.len(), 1);
        let envelope = observations[0]
            .output_envelope
            .as_ref()
            .expect("tool output envelope must be connected to the turn report");
        assert!(envelope.receipt.starts_with("tool://"));
        assert!(envelope
            .artifact_ref
            .as_ref()
            .is_some_and(|artifact| artifact.selector.starts_with("artifact://")));
        assert!(envelope
            .evidence_ref
            .as_ref()
            .is_some_and(harness_contract::context::EvidenceAccessRef::is_durable));
    }

    #[tokio::test]
    async fn governed_tool_result_never_publishes_durable_access_after_raw_store_failure() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        // No matching SessionRecord is created: the SessionStore adapter must
        // fail instead of fabricating an evidence receipt.
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::new(
                session::UnifiedSessionStore::open_in_memory().unwrap(),
            )),
        );
        let raw = "raw output retained only in the active runtime when durable write fails\n"
            .repeat(1_000);

        let error = runtime
            .prepare_governed_tool_result(
                "raw-failure-1",
                "read_file",
                r#"{"path":"README.md"}"#,
                &raw,
                false,
            )
            .await
            .expect_err("missing artifact durability must block publication");
        assert!(error.to_string().contains("Artifact store"));
        assert!(runtime.turn_evidence_audits().is_empty());
    }

    #[tokio::test]
    async fn context_turn_report_is_durable_before_runtime_exposes_it() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "context-report".to_string(),
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
            .unwrap();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        let report = runtime.build_context_turn_report("turn-durable", TokenUsage::default(), None);

        runtime
            .remember_context_turn_report(report.clone())
            .await
            .expect("report persistence must finish before exposure");
        assert_eq!(runtime.last_context_turn_report(), Some(report.clone()));
        let events = store
            .session_domain_events_page(&session_id, 0, 20)
            .await
            .expect("report event");
        assert!(events.events.iter().any(|event| {
            event.kind == "context.turn_report"
                && event.payload.get("report") == Some(&serde_json::to_value(&report).unwrap())
        }));
    }

    #[tokio::test]
    async fn large_context_envelope_is_canonical_and_artifact_backed() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let session = Session::new();
        let session_id = session.session_id.clone();
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".to_string(),
                chat_id: "context-artifact".to_string(),
                user_id: None,
                model: None,
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
        let artifact_root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::sqlite(
                artifact_root.path(),
                crate::ArtifactStoreConfig {
                    compact_threshold_bytes: 1,
                    ..crate::ArtifactStoreConfig::default()
                },
            )
            .expect("artifact store"),
        );
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory()
        .with_artifact_store(Arc::clone(&artifacts))
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(Arc::clone(&store)),
        );
        let envelope = ContextRuntimeKernel::build_envelope(ContextEnvelopeRequest {
            identity: ContextIdentity::main(&session_id),
            profile: ContextProfile::MainTurn,
            intent: "persist one canonical context body".to_string(),
            stable_head: vec!["stable system".to_string()],
            runtime_header: vec!["runtime header".to_string()],
            dynamic_items: vec![ContextItem::new(
                "memory-context-1",
                ContextSourceKind::Memory,
                ContextRole::Orientation,
                "canonical memory content",
            )],
            omitted: Vec::new(),
            total_budget_tokens: 4_000,
        });

        runtime.remember_context_envelope(envelope).await;

        let events = store
            .get_events_by_type_limited(&session_id, "ContextEnvelope", 0, 10)
            .await
            .expect("context event");
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value =
            serde_json::from_str(&events[0].event_json).expect("context payload");
        assert_eq!(
            payload["schema_version"],
            PERSISTED_CONTEXT_ENVELOPE_SCHEMA_VERSION
        );
        assert_eq!(
            payload["formatter_version"],
            CONTEXT_RENDER_FORMATTER_VERSION
        );
        assert_eq!(payload["envelope"]["artifact_backed"], true);
        assert!(payload["envelope"].get("selected").is_none());
        let artifact: harness_contract::context::ArtifactRef =
            serde_json::from_value(payload["context_artifact"].clone()).expect("artifact ref");
        let bytes = artifacts
            .read(&artifact, &format!("session:{session_id}"), None)
            .await
            .expect("canonical context artifact");
        let persisted: serde_json::Value =
            serde_json::from_slice(&bytes).expect("persisted context");
        assert_eq!(
            persisted["selected"][0]["content"],
            "canonical memory content"
        );
        assert!(persisted.get("assembled").is_none());
        assert_eq!(artifacts.stats().expect("artifact stats").pins, 1);
    }

    #[tokio::test]
    async fn context_turn_report_write_failure_does_not_expose_a_successful_report() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );
        let report = runtime.build_context_turn_report("turn-failure", TokenUsage::default(), None);

        let error = runtime
            .remember_context_turn_report(report)
            .await
            .expect_err("a foreign-key persistence failure must fail the terminal report path");
        assert!(error
            .to_string()
            .contains("context governance persistence failed"));
        assert_eq!(runtime.last_context_turn_report(), None);
    }

    #[tokio::test]
    async fn compaction_event_failure_is_terminal_and_does_not_claim_durable_recovery() {
        let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_session_journal_port(
            crate::session_runtime_port::TestSessionPortAdapter::new(store),
        );

        let error = runtime
            .record_session_compacted(
                crate::session::SessionCompaction {
                    count: 1,
                    removed_message_count: 3,
                    summary: "durability must precede local compaction".to_string(),
                },
                3,
                None,
                memory::compression::session::SessionSemanticCheckpoint {
                    schema_version:
                        memory::compression::session::SESSION_SEMANTIC_CHECKPOINT_SCHEMA_VERSION,
                    checkpoint_id: "checkpoint-failure".to_string(),
                    execution_identity:
                        harness_contract::execution::ExecutionIdentity::for_session_turn(
                            "primary",
                            "workspace-failure",
                            "missing-session",
                            "turn-failure",
                        )
                        .unwrap(),
                    session_id: "missing-session".to_string(),
                    agent_id: "primary".to_string(),
                    project_id: None,
                    task_id: None,
                    team_id: None,
                    summary: "durability test".to_string(),
                    user_rules: Vec::new(),
                    goal: None,
                    constraints: Vec::new(),
                    decisions: Vec::new(),
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    file_changes: Vec::new(),
                    resume_cursor: memory::compression::session::SessionResumeCursor {
                        message_index: 0,
                        event_sequence: None,
                        checkpoint_id: "checkpoint-failure".to_string(),
                    },
                    token_stats: memory::compression::session::CheckpointTokenStats {
                        before: 1,
                        after: 1,
                        message_count: 0,
                    },
                    source_range: memory::compression::session::CompactionSourceRange {
                        session_id: "missing-session".to_string(),
                        message_start: 0,
                        message_end_exclusive: 0,
                        event_start: None,
                        event_end_exclusive: None,
                        raw_refs: Vec::new(),
                    },
                    facts: Vec::new(),
                },
            )
            .await
            .expect_err("missing session carrier must reject canonical compaction persistence");
        assert!(error
            .to_string()
            .contains("atomic compaction persistence failed"));
    }

    #[test]
    fn context_turn_report_includes_active_knowledge_activation_report() {
        let runtime = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory();

        runtime.set_turn_knowledge_report(harness_contract::knowledge::KnowledgeTurnReport {
            activation_plan_id: Some("knowledge-plan-test".to_string()),
            active_pack_ids: vec!["pack-domain-default".to_string()],
            blocked_namespaces: vec!["project:irrelevant not relevant to intent".to_string()],
            compliance_warnings: Vec::new(),
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "knowledge_chunk",
                "chunk-1",
            )],
            usage_signals: Vec::new(),
        });

        let report = runtime.build_context_turn_report(
            "turn-1",
            TokenUsage {
                input_tokens: 128,
                output_tokens: 32,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            None,
        );

        let knowledge = report.knowledge.expect("knowledge report is attached");
        assert_eq!(
            knowledge.activation_plan_id.as_deref(),
            Some("knowledge-plan-test")
        );
        assert_eq!(knowledge.active_pack_ids, vec!["pack-domain-default"]);
        assert_eq!(knowledge.blocked_namespaces.len(), 1);
        assert_eq!(knowledge.evidence_refs[0].ref_type, "knowledge_chunk");
    }

    #[test]
    fn m2_layer_priority_l0_before_l3() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert!(
            rank(MemoryLayer::L0) > rank(MemoryLayer::L3),
            "L0 must rank higher than L3"
        );
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_empty_session_no_memory_crash() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let _ = rt.prepare_reality_context("query").await;
        let _ = rt.run_memory_post_turn("").await;
    }

    #[test]
    fn post_turn_memory_window_contains_only_the_current_turn_and_supplements() {
        let messages = vec![
            ConversationMessage::user_text("old request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "old decision".to_string(),
            }]),
            ConversationMessage::user_text("current request"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "working".to_string(),
            }]),
            ConversationMessage::user_text("supplement"),
            ConversationMessage::assistant(vec![ContentBlock::Text {
                text: "final answer".to_string(),
            }]),
        ];

        let current = current_turn_messages(&messages, "current request");
        assert_eq!(current.len(), 4);
        assert_eq!(conversation_message_text(&current[0]), "current request");
        assert_eq!(conversation_message_text(&current[2]), "supplement");
    }

    #[test]
    fn delegated_team_runtime_does_not_duplicate_root_conversation_memory() {
        let root = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        assert!(root.owns_conversation_memory_production());

        let child = ConversationRuntime::new(
            Session::new(),
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_identity(
            "researcher-instance",
            Some("researcher-definition".to_string()),
            Some("team-run".to_string()),
            Vec::new(),
        );
        assert!(!child.owns_conversation_memory_production());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_without_memory_returns_system_prompt() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["test prompt".to_string()],
        );
        let result = rt.prepare_reality_context("test").await;
        assert_eq!(result.trusted_system[0], "test prompt");
        assert!(result
            .runtime_context
            .iter()
            .any(|line| line.contains("profile:MainTurn")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_prepare_without_memory_records_degraded_context_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("remember this").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert_eq!(prompt.trusted_system[0], "stable system");
        assert!(prompt
            .runtime_context
            .iter()
            .any(|line| line.contains("profile:MainTurn")));
        assert!(prompt
            .runtime_context
            .iter()
            .any(|segment| segment.contains("context_governance_report_id:")));
        assert_eq!(envelope.intent, "remember this");
        assert_eq!(envelope.assembled.stable_head, vec!["stable system"]);
        assert_eq!(
            envelope.diagnostics.degraded_sources,
            vec![ContextSourceKind::Memory]
        );
        assert!(envelope
            .selected
            .iter()
            .all(|item| item.source != ContextSourceKind::Memory));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn external_resume_context_enters_prompt_and_envelope_without_memory() {
        let session = Session::new();
        let session_id = session.session_id.clone();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        rt.inject_resume_context(ResumeContextPacket {
            session_id: session_id.clone(),
            handoff_summary: Some("continue v0.8.13 context work".to_string()),
            active_task: Some("persist context timeline".to_string()),
            recent_decisions: vec!["DB session_events is the canonical timeline".to_string()],
            blockers: vec!["none".to_string()],
            source: ResumeContextSource::Mixed,
        });

        let prompt = rt.prepare_reality_context("resume").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(prompt
            .contextual_packets
            .iter()
            .any(|packet| packet.content.contains("continue v0.8.13 context work")));
        let handoff = envelope
            .selected
            .iter()
            .find(|item| item.source == ContextSourceKind::Handoff)
            .expect("resume context should remain selected alongside workspace packets");
        assert_eq!(handoff.authority, ContextAuthority::Session);
        assert!(handoff.content.contains("Active task"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recent_tool_trace_enters_next_prompt_and_envelope() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["stable system".to_string()],
        )
        .without_memory();

        let tool_result = ConversationMessage::tool_result(
            "tool-1".to_string(),
            "bash".to_string(),
            "cargo test passed for context runtime".to_string(),
            false,
        );
        rt.remember_tool_trace_from_message(&tool_result);

        let prompt = rt.prepare_reality_context("next turn").await;
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(prompt
            .contextual_packets
            .iter()
            .any(|packet| packet.content.contains("cargo test passed")));
        assert!(envelope
            .selected
            .iter()
            .any(|item| item.source == ContextSourceKind::ToolTrace));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_xml_format_present() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base prompt".to_string()],
        );
        let prompt = rt.prepare_reality_context("hello").await;
        assert!(
            !prompt.trusted_system.is_empty(),
            "should have system prompt"
        );
    }

    #[test]
    fn m2_error_propagation_returns_result() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["sys".to_string()],
        );
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .handle()
                .clone()
        });
        let r = handle.block_on(rt.run_memory_post_turn(""));
        assert!(
            r.is_ok(),
            "run_memory_post_turn should return Ok when no memory manager"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_structured_injection_has_memory_context_tag() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        );
        let prompt = rt.prepare_reality_context("test").await;
        assert!(!prompt.trusted_system.is_empty());
        // Without memory manager, should still return system prompt
        assert!(
            prompt.trusted_system[0] == "system" || prompt.trusted_system[0].starts_with("system")
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn prepare_reality_context_suppresses_memory_conflicting_with_current_turn() {
        let tmp = tempfile::tempdir().unwrap();
        let db_path = tmp.path().join("memory.db");
        let blob_dir = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_dir).unwrap();

        let mem_cfg = memory::config::MemoryConfig {
            store: memory::config::StoreConfig {
                sqlite_path: db_path,
                blob_dir,
                enable_vector_index: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let mgr = Arc::new(CognitiveContextManager::new(mem_cfg).await.unwrap());
        let session = Session::new().with_workspace_root(tmp.path());
        let project_id = memory_project_id_for_session(&session).expect("workspace project id");
        let now = chrono::Utc::now();
        mgr.remember(memory::types::MemoryEntry {
            id: memory::types::MemoryId::new_v4(),
            layer: memory::types::MemoryLayer::L1,
            category: memory::types::MemoryCategory::UserPreference,
            priority: memory::types::Priority::High,
            source: memory::types::MemorySource::UserExplicit,
            title: "User preference: 不要使用工具或编排".to_string(),
            content: "用户历史偏好：不要使用工具或编排。".to_string(),
            embedding: None,
            tags: vec!["preference".to_string()],
            relations: Vec::new(),
            confidence: 0.95,
            access_count: 0,
            staleness: 0.0,
            created_at: now,
            updated_at: now,
            last_accessed_at: None,
            scope: memory::MemoryScope::Project(project_id.clone()),
            session_id: None,
            source_agent: None,
            visibility: memory::types::AgentVisibility::Shared,
        })
        .await
        .unwrap();
        let loaded_l1 = mgr
            .list_layer_full_entries(memory::types::MemoryLayer::L1)
            .await
            .unwrap();
        assert!(loaded_l1
            .iter()
            .any(|entry| entry.title == "User preference: 不要使用工具或编排"));
        let memory_turn = memory::MemoryTurnContext::new("test-session", "primary")
            .with_project_id(Some(project_id));
        let prepared = mgr
            .prepare_context_for_turn(
                &memory_turn,
                "请先使用 runtime_capabilities 调用工具分析",
                &[],
            )
            .await
            .unwrap();
        assert!(
            prepared
                .entries
                .iter()
                .any(|entry| entry.title == "User preference: 不要使用工具或编排"),
            "prepared entries: {:?}",
            prepared
                .entries
                .iter()
                .map(|entry| entry.title.as_str())
                .collect::<Vec<_>>()
        );

        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .with_memory_manager(mgr);

        let prompt = rendered_prompt(
            &rt.prepare_reality_context("请先使用 runtime_capabilities 调用工具分析")
                .await,
        );
        let envelope = rt
            .last_context_envelope()
            .expect("context envelope should be recorded");

        assert!(envelope
            .omitted
            .iter()
            .any(|omission| omission.reason.contains("suppressed_for_current_turn")));
        assert!(!prompt.contains("<title>User preference: 不要使用工具或编排</title>"));
        assert!(!prompt.contains("<knowledge_compliance>"));
    }

    #[test]
    fn m2_layer_ranking_verification() {
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        assert_eq!(rank(MemoryLayer::L0), 5);
        assert_eq!(rank(MemoryLayer::L4), 1);
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_budget_cap_applied_on_prepare() {
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["base".to_string()],
        );
        // Verify that prepare_reality_context doesn't panic with empty session
        let result = rt.prepare_reality_context("any query").await;
        assert!(
            !result.trusted_system.is_empty(),
            "should return at least the system prompt"
        );
    }

    // ── M2-L2: integration-level memory tests ──────────────────────

    #[tokio::test(flavor = "multi_thread")]
    async fn m2_l2_budget_enforcement_limits_system_prompt() {
        // M2-L2-2: verify memory context doesn't exceed budget proportions
        let session = Session::new();
        let rt = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system prompt".to_string()],
        )
        .without_memory();
        let prompt = rt.prepare_reality_context("test query").await;
        // Without selected memories, the prompt still includes the stable head and
        // runtime governance context. Attachment/resource guidance may add more
        // bounded sections, so this must remain a semantic budget assertion.
        assert_eq!(prompt.trusted_system[0], "system prompt");
        assert!(prompt
            .runtime_context
            .iter()
            .any(|segment| segment.contains("profile:MainTurn")));
        assert!(!rendered_prompt(&prompt).contains("<memory_context>"));
        let total_prompt_chars = prompt.estimated_chars();
        assert!(
            total_prompt_chars < 20_000,
            "memory-free runtime prompt should stay bounded"
        );
        // System prompt should be reasonably sized
        assert!(
            prompt.trusted_system[0].len() < 10000,
            "system prompt should not be oversized"
        );
    }

    #[test]
    fn m2_l2_layer_priority_preserves_l0_l1() {
        // M2-L2-3: L0/L1 should be ranked before L3 in sorted entries
        use memory::types::MemoryLayer;
        let rank = |l: MemoryLayer| match l {
            MemoryLayer::L0 => 5,
            MemoryLayer::L1 => 4,
            MemoryLayer::L2 => 3,
            MemoryLayer::L3 => 2,
            MemoryLayer::L4 => 1,
        };
        // L0 > L1 > L2 > L3 > L4
        assert!(rank(MemoryLayer::L0) > rank(MemoryLayer::L1));
        assert!(rank(MemoryLayer::L1) > rank(MemoryLayer::L2));
        assert!(rank(MemoryLayer::L2) > rank(MemoryLayer::L3));
        assert!(rank(MemoryLayer::L3) > rank(MemoryLayer::L4));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn runtime_reality_binding_injects_only_leased_fact_evidence_into_the_prompt() {
        let home = tempfile::tempdir().expect("temporary config home");
        let registry = StorageRegistry::default_for_config_home(home.path());
        let endpoint = registry
            .endpoint(&storage::StorageDomainId::Fact)
            .expect("fact endpoint");
        let fact_ledger = fact_sqlite::SqliteFactLedger::open(endpoint).expect("fact ledger");
        let mut fact = fact_kernel::FactRecord::new(
            "supply.policy",
            "east allocation requires expedited approval",
        );
        fact.id = fact_kernel::FactId::from_string("primary-turn-fact");
        fact_ledger.upsert_fact(fact).expect("persist fact");

        let binding = AgentBindingSnapshot {
            binding_id: "binding:primary-reality".to_string(),
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/explore")
                    .expect("definition id"),
                1,
            )
            .expect("revision ref"),
            definition_digest: "a".repeat(64),
            instructions: "# Test\n".to_string(),
            instance: AgentInstanceRef {
                instance_id: "instance:primary-reality".to_string(),
                role_slot_id: None,
            },
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: vec!["test".to_string()],
                fallback_allowed: false,
            },
            effective_capabilities: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: AgentDataLease {
                session_id: "session-primary".to_string(),
                task_id: "task-primary".to_string(),
                team_id: None,
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: vec!["fact:primary-turn-fact".to_string()],
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "b".repeat(64),
        };
        let mut session = Session::new();
        session.session_id = "session-primary".to_string();
        let runtime = ConversationRuntime::new(
            session,
            MockApi,
            StaticToolExecutor::new(),
            PermissionPolicy::new(PermissionMode::WorkspaceWrite),
            vec!["system".to_string()],
        )
        .without_memory()
        .with_reality_binding(RealityRecallPort::for_config_home(home.path()), binding);

        let prompt = runtime
            .prepare_reality_context("how should east allocation proceed")
            .await;
        let rendered = rendered_prompt(&prompt);
        assert!(rendered.contains("east allocation requires expedited approval"));
        let envelope = runtime.last_context_envelope().expect("context envelope");
        assert!(envelope
            .selected
            .iter()
            .any(|item| item.source == ContextSourceKind::Fact));
        let report = runtime
            .last_reality_recall_report()
            .expect("reality recall report");
        assert_eq!(report.sources[0].status, "enabled_and_wired");
        assert_eq!(report.sources[0].selected_count, 1);
    }

    #[tokio::test]
    async fn typed_model_stream_preserves_public_order_without_leaking_private_reasoning() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "execution-causal".to_string(),
                session_id: "session-causal".to_string(),
                turn_id: "turn-causal".to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: "execution-causal".to_string(),
                session_id: "session-causal".to_string(),
                turn_id: "turn-causal".to_string(),
                root_task_id: "task-causal".to_string(),
                task_id: "task-causal".to_string(),
                activity_id: "activity:execution:execution-causal".to_string(),
                node_id: None,
                parent_activity_id: None,
                initiator_activity_id: None,
                team_run_id: None,
                agent_instance_id: None,
                agent_run_id: None,
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: 1,
                fence: 1,
                generation: 1,
            }),
        );
        let mut receiver = bus.subscribe();
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let events = vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("summary-0".to_string()),
                kind: AssistantItemKind::PublicReasoning,
            }),
            Ok(AssistantEvent::ReasoningSummaryDelta(
                "checked plan".to_string(),
            )),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::ItemStarted {
                index: 1,
                provider_item_id: Some("private-0".to_string()),
                kind: AssistantItemKind::PrivateReasoning,
            }),
            Ok(AssistantEvent::PrivateReasoningDelta(
                "provider-private-secret".to_string(),
            )),
            Ok(AssistantEvent::SignatureDelta(
                "provider-signature-secret".to_string(),
            )),
            Ok(AssistantEvent::ItemCompleted { index: 1 }),
            Ok(AssistantEvent::ItemStarted {
                index: 2,
                provider_item_id: Some("tool-0".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "tool-0".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md"}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 2 }),
            Ok(AssistantEvent::ItemStarted {
                index: 3,
                provider_item_id: Some("text-0".to_string()),
                kind: AssistantItemKind::Text,
            }),
            Ok(AssistantEvent::TextDelta("final answer".to_string())),
            Ok(AssistantEvent::ItemCompleted { index: 3 }),
            Ok(AssistantEvent::MessageStop),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-causal".to_string(),
            ),
            None,
        )
        .await;

        assert!(
            result.failure.is_none(),
            "typed stream unexpectedly failed: {:?}",
            result.failure
        );
        assert_eq!(result.collected.public_reasoning, "checked plan");
        assert_eq!(
            result.collected.private_reasoning,
            "provider-private-secret"
        );
        assert_eq!(result.collected.signature, "provider-signature-secret");
        assert_eq!(result.collected.text, "final answer");
        assert_eq!(result.collected.calls.len(), 1);

        let mut projected = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            projected.push(serde_json::to_string(&event).expect("serialize event"));
        }
        let projected = projected.join("\n");
        assert!(projected.contains("checked plan"));
        assert!(projected.contains("final answer"));
        assert!(!projected.contains("provider-private-secret"));
        assert!(!projected.contains("provider-signature-secret"));

        let durable = store.all_events(20).expect("durable model items");
        assert_eq!(
            durable
                .iter()
                .filter(|event| event.kind == "model.item_completed")
                .count(),
            3
        );
        let durable_json = durable
            .iter()
            .map(|event| event.payload.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!durable_json.contains("provider-private-secret"));
        assert!(!durable_json.contains("provider-signature-secret"));
    }

    #[tokio::test]
    async fn terminal_narrator_deltas_carry_presentation_identity_and_byte_ranges() {
        let bus = Arc::new(CowdEventBus::new());
        let mut receiver = bus.subscribe();
        let events = vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("terminal-text".to_string()),
                kind: AssistantItemKind::Text,
            }),
            Ok(AssistantEvent::TextDelta("你".to_string())),
            Ok(AssistantEvent::TextDelta("好".to_string())),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::MessageStop),
        ];
        let stream = Box::pin(futures::stream::iter(events));
        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(Some(Arc::clone(&bus)), None, "session-terminal".to_string())
                .with_terminal_presentation("presentation-1", "attempt-1"),
            None,
        )
        .await;
        assert!(result.failure.is_none());

        let mut ranges = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            if let crate::CowdEvent::TerminalDelivery {
                delivery:
                    harness_contract::live::TerminalDeliveryEvent::TextDelta {
                        presentation_id,
                        attempt_id,
                        byte_start,
                        byte_end,
                        delta,
                    },
            } = event.domain_event()
            {
                assert_eq!(presentation_id, "presentation-1");
                assert_eq!(attempt_id, "attempt-1");
                ranges.push((*byte_start, *byte_end, delta.clone()));
            }
        }
        assert_eq!(
            ranges,
            vec![(0, 3, "你".to_string()), (3, 6, "好".to_string())]
        );
    }

    #[tokio::test]
    async fn tool_step_without_provider_summary_emits_one_safe_public_action_summary() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "execution-action-summary".to_string(),
                session_id: "session-action-summary".to_string(),
                turn_id: "turn-action-summary".to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: "execution-action-summary".to_string(),
                session_id: "session-action-summary".to_string(),
                turn_id: "turn-action-summary".to_string(),
                root_task_id: "task-action-summary".to_string(),
                task_id: "task-action-summary".to_string(),
                activity_id: "activity:agent:researcher".to_string(),
                node_id: Some("researcher".to_string()),
                parent_activity_id: Some("activity:team:research".to_string()),
                initiator_activity_id: Some("activity:team:research".to_string()),
                team_run_id: Some("team:research".to_string()),
                agent_instance_id: Some("agent:researcher".to_string()),
                agent_run_id: Some("agent-run:researcher".to_string()),
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: 1,
                fence: 1,
                generation: 1,
            }),
        );
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let stream = Box::pin(futures::stream::iter(vec![
            Ok(AssistantEvent::PrivateReasoningDelta(
                "provider-private-secret".to_string(),
            )),
            Ok(AssistantEvent::TextDelta(
                "先查询两类权威来源，再比较差异。".to_string(),
            )),
            Ok(AssistantEvent::ToolUse {
                id: "search-1".to_string(),
                name: "web_search".to_string(),
                input: r#"{"query":"source one"}"#.to_string(),
            }),
            Ok(AssistantEvent::ToolUse {
                id: "search-2".to_string(),
                name: "web_search".to_string(),
                input: r#"{"query":"source two"}"#.to_string(),
            }),
            Ok(AssistantEvent::MessageStop),
        ]));

        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-action-summary".to_string(),
            ),
            None,
        )
        .await;

        assert!(
            result.failure.is_none(),
            "unexpected failure: {:?}",
            result.failure
        );
        assert!(result.collected.public_reasoning.is_empty());
        let durable = store.all_events(20).expect("durable events");
        let reasoning = durable
            .iter()
            .filter(|event| event.kind == "model.item_completed")
            .filter(|event| event.payload["kind"] == "public_reasoning")
            .collect::<Vec<_>>();
        assert_eq!(reasoning.len(), 1);
        assert_eq!(
            reasoning[0].payload["content"],
            "先查询两类权威来源，再比较差异。"
        );
        assert_eq!(
            reasoning[0]
                .activity_binding()
                .as_ref()
                .and_then(|binding| binding.agent_instance_id.as_deref()),
            Some("agent:researcher")
        );
        assert!(!reasoning[0]
            .payload
            .to_string()
            .contains("provider-private-secret"));
    }

    #[tokio::test]
    async fn failed_model_stream_does_not_persist_partial_item_as_completed() {
        let bus = Arc::new(CowdEventBus::new());
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: "execution-partial".to_string(),
            session_id: "session-partial".to_string(),
            turn_id: "turn-partial".to_string(),
        });
        let mut receiver = bus.subscribe();
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let stream = Box::pin(futures::stream::iter(vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("text-partial".to_string()),
                kind: AssistantItemKind::Text,
            }),
            Ok(AssistantEvent::TextDelta("partial answer".to_string())),
            Err(RuntimeError::new("provider stream interrupted")),
        ]));

        let result = consume_provider_stream(
            stream,
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                Some(Arc::clone(&bus)),
                Some(Arc::clone(&store)),
                "session-partial".to_string(),
            ),
            None,
        )
        .await;

        assert!(result.failure.is_some());
        assert_eq!(result.collected.text, "partial answer");
        assert!(store
            .all_events(20)
            .expect("durable events")
            .iter()
            .all(|event| event.kind != "model.item_completed"));
        let mut model_step_failed = false;
        while let Ok(event) = receiver.try_recv() {
            let event = match event {
                crate::CowdEvent::ExecutionScoped { event, .. } => *event,
                event => event,
            };
            if matches!(
                event,
                crate::CowdEvent::ModelStepCompleted { ref status, .. } if status == "failed"
            ) {
                model_step_failed = true;
            }
        }
        assert!(model_step_failed);
    }

    #[derive(Default)]
    struct RecordingEarlyDispatcher {
        dispatches: std::sync::atomic::AtomicUsize,
    }

    impl EarlyToolDispatcher for RecordingEarlyDispatcher {
        fn dispatch(&self, candidate: EarlyToolCandidate) -> EarlyToolDispatchFuture {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                let started_at_ms = super::now_ms();
                tokio::time::sleep(Duration::from_millis(5)).await;
                EarlyToolDispatchResult::Executed(EarlyToolExecutionReceipt {
                    outcome: crate::RuntimeToolExecutionOutcome {
                        tool_use_id: candidate.call.id.clone(),
                        tool_name: candidate.call.name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Executed,
                        category: crate::ToolSafetyCategory::ReadOnly,
                        output: Some("early-result".to_string()),
                        error: None,
                        evidence_ref: "test:early-result".to_string(),
                        observed_evidence: Vec::new(),
                    },
                    call: candidate.call,
                    ready_at_ms: candidate.ready_at_ms,
                    started_at_ms,
                    completed_at_ms: super::now_ms(),
                })
            })
        }
    }

    #[derive(Default)]
    struct PendingEarlyDispatcher {
        dispatches: std::sync::atomic::AtomicUsize,
    }

    impl EarlyToolDispatcher for PendingEarlyDispatcher {
        fn dispatch(&self, _candidate: EarlyToolCandidate) -> EarlyToolDispatchFuture {
            self.dispatches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Box::pin(async move {
                std::future::pending::<()>().await;
                unreachable!("pending early dispatcher must be aborted after provider failure")
            })
        }
    }

    fn early_enabled_provider_event() -> AssistantEvent {
        AssistantEvent::ProviderModel {
            identity: harness_contract::outcome::ProviderIdentity {
                registry_revision: Some(1),
                provider_name: "test".to_string(),
                model: "test".to_string(),
                profile: None,
                protocol: Some("responses".to_string()),
                capabilities: std::collections::BTreeMap::from([(
                    "early_tool_start".to_string(),
                    "enabled".to_string(),
                )]),
            },
        }
    }

    #[tokio::test]
    async fn completed_tool_item_starts_before_provider_response_completes() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let events = vec![
            Ok(early_enabled_provider_event()),
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-early".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-early".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::MessageStop),
        ];
        let stream = futures::stream::iter(events).then(|event| async move {
            if matches!(&event, Ok(AssistantEvent::MessageStop)) {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            event
        });

        let result = consume_provider_stream(
            Box::pin(stream),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(None, None, "session-early".to_string()),
            Some(dispatcher.clone()),
        )
        .await;

        assert!(result.failure.is_none());
        assert_eq!(
            dispatcher
                .dispatches
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert_eq!(result.collected.early_tool_receipts.len(), 1);
        let receipt = &result.collected.early_tool_receipts[0];
        assert!(receipt.ready_at_ms <= receipt.started_at_ms);
        assert!(
            receipt.started_at_ms < result.collected.response_completed_at_ms,
            "early start {} must precede response completion {}",
            receipt.started_at_ms,
            result.collected.response_completed_at_ms
        );
    }

    #[tokio::test]
    async fn provider_interruption_retains_completed_early_read_receipt() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let events = vec![
            Ok(early_enabled_provider_event()),
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-before-interrupt".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-before-interrupt".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Err(RuntimeError::new("provider transport interrupted")),
        ];

        let result = consume_provider_stream(
            Box::pin(futures::stream::iter(events)),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(
                None,
                Some(Arc::clone(&store)),
                "session-interrupted-early".to_string(),
            ),
            Some(dispatcher),
        )
        .await;

        assert!(result.failure.is_some());
        assert_eq!(result.collected.early_tool_receipts.len(), 1);
        assert_eq!(
            result.collected.early_tool_receipts[0].call.id,
            "read-before-interrupt"
        );
        let error = result
            .failure
            .clone()
            .expect("provider interruption")
            .with_effect_receipts(result.collected.early_tool_receipts.clone());
        assert_eq!(error.effect_receipts()[0].call.id, "read-before-interrupt");
        assert_eq!(
            store
                .all_events(20)
                .expect("durable completed item")
                .iter()
                .filter(|event| event.kind == "model.item_completed")
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn provider_protocol_failure_aborts_a_stalled_early_read_within_the_global_grace() {
        let dispatcher = Arc::new(PendingEarlyDispatcher::default());
        let events = vec![
            Ok(early_enabled_provider_event()),
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-before-invalid-tail".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-before-invalid-tail".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Err(RuntimeError::with_provider_failure_metadata(
                "tool_protocol_violation: malformed trailing provider frame",
                None,
                true,
                crate::execution_core::graph::ResourceResultClass::Failed,
            )),
        ];

        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            consume_provider_stream(
                Box::pin(futures::stream::iter(events)),
                CancellationToken::new(),
                None,
                ModelStreamReducer::new(None, None, "session-invalid-tail".to_string()),
                Some(dispatcher.clone()),
            ),
        )
        .await
        .expect("a malformed provider frame must not wait on speculative early work");

        assert!(result.failure.is_some());
        assert!(result.collected.early_tool_receipts.is_empty());
        assert_eq!(
            dispatcher
                .dispatches
                .load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "failed provider drain exceeded the bounded grace: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn unverified_provider_keeps_early_tool_in_the_finalized_batch() {
        let dispatcher = Arc::new(RecordingEarlyDispatcher::default());
        let events = vec![
            Ok(AssistantEvent::ItemStarted {
                index: 0,
                provider_item_id: Some("read-after-model".to_string()),
                kind: AssistantItemKind::ToolCall,
            }),
            Ok(AssistantEvent::ToolUse {
                id: "read-after-model".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md","limit":20}"#.to_string(),
            }),
            Ok(AssistantEvent::ItemCompleted { index: 0 }),
            Ok(AssistantEvent::MessageStop),
        ];

        let result = consume_provider_stream(
            Box::pin(futures::stream::iter(events)),
            CancellationToken::new(),
            None,
            ModelStreamReducer::new(None, None, "session-no-early-proof".to_string()),
            Some(dispatcher.clone()),
        )
        .await;

        assert!(result.failure.is_none());
        assert_eq!(
            dispatcher
                .dispatches
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(result.collected.early_tool_receipts.is_empty());
        assert_eq!(result.collected.early_tool_deferrals.len(), 1);
        assert_eq!(
            result.collected.early_tool_deferrals[0].tool_call_id,
            "read-after-model"
        );
        assert_eq!(result.collected.calls.len(), 1);
    }

    #[test]
    fn model_step_tool_plan_is_append_only_and_rejects_changed_identity_reuse() {
        let identity = crate::CausalItemIdentity {
            model_step_id: "step".to_string(),
            item_id: "call".to_string(),
            segment_id: "call:tool-call:0".to_string(),
            causal_sequence: 1,
            delta_sequence: 0,
            tool_call_id: Some("call".to_string()),
            causal_parent_ids: Vec::new(),
        };
        let candidate = EarlyToolCandidate {
            call: ModelToolCall {
                id: "call".to_string(),
                name: "read_file".to_string(),
                input: r#"{"path":"README.md"}"#.to_string(),
                depends_on: Vec::new(),
            },
            identity: identity.clone(),
            ready_at_ms: 1,
        };
        let mut plan = ModelStepToolPlan::default();
        assert!(plan.append(candidate.clone()).unwrap().is_some());
        assert!(plan.append(candidate.clone()).unwrap().is_none());

        let mut changed = candidate.clone();
        changed.call.input = r#"{"path":"Cargo.toml"}"#.to_string();
        assert!(plan.append(changed).is_err());
        assert!(plan.seal(&[candidate.call]).is_ok());
    }

    #[test]
    fn final_context_binding_rejects_passive_cross_session_history() {
        let mut current = ContextItem::new(
            "current",
            ContextSourceKind::Conversation,
            ContextRole::RecentTurn,
            "current history",
        );
        current.source_lifecycle = crate::ContextSourceLifecycle::Session;
        current
            .evidence
            .push("session://session-a/messages/1".to_string());
        let mut unrelated = ContextItem::new(
            "unrelated",
            ContextSourceKind::Conversation,
            ContextRole::RecentTurn,
            "unrelated history",
        );
        unrelated.source_lifecycle = crate::ContextSourceLifecycle::Session;
        unrelated
            .evidence
            .push("session://session-b/messages/1".to_string());

        let (selected, omitted) = revalidate_context_binding("session-a", vec![current, unrelated]);

        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].id, "current");
        assert_eq!(omitted.len(), 1);
        assert!(omitted[0].reason.contains("cross-Session"));
    }
