    #[tokio::test]
    async fn write_team_downgrade_retargets_registered_parent_to_execute_topology() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let mut current = ExecutionGraphCompiler
            .compile_conversation_turn(ExecutionCompileRequest {
                objective: "write Team fallback".to_string(),
                payload_ref: serde_json::json!({
                    "session_id": "retarget-session",
                    "compile_target": "evidence_graph",
                })
                .to_string(),
                target: crate::execution_core::RuntimeCompileTarget::EvidenceGraph,
                resource_scopes: Vec::new(),
            })
            .expect("initial Team parent graph");
        crate::test_support::attach_execution_graph_lineage(&mut current);
        let registered = services
            .execution_supervisor()
            .register_graph(current)
            .await
            .expect("registered initial graph");
        let stable_parent = registered.nodes.first().expect("initial root").id.clone();
        let replacement = compile_retargeted_conversation_graph(
            &registered,
            "write Team fallback",
            "retarget-session",
            None,
            crate::execution_core::RuntimeCompileTarget::ExecutionGraph,
            &stable_parent,
        )
        .expect("replacement topology");
        services
            .commit_service()
            .retarget_planned_graph_async(
                registered.clone(),
                replacement,
                "Team start unavailable; execute governed fallback".to_string(),
            )
            .await
            .expect("retarget commit");
        let retargeted = services
            .graph_state_store()
            .load(&registered.id)
            .expect("retargeted graph");

        assert_eq!(retargeted.revision, registered.revision + 1);
        assert_eq!(retargeted.id, registered.id);
        assert!(retargeted.nodes.iter().any(|node| {
            node.acceptance
                .criteria
                .contains(&"permission_and_policy_gate_required".to_string())
        }));
        assert!(retargeted.nodes.iter().any(|node| {
            node.acceptance
                .criteria
                .contains(&"mutation_resources_must_be_leased".to_string())
        }));
        assert!(!retargeted.nodes.iter().any(|node| {
            node.acceptance
                .criteria
                .contains(&"evidence_read_before_synthesis".to_string())
        }));
    }
    #[tokio::test(flavor = "multi_thread")]
    async fn model_step_only_plans_then_runner_executes_dependent_tool_wave_once() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let executed = Arc::new(AtomicUsize::new(0));
        let executions_seen_before_second_model = Arc::new(AtomicUsize::new(0));
        let order = Arc::new(Mutex::new(Vec::new()));
        let session = Session::new();
        let session_store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
        session_store
            .create_session(&session::SessionRecord {
                session_id: session.session_id.clone(),
                platform: "test".to_string(),
                chat_id: "dependent-wave".to_string(),
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
        let cowd_bus = crate::CowdEventBus::new();
        let _execution_scope = cowd_bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "test-root-execution".to_string(),
                session_id: session.session_id.clone(),
                turn_id: "test-turn".to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id: "test-root-execution".to_string(),
                session_id: session.session_id.clone(),
                turn_id: "test-turn".to_string(),
                root_task_id: "test-root-task".to_string(),
                task_id: "test-root-task".to_string(),
                activity_id: "activity:execution:test-root-execution".to_string(),
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
        let runtime = crate::ConversationRuntime::new(
            session,
            TwoToolClient {
                requests: 0,
                executed: Arc::clone(&executed),
                executions_seen_before_second_model: Arc::clone(
                    &executions_seen_before_second_model,
                ),
            },
            RecordingToolExecutor {
                executed: Arc::clone(&executed),
                order: Arc::clone(&order),
            },
            PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
            vec!["use requested tools".to_string()],
        )
        .without_memory()
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            session_store,
        ))
        .with_artifact_store(Arc::clone(services.artifact_store()))
        .with_cowd_event_bus(cowd_bus);

        let (_runtime, result) = submit_test_owned_conversation_turn(
            runtime,
            Arc::clone(&services),
            "read then update src/lib.rs",
            &SharedPrompter::none(),
            test_execution_lineage(),
        )
        .await;
        let summary = result.expect("turn result");

        assert_eq!(executed.load(Ordering::SeqCst), 2);
        assert_eq!(
            executions_seen_before_second_model.load(Ordering::SeqCst),
            2
        );
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["read_file", "write_file"]
        );
        assert_eq!(
            summary.tool_results.len(),
            3,
            "the durable turn trace includes the bootstrap tool_search receipt plus two authorized operations"
        );
        assert!(summary
            .tool_results
            .iter()
            .flat_map(|message| message.blocks.iter())
            .any(|block| matches!(block, crate::ContentBlock::ToolResult { tool_name, .. } if tool_name == "tool_search")));
        assert_eq!(summary.final_answer, "done once");
        assert_eq!(
            summary
                .assistant_messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter(|block| matches!(block, crate::ContentBlock::Text { text } if text == "done once"))
                .count(),
            1
        );
        let events = services.event_store().all_events(200).expect("events");
        assert!(events
            .iter()
            .any(|event| event.kind == "execution_graph.node_transitioned_and_replanned"));
        let event_kinds = events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>();
        for tool_name in ["read_file", "write_file"] {
            assert!(
                events.iter().any(|event| {
                    event.kind == "tool.invocation.started"
                        && event.payload["tool_name"] == tool_name
                }),
                "missing started event for {tool_name}; events={event_kinds:?}"
            );
            assert!(
                events.iter().any(|event| {
                    event.kind == "tool.invocation.completed"
                        && event.payload["tool_name"] == tool_name
                }),
                "missing completed event for {tool_name}; events={event_kinds:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn governed_runtime_tool_batch_fans_out_many_independent_reads_and_keeps_order() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let host: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(ConcurrentRuntimeToolHost {
            active: Arc::clone(&active),
            observed_peak: Arc::clone(&peak),
        });
        let ticket = NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "tools".to_string(),
            executor_kind: "tool_batch".to_string(),
            service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
            attempt: 1,
            idempotency_key: "batch".to_string(),
            payload_ref: String::new(),
        };
        let calls = (0..50)
            .map(|index| ModelToolCall {
                id: format!("read-{index}"),
                name: "read_file".to_string(),
                input: format!(r#"{{"path":"{index}"}}"#),
                depends_on: Vec::new(),
            })
            .collect::<Vec<_>>();

        let mut decision =
            crate::execution_core::build_runtime_execution_decision("parallel reads", None);
        decision.strategy.selected_candidate =
            harness_contract::strategy::ExecutionCandidateKind::ParallelTools;
        if !decision
            .strategy
            .modifiers
            .contains(&harness_contract::core::ExecutionModifier::Parallel)
        {
            decision
                .strategy
                .modifiers
                .push(harness_contract::core::ExecutionModifier::Parallel);
        }
        let tool_effects: std::collections::HashMap<
            String,
            harness_contract::tool::GovernedToolInvocation,
        > = calls
            .iter()
            .map(|call| {
                let normalized_input =
                    serde_json::from_str::<serde_json::Value>(&call.input).unwrap();
                let effect = harness_contract::tool::ToolEffectDescriptor {
                    tool_id: call.name.clone(),
                    descriptor_hash: format!("descriptor-{}", call.id),
                    effect_kind: harness_contract::tool::ToolEffectKind::Read,
                    idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
                    scopes: vec![harness_contract::policy::PermissionScope {
                        resource: harness_contract::policy::PermissionResource::Tool,
                        operation: harness_contract::policy::PermissionOperation::Read,
                        target: normalized_input
                            .get("path")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string),
                    }],
                    required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
                    approval_class: harness_contract::tool::ToolApprovalClass::None,
                    uses_network: false,
                    spawns_process: false,
                    mutates_packages: false,
                    mutates_system: false,
                    assessment: harness_contract::policy::EffectAssessment::default(),
                };
                (
                    call.id.clone(),
                    harness_contract::tool::GovernedToolInvocation {
                        contract_version: 1,
                        invocation_id: call.id.clone(),
                        intent: harness_contract::tool::ToolIntent {
                            invocation_id: call.id.clone(),
                            tool_name: call.name.clone(),
                            normalized_input,
                        },
                        effect: effect.clone(),
                        resource_demand: harness_contract::tool::ResourceDemand::default(),
                        explicit_dependencies: Vec::new(),
                        compiled_dependencies: Vec::new(),
                        catalog_revision: 1,
                        descriptor_set_hash: "test".to_string(),
                        idempotency_key: format!("{}:{}", call.name, call.id),
                    },
                )
            })
            .collect();
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let requests = calls
            .iter()
            .map(|call| crate::tool_dispatch::ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect::<Vec<_>>();
        let compilation = crate::GovernedToolCompiler.compile_partial(
            services.workspace_root(),
            &requests,
            |name, input| {
                requests
                    .iter()
                    .find(|request| {
                        request.tool_name == name
                            && serde_json::from_str::<serde_json::Value>(&request.input)
                                .unwrap_or(serde_json::Value::Null)
                                == *input
                    })
                    .and_then(|request| tool_effects.get(&request.tool_use_id))
                    .map(|invocation| {
                        (
                            invocation.effect.clone(),
                            invocation.catalog_revision,
                            invocation.descriptor_set_hash.clone(),
                        )
                    })
            },
        );
        let governed = execute_governed_runtime_tool_batch(
            host,
            None,
            &calls,
            "session",
            harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            0,
            None,
            None,
            &ticket,
            1,
            &std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
            &tool_effects,
            compilation,
            &decision,
            services.tool_execution_plane(),
            services.commit_service(),
            &BTreeMap::new(),
        )
        .await;
        let invocations = governed.invocations.clone();
        let messages = governed.messages;

        assert!(peak.load(Ordering::SeqCst) >= 2);
        assert!(
            peak.load(Ordering::SeqCst)
                <= crate::governed_tool_plan::default_parallel_tool_concurrency(),
            "the graph route must obey the same per-turn read fan-out cap"
        );
        assert_eq!(messages.len(), 50);
        assert_eq!(invocations.len(), 50);
        assert!(invocations.values().all(|invocation| {
            invocation.status == crate::ToolInvocationStatus::Completed
                && invocation.ended_at_ms.is_some()
        }));
        assert_eq!(
            governed.max_concurrency_observed,
            crate::governed_tool_plan::default_parallel_tool_concurrency()
        );
        assert_eq!(governed.parallel_batches, 1);
        assert!(matches!(
            messages[0].blocks.as_slice(),
            [ContentBlock::ToolResult { tool_use_id, .. }] if tool_use_id == "read-0"
        ));
        assert!(matches!(
            messages[49].blocks.as_slice(),
            [ContentBlock::ToolResult { tool_use_id, .. }] if tool_use_id == "read-49"
        ));
    }

    #[tokio::test]
    async fn retained_read_replay_keeps_original_wave_sequence_and_is_not_fresh() {
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let resolver = services.path_identity_resolver();
        let path = services.workspace_root().join("retained-read.txt");
        std::fs::write(&path, "retained").expect("fixture file");
        let evidence = resolver
            .observe_tool_scope(
                "read_file",
                &format!("read:{}", path.display()),
                Some("digest-retained"),
                7,
            )
            .expect("typed observation");
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: "original-read".to_string(),
                tool_name: "read_file".to_string(),
                input: r#"{"path":"retained-read.txt"}"#.to_string(),
                depends_on: Vec::new(),
            },
        );
        request.governed_plan_id = "plan".to_string();
        request.governed_plan_revision = 1;
        request.observation_wave_sequence = 7;
        request.idempotency_key = "retained-read-idempotency".to_string();
        request.category = crate::ToolSafetyCategory::ReadOnly;
        let original = crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some("retained".to_string()),
            error: None,
            evidence_ref: "tool://original-read".to_string(),
            observed_evidence: vec![evidence],
        };
        services
            .commit_service()
            .commit_readonly_tool_receipts(&[(request.clone(), original)])
            .expect("durable read receipt");

        let active = Arc::new(AtomicUsize::new(0));
        let host = ConcurrentRuntimeToolHost {
            active: Arc::clone(&active),
            observed_peak: Arc::new(AtomicUsize::new(0)),
        };
        let effect = harness_contract::tool::ToolEffectDescriptor {
            tool_id: "read_file".to_string(),
            descriptor_hash: "read-file-v1".to_string(),
            effect_kind: harness_contract::tool::ToolEffectKind::Read,
            idempotency: harness_contract::tool::ToolIdempotency::Idempotent,
            scopes: Vec::new(),
            required_permission: harness_contract::tool::ToolPermissionMode::ReadOnly,
            approval_class: harness_contract::tool::ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: harness_contract::policy::EffectAssessment::default(),
        };
        let mut replay = request;
        replay.tool_use_id = "replayed-read".to_string();
        replay.observation_wave_sequence = 99;
        let replayed =
            execute_fenced_runtime_tool(&host, services.commit_service(), &replay, Some(&effect))
                .await;

        assert_eq!(
            active.load(Ordering::SeqCst),
            0,
            "replay must not execute host"
        );
        assert_eq!(replayed.tool_use_id, "replayed-read");
        assert_eq!(replayed.observed_evidence.len(), 1);
        assert_eq!(replayed.observed_evidence[0].observed_at_sequence, 7);
        assert_eq!(
            replayed.observed_evidence[0].provenance,
            harness_contract::context::ObservedEvidenceProvenance::RetainedReplay
        );
    }

    #[test]
    fn dynamic_tool_nodes_preserve_file_resource_scopes() {
        let same_file = resource_scopes_for_tool_calls(&[
            ModelToolCall {
                id: "write-a".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/lib.rs","content":"a"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "write-b".into(),
                name: "edit_file".into(),
                input: r#"{"path":"src/lib.rs","old_string":"a","new_string":"b"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(same_file, vec!["write:src/lib.rs"]);

        let different_files = resource_scopes_for_tool_calls(&[
            ModelToolCall {
                id: "write-a".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/a.rs","content":"a"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "write-b".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/b.rs","content":"b"}"#.into(),
                depends_on: Vec::new(),
            },
        ]);
        assert_eq!(different_files, vec!["write:src/a.rs", "write:src/b.rs"]);
    }

    #[test]
    fn dynamic_inline_model_is_classified_as_evidence_analysis_work() {
        let ticket = NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "source".to_string(),
            executor_kind: "inline_model".to_string(),
            service_class: Default::default(),
            attempt: 1,
            idempotency_key: "source:attempt".to_string(),
            payload_ref: "{}".to_string(),
        };

        let node = dynamic_node(
            &ticket,
            1,
            "analyze",
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "inline_model",
        );

        assert_eq!(
            node.work.expect("work contract").role,
            harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze
        );
    }

    #[test]
    fn invalid_model_paths_use_conservative_graph_locks_for_tool_recovery() {
        let root = tempfile::tempdir().expect("workspace");
        let inside = root.path().join("src/lib.rs");
        let outside = root.path().with_file_name("mistyped-workspace/src/lib.rs");
        let calls = [
            ModelToolCall {
                id: "valid".into(),
                name: "grep_search".into(),
                input: serde_json::json!({"path": inside, "pattern": "Runtime"}).to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "typo".into(),
                name: "read_file".into(),
                input: serde_json::json!({"path": outside}).to_string(),
                depends_on: Vec::new(),
            },
        ];

        assert_eq!(
            graph_resource_scopes_for_tool_calls(&calls, root.path()),
            vec!["read:."]
        );
    }

    #[test]
    fn invalid_read_scope_with_a_write_takes_one_workspace_write_lock() {
        let root = tempfile::tempdir().expect("workspace");
        let calls = [
            ModelToolCall {
                id: "write".into(),
                name: "write_file".into(),
                input: r#"{"path":"src/lib.rs","content":"updated"}"#.into(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "typo".into(),
                name: "read_file".into(),
                input: r#"{"path":"../other/src/lib.rs"}"#.into(),
                depends_on: Vec::new(),
            },
        ];

        assert_eq!(
            graph_resource_scopes_for_tool_calls(&calls, root.path()),
            vec!["write:."]
        );
    }

    #[test]
    fn tool_batch_container_does_not_prevalidate_model_paths() {
        let workspace = tempfile::tempdir().expect("workspace");
        let ticket = NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "graph:1:model".to_string(),
            executor_kind: "inline_model".to_string(),
            service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
            attempt: 1,
            idempotency_key: "graph:1:model".to_string(),
            payload_ref: String::new(),
        };
        let calls = vec![
            ModelToolCall {
                id: "capabilities".to_string(),
                name: "runtime_capabilities".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
            ModelToolCall {
                id: "mistyped-read".to_string(),
                name: "read_many".to_string(),
                input: serde_json::json!({
                    "files": [
                        "crates/runtime/src/policy/mod.rs",
                        "crates/runtime/src/policy/does-not-exist.rs"
                    ]
                })
                .to_string(),
                depends_on: Vec::new(),
            },
        ];

        let nodes = tool_nodes_for_calls(&ticket, 1, "session", calls, workspace.path())
            .expect("tool batch nodes");

        assert!(
            nodes.iter().all(|node| node.resource_scopes.is_empty()),
            "only governed leaf calls may resolve model-supplied paths; the container must admit partial tool progress"
        );
    }

    #[test]
    fn focus_acceptance_requires_content_evidence_after_directory_discovery() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("runtime tree");
        std::fs::write(
            root.path().join("crates/runtime/src/lib.rs"),
            "pub fn run() {}",
        )
        .expect("source");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let discovery = resolver
            .observe_tool_scope("glob_search", "glob:crates/runtime", None, 1)
            .expect("discovery receipt");
        assert!(
            typed_satisfied_focus_acceptance_scope_keys(
                &["read:crates/runtime".to_string()],
                &[discovery],
                &[],
                &resolver,
            )
            .is_empty(),
            "file discovery must not close a source-content Focus contract"
        );

        let content_read = resolver
            .observe_tool_scope(
                "read_file",
                "read:crates/runtime/src/lib.rs",
                Some("digest"),
                2,
            )
            .expect("content receipt");
        assert_eq!(
            typed_satisfied_focus_acceptance_scope_keys(
                &["read:crates/runtime".to_string()],
                &[content_read],
                &[],
                &resolver,
            ),
            BTreeSet::from(["read:crates/runtime".to_string()]),
            "the Goal unknown must resolve with the same descendant receipt that closes pending"
        );
    }

    #[test]
    fn focus_acceptance_keeps_real_writes_and_post_write_reads_typed() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/lib.rs"), "b").unwrap();
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let write = resolver
            .observe_tool_scope("edit_file", "write:src/lib.rs", Some("same"), 1)
            .unwrap();
        let read = resolver
            .observe_tool_scope("read_file", "read:src/lib.rs", Some("same"), 2)
            .unwrap();
        assert_eq!(
            typed_satisfied_focus_acceptance_scope_keys(
                &["verify_after_write:src/lib.rs".to_string()],
                &[read],
                &[write],
                &resolver,
            ),
            BTreeSet::from(["verify_after_write:src/lib.rs".to_string()])
        );
    }

    #[test]
    fn registered_network_effect_closes_focus_only_for_successful_calls() {
        let messages = vec![
            ConversationMessage {
                role: crate::MessageRole::User,
                blocks: vec![ContentBlock::ToolResult {
                    tool_use_id: "search-ok".into(),
                    tool_name: "web_search".into(),
                    output: "official result".into(),
                    is_error: false,
                }],
                usage: None,
            },
            ConversationMessage {
                role: crate::MessageRole::User,
                blocks: vec![ContentBlock::ToolResult {
                    tool_use_id: "fetch-failed".into(),
                    tool_name: "web_fetch".into(),
                    output: "network failure".into(),
                    is_error: true,
                }],
                usage: None,
            },
        ];
        let successful_ids = successful_tool_call_ids(&messages);
        assert_eq!(successful_ids, BTreeSet::from(["search-ok".to_string()]));
        assert!(typed_satisfied_focus_acceptance_scope_keys(
            &["network:*".to_string()],
            &[],
            &[],
            &crate::path_identity::WorkspacePathIdentityResolver::discover(
                tempfile::tempdir().unwrap().path(),
            )
            .unwrap(),
        )
        .is_empty(), "a successful ToolResult is liveness only; no typed network receipt means no Focus proof");
    }

    #[test]
    fn write_attempt_paths_are_projectable_workspace_relative_refs() {
        let root = tempfile::tempdir().expect("workspace");
        let target = root.path().join("fixtures/target.txt");
        let calls = [ModelToolCall {
            id: "write".into(),
            name: "write_file".into(),
            input: serde_json::json!({"path": target, "content": "updated"}).to_string(),
            depends_on: Vec::new(),
        }];
        let mut attempts = Vec::new();

        record_write_attempt_paths(&mut attempts, &calls, root.path());

        assert_eq!(attempts, vec!["fixtures/target.txt"]);
    }

    #[test]
    fn focus_verification_compiles_exact_required_reads_and_post_write_reads() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("fixtures")).expect("fixtures");
        for path in ["source.txt", "a.txt", "b.txt"] {
            std::fs::write(workspace.path().join("fixtures").join(path), path)
                .expect("exact fixture");
        }
        let calls = focus_verification_tool_calls(
            &[
                "read:fixtures/source.txt".into(),
                "verify_after_write:fixtures/a.txt".into(),
                "verify_upstream_change:fixtures/b.txt".into(),
            ],
            7,
            workspace.path(),
        )
        .expect("exact verification calls");
        assert_eq!(calls.len(), 3);
        assert!(calls.iter().all(|call| call.name == "read_file"));
        assert_eq!(calls[0].id, "runtime-focus-verify-7-0");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&calls[0].input).expect("read input"),
            serde_json::json!({"path": "fixtures/source.txt", "complete": true})
        );
        assert!(focus_verification_tool_calls(
            &["workspace_change:src/lib.rs".into()],
            1,
            workspace.path(),
        )
        .is_none());
        assert!(focus_verification_tool_calls(
            &["verify_after_write:../outside.txt".into()],
            1,
            workspace.path(),
        )
        .is_none());
    }

    #[test]
    fn focus_verification_never_substitutes_a_descendant_for_directory_scope() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("crates/gateway/src")).expect("gateway tree");
        std::fs::write(
            workspace.path().join("crates/gateway/Cargo.toml"),
            "[package]",
        )
        .expect("gateway manifest");
        std::fs::write(
            workspace.path().join("crates/gateway/src/lib.rs"),
            "pub fn gateway() {}",
        )
        .expect("gateway source");

        assert!(focus_verification_tool_calls(
            &["read:crates/gateway".into()],
            8,
            workspace.path()
        )
        .is_none());
        assert!(!focus_scopes_are_exact_workspace_files(
            &["read:crates/gateway".into()],
            workspace.path()
        ));
        assert!(focus_scopes_are_exact_workspace_files(
            &["read:crates/gateway/src/lib.rs".into()],
            workspace.path()
        ));
    }

    #[test]
    fn runtime_followup_verification_uses_a_fresh_node_namespace() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("fixtures")).expect("fixtures");
        std::fs::write(workspace.path().join("fixtures/target.txt"), "verified")
            .expect("target");
        let ticket = NodeExecutionTicket {
            graph_id: "graph".to_string(),
            node_id: "graph:3:tools-1".to_string(),
            executor_kind: "tool_batch".to_string(),
            service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
            attempt: 1,
            idempotency_key: "write-batch".to_string(),
            payload_ref: String::new(),
        };
        let followup_iteration = 3usize.saturating_add(1);
        let calls = focus_verification_tool_calls(
            &["verify_after_write:fixtures/target.txt".into()],
            followup_iteration,
            workspace.path(),
        )
        .expect("verification calls");
        let nodes = tool_nodes_for_calls(
            &ticket,
            followup_iteration,
            "session",
            calls,
            workspace.path(),
        )
        .expect("verification nodes");

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "graph:4:tools-1");
        assert_ne!(nodes[0].id, ticket.node_id);
    }

    #[test]
    fn only_first_step_bounded_focus_evidence_is_prefetched() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(workspace.path().join("fixtures")).expect("fixtures");
        std::fs::write(workspace.path().join("fixtures/target.txt"), "target")
            .expect("target");
        let scopes = vec!["verify_upstream_change:fixtures/target.txt".to_string()];
        assert!(should_prefetch_focus_verification(
            true,
            true,
            false,
            &scopes,
            workspace.path(),
        ));
        assert!(!should_prefetch_focus_verification(
            false,
            true,
            false,
            &scopes,
            workspace.path(),
        ));
        assert!(!should_prefetch_focus_verification(
            true,
            true,
            true,
            &scopes,
            workspace.path(),
        ));
        assert!(!should_prefetch_focus_verification(
            true,
            true,
            false,
            &["verify_after_write:fixtures/target.txt".into()],
            workspace.path(),
        ));
        assert!(should_prefetch_focus_verification(
            true,
            true,
            false,
            &["read:fixtures/target.txt".into()],
            workspace.path(),
        ));
        assert!(!should_prefetch_focus_verification(
            true,
            true,
            false,
            &["read:fixtures".into()],
            workspace.path(),
        ));
    }

    #[test]
    fn runtime_prefetched_exact_read_closes_only_its_matching_focus_scope() {
        let calls = vec![ModelToolCall {
            id: "runtime-focus-verify-2-0".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "fixtures/target.txt", "complete": true}).to_string(),
            depends_on: Vec::new(),
        }];
        let required = vec![
            "read:fixtures/target.txt".to_string(),
            "read:fixtures/other.txt".to_string(),
        ];
        let satisfied = successful_runtime_focus_scope_keys(
            &calls,
            &BTreeSet::from(["runtime-focus-verify-2-0".to_string()]),
            &required,
        );
        assert_eq!(
            satisfied,
            BTreeSet::from(["read:fixtures/target.txt".to_string()])
        );
        assert!(
            successful_runtime_focus_scope_keys(&calls, &BTreeSet::new(), &required).is_empty()
        );

        let bounded_calls = vec![ModelToolCall {
            id: "runtime-focus-verify-2-0".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "fixtures/target.txt"}).to_string(),
            depends_on: Vec::new(),
        }];
        assert!(successful_runtime_focus_scope_keys(
            &bounded_calls,
            &BTreeSet::from(["runtime-focus-verify-2-0".to_string()]),
            &required,
        )
        .is_empty());
    }

    #[test]
    fn upstream_read_verification_does_not_require_a_reviewer_owned_write() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("fixtures")).expect("fixtures");
        std::fs::write(root.path().join("fixtures/target.txt"), "current").unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let read = resolver
            .observe_tool_scope("read_file", "read:fixtures/target.txt", Some("current"), 1)
            .unwrap();
        let required = vec!["verify_upstream_change:fixtures/target.txt".to_string()];
        let verified = typed_satisfied_focus_acceptance_scope_keys(
            &required,
            std::slice::from_ref(&read),
            &[],
            &resolver,
        );
        assert!(verified.contains("verify_upstream_change:fixtures/target.txt"));
        assert!(!verified.contains("verify_after_write:fixtures/target.txt"));

        let write = resolver
            .observe_tool_scope(
                "write_file",
                "write:fixtures/target.txt",
                Some("current"),
                1,
            )
            .unwrap();
        let read = resolver
            .observe_tool_scope("read_file", "read:fixtures/target.txt", Some("current"), 2)
            .unwrap();
        let required = vec!["verify_after_write:fixtures/target.txt".to_string()];
        let verified =
            typed_satisfied_focus_acceptance_scope_keys(&required, &[read], &[write], &resolver);
        assert!(verified.contains("verify_after_write:fixtures/target.txt"));
    }

    #[test]
    fn directory_write_scope_verifies_the_exact_committed_descendant() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("report")).expect("report directory");
        std::fs::write(root.path().join("report/index.html"), "report").unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let write = resolver
            .observe_tool_scope(
                "write_file",
                "write:report/index.html",
                Some("report-digest"),
                1,
            )
            .unwrap();
        assert_eq!(
            concrete_focus_verification_scopes(
                &["verify_after_write:report".to_string()],
                std::slice::from_ref(&write),
                &resolver,
            ),
            vec!["verify_after_write:report/index.html".to_string()]
        );

        let read = resolver
            .observe_tool_scope(
                "read_file",
                "read:report/index.html",
                Some("report-digest"),
                2,
            )
            .unwrap();
        assert_eq!(
            typed_satisfied_focus_acceptance_scope_keys(
                &["verify_after_write:report".to_string()],
                &[read],
                &[write],
                &resolver,
            ),
            BTreeSet::from(["verify_after_write:report".to_string()])
        );
    }

    #[test]
    fn completed_upstream_prefetch_is_explained_without_promoting_other_reads() {
        let verified = BTreeSet::from([
            "verify_upstream_change:fixtures/target.txt".to_string(),
            "verify_after_write:fixtures/owned.txt".to_string(),
        ]);
        let instruction =
            upstream_verification_completion_instruction(&verified).expect("reviewer instruction");

        assert!(instruction.contains("fixtures/target.txt"));
        assert!(!instruction.contains("fixtures/owned.txt"));
        assert!(instruction.contains("independent exact-path read"));
        assert!(instruction.contains("Tools are now disabled"));
        assert!(
            upstream_verification_completion_instruction(&BTreeSet::from([
                "verify_after_write:fixtures/owned.txt".to_string(),
            ]))
            .is_none()
        );
    }

    #[test]
    fn evaluation_scope_recovery_compiles_bounded_parallel_exact_reads() {
        let calls = evaluation_scope_recovery_tool_calls(
            &[
                "write:fixtures/target.txt".into(),
                "read:fixtures/protected.txt".into(),
                "session:ignored".into(),
            ],
            9,
        )
        .expect("bounded exact reads");
        assert_eq!(calls.len(), 2);
        assert!(calls
            .iter()
            .all(|call| { call.name == "read_file" && call.depends_on.is_empty() }));
        assert_eq!(calls[0].id, "runtime-eval-exact-read-9-0");
        assert!(calls[0].input.contains("fixtures/protected.txt"));
        assert!(calls[1].input.contains("fixtures/target.txt"));
        assert!(evaluation_scope_recovery_tool_calls(&["read:.".into()], 1).is_none());

        let too_many = (0..9)
            .map(|index| format!("read:fixtures/{index}.txt"))
            .collect::<Vec<_>>();
        assert!(evaluation_scope_recovery_tool_calls(&too_many, 1).is_none());
    }

    #[test]
    fn final_write_replan_is_single_use_and_requires_zero_write_attempts() {
        assert!(required_write_final_replan_allowed(3, "read:.", true, &[]));
        assert!(!required_write_final_replan_allowed(2, "read:.", true, &[]));
        assert!(!required_write_final_replan_allowed(4, "read:.", true, &[]));
        assert!(!required_write_final_replan_allowed(
            3,
            "read:src",
            true,
            &[]
        ));
        assert!(!required_write_final_replan_allowed(
            3,
            "read:.",
            true,
            &["fixtures/target.txt".into()],
        ));
        assert!(post_write_exact_read_recovery_allowed(
            3, "read:.", true, true,
        ));
        assert!(!post_write_exact_read_recovery_allowed(
            2, "read:.", true, true,
        ));
        assert!(!post_write_exact_read_recovery_allowed(
            3, "read:src", true, true,
        ));
        assert!(!post_write_exact_read_recovery_allowed(
            3, "read:.", true, false,
        ));
        assert_eq!(
            required_mutation_tool_allowlist(),
            BTreeSet::from(["edit_file".to_string(), "write_file".to_string()])
        );
    }

    #[test]
    fn only_verified_materialized_team_result_can_bypass_parent_model() {
        let verified = serde_json::json!({
            "status": "completed",
            "working_state_verified": true,
            "terminal_summary": "checked result",
            "execution": {"terminal_result_available": true},
            "delivery_envelope": {
                "envelope_id": "team-envelope",
                "revision": 7,
                "objective_id": "root-objective",
                "pipeline_status": "completed",
                "delivery_status": "satisfied",
                "created_at_ms": 10
            },
            "terminal_presentation": {
                "presentation_id": "team-presentation",
                "attempt_id": "team-attempt",
                "envelope_id": "team-envelope",
                "envelope_revision": 7,
                "state": "validating",
                "answer_origin": "team_synthesizer",
                "generated_at_ms": 11
            }
        });
        assert_eq!(
            verified_team_terminal_summary(&verified).as_deref(),
            Some("checked result")
        );

        let mut unverified = verified.clone();
        unverified["working_state_verified"] = serde_json::json!(false);
        assert!(verified_team_terminal_summary(&unverified).is_none());
        let mut missing_result = verified;
        missing_result["execution"]["terminal_result_available"] = serde_json::json!(false);
        assert!(verified_team_terminal_summary(&missing_result).is_none());
    }

    #[test]
    fn verified_team_evidence_bundles_require_root_synthesis() {
        let receipt = serde_json::json!({
            "status": "completed",
            "working_state_verified": true,
            "execution": {"terminal_result_available": true},
            "team_terminals": [
                {
                    "team_id": "team-runtime",
                    "working_state_verified": true,
                    "terminal_summary_kind": "verified_team_evidence_bundle",
                    "terminal_summary": "# Verified Team evidence bundle\n\n## Risk status\n\nRisk: no unresolved delivery-contract findings in this completed Team.\n\nobserved_source_paths: crates/runtime/src/conversation/host.rs",
                    "delivery_envelope": {
                        "envelope_id": "team-runtime-envelope",
                        "revision": 3,
                        "objective_id": "runtime-review",
                        "pipeline_status": "completed",
                        "delivery_status": "satisfied",
                        "created_at_ms": 10
                    }
                },
                {
                    "team_id": "team-memory",
                    "working_state_verified": true,
                    "terminal_summary_kind": "verified_team_evidence_bundle",
                    "terminal_summary": "# Verified Team evidence bundle\n\n## Risk status\n\nRisk: no unresolved delivery-contract findings in this completed Team.\n\nobserved_source_paths: crates/memory/src/store/mod.rs",
                    "delivery_envelope": {
                        "envelope_id": "team-memory-envelope",
                        "revision": 4,
                        "objective_id": "memory-review",
                        "pipeline_status": "completed",
                        "delivery_status": "satisfied",
                        "created_at_ms": 12
                    }
                }
            ]
        });
        let summary = verified_team_terminal_summary(&receipt)
            .expect("every completed Team has a runtime-derived evidence bundle");
        assert!(is_collaboration_evidence_carrier(&summary));
        assert!(summary.contains("team-runtime: # Verified Team evidence bundle"));
        assert!(summary.contains("crates/memory/src/store/mod.rs"));
        let carrier: serde_json::Value = serde_json::from_str(&summary).expect("typed carrier");
        assert_eq!(
            carrier.pointer("/root_runtime_attestation/status"),
            Some(&serde_json::json!("verified"))
        );
        assert_eq!(
            carrier.pointer("/root_runtime_attestation/verified_terminal_count"),
            Some(&serde_json::json!(2))
        );
        assert_eq!(
            carrier.pointer(
                "/root_runtime_attestation/role_local_visibility_gaps_do_not_negate_aggregate_attestation"
            ),
            Some(&serde_json::json!(true))
        );

        let mut root_unverified = receipt.clone();
        root_unverified["working_state_verified"] = serde_json::json!(false);
        assert!(verified_team_terminal_summary(&root_unverified).is_none());

        let mut child_unverified = receipt.clone();
        child_unverified["team_terminals"][1]["working_state_verified"] =
            serde_json::json!(false);
        assert!(verified_team_terminal_summary(&child_unverified).is_none());

        let mut unverified = receipt;
        unverified["team_terminals"][1]["terminal_summary_kind"] =
            serde_json::json!("untyped_summary");
        assert!(verified_team_terminal_summary(&unverified).is_none());
    }

    #[test]
    fn evaluation_scope_ceiling_is_mode_aware_and_canonical() {
        let allowed = "write:fixtures/auto-strategy-write/target.txt";
        assert!(evaluation_scope_authorizes(
            allowed,
            "write:fixtures//auto-strategy-write/./target.txt"
        ));
        assert!(evaluation_scope_authorizes(
            allowed,
            "read:fixtures/auto-strategy-write/target.txt"
        ));
        assert!(!evaluation_scope_authorizes(
            allowed,
            "write:fixtures/auto-strategy-write/protected.txt"
        ));
        assert!(!evaluation_scope_authorizes(
            allowed,
            "write:fixtures/auto-strategy-write"
        ));
        assert!(!evaluation_scope_authorizes(
            "read:fixtures/auto-strategy-write/target.txt",
            "write:fixtures/auto-strategy-write/target.txt"
        ));
        assert!(evaluation_scope_authorizes(
            "read:.",
            "read:fixtures/auto-strategy-protected/sentinel.txt"
        ));
        assert!(!evaluation_scope_authorizes(
            "read:.",
            "write:fixtures/auto-strategy-protected/sentinel.txt"
        ));
    }

    #[test]
    fn evaluation_scope_ceiling_canonicalizes_absolute_paths_inside_workspace() {
        let root = tempfile::tempdir().expect("workspace");
        let target = root.path().join("fixtures/auto-strategy-write/target.txt");
        std::fs::create_dir_all(target.parent().expect("target parent")).expect("fixture parent");
        std::fs::write(&target, "seed\n").expect("fixture target");
        let calls = [ModelToolCall {
            id: "read-target".into(),
            name: "read_file".into(),
            input: serde_json::json!({"path": target}).to_string(),
            depends_on: Vec::new(),
        }];

        assert_eq!(
            evaluation_scope_violation(
                &["write:fixtures/auto-strategy-write/target.txt".to_string()],
                &calls,
                root.path(),
            ),
            None
        );
    }

    #[test]
    fn parent_merge_metrics_require_injected_receipt_and_successful_parent() {
        let started = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_millis(25))
            .expect("monotonic instant");
        let (cost, count) = parent_merge_actuals(Some(started), true);
        assert!(cost >= 20);
        assert_eq!(count, 1);

        let (failed_cost, failed_count) = parent_merge_actuals(Some(started), false);
        assert!(failed_cost >= 20);
        assert_eq!(failed_count, 0);
        assert_eq!(parent_merge_actuals(None, true), (0, 0));
    }

    #[test]
    fn automatic_team_focuses_are_existing_bounded_workspace_scopes() {
        let root = tempfile::tempdir().expect("focus workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
        std::fs::create_dir_all(root.path().join("crates/gateway")).expect("gateway scope");
        std::fs::create_dir_all(root.path().join("crates/memory")).expect("memory scope");

        let read = bounded_workspace_focus_scopes(
            root.path(),
            "audit runtime and gateway independently",
            2,
            false,
            false,
        );
        assert_eq!(
            read,
            vec![
                "read:crates/gateway".to_string(),
                "read:crates/runtime".to_string()
            ]
        );
        assert!(read.iter().all(|scope| scope != "read:."));

        let write = bounded_workspace_focus_scopes(
            root.path(),
            "implement runtime and gateway changes",
            2,
            true,
            false,
        );
        assert_eq!(
            write,
            vec![
                "write:crates/gateway".to_string(),
                "write:crates/runtime".to_string()
            ]
        );
        let plan = write_focus_partition_plan("implement", write.clone());
        assert_eq!(plan.role_id, "implementer");
        assert_eq!(plan.slots[0].capability_cropped_refs, write);
    }

    #[test]
    fn automatic_team_prefers_explicit_domains_over_related_siblings() {
        let root = tempfile::tempdir().expect("focus workspace");
        for scope in ["gateway", "memory", "memory-postgres", "runtime"] {
            std::fs::create_dir_all(root.path().join("crates").join(scope)).expect("domain scope");
        }

        let read = bounded_workspace_focus_scopes(
            root.path(),
            "audit runtime, memory, and gateway as independent domains",
            3,
            false,
            false,
        );

        assert_eq!(
            read.iter().cloned().collect::<BTreeSet<_>>(),
            BTreeSet::from([
                "read:crates/gateway".to_string(),
                "read:crates/memory".to_string(),
                "read:crates/runtime".to_string(),
            ])
        );
        assert!(!read.contains(&"read:crates/memory-postgres".to_string()));
    }

    #[test]
    fn directory_focus_scope_accepts_only_safe_descendant_receipts() {
        let root = tempfile::tempdir().expect("focus workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("runtime scope");
        std::fs::create_dir_all(root.path().join("crates/runtime-old/src")).expect("sibling scope");
        std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "runtime").unwrap();
        std::fs::write(root.path().join("crates/runtime-old/src/lib.rs"), "old").unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
        let required = resolver.compile_obligation_or_unresolved("read:crates/runtime");
        let descendant = resolver
            .observe_tool_scope(
                "read_file",
                "read:crates/runtime/src/lib.rs",
                Some("digest"),
                1,
            )
            .unwrap();
        let sibling = resolver
            .observe_tool_scope(
                "read_file",
                "read:crates/runtime-old/src/lib.rs",
                Some("digest"),
                1,
            )
            .unwrap();
        let write = resolver
            .observe_tool_scope(
                "write_file",
                "write:crates/runtime/src/lib.rs",
                Some("digest"),
                1,
            )
            .unwrap();
        assert!(crate::path_identity::observed_evidence_satisfies(
            &required,
            &descendant,
        ));
        assert!(!crate::path_identity::observed_evidence_satisfies(
            &required, &sibling,
        ));
        assert!(!crate::path_identity::observed_evidence_satisfies(
            &required, &write,
        ));
        assert!(matches!(
            resolver
                .compile_obligation_or_unresolved("read:crates/runtime/../gateway/src/lib.rs")
                .target,
            harness_contract::context::EvidenceTargetIdentity::UnavailableWorkspace { .. }
        ));
    }
