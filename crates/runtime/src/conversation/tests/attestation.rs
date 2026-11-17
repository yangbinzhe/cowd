mod r1_root_scope_v2_original_owner {
    use super::*;
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
        ExecutionNodeStatus,
    };
    use harness_contract::goal::{ProofEffectPurpose, ProofEffectScope};

    struct PhysicalRootExecutor {
        host: tools::ToolHost,
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for PhysicalRootExecutor {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.host.pin_snapshot())
        }

        fn available_tool_names(&self) -> Vec<String> {
            vec!["read_file".into(), "edit".into(), "checkpoint".into()]
        }

        fn registered_tool_effect(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.host.pin_snapshot().describe_effect(name, input))
        }

        async fn execute_step_authorized_invocation_output(
            &self,
            binding: Option<&crate::step_snapshot::StepCallBinding>,
            invocation_id: &str,
            authorization: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            let binding = binding.expect("original provider Step");
            assert_eq!(binding.origin.call_id, invocation_id);
            let step = binding
                .resolve(Some(self.host.pin_snapshot()))
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
            self.calls.lock().unwrap().push(invocation_id.into());
            let output = step
                .lease()
                .execute_async(authorization, name, &serde_json::from_str(input).unwrap())
                .await
                .map_err(|error| crate::ToolError::new(error.to_string()))?;
            Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                output,
            ))
        }

        async fn execute_output(
            &self,
            _: &str,
            _: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("unbound execution is forbidden"))
        }
    }

    #[tokio::test]
    async fn root_nonowns_scope_v2_uses_original_toolbatch_owner_for_read_write() {
        if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
            return;
        }
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("input.txt"),
            "original complete input\n",
        )
        .unwrap();
        let services = crate::RuntimeServices::test_builder(
            workspace.path().join("home"),
            workspace.path(),
        )
        .build()
        .expect("original Host runtime services");
        let store = services.event_store().clone();
        let commits = services.commit_service().clone();
        let session = Session::new().with_workspace_root(workspace.path());
        let session_id = session.session_id.clone();
        let turn_id = "scope-v2-root-turn";
        let mut graph = ExecutionGraph::new("Read input.txt and write output.txt");
        graph.id = format!("root-scope-v2:{session_id}");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: session_id.clone(),
            turn_id: turn_id.into(),
            root_task_id: "scope-v2-task".into(),
            task_id: "scope-v2-task".into(),
            generation: 1,
        });
        let mut original_node = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "original caller",
        );
        original_node.id = "scope-v2-original-tool-owner".into();
        let mut decoy = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "not the caller",
        );
        decoy.id = "scope-v2-running-model-decoy".into();
        graph.nodes.extend([decoy, original_node.clone()]);
        let execution = crate::CowdExecutionContext {
            execution_id: graph.id.clone(),
            session_id: session_id.clone(),
            turn_id: turn_id.into(),
        };
        let sessions = Arc::new(crate::test_support::session_store());
        sessions
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".into(),
                chat_id: "root-scope-v2".into(),
                user_id: None,
                model: None,
                created_at: "2026-10-18T00:00:00Z".into(),
                last_activity: "2026-10-18T00:00:00Z".into(),
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".into(),
            })
            .await
            .unwrap();
        services
            .install_test_session_store(sessions.clone())
            .expect("original Host session owner");
        let host = tools::ToolHost::builtin("root-scope-v2", workspace.path())
            .with_authorization_lease_verifier(Arc::new(
                crate::AuthorizationNegotiator::verify_lease_signature,
            ));
        let lease = host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| matches!(definition.name.as_str(), "read_file" | "edit"))
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                serde_json::from_value(serde_json::json!({
                    "step_id":"scope-v2-original-step", "session_id":session_id, "turn_id":turn_id,
                    "execution_id":graph.id, "build_ref":"isolated-fixture"
                }))
                .unwrap(),
                lease,
                &advertised,
                &serde_json::json!({}),
                &serde_json::json!({}),
                std::collections::BTreeMap::new(),
                chrono::Utc::now().timestamp_millis() + 60_000,
            )
            .unwrap(),
        );
        let calls = Arc::new(Mutex::new(Vec::new()));
        let executor = PhysicalRootExecutor {
            host,
            calls: calls.clone(),
        };
        assert!(!executor.owns_durable_tool_effect("edit"));
        assert!(!executor.owns_durable_tool_effect("read_file"));
        let bus = crate::CowdEventBus::new();
        let mut runtime = crate::ConversationRuntime::new(
            session,
            FinalAnswerClient,
            executor,
            PermissionPolicy::new(crate::PermissionMode::WorkspaceWrite),
            vec!["read and write current files".into()],
        )
        .without_memory()
        .with_runtime_event_store(store.clone())
        .with_artifact_store(services.artifact_store().clone())
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            sessions,
        ))
        .with_cowd_event_bus(bus.clone());
        runtime
            .session_input_stream()
            .set_active_turn(Some(harness_contract::turn::TurnId::from_string(turn_id)));
        runtime
            .begin_turn_strategy(turn_id, "Read input.txt completely and write output.txt")
            .unwrap();
        runtime.bind_turn_strategy_execution(turn_id, &graph.id).unwrap();
        let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
            "id":"scope-v2-goal", "session_id":session_id,
            "objective":"Read input.txt completely and write output.txt",
            "criteria":[{"id":"checked","statement":"read and write","status":"open"}],
            "phase":"execution", "spec_revision":1, "spec_digest":"scope-v2-spec",
            // R4-034-fixture-fix: bind the fixture Goal to its original graph/session/turn owner.
            "scope":"internal", "completion":"open", "revision":1, "user_sequence":1,
            "execution_binding":{"objective_id":harness_contract::agent_action::root_objective_id(&session_id, turn_id),"session_id":session_id,
                "turn_id":turn_id,"root_execution_id":graph.id,"agentic_program_id":"scope-v2-program"}
        })).unwrap();
        services.goal_store().create(goal).unwrap();
        let mut turn_state = r1_branch_state(&session_id, "scope-v2-goal");
        turn_state.turn_id = turn_id.into();
        let turn_state = Arc::new(tokio::sync::Mutex::new(turn_state));
        let make_call = |call_id: &str, name: &str, input: serde_json::Value| ModelToolCall {
            binding: Some(crate::step_snapshot::StepCallBinding::provider(
                step.clone(),
                call_id,
            )),
            id: call_id.into(),
            name: name.into(),
            input: input.to_string(),
            depends_on: vec![],
        };
        let read = make_call(
            "root-v2-read",
            "read_file",
            serde_json::json!({"path":"input.txt","complete":true}),
        );
        let write = make_call(
            "root-v2-write",
            "edit",
            serde_json::json!({"action":"write","path":"output.txt","content":"physical result\n"}),
        );
        let failed_for_payload = make_call(
            "root-v2-missing-read", "read_file",
            serde_json::json!({"path":"missing.txt","complete":true}),
        );
        original_node.payload_ref = encode_tool_calls_with_continuation(
            &session_id, &[read.clone(), write.clone(), failed_for_payload], false,
        ).unwrap();
        *graph.nodes.iter_mut().find(|node| node.id == original_node.id).unwrap() = original_node.clone();
        let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
            services.executor_registry().clone(), services.graph_state_store().clone(),
            commits.clone(), services.resource_manager().clone(), services.scope_locks().clone(),
            services.worktree_leases().clone(), services.workspace_key(), services.workspace_root(),
        );
        let mut graph = runner.register(graph).await.unwrap();
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = commits.transition_node(&graph, "scope-v2-running-model-decoy", status, None, vec![])
                .unwrap().graph;
        }
        let snapshot = runner.pump_snapshot(&graph.id, &BTreeSet::new()).await.unwrap();
        let mut wave = runner.prepare_ready_wave(&graph.id, snapshot.ready).await.unwrap();
        assert_eq!(wave.len(), 1);
        let binding = wave[0].binding.as_ref().unwrap();
        let ticket = crate::execution_core::NodeExecutionTicket {
            graph_id: graph.id.clone(), node_id: original_node.id.clone(),
            executor_kind: original_node.executor_kind.clone(), service_class: graph.service_class,
            attempt: binding.attempt, idempotency_key: original_node.idempotency_key.clone(),
            payload_ref: original_node.payload_ref.clone(),
        };
        let graph = services.graph_state_store().load(&graph.id).unwrap();
        let original = store
            .latest_for_stream(&format!("{}:node:scope-v2-original-tool-owner", graph.id))
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let attempt = graph.recovery_cursor.node_attempts["scope-v2-original-tool-owner"];
        let _scope = bus.enter_execution_with_activity(execution.clone(), Some(original.clone()));
        assert_eq!(
            runtime.root_effect_owner().unwrap(),
            (original.clone(), attempt)
        );
        for call in [&read, &write] {
            let (result, _) = runtime
                .execute_tool_batch_step_with_owner(
                    std::slice::from_ref(call),
                    &crate::SharedPrompter::none(),
                    4,
                    &BTreeMap::new(), &BTreeMap::new(),
                    Some(&(original.clone(), attempt)), Some(&ticket), Some(&turn_state),
                    Some(services.as_ref()), Some(&commits),
                )
                .await
                .unwrap();
            assert_eq!(result.failed, 0, "{:?}", result.messages);
            let key = call.binding.as_ref().unwrap().effect_idempotency_key();
            let primary = store
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!(
                        "{key}:{}",
                        if call.name == "read_file" {
                            "read-receipt"
                        } else {
                            "receipt"
                        }
                    ),
                )
                .unwrap()
                .unwrap();
            let index = store
                .event_by_idempotency_key(
                    &format!(
                        "execution-agent-receipts:{}:scope-v2-original-tool-owner:{attempt}",
                        graph.id
                    ),
                    &format!("agent-tool-receipt:{key}"),
                )
                .unwrap()
                .unwrap();
            assert_eq!(primary.transaction_id, index.transaction_id);
            assert_eq!(
                primary.payload["root_effect_owner"]["binding"],
                serde_json::to_value(&original).unwrap()
            );
            assert_eq!(primary.payload["root_effect_owner"]["attempt"], attempt);
            assert_eq!(
                primary.payload["root_effect_owner"],
                index.payload["root_effect_owner"]
            );
            assert_eq!(
                primary.payload["resolved_effect_scope"],
                index.payload["resolved_effect_scope"]
            );
            assert!(primary.payload["replay"]["parent_execution"].is_null());
            let scope: ProofEffectScope =
                serde_json::from_value(primary.payload["resolved_effect_scope"].clone()).unwrap();
            let descriptor = step
                .lease()
                .describe_effect(&call.name, &serde_json::from_str(&call.input).unwrap());
            scope.validate_purpose_mapping(&descriptor).unwrap();
            assert!(scope.complete);
            let wanted = if call.name == "read_file" {
                ProofEffectPurpose::InputRead
            } else {
                ProofEffectPurpose::BusinessEffect
            };
            assert!(scope
                .scope_purposes
                .iter()
                .any(|entry| entry.purpose == wanted));
            if call.name == "read_file" {
                let seal = crate::context_evidence::decode_read_acquisition(
                    &primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                )
                .unwrap();
                assert_eq!(seal.node_id, "scope-v2-original-tool-owner");
                assert_eq!(seal.attempt, attempt);
                assert_eq!(seal.schema_version, 1);
                assert_eq!(
                    seal.body.source_bytes,
                    b"original complete input\n".len() as u64
                );
                assert_eq!(
                    seal.body.source_sha256,
                    crate::context_evidence::read_sha256(b"original complete input\n")
                );
                let crate::context_evidence::ReadExtent::FileLines {
                    start_line,
                    line_count,
                    total_lines,
                    source_omitted,
                    body_bytes,
                    ends_with_newline,
                    ..
                } = seal.body.extent
                else {
                    panic!("physical root read must retain its original file extent")
                };
                assert_eq!(start_line, 1);
                assert_eq!(line_count, total_lines);
                assert!(!source_omitted);
                assert!(ends_with_newline);
                assert_eq!(body_bytes, b"original complete input".len() as u64);
            }
        }
        assert_eq!(
            std::fs::read(workspace.path().join("output.txt")).unwrap(),
            b"physical result\n"
        );
        for mutation in 0..4 {
            let mut wrong = original.clone();
            match mutation {
                0 => wrong.node_id = None,
                1 => wrong.node_id = Some("foreign-node".into()),
                2 => wrong.fence += 1,
                _ => wrong.generation += 1,
            }
            let _wrong = bus.enter_execution_with_activity(execution.clone(), Some(wrong));
            assert!(runtime.root_effect_owner().is_err());
            let call = make_call(
                &format!("root-v2-wrong-owner-{mutation}"),
                "edit",
                serde_json::json!({"action":"write","path":"denied.txt","content":"must not execute"}),
            );
            let before = calls.lock().unwrap().len();
            let result = runtime
                .execute_tool_batch_step(&[call], &crate::SharedPrompter::none(), 4)
                .await;
            assert!(result.is_err() || result.as_ref().is_ok_and(|batch| batch.failed > 0));
            assert_eq!(calls.lock().unwrap().len(), before);
            assert!(!workspace.path().join("denied.txt").exists());
        }
        let _restored = bus.enter_execution_with_activity(execution, Some(original.clone()));
        let mut expired_snapshot = step.snapshot().clone();
        expired_snapshot.expires_at_ms = chrono::Utc::now().timestamp_millis() - 1;
        let expired = Arc::new(
            crate::step_snapshot::BoundStep::restore(expired_snapshot, step.lease().clone())
                .unwrap(),
        );
        let expired_call = ModelToolCall {
            binding: Some(crate::step_snapshot::StepCallBinding::provider(
                expired,
                "root-v2-expired-read",
            )),
            id: "root-v2-expired-read".into(),
            name: "read_file".into(),
            input: serde_json::json!({"path":"input.txt","complete":true}).to_string(),
            depends_on: vec![],
        };
        let before = calls.lock().unwrap().len();
        let (refused, _) = runtime
            .execute_tool_batch_step_with_owner(&[expired_call], &crate::SharedPrompter::none(), 4,
                &BTreeMap::new(), &BTreeMap::new(), Some(&(original.clone(), attempt)),
                Some(&ticket), Some(&turn_state), Some(services.as_ref()), Some(&commits))
            .await
            .unwrap();
        assert_eq!(refused.failed, 1);
        assert!(refused.messages.iter().flat_map(|message| &message.blocks).any(|block|
            matches!(block, ContentBlock::ToolResult { is_error:true, output,.. } if output.contains("step_origin_expired"))));
        assert_eq!(calls.lock().unwrap().len(), before);
        assert_eq!(
            std::fs::read(workspace.path().join("output.txt")).unwrap(),
            b"physical result\n"
        );
        let failed = make_call(
            "root-v2-missing-read",
            "read_file",
            serde_json::json!({"path":"missing.txt","complete":true}),
        );
        let (result, _) = runtime
            .execute_tool_batch_step_with_owner(
                std::slice::from_ref(&failed),
                &crate::SharedPrompter::none(),
                4,
                &BTreeMap::new(), &BTreeMap::new(), Some(&(original.clone(), attempt)),
                Some(&ticket), Some(&turn_state), Some(services.as_ref()), Some(&commits),
            )
            .await
            .unwrap();
        assert_eq!(result.failed, 1);
        let key = failed.binding.as_ref().unwrap().effect_idempotency_key();
        let primary = store
            .event_by_idempotency_key(
                &format!("execution-effect:{key}"),
                &format!("{key}:read-receipt"),
            )
            .unwrap()
            .unwrap();
        assert!(primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null());
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == "root-v2-read")
                .count(),
            1
        );
        // Replay the same prepared ticket through the real Host resolver; the
        // original read and write must remain once-only across that path.
        let runtime = Arc::new(tokio::sync::Mutex::new(Box::new(runtime)));
        services.tool_batch_executor().install_resolver(Arc::new(TurnToolResolver {
            session_id: session_id.clone(), graph_id: graph.id.clone(),
            runtime: Arc::downgrade(&runtime), state: Arc::downgrade(&turn_state),
            services: Arc::downgrade(&services),
        }));
        runner.execute_prepared_pump_node(
            &graph.id, wave.remove(0), Arc::new(tokio::sync::Notify::new()),
        ).await.unwrap();
        let replayed = services.graph_state_store().load(&graph.id).unwrap();
        assert_eq!(replayed.node_statuses[&original_node.id], ExecutionNodeStatus::Completed,
            "{:?}", replayed.node_results.get(&original_node.id));
        for call_id in ["root-v2-read", "root-v2-write"] {
            assert_eq!(calls.lock().unwrap().iter().filter(|call| call.as_str() == call_id).count(), 1);
        }
        assert_eq!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == "root-v2-write")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn delegated_leaf_does_not_double_acquire_the_single_tool_slot() {
    if crate::test_support::skip_if_env_absent("COWD_TEST_POSTGRES_URL") {
        return;
    }
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ScopeLockManager,
    };
    struct DelegatedTool {
        plane: Arc<crate::ToolExecutionPlane>,
        host: tools::ToolHost,
        executed: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl ToolExecutor for DelegatedTool {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.host.pin_snapshot())
        }
        fn available_tool_names(&self) -> Vec<String> {
            vec!["read_file".into()]
        }
        fn registered_tool_effect(&self, name: &str, input: &serde_json::Value) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.host.pin_snapshot().describe_effect(name, input))
        }
        fn owns_tool_resource_admission(&self, _: &str) -> bool { true }
        async fn execute_step_authorized_invocation_output(
            &self,
            binding: Option<&crate::step_snapshot::StepCallBinding>,
            invocation_id: &str,
            authorization: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            let binding = binding.expect("original provider step");
            assert_eq!(binding.origin.call_id, invocation_id);
            let step = binding.resolve(Some(self.host.pin_snapshot()))
                .map_err(|error| ToolError::new(error.to_string()))?;
            let input: serde_json::Value = serde_json::from_str(input).unwrap();
            let (output, admission) = self.plane.execute_async_classified_retained(
                &harness_contract::tool::ResourceDemand::default(), None, Default::default(),
                None, Some("single-slot"), step.lease().execute_async(authorization, name, &input),
            ).await;
            drop(admission);
            let output = output.map_err(|error| ToolError::new(error.to_string()))?
                .map_err(|error| ToolError::new(error.to_string()))?;
            self.executed.fetch_add(1, Ordering::SeqCst);
            Ok(harness_contract::context::ToolOutputDraft::bounded_inline(output))
        }
        async fn execute_output(&self, _: &str, _: &str) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            Err(ToolError::new("unbound execution is forbidden"))
        }
    }
    let workspace = tempfile::tempdir().unwrap();
    std::fs::write(workspace.path().join("README.md"), "single slot evidence\n").unwrap();
    let plane = Arc::new(crate::ToolExecutionPlane::new(
        Arc::new(ExecutionResourceManager::new([
            (ExecutionResourceKind::Tool, ResourceQuota::new(1, 1, 1).unwrap()),
            (ExecutionResourceKind::Custom("tool.cpu".into()), ResourceQuota::new(1, 1, 1).unwrap()),
        ])), Arc::new(ScopeLockManager::new()),
    ));
    let executed = Arc::new(AtomicUsize::new(0));
    let services = crate::RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .build().unwrap();
    let session = Session::new().with_workspace_root(workspace.path());
    let session_id = session.session_id.clone();
    // R4-034-fixture-fix: Goal creation moves below graph registration to bind the original execution id.
    let store = Arc::new(crate::test_support::session_store());
    store.create_session(&session::SessionRecord {
        session_id: session_id.clone(), platform: "test".into(), chat_id: "single-slot".into(),
        user_id: None, model: None, created_at: "2026-09-09T00:00:00Z".into(),
        last_activity: "2026-09-09T00:00:00Z".into(), message_count: 0,
        reset_policy: "manual".into(), metadata_json: None, input_tokens: 0, output_tokens: 0,
        status: "active".into(),
    }).await.unwrap();
    services.install_test_session_store(store.clone()).unwrap();
    let host = tools::ToolHost::builtin("single-slot", workspace.path())
        .with_authorization_lease_verifier(Arc::new(crate::AuthorizationNegotiator::verify_lease_signature));
    let lease = host.pin_snapshot();
    let definitions = lease.snapshot().catalog.definitions(None).into_iter()
        .filter(|definition| definition.name == "read_file")
        .map(|definition| crate::ProviderToolDefinition {
            name: definition.name, description: definition.description, input_schema: definition.input_schema,
        }).collect::<Vec<_>>();
    let mut graph = harness_contract::execution_graph::ExecutionGraph::new("read current file");
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: session_id.clone(), turn_id: "single-slot".into(), root_task_id: "single-slot-task".into(),
        task_id: "single-slot-task".into(), generation: 1,
    });
    // R4-034-fixture-fix: move Goal creation below graph registration so it can bind the graph id.
    let objective = harness_contract::agent_action::root_objective_id(&session_id, "single-slot");
    let goal: harness_contract::goal::GoalContract = serde_json::from_value(serde_json::json!({
        "id":"single-slot-goal", "session_id":session_id, "objective":"read current file",
        "criteria":[{"id":"read","statement":"read current file","status":"open"}],
        "phase":"execution", "spec_revision":1, "spec_digest":"single-slot-spec",
        "scope":"internal", "completion":"open", "revision":1, "user_sequence":1,
        "execution_binding":{"objective_id":objective,"session_id":session_id,
            "turn_id":"single-slot","root_execution_id":graph.id,
            "agentic_program_id":"single-slot-program"}
    })).unwrap();
    services.goal_store().create(goal).unwrap();
    let policy = PermissionPolicy::new(crate::PermissionMode::DangerFullAccess);
    let revision = policy.execution_policy_control().snapshot().revision;
    let bus = crate::CowdEventBus::new();
    let mut runtime = crate::ConversationRuntime::new(session, FinalAnswerClient,
        DelegatedTool { plane: plane.clone(), host, executed: executed.clone() },
        policy, vec!["bounded read".into()])
        .without_memory().with_runtime_event_store(services.event_store().clone())
        .with_tool_execution_plane(plane.clone())
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(store))
        .with_artifact_store(services.artifact_store().clone())
        .with_cowd_event_bus(bus.clone());
    runtime.begin_turn_strategy("single-slot", "read current file").unwrap();
    let strategy = runtime.bind_turn_strategy_execution("single-slot", &graph.id).unwrap();
    let step = Arc::new(crate::step_snapshot::BoundStep::bind(
        serde_json::from_value(serde_json::json!({"step_id":"single-slot-step","session_id":session_id,
            "turn_id":"single-slot","execution_id":graph.id,"build_ref":"isolated-fixture"})).unwrap(),
        lease, &definitions, &serde_json::json!({}), &serde_json::json!({}),
        BTreeMap::from([("policy".into(), revision.to_string())]), chrono::Utc::now().timestamp_millis()+60_000,
    ).unwrap());
    let calls = vec![ModelToolCall { binding: Some(crate::step_snapshot::StepCallBinding::provider(step, "read")),
        id: "read".into(), name: "read_file".into(), input: r#"{"path":"README.md","complete":true}"#.into(), depends_on: vec![] }];
    let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
        harness_contract::execution_graph::ExecutionNodeKind::ToolBatch, "tool_batch",
        encode_tool_calls_with_continuation(&session_id, &calls, false).unwrap());
    node.id = format!("{}:original-tools", graph.id);
    node.idempotency_key = format!("{}:original", node.id);
    graph.nodes.push(node.clone());
    let mut state = r1_branch_state(&session_id, "single-slot-goal");
    state.turn_id = "single-slot".into();
    state.task_understanding = Some(strategy.decision.strategy.understanding.clone());
    state.collaboration_obligation = strategy.decision.collaboration_obligation.clone();
    let runtime = Arc::new(tokio::sync::Mutex::new(Box::new(runtime)));
    let state = Arc::new(tokio::sync::Mutex::new(state));
    services.tool_batch_executor().install_resolver(Arc::new(TurnToolResolver {
        session_id: session_id.clone(), graph_id: graph.id.clone(), runtime: Arc::downgrade(&runtime),
        state: Arc::downgrade(&state), services: Arc::downgrade(&services),
    }));
    let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
        services.executor_registry().clone(), services.graph_state_store().clone(), services.commit_service().clone(),
        services.resource_manager().clone(), services.scope_locks().clone(), services.worktree_leases().clone(),
        services.workspace_key(), services.workspace_root());
    let registered = runner.register(graph).await.unwrap();
    let snapshot = runner.pump_snapshot(&registered.id, &BTreeSet::new()).await.unwrap();
    let mut wave = runner.prepare_ready_wave(&registered.id, snapshot.ready).await.unwrap();
    assert_eq!(wave.len(), 1);
    let activity = services.event_store()
        .latest_for_stream(&format!("{}:node:{}", registered.id, node.id)).unwrap().unwrap()
        .activity_binding().unwrap();
    let _scope = bus.enter_execution_with_activity(crate::CowdExecutionContext {
        execution_id: registered.id.clone(), session_id, turn_id: "single-slot".into(),
    }, Some(activity));
    tokio::time::timeout(std::time::Duration::from_secs(2),
        runner.execute_prepared_pump_node(&registered.id, wave.remove(0), Arc::new(tokio::sync::Notify::new())))
        .await.expect("outer conversation cannot hold the slot needed by its delegated leaf").unwrap();
    let graph = services.graph_state_store().load(&registered.id).unwrap();
    assert_eq!(graph.node_statuses[&node.id], harness_contract::execution_graph::ExecutionNodeStatus::Completed,
        "{:?}", graph.node_results.get(&node.id));
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(plane.stats().active, 0);
    assert_eq!(plane.stats().submitted, 1, "one invocation must acquire one Tool admission");
}

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




#[test]
fn dynamic_tool_nodes_preserve_file_resource_scopes() {
    let same_file = resource_scopes_for_tool_calls(&[
        ModelToolCall {
            binding: None,
            id: "write-a".into(),
            name: "edit".into(),
            input: r#"{"path":"src/lib.rs","content":"a"}"#.into(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "write-b".into(),
            name: "edit".into(),
            input: r#"{"path":"src/lib.rs","old_string":"a","new_string":"b"}"#.into(),
            depends_on: Vec::new(),
        },
    ]);
    assert_eq!(same_file, vec!["write:src/lib.rs"]);

    let different_files = resource_scopes_for_tool_calls(&[
        ModelToolCall {
            binding: None,
            id: "write-a".into(),
            name: "edit".into(),
            input: r#"{"path":"src/a.rs","content":"a"}"#.into(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "write-b".into(),
            name: "edit".into(),
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
            binding: None,
            id: "valid".into(),
            name: "grep_search".into(),
            input: serde_json::json!({"path": inside, "pattern": "Runtime"}).to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
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
            binding: None,
            id: "write".into(),
            name: "edit".into(),
            input: r#"{"path":"src/lib.rs","content":"updated"}"#.into(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
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
            binding: None,
            id: "capabilities".to_string(),
            name: "runtime_capabilities".to_string(),
            input: "{}".to_string(),
            depends_on: Vec::new(),
        },
        ModelToolCall {
            binding: None,
            id: "mistyped-read".to_string(),
            name: "tool_orchestrate".to_string(),
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

    let nodes = tool_nodes_for_calls(&ticket, 1, "session", calls, workspace.path(), None, false)
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
        .observe_tool_scope("edit", "write:src/lib.rs", Some("same"), 1)
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
    let rejected_action = harness_contract::agent_action::AgentActionObservation {
        receipt_id: "rejection:stale".to_string(),
        action_id: "action:stale".to_string(),
        action: "task_review".to_string(),
        program_id: "program:test".to_string(),
        revision: 9,
        status: harness_contract::agent_action::AgentActionStatus::Rejected,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: vec!["inspect current state and retry".to_string()],
        projection: None,
        error: Some(
            harness_contract::agent_action::AgentActionErrorObservation {
                code: "stale_revision".to_string(),
                message: "expected Program revision 8, actual 9".to_string(),
                recoverable: true,
            },
        ),
    };
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
        ConversationMessage::tool_result(
            "review-rejected",
            "task_review",
            serde_json::to_string(&rejected_action).expect("rejected action receipt"),
            false,
        ),
    ];
    let successful_ids = successful_tool_call_ids(&messages);
    assert_eq!(successful_ids, BTreeSet::from(["search-ok".to_string()]));
    assert!(
        typed_satisfied_focus_acceptance_scope_keys(
            &["network:*".to_string()],
            &[],
            &[],
            &crate::path_identity::WorkspacePathIdentityResolver::discover(
                tempfile::tempdir().unwrap().path(),
            )
            .unwrap(),
        )
        .is_empty(),
        "a successful ToolResult is liveness only; no typed network receipt means no Focus proof"
    );
}

#[test]
fn write_attempt_paths_are_projectable_workspace_relative_refs() {
    let root = tempfile::tempdir().expect("workspace");
    let target = root.path().join("fixtures/target.txt");
    let calls = [ModelToolCall {
        binding: None,
        id: "write".into(),
        name: "edit".into(),
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
        std::fs::write(workspace.path().join("fixtures").join(path), path).expect("exact fixture");
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
    assert_eq!(calls.iter().map(|call| call.id.as_str()).collect::<Vec<_>>(), ["runtime-focus-verify-7-0", "runtime-focus-verify-7-1", "runtime-focus-verify-7-2"]);
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

    assert!(
        focus_verification_tool_calls(&["read:crates/gateway".into()], 8, workspace.path())
            .is_none()
    );
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
    std::fs::write(workspace.path().join("fixtures/target.txt"), "verified").expect("target");
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
        None,
        false,
    )
    .expect("verification nodes");

    assert!(matches!(nodes.as_slice(), [item] if item.id == "graph:4:tools-1"));
    assert_eq!(nodes[0].id, "graph:4:tools-1");
    assert_ne!(nodes[0].id, ticket.node_id);
}

#[test]
fn only_first_step_bounded_focus_evidence_is_prefetched() {
    let workspace = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(workspace.path().join("fixtures")).expect("fixtures");
    std::fs::write(workspace.path().join("fixtures/target.txt"), "target").expect("target");
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
        binding: None,
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
    assert!(successful_runtime_focus_scope_keys(&calls, &BTreeSet::new(), &required).is_empty());

    let bounded_calls = vec![ModelToolCall {
        binding: None,
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
        .observe_tool_scope("edit", "write:fixtures/target.txt", Some("current"), 1)
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
        .observe_tool_scope("edit", "write:report/index.html", Some("report-digest"), 1)
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
        binding: None,
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
        .observe_tool_scope("edit", "write:crates/runtime/src/lib.rs", Some("digest"), 1)
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

#[test]
fn accepted_goal_replaces_legacy_predictions_but_missing_source_keeps_its_original_baseline() {
    use harness_contract::goal::{GoalInputCommitment, GoalInputContract, GoalInputSpan};
    let mut goal: GoalContract = serde_json::from_value(serde_json::json!({
        "id":"goal:current","session_id":"session","objective":"current instruction",
        "criteria":[{"id":"intent","statement":"current instruction","status":"open"}],
        "phase":"execution","spec_revision":2,"spec_digest":"a".repeat(64),"scope":"user_objective",
        "completion":"open","revision":2,"user_sequence":2
    }))
    .unwrap();
    goal.input_contract = Some(GoalInputContract {
        version: 1,
        principal_id: "session:session".into(),
        session_id: "session".into(),
        turn_id: "turn".into(),
        source_ref: "original-source".into(),
        input_revision: 1,
        content_sha256: "b".repeat(64),
        commitments: vec![],
        proposed_workspace_scopes: vec!["write:old-report.txt".into()],
    });
    assert_eq!(
        current_turn_write_scopes(&goal, false, &[], true),
        vec!["write:old-report.txt"]
    );
    assert!(current_turn_forbids_tools(&goal, true));
    assert!(current_turn_forbids_workspace_write(&goal, true));
    goal.input_contract.as_mut().unwrap().version = 2;
    assert!(
        current_turn_write_scopes(&goal, false, &[], true).is_empty(),
        "an authenticated revision may withdraw the old artifact obligation"
    );
    assert!(
        !current_turn_forbids_tools(&goal, true),
        "the old prediction cannot override an accepted revision"
    );
    assert!(!current_turn_forbids_workspace_write(&goal, true));
    goal.input_contract.as_mut().unwrap().commitments = vec![GoalInputCommitment::WorkspaceWrite {
        path: None,
        source: GoalInputSpan { start: 0, end: 3 },
    }];
    assert_eq!(
        current_turn_write_scopes(&goal, false, &[], false),
        vec![String::new()],
        "user-selected naming remains autonomous"
    );
    assert_eq!(
        current_turn_write_scopes(&goal, true, &["write:delegated.txt".into()], false),
        vec!["write:delegated.txt"],
        "a delegated exact scope is a separate existing contract"
    );
}
