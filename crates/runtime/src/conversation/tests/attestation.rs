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
            vec!["read_file".into(), "edit".into()]
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
    async fn root_nonowns_scope_v2_uses_original_toolbatch_owner_for_read_write_and_replay() {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(
            workspace.path().join("input.txt"),
            "original complete input\n",
        )
        .unwrap();
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let commits = crate::ExecutionCommitService::new(store.clone());
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
        graph.nodes.extend([decoy, original_node]);
        let mut graph = commits.register_graph(graph).unwrap().graph;
        for node_id in [
            "scope-v2-running-model-decoy",
            "scope-v2-original-tool-owner",
        ] {
            for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
                graph = commits
                    .transition_node(&graph, node_id, status, None, vec![])
                    .unwrap()
                    .graph;
            }
        }
        let original = store
            .latest_for_stream(&format!("{}:node:scope-v2-original-tool-owner", graph.id))
            .unwrap()
            .unwrap()
            .activity_binding()
            .unwrap();
        let attempt = graph.recovery_cursor.node_attempts["scope-v2-original-tool-owner"];
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
        .with_artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        )))
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
        let _scope = bus.enter_execution_with_activity(execution.clone(), Some(original.clone()));
        assert_eq!(
            runtime.root_effect_owner().unwrap(),
            (original.clone(), attempt)
        );
        for call in [&read, &write] {
            let result = runtime
                .execute_tool_batch_step(
                    std::slice::from_ref(call),
                    &crate::SharedPrompter::none(),
                    4,
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
            let physical_before = calls.lock().unwrap().len();
            let replay = runtime
                .execute_tool_batch_step(
                    std::slice::from_ref(call),
                    &crate::SharedPrompter::none(),
                    4,
                )
                .await
                .unwrap();
            assert_eq!(replay.failed, 0);
            assert_eq!(calls.lock().unwrap().len(), physical_before);
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
        let _restored = bus.enter_execution_with_activity(execution, Some(original));
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
        let refused = runtime
            .execute_tool_batch_step(&[expired_call], &crate::SharedPrompter::none(), 4)
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
        let result = runtime
            .execute_tool_batch_step(
                std::slice::from_ref(&failed),
                &crate::SharedPrompter::none(),
                4,
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

mod r1_rejected_batch_regressions {
    use super::*;

    #[derive(Clone, Copy, Debug)]
    enum BatchCase {
        InvalidTaskId,
        Partial,
        Strategy,
        CompletedOwnerNegative,
    }

    struct CountingHost {
        inner: ConcurrentRuntimeToolHost,
        executed: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for CountingHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            self.executed
                .lock()
                .unwrap()
                .push(request.tool_use_id.clone());
            self.inner.execute_runtime_tool(request).await
        }
    }

    async fn run_rejected_batch_through_original_compactor(case: BatchCase) {
        let workspace = tempfile::tempdir().expect("isolated builtin workspace");
        let services = crate::RuntimeServices::in_memory().expect("runtime services");
        let session = Session::new();
        let session_id = session.session_id.clone();
        let stream_id = format!("session:{session_id}");
        let session_store = Arc::new(crate::test_support::session_store());
        session_store
            .create_session(&session::SessionRecord {
                session_id: session_id.clone(),
                platform: "test".into(),
                chat_id: "r1-rejected-batch".into(),
                user_id: None,
                model: None,
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
            .expect("real Session record");
        let bus = crate::CowdEventBus::new();
        let execution = crate::CowdExecutionContext {
            execution_id: format!("r1-batch-root:{session_id}"),
            session_id: session_id.clone(),
            turn_id: "r1-batch-turn".into(),
        };
        crate::test_support::register_running_root(Arc::clone(services.event_store()), &execution);
        let original_graph = services
            .graph_state_store()
            .load(&execution.execution_id)
            .unwrap();
        assert_eq!(original_graph.nodes.len(), 1);
        let binding = services
            .event_store()
            .latest_for_stream(&format!(
                "{}:node:{}",
                original_graph.id, original_graph.nodes[0].id
            ))
            .unwrap()
            .unwrap()
            .activity_binding()
            .expect("original running node activity");
        let _execution_scope = bus.enter_execution_with_activity(execution.clone(), Some(binding));
        let runtime = crate::ConversationRuntime::new(
            session,
            FinalAnswerClient,
            RecordingToolExecutor {
                executed: Arc::new(AtomicUsize::new(0)),
                order: Arc::new(Mutex::new(Vec::new())),
            },
            PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            vec!["read existing results".into()],
        )
        .without_memory()
        .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
            Arc::clone(&session_store),
        ))
        .with_artifact_store(Arc::clone(services.artifact_store()))
        .with_runtime_event_store(Arc::clone(services.event_store()))
        .with_cowd_event_bus(bus);
        let runtime = Arc::new(tokio::sync::Mutex::new(runtime));

        let tool_host = tools::ToolHost::builtin("r1-rejected-batch", workspace.path());
        let lease = tool_host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == "read_file")
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(advertised.len(), 1);
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                serde_json::from_value(serde_json::json!({
                    "step_id": "r1-batch-step",
                    "session_id": session_id,
                    "turn_id": execution.turn_id,
                    "execution_id": execution.execution_id,
                    "build_ref": "isolated-fixture"
                }))
                .unwrap(),
                lease,
                &advertised,
                &serde_json::json!({}),
                &serde_json::json!({}),
                BTreeMap::new(),
                i64::MAX,
            )
            .expect("real builtin pinned Step"),
        );
        let specifications = match case {
            BatchCase::InvalidTaskId => vec![
                (" invalid-read ", "read_file", Vec::new()),
                ("canonical-read", "read_file", Vec::new()),
            ],
            BatchCase::Partial => vec![
                ("invalid", "not_registered", Vec::new()),
                ("blocked", "read_file", vec!["invalid".to_string()]),
                ("healthy", "read_file", Vec::new()),
            ],
            BatchCase::Strategy => vec![
                ("strategy-read-1", "read_file", Vec::new()),
                ("strategy-read-2", "read_file", Vec::new()),
            ],
            BatchCase::CompletedOwnerNegative => {
                vec![("healthy", "read_file", Vec::new())]
            }
        };
        let calls = specifications
            .into_iter()
            .map(|(call_id, name, depends_on)| ModelToolCall {
                id: call_id.into(),
                name: name.into(),
                input: serde_json::json!({"path": format!("{call_id}.txt")}).to_string(),
                depends_on,
                binding: (name == "read_file").then(|| {
                    crate::step_snapshot::StepCallBinding::provider(Arc::clone(&step), call_id)
                }),
            })
            .collect::<Vec<_>>();
        let prepared = calls
            .iter()
            .filter(|call| call.name == "read_file")
            .map(|call| {
                let mut invocation = step.lease().prepare_governed_invocation(
                    &call.id,
                    &call.name,
                    &serde_json::from_str(&call.input).unwrap(),
                    &call.depends_on,
                );
                invocation.origin = Some(call.binding.as_ref().unwrap().origin.clone());
                (call.id.clone(), invocation)
            })
            .collect::<HashMap<_, _>>();
        let requests = calls
            .iter()
            .map(|call| crate::tool_dispatch::ToolRequest {
                tool_use_id: call.id.clone(),
                tool_name: call.name.clone(),
                input: call.input.clone(),
                depends_on: call.depends_on.clone(),
            })
            .collect::<Vec<_>>();
        let mut compilation = crate::GovernedToolCompiler.compile_partial(
            workspace.path(),
            &requests,
            |name, input| {
                requests
                    .iter()
                    .find(|request| {
                        request.tool_name == name
                            && serde_json::from_str::<serde_json::Value>(&request.input).unwrap()
                                == *input
                    })
                    .and_then(|request| prepared.get(&request.tool_use_id))
                    .map(|invocation| {
                        (
                            invocation.effect.clone(),
                            invocation.catalog_revision,
                            invocation.descriptor_set_hash.clone(),
                        )
                    })
            },
        );
        if let Ok(compiled) = compilation.as_mut() {
            if let Some(plan) = compiled.plan.as_mut() {
                crate::step_snapshot::bind_plan_calls(plan, &calls);
            }
        }
        if let Ok(compiled) = &compilation {
            if let Some(plan) = &compiled.plan {
                for task in &plan.tasks {
                    let call = calls
                        .iter()
                        .find(|call| call.id == task.tool_call_id)
                        .unwrap();
                    assert_eq!(
                        task.invocation.origin.as_ref(),
                        call.binding.as_ref().map(|binding| &binding.origin)
                    );
                }
            }
        }
        // The retired Strategy denial case now exercises a real prepared
        // effect disagreement. Original Step/call and all raw/compaction
        // negative oracles below remain unchanged.
        if matches!(case, BatchCase::Strategy) {
            let plan = compilation.as_mut().unwrap().plan.as_mut().unwrap();
            plan.tasks[0].effect.effect_kind = harness_contract::tool::ToolEffectKind::Write;
        }
        let decision =
            crate::execution_core::build_runtime_execution_decision("read existing files", None);
        let expected_plan = match case {
            BatchCase::InvalidTaskId => {
                assert!(matches!(
                    &compilation,
                    Err(crate::GovernedToolCompileError::InvalidTaskId {
                        index: 0, task_id, ..
                    }) if task_id == " invalid-read "
                ));
                None
            }
            _ => {
                let compiled = compilation.as_ref().expect("original compiler succeeds");
                let plan = compiled.plan.as_ref().expect("nonempty real plan");
                assert!(!plan.tasks.is_empty());
                if matches!(case, BatchCase::Partial) {
                    assert_eq!(
                        compiled
                            .rejected
                            .iter()
                            .map(|rejection| rejection.tool_call_id.as_str())
                            .collect::<Vec<_>>(),
                        ["invalid", "blocked"],
                    );
                    assert_eq!(plan.tasks.len(), 1);
                    assert_eq!(plan.tasks[0].tool_call_id, "healthy");
                } else {
                    assert!(compiled.rejected.is_empty());
                    assert_eq!(plan.tasks.len(), calls.len());
                }
                if matches!(case, BatchCase::Strategy) {
                    let validation = plan.validate_against_execution_decision(&decision);
                    assert!(!validation.allowed);
                    assert!(validation
                        .findings
                        .iter()
                        .any(|finding| finding == "prepared_effect_identity_mismatch"));
                } else {
                    assert!(plan.validate_against_execution_decision(&decision).allowed);
                }
                Some((plan.plan_id.clone(), plan.revision))
            }
        };
        eprintln!("B2 {case:?}: original compilation={compilation:?}");
        let executed = Arc::new(Mutex::new(Vec::new()));
        let host: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(CountingHost {
            executed: Arc::clone(&executed),
            inner: ConcurrentRuntimeToolHost {
                active: Arc::new(AtomicUsize::new(0)),
                observed_peak: Arc::new(AtomicUsize::new(0)),
            },
        });
        let ticket = NodeExecutionTicket {
            graph_id: "r1-batch-graph".into(),
            node_id: "r1-batch-tools".into(),
            executor_kind: "tool_batch".into(),
            service_class: harness_contract::execution_graph::ExecutionServiceClass::Interactive,
            attempt: 1,
            idempotency_key: format!("r1-batch:{session_id}"),
            payload_ref: String::new(),
        };
        let before = services.event_store().list_stream(&stream_id).unwrap();
        let governed = execute_governed_runtime_tool_batch(
            host,
            None,
            &calls,
            &session_id,
            harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            0,
            None,
            None,
            None,
            &ticket,
            1,
            &HashMap::new(),
            &HashMap::new(),
            &prepared,
            compilation,
            &decision,
            services.tool_execution_plane(),
            services.tool_batch_executor(),
            services.commit_service(),
            &BTreeMap::new(),
        )
        .await;
        let executed_ids = executed.lock().unwrap().clone();
        let has_healthy = matches!(case, BatchCase::Partial | BatchCase::CompletedOwnerNegative);
        assert_eq!(
            executed_ids,
            if has_healthy {
                vec!["healthy".to_string()]
            } else {
                Vec::new()
            },
        );
        assert_eq!(governed.invocations.len(), calls.len());
        assert_eq!(governed.messages.len(), calls.len());
        for call in &calls {
            let record = &governed.invocations[&call.id];
            assert_eq!(record.session_id, session_id);
            assert_eq!(record.tool_call_id, call.id);
            assert_eq!(record.tool_name, call.name);
            assert_eq!(
                record.input_hash,
                crate::tool_invocation::stable_hash(&call.input)
            );
            assert!(record.ended_at_ms.is_some());
            assert_eq!(
                record.status,
                if has_healthy && call.id == "healthy" {
                    crate::ToolInvocationStatus::Completed
                } else {
                    crate::ToolInvocationStatus::Failed
                },
                "{case:?}: call={} record={record:?}; messages={:?}",
                call.id,
                governed.messages,
            );
            if matches!(case, BatchCase::Strategy) {
                let (plan_id, revision) = expected_plan.as_ref().unwrap();
                assert_eq!(record.governed_plan_id.as_ref(), Some(plan_id));
                assert_eq!(record.governed_plan_revision, Some(*revision));
            }
        }

        if matches!(case, BatchCase::CompletedOwnerNegative) {
            let pristine = services.event_store().list_stream(&stream_id).unwrap();
            let raw_before = session_store
                .session_domain_events_page(&session_id, 0, 100)
                .await
                .unwrap()
                .events
                .into_iter()
                .filter(|event| event.kind == "evidence.raw.persisted")
                .collect::<Vec<_>>();
            assert!(raw_before.is_empty());
            let audits_before = runtime.lock().await.turn_evidence_audit_count_for_test();
            for hostile_field in [
                "foreign-session",
                "unknown-session",
                "call",
                "name",
                "input",
            ] {
                let mut hostile = governed.invocations.clone();
                let record = hostile.get_mut("healthy").unwrap();
                match hostile_field {
                    "foreign-session" => record.session_id = "foreign-session".into(),
                    "unknown-session" => record.session_id = "unknown".into(),
                    "call" => record.tool_call_id = "foreign-call".into(),
                    "name" => record.tool_name = "foreign-tool".into(),
                    "input" => {
                        record.input_hash = crate::tool_invocation::stable_hash("foreign input")
                    }
                    _ => unreachable!(),
                }
                let rejected = compact_governed_tool_messages(
                    &runtime,
                    &calls,
                    governed.messages.clone(),
                    &hostile,
                )
                .await
                .expect_err("explicit hostile clone must fail the original consumer");
                assert!(rejected
                    .to_string()
                    .contains("crosses its original call/input owner"));
                let after = services.event_store().list_stream(&stream_id).unwrap();
                assert_eq!(
                    after
                        .iter()
                        .map(|event| &event.event_id)
                        .collect::<Vec<_>>(),
                    pristine
                        .iter()
                        .map(|event| &event.event_id)
                        .collect::<Vec<_>>(),
                );
                assert_eq!(
                    runtime.lock().await.turn_evidence_audit_count_for_test(),
                    audits_before,
                );
                let raw_after = session_store
                    .session_domain_events_page(&session_id, 0, 100)
                    .await
                    .unwrap()
                    .events
                    .into_iter()
                    .filter(|event| event.kind == "evidence.raw.persisted")
                    .collect::<Vec<_>>();
                assert!(
                    raw_after.is_empty(),
                    "hostile clone must not persist raw evidence"
                );
                eprintln!("B2 hostile completed clone {hostile_field}: compactor rejected");
            }
        }

        let original_messages = governed.messages.clone();
        let compacted = compact_governed_tool_messages(
            &runtime,
            &calls,
            governed.messages,
            &governed.invocations,
        )
        .await
        .expect("unaltered real batch must cross original compactor");
        assert_eq!(compacted.len(), calls.len());
        let events = services.event_store().list_stream(&stream_id).unwrap();
        let raw_events = session_store
            .session_domain_events_page(&session_id, 0, 100)
            .await
            .unwrap()
            .events
            .into_iter()
            .filter(|event| event.kind == "evidence.raw.persisted")
            .collect::<Vec<_>>();
        assert_eq!(raw_events.len(), calls.len());
        let new_events = events
            .iter()
            .filter(|event| !before.iter().any(|old| old.event_id == event.event_id))
            .collect::<Vec<_>>();
        assert!(
            !new_events.is_empty(),
            "C3 must not pass on an empty event store"
        );
        let mut completed_count = 0;
        for (index, call) in calls.iter().enumerate() {
            let healthy = has_healthy && call.id == "healthy";
            let expected_error = !healthy;
            let raw_output = match original_messages[index].blocks.as_slice() {
                [ContentBlock::ToolResult {
                    tool_use_id,
                    tool_name,
                    output,
                    is_error,
                }] => {
                    assert_eq!(tool_use_id, &call.id);
                    assert_eq!(tool_name, &call.name);
                    assert_eq!(*is_error, expected_error);
                    output
                }
                _ => panic!("one original ToolResult per call"),
            };
            if matches!(case, BatchCase::Strategy) {
                assert!(raw_output.contains("runtime strategy lease"));
                assert!(raw_output.contains("prepared_effect_identity_mismatch"));
            }
            assert!(matches!(
                compacted[index].blocks.as_slice(),
                [ContentBlock::ToolResult { tool_use_id, tool_name, is_error, .. }]
                    if tool_use_id == &call.id && tool_name == &call.name && *is_error == expected_error
            ));
            let call_events = new_events
                .iter()
                .filter(|event| event.payload["tool_call_id"] == call.id)
                .collect::<Vec<_>>();
            let started = call_events
                .iter()
                .filter(|event| event.kind == "tool.invocation.started")
                .collect::<Vec<_>>();
            assert_eq!(started.len(), 1, "real started for {}", call.id);
            let terminal_kind = if healthy {
                "tool.invocation.completed"
            } else {
                "tool.invocation.failed"
            };
            let terminals = call_events
                .iter()
                .filter(|event| event.kind == terminal_kind)
                .collect::<Vec<_>>();
            assert_eq!(terminals.len(), 1, "real terminal for {}", call.id);
            assert_eq!(
                call_events
                    .iter()
                    .filter(|event| event.kind == "tool.invocation.completed")
                    .count(),
                usize::from(healthy),
            );
            for event in [started[0], terminals[0]] {
                assert_eq!(event.stream_id, stream_id);
                assert_eq!(event.payload["session_id"], session_id);
                assert_eq!(event.payload["tool_name"], call.name);
                assert_eq!(
                    event.payload["input_hash"],
                    crate::tool_invocation::stable_hash(&call.input)
                );
            }
            let terminal = terminals[0];
            assert_eq!(terminal.actor.as_deref(), Some("conversation_runtime"));
            assert_eq!(
                terminal.payload["status"],
                if healthy { "completed" } else { "failed" }
            );
            let raw = &terminal.payload["runtime_raw_content_v1"];
            assert_eq!(raw["version"], 1);
            assert_eq!(raw["tool_call_id"], call.id);
            assert_eq!(raw["ref_id"], terminal.payload["full_output_ref"]);
            assert!(raw["ref_id"]
                .as_str()
                .unwrap()
                .starts_with("tool://tool-raw-"));
            assert_eq!(raw["visibility_scope"], format!("session:{session_id}"));
            assert_eq!(
                raw["sha256"],
                crate::context_evidence::read_sha256(raw_output.as_bytes())
            );
            assert_eq!(raw["byte_count"], raw_output.len() as u64);
            assert_eq!(
                raw_events
                    .iter()
                    .filter(|event| {
                        event.payload["tool_call_id"] == call.id
                            && event.payload["session_id"] == session_id
                            && event.payload["artifact_selector"] == raw["selector"]
                            && event.payload["is_error"] == expected_error
                    })
                    .count(),
                1,
                "same-Session raw receipt for {}",
                call.id
            );
            let artifact = harness_contract::context::ArtifactRef::durable(
                raw["selector"].as_str().unwrap(),
                raw["sha256"].as_str().unwrap(),
                raw["byte_count"].as_u64().unwrap(),
                raw["media_type"].as_str().unwrap(),
                raw["visibility_scope"].as_str().unwrap(),
            );
            let bytes = services
                .artifact_store()
                .read(&artifact, &format!("session:{session_id}"), None)
                .await
                .expect("original writer persisted actual raw bytes");
            assert_eq!(bytes, raw_output.as_bytes());
            if healthy {
                completed_count += 1;
                assert_eq!(raw_output, "read_file complete");
                assert!(matches!(
                    compacted[index].blocks.as_slice(),
                    [ContentBlock::ToolResult { output, .. }] if output.contains(raw_output)
                ));
            }
        }
        assert_eq!(completed_count, usize::from(has_healthy));
        eprintln!(
            "B2 {case:?}: executed={executed_ids:?}; compactor=Ok; completed/raw={completed_count}"
        );
    }

    #[tokio::test]
    async fn r1_read_delivery_host_compile_rejection_keeps_original_session_through_compactor() {
        run_rejected_batch_through_original_compactor(BatchCase::InvalidTaskId).await;
    }

    #[tokio::test]
    async fn r1_read_delivery_host_partial_rejection_preserves_healthy_sibling() {
        run_rejected_batch_through_original_compactor(BatchCase::Partial).await;
    }

    #[tokio::test]
    async fn r1_read_delivery_host_strategy_rejection_keeps_original_session_through_compactor() {
        run_rejected_batch_through_original_compactor(BatchCase::Strategy).await;
    }

    #[tokio::test]
    async fn r1_read_delivery_host_completed_foreign_session_still_fails_compactor() {
        run_rejected_batch_through_original_compactor(BatchCase::CompletedOwnerNegative).await;
    }
}

#[tokio::test]
async fn delegated_leaf_does_not_double_acquire_the_single_tool_slot() {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ExecutionResourceManager, ResourceQuota, ScopeLockManager,
    };
    struct DelegatedTool {
        plane: Arc<crate::ToolExecutionPlane>,
        inner: RecordingToolExecutor,
    }
    #[async_trait::async_trait]
    impl ToolExecutor for DelegatedTool {
        fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
            self.inner.tool_discovery_receipt()
        }
        async fn execute_output(
            &self,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            let (output, admission) = self
                .plane
                .execute_async_classified_retained(
                    &harness_contract::tool::ResourceDemand::default(),
                    None,
                    Default::default(),
                    None,
                    Some("single-slot"),
                    self.inner.execute_output(name, input),
                )
                .await;
            drop(admission);
            output.map_err(|error| ToolError::new(error.to_string()))?
        }
        fn available_tool_names(&self) -> Vec<String> {
            self.inner.available_tool_names()
        }
        fn registered_tool_effect(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            self.inner.registered_tool_effect(name, input)
        }
        fn owns_tool_resource_admission(&self, _: &str) -> bool {
            true
        }
        async fn execute_authorized_output(
            &self,
            _: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            self.execute_output(name, input).await
        }
    }
    let plane = Arc::new(crate::ToolExecutionPlane::new(
        Arc::new(ExecutionResourceManager::new([
            (
                ExecutionResourceKind::Tool,
                ResourceQuota::new(1, 1, 1).unwrap(),
            ),
            (
                ExecutionResourceKind::Custom("tool.cpu".into()),
                ResourceQuota::new(1, 1, 1).unwrap(),
            ),
        ])),
        Arc::new(ScopeLockManager::new()),
    ));
    let executed = Arc::new(AtomicUsize::new(0));
    let services = crate::RuntimeServices::in_memory().unwrap();
    let session = Session::new();
    let store = Arc::new(crate::test_support::session_store());
    store
        .create_session(&session::SessionRecord {
            session_id: session.session_id.clone(),
            platform: "test".into(),
            chat_id: "single-slot".into(),
            user_id: None,
            model: None,
            created_at: "2026-09-09T00:00:00Z".into(),
            last_activity: "2026-09-09T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .unwrap();
    let runtime = crate::ConversationRuntime::new(
        session,
        FinalAnswerClient,
        DelegatedTool {
            plane: plane.clone(),
            inner: RecordingToolExecutor {
                executed: executed.clone(),
                order: Arc::new(Mutex::new(vec![])),
            },
        },
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["bounded read".into()],
    )
    .without_memory()
    .with_runtime_event_store(services.event_store().clone())
    .with_tool_execution_plane(plane.clone())
    .with_session_journal_port(crate::session_runtime_port::TestSessionPortAdapter::new(
        store,
    ))
    .with_artifact_store(services.artifact_store().clone());
    runtime
        .begin_turn_strategy("single-slot", "read current file")
        .unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        runtime.execute_tool_batch_step(
            &[ModelToolCall {
                binding: None,
                id: "read".into(),
                name: "read_file".into(),
                input: "{\"path\":\"README.md\"}".into(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            1,
        ),
    )
    .await
    .expect("outer conversation cannot hold the slot needed by its delegated leaf")
    .unwrap();
    assert_eq!(result.failed, 0, "{:?}", result.messages);
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(plane.stats().active, 0);
    assert_eq!(
        plane.stats().submitted,
        1,
        "one invocation must acquire one Tool admission"
    );
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
#[tokio::test(flavor = "multi_thread")]
async fn model_step_only_plans_then_runner_executes_dependent_tool_wave_once() {
    exercise_dependency_wave_with_foreign_refusal(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_session_refusal_does_not_disable_dependency_tools_for_same_turn_id() {
    exercise_dependency_wave_with_foreign_refusal(true).await;
}

async fn exercise_dependency_wave_with_foreign_refusal(foreign_refusal: bool) {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let executed = Arc::new(AtomicUsize::new(0));
    let executions_seen_before_second_model = Arc::new(AtomicUsize::new(0));
    let order = Arc::new(Mutex::new(Vec::new()));
    let session = Session::new();
    let session_store = Arc::new(crate::test_support::session_store());
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
    crate::test_support::register_running_root(
        Arc::clone(services.event_store()),
        &crate::CowdExecutionContext {
            execution_id: "test-root-execution".into(),
            session_id: session.session_id.clone(),
            turn_id: "test-turn".into(),
        },
    );
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
    if foreign_refusal {
        crate::repetition_guard::mark_refusal_ignored(
            &crate::repetition_guard::RepetitionScope::turn("foreign-refusal-session", "turn-test"),
        );
    }
    let runtime = crate::ConversationRuntime::new(
        session,
        TwoToolClient {
            requests: 0,
            event_store: Arc::clone(services.event_store()),
            exposure: None,
            executed: Arc::clone(&executed),
            executions_seen_before_second_model: Arc::clone(&executions_seen_before_second_model),
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
    assert_eq!(order.lock().unwrap().as_slice(), ["read_file", "edit"]);
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
            .filter(
                |block| matches!(block, crate::ContentBlock::Text { text } if text == "done once")
            )
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
    for tool_name in ["read_file", "edit"] {
        assert!(
            events.iter().any(|event| {
                event.kind == "tool.invocation.started" && event.payload["tool_name"] == tool_name
            }),
            "missing started event for {tool_name}; events={event_kinds:?}"
        );
        assert!(
            events.iter().any(|event| {
                event.kind == "tool.invocation.completed" && event.payload["tool_name"] == tool_name
            }),
            "missing completed event for {tool_name}; events={event_kinds:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn governed_runtime_tool_batch_fans_out_many_independent_reads_and_keeps_order() {
    struct LeaseCheckingHost {
        inner: ConcurrentRuntimeToolHost,
        lease: harness_contract::agent::AgentDataLease,
    }
    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for LeaseCheckingHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            assert_eq!(request.reality_context.as_ref(), Some(&self.lease));
            self.inner.execute_runtime_tool(request).await
        }
    }
    let reality_lease = harness_contract::agent::AgentDataLease {
        session_id: "session".into(),
        task_id: "task".into(),
        team_id: None,
        read_scopes: vec![harness_contract::agent::CognitiveReadScope::Session],
        write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
        fact_boundaries: vec!["observed".into()],
        fact_refs: vec!["fact:exact".into()],
        matrix_snapshot_refs: vec!["matrix:source_snapshot:exact".into()],
    };
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let host: Arc<dyn crate::RuntimeExecutionHost> = Arc::new(LeaseCheckingHost {
        lease: reality_lease.clone(),
        inner: ConcurrentRuntimeToolHost {
            active: Arc::clone(&active),
            observed_peak: Arc::clone(&peak),
        },
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
            binding: None,
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
            let normalized_input = serde_json::from_str::<serde_json::Value>(&call.input).unwrap();
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
                    origin: None,
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
        Some(&reality_lease),
        None,
        &ticket,
        1,
        &std::collections::HashMap::new(),
        &std::collections::HashMap::new(),
        &tool_effects,
        compilation,
        &decision,
        services.tool_execution_plane(),
        services.tool_batch_executor(),
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
    let mut request =
        crate::RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
            tool_use_id: "original-read".to_string(),
            tool_name: "read_file".to_string(),
            input: r#"{"path":"retained-read.txt"}"#.to_string(),
            depends_on: Vec::new(),
        });
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
    let replayed = execute_fenced_runtime_tool(
        &host,
        services.commit_service(),
        &replay,
        Some(&effect),
        None,
    )
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

    assert_eq!(nodes.len(), 1);
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

/// Original Host -> PG reservation -> Graph/Goal commit. On the second runtime
/// only the normal startup gate drives the receipt replay; the test never calls
/// a business retry/reconcile method or restarts the APP/model.
#[tokio::test]
async fn authenticated_host_goal_commit_recovers_receipt_in_a_fresh_runtime_without_provider_replay(
) {
    authenticated_host_application_window(HostInputFaultWindow::AfterCommitRestart).await;
}

#[tokio::test]
async fn authenticated_host_cancel_before_goal_commit_settles_only_after_original_binding_exit() {
    authenticated_host_application_window(HostInputFaultWindow::BeforeCommitCancel).await;
}

#[tokio::test]
async fn authenticated_host_cancel_after_goal_commit_keeps_applied_until_original_binding_exit() {
    authenticated_host_application_window(HostInputFaultWindow::AfterCommitCancel).await;
}

#[tokio::test]
async fn authenticated_host_progress_replays_without_goal_mutation_or_a_second_provider() {
    authenticated_host_application_window(HostInputFaultWindow::ProgressRestart).await;
}

#[tokio::test]
async fn authenticated_host_program_commit_replays_only_its_original_attempt_and_graph_cut() {
    authenticated_host_application_window(HostInputFaultWindow::ProgramCommitReplay).await;
}

#[tokio::test]
async fn authenticated_host_program_commit_cold_start_restores_only_the_original_graph_outcome() {
    authenticated_host_application_window(HostInputFaultWindow::ProgramCommitCold).await;
}

#[tokio::test]
async fn authenticated_host_team_zero_to_one_revision_reaches_original_team_create() {
    authenticated_host_application_window(HostInputFaultWindow::TeamRevision).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HostInputFaultWindow {
    AfterCommitRestart,
    BeforeCommitCancel,
    AfterCommitCancel,
    ProgressRestart,
    ProgramCommitReplay,
    ProgramCommitCold,
    TeamRevision,
}

async fn authenticated_host_application_window(window: HostInputFaultWindow) {
    let cancel_window = matches!(
        window,
        HostInputFaultWindow::BeforeCommitCancel | HostInputFaultWindow::AfterCommitCancel
    );
    let progress_input = window == HostInputFaultWindow::ProgressRestart;
    let team_revision = window == HostInputFaultWindow::TeamRevision;
    let cancel_before_commit = window == HostInputFaultWindow::BeforeCommitCancel;
    let program_window = matches!(
        window,
        HostInputFaultWindow::ProgramCommitReplay | HostInputFaultWindow::ProgramCommitCold
    );
    let cold_program = window == HostInputFaultWindow::ProgramCommitCold;
    use crate::execution_core::graph::executors::ScopedNodeBackend;
    use harness_contract::input_disposition::InputApplicationState;
    use harness_contract::turn::{SessionInputUserSource, SESSION_INPUT_USER_SOURCE_KEY};
    use sha2::Digest;
    let root_path = tempfile::tempdir().unwrap().keep();
    eprintln!("user_goal_fixture_root={}", root_path.display());
    let root = root_path.as_path();
    let store = Arc::new(crate::test_support::session_store());
    let session_id = format!("user-goal-host-{}", uuid::Uuid::new_v4());
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.clone(),
            platform: "unit".into(),
            chat_id: session_id.clone(),
            user_id: Some("user-7".into()),
            model: None,
            created_at: "2026-10-26T00:00:00Z".into(),
            last_activity: "2026-10-26T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "never".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .unwrap();
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store.clone());
    let events = Arc::new(crate::RuntimeEventStore::for_test());
    let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
        root.join("artifacts"),
    ));
    let services = crate::RuntimeServices::test_builder(root, root)
        .runtime_event_store(events.clone())
        .artifact_store(artifacts.clone())
        .session_ports(ports.clone(), ports.clone(), ports.clone(), ports.clone())
        .build()
        .unwrap();
    let mut original_owner = None;
    let (graph, ticket) = if cold_program {
        use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
        let mut planned = ExecutionGraph::new("answer directly");
        planned.lineage = Some(ExecutionGraphLineage {
            session_id: session_id.clone(),
            turn_id: "r1-branch-turn".into(),
            root_task_id: "r1-task".into(),
            task_id: "r1-task".into(),
            generation: 1,
        });
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        planned.nodes.push(node.clone());
        let planned = services
            .commit_service()
            .register_graph(planned)
            .unwrap()
            .graph;
        let ready = services
            .commit_service()
            .transition_node(&planned, &node.id, ExecutionNodeStatus::Ready, None, vec![])
            .unwrap()
            .graph;
        let ticket = NodeExecutionTicket {
            graph_id: ready.id.clone(),
            node_id: node.id.clone(),
            executor_kind: node.executor_kind.clone(),
            service_class: ready.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let binding = crate::execution_core::graph::ExecutionNodeBinding {
            executor_kind: ticket.executor_kind.clone(),
            ticket_idempotency_key: ticket.idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        let key =
            crate::ExecutionCommitService::binding_owner_key_for_test(&ready, &node.id, &binding);
        original_owner = Some(
            services
                .scope_locks()
                .acquire_binding_owner(&key, || {
                    services
                        .commit_service()
                        .validate_unadmitted_binding_start(&ready, &node, &ticket)
                        .map_err(|error| error.to_string())
                })
                .unwrap(),
        );
        let running = services
            .commit_service()
            .bind_and_start_nodes(
                &ready,
                std::collections::BTreeMap::from([(node.id, binding)]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        services.record_live_execution(&session_id, running.id.clone(), "r1-branch-turn".into());
        (running, ticket)
    } else if cancel_window {
        use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
        services
            .recover_execution_graphs_on_startup()
            .await
            .unwrap();
        services.release_recovered_producers().await.unwrap();
        let mut graph = ExecutionGraph::new("answer directly");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: session_id.clone(),
            turn_id: "r1-branch-turn".into(),
            root_task_id: "r1-task".into(),
            task_id: "r1-task".into(),
            generation: 1,
        });
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph.nodes.push(node.clone());
        let graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        services.record_live_execution(&session_id, graph.id.clone(), "r1-branch-turn".into());
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id,
            executor_kind: node.executor_kind,
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key,
            payload_ref: node.payload_ref,
        };
        (graph, ticket)
    } else {
        r1_branch_graph(
            &services,
            &session_id,
            vec![],
            ExecutionNodeKind::InlineModel,
        )
    };
    let goal_id = format!("goal:{}", graph.id);
    let goal:GoalContract=serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":session_id,"objective":"Write a report file",
        "criteria":[{"id":"user_intent","statement":"Write a report file","status":"open"}],
        "phase":"execution","spec_revision":1,"spec_digest":"b".repeat(64),"scope":"user_objective","completion":"open","revision":1,"user_sequence":1,
        "user_intent_criterion_id":"user_intent","source_intent_ref":"session_message:host-message",
        "execution_binding":{"objective_id":"host-objective","session_id":session_id,"turn_id":"r1-branch-turn","root_execution_id":graph.id,"agentic_program_id":"host-program"}
    })).unwrap();
    services.goal_store().create(goal).unwrap();
    if program_window {
        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
            TeamCreateInput,
        };
        let actor = AgentActorBinding {
            objective_id: "host-objective".into(),
            program_id: "host-program".into(),
            session_id: session_id.clone(),
            turn_id: "r1-branch-turn".into(),
            root_execution_id: Some(graph.id.clone()),
            required_team_count: 1,
            exact_team_count: None,
            objective_summary: "original Program objective".into(),
            model_lease: "controlled-unit".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec![],
            actor_id: "original-root".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let created = services
            .agent_action_service()
            .apply(&AgentActionEnvelope {
                action_id: "host-original-team".into(),
                actor,
                expected_revision: None,
                action: AgentAction::TeamCreate(TeamCreateInput {
                    name: "original-team".into(),
                    mission: "original bounded work".into(),
                    objective: None,
                }),
            })
            .unwrap();
        assert_eq!(created.status, AgentActionStatus::Applied);
        assert_eq!(
            services
                .agent_action_service()
                .project("host-program")
                .unwrap()
                .revision,
            2,
            "one Program opened record plus the original TeamCreate action"
        );
    }
    let team_action = |id: &str| {
        use harness_contract::agent_action::{
            AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, TeamCreateInput,
        };
        AgentActionEnvelope {
            action_id: id.into(),
            expected_revision: None,
            actor: AgentActorBinding {
                objective_id: "host-objective".into(),
                program_id: "host-program".into(),
                session_id: session_id.clone(),
                turn_id: "r1-branch-turn".into(),
                root_execution_id: Some(graph.id.clone()),
                required_team_count: 0,
                exact_team_count: Some(0),
                objective_summary: "original no-Team objective".into(),
                model_lease: "controlled-unit".into(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: vec![],
                actor_id: "original-root".into(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            action: AgentAction::TeamCreate(TeamCreateInput {
                name: id.into(),
                mission: "explain through one real Team".into(),
                objective: None,
            }),
        }
    };
    if team_revision {
        let denied = services
            .agent_action_service()
            .apply(&team_action("before-revision"))
            .unwrap();
        assert_eq!(
            denied.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(denied.error.unwrap().code, "team_ceiling_reached");
    }
    let text = if team_revision {
        "Use one Team to explain the result directly."
    } else if progress_input {
        "Please continue as planned; this is a progress check."
    } else {
        "Do not generate the report file; explain the result directly."
    };
    let origin = SessionInputUserSource {
        principal_id: "user-7".into(),
        session_id: session_id.clone(),
        input_id: "host-input".into(),
        request_id: "host-request".into(),
        message_id: "host-message".into(),
        session_generation: 1,
        content_sha256: format!("{:x}", sha2::Sha256::digest(text.as_bytes())),
    };
    let now = crate::execution_core::goal::user_application_clock();
    let input = store
        .append_ingress_with_runtime_outbox(
            &session_id,
            "user",
            Some(&serde_json::json!([{"type":"text","text":text}]).to_string()),
            now,
            &session::SessionRuntimeOutboxRequest {
                input_id: "host-input".into(),
                request_id: "host-request".into(),
                turn_id: "r1-branch-turn".into(),
                message_id: "host-message".into(),
                session_generation: 1,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: Some(
                    serde_json::json!({"metadata":{SESSION_INPUT_USER_SOURCE_KEY:origin}})
                        .to_string(),
                ),
                task_route_hint: None,
                created_at_ms: now,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("host-original-owner", now, 600_000, 1, &[])
        .await
        .unwrap()
        .remove(0);
    let running = store
        .mark_session_runtime_outbox_running(
            &input.request_id,
            "host-original-owner",
            1,
            claimed.claim_token.as_ref().unwrap(),
            claimed.revision,
            now,
        )
        .await
        .unwrap();
    struct UserProposalApi(Arc<AtomicUsize>, bool, bool);
    impl ApiClient for UserProposalApi {
        fn stream(
            &mut self,
            _: ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.fetch_add(1, Ordering::SeqCst);
            if self.2 {
                return Box::pin(stream::iter(vec![Ok(AssistantEvent::TextDelta(r#"{"decisions":[{"input_slots":[0],"action":"amend_current_turn","relation":"supplement","objective":"Use one Team to explain the result directly","confidence_basis_points":1,"reason":"The user now requests one Team","business_revision":{"commitments":[{"kind":"team_count","minimum":1,"maximum":1,"source":{"start":0,"end":12}}],"constraints":[],"retired_criteria":[],"retired_obligations":[]}}]}"#.into())),Ok(AssistantEvent::Usage(model_protocol::usage::TokenUsage{input_tokens:31,output_tokens:19,..Default::default()})),Ok(AssistantEvent::MessageStop)]));
            }
            if self.1 {
                return Box::pin(stream::iter(vec![Ok(AssistantEvent::TextDelta(r#"{"decisions":[{"input_slots":[0],"action":"progress_or_control","relation":"progress","objective":"Continue the original Goal unchanged","confidence_basis_points":1,"reason":"The user requested a progress check without amending the business request"}]}"#.into())),Ok(AssistantEvent::Usage(model_protocol::usage::TokenUsage{input_tokens:31,output_tokens:19,..Default::default()})),Ok(AssistantEvent::MessageStop)]));
            }
            Box::pin(stream::iter(vec![Ok(AssistantEvent::TextDelta(r#"{"decisions":[{"input_slots":[0],"action":"amend_current_turn","relation":"supplement","objective":"Explain the result directly without creating the report file","confidence_basis_points":1,"reason":"The user changed the requested deliverable","business_revision":{"commitments":[],"constraints":[],"retired_criteria":[],"retired_obligations":[]}}]}"#.into())),
                Ok(AssistantEvent::Usage(model_protocol::usage::TokenUsage{input_tokens:31,output_tokens:19,..Default::default()})),Ok(AssistantEvent::MessageStop)]))
        }
    }
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let mut session = Session::new();
    session.session_id = session_id.clone();
    let (runtime, mut state, scope) = r1_observation_branch_runtime(
        session,
        UserProposalApi(provider_calls.clone(), progress_input, team_revision),
        &services,
        &graph,
        &goal_id,
    );
    state.content = text.into();
    state.ingress = Some(TurnIngressRef {
        request_id: running.request_id.clone(),
        turn_id: running.turn_id.clone(),
        message_id: running.message_id.clone(),
        session_id: session_id.clone(),
        primary_task_id: "r1-task".into(),
        root_task_id: "r1-task".into(),
        session_generation: 1,
        input_sequence: running.sequence as u64,
        claim_owner: "host-original-owner".into(),
        claim_token: running.claim_token.clone().unwrap(),
        claim_revision: running.claim_fence_epoch.unwrap(),
        handoff_id: None,
    });
    let state = Arc::new(tokio::sync::Mutex::new(state));
    let backend = TurnModelStepBackend {
        ticket: ticket.clone(),
        stop: crate::CancellationToken::new(),
        runtime,
        state: state.clone(),
        services: services.clone(),
    };
    if cancel_window {
        use crate::execution_core::graph::executors::{
            ScopedNodeBackend, ScopedNodeBackendResolver,
        };
        use harness_contract::input_disposition::InputApplicationCancellationState;
        struct HeldHost {
            inner: Arc<dyn ScopedNodeBackend>,
            before_commit: bool,
            ticket: NodeExecutionTicket,
            entered: tokio::sync::Notify,
            stopped: crate::CancellationToken,
            allow_exit: crate::CancellationToken,
            exited: std::sync::atomic::AtomicBool,
        }
        #[async_trait::async_trait]
        impl ScopedNodeBackend for HeldHost {
            fn retains_physical_execution(&self) -> bool {
                true
            }
            fn request_stop(&self, ticket: &NodeExecutionTicket) {
                if ticket == &self.ticket {
                    self.inner.request_stop(ticket);
                    self.stopped.cancel();
                }
            }
            async fn execute(
                &self,
                ticket: &NodeExecutionTicket,
            ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
                let outcome = self.inner.execute(ticket).await?;
                if !self.before_commit {
                    return Ok(outcome);
                }
                self.entered.notify_one();
                self.stopped.cancelled().await;
                self.allow_exit.cancelled().await;
                self.exited.store(true, Ordering::SeqCst);
                Err(NodeExecutorError::Cancel {
                    node_id: ticket.node_id.clone(),
                    reason: "original user cancellation before Goal commit".into(),
                })
            }
            async fn after_commit(
                &self,
                ticket: &NodeExecutionTicket,
            ) -> Result<(), NodeExecutorError> {
                if !self.before_commit {
                    self.entered.notify_one();
                    self.allow_exit.cancelled().await;
                    self.inner.after_commit(ticket).await?;
                    self.exited.store(true, Ordering::SeqCst);
                }
                Ok(())
            }
        }

        struct HostResolver {
            ticket: NodeExecutionTicket,
            backend: std::sync::Weak<HeldHost>,
        }
        impl ScopedNodeBackendResolver for HostResolver {
            fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
                (ticket == &self.ticket)
                    .then(|| self.backend.upgrade())
                    .flatten()
                    .map(|backend| backend as Arc<dyn ScopedNodeBackend>)
            }
        }
        let held = Arc::new(HeldHost {
            inner: Arc::new(backend),
            before_commit: cancel_before_commit,
            ticket: ticket.clone(),
            entered: tokio::sync::Notify::new(),
            stopped: crate::CancellationToken::new(),
            allow_exit: crate::CancellationToken::new(),
            exited: std::sync::atomic::AtomicBool::new(false),
        });
        services
            .model_step_executor()
            .install_resolver(Arc::new(HostResolver {
                ticket: ticket.clone(),
                backend: Arc::downgrade(&held),
            }));
        services
            .execution_supervisor()
            .admit_registered(&graph.id)
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), held.entered.notified())
            .await
            .expect("production Host must prepare before cancellation");
        if !cancel_before_commit {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                loop {
                    let row = store
                        .get_session_runtime_outbox("host-request")
                        .await
                        .unwrap()
                        .unwrap();
                    if row
                        .application_receipt
                        .as_ref()
                        .is_some_and(|receipt| receipt.state == InputApplicationState::Applied)
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("commit-first witness must observe original Applied before user cancellation");
        }
        let reserved = store
            .get_session_runtime_outbox("host-request")
            .await
            .unwrap()
            .unwrap();
        if cancel_before_commit {
            assert_eq!(
                reserved.application_receipt.as_ref().unwrap().state,
                InputApplicationState::Materializing
            );
        } else {
            assert_eq!(
                services
                    .goal_store()
                    .get(&goal_id)
                    .unwrap()
                    .unwrap()
                    .revision,
                2
            );
            assert_eq!(
                reserved.application_receipt.as_ref().unwrap().state,
                InputApplicationState::Applied
            );
        }
        if cancel_before_commit {
            assert!(held.inner.recover_known_outcome(&ticket).await.unwrap().is_none(),
                "only a prepared artifact with no committed business application is not a known cold outcome");
            assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                services
                    .goal_store()
                    .get(&goal_id)
                    .unwrap()
                    .unwrap()
                    .revision,
                1
            );
        }
        let cancelled = store
            .cancel_session_runtime_outbox(
                "host-input",
                1,
                reserved.revision,
                "user-7",
                "withdraw the pending revision",
                crate::execution_core::goal::user_application_clock(),
            )
            .await
            .unwrap();
        assert_eq!(
            cancelled
                .application_receipt
                .as_ref()
                .unwrap()
                .cancellation
                .as_ref()
                .unwrap()
                .state,
            InputApplicationCancellationState::Requested
        );
        let cancel_services = services.clone();
        let graph_id = graph.id.clone();
        let cancelling = tokio::spawn(async move {
            cancel_services
                .cancel_execution_tree(&graph_id, "original user cancellation")
                .await
        });
        if cancel_before_commit {
            tokio::time::timeout(std::time::Duration::from_secs(10), held.stopped.cancelled())
                .await
                .expect("original cancel must reach exact held backend");
        }
        assert!(!held.exited.load(Ordering::SeqCst));
        let before_exit = store
            .get_session_runtime_outbox("host-request")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            before_exit
                .application_receipt
                .as_ref()
                .unwrap()
                .cancellation
                .as_ref()
                .unwrap()
                .state,
            InputApplicationCancellationState::Requested,
            "no physical exit means no settled receipt"
        );
        assert_eq!(
            services
                .goal_store()
                .get(&goal_id)
                .unwrap()
                .unwrap()
                .revision,
            if cancel_before_commit { 1 } else { 2 }
        );
        let not_closed = services.graph_state_store().load(&graph.id).unwrap();
        let (_, starts) = services
            .graph_state_store()
            .binding_starts_page(&not_closed.id, 0, not_closed.revision)
            .unwrap();
        let (node, original, start) = starts
            .into_iter()
            .find(|(node, _, _)| node == &ticket.node_id)
            .expect("real original binding start");
        assert!(
            !services
                .graph_state_store()
                .binding_lease_closure(&not_closed, &node, &original, start)
                .unwrap(),
            "blocked physical callback cannot certify its own closure"
        );
        held.allow_exit.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(10), cancelling)
            .await
            .expect("bounded original cancel")
            .unwrap()
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let current = store
                    .get_session_runtime_outbox("host-request")
                    .await
                    .unwrap()
                    .unwrap();
                if current
                    .application_receipt
                    .as_ref()
                    .unwrap()
                    .cancellation
                    .as_ref()
                    .unwrap()
                    .state
                    == InputApplicationCancellationState::Settled
                {
                    assert_eq!(
                        current.application_receipt.as_ref().unwrap().state,
                        if cancel_before_commit {
                            InputApplicationState::Failed
                        } else {
                            InputApplicationState::Applied
                        }
                    );
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("original automatic lane must settle after exact physical closure");
        assert!(held.exited.load(Ordering::SeqCst));
        assert_eq!(
            provider_calls.load(Ordering::SeqCst),
            1,
            "cancel recovery must not reinterpret user input"
        );
        let closed = services.graph_state_store().load(&graph.id).unwrap();
        let mut after = 0;
        let mut matched = 0;
        while after < closed.revision {
            let (next, starts) = services
                .graph_state_store()
                .binding_starts_page(&closed.id, after, closed.revision)
                .unwrap();
            assert!(next > after);
            for (node, binding, start) in starts {
                if node == ticket.node_id && binding.attempt == ticket.attempt {
                    assert!(services
                        .graph_state_store()
                        .binding_lease_closure(&closed, &node, &binding, start)
                        .unwrap());
                    matched += 1;
                }
            }
            after = next;
        }
        assert_eq!(matched, 1, "the original binding must really close once");
        assert_eq!(
            services
                .goal_store()
                .get(&goal_id)
                .unwrap()
                .unwrap()
                .revision,
            if cancel_before_commit { 1 } else { 2 },
            "cancellation cannot erase an already committed Goal revision"
        );
        let weak = Arc::downgrade(&services);
        drop(held);
        drop(state);
        drop(scope);
        services.shutdown_maintenance().await;
        assert_eq!(services.shutdown_execution().await.forced_aborts, 0);
        drop(services);
        assert!(
            weak.upgrade().is_none(),
            "test resolver must not retain a second Runtime owner after physical cleanup"
        );
        std::fs::remove_dir_all(&root_path).unwrap();
        return;
    }
    let outcome = backend.execute(&ticket).await.unwrap();
    assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    assert_eq!(outcome.result.usage.input_tokens, 31);
    assert_eq!(outcome.result.usage.output_tokens, 19);
    assert!(!outcome
        .result
        .result_ref
        .as_deref()
        .unwrap()
        .starts_with("assistant_json:"));
    assert!(
        state.lock().await.assistant_messages.is_empty(),
        "internal JSON cannot publish an assistant answer"
    );
    let reserved = store
        .get_session_runtime_outbox("host-request")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        reserved.application_receipt.as_ref().unwrap().state,
        InputApplicationState::Materializing
    );
    assert_eq!(reserved.status, session::SessionRuntimeInputStatus::Running);
    assert_eq!(
        services
            .goal_store()
            .get(&goal_id)
            .unwrap()
            .unwrap()
            .objective,
        if program_window {
            "Explain the result directly without creating the report file"
        } else {
            "Write a report file"
        }
    );
    let frozen = crate::execution_core::goal::read_user_goal_application(
        services.as_ref(),
        reserved.application_receipt.as_ref().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        Some(&frozen.continuation),
        outcome.replan.as_ref(),
        "the artifact must freeze the complete original continuation before reservation"
    );
    // Each negative starts from the independently successful original carrier.
    let mut wrong_digest = reserved.application_receipt.as_ref().unwrap().clone();
    wrong_digest
        .prepared_application
        .as_mut()
        .unwrap()
        .artifact_sha256 = "0".repeat(64);
    let denied =
        crate::execution_core::goal::read_user_goal_application(services.as_ref(), &wrong_digest)
            .await
            .err()
            .expect("tampered digest cannot authorize recovery");
    assert_eq!(denied, "user application artifact identity/budget mismatch");
    let mut foreign = reserved.application_receipt.as_ref().unwrap().clone();
    foreign.prepared_application.as_mut().unwrap().turn_id = "foreign-turn".into();
    let denied =
        crate::execution_core::goal::read_user_goal_application(services.as_ref(), &foreign)
            .await
            .err()
            .expect("foreign turn cannot borrow the original artifact");
    assert_eq!(
        denied,
        "user application prepared content crosses source/attempt"
    );

    // Legacy/malformed artifacts cannot invent a new continuation on recovery.
    let mut incomplete = serde_json::to_value(&frozen).unwrap();
    incomplete.as_object_mut().unwrap().remove("continuation");
    assert!(
        serde_json::from_value::<crate::execution_core::goal::PreparedUserGoalApplication>(
            incomplete
        )
        .is_err()
    );
    if window == HostInputFaultWindow::ProgramCommitReplay {
        assert_eq!(
            services
                .goal_store()
                .get(&goal_id)
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        assert!(
            services
                .graph_state_store()
                .load(&graph.id)
                .unwrap()
                .node_results
                .get(&ticket.node_id)
                .is_none(),
            "the original Program/Goal transaction does not imply its Graph outcome committed"
        );
        let mut foreign = ticket.clone();
        foreign.attempt += 1;
        let rejected = backend.execute(&foreign).await;
        assert!(
            matches!(rejected,Err(NodeExecutorError::Recover{reason,..}) if reason=="prepared user input cannot borrow another original attempt")
        );
        let replay = backend
            .execute(&ticket)
            .await
            .expect("original Applied source remains visible before root ACK");
        assert_eq!(replay.replan, outcome.replan);
        assert_eq!(replay.result, outcome.result);
        assert_eq!(
            replay.source_revisions.get(&graph.id),
            Some(&graph.revision)
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            services
                .goal_store()
                .get(&goal_id)
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        // A genuine concurrent Graph change must still reject the original
        // replay, even though the user's Goal fact is already committed.
        let current = services.graph_state_store().load(&graph.id).unwrap();
        let moved = services
            .commit_service()
            .replan(
                &current,
                vec![ExecutionNodeSpec::new(
                    ExecutionNodeKind::InlineModel,
                    "inline_model",
                    "concurrent required work",
                )],
                vec![],
                "concurrent graph change".into(),
            )
            .unwrap()
            .graph;
        let rejected = services
            .commit_service()
            .execution_outcome_sources(&moved, &ticket, &replay)
            .unwrap_err();
        assert!(
            matches!(rejected,crate::execution_core::graph::ExecutionCommitError::InvalidCommand(reason) if reason=="outcome Graph source conflicts with original Graph CAS")
        );
        assert_eq!(
            services
                .goal_store()
                .get(&goal_id)
                .unwrap()
                .unwrap()
                .revision,
            2
        );
        drop(backend);
        drop(state);
        drop(scope);
        services.shutdown_maintenance().await;
        assert_eq!(services.shutdown_execution().await.forced_aborts, 0);
        drop(services);
        std::fs::remove_dir_all(&root_path).unwrap();
        return;
    }
    if cold_program {
        let current = services.graph_state_store().load(&graph.id).unwrap();
        let binding = services
            .graph_state_store()
            .current_binding(&current, &ticket.node_id, ticket.attempt)
            .unwrap();
        let key = crate::ExecutionCommitService::binding_owner_key_for_test(
            &current,
            &ticket.node_id,
            &binding,
        );
        assert!(
            services
                .scope_locks()
                .try_recover_binding_owner(&key)
                .unwrap()
                .is_none(),
            "a live old owner cannot be replaced even with an Applied Goal fact"
        );
        let known = backend
            .recover_known_outcome(&ticket)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(known.result, outcome.result);
        assert_eq!(known.replan, outcome.replan);
        assert_eq!(
            store
                .get_session_runtime_outbox("host-request")
                .await
                .unwrap()
                .unwrap()
                .application_receipt
                .unwrap()
                .state,
            InputApplicationState::Materializing,
            "known-only query must not settle the original receipt before its Graph callback"
        );
    }
    if !cold_program {
        let sources = services
            .commit_service()
            .execution_outcome_sources(&graph, &ticket, &outcome)
            .unwrap();
        services
            .commit_service()
            .transition_execution_outcome(&graph, &ticket, &outcome, &sources)
            .unwrap();
    } else {
        assert!(services
            .graph_state_store()
            .load(&graph.id)
            .unwrap()
            .node_results
            .get(&ticket.node_id)
            .is_none());
        assert!(matches!(services.commit_service().inspect_execution_effect(&ticket).unwrap(),crate::execution_core::graph::ExecutionEffectState::Uncertain),
            "Goal commit is not an effect receipt; only the later original atomic Graph commit may publish that receipt");
    }
    assert_eq!(
        services
            .goal_store()
            .get(&goal_id)
            .unwrap()
            .unwrap()
            .objective,
        if team_revision {
            "Use one Team to explain the result directly"
        } else if progress_input {
            "Write a report file"
        } else {
            "Explain the result directly without creating the report file"
        }
    );
    assert_eq!(
        store
            .get_session_runtime_outbox("host-request")
            .await
            .unwrap()
            .unwrap()
            .application_receipt
            .unwrap()
            .state,
        InputApplicationState::Materializing,
        "fault window: Goal committed, original Session Applied result still missing"
    );
    if team_revision {
        let current = services.goal_store().get(&goal_id).unwrap().unwrap();
        assert_eq!(
            current.input_contract.as_ref().unwrap().team_bounds(),
            Some((1, Some(1)))
        );
        let mut foreign = team_action("foreign-session");
        foreign.actor.session_id = "foreign-session".into();
        let denied = services.agent_action_service().apply(&foreign).unwrap();
        assert_eq!(denied.error.unwrap().code, "current_goal_binding_mismatch");
        assert!(services
            .agent_action_service()
            .project_if_exists("host-program")
            .unwrap()
            .is_none());
        let writer = services.clone();
        let changed_goal = goal_id.clone();
        let hit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = hit.clone();
        let rejected = crate::AgentActionService::with_team_quantity_commit_hook_for_test(
            move |_| {
                observed.store(true, Ordering::SeqCst);
                let (_, _, event) = writer
                    .goal_store()
                    .revision_event(
                        &changed_goal,
                        2,
                        3,
                        "concurrent original Goal revision",
                        |goal| {
                            goal.constraints.push("a concurrent user constraint".into());
                            vec!["constraints".into()]
                        },
                    )
                    .unwrap();
                // Independent storage-writer interleaving: use the original
                // Goal producer and backend transaction, bypassing only this
                // process's already-held ordered source lock (not its CAS).
                writer
                    .event_store()
                    .inject_content_transaction_fault(crate::AppendTransactionRequest {
                        transaction_id: "team-quantity-concurrent-goal".into(),
                        expected_streams: vec![crate::ExpectedStreamRevision {
                            stream_id: event.event.stream_id.clone(),
                            expected_revision: 2,
                        }],
                        events: vec![event],
                    })
                    .unwrap();
            },
            || {
                services
                    .agent_action_service()
                    .apply(&team_action("stale-goal"))
            },
        );
        assert!(
            hit.load(Ordering::SeqCst),
            "real TeamCreate must reach its actual transaction boundary"
        );
        assert!(
            matches!(rejected,Err(crate::agentic::AgentActionServiceError::Store(crate::RuntimeEventStoreError::StaleRevision{ref stream_id,expected:2,actual:3})) if stream_id==&format!("goal:{goal_id}")),
            "{rejected:?}"
        );
        assert!(
            services
                .agent_action_service()
                .project_if_exists("host-program")
                .unwrap()
                .is_none(),
            "Goal race cannot partially create the Program/Team"
        );
        let mut ungranted = team_action("ungranted-agent");
        ungranted.actor.kind = harness_contract::agent_action::AgentActorKind::Agent;
        ungranted.actor.agent_id = Some("ungranted-agent".into());
        ungranted.actor.team_id = Some("ungranted-team".into());
        ungranted.actor.execution_id = Some("ungranted-execution".into());
        let denied = services.agent_action_service().apply(&ungranted).unwrap();
        assert_eq!(denied.error.unwrap().code, "team_create_not_delegated");
        let allowed = services
            .agent_action_service()
            .apply(&team_action("after-revision"))
            .unwrap();
        assert_eq!(allowed.status,harness_contract::agent_action::AgentActionStatus::Applied,
            "the same actor/grant must consume the current authenticated Team1 business constraint: {:?}",allowed.error);
        let replay = services
            .agent_action_service()
            .apply(&team_action("after-revision"))
            .unwrap();
        assert_eq!(
            replay.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let program = services
            .agent_action_service()
            .project("host-program")
            .unwrap();
        assert_eq!(program.teams.len(), 1);
        assert_eq!(
            program.required_team_count, 0,
            "frozen actor metadata is not rewritten"
        );
        assert_eq!(program.exact_team_count, Some(0));
        let current = services.goal_store().get(&goal_id).unwrap().unwrap();
        assert_eq!(
            current_turn_required_team_execution_count(&current, None, false, false),
            1
        );
        assert_eq!(
            current_turn_required_team_execution_count(&current, None, false, true),
            0
        );
        assert_eq!(
            current_turn_required_team_execution_count(&current, None, true, false),
            0
        );
        let view = crate::execution_core::goal::assessment::read_bound_goal_assessment(
            services.event_store(),
            &program,
        )
        .unwrap()
        .view(&program);
        assert_eq!(view["facts"]["required_teams"], 1);
        assert_eq!(view["team_quantity"]["minimum"], 1);
        assert_eq!(view["team_quantity"]["maximum"], 1);
        assert_eq!(view["team_quantity"]["exact"], 1);
        let mut obsolete = services.goal_store().projection(&goal_id).unwrap().unwrap();
        obsolete.goal.input_contract.as_mut().unwrap().commitments =
            vec![harness_contract::goal::GoalInputCommitment::TeamCount {
                minimum: 0,
                maximum: Some(0),
                source: harness_contract::goal::GoalInputSpan { start: 0, end: 12 },
            }];
        assert!(
            crate::execution_core::goal::assessment::assess_bound_goal_projection(
                &program,
                Some(&obsolete)
            )
            .is_err(),
            "actual Teams above the current ceiling must still fail closed"
        );
        assert!(replay.duplicate);
        let too_many = services
            .agent_action_service()
            .apply(&team_action("second-team"))
            .unwrap();
        assert_eq!(too_many.error.unwrap().code, "team_ceiling_reached");
        assert_eq!(
            services
                .agent_action_service()
                .project("host-program")
                .unwrap()
                .teams
                .len(),
            1
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
        drop(backend);
        drop(state);
        drop(scope);
        services.shutdown_maintenance().await;
        assert_eq!(services.shutdown_execution().await.forced_aborts, 0);
        drop(services);
        std::fs::remove_dir_all(&root_path).unwrap();
        return;
    }
    // Simulated process boundary: preserve only the original durable ports and
    // artifact owner; drop every old Runtime/Host/backend and prove it by Weak.
    let weak = Arc::downgrade(&services);
    drop(backend);
    drop(state);
    drop(scope);
    services.shutdown_maintenance().await;
    let shutdown = services.shutdown_execution().await;
    assert_eq!(shutdown.forced_aborts, 0);
    drop(services);
    assert!(
        weak.upgrade().is_none(),
        "old Runtime must not do recovery work"
    );
    drop(original_owner); // Old physical Host future has returned; release only its real owner lock.

    let fresh_ports = crate::session_runtime_port::TestSessionPortAdapter::new(store.clone());
    let fresh = crate::RuntimeServices::test_builder(root, root)
        .runtime_event_store(events)
        .artifact_store(artifacts)
        .session_ports(
            fresh_ports.clone(),
            fresh_ports.clone(),
            fresh_ports.clone(),
            fresh_ports,
        )
        .build()
        .unwrap();
    let rebound = if cold_program {
        let mut session = crate::session::Session::new();
        session.session_id = session_id.clone();
        let current = fresh.graph_state_store().load(&graph.id).unwrap();
        fresh.record_live_execution(&session_id, current.id.clone(), "r1-branch-turn".into());
        let (runtime, state, scope) = r1_observation_branch_runtime(
            session,
            UserProposalApi(provider_calls.clone(), false, false),
            &fresh,
            &current,
            &goal_id,
        );
        let backend = Arc::new(TurnModelStepBackend {
            runtime,
            state: Arc::new(tokio::sync::Mutex::new(state)),
            services: fresh.clone(),
            stop: crate::CancellationToken::new(),
            ticket: ticket.clone(),
        });
        struct ColdHostResolver<C: ApiClient + Send + Sync + 'static, T: ToolExecutor> {
            ticket: NodeExecutionTicket,
            backend: std::sync::Weak<TurnModelStepBackend<C, T>>,
        }
        impl<C: ApiClient + Send + Sync + 'static, T: ToolExecutor + 'static>
            crate::execution_core::graph::executors::ScopedNodeBackendResolver
            for ColdHostResolver<C, T>
        {
            fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn ScopedNodeBackend>> {
                (ticket == &self.ticket)
                    .then(|| self.backend.upgrade())
                    .flatten()
                    .map(|backend| backend as Arc<dyn ScopedNodeBackend>)
            }
        }
        fresh
            .model_step_executor()
            .install_resolver(Arc::new(ColdHostResolver {
                ticket: ticket.clone(),
                backend: Arc::downgrade(&backend),
            }));
        Some((backend, scope))
    } else {
        None
    };
    fresh.recover_execution_graphs_on_startup().await.unwrap();
    fresh.release_recovered_producers().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let current = store
                .get_session_runtime_outbox("host-request")
                .await
                .unwrap()
                .unwrap();
            if current
                .application_receipt
                .as_ref()
                .is_some_and(|r| r.state == InputApplicationState::Applied)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original automatic settled lane must replay the exact committed receipt");
    assert_eq!(
        provider_calls.load(Ordering::SeqCst),
        1,
        "no second interpretation/provider or APP start after rebuilding Runtime"
    );
    assert_eq!(
        fresh.goal_store().get(&goal_id).unwrap().unwrap().revision,
        if progress_input { 1 } else { 2 },
        "replay cannot repeat or manufacture Goal adoption"
    );
    if cold_program {
        tokio::time::timeout(std::time::Duration::from_secs(10),async {
            loop {
                let restored=fresh.graph_state_store().load(&graph.id).unwrap();
                if let Some(result)=restored.node_results.get(&ticket.node_id) {
                    let (_,starts)=fresh.graph_state_store().binding_starts_page(&graph.id,0,restored.revision).unwrap();
                    if let Some((node,binding,start))=starts.into_iter().find(|(node,binding,_)|node==&ticket.node_id && binding.attempt==ticket.attempt) {
                        if fresh.graph_state_store().binding_lease_closure(&restored,&node,&binding,start).unwrap() {
                            assert_eq!(result,&outcome.result);
                            assert_eq!(restored.nodes.iter().filter(|node|frozen.continuation.nodes.iter().any(|expected|expected==*node)).count(),frozen.continuation.nodes.len());
                            assert!(matches!(fresh.commit_service().inspect_execution_effect(&ticket).unwrap(),crate::execution_core::graph::ExecutionEffectState::Completed(_)));
                            break;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        }).await.expect("only the original startup/recovery owner may restore Graph/effect then physical closure");
        assert_eq!(
            fresh.goal_store().get(&goal_id).unwrap().unwrap().revision,
            2
        );
        let original_program = fresh
            .agent_action_service()
            .project("host-program")
            .unwrap();
        assert_eq!(
            original_program.revision, 3,
            "one Program opened record, one TeamCreate and one user revision only"
        );
        assert_eq!(
            original_program.objective_summary,
            "original Program objective"
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 1);
    }
    drop(rebound);
    fresh.shutdown_maintenance().await;
    assert_eq!(fresh.shutdown_execution().await.forced_aborts, 0);
    drop(fresh);
    std::fs::remove_dir_all(&root_path).unwrap();
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
