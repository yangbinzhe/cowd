use super::*;

mod pre_effect_admission_555 {
    use super::*;

    struct PinnedTools555 {
        lease: tools::ToolHostLease,
        root_closed: bool,
        preflight: Option<(
            crate::execution_core::graph::ExecutionCommitService,
            harness_contract::execution_graph::ExecutionParentBinding,
            u32,
        )>,
    }

    #[async_trait::async_trait]
    impl ToolExecutor for PinnedTools555 {
        fn available_tool_names(&self) -> Vec<String> {
            let mut names = vec!["workspace_snapshot".into(), "read_file".into()];
            if self.preflight.is_some() || self.root_closed {
                names.push("edit".into());
            }
            names
        }

        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            Some(self.lease.clone())
        }

        fn registered_tool_effect(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.lease.describe_effect(name, input))
        }

        fn owns_durable_tool_effect(&self, _name: &str) -> bool {
            self.preflight.is_some()
        }

        fn owns_tool_resource_admission(&self, _name: &str) -> bool {
            self.preflight.is_some()
        }

        async fn execute_step_authorized_invocation_output(
            &self,
            binding: Option<&crate::step_snapshot::StepCallBinding>,
            invocation_id: &str,
            authorization: &harness_contract::tool::ToolExecutionAuthorization,
            name: &str,
            input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            let (commit, parent, attempt) = self
                .preflight
                .as_ref()
                .expect("580 original legacy preflight");
            let binding = binding.unwrap();
            let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: invocation_id.into(),
                    tool_name: name.into(),
                    input: input.into(),
                    depends_on: Vec::new(),
                },
            );
            request.session_id = Some(binding.snapshot.identity.session_id.clone());
            request.idempotency_key = binding.effect_idempotency_key();
            request.policy_revision = authorization.policy_revision;
            request.authorization = Some(authorization.clone());
            request.step_binding = Some(binding.clone());
            request.parent_execution = Some(parent.clone());
            request.parent_execution_attempt = Some(*attempt);
            request.compiled_invocation = Some(
                binding
                    .prepare_invocation(
                        Some(self.lease.clone()),
                        name,
                        &serde_json::from_str(input).unwrap(),
                    )
                    .unwrap(),
            );
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let error = commit
                .preflight_fresh_bound_tool(&request, effect)
                .expect_err("known denied original parent cannot admit a fresh write");
            assert!(error
                .pre_effect_refusal_for(invocation_id, name, input, Some(binding))
                .is_some());
            assert!(crate::ToolError::new(error.to_string())
                .pre_effect_refusal_for(invocation_id, name, input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for("foreign-call", name, input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for(invocation_id, "read_file", input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for(invocation_id, name, "{}", Some(binding))
                .is_none());
            let mut foreign = binding.clone();
            foreign.origin.origin_step_id.push_str(":foreign");
            assert!(error
                .pre_effect_refusal_for(invocation_id, name, input, Some(&foreign))
                .is_none());
            for mutation in ["turn", "execution", "policy"] {
                let mut foreign = binding.clone();
                let snapshot = Arc::make_mut(&mut foreign.snapshot);
                match mutation {
                    "turn" => snapshot.identity.turn_id.push_str(":foreign"),
                    "execution" => snapshot.identity.execution_id = Some("foreign".into()),
                    _ => {
                        snapshot
                            .related_revisions
                            .insert("policy".into(), "foreign".into());
                    }
                }
                assert_eq!(foreign.origin, binding.origin);
                assert_eq!(
                    foreign.effect_idempotency_key(),
                    binding.effect_idempotency_key()
                );
                assert!(
                    error
                        .pre_effect_refusal_for(invocation_id, name, input, Some(&foreign))
                        .is_none(),
                    "{mutation}"
                );
            }
            let mut unknown_source = request.clone();
            unknown_source
                .parent_execution
                .as_mut()
                .unwrap()
                .execution_id
                .push_str(":missing");
            let unknown_error = commit
                .preflight_fresh_bound_tool(&unknown_source, effect)
                .unwrap_err();
            assert!(unknown_error
                .pre_effect_refusal_for(invocation_id, name, input, Some(binding))
                .is_none());
            Err(error)
        }

        async fn execute_output(
            &self,
            _name: &str,
            _input: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
            Err(crate::ToolError::new("555 forbids unbound tool execution"))
        }
    }

    struct NativeHost555 {
        host: tools::ToolHost,
        entered: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for NativeHost555 {
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
            self.entered.fetch_add(1, Ordering::SeqCst);
            let step = request
                .step_binding
                .as_ref()
                .unwrap()
                .resolve(Some(self.host.pin_snapshot()))
                .unwrap();
            let input = serde_json::from_str(&request.input).unwrap();
            let result = step
                .lease()
                .execute_async(
                    request.authorization.as_ref().unwrap(),
                    &request.tool_name,
                    &input,
                )
                .await;
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

    #[derive(Clone, Copy, Debug)]
    enum Case555 {
        AbsoluteRoot,
        RelativeRoot,
        OutsideRoot,
        BodyFailure,
        BodyFailureBlockedWrite,
        ScopeRefusalBlockedWrite,
        BodyFailureBlockedChain,
        Mixed,
        MissingSessionJournal,
        NativePreflightRefusal,
        AuthorizationGap,
        MixedAuthorizationGap,
        RootGoalClosed,
    }

    async fn original_host_chain555(case: Case555) {
        original_host_chain733(case, false).await;
    }

    async fn original_host_chain733(case: Case555, blocked_native: bool) {
        let fixture = PublicationFixture::new();
        if matches!(case, Case555::NativePreflightRefusal) {
            fixture
                .services
                .commit_service()
                .transition_node(
                    &fixture.graph,
                    &fixture.ticket.node_id,
                    ExecutionNodeStatus::Cancelled,
                    None,
                    Vec::new(),
                )
                .unwrap();
        }
        std::fs::write(fixture.root.join("proof.txt"), "original evidence\n").unwrap();
        std::fs::write(fixture.root.join(".env"), "fixture_secret=not_real\n").unwrap();
        let root = std::fs::canonicalize(&fixture.root).unwrap();
        let host = Arc::new(NativeHost555 {
            host: tools::ToolHost::builtin("pre-effect-555", &root)
                .with_authorization_lease_verifier(Arc::new(
                    crate::AuthorizationNegotiator::verify_lease_signature,
                )),
            entered: AtomicUsize::new(0),
        });
        let mut builder = crate::RuntimeServices::builder(root.join("home"), &root)
            .runtime_event_store(Arc::new(crate::RuntimeEventStore::from_backend(
                fixture.backend.clone(),
            )))
            .artifact_store(fixture.artifacts.clone())
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()));
        if !matches!(case, Case555::NativePreflightRefusal) {
            builder = builder.tool_execution_host(host.clone());
        }
        let services = builder.build().unwrap();
        let provider_calls = Arc::new(AtomicUsize::new(0));
        let policy = PermissionPolicy::new(
            if matches!(
                case,
                Case555::NativePreflightRefusal
                    | Case555::BodyFailureBlockedWrite
                    | Case555::ScopeRefusalBlockedWrite
                    | Case555::BodyFailureBlockedChain
                    | Case555::RootGoalClosed
            ) {
                harness_contract::policy::PermissionMode::WorkspaceWrite
            } else {
                harness_contract::policy::PermissionMode::ReadOnly
            },
        );
        let revision = policy.execution_policy_control().snapshot().revision;
        let session_store = Arc::new(crate::test_support::session_store());
        session_store
            .create_session(&session::SessionRecord {
                session_id: fixture.session.session_id.clone(),
                platform: "test".into(),
                chat_id: fixture.session.session_id.clone(),
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
        let mut runtime = crate::ConversationRuntime::new(
            fixture.session.clone(),
            S01Client518(provider_calls.clone()),
            PinnedTools555 {
                lease: host.host.pin_snapshot(),
                root_closed: matches!(case, Case555::RootGoalClosed),
                preflight: matches!(case, Case555::NativePreflightRefusal).then(|| {
                    (
                        services.commit_service().clone(),
                        harness_contract::execution_graph::ExecutionParentBinding {
                            execution_id: fixture.ticket.graph_id.clone(),
                            node_id: fixture.ticket.node_id.clone(),
                        },
                        fixture.ticket.attempt,
                    )
                }),
            },
            policy,
            vec!["Read workspace files and report the observed result.".into()],
        )
        .without_memory()
        .with_runtime_event_store(services.event_store().clone())
        .with_artifact_store(fixture.artifacts.clone())
        .with_cowd_event_bus(crate::CowdEventBus::new());
        if !matches!(case, Case555::MissingSessionJournal) {
            runtime = runtime.with_session_journal_port(
                crate::session_runtime_port::TestSessionPortAdapter::new(session_store),
            );
        }
        let mut graph = ExecutionGraph::new("Read workspace files and report the observed result.");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: fixture.session.session_id.clone(),
            turn_id: "471-turn".into(),
            root_task_id: "471-task".into(),
            task_id: "471-task".into(),
            generation: 1,
        });
        runtime
            .begin_turn_strategy(
                "471-turn",
                "Read workspace files and report the observed result.",
            )
            .unwrap();
        let strategy = runtime
            .bind_turn_strategy_execution("471-turn", &graph.id)
            .unwrap();
        let _root_scope = if matches!(case, Case555::RootGoalClosed) {
            let objective = harness_contract::agent_action::root_objective_id(
                &fixture.session.session_id,
                "471-turn",
            );
            let goal: GoalContract = serde_json::from_value(serde_json::json!({
                "id":format!("goal:{}",graph.id),"session_id":fixture.session.session_id,
                "objective":"closed root must still publish a refusal transcript",
                "criteria":[{"id":"checked","statement":"original work","status":"open"}],
                "phase":"execution","spec_revision":1,"spec_digest":"root-closed-original",
                "scope":"internal","completion":"open","revision":1,"user_sequence":1,
                "execution_binding":{"objective_id":objective,"session_id":fixture.session.session_id,
                    "turn_id":"471-turn","root_execution_id":graph.id,
                    "agentic_program_id":harness_contract::agent_action::program_id_for_objective(&objective)}
            })).unwrap();
            let goal = services.goal_store().create(goal).unwrap();
            services
                .goal_store()
                .complete(
                    &goal.id,
                    goal.revision,
                    GoalCompletion::Cancelled,
                    "original owner closed the Goal",
                )
                .unwrap();
            Some(
                runtime
                    .cowd_bus()
                    .unwrap()
                    .enter_execution(crate::CowdExecutionContext {
                        execution_id: graph.id.clone(),
                        session_id: fixture.session.session_id.clone(),
                        turn_id: "471-turn".into(),
                    }),
            )
        } else {
            None
        };
        let lease = host.host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| {
                matches!(definition.name.as_str(), "workspace_snapshot" | "read_file")
                    || (matches!(
                        case,
                        Case555::NativePreflightRefusal
                            | Case555::BodyFailureBlockedWrite
                            | Case555::ScopeRefusalBlockedWrite
                            | Case555::BodyFailureBlockedChain
                            | Case555::RootGoalClosed
                    ) && definition.name == "edit")
            })
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            advertised.len(),
            if matches!(
                case,
                Case555::NativePreflightRefusal
                    | Case555::BodyFailureBlockedWrite
                    | Case555::ScopeRefusalBlockedWrite
                    | Case555::BodyFailureBlockedChain
                    | Case555::RootGoalClosed
            ) {
                3
            } else {
                2
            }
        );
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                serde_json::from_value(serde_json::json!({
                    "step_id":format!("555-step:{}",graph.id),
                    "session_id":fixture.session.session_id,
                    "turn_id":"471-turn","execution_id":graph.id,"build_ref":"isolated-fixture"
                }))
                .unwrap(),
                lease,
                &advertised,
                &serde_json::json!({}),
                &serde_json::json!({}),
                BTreeMap::from([("policy".into(), revision.to_string())]),
                chrono::Utc::now().timestamp_millis() + 60_000,
            )
            .unwrap(),
        );
        let make_call = |id: &str, name: &str, input: serde_json::Value| ModelToolCall {
            id: id.into(),
            name: name.into(),
            input: input.to_string(),
            depends_on: Vec::new(),
            binding: Some(crate::step_snapshot::StepCallBinding::provider(
                step.clone(),
                id,
            )),
        };
        let rejected_scope = root.join("escape-link");
        if matches!(case, Case555::OutsideRoot | Case555::Mixed | Case555::ScopeRefusalBlockedWrite) {
            std::os::unix::fs::symlink(root.parent().unwrap(), &rejected_scope).unwrap();
        }
        let mut calls = match case {
            Case555::BodyFailureBlockedWrite | Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain => {
                let first = if matches!(case, Case555::ScopeRefusalBlockedWrite) {
                    make_call("555-denied", "workspace_snapshot", serde_json::json!({"roots":[rejected_scope],"max_files":20,"page_size":10}))
                } else {
                    make_call("555-missing", "read_file", serde_json::json!({"path":"missing.txt","complete":true}))
                };
                let mut blocked = make_call("733-blocked", "edit", serde_json::json!({"action":"write","path":"denied.txt","content":"must never execute"}));
                blocked.depends_on.push(first.id.clone());
                let mut planned = vec![first, blocked];
                if matches!(case, Case555::BodyFailureBlockedChain) {
                    let mut transitive = make_call("733-transitive", "edit", serde_json::json!({"action":"write","path":"transitive.txt","content":"must never execute"}));
                    transitive.depends_on.push("733-blocked".into());
                    planned.push(transitive);
                }
                planned
            }
            Case555::RootGoalClosed => vec![make_call(
                "555-denied",
                "edit",
                serde_json::json!({"action":"write","path":"denied.txt","content":"must never execute"}),
            )],
            Case555::AbsoluteRoot => vec![make_call(
                "555-root",
                "workspace_snapshot",
                serde_json::json!({"roots":[root],"max_files":20,"page_size":10}),
            )],
            Case555::RelativeRoot => vec![make_call(
                "555-root",
                "workspace_snapshot",
                serde_json::json!({"roots":["."],"max_files":20,"page_size":10}),
            )],
            Case555::OutsideRoot => vec![make_call(
                "555-denied",
                "workspace_snapshot",
                serde_json::json!({"roots":[rejected_scope],"max_files":20,"page_size":10}),
            )],
            Case555::BodyFailure => vec![make_call(
                "555-missing",
                "read_file",
                serde_json::json!({"path":"missing.txt","complete":true}),
            )],
            Case555::AuthorizationGap => vec![make_call(
                "555-denied",
                "read_file",
                serde_json::json!({"path":".env","complete":true}),
            )],
            Case555::MixedAuthorizationGap => vec![
                make_call("555-denied", "read_file", serde_json::json!({"path":".env","complete":true})),
                make_call("555-read", "read_file", serde_json::json!({"path":"proof.txt","complete":true})),
            ],
            Case555::MissingSessionJournal => vec![make_call(
                "555-read",
                "read_file",
                serde_json::json!({"path":"proof.txt","complete":true}),
            )],
            Case555::NativePreflightRefusal => vec![make_call(
                "580-preflight-denied",
                "edit",
                serde_json::json!({"action":"write","path":"denied.txt","content":"must never execute"}),
            )],
            Case555::Mixed => vec![
                make_call(
                    "555-denied",
                    "workspace_snapshot",
                    serde_json::json!({"roots":[rejected_scope],"max_files":20,"page_size":10}),
                ),
                make_call(
                    "555-read",
                    "read_file",
                    serde_json::json!({"path":"proof.txt","complete":true}),
                ),
            ],
        };
        if blocked_native {
            assert!(matches!(case, Case555::NativePreflightRefusal));
            let mut blocked = make_call("733-blocked", "edit", serde_json::json!({"action":"write","path":"denied.txt","content":"must never execute"}));
            blocked.depends_on.push(calls[0].id.clone());
            calls.push(blocked);
        }
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            encode_tool_calls_with_continuation(&fixture.session.session_id, &calls, false)
                .unwrap(),
        );
        node.id = format!("{}:original-tools", graph.id);
        node.idempotency_key = format!("{}:original", node.id);
        graph.nodes.push(node.clone());
        let mut state = publication_state(&fixture.session.session_id, &fixture.goal_id);
        state.content = "Read workspace files and report the observed result.".into();
        state.task_understanding = Some(strategy.decision.strategy.understanding.clone());
        state.collaboration_obligation = strategy.decision.collaboration_obligation.clone();
        let runtime = Arc::new(tokio::sync::Mutex::new(runtime));
        let state = Arc::new(tokio::sync::Mutex::new(state));
        services
            .tool_batch_executor()
            .install_resolver(Arc::new(TurnToolResolver {
                session_id: fixture.session.session_id.clone(),
                graph_id: graph.id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
                services: Arc::downgrade(&services),
            }));
        let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            services.resource_manager().clone(),
            services.scope_locks().clone(),
            services.worktree_leases().clone(),
            services.workspace_key(),
            services.workspace_root(),
        );
        let registered = runner.register(graph).await.unwrap();
        let snapshot = runner
            .pump_snapshot(&registered.id, &BTreeSet::new())
            .await
            .unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let mut wave = runner
            .prepare_ready_wave(&registered.id, snapshot.ready)
            .await
            .unwrap();
        assert_eq!(wave.len(), 1);
        let binding = wave[0].binding.clone().unwrap();
        let ticket = NodeExecutionTicket {
            graph_id: registered.id.clone(),
            node_id: node.id.clone(),
            executor_kind: "tool_batch".into(),
            service_class: registered.service_class,
            attempt: binding.attempt,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        let driven = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            runner.execute_prepared_pump_node(
                &registered.id,
                wave.remove(0),
                Arc::new(tokio::sync::Notify::new()),
            ),
        )
        .await;
        if !matches!(&driven, Ok(Ok(_))) {
            let primary = calls.iter().map(|call| {
                let key = call.binding.as_ref().unwrap().effect_idempotency_key();
                let stream = format!("execution-effect:{key}");
                let records = ["intent", "receipt", "read-receipt"].map(|suffix| {
                    match services.event_store().event_by_idempotency_key(
                        &stream, &format!("{key}:{suffix}"),
                    ) {
                        Ok(Some(event)) => serde_json::json!({
                            "kind": event.kind, "stream": event.stream_id,
                            "transaction": event.transaction_id, "position": event.transaction_index,
                            "cursor": event.commit_cursor, "refs": event.refs,
                            "input_sha256": event.payload["input_sha256"],
                            "origin": event.payload["replay"]["origin"],
                            "parent": event.payload["replay"]["parent_execution"],
                            "root_owner": event.payload["root_effect_owner"],
                            "scope": event.payload["resolved_effect_scope"],
                            "status": event.payload["outcome"]["status"],
                            "error": event.payload["outcome"]["error"].as_str()
                                .map(|error| error.chars().take(600).collect::<String>()),
                        }),
                        Ok(None) => serde_json::Value::Null,
                        Err(error) => serde_json::json!({"lookup_error": error.to_string()}),
                    }
                });
                serde_json::json!({"call":call.id,"tool":call.name,"key":key,
                    "original_input_sha256":crate::context_evidence::read_sha256(call.input.as_bytes()),
                    "records":records})
            }).collect::<Vec<_>>();
            let graph = match services.graph_state_store().load(&registered.id) {
                Ok(graph) => serde_json::json!({"revision":graph.revision,
                    "statuses":graph.node_statuses,"result":graph.node_results.get(&node.id)}),
                Err(error) => serde_json::json!({"lookup_error":error.to_string()}),
            };
            let index = format!(
                "execution-agent-receipts:{}:{}:{}",
                ticket.graph_id, ticket.node_id, ticket.attempt
            );
            let index =
                match services.event_store().list_stream(&index) {
                    Ok(events) => {
                        serde_json::json!(events.iter().take(16).map(|event|
                    serde_json::json!({"kind":event.kind,"sequence":event.sequence,
                        "transaction":event.transaction_id,"cursor":event.commit_cursor,
                        "key":event.payload["idempotency_key"],
                        "call":event.payload["outcome"]["tool_use_id"]})
                ).collect::<Vec<_>>())
                    }
                    Err(error) => serde_json::json!({"lookup_error":error.to_string()}),
                };
            let messages = runtime
                .lock()
                .await
                .session_snapshot()
                .await
                .materialize_messages();
            let tool_results = messages.iter().flat_map(|message| &message.blocks)
                .filter_map(|block| match block {
                    crate::session::ContentBlock::ToolResult {
                        tool_use_id, tool_name, output, is_error,
                    } if calls.iter().any(|call| call.id == *tool_use_id) => {
                        Some(serde_json::json!({"call":tool_use_id,"tool":tool_name,
                            "is_error":is_error,"output_bytes":output.len(),
                            "output_sha256":crate::context_evidence::read_sha256(output.as_bytes()),
                            "output_prefix":output.chars().take(800).collect::<String>()}))
                    }
                    _ => None,
                }).take(calls.len()).collect::<Vec<_>>();
            let transcript = match services.commit_service().committed_host_transcript(&ticket) {
                Ok(document) => {
                    let blocks = document.messages.iter().map(|message|
                        serde_json::from_value::<Vec<crate::session::ContentBlock>>(message["blocks"].clone())
                    ).collect::<Result<Vec<_>, _>>();
                    match blocks {
                        Ok(blocks) => serde_json::json!({"tool_results":blocks.iter().flatten()
                            .filter_map(|block| match block {
                                crate::session::ContentBlock::ToolResult {
                                    tool_use_id, tool_name, output, is_error,
                                } if calls.iter().any(|call| call.id == *tool_use_id) => {
                                    Some(serde_json::json!({"call":tool_use_id,"tool":tool_name,
                                        "is_error":is_error,"output_bytes":output.len(),
                                        "output_sha256":crate::context_evidence::read_sha256(output.as_bytes()),
                                        "output_prefix":output.chars().take(800).collect::<String>()}))
                                }
                                _ => None,
                            }).take(calls.len()).collect::<Vec<_>>()}),
                        Err(error) => serde_json::json!({"decode_error":format!("{:?}", error.classify()),
                            "line":error.line(),"column":error.column()}),
                    }
                }
                Err(_) => serde_json::json!({"lookup_error":"original_transcript_lookup_rejected"}),
            };
            let diagnostic = serde_json::json!({"case":format!("{case:?}"),
                "drive":format!("{driven:?}").chars().take(1200).collect::<String>(),
                "graph":graph,"primary":primary,"original_index":index,
                "host_body_calls":host.entered.load(Ordering::SeqCst),
                "tool_results":tool_results,"committed_transcript":transcript});
            let body = serde_json::to_string_pretty(&diagnostic).unwrap();
            std::fs::write(fixture.root.join("619-drive-failure.json"), &body).unwrap();
            eprintln!("R1_HOST_619_ORIGINAL_DRIVE_FAILURE {body}");
        }
        driven.unwrap().unwrap();
        if matches!(case, Case555::RootGoalClosed) {
            let call = &calls[0];
            let binding = call.binding.as_ref().unwrap();
            let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
                &crate::tool_dispatch::ToolRequest {
                    tool_use_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    input: call.input.clone(),
                    depends_on: vec![],
                },
            );
            request.session_id = Some(fixture.session.session_id.clone());
            request.idempotency_key = binding.effect_idempotency_key();
            request.step_binding = Some(binding.clone());
            let context = crate::CowdExecutionContext {
                execution_id: registered.id.clone(),
                session_id: fixture.session.session_id.clone(),
                turn_id: "471-turn".into(),
            };
            let descriptor = host
                .host
                .pin_snapshot()
                .describe_effect(&call.name, &serde_json::from_str(&call.input).unwrap());
            let error = services
                .commit_service()
                .preflight_fresh_root_tool(&request, &descriptor, &context)
                .unwrap_err();
            assert!(error
                .pre_effect_refusal_for(&call.id, &call.name, &call.input, Some(binding))
                .is_some());
            assert!(crate::ToolError::new(error.to_string())
                .pre_effect_refusal_for(&call.id, &call.name, &call.input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for("foreign-call", &call.name, &call.input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for(&call.id, "foreign-tool", &call.input, Some(binding))
                .is_none());
            assert!(error
                .pre_effect_refusal_for(&call.id, &call.name, "{}", Some(binding))
                .is_none());
            for dimension in ["turn", "execution", "attempt", "policy"] {
                let mut foreign = binding.clone();
                let snapshot = Arc::make_mut(&mut foreign.snapshot);
                match dimension {
                    "turn" => snapshot.identity.turn_id.push_str(":foreign"),
                    "execution" => {
                        snapshot.identity.execution_id = Some("foreign-execution".into())
                    }
                    "attempt" => snapshot.provider_attempt.attempt_id.push_str(":foreign"),
                    _ => {
                        snapshot
                            .related_revisions
                            .insert("policy".into(), "foreign".into());
                    }
                }
                assert!(
                    error
                        .pre_effect_refusal_for(&call.id, &call.name, &call.input, Some(&foreign))
                        .is_none(),
                    "{dimension}"
                );
            }
            let mut unknown = context.clone();
            unknown.execution_id.push_str(":unavailable");
            let unknown = services
                .commit_service()
                .preflight_fresh_root_tool(&request, &descriptor, &unknown)
                .unwrap_err();
            assert!(unknown
                .pre_effect_refusal_for(&call.id, &call.name, &call.input, Some(binding))
                .is_none());
            let mut prior = request.clone();
            let prior_binding = crate::step_snapshot::StepCallBinding::provider(
                step.clone(),
                "independent-prior-intent",
            );
            prior.tool_use_id = prior_binding.origin.call_id.clone();
            prior.idempotency_key = prior_binding.effect_idempotency_key();
            prior.step_binding = Some(prior_binding.clone());
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: format!("execution-effect:{}", prior.idempotency_key),
                    scope: crate::RuntimeEventScope::ExecutionNode,
                    kind: "execution.effect.intent".into(),
                    status: Some("inflight".into()),
                    actor: Some("governed_tool".into()),
                    refs: vec![],
                    payload: serde_json::json!({"negative_fixture":"unresolved original history"}),
                })
                .unwrap();
            let prior_error = services
                .commit_service()
                .preflight_fresh_root_tool(&prior, &descriptor, &context)
                .unwrap_err();
            assert!(prior_error
                .pre_effect_refusal_for(
                    &prior.tool_use_id,
                    &prior.tool_name,
                    &prior.input,
                    Some(&prior_binding)
                )
                .is_none());
        }
        let committed = services.graph_state_store().load(&registered.id).unwrap();
        let original_result = &committed.node_results[&node.id];
        let result_json = serde_json::to_string_pretty(original_result).unwrap();
        std::fs::write(
            fixture.root.join("580-original-node-result.json"),
            &result_json,
        )
        .unwrap();
        if matches!(case, Case555::MissingSessionJournal) {
            assert_eq!(
                original_result.status,
                ExecutionNodeStatus::Failed,
                "{result_json}"
            );
            assert!(
                original_result
                    .failure
                    .as_ref()
                    .is_some_and(|failure| failure.message.contains(
                        "raw tool evidence cannot be published without the Session store"
                    )),
                "{result_json}"
            );
            assert_eq!(host.entered.load(Ordering::SeqCst), 1);
            let key = calls[0].binding.as_ref().unwrap().effect_idempotency_key();
            assert!(services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!("{key}:read-receipt"),
                )
                .unwrap()
                .is_some());
            assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
            assert!(runner.drain_physical_execution().await.is_empty());
            return;
        }
        assert_eq!(
            committed.node_statuses[&node.id],
            ExecutionNodeStatus::Completed,
            "case={case:?}; complete original node_result={result_json}"
        );
        assert!(original_model_closed518(&services, &ticket));
        let source = services
            .event_store()
            .latest_for_stream_kind(
                &format!("{}:node:{}", registered.id, node.id),
                "execution_node.transitioned",
            )
            .unwrap()
            .unwrap();
        let source = services.event_store().resolve_read_record(source).unwrap();
        assert_eq!(source.payload["graph_id"], registered.id);
        assert_eq!(source.payload["node_id"], node.id);
        assert_eq!(source.payload["result"], serde_json::to_value(original_result).unwrap());
        let refusals = source.payload["no_dispatch"]["calls"].as_array();
        let refused = matches!(
            case,
            Case555::OutsideRoot
                | Case555::Mixed
                | Case555::NativePreflightRefusal
                | Case555::RootGoalClosed
                | Case555::AuthorizationGap
                | Case555::MixedAuthorizationGap
                | Case555::BodyFailureBlockedWrite
                | Case555::ScopeRefusalBlockedWrite
                | Case555::BodyFailureBlockedChain
        );
        assert_eq!(
            services.commit_service()
                .authenticated_no_dispatch_sources(&committed, &node.id, &binding)
                .unwrap().is_some(),
            refused,
            "case={case:?} no_dispatch={} body_calls={} result={}",
            source.payload["no_dispatch"],
            host.entered.load(Ordering::SeqCst),
            source.payload["result"],
        );
        if refused {
            let refusals = refusals.expect("original canonical NoDispatch fact");
            assert_eq!(refusals.len(), if blocked_native || matches!(case, Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain) { 2 } else { 1 });
            let expected = calls.iter()
                .filter(|call| matches!(call.id.as_str(), "555-denied" | "580-preflight-denied" | "733-blocked" | "733-transitive"))
                .map(|call| call.id.as_str())
                .collect::<BTreeSet<_>>();
            let actual = refusals.iter()
                .map(|fact| fact["call_id"].as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(actual, expected);
        } else {
            assert!(source.payload["no_dispatch"].is_null());
        }
        let expected_body_calls = if matches!(
            case,
            Case555::OutsideRoot
                | Case555::NativePreflightRefusal
                | Case555::AuthorizationGap
                | Case555::ScopeRefusalBlockedWrite
                | Case555::RootGoalClosed
        ) {
            0
        } else {
            1
        };
        assert_eq!(host.entered.load(Ordering::SeqCst), expected_body_calls);
        for call in &calls {
            let key = call.binding.as_ref().unwrap().effect_idempotency_key();
            let receipt = services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!("{key}:read-receipt"),
                )
                .unwrap();
            assert_eq!(
                receipt.is_some(),
                !matches!(call.id.as_str(), "555-denied" | "580-preflight-denied" | "733-blocked" | "733-transitive")
            );
            if call.id == "555-missing" {
                let receipt = receipt.as_ref().unwrap();
                assert_eq!(receipt.payload["outcome"]["status"], "failed");
                assert!(receipt.payload["outcome"]["error"].as_str().unwrap().contains("No such file"));
                assert!(receipt.payload["read_acquisition_v1"].is_null());
                assert!(!source.payload["no_dispatch"]["calls"].as_array().is_some_and(|facts| {
                    facts.iter().any(|fact| fact["call_id"] == call.id)
                }));
            }
            assert!(services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!("{key}:intent"),
                )
                .unwrap()
                .is_none());
        }
        let document = services
            .commit_service()
            .committed_host_transcript(&ticket)
            .unwrap();
        assert_eq!(document.messages.len(), calls.len());
        let successor = runner
            .pump_snapshot(&registered.id, &BTreeSet::new())
            .await
            .unwrap();
        assert!(
            !successor.ready.is_empty(),
            "original closed batch must release its successor"
        );
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(!root.join("denied.txt").exists());
        assert!(!root.join("transitive.txt").exists());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        assert!(runner.drain_physical_execution().await.is_empty());
    }

    #[tokio::test]
    async fn real_host_555_absolute_workspace_root_closes_original_batch_and_releases_successor() {
        original_host_chain555(Case555::AbsoluteRoot).await;
    }

    #[tokio::test]
    async fn real_host_685_secret_authorization_gap_closes_original_without_dispatch() {
        original_host_chain555(Case555::AuthorizationGap).await;
    }

    #[tokio::test]
    async fn real_host_685_mixed_secret_gap_and_real_receipt_release_successor() {
        original_host_chain555(Case555::MixedAuthorizationGap).await;
    }

    #[tokio::test]
    async fn real_host_555_relative_workspace_root_retains_existing_admission_and_receipt() {
        original_host_chain555(Case555::RelativeRoot).await;
    }

    #[tokio::test]
    async fn root_goal_refusal_closes_original_host_binding_without_a_tool_body_or_provider() {
        original_host_chain555(Case555::RootGoalClosed).await;
    }

    #[tokio::test]
    async fn real_host_555_pre_effect_scope_rejection_has_original_no_dispatch_not_physical_timeout(
    ) {
        original_host_chain555(Case555::OutsideRoot).await;
    }

    #[tokio::test]
    async fn real_host_555_native_body_failure_is_receipted_not_no_dispatch() {
        original_host_chain555(Case555::BodyFailure).await;
    }

    #[tokio::test]
    async fn real_host_733_body_failure_and_unexecuted_write_close_original_successor() {
        original_host_chain555(Case555::BodyFailureBlockedWrite).await;
    }

    #[tokio::test]
    async fn real_host_733_scope_refusal_and_unexecuted_write_close_original_successor() {
        original_host_chain555(Case555::ScopeRefusalBlockedWrite).await;
    }

    #[tokio::test]
    async fn real_host_733_transitive_unexecuted_writes_never_enter_host() {
        original_host_chain555(Case555::BodyFailureBlockedChain).await;
    }

    #[tokio::test]
    async fn real_host_733_native_typed_pre_effect_refusal_blocks_write_without_dispatch() {
        original_host_chain733(Case555::NativePreflightRefusal, true).await;
    }

    #[tokio::test]
    async fn real_host_555_mixed_original_receipt_and_refusal_close_only_their_exact_calls() {
        original_host_chain555(Case555::Mixed).await;
    }

    #[tokio::test]
    async fn real_host_580_missing_session_journal_rejects_raw_publication_after_real_read_receipt()
    {
        original_host_chain555(Case555::MissingSessionJournal).await;
    }

    #[tokio::test]
    async fn real_host_580_native_preflight_typed_refusal_closes_original_legacy_batch_without_effect(
    ) {
        original_host_chain555(Case555::NativePreflightRefusal).await;
    }

    #[test]
    fn real_host_555_workspace_root_mapping_is_generic_and_does_not_admit_escape() {
        let fixture = PublicationFixture::new();
        let root = std::fs::canonicalize(&fixture.root).unwrap();
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(&root).unwrap();
        let host = tools::ToolHost::builtin("scope-555", &root);
        for name in ["read_file", "edit", "workspace_snapshot"] {
            let input = match name {
                "edit" => {
                    serde_json::json!({"action":"write","path":root,"content":"never executed"})
                }
                "workspace_snapshot" => {
                    serde_json::json!({"roots":[root],"max_files":20,"page_size":10})
                }
                _ => serde_json::json!({"path":root}),
            };
            let descriptor = host.pin_snapshot().describe_effect(name, &input);
            let (locks, identities) =
                crate::bound_tool_batch::bound_effect_lock_requests(&resolver, &descriptor)
                    .unwrap();
            assert!(!locks.is_empty(), "{name}");
            let accesses = crate::tool_host::effect_file_accesses(&descriptor);
            let expected_identity_targets = accesses
                .iter()
                .filter_map(|(path, _)| {
                    let normalized = crate::governed_tool_plan::normalize_resource_path(path);
                    (!resolver.is_workspace_scope(&normalized)).then(|| path.clone())
                })
                .collect::<BTreeSet<_>>();
            assert_eq!(
                identities
                    .iter()
                    .map(|(path, _)| path.clone())
                    .collect::<BTreeSet<_>>(),
                expected_identity_targets,
                "{name}: all non-root scopes retain their exact identities"
            );
            for (path, identity) in &identities {
                assert_eq!(*identity, resolver.resolve_planned_file(path).unwrap());
                assert_ne!(
                    identity.workspace_relative_path, ".",
                    "{name}: root must stay a workspace lock"
                );
                if path == ".cowd/checkpoints" {
                    assert_eq!(
                        identity.object_kind,
                        harness_contract::context::WorkspaceObjectKind::Directory
                    );
                }
            }
            assert!(locks.iter().any(|lock| lock.scope
                == crate::execution_core::graph::ScopedResource::workspace(
                    resolver.workspace_id()
                )
                .unwrap()));
        }
        assert_eq!(
            resolver.resolve_planned_file(".").unwrap().object_kind,
            harness_contract::context::WorkspaceObjectKind::Directory
        );
        assert!(resolver.is_workspace_scope("."));
        assert!(resolver.is_workspace_scope(root.to_str().unwrap()));
        for path in [
            root.parent().unwrap().to_path_buf(),
            root.with_extension("sibling"),
            root.join(".."),
            root.join("child"),
        ] {
            assert!(!resolver.is_workspace_scope(path.to_str().unwrap()));
        }
        let mut effect = host
            .pin_snapshot()
            .describe_effect("read_file", &serde_json::json!({"path":"proof.txt"}));
        for path in [
            "../escape".to_string(),
            root.with_extension("sibling").display().to_string(),
        ] {
            for scope in &mut effect.scopes {
                if scope.resource == harness_contract::policy::PermissionResource::File {
                    scope.target = Some(path.clone());
                }
            }
            assert!(
                crate::bound_tool_batch::bound_effect_lock_requests(&resolver, &effect).is_err()
            );
        }
    }
}

#[derive(Clone)]
struct S01Client518(Arc<AtomicUsize>);

impl ApiClient for S01Client518 {
    fn stream(
        &mut self,
        _request: crate::ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::AssistantEvent, RuntimeError>> + Send + '_>> {
        if self.0.fetch_add(1, Ordering::SeqCst) != 0 {
            return Box::pin(stream::iter(vec![Err(RuntimeError::new(
                "518 forbids provider replay",
            ))]));
        }
        Box::pin(stream::iter(vec![
            Ok(crate::AssistantEvent::PrivateReasoningDelta(
                "Original S01 deterministic reasoning.".into(),
            )),
            Ok(crate::AssistantEvent::SignatureDelta(
                "518-original-signature".into(),
            )),
            Ok(crate::AssistantEvent::TextDelta("56".into())),
            Ok(crate::AssistantEvent::Usage(
                model_protocol::usage::TokenUsage {
                    input_tokens: 3189,
                    output_tokens: 57,
                    cache_creation_input_tokens: 0,
                    cache_read_input_tokens: 0,
                },
            )),
            Ok(crate::AssistantEvent::MessageStop),
        ]))
    }
}

fn original_model_closed518(
    services: &crate::RuntimeServices,
    ticket: &NodeExecutionTicket,
) -> bool {
    let state = services.graph_state_store();
    let graph = state.load(&ticket.graph_id).unwrap();
    let binding = state
        .current_binding(&graph, &ticket.node_id, ticket.attempt)
        .unwrap();
    let start = state
        .binding_start_position(&graph, &ticket.node_id, &binding)
        .unwrap()
        .unwrap();
    state
        .binding_lease_closure(&graph, &ticket.node_id, &binding, start)
        .unwrap()
}

#[tokio::test]
async fn real_host_507_existing_goal_authority_is_pinned_without_redeclaring_completion() {
    for interleaved_progress in [false, true] {
        let fixture = PublicationFixture::new();
        let original_goal = fixture
            .services
            .goal_store()
            .get(&fixture.goal_id)
            .unwrap()
            .unwrap();
        fixture
            .services
            .goal_store()
            .complete(
                &fixture.goal_id,
                original_goal.revision,
                GoalCompletion::Satisfied,
                "independent original immutable decision",
            )
            .unwrap();
        let authority = fixture
            .services
            .goal_store()
            .projection(&fixture.goal_id)
            .unwrap()
            .unwrap();
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
        assert!(!outcome
            .domain_events
            .iter()
            .any(|event| event.event.kind == "goal.completed"));
        assert_eq!(
            outcome
                .source_revisions
                .get(&format!("goal:{}", fixture.goal_id)),
            Some(&authority.stream_revision)
        );
        assert!(!outcome
            .domain_events
            .iter()
            .any(|event| event.event.kind == "goal.observation"));
        if interleaved_progress {
            let identity = {
                let state = hot.state.lock().await;
                runtime_observation_identity(&fixture.services, &state, &fixture.ticket)
            };
            fixture
                .services
                .goal_store()
                .record_observation(runtime_observation(
                    identity,
                    RuntimeObservationKind::GraphProgress,
                    "test.507.after_independent_goal_terminal",
                    authority.stream_revision,
                    "Independent progress after an already-owned terminal".into(),
                    "507-owned-terminal-progress".into(),
                    ObservationResultClass::Informational,
                ))
                .unwrap();
            let expected_goal = fixture
                .services
                .goal_store()
                .projection(&fixture.goal_id)
                .unwrap()
                .unwrap();
            let cursor = fixture
                .services
                .event_store()
                .durable_commit_cursor()
                .unwrap();
            assert!(fixture
                .services
                .commit_service()
                .transition_execution_outcome(
                    &fixture.graph,
                    &fixture.ticket,
                    &outcome,
                    &BTreeMap::new(),
                )
                .is_err());
            assert_eq!(
                fixture
                    .services
                    .event_store()
                    .durable_commit_cursor()
                    .unwrap(),
                cursor
            );
            assert_eq!(
                fixture
                    .services
                    .goal_store()
                    .projection(&fixture.goal_id)
                    .unwrap()
                    .unwrap(),
                expected_goal
            );
            assert_rejected_without_publication(&hot, &fixture.ticket).await;
        } else {
            fixture.commit(&outcome);
            let committed = fixture
                .services
                .goal_store()
                .projection(&fixture.goal_id)
                .unwrap()
                .unwrap();
            assert_eq!(committed, authority);
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}


#[tokio::test]
async fn real_host_471_independent_goal_source_cas_rejects_stale_terminal_without_writes() {
    for (independent_terminal, use_captured_sources) in
        [(true, false), (true, true), (false, false), (false, true)]
    {
        let fixture = PublicationFixture::new();
        let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        let outcome = host.synthesize(&fixture.ticket).await.unwrap();
        let goal_stream_id = format!("goal:{}", fixture.goal_id);
        let source_revision = *outcome
            .source_revisions
            .get(&goal_stream_id)
            .expect("original Host Goal source cut");
        assert_eq!(
            source_revision,
            fixture
                .services
                .goal_store()
                .projection(&fixture.goal_id)
                .unwrap()
                .unwrap()
                .stream_revision
        );
        let goal = fixture
            .services
            .goal_store()
            .get(&fixture.goal_id)
            .unwrap()
            .unwrap();
        if independent_terminal {
            fixture
                .services
                .goal_store()
                .complete(
                    &fixture.goal_id,
                    goal.revision,
                    GoalCompletion::Partial,
                    "independent original Goal decision",
                )
                .unwrap();
        } else {
            let identity = {
                let state = host.state.lock().await;
                runtime_observation_identity(&fixture.services, &state, &fixture.ticket)
            };
            fixture
                .services
                .goal_store()
                .record_observation(runtime_observation(
                    identity,
                    RuntimeObservationKind::GraphProgress,
                    "test.507.independent_goal_progress",
                    source_revision,
                    "Independent progress advanced only the original Goal stream".into(),
                    "507-independent-goal-progress".into(),
                    ObservationResultClass::Informational,
                ))
                .unwrap();
        }
        let expected_goal = fixture
            .services
            .goal_store()
            .projection(&fixture.goal_id)
            .unwrap()
            .unwrap();
        assert!(expected_goal.stream_revision > source_revision);
        let sources = fixture
            .services
            .commit_service()
            .execution_outcome_sources(&fixture.graph, &fixture.ticket, &outcome)
            .unwrap();
        assert_eq!(sources.get(&goal_stream_id), Some(&source_revision));
        let cursor = fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap();
        let graph_stream = fixture
            .services
            .event_store()
            .list_stream(&fixture.graph.id)
            .unwrap();
        let goal_stream = fixture
            .services
            .event_store()
            .list_stream(&goal_stream_id)
            .unwrap();
        let expected = if use_captured_sources {
            sources
        } else {
            BTreeMap::new()
        };
        {
            let error = fixture
                .services
                .commit_service()
                .transition_execution_outcome(&fixture.graph, &fixture.ticket, &outcome, &expected)
                .err()
                .expect("original Goal source must reject the stale outcome");
            assert!(
                matches!(
                    error,
                    crate::execution_core::graph::ExecutionCommitError::EventStore(
                        crate::RuntimeEventStoreError::StaleRevision {
                            ref stream_id,
                            expected,
                            actual,
                        }
                    ) if stream_id == &goal_stream_id
                        && expected == source_revision
                        && actual == expected_goal.stream_revision
                ),
                "original independent Goal source must reject stale terminal: {error}"
            );
            assert_eq!(
                fixture
                    .services
                    .event_store()
                    .durable_commit_cursor()
                    .unwrap(),
                cursor
            );
            assert_eq!(
                fixture
                    .services
                    .event_store()
                    .list_stream(&fixture.graph.id)
                    .unwrap(),
                graph_stream
            );
            assert_eq!(
                fixture
                    .services
                    .event_store()
                    .list_stream(&goal_stream_id)
                    .unwrap(),
                goal_stream
            );
            assert_eq!(
                fixture
                    .services
                    .goal_store()
                    .projection(&fixture.goal_id)
                    .unwrap()
                    .unwrap(),
                expected_goal
            );
            assert!(fixture
                .services
                .commit_service()
                .committed_host_transcript(&fixture.ticket)
                .is_err());
            assert!(fixture
                .services
                .event_store()
                .session_terminal_for_execution(
                    &fixture.session.session_id,
                    &fixture.graph.id,
                    "471-turn",
                )
                .unwrap()
                .is_none());
        }
        assert_rejected_without_publication(&host, &fixture.ticket).await;
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_host_471_foreign_pending_terminal_pin_cannot_be_promoted() {
    let fixture = PublicationFixture::new();
    let (host, _store, _ingress) = claimed_ingress_host(&fixture, false).await;
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    fixture.commit(&outcome);
    host.state
        .lock()
        .await
        .pending_terminal_artifact
        .as_mut()
        .unwrap()
        .durable_owner = "foreign-terminal-owner".into();
    assert_rejected_without_publication(&host, &fixture.ticket).await;
}


#[tokio::test]
async fn real_host_471_reserved_publication_tampering_fails_atomically_before_writes() {
    for mutation in [
        "actor",
        "session",
        "turn",
        "attempt",
        "generation",
        "digest",
    ] {
        let fixture = PublicationFixture::new();
        let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        let mut outcome = host.synthesize(&fixture.ticket).await.unwrap();
        let event = outcome
            .domain_events
            .iter_mut()
            .find(|input| input.event.kind == "execution.host.transcript")
            .unwrap();
        match mutation {
            "actor" => event.event.actor = Some("inline_model".into()),
            "session" => event.event.payload["session_id"] = "foreign".into(),
            "turn" => event.event.payload["turn_id"] = "foreign".into(),
            "attempt" => event.event.payload["attempt"] = serde_json::json!(2),
            "generation" => {
                event.event.payload["_runtime_activity_binding"]["generation"] =
                    serde_json::json!(2)
            }
            "digest" => outcome.result.result_ref = Some("turn-result:foreign:digest".into()),
            _ => unreachable!(),
        }
        let cursor = fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap();
        let before = messages(&host).await;
        assert!(
            fixture
                .services
                .commit_service()
                .transition_execution_outcome(
                    &fixture.graph,
                    &fixture.ticket,
                    &outcome,
                    &BTreeMap::new(),
                )
                .is_err(),
            "reserved publication mutation {mutation} must not commit"
        );
        assert_eq!(
            fixture
                .services
                .event_store()
                .durable_commit_cursor()
                .unwrap(),
            cursor
        );
        assert_eq!(messages(&host).await, before);
        assert!(fixture
            .services
            .event_store()
            .session_terminal_for_execution(
                &fixture.session.session_id,
                &fixture.graph.id,
                "471-turn",
            )
            .unwrap()
            .is_none());
    }
}



async fn claimed_ingress_host(
    fixture: &PublicationFixture,
    stale: bool,
) -> (
    PublicationHost,
    Arc<session::UnifiedSessionStore>,
    TurnIngressRef,
) {
    claimed_ingress_host_with_prompt(fixture, stale, "answer directly").await
}

async fn claimed_ingress_host_with_prompt(
    fixture: &PublicationFixture,
    stale: bool,
    prompt: &str,
) -> (
    PublicationHost,
    Arc<session::UnifiedSessionStore>,
    TurnIngressRef,
) {
    let store = Arc::new(crate::test_support::session_store());
    let session_id = &fixture.session.session_id;
    store
        .create_session(&session::SessionRecord {
            session_id: session_id.clone(),
            platform: "test".into(),
            chat_id: session_id.clone(),
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
    let now = crate::tool_invocation::now_ms();
    let request_id = format!("471-request-{}", uuid::Uuid::new_v4());
    let message_id = format!("471-message-{}", uuid::Uuid::new_v4());
    let request = session::SessionRuntimeOutboxRequest {
        input_id: format!("471-input-{request_id}"),
        request_id: request_id.clone(),
        turn_id: "471-turn".into(),
        message_id: message_id.clone(),
        session_generation: 1,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        created_at_ms: now,
        runtime_options_json: None,
    };
    let content = serde_json::json!([{"type":"text","text":prompt}]).to_string();
    store
        .append_ingress_with_runtime_outbox(session_id, "user", Some(&content), now, &request)
        .await
        .unwrap();
    let claimed = store
        .claim_session_runtime_outbox("471-worker", now, 60_000, 1, &[])
        .await
        .unwrap()
        .remove(0);
    let token = claimed.claim_token.clone().unwrap();
    let running = store
        .mark_session_runtime_outbox_running(
            &request_id,
            "471-worker",
            1,
            &token,
            claimed.revision,
            now,
        )
        .await
        .unwrap();
    let fence = crate::SessionExecutionFence::from_claim(
        crate::session_runtime_port::TestSessionPortAdapter::new(store.clone()),
        &request_id,
        session_id,
        1,
        running.sequence,
        "471-worker",
        &token,
    )
    .unwrap();
    let ingress = TurnIngressRef {
        request_id: request_id.clone(),
        turn_id: "471-turn".into(),
        message_id,
        session_id: session_id.clone(),
        primary_task_id: "471-task".into(),
        root_task_id: "471-task".into(),
        session_generation: 1,
        input_sequence: running.sequence as u64,
        claim_owner: "471-worker".into(),
        claim_token: token,
        claim_revision: running.claim_fence_epoch.unwrap(),
        handoff_id: None,
    };
    if stale {
        store
            .advance_session_input_generation(
                session_id,
                1,
                true,
                "test",
                "invalidate original terminal claim",
                now + 1,
            )
            .await
            .unwrap();
    }
    let runtime = PublicationRuntime::new(
        fixture.session.clone(),
        NoReplayClient(fixture.calls.clone()),
        NoReplayTools,
        PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
        vec!["answer directly".into()],
    )
    .without_memory()
    .with_runtime_event_store(fixture.services.event_store().clone())
    .with_artifact_store(fixture.artifacts.clone())
    .with_session_execution_fence(fence);
    runtime
        .begin_turn_strategy("471-turn", "answer directly")
        .unwrap();
    runtime
        .bind_turn_strategy_execution("471-turn", &fixture.graph.id)
        .unwrap();
    let mut state = publication_state(session_id, &fixture.goal_id);
    state.terminal_override = Some((GoalCompletion::Satisfied, ORIGINAL_ANSWER.into()));
    state.ingress = Some(ingress.clone());
    (
        PublicationHost {
            runtime: Arc::new(tokio::sync::Mutex::new(runtime)),
            state: Arc::new(tokio::sync::Mutex::new(state)),
            services: fixture.services.clone(),
        },
        store,
        ingress,
    )
}


#[tokio::test]
async fn real_host_471_stale_ingress_generation_does_not_publish_or_forge_claim() {
    let fixture = PublicationFixture::new();
    let (host, _store, ingress) = claimed_ingress_host(&fixture, true).await;
    let before = messages(&host).await;
    let cursor = fixture
        .services
        .event_store()
        .durable_commit_cursor()
        .unwrap();
    assert!(host.synthesize(&fixture.ticket).await.is_err());
    assert_eq!(messages(&host).await, before);
    assert_eq!(
        fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap(),
        cursor
    );
    assert!(fixture
        .services
        .event_store()
        .session_terminal(&format!("turn-terminal:{}", ingress.request_id))
        .unwrap()
        .is_none());
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}




use crate::execution_core::graph::executors::SynthesizeBackend;
use crate::execution_core::graph::ExecutionNodeBinding;
use crate::runtime_event_store::EphemeralRuntimeEventStore;
use futures::{stream, Stream};
use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};

const ORIGINAL_ANSWER: &str = "Original terminal answer 471: the verified result survives restart.";

struct NoReplayClient(Arc<AtomicUsize>);

impl ApiClient for NoReplayClient {
    fn stream(
        &mut self,
        _request: crate::ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<crate::AssistantEvent, RuntimeError>> + Send + '_>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(stream::iter(vec![Err(RuntimeError::new(
            "471 forbids provider replay",
        ))]))
    }
}

struct NoReplayTools;

#[async_trait::async_trait]
impl ToolExecutor for NoReplayTools {
    async fn execute_output(
        &self,
        name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        Err(crate::ToolError::new(format!(
            "471 forbids tool replay: {name}"
        )))
    }
}

type PublicationRuntime = crate::ConversationRuntime<NoReplayClient, NoReplayTools>;
type PublicationHost = TurnSynthesizeBackend<NoReplayClient, NoReplayTools>;

struct PublicationFixture {
    root: std::path::PathBuf,
    backend: Arc<EphemeralRuntimeEventStore>,
    artifacts: Arc<crate::ArtifactStore>,
    services: Arc<crate::RuntimeServices>,
    session: Session,
    goal_id: String,
    graph: ExecutionGraph,
    ticket: NodeExecutionTicket,
    calls: Arc<AtomicUsize>,
}

impl PublicationFixture {
    fn new() -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("COWD_TEST_FIXTURE_ROOT").expect("explicit private fixture root"),
        )
        .join(format!("synthesize-471-{}", uuid::Uuid::new_v4()));
        assert!(root.is_absolute(), "fixture root must be absolute");
        std::fs::create_dir_all(&root).unwrap();
        let backend = Arc::new(EphemeralRuntimeEventStore::new());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            root.join("artifacts"),
        ));
        let services = Self::services_at(&root, backend.clone(), artifacts.clone());
        let mut session = Session::new();
        session.extend_messages([ConversationMessage::user_text("answer directly")]);
        let goal_id = format!("471-goal-{}", uuid::Uuid::new_v4());
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":goal_id,"session_id":session.session_id,"objective":"answer directly",
            "criteria":[{"id":"checked","statement":"answer checked","status":"satisfied"}],
            "phase":"execution","spec_revision":1,"spec_digest":"471-spec",
            "scope":"internal","completion":"open","revision":1,"user_sequence":1
        }))
        .unwrap();
        services.goal_store().create(goal).unwrap();
        let mut graph = ExecutionGraph::new("answer directly");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: session.session_id.clone(),
            turn_id: "471-turn".into(),
            root_task_id: "471-task".into(),
            task_id: "471-task".into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::Synthesize, "synthesize", "{}");
        node.id = format!("{}:synthesize", graph.id);
        node.idempotency_key = format!("{}:original", node.id);
        graph.nodes.push(node.clone());
        graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        graph = services
            .commit_service()
            .transition_node(
                &graph,
                &node.id,
                ExecutionNodeStatus::Ready,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        let ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(),
            node_id: node.id.clone(),
            executor_kind: "synthesize".into(),
            service_class: graph.service_class,
            attempt: 1,
            idempotency_key: node.idempotency_key.clone(),
            payload_ref: node.payload_ref.clone(),
        };
        graph = services
            .commit_service()
            .bind_and_start_nodes(
                &graph,
                BTreeMap::from([(
                    node.id,
                    ExecutionNodeBinding {
                        executor_kind: "synthesize".into(),
                        ticket_idempotency_key: ticket.idempotency_key.clone(),
                        attempt: 1,
                        resource_lease_refs: Vec::new(),
                        scope_lease_ref: None,
                        worktree_lease_ref: None,
                    },
                )]),
                vec![ticket.clone()],
            )
            .unwrap()
            .graph;
        Self {
            root,
            backend,
            artifacts,
            services,
            session,
            goal_id,
            graph,
            ticket,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    // Build the original Goal/model candidate transaction before exercising
    // the production Synthesize backend. This does not pre-mark Goal success.
    fn with_original_candidate(answer: &str) -> Self {
        let root = std::path::PathBuf::from(
            std::env::var_os("COWD_TEST_FIXTURE_ROOT").expect("explicit private fixture root"),
        ).join(format!("rb-candidate-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let backend = Arc::new(EphemeralRuntimeEventStore::new());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(root.join("artifacts")));
        let services = Self::services_at(&root, backend.clone(), artifacts.clone());
        let mut session = Session::new();
        session.extend_messages([ConversationMessage::user_text("answer directly")]);
        let goal_id = format!("rb-goal-{}", uuid::Uuid::new_v4());
        let mut graph = ExecutionGraph::new("answer directly");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: session.session_id.clone(), turn_id: "471-turn".into(),
            root_task_id: "471-task".into(), task_id: "471-task".into(), generation: 1,
        });
        let objective = harness_contract::agent_action::root_objective_id(&session.session_id, "471-turn");
        let program = harness_contract::agent_action::program_id_for_objective(&objective);
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":goal_id,"session_id":session.session_id,"objective":"answer directly",
            "criteria":[{"id":"checked","statement":"answer checked","status":"satisfied"}],
            "phase":"execution","spec_revision":1,"spec_digest":"rb-spec",
            "scope":"internal","completion":"open","revision":1,"user_sequence":1,
            "source_intent_ref":"session_message:original",
            "execution_binding":{"objective_id":objective,"session_id":session.session_id,
                "turn_id":"471-turn","root_execution_id":graph.id,"agentic_program_id":program}
        })).unwrap();
        services.goal_store().create(goal).unwrap();
        let model = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph.nodes.push(model.clone());
        graph = services.commit_service().register_graph(graph).unwrap().graph;
        graph = services.commit_service().transition_node(&graph, &model.id, ExecutionNodeStatus::Ready, None, vec![]).unwrap().graph;
        let model_ticket = NodeExecutionTicket {
            graph_id: graph.id.clone(), node_id: model.id.clone(), executor_kind: model.executor_kind.clone(),
            service_class: graph.service_class, attempt: 1, idempotency_key: model.idempotency_key.clone(), payload_ref: model.payload_ref.clone(),
        };
        graph = services.commit_service().bind_and_start_node(&graph, &model.id, ExecutionNodeBinding {
            executor_kind:model_ticket.executor_kind.clone(), ticket_idempotency_key:model_ticket.idempotency_key.clone(),
            attempt:1, resource_lease_refs:vec![], scope_lease_ref:None, worktree_lease_ref:None,
        }).unwrap().graph;
        let state = publication_state(&session.session_id, &goal_id);
        let identity = runtime_observation_identity(&services, &state, &model_ticket);
        let source = services.goal_store().projection(&goal_id).unwrap().unwrap();
        let observation = runtime_observation(identity, RuntimeObservationKind::GraphProgress,
            "runtime.model_step", source.stream_revision, "original candidate".into(),
            "rb-original-candidate".into(), ObservationResultClass::Informational);
        let event = services.goal_store().turn_candidate_event_at(&source, &observation, &model_ticket, answer).unwrap();
        let mut synth = ExecutionNodeSpec::new(ExecutionNodeKind::Synthesize, "synthesize", "{}");
        synth.id = format!("{}:synthesize", graph.id);
        synth.idempotency_key = format!("{}:original", synth.id);
        let result = harness_contract::execution_graph::ExecutionNodeResult {
            status:ExecutionNodeStatus::Completed,
            result_ref:Some(format!("assistant_json:{}", serde_json::to_string(answer).unwrap())),
            summary:None, wait_class:None, evidence_refs:vec![], failure:None,
            usage:ExecutionUsage::default(), finished_at_ms:1,
        };
        graph = services.commit_service().transition_node_with_replan(&graph, &model.id, result,
            vec![event], vec![synth.clone()], vec![], "original candidate".into(), true).unwrap().graph;
        graph = services.commit_service().transition_node(&graph, &synth.id, ExecutionNodeStatus::Ready, None, vec![]).unwrap().graph;
        let ticket = NodeExecutionTicket { graph_id:graph.id.clone(), node_id:synth.id.clone(),
            executor_kind:synth.executor_kind.clone(), service_class:graph.service_class, attempt:1,
            idempotency_key:synth.idempotency_key.clone(), payload_ref:synth.payload_ref.clone() };
        graph = services.commit_service().bind_and_start_nodes(&graph, BTreeMap::from([(synth.id.clone(), ExecutionNodeBinding {
            executor_kind:ticket.executor_kind.clone(), ticket_idempotency_key:ticket.idempotency_key.clone(),
            attempt:1, resource_lease_refs:vec![], scope_lease_ref:None, worktree_lease_ref:None,
        })]), vec![ticket.clone()]).unwrap().graph;
        Self { root, backend, artifacts, services, session, goal_id, graph, ticket, calls:Arc::new(AtomicUsize::new(0)) }
    }

    fn services_at(
        root: &std::path::Path,
        backend: Arc<EphemeralRuntimeEventStore>,
        artifacts: Arc<crate::ArtifactStore>,
    ) -> Arc<crate::RuntimeServices> {
        crate::RuntimeServices::builder(root.join("home"), root)
            .runtime_event_store(Arc::new(crate::RuntimeEventStore::from_backend(backend)))
            .artifact_store(artifacts)
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .build()
            .unwrap()
    }

    fn host(
        &self,
        services: Arc<crate::RuntimeServices>,
        session: Session,
        hot: bool,
    ) -> PublicationHost {
        let runtime = PublicationRuntime::new(
            session,
            NoReplayClient(self.calls.clone()),
            NoReplayTools,
            PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
            vec!["answer directly".into()],
        )
        .without_memory()
        .with_runtime_event_store(services.event_store().clone())
        .with_artifact_store(services.artifact_store().clone());
        runtime
            .begin_turn_strategy("471-turn", "answer directly")
            .unwrap();
        runtime
            .bind_turn_strategy_execution("471-turn", &self.graph.id)
            .unwrap();
        let mut state = publication_state(&self.session.session_id, &self.goal_id);
        if hot {
            state.terminal_override = Some((GoalCompletion::Satisfied, ORIGINAL_ANSWER.into()));
        }
        PublicationHost {
            runtime: Arc::new(tokio::sync::Mutex::new(runtime)),
            state: Arc::new(tokio::sync::Mutex::new(state)),
            services,
        }
    }

    fn commit(&self, outcome: &NodeExecutionOutcome) {
        self.services
            .commit_service()
            .transition_execution_outcome(&self.graph, &self.ticket, outcome, &BTreeMap::new())
            .expect("original atomic Graph/node/NodeEffect/Host result");
    }
}

fn publication_state(session_id: &str, goal_id: &str) -> TurnGraphState {
    TurnGraphState {
        content: "answer directly".into(),
        task_understanding: Some(harness_contract::strategy::understand(
            &harness_contract::strategy::StrategyInput::from_prompt("answer directly"),
        )),
        collaboration_obligation: None,
        prompter: SharedPrompter::none(),
        first_model_step: true,
        pending_next_model_context: Vec::new(),
        persistent_collaboration_context: Vec::new(),
        assistant_messages: Vec::new(),
        tool_results: Vec::new(),
        iterations: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_create_tokens: 0,
        cache_read_tokens: 0,
        cache_dimensions_known: false,
        provider_usage_attempt_observed: false,
        output_chars: 0,
        output_chunks: 0,
        wall_duration_ms: 0,
        model: None,
        models_used: Vec::new(),
        first_token_latency_ms: None,
        active_stream_duration_ms: 0,
        summary: None,
        failure: None,
        terminal_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        pending_transcript: std::collections::BTreeMap::new(),
        pending_member_batches: std::collections::BTreeMap::new(),
        pending_agent_input_consumption: std::collections::BTreeMap::new(),
        deferred_agent_input_consumption: Vec::new(),
        ingress: None,
        turn_transcript_start: 0,
        session_id: session_id.into(),
        turn_id: "471-turn".into(),
        goal_id: goal_id.into(),
        context_window: 0,
        safety_lease: crate::execution_core::SafetyFusePolicy::derive(
            0,
            harness_contract::core::TaskComplexity::Simple,
            None,
        ),
        terminal_override: None,
        delivery_envelope: None,
        terminal_presentation: None,
        terminal_commit_owner: None,
        authenticated_terminal_owner: None,
        committed_terminal_answer: None,
        committed_terminal_completion: None,
        last_verified_progress: false,
        consecutive_unverified_model_steps: 0,
        closure_ready_steps: 0,
        previous_objective_milestones: Default::default(),
        verified_collaboration_progress: Default::default(),
        verified_answer_ready_steps: 0,
        final_answer_observed: false,
        collaboration_convergence_replans: 0,
        delegated_redrives: 0,
        completion_gap_continuations: 0,
        started_at: std::time::Instant::now(),
        reasoning_only_attempts: 0,
        force_text_only_next_model: false,
        force_tool_allowlist_next_model: None,
        force_tool_call_next_model: false,
        force_reasoning_effort_next_model: None,
        terminal_recovery_attempts: 0,
        provider_protocol_recovery_attempts: 0,
        provider_frame_repair_attempts: std::collections::BTreeMap::new(),
        agentic_protocol_recovery_attempts: 0,
        agentic_program_context_revision: None,
        execution_role: TurnExecutionRole::RootTurn,
        bounded_evidence_role: false,
        focus_novelty_target_bp: 0,
        focus_acceptance_scopes: Vec::new(),
        focus_acceptance_pending_scopes: Vec::new(),
        focus_required_output_fields: Vec::new(),
        structured_output_replans: 0,
        focus_observed_resource_scopes: BTreeSet::new(),
        focus_observed_evidence: Vec::new(),
        focus_action_rejections: 0,
        pending_focus_terminal_candidate: None,
        focus_discovery_replans: 0,
        focus_verification_prefetched: false,
        clean_terminal_synthesis_next: false,
        clean_terminal_synthesis_attempted: false,
        clean_terminal_retry_attempted: false,
        terminal_failure_narration: None,
        consecutive_tool_failure_batches: 0,
        cold_start_failure_replans: 0,
        consecutive_low_novelty_batches: 0,
        successful_tool_calls: 0,
        last_terminal_answer_digest: String::new(),
        repeated_terminal_answers: 0,
        completion_gap_outstanding: false,
        outstanding_closure_rungs: Vec::new(),
        tool_receipts_observed: 0,
        duplicate_tool_calls: 0,
        identical_action_digest: String::new(),
        identical_action_result_digest: String::new(),
        identical_action_streak: 0,
        per_turn_tool_counts: BTreeMap::new(),
        write_attempt_paths: Vec::new(),

        committed_workspace_write_observed: false,
        committed_workspace_write_scopes: BTreeSet::new(),
        committed_workspace_observed_evidence: Vec::new(),
        workspace_mutation_effect_sequences: BTreeSet::new(),

        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        early_tool_receipts: BTreeMap::new(),
        evaluation_resource_scopes: Vec::new(),
        evaluation_scope_rejections: 0,
        evaluation_judge_only: false,
        collaboration_started: false,
        pending_root_control_plane_requirement: None,

        root_language_replan_attempted: false,
        pending_terminal_artifact: None,
        pending_controlled_recovery_claim_fingerprints: Vec::new(),
        pending_disposition_inputs: Vec::new(),
        terminal_tracker: crate::liveness::TerminalTracker::new(
            crate::liveness::TerminalObligation::default(),
        ),
        terminal_satisfaction_since: None,
    }
}

async fn messages(host: &PublicationHost) -> Vec<ConversationMessage> {
    host.runtime
        .lock()
        .await
        .session_snapshot()
        .await
        .materialize_messages()
}

async fn assert_rejected_without_publication(host: &PublicationHost, ticket: &NodeExecutionTicket) {
    let before = messages(host).await;
    let cursor = host.services.event_store().durable_commit_cursor().unwrap();
    assert!(host.after_commit(ticket).await.is_err());
    assert_eq!(messages(host).await, before);
    assert!(host.state.lock().await.summary.is_none());
    assert_eq!(
        host.services.event_store().durable_commit_cursor().unwrap(),
        cursor
    );
}


#[test]
fn rb_machine_json_preserves_explicit_field_language_requirements() {
    let objective = "请读取文件，最终只返回 JSON 对象，path 为仓库路径。";
    assert!(!response_language_mismatch_for_role(objective, r#"{"path":"crates/runtime/Cargo.toml"}"#, false));
    let chinese = "只返回 JSON 对象；summary 字段必须使用中文。";
    assert!(response_language_mismatch_for_role(chinese, r#"{"summary":"done","path":"x"}"#, false));
    assert!(!response_language_mismatch_for_role(chinese, r#"{"summary":"完成","path":"x"}"#, false));
    assert!(response_language_mismatch_for_role("请用中文回答。", "completed", false));
}

#[tokio::test]
async fn rb_actual_api_request_recovery_visibility_and_once() {
    #[derive(Clone)]
    struct Capture(Arc<std::sync::Mutex<Vec<crate::ApiRequest>>>);
    impl ApiClient for Capture {
        fn stream(&mut self, request: crate::ApiRequest) -> Pin<Box<dyn Stream<Item = Result<crate::AssistantEvent, RuntimeError>> + Send + '_>> {
            self.0.lock().unwrap().push(request);
            Box::pin(stream::iter(vec![Ok(crate::AssistantEvent::TextDelta("ok".into())), Ok(crate::AssistantEvent::MessageStop)]))
        }
    }
    async fn capture_once(feedback_item: Option<ContextItem>) -> Vec<crate::ApiRequest> {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let original = "请只返回 JSON 对象，summary 字段使用中文。";
        let old_feedback = "Runtime terminal contract recovery: the previous presentation was rejected.";
        let content = if feedback_item.is_some() { original.to_string() } else { format!("{original}\n\n{old_feedback}") };
        let mut session = Session::new();
        session.extend_messages([ConversationMessage::user_text(original)]);
        let mut runtime = crate::ConversationRuntime::new(session, Capture(seen.clone()), NoReplayTools,
            PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly), vec!["system".into()]).without_memory();
        runtime.set_active_model("qwen3.8-max");
        runtime.begin_turn_strategy("rb-contract-visibility", original).unwrap();
        if let Some(item) = feedback_item {
            runtime.push_next_model_context_item(item);
        }
        runtime.execute_model_step(&content, false).await.unwrap();
        runtime.execute_model_step(&content, false).await.unwrap();
        let requests = seen.lock().unwrap().clone();
        requests
    }
    fn wire_text(request: &crate::ApiRequest) -> String {
        let mut sections = request.prompt.trusted_system.clone();
        sections.extend(request.prompt.contextual_messages());
        sections.push(format!("{:?}", request.messages));
        sections.join("\n")
    }
    let candidate = r#"{"summary":"done"}"#;
    let fixture = PublicationFixture::with_original_candidate(candidate);
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    {
        let mut state = host.state.lock().await;
        state.content = "请只返回 JSON 对象；summary 字段使用中文。".into();
        state.terminal_override = Some((GoalCompletion::Satisfied, candidate.into()));
    }
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    assert!(outcome.replan.is_some());
    let item = {
        let state = host.state.lock().await;
        assert_eq!(state.terminal_recovery_attempts, 1, "feedback must use the existing bounded recovery");
        state.pending_next_model_context.iter()
            .find(|item| item.id.starts_with("terminal-contract-recovery:"))
            .expect("original Host recovery feedback").clone()
    };
    assert!(item.content.contains("summary"));
    assert!(!item.content.contains(&fixture.ticket.idempotency_key));
    assert!(!item.content.contains(&fixture.session.session_id));
    let original = capture_once(None).await;
    let once = capture_once(Some(item)).await;
    assert_eq!(original.len(), 2);
    assert_eq!(once.len(), 2);
    assert!(once.iter().all(|request| wire_text(request).contains("summary 字段使用中文")),
        "the original user contract must remain in the captured request");
    let original_counts = original.iter().map(|r| wire_text(r).matches("Runtime terminal contract recovery: the previous presentation was rejected.").count()).collect::<Vec<_>>();
    let once_counts = once.iter().map(|r| wire_text(r).matches("explicit language requirement for field(s) summary").count()).collect::<Vec<_>>();
    println!("RB_ORIGINAL_CONTENT_COUNTS={original_counts:?} RB_ONE_SHOT_COUNTS={once_counts:?}");
    assert!(once_counts[0] > 0 && once_counts[1] == 0, "one-shot feedback must not persist into a later request");
}

#[tokio::test]
async fn rb_real_host_rejects_invalid_strict_terminal_before_goal_commit() {
    let candidate = "```json\n{}\n```";
    let fixture = PublicationFixture::with_original_candidate(candidate);
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    {
        let mut state = host.state.lock().await;
        state.content = "只返回 JSON 对象。".into();
        state.terminal_override = Some((GoalCompletion::Satisfied, candidate.into()));
    }
    let before = messages(&host).await;
    let goal_before = fixture.services.goal_store().get(&fixture.goal_id).unwrap().unwrap();
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    assert!(outcome.replan.is_some(), "invalid root producer must replan");
    assert!(outcome.terminal_presentation.is_none());
    assert_eq!(messages(&host).await, before);
    assert_eq!(fixture.services.goal_store().get(&fixture.goal_id).unwrap().unwrap(), goal_before);
    let state = host.state.lock().await;
    assert!(state.committed_terminal_answer.is_none());
    let feedback = state.pending_next_model_context.iter()
        .filter(|item| item.id.starts_with("terminal-contract-recovery:"))
        .collect::<Vec<_>>();
    assert_eq!(feedback.len(), 1);
    assert!(feedback[0].content.contains("not a JSON object"));
    assert!(!feedback[0].content.contains(&fixture.ticket.idempotency_key));
    assert!(!feedback[0].content.contains(&fixture.session.session_id));
    drop(state);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rb_human_answer_does_not_require_json() {
    let fixture = PublicationFixture::with_original_candidate(ORIGINAL_ANSWER);
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    assert!(outcome.replan.is_none());
    fixture.commit(&outcome);
    host.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(host.state.lock().await.summary.as_ref().unwrap().final_answer, ORIGINAL_ANSWER);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rb_goal_bound_candidate_cannot_be_replaced() {
    let fixture = PublicationFixture::with_original_candidate(r#"{"path":"original"}"#);
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    {
        let mut state = host.state.lock().await;
        state.content = "只返回 JSON 对象。".into();
        state.terminal_override = Some((GoalCompletion::Satisfied, r#"{"path":"foreign"}"#.into()));
    }
    let before = messages(&host).await;
    let result = host.synthesize(&fixture.ticket).await;
    assert!(result.is_err(), "a different answer cannot borrow the original Goal candidate");
    assert_eq!(messages(&host).await, before);
    assert!(host.state.lock().await.committed_terminal_answer.is_none());
}

#[tokio::test]
async fn real_host_471_hot_repeated_and_concurrent_after_commit_append_once() {
    let fixture = PublicationFixture::new();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    fixture.commit(&outcome);
    let (first, second) = tokio::join!(
        host.after_commit(&fixture.ticket),
        host.after_commit(&fixture.ticket)
    );
    first.unwrap();
    second.unwrap();
    let summary = host.state.lock().await.summary.clone().unwrap();
    host.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(host.state.lock().await.summary.as_ref(), Some(&summary));
    assert_eq!(
        messages(&host).await.len(),
        fixture.session.message_count() + 1
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}







#[tokio::test]
async fn real_host_471_stale_graph_commit_cannot_publish_or_acknowledge() {
    let fixture = PublicationFixture::new();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    fixture
        .services
        .commit_service()
        .transition_node(
            &fixture.graph,
            &fixture.ticket.node_id,
            ExecutionNodeStatus::Failed,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::Failed,
                ..completed_result(None, ExecutionUsage::default())
            }),
            Vec::new(),
        )
        .unwrap();
    let before = fixture
        .services
        .event_store()
        .durable_commit_cursor()
        .unwrap();
    assert!(fixture
        .services
        .commit_service()
        .transition_execution_outcome(&fixture.graph, &fixture.ticket, &outcome, &BTreeMap::new(),)
        .is_err());
    assert_eq!(
        fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap(),
        before
    );
    assert_rejected_without_publication(&host, &fixture.ticket).await;
}

#[tokio::test]
async fn real_host_471_unsatisfied_goal_proof_cannot_be_promoted_by_terminal_text() {
    let fixture = PublicationFixture::new();
    let blocked_goal_id = format!("471-unsatisfied-{}", uuid::Uuid::new_v4());
    let goal: GoalContract = serde_json::from_value(serde_json::json!({
        "id":blocked_goal_id,"session_id":fixture.session.session_id,"objective":"answer directly",
        "criteria":[{"id":"independent","statement":"requires actual proof","status":"open"}],
        "phase":"execution","spec_revision":1,"spec_digest":"471-independent",
        "scope":"internal","completion":"open","revision":1,"user_sequence":1
    }))
    .unwrap();
    fixture.services.goal_store().create(goal).unwrap();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    host.state.lock().await.goal_id = blocked_goal_id.clone();
    let before = messages(&host).await;
    let cursor = fixture
        .services
        .event_store()
        .durable_commit_cursor()
        .unwrap();
    assert!(host.synthesize(&fixture.ticket).await.is_err());
    assert_eq!(messages(&host).await, before);
    assert_eq!(
        fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap(),
        cursor
    );
    assert_eq!(
        fixture
            .services
            .goal_store()
            .get(&blocked_goal_id)
            .unwrap()
            .unwrap()
            .completion,
        GoalCompletion::Open
    );
}

#[tokio::test]
async fn synthesize_471_terminal_shape_completion_and_usage_are_not_inferred() {
    let fixture = PublicationFixture::new();
    let message =
        synthesize_message_value(&ConversationMessage::assistant(vec![ContentBlock::Text {
            text: ORIGINAL_ANSWER.into(),
        }]));
    let mut outcome = NodeExecutionOutcome::new(completed_result(
        Some(format!(
            "turn-result:{}:{:x}",
            fixture.graph.id,
            Sha256::digest(ORIGINAL_ANSWER.as_bytes())
        )),
        ExecutionUsage::default(),
    ));
    for completion in [
        GoalCompletion::Satisfied,
        GoalCompletion::Partial,
        GoalCompletion::Blocked,
        GoalCompletion::Failed,
        GoalCompletion::Cancelled,
    ] {
        assert_eq!(
            validate_synthesize_publication(
                &fixture.ticket,
                &fixture.ticket,
                &[message.clone()],
                Some(completion),
                &outcome
            )
            .unwrap(),
            Some((ORIGINAL_ANSWER.into(), completion))
        );
    }
    assert!(validate_synthesize_publication(
        &fixture.ticket,
        &fixture.ticket,
        &[message.clone()],
        None,
        &outcome
    )
    .is_err());
    for completion in [
        GoalCompletion::Open,
        GoalCompletion::WaitingExternalDecision,
    ] {
        assert!(validate_synthesize_publication(
            &fixture.ticket,
            &fixture.ticket,
            &[message.clone()],
            Some(completion),
            &outcome
        )
        .is_err());
    }
    assert!(
        validate_synthesize_publication(&fixture.ticket, &fixture.ticket, &[], None, &outcome)
            .is_err()
    );
    let mut forged_usage = message.clone();
    forged_usage["usage"] = serde_json::json!({"input_tokens":0,"output_tokens":0,
        "cache_creation_input_tokens":0,"cache_read_input_tokens":0});
    assert!(synthesize_publication_message(&forged_usage).is_err());
    outcome.result.result_ref = Some("wrong original digest".into());
    assert!(validate_synthesize_publication(
        &fixture.ticket,
        &fixture.ticket,
        &[message],
        Some(GoalCompletion::Satisfied),
        &outcome
    )
    .is_err());
}
