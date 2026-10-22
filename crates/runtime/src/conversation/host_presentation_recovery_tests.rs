use super::*;

mod pre_effect_admission_555 {
    use super::*;
    use crate::execution_core::NodeExecutor;

    struct PinnedTools555 {
        lease: tools::ToolHostLease,
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
            if self.preflight.is_some() {
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
        let policy = PermissionPolicy::new(if matches!(case, Case555::NativePreflightRefusal | Case555::BodyFailureBlockedWrite | Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain) {
            harness_contract::policy::PermissionMode::WorkspaceWrite
        } else {
            harness_contract::policy::PermissionMode::ReadOnly
        });
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
        let lease = host.host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| {
                matches!(definition.name.as_str(), "workspace_snapshot" | "read_file")
                    || (matches!(case, Case555::NativePreflightRefusal | Case555::BodyFailureBlockedWrite | Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain)
                        && definition.name == "edit")
            })
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            advertised.len(),
            if matches!(case, Case555::NativePreflightRefusal | Case555::BodyFailureBlockedWrite | Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain) {
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
            runner.drain_physical_execution().await;
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
            Case555::OutsideRoot | Case555::Mixed | Case555::NativePreflightRefusal
                | Case555::AuthorizationGap | Case555::MixedAuthorizationGap
                | Case555::BodyFailureBlockedWrite | Case555::ScopeRefusalBlockedWrite | Case555::BodyFailureBlockedChain
        );
        assert_eq!(
            services.commit_service()
                .authenticated_no_dispatch_sources(&committed, &node.id, &binding)
                .unwrap().is_some(),
            refused,
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
        let expected_body_calls =
            if matches!(case, Case555::OutsideRoot | Case555::NativePreflightRefusal | Case555::AuthorizationGap | Case555::ScopeRefusalBlockedWrite) {
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
        let before = runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages();
        services
            .tool_batch_executor()
            .after_commit(&ticket)
            .await
            .unwrap();
        assert_eq!(
            runtime
                .lock()
                .await
                .session_snapshot()
                .await
                .materialize_messages(),
            before
        );
        runner.recover_graph(&registered.id).await.unwrap();
        assert_eq!(host.entered.load(Ordering::SeqCst), expected_body_calls);
        assert!(!root.join("denied.txt").exists());
        assert!(!root.join("transitive.txt").exists());
        assert_eq!(provider_calls.load(Ordering::SeqCst), 0);
        runner.drain_physical_execution().await;
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

const S01_PROMPT_518: &str =
    "只回答 7 乘以 8 的结果。不要调用工具、创建团队、创建任务或写文件，不要附加解释。\n";

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

struct CallbackPhases518 {
    before_publication: tokio::sync::Semaphore,
    allow_publication: tokio::sync::Semaphore,
    after_publication: tokio::sync::Semaphore,
    allow_callback_return: tokio::sync::Semaphore,
    returned: std::sync::atomic::AtomicBool,
    model_ticket: std::sync::Mutex<Option<NodeExecutionTicket>>,
    synthesize_observations:
        std::sync::Mutex<Vec<(NodeExecutionTicket, Vec<ConversationMessage>, bool, bool)>>,
}

impl CallbackPhases518 {
    fn new() -> Self {
        Self {
            before_publication: tokio::sync::Semaphore::new(0),
            allow_publication: tokio::sync::Semaphore::new(0),
            after_publication: tokio::sync::Semaphore::new(0),
            allow_callback_return: tokio::sync::Semaphore::new(0),
            returned: std::sync::atomic::AtomicBool::new(false),
            model_ticket: std::sync::Mutex::new(None),
            synthesize_observations: std::sync::Mutex::new(Vec::new()),
        }
    }
}

struct UnpauseOnDrop518(Arc<CallbackPhases518>);
impl Drop for UnpauseOnDrop518 {
    fn drop(&mut self) {
        self.0.allow_publication.add_permits(16);
        self.0.allow_callback_return.add_permits(16);
    }
}

struct PausedModel518 {
    inner: Arc<dyn crate::execution_core::graph::executors::ScopedNodeBackend>,
    phases: Arc<CallbackPhases518>,
}

#[async_trait::async_trait]
impl crate::execution_core::graph::executors::ScopedNodeBackend for PausedModel518 {
    fn retains_physical_execution(&self) -> bool {
        self.inner.retains_physical_execution()
    }
    async fn execute(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, NodeExecutorError> {
        *self.phases.model_ticket.lock().unwrap() = Some(ticket.clone());
        self.inner.execute(ticket).await
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), NodeExecutorError> {
        self.phases.before_publication.add_permits(1);
        self.phases
            .allow_publication
            .acquire()
            .await
            .unwrap()
            .forget();
        self.inner.after_commit(ticket).await?;
        self.phases.after_publication.add_permits(1);
        self.phases
            .allow_callback_return
            .acquire()
            .await
            .unwrap()
            .forget();
        self.phases.returned.store(true, Ordering::SeqCst);
        Ok(())
    }
}

struct PausedModelResolver518 {
    original: TurnModelResolver<S01Client518, NoReplayTools>,
    phases: Arc<CallbackPhases518>,
}

impl crate::execution_core::graph::executors::ScopedNodeBackendResolver for PausedModelResolver518 {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<Arc<dyn crate::execution_core::graph::executors::ScopedNodeBackend>> {
        self.original.resolve(ticket).map(|inner| {
            Arc::new(PausedModel518 {
                inner,
                phases: self.phases.clone(),
            }) as Arc<dyn crate::execution_core::graph::executors::ScopedNodeBackend>
        })
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

struct ObservedSynthesize518 {
    inner: Arc<dyn SynthesizeBackend>,
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<S01Client518, NoReplayTools>>>,
    services: Arc<crate::RuntimeServices>,
    phases: Arc<CallbackPhases518>,
}

#[async_trait::async_trait]
impl SynthesizeBackend for ObservedSynthesize518 {
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let prefix = self
            .runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages();
        let original = self.phases.model_ticket.lock().unwrap().clone().unwrap();
        let closed = original_model_closed518(&self.services, &original);
        self.phases.synthesize_observations.lock().unwrap().push((
            ticket.clone(),
            prefix,
            self.phases.returned.load(Ordering::SeqCst),
            closed,
        ));
        self.inner.synthesize(ticket).await
    }
    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        self.inner.after_commit(ticket).await
    }
    async fn after_abort(&self, ticket: &NodeExecutionTicket, reason: &str) -> Result<(), String> {
        self.inner.after_abort(ticket, reason).await
    }
}

struct ObservedSynthesizeResolver518 {
    original: TurnSynthesizeResolver<S01Client518, NoReplayTools>,
    runtime: Arc<tokio::sync::Mutex<crate::ConversationRuntime<S01Client518, NoReplayTools>>>,
    services: Arc<crate::RuntimeServices>,
    phases: Arc<CallbackPhases518>,
}

impl crate::execution_core::graph::executors::SynthesizeBackendResolver
    for ObservedSynthesizeResolver518
{
    fn resolve(&self, ticket: &NodeExecutionTicket) -> Option<Arc<dyn SynthesizeBackend>> {
        self.original.resolve(ticket).map(|inner| {
            Arc::new(ObservedSynthesize518 {
                inner,
                runtime: self.runtime.clone(),
                services: self.services.clone(),
                phases: self.phases.clone(),
            }) as Arc<dyn SynthesizeBackend>
        })
    }
}

async fn runner_successor_probe518(
    runner: &crate::execution_core::graph::ExecutionGraphRunner,
    graph_id: &str,
    model_ticket: &NodeExecutionTicket,
) -> bool {
    let snapshot = runner
        .pump_snapshot(graph_id, &BTreeSet::from([model_ticket.node_id.clone()]))
        .await
        .unwrap();
    let prepared = runner
        .prepare_ready_wave(graph_id, snapshot.ready)
        .await
        .unwrap();
    prepared.is_empty()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_host_518_runner_model_publication_and_original_closure_precede_synthesize_staging() {
    let fixture = PublicationFixture::new();
    let (_unused, store, ingress) =
        claimed_ingress_host_with_prompt(&fixture, false, S01_PROMPT_518).await;
    let fence = crate::SessionExecutionFence::from_claim(
        crate::session_runtime_port::TestSessionPortAdapter::new(store),
        &ingress.request_id,
        &ingress.session_id,
        ingress.session_generation,
        usize::try_from(ingress.input_sequence).unwrap(),
        &ingress.claim_owner,
        &ingress.claim_token,
    )
    .unwrap();
    let mut session = fixture.session.clone();
    session.truncate_messages(0);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut runtime = crate::ConversationRuntime::new(
        session,
        S01Client518(calls.clone()),
        NoReplayTools,
        PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
        vec![S01_PROMPT_518.into()],
    )
    .without_memory()
    .with_runtime_event_store(fixture.services.event_store().clone())
    .with_artifact_store(fixture.artifacts.clone())
    .with_cowd_event_bus(crate::CowdEventBus::new())
    .with_session_execution_fence(fence)
    .with_model_context_window(262_144);
    runtime.set_active_model("518-deterministic-model");
    let mut graph = ExecutionGraph::new(S01_PROMPT_518);
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: fixture.session.session_id.clone(),
        turn_id: "471-turn".into(),
        root_task_id: ingress.root_task_id.clone(),
        task_id: ingress.primary_task_id.clone(),
        generation: 1,
    });
    let mut model = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        serde_json::json!({"session_id":ingress.session_id,"turn_id":ingress.turn_id,
            "content":S01_PROMPT_518,"objective":S01_PROMPT_518.trim(),"kind":"conversation_turn",
            "compile_target":"inline_model","ingress":ingress})
        .to_string(),
    );
    model.id = format!("{}:model", graph.id);
    model.idempotency_key = format!("{}:original", model.id);
    graph.nodes.push(model);
    runtime
        .begin_turn_strategy("471-turn", S01_PROMPT_518)
        .unwrap();
    let strategy = runtime
        .bind_turn_strategy_execution("471-turn", &graph.id)
        .unwrap();
    assert!(strategy.decision.executable);
    runtime.session_input_stream().set_active_turn(Some(
        harness_contract::turn::TurnId::from_string("471-turn"),
    ));
    let mut state = publication_state(&fixture.session.session_id, &fixture.goal_id);
    state.content = S01_PROMPT_518.into();
    state.task_understanding = Some(strategy.decision.strategy.understanding.clone());
    state.collaboration_obligation = strategy.decision.collaboration_obligation.clone();
    state.context_window = runtime.model_context_window();
    state.safety_lease = crate::execution_core::SafetyFusePolicy::derive(
        state.context_window,
        strategy.decision.complexity(),
        None,
    );
    let original_ingress = ingress.clone();
    state.ingress = Some(ingress);
    let runtime = Arc::new(tokio::sync::Mutex::new(runtime));
    let state = Arc::new(tokio::sync::Mutex::new(state));
    let phases = Arc::new(CallbackPhases518::new());
    let _unpause = UnpauseOnDrop518(phases.clone());
    fixture
        .services
        .model_step_executor()
        .install_resolver(Arc::new(PausedModelResolver518 {
            original: TurnModelResolver {
                session_id: fixture.session.session_id.clone(),
                graph_id: graph.id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
                services: Arc::downgrade(&fixture.services),
            },
            phases: phases.clone(),
        }));
    fixture
        .services
        .synthesize_executor()
        .install_resolver(Arc::new(ObservedSynthesizeResolver518 {
            original: TurnSynthesizeResolver {
                session_id: fixture.session.session_id.clone(),
                graph_id: graph.id.clone(),
                runtime: Arc::downgrade(&runtime),
                state: Arc::downgrade(&state),
                services: Arc::downgrade(&fixture.services),
            },
            runtime: runtime.clone(),
            services: fixture.services.clone(),
            phases: phases.clone(),
        }));
    let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
        fixture.services.executor_registry().clone(),
        fixture.services.graph_state_store().clone(),
        fixture.services.commit_service().clone(),
        fixture.services.resource_manager().clone(),
        fixture.services.scope_locks().clone(),
        fixture.services.worktree_leases().clone(),
        fixture.services.workspace_key(),
        fixture.services.workspace_root(),
    );
    let registered = runner.register(graph).await.unwrap();
    let driver_runner = runner.clone();
    let graph_id = registered.id.clone();
    let driver = tokio::spawn(async move { driver_runner.run_until_quiescent(&graph_id).await });
    let wait = std::time::Duration::from_secs(10);
    tokio::time::timeout(wait, phases.before_publication.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let model_ticket = phases.model_ticket.lock().unwrap().clone().unwrap();
    let before = tokio::time::timeout(
        wait,
        runner_successor_probe518(&runner, &registered.id, &model_ticket),
    )
    .await;
    let staged_before = phases.synthesize_observations.lock().unwrap().len();
    let empty_before = runtime
        .lock()
        .await
        .session_snapshot()
        .await
        .message_count()
        == 0;
    let closed_before = original_model_closed518(&fixture.services, &model_ticket);
    let completed_before = fixture
        .services
        .graph_state_store()
        .load(&registered.id)
        .unwrap()
        .node_statuses
        .get(&model_ticket.node_id)
        == Some(&ExecutionNodeStatus::Completed);
    phases.allow_publication.add_permits(1);
    tokio::time::timeout(wait, phases.after_publication.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let published = runtime
        .lock()
        .await
        .session_snapshot()
        .await
        .materialize_messages();
    let during = tokio::time::timeout(
        wait,
        runner_successor_probe518(&runner, &registered.id, &model_ticket),
    )
    .await;
    let staged_during = phases.synthesize_observations.lock().unwrap().len();
    let closed_during = original_model_closed518(&fixture.services, &model_ticket);
    let completed_during = fixture
        .services
        .graph_state_store()
        .load(&registered.id)
        .unwrap()
        .node_statuses
        .get(&model_ticket.node_id)
        == Some(&ExecutionNodeStatus::Completed);
    phases.allow_callback_return.add_permits(1);
    let result = tokio::time::timeout(std::time::Duration::from_secs(60), driver)
        .await
        .unwrap()
        .unwrap();
    runner.drain_physical_execution().await;
    assert!(
        before.unwrap(),
        "successor admitted before original model publication"
    );
    assert!(
        during.unwrap(),
        "successor admitted before original callback/closure"
    );
    assert!(empty_before);
    assert_eq!(staged_before, 0);
    assert_eq!(staged_during, 0);
    assert!(!closed_before && !closed_during);
    assert!(completed_before && completed_during);
    let report = result.unwrap();
    assert_eq!(report.failed, 0);
    assert_eq!(report.blocked, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let model_document = fixture
        .services
        .commit_service()
        .committed_host_transcript(&model_ticket)
        .unwrap();
    assert_eq!(model_document.prefix_count, 0);
    assert_eq!(
        model_document.messages,
        published
            .iter()
            .map(synthesize_message_value)
            .collect::<Vec<_>>()
    );
    assert_eq!(published.len(), 2);
    assert_eq!(published[0], ConversationMessage::user_text(S01_PROMPT_518));
    assert!(matches!(published[1].blocks.as_slice(), [
        ContentBlock::Thinking { signature: Some(signature), .. }, ContentBlock::Text { text }
    ] if signature == "518-original-signature" && text == "56"));
    assert_eq!(
        published[1].usage,
        Some(model_protocol::usage::TokenUsage {
            input_tokens: 3189,
            output_tokens: 57,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        })
    );
    let observations = phases.synthesize_observations.lock().unwrap().clone();
    assert_eq!(observations.len(), 1);
    let (terminal_ticket, terminal_prefix, callback_returned, closure_verified) = &observations[0];
    assert!(*callback_returned && *closure_verified);
    assert_eq!(*terminal_prefix, published);
    let document = fixture
        .services
        .commit_service()
        .committed_host_transcript(terminal_ticket)
        .unwrap();
    assert_eq!(document.prefix_count, 2);
    assert_eq!(
        document.prefix_sha256,
        synthesize_prefix_digest(&published).unwrap()
    );
    assert_eq!(document.messages[0]["usage"], serde_json::Value::Null);
    assert!(original_model_closed518(&fixture.services, &model_ticket));
    assert!(original_model_closed518(&fixture.services, terminal_ticket));
    assert!(runner
        .active_binding_for_ticket_for_test(&model_ticket)
        .await
        .is_none());
    assert!(runner
        .active_binding_for_ticket_for_test(terminal_ticket)
        .await
        .is_none());
    assert_eq!(
        runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages(),
        published
    );
    assert_eq!(
        state.lock().await.summary.as_ref().unwrap().final_answer,
        "56"
    );
    let replay = TurnSynthesizeBackend {
        runtime: runtime.clone(),
        state: state.clone(),
        services: fixture.services.clone(),
    };
    let original_terminal = fixture
        .services
        .event_store()
        .session_terminal_for_execution(&fixture.session.session_id, &registered.id, "471-turn")
        .unwrap()
        .unwrap();
    assert_eq!(
        original_terminal.request_id.as_deref(),
        Some(original_ingress.request_id.as_str())
    );
    assert_eq!(
        original_terminal.input_claim_token.as_deref(),
        Some(original_ingress.claim_token.as_str())
    );
    replay.after_commit(terminal_ticket).await.unwrap();
    assert_eq!(
        runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages(),
        published
    );
    let cold_services = fixture.restart_services();
    let mut cold_session = runtime.lock().await.session_snapshot().await;
    cold_session.extend_messages([ConversationMessage::user_text("a later turn")]);
    let expected = cold_session.materialize_messages();
    let cold_runtime = crate::ConversationRuntime::new(
        cold_session,
        S01Client518(calls.clone()),
        NoReplayTools,
        PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
        vec![S01_PROMPT_518.into()],
    )
    .without_memory()
    .with_runtime_event_store(cold_services.event_store().clone())
    .with_artifact_store(fixture.artifacts.clone());
    cold_runtime
        .begin_turn_strategy("471-turn", S01_PROMPT_518)
        .unwrap();
    cold_runtime
        .bind_turn_strategy_execution("471-turn", &registered.id)
        .unwrap();
    let cold = TurnSynthesizeBackend {
        runtime: Arc::new(tokio::sync::Mutex::new(cold_runtime)),
        state: Arc::new(tokio::sync::Mutex::new(publication_state(
            &fixture.session.session_id,
            &fixture.goal_id,
        ))),
        services: cold_services,
    };
    cold.after_commit(terminal_ticket).await.unwrap();
    cold.after_commit(terminal_ticket).await.unwrap();
    assert_eq!(
        cold.runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages(),
        expected
    );
    assert_eq!(fixture.services.event_store().session_terminal_for_execution(
        &fixture.session.session_id, &registered.id, "471-turn",
    ).unwrap(), Some(original_terminal));
    let cursor = fixture
        .services
        .event_store()
        .durable_commit_cursor()
        .unwrap();
    let mut wrong = published.clone();
    wrong[0] = ConversationMessage::user_text("different original user, same final answer");
    cold.runtime
        .lock()
        .await
        .session_mut_async()
        .await
        .replace_messages(wrong.clone());
    {
        let owner = cold.runtime.lock().await;
        owner
            .begin_turn_strategy("471-turn", S01_PROMPT_518)
            .unwrap();
        owner
            .bind_turn_strategy_execution("471-turn", &registered.id)
            .unwrap();
    }
    cold.state.lock().await.summary = None;
    let error = cold.after_commit(terminal_ticket).await.unwrap_err();
    assert!(error.contains("prefix"), "{error}");
    assert_eq!(
        cold.runtime
            .lock()
            .await
            .session_snapshot()
            .await
            .materialize_messages(),
        wrong
    );
    assert!(cold.state.lock().await.summary.is_none());
    assert_eq!(
        fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap(),
        cursor
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn real_host_471_original_terminal_survives_native_claim_recovery_without_reauthorizing() {
    let fixture = PublicationFixture::new();
    let (host, store, ingress) = claimed_ingress_host(&fixture, false).await;
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    fixture.commit(&outcome);
    let terminal_id = format!("turn-terminal:{}", ingress.request_id);
    let original = fixture
        .services
        .event_store()
        .session_terminal(&terminal_id)
        .unwrap()
        .unwrap();
    let recovery_time = crate::tool_invocation::now_ms().saturating_add(60_001);
    let reclaimed = store
        .claim_session_runtime_outbox("471-recovery-worker", recovery_time, 60_000, 1, &[])
        .await
        .unwrap()
        .remove(0);
    assert_eq!(reclaimed.request_id, ingress.request_id);
    let token = reclaimed.claim_token.clone().unwrap();
    assert_ne!(token, ingress.claim_token);
    let running = store
        .mark_session_runtime_outbox_running(
            &ingress.request_id,
            "471-recovery-worker",
            ingress.session_generation,
            &token,
            reclaimed.revision,
            recovery_time,
        )
        .await
        .unwrap();
    let fence = crate::SessionExecutionFence::from_claim(
        crate::session_runtime_port::TestSessionPortAdapter::new(store),
        &ingress.request_id,
        &ingress.session_id,
        ingress.session_generation,
        running.sequence,
        "471-recovery-worker",
        &token,
    )
    .unwrap();
    let snapshot = fence
        .verify(crate::SessionExecutionFencePhase::TerminalCommit)
        .await
        .unwrap();
    let adopted = fixture
        .services
        .event_store()
        .adopt_session_terminal_fence(
            &crate::runtime_event_store::RuntimeSessionTerminalFenceAdoption {
                terminal_id: terminal_id.clone(),
                expected_terminal_revision: original.revision,
                request_id: snapshot.request_id,
                session_id: snapshot.session_id,
                turn_id: ingress.turn_id.clone(),
                session_generation: snapshot.session_generation,
                input_sequence: running.sequence as u64,
                claim_owner: snapshot.claim_owner,
                claim_token: snapshot.claim_token,
                claim_revision: snapshot.claim_fence_epoch,
                claim_expires_at_ms: running.claim_expires_at_ms.unwrap(),
                adopted_at_ms: recovery_time,
            },
        )
        .unwrap();
    assert_eq!(adopted.payload_ref, original.payload_ref);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(
        cold.state
            .lock()
            .await
            .summary
            .as_ref()
            .unwrap()
            .final_answer,
        ORIGINAL_ANSWER
    );
    assert_eq!(
        cold.services
            .event_store()
            .session_terminal(&terminal_id)
            .unwrap(),
        Some(adopted)
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
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
            let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
            cold.after_commit(&fixture.ticket).await.unwrap();
            cold.after_commit(&fixture.ticket).await.unwrap();
            assert_eq!(
                cold.state
                    .lock()
                    .await
                    .summary
                    .as_ref()
                    .unwrap()
                    .final_answer,
                ORIGINAL_ANSWER
            );
            assert_eq!(
                messages(&cold).await.len(),
                fixture.session.message_count() + 1
            );
            assert_eq!(
                fixture
                    .services
                    .goal_store()
                    .projection(&fixture.goal_id)
                    .unwrap()
                    .unwrap(),
                committed
            );
        }
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_host_507_committed_publication_replays_after_independent_goal_progress() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    let saved_session = cold.runtime.lock().await.session_snapshot().await;
    let saved_messages = saved_session.materialize_messages();
    let original_document = fixture
        .services
        .commit_service()
        .committed_host_transcript(&fixture.ticket)
        .unwrap();
    let goal = fixture
        .services
        .goal_store()
        .projection(&fixture.goal_id)
        .unwrap()
        .unwrap();
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
            "test.507.after_original_atomic_terminal",
            goal.stream_revision,
            "Independent progress after the original committed Host terminal".into(),
            "507-post-commit-goal-progress".into(),
            ObservationResultClass::Informational,
        ))
        .unwrap();
    let expected_goal = fixture
        .services
        .goal_store()
        .projection(&fixture.goal_id)
        .unwrap()
        .unwrap();
    assert_eq!(expected_goal.goal, goal.goal);
    assert!(expected_goal.stream_revision > goal.stream_revision);
    let replay = fixture.host(fixture.restart_services(), saved_session, false);
    replay.after_commit(&fixture.ticket).await.unwrap();
    let cursor = fixture
        .services
        .event_store()
        .durable_commit_cursor()
        .unwrap();
    replay.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(
        fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap(),
        cursor
    );
    assert_eq!(messages(&replay).await, saved_messages);
    assert_eq!(
        replay
            .state
            .lock()
            .await
            .summary
            .as_ref()
            .unwrap()
            .final_answer,
        ORIGINAL_ANSWER
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
    assert_eq!(
        serde_json::to_value(
            fixture
                .services
                .commit_service()
                .committed_host_transcript(&fixture.ticket)
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(original_document).unwrap()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

fn install_original_gap_program(
    fixture: &PublicationFixture,
    request_completion: bool,
) -> (String, String) {
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActionStatus, AgentActorBinding, AgentActorKind,
        MessagePublishInput, ObjectiveCompleteRequestInput,
    };
    let objective_id =
        harness_contract::agent_action::root_objective_id(&fixture.session.session_id, "471-turn");
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let actor = AgentActorBinding {
        objective_id: objective_id.clone(),
        program_id: program_id.clone(),
        session_id: fixture.session.session_id.clone(),
        turn_id: "471-turn".into(),
        root_execution_id: Some(fixture.graph.id.clone()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "answer directly".into(),
        model_lease: "507b-fixture-model".into(),
        permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
        resource_scopes: vec![format!("session:{}", fixture.session.session_id)],
        actor_id: format!("root:{}", fixture.session.session_id),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let goal_id = format!("goal:{}", fixture.graph.id);
    let goal: GoalContract = serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":fixture.session.session_id,"objective":"answer directly",
        "criteria":[{"id":"user_intent","statement":"original independent review required",
            "status":"open","source_refs":["session_message:original"],
            "required_evidence":[format!("execution_graph:{}", fixture.graph.id)]}],
        "phase":"execution","spec_revision":1,"spec_digest":"507b-original-open-contract",
        "scope":"user_objective","completion":"open","revision":1,"user_sequence":1,
        "source_intent_ref":"session_message:original","user_intent_criterion_id":"user_intent",
        "execution_binding":{"objective_id":objective_id,"session_id":fixture.session.session_id,
            "turn_id":"471-turn","root_execution_id":fixture.graph.id,
            "agentic_program_id":program_id}
    }))
    .unwrap();
    fixture.services.goal_store().create(goal).unwrap();
    let opened = fixture
        .services
        .agent_action_service()
        .apply(&AgentActionEnvelope {
            action_id: "507b-original-program-open".into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::MessagePublish(MessagePublishInput {
                topic_ref: format!("topic:{program_id}"),
                summary: Some("Original work has an independently unreviewed result".into()),
                content_ref: None,
                refs: Vec::new(),
                recipients: Vec::new(),
                intent: None,
                issue_dispositions: Vec::new(),
            }),
        })
        .unwrap();
    assert_eq!(opened.status, AgentActionStatus::Applied);
    if request_completion {
        let request = fixture
            .services
            .agent_action_service()
            .apply(&AgentActionEnvelope {
                action_id: "507b-original-completion-request".into(),
                actor,
                expected_revision: None,
                action: AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: vec!["tool://507b-pending-unreviewed-result".into()],
                    evidence_refs: Vec::new(),
                    unresolved: Vec::new(),
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                }),
            })
            .unwrap();
        assert_eq!(request.status, AgentActionStatus::Applied);
        assert_eq!(
            fixture
                .services
                .agent_action_service()
                .project(&program_id)
                .unwrap()
                .status,
            crate::AgenticProgramStatus::CompletionRequested,
        );
    }
    let original = fixture
        .services
        .goal_store()
        .projection(&goal_id)
        .unwrap()
        .unwrap();
    let state = publication_state(&fixture.session.session_id, &goal_id);
    let identity = runtime_observation_identity(&fixture.services, &state, &fixture.ticket);
    fixture
        .services
        .goal_store()
        .record_observation(runtime_observation(
            identity,
            RuntimeObservationKind::GraphProgress,
            "test.507b.before_drive_stream_progress",
            original.stream_revision,
            "Original Goal source advanced without changing its semantic contract".into(),
            "507b-before-drive-source-progress".into(),
            ObservationResultClass::Informational,
        ))
        .unwrap();
    let advanced = fixture
        .services
        .goal_store()
        .projection(&goal_id)
        .unwrap()
        .unwrap();
    assert_eq!(advanced.goal, original.goal);
    assert!(advanced.stream_revision > advanced.goal.revision);
    (goal_id, program_id)
}

#[tokio::test]
async fn real_host_507b_real_gap_after_drive_preserves_all_original_goal_cuts() {
    for same_goal in [false, true] {
        for race_target in ["none", "program_goal", "state_goal"] {
            if same_goal && race_target == "state_goal" {
                continue;
            }
            let fixture = PublicationFixture::new();
            let (program_goal_id, program_id) = install_original_gap_program(&fixture, true);
            let before_drive = fixture
                .services
                .goal_store()
                .projection(&program_goal_id)
                .unwrap()
                .unwrap();
            let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
            if same_goal {
                hot.state.lock().await.goal_id = program_goal_id.clone();
            }
            let state_goal_id = hot.state.lock().await.goal_id.clone();
            assert_eq!(same_goal, state_goal_id == program_goal_id);
            let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
            let after_drive = fixture
                .services
                .goal_store()
                .projection(&program_goal_id)
                .unwrap()
                .unwrap();
            assert!(after_drive.stream_revision > before_drive.stream_revision);
            assert!(after_drive.goal.revision > before_drive.goal.revision);
            assert!(after_drive.stream_revision > after_drive.goal.revision);
            assert_eq!(after_drive.goal.completion, GoalCompletion::Open);
            assert_eq!(
                after_drive.goal.waiting.as_ref().unwrap().reason_code,
                "completion_gap"
            );
            assert!(hot.state.lock().await.completion_gap_outstanding);
            assert!(outcome
                .result
                .result_ref
                .as_ref()
                .unwrap()
                .ends_with(":completion-gap-continuation"));
            let replan = outcome
                .replan
                .as_ref()
                .expect("real gap returned the original continuation");
            assert!(replan
                .reason
                .starts_with("Objective completion gap remains;"));
            assert_eq!(replan.nodes.len(), 1);
            assert_eq!(replan.nodes[0].kind, ExecutionNodeKind::InlineModel);
            assert_eq!(
                outcome
                    .source_revisions
                    .get(&format!("goal:{program_goal_id}")),
                Some(&after_drive.stream_revision)
            );
            let state_goal = fixture
                .services
                .goal_store()
                .projection(&state_goal_id)
                .unwrap()
                .unwrap();
            assert_eq!(
                outcome
                    .source_revisions
                    .get(&format!("goal:{state_goal_id}")),
                Some(&state_goal.stream_revision)
            );
            assert_eq!(
                outcome
                    .source_revisions
                    .get(&format!("agentic-program:{program_id}")),
                Some(
                    &fixture
                        .services
                        .agent_action_service()
                        .program_revision(&program_id)
                        .unwrap()
                ),
            );
            let document = outcome
                .domain_events
                .iter()
                .find(|event| event.event.kind == "execution.host.transcript")
                .unwrap();
            assert_eq!(document.event.actor.as_deref(), Some("synthesize"));
            if race_target == "none" {
                fixture.commit(&outcome);
                let stored = fixture
                    .services
                    .commit_service()
                    .committed_host_transcript(&fixture.ticket)
                    .unwrap();
                assert!(stored.messages.is_empty());
                assert_eq!(stored.terminal_completion, None);
                assert_eq!(
                    fixture
                        .services
                        .goal_store()
                        .projection(&program_goal_id)
                        .unwrap()
                        .unwrap(),
                    after_drive
                );
                let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
                cold.state.lock().await.goal_id = state_goal_id;
                cold.after_commit(&fixture.ticket).await.unwrap();
                cold.after_commit(&fixture.ticket).await.unwrap();
                assert!(cold.state.lock().await.summary.is_none());
                assert_eq!(
                    messages(&cold).await,
                    fixture.session.materialize_messages()
                );
            } else {
                let raced_id = if race_target == "program_goal" {
                    &program_goal_id
                } else {
                    &state_goal_id
                };
                let original = fixture
                    .services
                    .goal_store()
                    .projection(raced_id)
                    .unwrap()
                    .unwrap();
                let mut identity = {
                    let state = hot.state.lock().await;
                    runtime_observation_identity(&fixture.services, &state, &fixture.ticket)
                };
                identity.goal_id = raced_id.clone();
                fixture
                    .services
                    .goal_store()
                    .record_observation(runtime_observation(
                        identity,
                        RuntimeObservationKind::GraphProgress,
                        "test.507b.independent_gap_goal_progress",
                        original.stream_revision,
                        "Independent progress changed only the consumed Goal stream source".into(),
                        "507b-gap-goal-progress".into(),
                        ObservationResultClass::Informational,
                    ))
                    .unwrap();
                let advanced = fixture
                    .services
                    .goal_store()
                    .projection(raced_id)
                    .unwrap()
                    .unwrap();
                assert_eq!(advanced.goal, original.goal);
                assert_eq!(advanced.goal.revision, original.goal.revision);
                assert!(advanced.stream_revision > original.stream_revision);
                let source_id = format!("goal:{raced_id}");
                let captured = fixture
                    .services
                    .commit_service()
                    .execution_outcome_sources(&fixture.graph, &fixture.ticket, &outcome)
                    .unwrap();
                assert_eq!(captured.get(&source_id), Some(&original.stream_revision));
                let cursor = fixture
                    .services
                    .event_store()
                    .durable_commit_cursor()
                    .unwrap();
                let graph_events = fixture
                    .services
                    .event_store()
                    .list_stream(&fixture.graph.id)
                    .unwrap();
                let goal_events = fixture
                    .services
                    .event_store()
                    .list_stream(&source_id)
                    .unwrap();
                let effect_id = ExecutionNodeBinding::effect_stream_id(&fixture.ticket);
                let effect_events = fixture
                    .services
                    .event_store()
                    .list_stream(&effect_id)
                    .unwrap();
                let error = fixture
                    .services
                    .commit_service()
                    .transition_execution_outcome(
                        &fixture.graph,
                        &fixture.ticket,
                        &outcome,
                        &BTreeMap::new(),
                    )
                    .err()
                    .expect("original Goal source must reject the stale outcome");
                assert!(
                    matches!(
                        error,
                        crate::execution_core::graph::ExecutionCommitError::EventStore(
                            crate::RuntimeEventStoreError::StaleRevision { ref stream_id, expected, actual }
                        ) if stream_id == &source_id
                            && expected == original.stream_revision
                            && actual == advanced.stream_revision
                    ),
                    "{error}"
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
                    graph_events
                );
                assert_eq!(
                    fixture
                        .services
                        .event_store()
                        .list_stream(&source_id)
                        .unwrap(),
                    goal_events
                );
                assert_eq!(
                    fixture
                        .services
                        .event_store()
                        .list_stream(&effect_id)
                        .unwrap(),
                    effect_events
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
                assert_rejected_without_publication(&hot, &fixture.ticket).await;
            }
            assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
        }
    }
}

#[tokio::test]
async fn real_host_507b_real_closure_rung_replan_keeps_both_goal_sources() {
    let fixture = PublicationFixture::new();
    let (program_goal_id, program_id) = install_original_gap_program(&fixture, false);
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
    assert!(outcome
        .result
        .result_ref
        .as_ref()
        .unwrap()
        .ends_with(":closure-rung-continuation"));
    assert!(outcome
        .replan
        .as_ref()
        .unwrap()
        .reason
        .starts_with("Program-owned rungs remain;"));
    for goal_id in [&fixture.goal_id, &program_goal_id] {
        let goal = fixture
            .services
            .goal_store()
            .projection(goal_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            outcome.source_revisions.get(&format!("goal:{goal_id}")),
            Some(&goal.stream_revision)
        );
    }
    assert_eq!(
        outcome
            .source_revisions
            .get(&format!("agentic-program:{program_id}")),
        Some(
            &fixture
                .services
                .agent_action_service()
                .program_revision(&program_id)
                .unwrap()
        ),
    );
    fixture.commit(&outcome);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert!(cold.state.lock().await.summary.is_none());
    assert_eq!(
        messages(&cold).await,
        fixture.session.materialize_messages()
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

struct RejectOnceContextJournal {
    inner: Arc<dyn crate::SessionRuntimeJournalPort>,
    reject: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::SessionRuntimeJournalPort for RejectOnceContextJournal {
    async fn append_event(
        &self,
        event: &crate::session_runtime_port::RuntimeSessionEvent,
    ) -> Result<crate::session_runtime_port::RuntimeSessionEventReceipt, session::SessionError>
    {
        if self.reject.swap(false, Ordering::SeqCst) {
            return Err(session::SessionError::Store(
                "471 original journal publication fault".into(),
            ));
        }
        self.inner.append_event(event).await
    }

    async fn append_context_envelope_if_absent(
        &self,
        record: &crate::session_runtime_port::RuntimeContextEnvelopeRecord,
    ) -> Result<
        Option<crate::session_runtime_port::RuntimeSessionEventReceipt>,
        session::SessionError,
    > {
        self.inner.append_context_envelope_if_absent(record).await
    }

    async fn append_compaction_bundle_if_absent(
        &self,
        events: &[crate::session_runtime_port::RuntimeSessionEvent],
        checkpoint_id: &str,
    ) -> Result<bool, session::SessionError> {
        self.inner
            .append_compaction_bundle_if_absent(events, checkpoint_id)
            .await
    }
}

#[tokio::test]
async fn real_host_471_failed_summary_publication_leaves_session_unchanged_and_retries_original() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
    let store = Arc::new(crate::test_support::session_store());
    store
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
    let services = fixture.restart_services();
    let journal = Arc::new(RejectOnceContextJournal {
        inner: crate::session_runtime_port::TestSessionPortAdapter::new(store),
        reject: std::sync::atomic::AtomicBool::new(true),
    });
    let runtime = PublicationRuntime::new(
        fixture.session.clone(),
        NoReplayClient(fixture.calls.clone()),
        NoReplayTools,
        PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
        vec!["answer directly".into()],
    )
    .without_memory()
    .with_runtime_event_store(services.event_store().clone())
    .with_artifact_store(fixture.artifacts.clone())
    .with_session_journal_port(journal);
    runtime
        .begin_turn_strategy("471-turn", "answer directly")
        .unwrap();
    runtime
        .bind_turn_strategy_execution("471-turn", &fixture.graph.id)
        .unwrap();
    let cold = PublicationHost {
        runtime: Arc::new(tokio::sync::Mutex::new(runtime)),
        state: Arc::new(tokio::sync::Mutex::new(publication_state(
            &fixture.session.session_id,
            &fixture.goal_id,
        ))),
        services,
    };
    assert_rejected_without_publication(&cold, &fixture.ticket).await;
    cold.after_commit(&fixture.ticket).await.unwrap();
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(
        messages(&cold).await.len(),
        fixture.session.message_count() + 1
    );
    assert_eq!(
        cold.state
            .lock()
            .await
            .summary
            .as_ref()
            .unwrap()
            .final_answer,
        ORIGINAL_ANSWER
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
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
async fn real_host_471_missing_original_controlled_recovery_record_cannot_ack_hot_claims() {
    let fixture = PublicationFixture::new();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let mut outcome = host.synthesize(&fixture.ticket).await.unwrap();
    let original = crate::authorization_negotiator::controlled_recovery_terminal_event(
        &crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
            recovery_scope: "turn:471-turn".into(),
            session_id: fixture.session.session_id.clone(),
            turn_id: "471-turn".into(),
            execution_id: fixture.graph.id.clone(),
            fingerprints: Vec::new(),
        },
    )
    .unwrap();
    outcome
        .domain_events
        .retain(|event| event.event.kind != original.event.kind);
    fixture.commit(&outcome);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    assert_rejected_without_publication(&cold, &fixture.ticket).await;
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

#[tokio::test]
async fn real_host_471_missing_or_foreign_strategy_is_rejected_before_session_append() {
    for strategy_turn in [None, Some("foreign-turn")] {
        let fixture = PublicationFixture::new();
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
        let services = fixture.restart_services();
        let runtime = PublicationRuntime::new(
            fixture.session.clone(),
            NoReplayClient(fixture.calls.clone()),
            NoReplayTools,
            PermissionPolicy::new(harness_contract::policy::PermissionMode::ReadOnly),
            vec!["answer directly".into()],
        )
        .without_memory()
        .with_runtime_event_store(services.event_store().clone())
        .with_artifact_store(fixture.artifacts.clone());
        if let Some(turn) = strategy_turn {
            runtime
                .begin_turn_strategy(turn, "answer directly")
                .unwrap();
        }
        let cold = PublicationHost {
            runtime: Arc::new(tokio::sync::Mutex::new(runtime)),
            state: Arc::new(tokio::sync::Mutex::new(publication_state(
                &fixture.session.session_id,
                &fixture.goal_id,
            ))),
            services,
        };
        assert_rejected_without_publication(&cold, &fixture.ticket).await;
    }
}

#[tokio::test]
async fn real_host_471_canonical_own_usage_is_restored_without_descendant_or_provider_replay() {
    for own_observed in [true, false] {
        let fixture = PublicationFixture::new();
        fixture.services.record_live_execution(
            &fixture.session.session_id,
            fixture.graph.id.clone(),
            "471-turn".into(),
        );
        let bus = crate::CowdEventBus::new();
        let mut surface_relay = bus.subscribe();
        let _lease = fixture
            .services
            .bind_active_execution_bus(fixture.graph.id.clone(), bus.clone())
            .unwrap();
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: fixture.graph.id.clone(),
            session_id: fixture.session.session_id.clone(),
            turn_id: "471-turn".into(),
        });
        bus.emit(CowdEvent::ExecutionPhase {
            status: harness_contract::projection::ExecutionLiveStatus::CallingModel,
            detail: Some("Original admitted model usage fixture".into()),
        });
        let original_usage = crate::RunModelTelemetry {
            model: Some("fixture-original-model".into()),
            models_used: vec!["fixture-original-model".into()],
            input_tokens: 19,
            output_tokens: 7,
            cache_create_tokens: 3,
            cache_read_tokens: 5,
            total_tokens: 34,
            usage_source: "provider_actual".into(),
            cache_dimensions_known: true,
            ..unknown_recovered_terminal_telemetry()
        };
        if own_observed {
            bus.emit(CowdEvent::RunModelTelemetry {
                telemetry: original_usage.clone(),
            });
        }
        let child_id = format!("{}:original-descendant", fixture.graph.id);
        let child_bus = crate::CowdEventBus::new();
        child_bus.forward_to(
            &bus,
            crate::CowdExecutionLineage {
                parent_execution_id: fixture.graph.id.clone(),
                graph_id: fixture.graph.id.clone(),
                node_id: "fixture-original-child".into(),
                team_id: None,
                agent_id: None,
            },
        );
        let child_usage = crate::RunModelTelemetry {
            model: Some("fixture-descendant-model".into()),
            models_used: vec!["fixture-descendant-model".into()],
            input_tokens: 101,
            output_tokens: 103,
            cache_create_tokens: 0,
            cache_read_tokens: 0,
            total_tokens: 204,
            usage_source: "provider_actual".into(),
            cache_dimensions_known: true,
            ..unknown_recovered_terminal_telemetry()
        };
        {
            let _child_scope = child_bus.enter_execution(crate::CowdExecutionContext {
                execution_id: child_id.clone(),
                session_id: fixture.session.session_id.clone(),
                turn_id: "471-turn".into(),
            });
            child_bus.emit(CowdEvent::RunModelTelemetry {
                telemetry: child_usage.clone(),
            });
        }
        bus.emit(CowdEvent::ExecutionPhase {
            status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
            detail: Some("Original Synthesize publication boundary".into()),
        });
        let expected_own = own_observed.then(|| original_usage.clone());
        let observed = fixture
            .services
            .execution_model_usage_snapshot(&fixture.graph.id)
            .expect("synchronous bus usage observation before Surface relay");
        assert_eq!(observed.own, expected_own);
        assert_eq!(observed.descendants.get(&child_id), Some(&child_usage));
        assert_eq!(
            fixture
                .services
                .execution_live(&fixture.graph.id)
                .unwrap()
                .status,
            harness_contract::projection::ExecutionLiveStatus::Queued,
            "the synchronous telemetry observer must not consume lifecycle phases"
        );
        let projection_id = format!("execution-live:{}", fixture.graph.id);
        let before_relay = fixture
            .services
            .event_store()
            .projection_checkpoint(&projection_id)
            .unwrap()
            .expect("original execution registration checkpoint");
        let mut relayed_phases = 0;
        let mut skipped_telemetry = 0;
        for _ in 0..(3 + usize::from(own_observed)) {
            let event = surface_relay
                .try_recv()
                .expect("all original bus events must reach the Surface relay");
            if !bus.has_live_telemetry_observer()
                || !matches!(event.domain_event(), CowdEvent::RunModelTelemetry { .. })
            {
                assert!(matches!(
                    event.domain_event(),
                    CowdEvent::ExecutionPhase { .. }
                ));
                assert_eq!(
                    event.execution_context().unwrap().execution_id,
                    fixture.graph.id
                );
                fixture
                    .services
                    .observe_live_execution_event(&fixture.session.session_id, &event);
                relayed_phases += 1;
            } else {
                skipped_telemetry += 1;
            }
        }
        assert!(matches!(
            surface_relay.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
        assert_eq!(relayed_phases, 2);
        assert_eq!(skipped_telemetry, 1 + usize::from(own_observed));
        let checkpoint = fixture
            .services
            .event_store()
            .projection_checkpoint(&projection_id)
            .unwrap()
            .expect("original Finalizing phase must checkpoint observed usage");
        assert!(checkpoint.revision > before_relay.revision);
        assert_eq!(
            checkpoint.payload["own_model_usage"],
            serde_json::to_value(&expected_own).unwrap()
        );
        assert_eq!(
            checkpoint.payload["descendant_model_usage"][&child_id],
            serde_json::to_value(&child_usage).unwrap()
        );
        assert_eq!(
            fixture
                .services
                .execution_live(&fixture.graph.id)
                .unwrap()
                .status,
            harness_contract::projection::ExecutionLiveStatus::Finalizing
        );
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
        let restarted = fixture.restart_services();
        let persisted = restarted
            .execution_model_usage_snapshot(&fixture.graph.id)
            .expect("original execution usage checkpoint");
        assert_eq!(persisted.own, own_observed.then(|| original_usage.clone()));
        assert_eq!(persisted.descendants.get(&child_id), Some(&child_usage));
        let expected = if own_observed {
            original_usage
        } else {
            unknown_recovered_terminal_telemetry()
        };
        let cold = fixture.host(restarted, fixture.session.clone(), false);
        cold.after_commit(&fixture.ticket).await.unwrap();
        cold.after_commit(&fixture.ticket).await.unwrap();
        let summary = cold.state.lock().await.summary.clone().unwrap();
        assert_eq!(summary.model_telemetry, expected);
        assert_eq!(summary.usage.input_tokens as u64, expected.input_tokens);
        assert_eq!(summary.usage.output_tokens as u64, expected.output_tokens);
        assert_eq!(
            summary.usage.cache_creation_input_tokens as u64,
            expected.cache_create_tokens
        );
        assert_eq!(
            summary.usage.cache_read_input_tokens as u64,
            expected.cache_read_tokens
        );
        assert_eq!(summary.final_answer, ORIGINAL_ANSWER);
        assert_eq!(
            messages(&cold).await.len(),
            fixture.session.message_count() + 1
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
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
async fn real_host_471_ingress_keeps_original_claim_terminal_body_and_single_outbox() {
    let fixture = PublicationFixture::new();
    let (host, _store, ingress) = claimed_ingress_host(&fixture, false).await;
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    let requested = outcome
        .domain_events
        .iter()
        .find(|input| input.event.kind == "runtime.session.terminal_requested")
        .unwrap();
    let terminal: crate::runtime_event_store::SessionTerminalInput =
        serde_json::from_value(requested.event.payload.clone()).unwrap();
    assert_eq!(
        terminal.input_claim_token.as_deref(),
        Some(ingress.claim_token.as_str())
    );
    assert_eq!(
        terminal.input_claim_owner.as_deref(),
        Some(ingress.claim_owner.as_str())
    );
    assert_eq!(terminal.input_claim_revision, Some(ingress.claim_revision));
    assert_eq!(
        terminal.session_generation,
        Some(ingress.session_generation)
    );
    assert_eq!(terminal.input_sequence, Some(ingress.input_sequence));
    let artifact = crate::decode_session_terminal_artifact_ref(&terminal.payload_ref).unwrap();
    let origin = crate::PreparedArtifactOrigin::TerminalBody {
        terminal_id: terminal.terminal_id.clone(),
        session_id: ingress.session_id.clone(),
        execution_id: fixture.graph.id.clone(),
        turn_id: ingress.turn_id.clone(),
        event_stream: requested.event.stream_id.clone(),
        event_key: requested.idempotency_key.clone().unwrap(),
    };
    let bytes = fixture
        .artifacts
        .read_prepared_blocking(&artifact, &artifact.visibility_scope, &origin)
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["text"], ORIGINAL_ANSWER);
    fixture.commit(&outcome);
    let original = fixture
        .services
        .event_store()
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .unwrap();
    assert_eq!(original.status, "pending");
    drop(host);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(
        cold.services
            .event_store()
            .session_terminal(&terminal.terminal_id)
            .unwrap(),
        Some(original)
    );
    assert_eq!(
        cold.state
            .lock()
            .await
            .summary
            .as_ref()
            .unwrap()
            .final_answer,
        ORIGINAL_ANSWER
    );
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
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

#[tokio::test]
async fn real_host_471_missing_original_prepared_blob_fails_without_empty_reconstruction() {
    let fixture = PublicationFixture::new();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    host.state.lock().await.terminal_override =
        Some((GoalCompletion::Satisfied, "x".repeat(300_000)));
    let outcome = host.synthesize(&fixture.ticket).await.unwrap();
    let event = outcome
        .domain_events
        .iter()
        .find(|input| input.event.kind == "execution.host.transcript")
        .unwrap();
    let artifact: harness_contract::context::ArtifactRef =
        serde_json::from_value(event.event.payload["body"]["body"]["artifact"].clone()).unwrap();
    let object = fixture
        .artifacts
        .repository()
        .object(&artifact.sha256)
        .unwrap()
        .unwrap();
    assert_eq!(object.tier, crate::ArtifactObjectTier::Blob);
    fixture.commit(&outcome);
    let digest = artifact.sha256.strip_prefix("sha256:").unwrap();
    let path = fixture
        .root
        .join("artifacts")
        .join("objects")
        .join(&digest[..2])
        .join(digest)
        .join(object.generation);
    std::fs::remove_file(path).unwrap();
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    assert_rejected_without_publication(&cold, &fixture.ticket).await;
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn real_host_471_exact_non_satisfied_completion_survives_without_new_provider_attempt() {
    for completion in [
        GoalCompletion::Partial,
        GoalCompletion::Blocked,
        GoalCompletion::Failed,
        GoalCompletion::Cancelled,
    ] {
        let fixture = PublicationFixture::new();
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        {
            let mut state = hot.state.lock().await;
            state.execution_role = crate::TurnExecutionRole::DelegatedLeaf;
            state.terminal_override = Some((completion, ORIGINAL_ANSWER.into()));
        }
        let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
        fixture.commit(&outcome);
        let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
        cold.state.lock().await.execution_role = crate::TurnExecutionRole::DelegatedLeaf;
        cold.after_commit(&fixture.ticket).await.unwrap();
        cold.after_commit(&fixture.ticket).await.unwrap();
        let summary = cold.state.lock().await.summary.clone().unwrap();
        assert_eq!(summary.terminal_completion, completion);
        assert_eq!(summary.final_answer, ORIGINAL_ANSWER);
        assert_eq!(summary.assistant_messages.len(), 1);
        assert_eq!(summary.model_telemetry.usage_source, "unknown_recovered");
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
    for (completion, role) in [
        (GoalCompletion::Open, crate::TurnExecutionRole::RootTurn),
        (
            GoalCompletion::Open,
            crate::TurnExecutionRole::DelegatedLeaf,
        ),
        (
            GoalCompletion::WaitingExternalDecision,
            crate::TurnExecutionRole::RootTurn,
        ),
        (
            GoalCompletion::WaitingExternalDecision,
            crate::TurnExecutionRole::DelegatedLeaf,
        ),
    ] {
        let fixture = PublicationFixture::new();
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        {
            let mut state = hot.state.lock().await;
            state.execution_role = role;
            state.terminal_override = Some((completion, ORIGINAL_ANSWER.into()));
        }
        let cursor = fixture
            .services
            .event_store()
            .durable_commit_cursor()
            .unwrap();
        let before = messages(&hot).await;
        let original_goal = fixture
            .services
            .goal_store()
            .projection(&fixture.goal_id)
            .unwrap()
            .unwrap();
        let error = hot.synthesize(&fixture.ticket).await.unwrap_err();
        assert!(error.contains("immutable Goal completion"), "{error}");
        assert_eq!(
            fixture
                .services
                .event_store()
                .durable_commit_cursor()
                .unwrap(),
            cursor
        );
        assert_eq!(messages(&hot).await, before);
        assert_eq!(
            fixture
                .services
                .goal_store()
                .projection(&fixture.goal_id)
                .unwrap()
                .unwrap(),
            original_goal
        );
        assert!(hot.state.lock().await.summary.is_none());
        assert!(hot.state.lock().await.terminal_commit_owner.is_none());
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
        assert_rejected_without_publication(&hot, &fixture.ticket).await;
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_host_471_original_honest_partial_replan_has_explicit_empty_publication() {
    let fixture = PublicationFixture::new();
    let host = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let mut outcome = host
        .honest_partial_terminal(&fixture.ticket, "independent review is pending", 3)
        .await;
    host.stage_terminal_publication(&fixture.ticket, &mut outcome)
        .await
        .unwrap();
    fixture.commit(&outcome);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert!(cold.state.lock().await.summary.is_none());
    assert_eq!(
        messages(&cold).await,
        fixture.session.materialize_messages()
    );
    let publication = cold
        .services
        .commit_service()
        .committed_host_transcript(&fixture.ticket)
        .unwrap();
    assert_eq!(publication.terminal_completion, None);
    assert!(publication.messages.is_empty());
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

    fn restart_services(&self) -> Arc<crate::RuntimeServices> {
        Self::services_at(&self.root, self.backend.clone(), self.artifacts.clone())
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
        committed_terminal_answer: None,
        committed_terminal_completion: None,
        last_verified_progress: false,
        consecutive_unverified_model_steps: 0,
        closure_ready_steps: 0,
        previous_objective_milestones: Default::default(),
        verified_answer_ready_steps: 0,
        final_answer_observed: false,
        collaboration_convergence_replans: 0,
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
        required_write_for_completion: false,
        required_workspace_write_scopes: Vec::new(),
        committed_workspace_write_observed: false,
        committed_workspace_write_scopes: BTreeSet::new(),
        committed_workspace_observed_evidence: Vec::new(),
        workspace_mutation_effect_sequences: BTreeSet::new(),
        required_write_replans: 0,
        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        early_tool_receipts: BTreeMap::new(),
        evaluation_resource_scopes: Vec::new(),
        evaluation_scope_rejections: 0,
        evaluation_judge_only: false,
        collaboration_started: false,
        pending_root_control_plane_requirement: None,
        root_write_replans: 0,
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

#[tokio::test]
async fn real_host_471_cli_original_terminal_recovers_after_owner_restart() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
    fixture.commit(&outcome);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    let original_document = fixture
        .services
        .commit_service()
        .committed_host_transcript(&fixture.ticket)
        .unwrap();
    assert_eq!(
        original_document.terminal_completion,
        Some(GoalCompletion::Satisfied)
    );
    assert_eq!(
        original_document.messages[0]["usage"],
        serde_json::Value::Null
    );
    assert!(fixture
        .services
        .event_store()
        .session_terminal_for_execution(&fixture.session.session_id, &fixture.graph.id, "471-turn",)
        .unwrap()
        .is_none());
    drop(hot);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    assert!(cold.state.lock().await.terminal_commit_owner.is_none());
    cold.after_commit(&fixture.ticket).await.unwrap();
    let summary = cold.state.lock().await.summary.clone().unwrap();
    assert_eq!(summary.final_answer, ORIGINAL_ANSWER);
    assert_eq!(summary.terminal_completion, GoalCompletion::Satisfied);
    assert_eq!(summary.model_telemetry.usage_source, "unknown_recovered");
    assert!(!summary.model_telemetry.cache_dimensions_known);
    assert_eq!(summary.model_telemetry.active_stream_duration_ms, None);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        messages(&cold).await.len(),
        fixture.session.message_count() + 1
    );
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
async fn real_host_471_recovered_existing_terminal_and_later_suffix_are_not_duplicated() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.after_commit(&fixture.ticket).await.unwrap();
    let mut durable_session = cold.runtime.lock().await.session_snapshot().await;
    durable_session.extend_messages([ConversationMessage::user_text("a later turn")]);
    let expected = durable_session.materialize_messages();
    drop(cold);
    let restarted = fixture.host(fixture.restart_services(), durable_session, false);
    restarted.after_commit(&fixture.ticket).await.unwrap();
    restarted.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(messages(&restarted).await, expected);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn real_host_471_existing_provider_answer_retains_actual_usage_without_duplicate() {
    for with_thinking in [false, true] {
        let fixture = PublicationFixture::new();
        let mut session = fixture.session.clone();
        let mut blocks = Vec::new();
        if with_thinking {
            blocks.push(ContentBlock::Thinking {
                thinking: "original provider metadata".into(),
                signature: None,
            });
        }
        blocks.push(ContentBlock::Text {
            text: ORIGINAL_ANSWER.into(),
        });
        let original = ConversationMessage::assistant_with_usage(
            blocks,
            Some(model_protocol::usage::TokenUsage {
                input_tokens: 17,
                output_tokens: 9,
                cache_creation_input_tokens: 3,
                cache_read_input_tokens: 5,
            }),
        );
        session.extend_messages([original.clone()]);
        let hot = fixture.host(fixture.services.clone(), session.clone(), true);
        fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
        let cold = fixture.host(fixture.restart_services(), session, false);
        cold.after_commit(&fixture.ticket).await.unwrap();
        assert_eq!(messages(&cold).await.last(), Some(&original));
        assert_eq!(
            messages(&cold).await.len(),
            fixture.session.message_count() + 1
        );
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_host_471_input_barrier_empty_publication_recovers_without_terminal() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    let stream = hot.runtime.lock().await.session_input_stream();
    let turn = TurnId::from_string("471-turn");
    stream.set_active_turn(Some(turn.clone()));
    let input = SessionInputEnvelope::text(
        &fixture.session.session_id,
        harness_contract::turn::InputSourceKind::Webui,
        "new original input",
    );
    let mut receipt = stream.admit(
        input.clone(),
        crate::input_classifier::RuntimeInputState::active(turn),
    );
    receipt.cursor = Some(harness_contract::turn::SessionInputCursor::new(1, 1));
    stream.project_durable(input, receipt);
    let outcome = hot.synthesize(&fixture.ticket).await.unwrap();
    assert!(outcome.replan.is_some());
    fixture.commit(&outcome);
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    let before = messages(&cold).await;
    cold.after_commit(&fixture.ticket).await.unwrap();
    cold.after_commit(&fixture.ticket).await.unwrap();
    assert_eq!(messages(&cold).await, before);
    assert!(cold.state.lock().await.summary.is_none());
    let publication = cold
        .services
        .commit_service()
        .committed_host_transcript(&fixture.ticket)
        .unwrap();
    assert!(publication.messages.is_empty());
    assert_eq!(publication.terminal_completion, None);
    assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn real_host_471_uncommitted_or_missing_original_publication_never_succeeds() {
    for commit_without_publication in [false, true] {
        let fixture = PublicationFixture::new();
        let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
        let mut outcome = hot.synthesize(&fixture.ticket).await.unwrap();
        if commit_without_publication {
            outcome
                .domain_events
                .retain(|event| event.event.kind != "execution.host.transcript");
            outcome
                .result
                .evidence_refs
                .retain(|reference| !reference.retrieval_selector.starts_with("host-transcript:"));
            fixture.commit(&outcome);
        }
        let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
        assert_rejected_without_publication(&cold, &fixture.ticket).await;
        assert_eq!(fixture.calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn real_host_471_wrong_ticket_session_turn_and_prefix_reject_without_writes() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
    for mutation in [
        "attempt", "payload", "key", "node", "graph", "executor", "session", "turn", "prefix",
    ] {
        let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
        let mut ticket = fixture.ticket.clone();
        match mutation {
            "attempt" => ticket.attempt += 1,
            "payload" => ticket.payload_ref = "foreign".into(),
            "key" => ticket.idempotency_key.push_str(":foreign"),
            "node" => ticket.node_id.push_str(":foreign"),
            "graph" => ticket.graph_id.push_str(":foreign"),
            "executor" => ticket.executor_kind = "inline_model".into(),
            "session" => cold.state.lock().await.session_id.push_str(":foreign"),
            "turn" => cold.state.lock().await.turn_id.push_str(":foreign"),
            "prefix" => cold
                .runtime
                .lock()
                .await
                .session_mut_async()
                .await
                .truncate_messages(0),
            _ => unreachable!(),
        }
        assert_rejected_without_publication(&cold, &ticket).await;
    }
}

#[tokio::test]
async fn real_host_471_divergent_published_segment_and_hot_cache_reject_without_writes() {
    let fixture = PublicationFixture::new();
    let hot = fixture.host(fixture.services.clone(), fixture.session.clone(), true);
    fixture.commit(&hot.synthesize(&fixture.ticket).await.unwrap());
    hot.state.lock().await.committed_terminal_answer = Some("forged cache".into());
    assert_rejected_without_publication(&hot, &fixture.ticket).await;
    let cold = fixture.host(fixture.restart_services(), fixture.session.clone(), false);
    cold.runtime
        .lock()
        .await
        .session_mut_async()
        .await
        .extend_messages([ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "foreign terminal".into(),
        }])]);
    assert_rejected_without_publication(&cold, &fixture.ticket).await;
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
