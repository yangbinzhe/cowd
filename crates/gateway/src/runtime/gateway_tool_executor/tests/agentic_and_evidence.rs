    #[test]
    fn evidence_retrieve_request_matches_published_query_schema() {
        let request: EvidenceRetrieveToolRequest = serde_json::from_value(serde_json::json!({
            "evidence_ref": "artifact://sha256:abc",
            "query": "acceptance evidence",
            "limit": 4
        }))
        .expect("published evidence_retrieve schema must deserialize");
        assert_eq!(request.evidence_ref, "artifact://sha256:abc");
        assert_eq!(request.query.as_deref(), Some("acceptance evidence"));
        assert_eq!(request.limit, Some(4));
        assert!(
            serde_json::from_value::<EvidenceRetrieveToolRequest>(serde_json::json!({
                "evidence_ref": "artifact://sha256:abc",
                "selector": "legacy-hidden-field"
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn logical_tool_evidence_resolves_through_session_receipt_to_opaque_artifact() {
        let temporary = tempfile::tempdir().expect("temporary Runtime root");
        let workspace = temporary.path().join("workspace");
        std::fs::create_dir_all(&workspace).expect("workspace");
        let store = Arc::new(crate::pg_test_support::session_store());
        let session_id = "session-logical-evidence";
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.to_string(),
                platform: "test".to_string(),
                chat_id: "logical-evidence".to_string(),
                user_id: None,
                model: None,
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
            .expect("create Session");
        let repository = Arc::new(
            crate::services::session_service::repository::SessionRepository::new(
                Arc::new(crate::active_session::ActiveSessionDirectory::new()),
                Some(Arc::clone(&store)),
                crate::event_bus::SessionProjectionHub::new(),
            ),
        );
        let presence = Arc::new(
            crate::services::session_service::presence::SessionPresenceLedger::with_store(
                Arc::clone(&store),
            ),
        );
        let session_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
                repository, presence,
            );
        let services = runtime::RuntimeServices::builder(
            temporary.path().join("home"),
            &workspace,
        )
        .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
        .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
        .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(
            temporary.path().join("artifacts"),
        )))
        .build()
        .expect("Runtime services");
        services
            .install_session_ports(
                session_port.clone(),
                session_port.clone(),
                session_port.clone(),
                session_port,
            )
            .expect("Session ports");
        let artifact = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".to_string(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(23),
                    original_name: Some("logical-evidence.raw".to_string()),
                },
                b"durable mapped evidence",
            )
            .await
            .expect("artifact");
        let evidence_id = "tool-raw-logical-evidence";
        for index in 0..129 {
            store
                .append_session_domain_event_allocating_sequence(
                    &session::SessionDomainEvent::new(
                        session_id,
                        0,
                        session::SessionDomainScope::Tool,
                        runtime::RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
                        serde_json::json!({"evidence_id": format!("unrelated-{index}")}),
                        index,
                    ),
                )
                .await
                .expect("unrelated evidence receipt");
        }
        store
            .append_session_domain_event_allocating_sequence(&session::SessionDomainEvent::new(
                session_id,
                0,
                session::SessionDomainScope::Tool,
                runtime::RuntimeSessionEventKind::EvidenceRawPersisted.as_str(),
                serde_json::json!({
                    "evidence_id": evidence_id,
                    "content_hash": artifact.sha256,
                    "byte_count": artifact.bytes,
                    "media_type": artifact.media_type,
                    "artifact_selector": artifact.selector,
                    "visibility_scope": artifact.visibility_scope,
                }),
                1,
            ))
            .await
            .expect("raw evidence receipt");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind Runtime services");

        let actor = root_agent_action_actor(RuntimeToolExecutionBinding {
            action_id: None,
            session_id: Some(session_id),
            authorized_scopes: &[],
            memory_context: None,
            model_lease: Some("deepseek-v4-flash"),
            parent_execution: None,
            execution_decision: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        });
        let action = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                task_ref: "task:any".to_string(),
                artifact_refs: vec!["artifact:any".to_string()],
                evidence_refs: vec![format!("tool://{evidence_id}")],
                unresolved: Vec::new(),
            },
        );
        services
            .validate_agent_action_evidence(&actor, &action)
            .await
            .expect("logical evidence is authenticated before Program mutation");

        let output = executor
            .execute_evidence_retrieve(
                EvidenceRetrieveToolRequest {
                    evidence_ref: format!("tool://{evidence_id}"),
                    query: Some("mapped".to_string()),
                    limit: Some(2),
                },
                Some(session_id),
                &["workspace:.".to_string()],
            )
            .await
            .expect("resolve logical tool evidence");

        assert!(output.contains("durable mapped evidence"), "{output}");
        assert!(output.contains("\"available\": true"), "{output}");
    }

    #[tokio::test]
    async fn bound_root_model_bootstraps_program_and_inherits_evidence_scope() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let mut graph =
            harness_contract::execution_graph::ExecutionGraph::new("root Agent-first bootstrap");
        graph.id = "graph-root-agent-first".to_string();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session-root-agent-first".to_string(),
            turn_id: "turn-root-agent-first".to_string(),
            root_task_id: "task-root-agent-first".to_string(),
            task_id: "task-root-agent-first".to_string(),
            generation: 1,
        });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel,
            "inline_model",
            "payload:root-model",
        );
        node.id = "graph-root-agent-first:model".to_string();
        node.idempotency_key = "root-model".to_string();
        graph.nodes.push(node.clone());
        services
            .commit_service()
            .register_graph(graph)
            .expect("register root graph");

        let mut decision = runtime::build_runtime_execution_decision(
            "建立1个团队，只读检查 cowd-dev/Cargo.toml",
            None,
        );
        decision.execution_graph_ref = Some("graph-root-agent-first".to_string());
        decision.session_ref = Some("session-root-agent-first".to_string());
        decision.turn_ref = Some("turn-root-agent-first".to_string());
        let parent = harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: "graph-root-agent-first".to_string(),
            node_id: node.id,
        };
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .expect("Agent action tools");
        let executor = GatewayToolExecutor::new(None, false, registry);
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let binding = RuntimeToolExecutionBinding {
            action_id: Some("bound-root-create-team"),
            session_id: Some("session-root-agent-first"),
            authorized_scopes: &[],
            memory_context: None,
            model_lease: Some("deepseek-v4-flash"),
            parent_execution: Some(&parent),
            execution_decision: Some(&decision),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        };
        let receipt = executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                serde_json::json!({
                    "name": "Evidence Team",
                    "mission": "inspect the exact admitted evidence scope"
                }),
                binding,
            )
            .await
            .expect("bound root creates Team");
        let receipt: harness_contract::agent_action::AgentActionObservation =
            serde_json::from_str(&receipt).expect("Team receipt");
        assert_eq!(
            receipt.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let program = services
            .agent_action_service()
            .project(&harness_contract::agent_action::program_id_for_objective(
                &harness_contract::agent_action::root_objective_id(
                    "session-root-agent-first",
                    "turn-root-agent-first",
                ),
            ))
            .expect("durable Program");
        assert_eq!(program.teams.len(), 1);
        assert_eq!(
            program.resource_scopes,
            vec!["read:cowd-dev/Cargo.toml".to_string()]
        );

        // A later dynamically appended model node may carry a refreshed
        // decision preview/model binding. It is the same authenticated root,
        // so Runtime must inherit the Program's first durable binding instead
        // of rejecting every subsequent small action as a mismatch.
        let mut continuation_decision = decision.clone();
        continuation_decision.user_intent_preview =
            "dynamic replan text must not replace frozen Program authority".to_string();
        continuation_decision
            .strategy
            .understanding
            .required_workspace_evidence_scopes = Vec::new();
        let continuation = RuntimeToolExecutionBinding {
            action_id: Some("bound-root-invite-agent"),
            model_lease: Some("refreshed-model-binding"),
            execution_decision: Some(&continuation_decision),
            ..binding
        };
        let team_ref = program.teams.keys().next().expect("created Team").clone();
        let receipt = executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::AGENT_INVITE_TOOL_ID,
                serde_json::json!({
                    "team_ref": team_ref,
                    "role": "Independent Reviewer",
                    "mission": "independently verify the admitted file evidence",
                    "required_capabilities": ["read"]
                }),
                continuation,
            )
            .await
            .expect("continuation inherits frozen Program binding");
        let receipt: harness_contract::agent_action::AgentActionObservation =
            serde_json::from_str(&receipt).expect("Agent receipt");
        assert_eq!(
            receipt.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let continued = services
            .agent_action_service()
            .project(&program.program_id)
            .expect("continued durable Program");
        assert_eq!(continued.agents.len(), 1);
        assert_eq!(continued.model_lease, "deepseek-v4-flash");
        assert_eq!(continued.resource_scopes, program.resource_scopes);

        let foreign = RuntimeToolExecutionBinding {
            action_id: Some("foreign-root-create-team"),
            session_id: Some("session-foreign"),
            ..binding
        };
        assert!(executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                serde_json::json!({"name":"Foreign", "mission":"must fail"}),
                foreign,
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn agent_action_gateway_commits_small_actions_and_rejects_fake_content() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .expect("Agent action tools");
        let executor = GatewayToolExecutor::new(None, false, registry);
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let binding = RuntimeToolExecutionBinding {
            action_id: Some("create-team"),
            session_id: Some("session-agentic-gateway"),
            authorized_scopes: &[],
            memory_context: None,
            model_lease: Some("default"),
            parent_execution: None,
            execution_decision: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        };
        let created = executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                serde_json::json!({
                    "name": "Evidence Team",
                    "mission": "produce independently reviewable evidence"
                }),
                binding,
            )
            .await
            .expect("create Team");
        let created: harness_contract::agent_action::AgentActionObservation =
            serde_json::from_str(&created).expect("Team receipt");
        assert_eq!(
            created.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert_eq!(created.changed_refs.len(), 1);

        let artifact = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/markdown".to_string(),
                    visibility_scope: "session:session-agentic-gateway".to_string(),
                    expected_bytes: Some(20),
                    original_name: Some("agent-report.md".to_string()),
                },
                b"durable agent report",
            )
            .await
            .expect("durable Agent content");
        let committed = executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID,
                serde_json::json!({
                    "content_ref": artifact.selector,
                    "kind": "report",
                    "title": "Durable report"
                }),
                RuntimeToolExecutionBinding {
                    action_id: Some("durable-artifact"),
                    ..binding
                },
            )
            .await
            .expect("commit durable artifact");
        let committed: serde_json::Value =
            serde_json::from_str(&committed).expect("artifact receipt");
        assert_eq!(
            committed["content_ref"].as_str(),
            Some(artifact.selector.as_str()),
            "receipt must echo the Runtime-parsed physical content_ref"
        );

        let rejected = executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::ARTIFACT_COMMIT_TOOL_ID,
                serde_json::json!({
                    "content_ref": "artifact://invented",
                    "kind": "report",
                    "title": "Invented report"
                }),
                RuntimeToolExecutionBinding {
                    action_id: Some("fake-artifact"),
                    ..binding
                },
            )
            .await
            .expect("structured rejection");
        let rejected: harness_contract::agent_action::AgentActionObservation =
            serde_json::from_str(&rejected).expect("artifact rejection");
        assert_eq!(
            rejected.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(
            rejected.error.expect("error").code,
            "artifact_content_not_durable"
        );

        let actor = root_agent_action_actor(binding);
        let direct_artifact = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                task_ref: "task:any".to_string(),
                artifact_refs: vec!["artifact:any".to_string()],
                evidence_refs: vec![artifact.selector.clone()],
                unresolved: Vec::new(),
            },
        );
        services
            .validate_agent_action_evidence(&actor, &direct_artifact)
            .await
            .expect("same-Session direct artifact evidence is readable");

        let foreign_artifact = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".to_string(),
                    visibility_scope: "session:foreign-session".to_string(),
                    expected_bytes: Some(16),
                    original_name: Some("foreign.txt".to_string()),
                },
                b"foreign evidence",
            )
            .await
            .expect("foreign artifact");
        let foreign_evidence = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                task_ref: "task:any".to_string(),
                artifact_refs: vec!["artifact:any".to_string()],
                evidence_refs: vec![foreign_artifact.selector],
                unresolved: Vec::new(),
            },
        );
        let error = services
            .validate_agent_action_evidence(&actor, &foreign_evidence)
            .await
            .expect_err("cross-Session private artifact evidence must fail closed");
        assert!(error.to_string().contains("is not readable in Session"));

        let fake_evidence = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                task_ref: "task:any".to_string(),
                artifact_refs: vec!["artifact:any".to_string()],
                evidence_refs: vec!["tool://invented-receipt".to_string()],
                unresolved: Vec::new(),
            },
        );
        let error = services
            .validate_agent_action_evidence(&actor, &fake_evidence)
            .await
            .expect_err("invented logical evidence must fail at trusted ingress");
        assert!(
            error
                .to_string()
                .contains("has no canonical durable receipt"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn committed_agent_action_is_not_relabelled_failed_when_dispatch_is_deferred() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .expect("Agent action tools");
        let executor = GatewayToolExecutor::new(None, false, registry);
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let base = RuntimeToolExecutionBinding {
            action_id: Some("deferred-team"),
            session_id: Some("session-deferred-dispatch"),
            authorized_scopes: &[],
            memory_context: None,
            model_lease: Some("deepseek-v4-flash"),
            parent_execution: None,
            execution_decision: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        };
        let team: harness_contract::agent_action::AgentActionObservation = serde_json::from_str(
            &executor
                .execute_runtime_tool_with_binding(
                    harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                    serde_json::json!({
                        "name": "Deferred Team",
                        "mission": "prove semantic and physical outcomes stay distinct"
                    }),
                    base,
                )
                .await
                .expect("create Team"),
        )
        .expect("Team receipt");
        let team_ref = team.changed_refs[0].clone();
        executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::AGENT_INVITE_TOOL_ID,
                serde_json::json!({
                    "team_ref": team_ref,
                    "role": "Domain Specialist",
                    "mission": "requires an intentionally unavailable catalog capability",
                    "required_capabilities": ["custom_domain_operation"]
                }),
                RuntimeToolExecutionBinding {
                    action_id: Some("deferred-agent"),
                    ..base
                },
            )
            .await
            .expect("invite remains a semantic action");
        let published: harness_contract::agent_action::AgentActionObservation =
            serde_json::from_str(
                &executor
                    .execute_runtime_tool_with_binding(
                        harness_contract::agent_action::TASK_PUBLISH_TOOL_ID,
                        serde_json::json!({
                            "team_ref": team_ref,
                            "title": "Unavailable physical work",
                            "objective": "exercise deferred dispatch",
                            "acceptance": "durable Task remains visible",
                            "required_capabilities": ["custom_domain_operation"],
                            "depends_on": []
                        }),
                        RuntimeToolExecutionBinding {
                            action_id: Some("deferred-task"),
                            ..base
                        },
                    )
                    .await
                    .expect("durable action must return an applied receipt"),
            )
            .expect("Task receipt");

        assert_eq!(
            published.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert!(published.actionable.iter().any(|item| {
            item.contains("Semantic action committed") && item.contains("dispatch is deferred")
        }));
        let projection = services
            .agent_action_service()
            .project(&published.program_id)
            .expect("durable Program");
        assert_eq!(projection.tasks.len(), 1);
    }

    #[test]
    fn successful_gateway_file_fact_has_typed_identity_and_digest() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        std::fs::write(services.workspace_root().join("identity.txt"), "checked")
            .expect("workspace fixture");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "typed-receipt".to_string(),
                tool_name: "read_file".to_string(),
                input: r#"{"path":"identity.txt"}"#.to_string(),
                depends_on: Vec::new(),
            },
        );
        request.category = runtime::ToolSafetyCategory::ReadOnly;
        request.governed_plan_revision = 7;
        request.observation_wave_sequence = 7;
        let output = serde_json::json!({
            "type": "text",
            "truncated": false,
            "file": {
                "filePath": services.workspace_root().join("identity.txt"),
                "content": "checked",
                "numLines": 1,
                "startLine": 1,
                "totalLines": 1,
                "sha256": format!("{:x}", Sha256::digest(b"checked")),
                "byteLength": 7
            }
        })
        .to_string();
        let observed = gateway_observed_evidence(
            &executor,
            &request,
            &output,
            "gateway-tool:test:7:typed-receipt",
        );
        assert_eq!(observed.len(), 1);
        assert_eq!(observed[0].observed_at_sequence, 7);
        assert!(matches!(
            &observed[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.path.workspace_relative_path == "identity.txt"
                    && scope.path.observed_revision_or_digest.is_some()
        ));
        request.tool_name = "unrelated_tool".to_string();
        assert!(gateway_observed_evidence(
            &executor,
            &request,
            &output,
            "gateway-tool:test:7:unrelated",
        )
        .is_empty());
    }

    #[test]
    fn gateway_windowed_read_never_mints_exact_content_evidence() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        std::fs::write(
            services.workspace_root().join("windowed.txt"),
            "one\ntwo\nthree",
        )
        .expect("workspace fixture");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "windowed-read".to_string(),
                tool_name: "read_file".to_string(),
                input: r#"{"path":"windowed.txt","offset":1,"limit":1}"#.to_string(),
                depends_on: Vec::new(),
            },
        );
        request.category = runtime::ToolSafetyCategory::ReadOnly;
        let base = serde_json::json!({
            "type": "text",
            "file": {
                "filePath": services.workspace_root().join("windowed.txt"),
                "content": "two",
                "numLines": 1,
                "startLine": 2,
                "totalLines": 3,
                "sha256": format!("{:x}", Sha256::digest(b"one\ntwo\nthree")),
                "byteLength": 13
            }
        });
        let mut truncated = base.clone();
        truncated["truncated"] = serde_json::Value::Bool(true);
        assert!(gateway_observed_evidence(
            &executor,
            &request,
            &truncated.to_string(),
            "gateway-tool:test:windowed-truncated",
        )
        .is_empty());

        let mut offset = base;
        offset["truncated"] = serde_json::Value::Bool(false);
        assert!(gateway_observed_evidence(
            &executor,
            &request,
            &offset.to_string(),
            "gateway-tool:test:windowed-offset",
        )
        .is_empty());
    }

    #[test]
    fn successful_gateway_edit_fact_hashes_exact_result_without_rereading_disk() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        std::fs::write(
            services.workspace_root().join("edit.txt"),
            "alpha beta alpha",
        )
        .expect("workspace fixture");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "typed-edit-receipt".to_string(),
                tool_name: "edit_file".to_string(),
                input: r#"{"path":"edit.txt"}"#.to_string(),
                depends_on: Vec::new(),
            },
        );
        request.category = runtime::ToolSafetyCategory::WriteLocal;
        request.governed_plan_revision = 9;
        request.observation_wave_sequence = 9;
        let output = serde_json::json!({
            "filePath": services.workspace_root().join("edit.txt"),
            "oldString": "alpha",
            "newString": "omega",
            "originalFile": "alpha beta alpha",
            "structuredPatch": [],
            "userModified": false,
            "replaceAll": false,
            "gitDiff": null
        })
        .to_string();
        std::fs::write(services.workspace_root().join("edit.txt"), "different now")
            .expect("concurrent fixture");

        let observed = gateway_observed_evidence(
            &executor,
            &request,
            &output,
            "gateway-tool:test:9:typed-edit-receipt",
        );
        let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
            &observed[0].target
        else {
            panic!("workspace evidence expected");
        };
        let expected = format!("{:x}", Sha256::digest(b"omega beta alpha"));
        assert_eq!(
            scope.path.observed_revision_or_digest.as_deref(),
            Some(expected.as_str())
        );
        assert_eq!(
            observed[0].workspace_prior_state,
            Some(harness_contract::context::WorkspacePriorState::Existing {
                sha256: format!("{:x}", Sha256::digest(b"alpha beta alpha")),
            })
        );
    }

    #[test]
    fn gateway_write_receipt_distinguishes_attested_absence_from_existing_preimage() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "typed-write-receipt".to_string(),
                tool_name: "write_file".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 10;

        let created = serde_json::json!({
            "type": "create",
            "filePath": services.workspace_root().join("created.txt"),
            "content": "created",
            "originalFile": null
        });
        let created =
            gateway_observed_evidence(&executor, &request, &created.to_string(), "created");
        assert_eq!(
            created[0].workspace_prior_state,
            Some(harness_contract::context::WorkspacePriorState::Absent)
        );

        let updated = serde_json::json!({
            "type": "update",
            "filePath": services.workspace_root().join("updated.txt"),
            "content": "after",
            "originalFile": "before"
        });
        let updated =
            gateway_observed_evidence(&executor, &request, &updated.to_string(), "updated");
        assert_eq!(
            updated[0].workspace_prior_state,
            Some(harness_contract::context::WorkspacePriorState::Existing {
                sha256: format!("{:x}", Sha256::digest(b"before")),
            })
        );

        let ambiguous = serde_json::json!({
            "type": "create",
            "filePath": services.workspace_root().join("ambiguous.txt"),
            "content": "after",
            "originalFile": "unreadable-is-not-absent"
        });
        assert!(gateway_observed_evidence(
            &executor,
            &request,
            &ambiguous.to_string(),
            "ambiguous",
        )
        .is_empty());
    }

    #[test]
    fn compact_write_receipt_keeps_typed_workspace_evidence_after_body_elision() {
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind runtime services");
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: "compact-write-receipt".to_string(),
                tool_name: "write_file".to_string(),
                input: "{}".to_string(),
                depends_on: Vec::new(),
            },
        );
        request.observation_wave_sequence = 11;

        // This is the canonical model-facing receipt emitted after the full
        // adapter response is intentionally elided from the conversation.
        let content = "created through compact receipt";
        let output = serde_json::json!({
            "operation": "create",
            "path": services.workspace_root().join("compact-created.txt"),
            "content_bytes": content.len(),
            "content_sha256": format!("{:x}", Sha256::digest(content.as_bytes())),
            "prior_bytes": 0,
            "prior_sha256": null,
            "replacement_count": 0,
            "replace_all": true,
        });
        let observed =
            gateway_observed_evidence(&executor, &request, &output.to_string(), "compact-create");

        assert_eq!(observed.len(), 1);
        assert!(matches!(
            &observed[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                if scope.access_mode == harness_contract::context::WorkspaceAccessMode::Write
                    && scope.path.workspace_relative_path == "compact-created.txt"
        ));
        assert_eq!(
            observed[0].workspace_prior_state,
            Some(harness_contract::context::WorkspacePriorState::Absent)
        );

        let mut malformed = output;
        malformed["content_sha256"] = serde_json::json!("not-a-digest");
        assert!(gateway_observed_evidence(
            &executor,
            &request,
            &malformed.to_string(),
            "compact-malformed",
        )
        .is_empty());
    }

    use serde_json::json;
    use tools::permissions::PermissionMode as ToolPermissionMode;
    use tools::RuntimeToolDefinition;
