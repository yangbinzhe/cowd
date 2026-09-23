    #[test]
    fn evidence_retrieve_request_matches_published_query_schema() {
        let request: EvidenceRetrieveToolRequest = serde_json::from_value(serde_json::json!({
            "evidence_ref": "artifact://sha256:abc",
            "query": "acceptance evidence",
            "cursor": "opaque-page-cursor",
            "limit": 4
        }))
        .expect("published evidence_retrieve schema must deserialize");
        assert_eq!(request.evidence_ref, "artifact://sha256:abc");
        assert_eq!(request.query.as_deref(), Some("acceptance evidence"));
        assert_eq!(request.limit, Some(4));
        assert_eq!(request.cursor.as_deref(), Some("opaque-page-cursor"));
        assert!(
            serde_json::from_value::<EvidenceRetrieveToolRequest>(serde_json::json!({
                "evidence_ref": "artifact://sha256:abc",
                "selector": "legacy-hidden-field"
            }))
            .is_err()
        );
    }

    #[tokio::test]
    async fn external_decision_evidence_uses_runtime_source_and_authenticated_pagination() {
        use harness_contract::execution_graph::*;
        use harness_contract::policy::*;
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let mut graph = ExecutionGraph::new("external decision Gateway");
        graph.lineage = Some(ExecutionGraphLineage {
            session_id:"decision-session".into(), turn_id:"decision-turn".into(),
            root_task_id:"decision-task".into(), task_id:"decision-task".into(), generation:1,
        });
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::Approval, "approval", "decision source");
        let reference = runtime::execution_core::graph::executors::graph_approval_id(&graph.id, &node.id);
        graph.nodes.push(node);
        services.commit_service().register_graph(graph).unwrap();
        let source = ApprovalSource {kind:ApprovalSourceKind::Session, session_id:Some("decision-session".into()),
            agent_id:None, team_id:None, mission_id:None, resource_ref:None, review_ref:None, application:None};
        services.approval_queue().submit_scoped(&reference, SubmitApprovalRequest {
            context:ApprovalContext::owned(&source, "decide work", "test-workspace"), source,
            action:"decide work".into(), summary:"Review requested work".into(),
            risk:harness_contract::core::TaskRisk::Medium, domain:ApprovalDomain::Execution,
            blocks_execution:true, evidence_refs:vec![], timeout_policy:ApprovalTimeoutPolicy::AutoDeny,
        }).unwrap();
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        executor.bind_runtime_services(Arc::clone(&services)).unwrap();
        let request = || serde_json::from_value::<EvidenceRetrieveToolRequest>(serde_json::json!({"evidence_ref":reference, "limit":1})).unwrap();
        assert!(executor.execute_evidence_retrieve(request(), Some("decision-session"), &[]).await.is_err());
        let reason = "Operator checked the proposed work and declined. ".repeat(1200);
        services.approval_queue().decide_surface_human("operator:gateway", ApprovalDecisionCommand {
            approval_id:reference.clone(), approved:false, skip:false, reason:reason.clone(), scope:ApprovalGrantScope::Once,
            actor:ApprovalDecisionActor {kind:ApprovalDecisionActorKind::Human, actor_id:"surface-replaces-this".into()}, evidence_refs:vec![],
        }).unwrap();
        assert!(executor.execute_evidence_retrieve(request(), None, &[]).await.is_err());
        assert!(executor.execute_evidence_retrieve(request(), Some("other-session"), &["session:decision-session".into()]).await.is_err());
        let mut next = request();
        let mut text = String::new();
        let mut pages = 0;
        let mut expected_hash = None;
        loop {
            let value:serde_json::Value = serde_json::from_str(&executor.execute_evidence_retrieve(next, Some("decision-session"), &[]).await.unwrap()).unwrap();
            assert_eq!(value["available"], true);
            if let Some(hash) = &expected_hash { assert_eq!(&value["sha256"], hash); }
            expected_hash = Some(value["sha256"].clone());
            for chunk in value["chunks"].as_array().unwrap() { text.push_str(chunk["content"].as_str().unwrap()); }
            pages += 1;
            assert!(pages < 200, "pagination must terminate");
            if value["next_request"].is_null() { break; }
            next = serde_json::from_value(value["next_request"].clone()).unwrap();
        }
        assert!(pages > 1);
        let content:serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(content["approval"]["decision"]["reason"], reason);
        assert_eq!(content["approval"]["decision"]["approved"], false);
        assert_eq!(content["approval"]["decision"]["actor"]["actor_id"], "operator:gateway");
        assert_eq!(content["scope"], "approval_decision_only");
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

        // A durable Session selector publishes exactly the chosen block, never
        // the assistant preamble or a neighbouring message.
        let blocks = serde_json::json!([
            {"type": "text", "text": "publication preamble"},
            {"type": "text", "text": "<html>正文\r\n</html>\n"},
            {"type": "tool_use", "id": "tool-block", "name": "read_file", "input": {}}
        ]);
        store.insert_message(&session::SessionMessage {
            stable_message_id: "publication-message".to_string(),
            session_id: session_id.to_string(), sequence: 0, role: "assistant".to_string(),
            content_json: blocks.to_string(), blocks_count: 3,
            tool_use_id: None, tool_name: None, token_usage_json: None, created_at_ms: 2,
        }).await.expect("durable source blocks");
        let binding = RuntimeToolExecutionBinding {
            action_id: None, session_id: Some(session_id), authorized_scopes: &[],
            memory_context: None, reality_context: None, model_lease: Some("test"), parent_execution: None,
            execution_decision: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        };
        use sha2::{Digest, Sha256};
        let block_hash = format!("{:x}", Sha256::digest(serde_json::to_vec(&blocks[1]).unwrap()));
        let request = serde_json::json!({
            "source": "message_block", "message_id": "publication-message",
            "block_index": 1, "sha256": block_hash, "media_type": "text/html"
        });
        let published: serde_json::Value = serde_json::from_str(
            &executor.execute_content_publication("artifact_publish", request.clone(), binding)
                .await.expect("publish selected durable block")
        ).unwrap();
        let (_, body) = services.read_authorized_publication(
            published["content_ref"].as_str().unwrap(), published["sha256"].as_str().unwrap(),
            session_id, &[],
        ).await.expect("published body");
        assert_eq!(body, blocks[1]["text"].as_str().unwrap().as_bytes());
        let mut other_session = binding;
        other_session.session_id = Some("unrelated-session");
        assert!(executor.execute_content_publication("artifact_publish", request.clone(), other_session).await.is_err());
        for index in [0, 2, 3] {
            let mut wrong = request.clone();
            wrong["block_index"] = serde_json::json!(index);
            if index == 2 {
                wrong["sha256"] = serde_json::json!(format!("{:x}", Sha256::digest(serde_json::to_vec(&blocks[2]).unwrap())));
            }
            assert!(executor.execute_content_publication("artifact_publish", wrong, binding).await.is_err());
        }

        // An explicitly selected but empty/whitespace-only text block must be
        // refused: authored content must never become an empty Task artifact
        // that only surfaces as a reviewer rework after burning the budget.
        let empty_blocks = serde_json::json!([{"type": "text", "text": "   \n  "}]);
        store.insert_message(&session::SessionMessage {
            stable_message_id: "empty-publication-message".to_string(),
            session_id: session_id.to_string(), sequence: 1, role: "assistant".to_string(),
            content_json: empty_blocks.to_string(), blocks_count: 1,
            tool_use_id: None, tool_name: None, token_usage_json: None, created_at_ms: 3,
        }).await.expect("empty source block");
        let empty_request = serde_json::json!({
            "source": "message_block", "message_id": "empty-publication-message",
            "block_index": 0,
            "sha256": format!("{:x}", Sha256::digest(serde_json::to_vec(&empty_blocks[0]).unwrap())),
            "media_type": "text/plain"
        });
        let empty_error = executor
            .execute_content_publication("artifact_publish", empty_request, binding)
            .await
            .expect_err("empty text block must not publish");
        assert!(
            empty_error.to_string().contains("empty text block"),
            "unexpected error: {empty_error}"
        );

        let actor = root_agent_action_actor(RuntimeToolExecutionBinding {
            action_id: None,
            session_id: Some(session_id),
            authorized_scopes: &[],
            memory_context: None, reality_context: None,
            model_lease: Some("deepseek-v4-flash"),
            parent_execution: None,
            execution_decision: None,
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        });
        let mut adjudication = harness_contract::agent_action::MessagePublishInput {
            topic_ref: "topic:program".into(), summary: Some("disclose the sourced limitation".into()),
            content_ref: None, refs: Vec::new(), recipients: Vec::new(), intent: None,
            issue_dispositions: vec![harness_contract::agent_action::IssueDisposition {
                issue_ref: "issue:source-validated-by-program".into(),
                disposition: harness_contract::agent_action::IssueDispositionKind::Disclose,
                reason_ref: artifact.selector.clone(), evidence_refs: vec![format!("tool://{evidence_id}")],
            }],
        };
        services.validate_agent_action_evidence(&actor, &harness_contract::agent_action::AgentAction::MessagePublish(adjudication.clone()))
            .await.expect("adjudication reasons and evidence use real Session authority");
        adjudication.issue_dispositions[0].reason_ref = "artifact://missing-disposition-reason".into();
        assert!(services.validate_agent_action_evidence(&actor, &harness_contract::agent_action::AgentAction::MessagePublish(adjudication)).await.is_err());

        let action = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                deliverable: None,
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
                    cursor: None,
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
            memory_context: None, reality_context: None,
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
        let before_foreign = services
            .agent_action_service()
            .project(&program.program_id)
            .expect("Program before rejected foreign root write");
        assert!(executor
            .execute_runtime_tool_with_binding(
                harness_contract::agent_action::TEAM_CREATE_TOOL_ID,
                serde_json::json!({"name":"Foreign", "mission":"must fail"}),
                foreign,
            )
            .await
            .is_err());
        assert_eq!(
            services
                .agent_action_service()
                .project(&program.program_id)
                .expect("Program after rejected foreign root write"),
            before_foreign,
            "a cross-Session root write must not change the durable Program"
        );
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
            memory_context: None, reality_context: None,
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

        let actor = root_agent_action_actor(binding);
        let before_rejected_external_write = services
            .agent_action_service()
            .project(&actor.program_id)
            .expect("Program before rejected external write");
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
        assert_eq!(
            services
                .agent_action_service()
                .project(&actor.program_id)
                .expect("Program after rejected external write"),
            before_rejected_external_write,
            "Gateway must reject invented content before writing business state"
        );

        let direct_artifact = harness_contract::agent_action::AgentAction::TaskSubmit(
            harness_contract::agent_action::TaskSubmitInput {
                deliverable: None,
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
                deliverable: None,
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
                deliverable: None,
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
        assert_eq!(
            services
                .agent_action_service()
                .project(&actor.program_id)
                .expect("Program after rejected forged external evidence"),
            before_rejected_external_write,
            "unreadable or forged external evidence must not write business state"
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
            memory_context: None, reality_context: None,
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

    #[tokio::test]
    async fn explicit_publication_roundtrip_and_scope_checks() {
        use sha2::{Digest, Sha256};
        let temp = tempfile::tempdir().unwrap();
        let registry = GatewayToolRegistry::builtin().with_runtime_tools(
            crate::runtime_bootstrap::runtime_capability_tool_definitions()
        ).unwrap();
        let host = Arc::new(ToolHost::new("publication-test", temp.path(), ToolHostSnapshot::new(
            Arc::new(registry), Arc::new(tools::lsp_client::LspRegistry::new()), None,
        )));
        let executor = GatewayToolExecutor::from_tool_host(None, false, host.clone())
            .with_runtime_session_id("publication-session");
        let services = runtime::RuntimeServices::builder(temp.path().join("home"), temp.path())
            .runtime_event_store(Arc::new(runtime::RuntimeEventStore::for_test()))
            .task_aggregate_service(Arc::new(runtime::TaskAggregateService::for_test()))
            .artifact_store(Arc::new(runtime::ArtifactStore::for_test_default(temp.path().join("artifacts"))))
            .build().unwrap();
        executor.bind_runtime_services(services.clone()).unwrap();
        let body = "<html>中文\r\n完整正文</html>\r\n";
        std::fs::write(temp.path().join("source.html"), body).unwrap();
        let hash = format!("{:x}", Sha256::digest(body.as_bytes()));
        let input = serde_json::json!({"source":"file","path":"source.html","sha256":hash,"media_type":"text/html"});
        host.pin_snapshot().validate_input("artifact_publish", &input).unwrap();
        let binding = RuntimeToolExecutionBinding { action_id: Some("publish"), session_id: Some("publication-session"),
            authorized_scopes: &[], memory_context: None, reality_context: None, model_lease: None, parent_execution: None,
            execution_decision: None, permission_ceiling: harness_contract::policy::PermissionMode::WorkspaceWrite };
        let published: serde_json::Value = serde_json::from_str(&executor.execute_runtime_tool_with_binding(
            "artifact_publish", input, binding,
        ).await.unwrap()).unwrap();
        assert_eq!(published["bytes"], body.len());
        assert_eq!(published["sha256"], format!("sha256:{hash}"));
        let export = serde_json::json!({"content_ref": published["content_ref"], "sha256": published["sha256"], "path":"result.html"});
        host.pin_snapshot().validate_input("artifact_materialize", &export).unwrap();
        for index in 0..2 {
            let result: serde_json::Value = serde_json::from_str(&executor.execute_runtime_tool_with_binding("artifact_materialize", export.clone(), binding).await.unwrap()).unwrap();
            assert_eq!(result["created"], index == 0);
            let evidence = materialized_artifact_evidence(services.path_identity_resolver(), &result, index + 1).unwrap();
            assert_eq!(evidence.workspace_prior_state.is_some(), index == 0);
        }
        assert_eq!(std::fs::read(temp.path().join("result.html")).unwrap(), body.as_bytes());
        let denied = RuntimeToolExecutionBinding { session_id: Some("other-session"), ..binding };
        assert!(executor.execute_runtime_tool_with_binding("artifact_materialize", export.clone(), denied).await.is_err());
        let parent = harness_contract::execution_graph::ExecutionParentBinding { execution_id:"parent".into(), node_id:"agent".into() };
        let scopes = vec!["write:unrelated".to_string(), "session:publication-session".to_string()];
        let scoped = RuntimeToolExecutionBinding { parent_execution: Some(&parent), authorized_scopes: &scopes, ..binding };
        assert!(executor.execute_runtime_tool_with_binding("artifact_materialize", export, scoped).await.is_err());
        std::fs::write(temp.path().join("source.html"), "changed").unwrap();
        assert!(executor.execute_runtime_tool_with_binding("artifact_publish", serde_json::json!({
            "source":"file","path":"source.html","sha256":hash,"media_type":"text/html"
        }), binding).await.is_err());
    }

    #[tokio::test]
    async fn context_program_discovery_uses_one_authorized_revision_and_real_requests() {
        let services = runtime::RuntimeServices::in_memory().unwrap();
        let registry = GatewayToolRegistry::builtin().with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions()).unwrap();
        let executor = GatewayToolExecutor::new(None,false,registry);
        executor.bind_runtime_services(services.clone()).unwrap();
        let binding = RuntimeToolExecutionBinding {action_id:Some("program-discovery"),session_id:Some("program-directory-session"),
            authorized_scopes:&[],memory_context: None, reality_context: None,model_lease:Some("test-model"),parent_execution:None,execution_decision:None,
            permission_ceiling:harness_contract::policy::PermissionMode::ReadOnly};
        let actor = root_agent_action_actor(binding);
        let actions = services.agent_action_service();
        for index in 0..35 {
            let result = services.submit_agent_action(&harness_contract::agent_action::AgentActionEnvelope {action_id:format!("directory-team-{index}"),actor:actor.clone(),expected_revision:None,
                action:harness_contract::agent_action::AgentAction::TeamCreate(harness_contract::agent_action::TeamCreateInput {name:format!("Directory Team {index:02}"),mission:"directory test".into(),objective:None})}).await.unwrap();
            assert_eq!(result.status,harness_contract::agent_action::AgentActionStatus::Applied);
        }
        let revision = actions.project(&actor.program_id).unwrap().revision;
        let mut request = serde_json::json!({"source":"program"});
        let mut refs = std::collections::BTreeSet::new(); let mut first_next = None;
        loop {
            executor.tool_host.pin_snapshot().validate_input("context_retrieve",&request).unwrap();
            let output = executor.execute_runtime_tool_with_binding("context_retrieve",request,binding).await.unwrap();
            let page:serde_json::Value = serde_json::from_str(&output).unwrap();
            assert_eq!(page["revision"],revision);
            for item in page["selected"].as_array().unwrap() {
                assert!(refs.insert(item["ref"].as_str().unwrap().to_owned()));
                executor.tool_host.pin_snapshot().validate_input("context_retrieve",&item["read_request"]).unwrap();
                let exact = executor.execute_runtime_tool_with_binding("context_retrieve",item["read_request"].clone(),binding).await.unwrap();
                assert!(exact.contains("Directory Team"));
            }
            if page["next_request"].is_null() { assert_eq!(page["coverage"]["complete"],true); break; }
            first_next = Some(page["next_request"].clone()); request = page["next_request"].clone();
        }
        assert_eq!(refs.len(),35); assert_eq!(actions.project(&actor.program_id).unwrap().revision,revision);
        let other = RuntimeToolExecutionBinding {session_id:Some("other-session"),..binding};
        assert!(executor.execute_runtime_tool_with_binding("context_retrieve",first_next.clone().unwrap(),other).await.is_err());
        let focused:serde_json::Value=serde_json::from_str(&executor.execute_runtime_tool_with_binding("context_retrieve",serde_json::json!({"source":"program","query":"Directory Team 34"}),binding).await.unwrap()).unwrap();
        assert_eq!(focused["selected_count"],1);
        let changed = serde_json::json!({"source":"program","query":"changed","cursor":first_next.unwrap()["cursor"]});
        assert!(executor.execute_runtime_tool_with_binding("context_retrieve",changed,binding).await.is_err());
    }

    #[tokio::test]
    async fn context_artifact_catalog_returns_readable_scoped_content_and_lossless_binary() {
        use base64::Engine;
        let services=runtime::RuntimeServices::in_memory().unwrap();
        let registry=GatewayToolRegistry::builtin().with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions()).unwrap();
        let executor=GatewayToolExecutor::new(None,false,registry);
        executor.bind_runtime_services(services.clone()).unwrap();
        let binding=RuntimeToolExecutionBinding {action_id:Some("artifact-directory"),session_id:Some("artifact-directory-session"),
            authorized_scopes:&[],memory_context: None, reality_context: None,model_lease:None,parent_execution:None,execution_decision:None,
            permission_ceiling:harness_contract::policy::PermissionMode::ReadOnly};
        let descriptor=harness_contract::context::ArtifactWriteDescriptor {media_type:"text/plain".into(),visibility_scope:"session:artifact-directory-session".into(),expected_bytes:None,original_name:None};
        let mut expected=std::collections::BTreeSet::new();
        for index in 0..35 { expected.insert(services.artifact_store().write_bytes(descriptor.clone(),format!("published {index}").as_bytes()).await.unwrap().selector); }
        let hidden=services.artifact_store().write_bytes(harness_contract::context::ArtifactWriteDescriptor {visibility_scope:"session:private".into(),..descriptor.clone()},b"private").await.unwrap();
        let mut request=serde_json::json!({"source":"artifact","query":"text/plain","limit":3});
        let mut actual=std::collections::BTreeSet::new(); let mut pages=0;
        loop {
            executor.tool_host.pin_snapshot().validate_input("context_retrieve",&request).unwrap();
            let page:serde_json::Value=serde_json::from_str(&executor.execute_runtime_tool_with_binding("context_retrieve",request,binding).await.unwrap()).unwrap();
            pages+=1; assert!(pages<20);
            for item in page["selected"].as_array().unwrap() {
                assert!(actual.insert(item["ref"].as_str().unwrap().to_string()));
                executor.tool_host.pin_snapshot().validate_input("evidence_retrieve",&item["read_request"]).unwrap();
                let read:serde_json::Value=serde_json::from_str(&executor.execute_runtime_tool_with_binding("evidence_retrieve",item["read_request"].clone(),binding).await.unwrap()).unwrap();
                assert_eq!(read["available"],true); assert_eq!(read["sha256"],item["sha256"]);
            }
            if page["next_request"].is_null() { break; }
            services.artifact_store().write_bytes(descriptor.clone(),b"own tool receipt").await.unwrap();
            request=page["next_request"].clone();
        }
        assert_eq!(actual,expected); assert!(!actual.contains(&hidden.selector));
        let bytes=(0..50_000).map(|index| (index%256) as u8).collect::<Vec<_>>();
        let binary=services.artifact_store().write_bytes(harness_contract::context::ArtifactWriteDescriptor {media_type:"application/octet-stream".into(),..descriptor},&bytes).await.unwrap();
        let mut request=serde_json::json!({"evidence_ref":binary.selector,"limit":1}); let mut encoded=String::new();
        loop {
            let read:serde_json::Value=serde_json::from_str(&executor.execute_runtime_tool_with_binding("evidence_retrieve",request,binding).await.unwrap()).unwrap();
            assert_eq!(read["encoding"],"base64"); assert_eq!(read["sha256"],binary.sha256);
            for chunk in read["chunks"].as_array().unwrap() { encoded.push_str(chunk["content"].as_str().unwrap()); }
            if read["next_request"].is_null() { break; } request=read["next_request"].clone();
        }
        assert_eq!(base64::engine::general_purpose::STANDARD.decode(encoded).unwrap(),bytes);
        assert!(executor.execute_runtime_tool_with_binding("evidence_retrieve",serde_json::json!({"evidence_ref":binary.selector,"query":"needle"}),binding).await.is_err());
    }
