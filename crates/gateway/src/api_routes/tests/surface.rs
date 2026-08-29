// Legacy API behavior shard; included into one shared test scope.
    #[tokio::test]
    async fn approval_pending_filters_keep_chat_blockers_separate_from_other_domains() {
        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        let queue = runtime_services.approval_queue();
        let submit = |id: &str,
                      session_id: &str,
                      domain: harness_contract::policy::ApprovalDomain,
                      blocks_execution: bool| {
            let source = runtime::ApprovalSource {
                kind: runtime::ApprovalSourceKind::Session,
                session_id: Some(session_id.to_string()),
                agent_id: None,
                team_id: None,
                mission_id: None,
                resource_ref: None,
                review_ref: None,
                application: None,
            };
            queue
                .submit_scoped(
                    id,
                    runtime::SubmitGlobalApprovalRequest {
                        context: harness_contract::policy::ApprovalContext::owned(
                            &source,
                            id,
                            "workspace:approval-filter",
                        ),
                        source,
                        action: id.to_string(),
                        summary: id.to_string(),
                        risk: harness_contract::core::TaskRisk::High,
                        domain,
                        blocks_execution,
                        evidence_refs: Vec::new(),
                        timeout_policy: runtime::ApprovalTimeoutPolicy::Pending,
                    },
                )
                .unwrap();
        };
        submit(
            "approval-filter:chat",
            "chat-session",
            harness_contract::policy::ApprovalDomain::Execution,
            true,
        );
        submit(
            "approval-filter:knowledge",
            "chat-session",
            harness_contract::policy::ApprovalDomain::Knowledge,
            false,
        );
        submit(
            "approval-filter:other-session",
            "other-session",
            harness_contract::policy::ApprovalDomain::Execution,
            true,
        );

        let response = api_router(state)
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending?session_id=chat-session&domain=execution&blocks_execution=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let pending = body["pending"].as_array().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0]["approval_id"], "approval-filter:chat");
        assert_eq!(pending[0]["domain"], "execution");
        assert_eq!(pending[0]["blocks_execution"], true);
    }
    #[tokio::test]
    async fn approval_api_resumes_the_same_execution_graph_and_rejects_stale_decisions() {
        use harness_contract::execution_graph::{
            ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
            ExecutionNodeStatus,
        };

        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        runtime_services
            .executor_registry()
            .register(Arc::new(ApprovalResumeTestExecutor))
            .expect("test tool executor");

        let mut graph = ExecutionGraph::new("gateway approval resume").with_lineage(
            harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: "approval-api-session".to_string(),
                turn_id: "approval-api-turn".to_string(),
                root_task_id: "approval-api-task".to_string(),
                task_id: "approval-api-task".to_string(),
                generation: 1,
            },
        );
        let approval = ExecutionNodeSpec::new(
            ExecutionNodeKind::Approval,
            "approval",
            serde_json::json!({
                "action": "write",
                "summary": "approve graph continuation",
                "session_id": "approval-api-session"
            })
            .to_string(),
        );
        let tool = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "approval_resume_test_tool",
            "tool:after-approval",
        );
        graph.edges.push(ExecutionEdge {
            from: approval.id.clone(),
            to: tool.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes = vec![approval.clone(), tool.clone()];
        let graph_id = graph.id.clone();
        let (_, report) = runtime_services
            .execution_supervisor()
            .submit_and_wait(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph reaches approval wait");
        assert_eq!(report.waiting, 1);
        let waiting = runtime_services
            .graph_state_store()
            .load(&graph_id)
            .expect("waiting graph");
        assert_eq!(
            waiting.node_statuses[&approval.id],
            ExecutionNodeStatus::WaitingApproval
        );
        assert_eq!(
            waiting.node_statuses[&tool.id],
            ExecutionNodeStatus::Planned
        );
        assert!(matches!(
            runtime_services
                .execution_supervisor()
                .command_graph(
                    &graph_id,
                    harness_contract::execution_graph::ExecutionGraphCommand::SubmitApproval {
                        expected_revision: waiting.revision.saturating_sub(1),
                        node_id: approval.id.clone(),
                        decision: Box::new(harness_contract::policy::ApprovalDecisionCommand {
                            approval_id: "stale-test-decision".to_string(),
                            approved: true,
                            skip: false,
                            reason: "stale command".to_string(),
                            scope: harness_contract::policy::ApprovalGrantScope::Once,
                            actor: harness_contract::policy::ApprovalDecisionActor {
                                kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                                actor_id: "test-human".to_string(),
                            },
                            evidence_refs: vec!["test.stale".to_string()],
                        }),
                    },
                )
                .await,
            Err(runtime::execution_core::ExecutionRunnerError::Commit(
                runtime::execution_core::graph::ExecutionCommitError::StaleRevision { .. }
            ))
        ));

        let approval_id =
            runtime::execution_core::graph::executors::graph_approval_id(&graph_id, &approval.id);
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "scope": "once",
                            "reason": "verified by operator"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let response_body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "graph approval response: {}",
            String::from_utf8_lossy(&response_body)
        );
        let body: serde_json::Value = serde_json::from_slice(&response_body).unwrap();
        assert_eq!(body["execution_graph"]["graph_id"], graph_id);
        assert_eq!(body["execution_graph"]["node_status"], "ready");
        assert_eq!(body["receipt"]["decision"]["scope"], "once");
        assert_eq!(
            body["receipt"]["decision"]["reason"],
            "verified by operator"
        );
        assert!(body["receipt"]["decision"]["actor"]["actor_id"]
            .as_str()
            .is_some_and(|actor| !actor.is_empty()));
        assert!(runtime_services
            .approval_queue()
            .grants()
            .iter()
            .any(|grant| grant.approval_id == approval_id
                && grant.scope == harness_contract::policy::ApprovalGrantScope::Once));

        runtime_services
            .execution_supervisor()
            .wait_for_quiescence(&graph_id)
            .await
            .expect("approved graph reaches quiescence through the supervisor");

        let terminal = runtime_services
            .graph_state_store()
            .load(&graph_id)
            .expect("terminal graph");
        assert_eq!(
            terminal.node_statuses[&approval.id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            terminal.node_statuses[&tool.id],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            terminal.node_results[&tool.id].result_ref.as_deref(),
            Some(format!("tool-result:{}", tool.id).as_str())
        );

        let duplicate = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "scope": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(duplicate.status(), StatusCode::OK);
        let duplicate_body: serde_json::Value =
            serde_json::from_slice(&to_bytes(duplicate.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(duplicate_body["status"], "already_applied");

        let conflicting = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": false,
                            "scope": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(conflicting.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn cross_plane_waiting_approval_resumes_tool_and_reaches_terminal_graph() {
        let state = test_state();
        let runtime_services = state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .runtime_services();
        let action = runtime::CrossPlaneAction::new("operator", "channel.send");
        let decision = runtime::CrossPlanePolicyDecision {
            decision: runtime::CrossPlaneDecisionKind::RequireSingleApproval,
            reason: "operator approval required".to_string(),
            matched_grant: None,
            required_approval: Some(runtime::GrantType::SingleUse),
            degrade_to: None,
        };
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let graph_key = format!("cross-plane-approval-{}", uuid::Uuid::new_v4());
        let projection = state
            .services
            .cross_plane
            .execute_commit_graph(
                &action,
                &decision,
                &graph_key,
                None,
                Arc::new(CrossPlaneApprovalTestBackend {
                    calls: Arc::clone(&calls),
                }),
            )
            .await
            .expect("cross-plane graph reaches approval wait");
        let approval = projection
            .nodes
            .iter()
            .find(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::Approval
            })
            .expect("approval node");
        assert_eq!(
            approval.status,
            harness_contract::execution_graph::ExecutionNodeStatus::WaitingApproval
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        let conflicting_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let conflicting_action =
            runtime::CrossPlaneAction::new("different-operator", "service.execute");
        let conflict = state
            .services
            .cross_plane
            .execute_commit_graph(
                &conflicting_action,
                &decision,
                &graph_key,
                None,
                Arc::new(CrossPlaneApprovalTestBackend {
                    calls: Arc::clone(&conflicting_calls),
                }),
            )
            .await;
        assert!(conflict.is_err());
        assert_eq!(
            conflicting_calls.load(std::sync::atomic::Ordering::SeqCst),
            0
        );

        let approval_id = runtime::execution_core::graph::executors::graph_approval_id(
            &projection.graph_id,
            &approval.node_id,
        );
        let response = api_router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "scope": "once"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        runtime_services
            .execution_supervisor()
            .wait_for_quiescence(&projection.graph_id)
            .await
            .expect("approved cross-plane graph reaches quiescence");
        let terminal = runtime_services
            .execution_supervisor()
            .projection(&projection.graph_id)
            .await
            .expect("terminal cross-plane graph");
        let tool = terminal
            .nodes
            .iter()
            .find(|node| {
                node.kind == harness_contract::execution_graph::ExecutionNodeKind::ToolBatch
            })
            .expect("tool node");
        assert_eq!(
            tool.status,
            harness_contract::execution_graph::ExecutionNodeStatus::Completed
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cross_plane_terminal_graph_recovers_before_owner_receipt_without_reexecution() {
        let state = test_state();
        let action = runtime::CrossPlaneAction::new("operator", "channel.send");
        let decision = runtime::CrossPlanePolicyDecision {
            decision: runtime::CrossPlaneDecisionKind::Allow,
            reason: "test grant".to_string(),
            matched_grant: None,
            required_approval: None,
            degrade_to: None,
        };
        let graph_key = format!("cross-plane-terminal-window-{}", uuid::Uuid::new_v4());
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first = state
            .services
            .cross_plane
            .execute_commit_graph(
                &action,
                &decision,
                &graph_key,
                None,
                Arc::new(CrossPlaneApprovalTestBackend {
                    calls: Arc::clone(&calls),
                }),
            )
            .await
            .expect("first graph execution");
        assert!(first.nodes.iter().all(|node| node.status.is_terminal()));
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(state
            .services
            .cross_plane
            .find_execution_by_idempotency_key(&graph_key)
            .is_none());

        let recovered = state
            .services
            .cross_plane
            .execute_commit_graph(
                &action,
                &decision,
                &graph_key,
                None,
                Arc::new(CrossPlaneApprovalTestBackend {
                    calls: Arc::clone(&calls),
                }),
            )
            .await
            .expect("terminal graph recovery");
        assert_eq!(recovered.graph_id, first.graph_id);
        assert_eq!(recovered.commit_cursor, first.commit_cursor);
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn canonical_approval_config_is_available_and_solo_route_is_removed() {
        let state = test_state();
        let config_path = state.config_home.join("config.yaml");
        let app = api_router(Arc::clone(&state));
        let config_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(config_response.status(), StatusCode::OK);
        let body = to_bytes(config_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["profile"], "balanced");
        assert_eq!(json["low_risk_timeout"], "auto_approve_once");

        let update_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/approval/config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "profile": "autonomous",
                            "low_risk_timeout": "pending"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update_response.status(), StatusCode::OK);
        let persisted: serde_yaml::Value =
            serde_yaml::from_str(&std::fs::read_to_string(config_path).unwrap()).unwrap();
        assert_eq!(persisted["approval"]["profile"], "autonomous");
        assert_eq!(persisted["approval"]["low_risk_timeout"], "pending");

        let solo_response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/solo")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        // The exact-id GET route still owns this path shape, so an unregistered
        // POST is represented by Axum as method-not-allowed rather than not-found.
        assert_eq!(solo_response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn approval_config_update_requires_verified_management_capability() {
        let state = test_state();
        let principal = runtime::VerifiedPrincipal::from_test_claims(
            harness_contract::security::PrincipalClaims {
                principal_id: "approval-reviewer-only".to_string(),
                tenant_id: "tenant:test".to_string(),
                grant_id: "grant:approval-reviewer".to_string(),
                kind: harness_contract::security::PrincipalKind::Human,
                scopes: vec!["gateway".to_string()],
                capabilities: vec!["approval.respond".to_string()],
                assurance: harness_contract::security::PrincipalAssurance::HumanInteractive,
                issuer: "approval-config-test".to_string(),
                issued_at_ms: 1,
                expires_at_ms: None,
                credential_fingerprint: "approval-config-test".to_string(),
                credential_epoch: 1,
                profile_revision: 1,
                app_profiles: std::collections::BTreeMap::new(),
            },
        );
        let app = approval_routes::router()
            .layer(Extension(AuthenticatedPrincipal(principal)))
            .with_state(state);
        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/api/approval/config")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "profile": "autonomous",
                            "low_risk_timeout": "pending"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn platform_readiness_defaults_to_disabled_without_config() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/platforms")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let platforms = json.as_array().unwrap();
        assert!(platforms.iter().any(|item| item["name"] == "feishu"
            && item["status"] == "disabled"
            && item["credential_present"] == false));
        assert!(platforms.iter().any(|item| {
            item["name"] == "wechat-ilink"
                && item["capabilities"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("message.ingress"))
                && item["capabilities"]
                    .as_array()
                    .unwrap()
                    .contains(&serde_json::json!("message.send.text"))
        }));
    }

    #[tokio::test]
    async fn platform_readiness_reports_missing_fields_without_leaking_secrets() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "cli_app_id",
                        "app_secret": ""
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/platforms/feishu")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["readiness"]["status"], "degraded");
        assert_eq!(json["readiness"]["credential_present"], false);
        assert!(json["readiness"]["missing_required"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("app_secret")));
        assert!(!json.to_string().contains("cli_app_id"));
    }

    #[tokio::test]
    async fn connector_routes_expose_contract_snapshot_with_local_service_account() {
        let workspace = unique_test_workspace("connector-empty");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let summary: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(summary["kind"], "connector_summary");
        assert_eq!(summary["summary"]["account_count"], 1);
        assert!(summary["summary"]["capability_count"].as_u64().unwrap() >= 4);
        assert_eq!(summary["summary"]["resource_count"], 0);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let capabilities: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(capabilities["kind"], "connector_capabilities");
        let list = capabilities["capabilities"].as_array().unwrap();
        assert!(list
            .iter()
            .any(|item| item["capability_id"] == "governance.cross_plane.audit"));
        assert!(list
            .iter()
            .any(|item| item["capability_id"] == "service.local.docs.read"
                && item["plane"] == "service"));
        assert!(!list.iter().any(|item| {
            item["capability_id"]
                .as_str()
                .unwrap_or_default()
                .contains("feishu_document_operation")
        }));
    }

    #[tokio::test]
    async fn connector_accounts_exclude_surface_message_platforms() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "name": "feishu-main",
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "cli_app_id",
                        "app_secret": ""
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/accounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_accounts");
        assert_eq!(json["total"], 1);
        assert!(json["accounts"]
            .as_array()
            .unwrap()
            .iter()
            .all(|account| account["provider"] != "feishu"));
        assert!(!json.to_string().contains("cli_app_id"));
    }

    #[tokio::test]
    async fn connector_routes_project_configured_mcp_servers_into_runtime_contract() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "mcpServers": {
                "github.com": {
                    "type": "stdio",
                    "command": "npx",
                    "args": ["-y", "@modelcontextprotocol/server-github"],
                    "env": {
                        "GITHUB_TOKEN": "secret-token"
                    }
                },
                "broken": {
                    "type": "stdio"
                }
            }
        })));

        let accounts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/accounts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accounts.status(), StatusCode::OK);
        let body = to_bytes(accounts.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_accounts");
        assert_eq!(json["total"], 3);
        assert!(json["accounts"].as_array().unwrap().iter().any(|account| {
            account["provider"] == "mcp"
                && account["account_id"] == "github.com"
                && account["auth_mode"] == "stdio"
                && account["health"]["status"] == "ready"
                && account["enabled_bindings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item == "mcp.github_com.server")
        }));
        assert!(json["accounts"].as_array().unwrap().iter().any(|account| {
            account["provider"] == "mcp"
                && account["account_id"] == "broken"
                && account["health"]["status"] == "degraded"
        }));
        assert!(!json.to_string().contains("secret-token"));

        let capabilities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/capabilities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(capabilities.status(), StatusCode::OK);
        let body = to_bytes(capabilities.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |capability| capability["capability_id"] == "mcp.github_com.server"
                    && capability["plane"] == "mcp"
                    && capability["supports_commit"] == false
            ));

        let mcp_servers = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/mcp/servers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mcp_servers.status(), StatusCode::OK);
        let body = to_bytes(mcp_servers.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_mcp_servers");
        assert_eq!(json["summary"]["total"], 2);
        assert_eq!(json["summary"]["ready"], 1);
        assert_eq!(json["summary"]["degraded"], 1);
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "broken"
                && server["status"] == "degraded"
                && server["missing_required"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|item| item == "command")
        }));
        assert!(!json.to_string().contains("secret-token"));

        let mcp_probe = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/mcp/servers?probe=true&timeout_ms=75")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mcp_probe.status(), StatusCode::OK);
        let body = to_bytes(mcp_probe.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["probe"]["requested"], true);
        assert_eq!(json["probe"]["timeout_ms"], 75);
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "github.com"
                && server["probe"]["requested"] == true
                && server["probe"]["mode"] == "config_only"
                && server["probe"]["status"] == "declared"
        }));
        assert!(json["servers"].as_array().unwrap().iter().any(|server| {
            server["name"] == "broken"
                && server["probe"]["requested"] == true
                && server["probe"]["status"] == "degraded"
        }));
        assert!(!json.to_string().contains("secret-token"));
    }

    #[tokio::test]
    async fn local_docs_service_connector_executes_through_cross_plane_receipt() {
        let workspace = unique_test_workspace("connector-mock-docs");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let tools = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/services/local.docs/tools")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(tools.status(), StatusCode::OK);
        let body = to_bytes(tools.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_service_tools");
        assert_eq!(json["service"]["id"], "local.docs");
        assert!(json["tools"].as_array().unwrap().iter().any(|tool| {
            tool["capability_id"] == "service.local.docs.read" && tool["plane"] == "service"
        }));

        let key = format!("mock-docs-{}", uuid::Uuid::new_v4());
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "doc-1",
            "title": "Architecture",
            "mode": "dry_run",
            "idempotency_key": key
        });
        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(first_json["kind"], "connector_service_execution");
        assert_eq!(first_json["service"], "local.docs");
        assert_eq!(first_json["replayed"], false);
        assert_eq!(first_json["resource_persisted"], true);
        assert_eq!(
            first_json["result"]["resource"]["reference"],
            "service://local.docs/document/doc-1"
        );
        assert_eq!(
            first_json["receipt"]["action"]["requested_capability"],
            "service.local.docs.read"
        );
        assert_eq!(
            first_json["receipt"]["action"]["resource_ref"],
            "service://local.docs/document/doc-1"
        );
        let receipt_id = first_json["receipt"]["id"].as_str().unwrap().to_string();

        let resources = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let resources_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(resources_json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |resource| resource["reference"] == "service://local.docs/document/doc-1"
                    && resource["title"] == "Architecture"
            ));

        let replay = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
        let replay_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(replay_json["replayed"], true);
        assert_eq!(replay_json["receipt"]["id"], receipt_id);
    }

    #[tokio::test]
    async fn connector_resources_survive_new_app_state_for_same_workspace() {
        let workspace = unique_test_workspace("connector-resources");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace.clone(),
        ));
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "persisted-doc",
            "title": "Persisted Runtime Resource",
            "mode": "dry_run",
            "idempotency_key": format!("persisted-doc-{}", uuid::Uuid::new_v4())
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let reopened = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let resources = reopened
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?q=Persisted")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let resources_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(resources_json["status"], "available");
        assert!(resources_json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["reference"]
                == "service://local.docs/document/persisted-doc"
                && resource["title"] == "Persisted Runtime Resource"));
    }

    #[tokio::test]
    async fn connector_resources_clamp_large_page_requests() {
        let workspace = unique_test_workspace("connector-resource-page-limit");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?limit=999&offset=0")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resources");
        assert_eq!(json["limit"], 200);
    }

    #[tokio::test]
    async fn connector_resource_revalidation_route_updates_index_state() {
        let workspace = unique_test_workspace("connector-resource-revalidate");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace,
        ));
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "revalidate-doc",
            "title": "Revalidate Doc",
            "mode": "commit",
            "idempotency_key": format!("revalidate-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let revalidate = serde_json::json!({
            "reference": "service://local.docs/document/revalidate-doc",
            "state": "stale"
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/revalidate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(revalidate.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_revalidation");
        assert_eq!(json["ok"], true);
        assert_eq!(json["resource"]["indexed_state"], "stale");

        let resources = app
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/resources?q=Revalidate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resources.status(), StatusCode::OK);
        let body = to_bytes(resources.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["indexed_state"] == "stale"));
    }

    #[tokio::test]
    async fn connector_resource_promote_memory_creates_metadata_only_memory() {
        let tmp =
            std::env::temp_dir().join(format!("cowd-api-resource-memory-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&tmp).unwrap();
        let manager = Arc::new(
            CognitiveContextManager::new(test_memory_config(&tmp.join("memory.db")))
                .await
                .unwrap(),
        );
        let app = api_router(test_state_with_memory_and_workspace(manager, tmp.clone()));
        let request = serde_json::json!({
            "tool_id": "service.local.docs.read",
            "resource_id": "memory-doc",
            "title": "Memory Bridge Doc",
            "mode": "commit",
            "idempotency_key": format!("memory-bridge-{}", uuid::Uuid::new_v4())
        });
        let execute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(execute.status(), StatusCode::OK);

        let promote = serde_json::json!({
            "reference": "service://local.docs/document/memory-doc",
            "session_id": "resource-memory-session"
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/promote-memory")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(promote.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_memory_promotion");
        assert_eq!(json["ok"], true);
        assert_eq!(json["layer"], "L3");
        let first_memory_id = json["memory_id"].clone();

        let replay = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/resources/promote-memory")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(promote.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "connector_resource_memory_promotion");
        assert_eq!(json["ok"], true);
        assert_eq!(json["replayed"], true);
        assert_eq!(json["memory_id"], first_memory_id);

        let entries = app
            .oneshot(
                Request::builder()
                    .uri("/api/memory/L3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(entries.status(), StatusCode::OK);
        let body = to_bytes(entries.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let entry = json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["title"] == "Connector resource: Memory Bridge Doc")
            .expect("promoted resource memory should exist");
        let content = entry["content"].as_str().unwrap_or_default();
        assert!(content.contains("service://local.docs/document/memory-doc"));
        assert!(content.contains("body_policy: metadata_only"));
        assert!(!content.contains("external document body"));
        let duplicate_count = json["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| {
                entry["content"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("ref: service://local.docs/document/memory-doc")
            })
            .count();
        assert_eq!(duplicate_count, 1);
        std::fs::remove_dir_all(tmp).ok();
    }

    #[tokio::test]
    async fn cross_plane_policy_simulation_does_not_consume_single_use_grant() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("service.feishu.drive.download.{suffix}");
        let grant_id = format!("grant-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");

        let audit = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
        let audit_body = to_bytes(audit.into_body(), usize::MAX).await.unwrap();
        let audit_json: serde_json::Value = serde_json::from_slice(&audit_body).unwrap();
        let records = audit_json["records"].as_array().unwrap();
        assert!(
            records.iter().all(|record| {
                record["evidence"]["consumed_grant_id"].as_str() != Some(grant_id.as_str())
            }),
            "policy simulation must not consume single-use grants"
        );
    }

    #[tokio::test]
    async fn connector_service_commit_consumes_single_use_grant_after_effect_receipt() {
        let state = test_state();
        publish_test_session_policy(&state.services, "service-commit-session");
        let app = api_router(state);
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = "service.local.docs.read";
        let grant_id = format!("grant-service-commit-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "source_channel": "channel://wechat/chat/service-commit",
            "session_id": "service-commit-session",
            "tool_id": capability,
            "resource_id": format!("doc-{suffix}"),
            "title": "Service Commit",
            "mode": "commit",
            "idempotency_key": format!("idem-service-commit-{suffix}")
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/connectors/services/local.docs/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"]["status"], "executed");
        assert_eq!(json["receipt"]["dispatch_status"], "service_executed");
        assert!(json["execution_graph"]["graph_id"].as_str().is_some());
        assert_eq!(
            json["receipt"]["execution_graph_id"],
            json["execution_graph"]["graph_id"]
        );
        assert!(json["receipt"]["audit_record_id"].as_str().is_some());

        let audit = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let audit_body = to_bytes(audit.into_body(), usize::MAX).await.unwrap();
        let audit_json: serde_json::Value = serde_json::from_slice(&audit_body).unwrap();
        let planned = audit_json["records"]
            .as_array()
            .unwrap()
            .iter()
            .find(|record| record["action"]["actor_principal"] == principal)
            .expect("commit planning must be audited");
        assert_eq!(planned["evidence"]["consumed_grant_id"], grant_id);
        assert_eq!(planned["evidence"]["remaining_uses_after"], 0);

        let action = serde_json::json!({
            "requested_capability": capability,
            "provider_account": "local.docs",
            "source_channel": "channel://wechat/chat/service-commit",
            "resource_ref": null,
            "target_ref": null,
            "session_id": "service-commit-session",
            "risk": "medium",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_identity_resolve_matches_cross_channel_contact_key() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("demo-{suffix}@example.com");
        let principal = format!("user:demo-{suffix}");
        let identity = serde_json::json!({
            "id": format!("idb-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/demo?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/identities")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(identity.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = serde_json::json!({
            "identity_ref": format!("channel://wechat/user/demo?email={email}")
        });
        let resolved = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/identity/resolve")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resolved.status(), StatusCode::OK);
        let body = to_bytes(resolved.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_identity_resolution");
        assert_eq!(json["resolved"]["principal_id"], principal);
        assert_eq!(json["resolved"]["trust"], "verified");
        assert_eq!(json["resolved"]["match_kind"], "contact_key");
    }

    #[tokio::test]
    async fn cross_plane_policy_simulation_resolves_actor_identity_before_decision() {
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("policy-{suffix}@example.com");
        let principal = gateway_test_actor();
        let capability = format!("service.feishu.drive.download.{suffix}");

        let identity = serde_json::json!({
            "id": format!("idb-policy-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/policy?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });
        let grant = serde_json::json!({
            "id": format!("grant-policy-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        for (uri, body) in [
            ("/api/cross-plane/identities", identity),
            ("/api/cross-plane/grants", grant),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let action = serde_json::json!({
            "actor_identity_ref": format!("channel://wechat/user/policy?email={email}"),
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "unknown"
        });
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["decision"]["decision"], "allow");
        assert_eq!(json["action"]["actor_principal"], principal);
        assert_eq!(
            json["decision"]["matched_grant"]["principal_id"],
            json["action"]["actor_principal"]
        );
    }

    #[tokio::test]
    async fn cross_plane_preflight_combines_identity_policy_and_platform_without_consuming_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let email = format!("preflight-{suffix}@example.com");
        let principal = gateway_test_actor();
        let capability = format!("service.feishu.drive.download.{suffix}");
        let identity = serde_json::json!({
            "id": format!("idb-preflight-{suffix}"),
            "principal_id": principal,
            "identity_ref": format!("channel://feishu/user/preflight?email={email}"),
            "trust": "verified",
            "source": "test",
            "created_at": "2026-06-07T00:00:00Z",
            "expires_at": null
        });
        let grant = serde_json::json!({
            "id": format!("grant-preflight-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });

        for (uri, body) in [
            ("/api/cross-plane/identities", identity),
            ("/api/cross-plane/grants", grant),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let action = serde_json::json!({
            "actor_identity_ref": format!("channel://wechat/user/preflight?email={email}"),
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "unknown"
        });
        let preflight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/preflight")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::OK);
        let preflight_body = to_bytes(preflight.into_body(), usize::MAX).await.unwrap();
        let preflight_json: serde_json::Value = serde_json::from_slice(&preflight_body).unwrap();
        assert_eq!(preflight_json["kind"], "cross_plane_action_preflight");
        assert_eq!(preflight_json["executable"], true);
        assert_eq!(preflight_json["target_platform"], "feishu");
        assert_eq!(preflight_json["platform_readiness"]["configured"], true);
        assert!(matches!(
            preflight_json["platform_readiness"]["status"].as_str(),
            Some("ready" | "configured")
        ));
        assert_eq!(preflight_json["decision"]["decision"], "allow");
        assert_eq!(preflight_json["action"]["actor_principal"], principal);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_execute_dry_run_audits_without_consuming_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant_id = format!("grant-execute-dry-run-{suffix}");
        let grant = serde_json::json!({
            "id": grant_id,
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-{suffix}"),
            "action": action
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_action_execution");
        assert_eq!(json["status"], "planned");
        assert_eq!(json["dispatch_status"], "dry_run");
        assert_eq!(json["executable"], false);
        assert_eq!(json["dispatched"], false);
        assert!(json["audit_record_id"]
            .as_str()
            .unwrap()
            .starts_with("cpa-"));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        cross_plane_intent_from_action(&json["action"]).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        cross_plane_intent_from_action(&json["action"]).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_execute_replays_idempotency_key_without_duplicate_audit() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-execute-idempotent-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let execute = serde_json::json!({
            "mode": "dry_run",
            "idempotency_key": format!("idem-{suffix}"),
            "action": {
                "actor_identity_ref": null,
                "source_channel": "channel://wechat/chat/test",
                "session_id": "test-session",
                "requested_capability": capability,
                "provider_account": "mock-docs-main",
                "target_ref": null,
                "resource_ref": null,
                "risk": "high",
                "data_classification": "internal",
                "identity_trust": "verified"
            }
        });

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["idempotent_replay"], false);

        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second_body = to_bytes(second.into_body(), usize::MAX).await.unwrap();
        let second_json: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
        assert_eq!(second_json["idempotent_replay"], true);
        assert_eq!(
            second_json["execution_receipt"]["id"],
            first_json["execution_receipt"]["id"]
        );
        assert_eq!(
            second_json["audit_record_id"],
            first_json["audit_record_id"]
        );

        let executions = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/executions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let executions_body = to_bytes(executions.into_body(), usize::MAX).await.unwrap();
        let executions_json: serde_json::Value = serde_json::from_slice(&executions_body).unwrap();
        let matching = executions_json["executions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|receipt| receipt["id"] == first_json["execution_receipt"]["id"])
            .count();
        assert_eq!(matching, 1);
    }

    #[tokio::test]
    async fn cross_plane_execute_commit_blocks_without_live_adapter_and_preserves_grant() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [{
                    "platformType": "feishu",
                    "enabled": true,
                    "app_id": "app-id",
                    "app_secret": "app-secret"
                }]
            }
        })));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-execute-commit-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "single_use",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/test",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": null,
            "resource_ref": null,
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let execute = serde_json::json!({
            "mode": "commit",
            "action": action
        });
        let executed = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/execute")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(execute.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(executed.status(), StatusCode::OK);
        let body = to_bytes(executed.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "blocked");
        assert_eq!(json["dispatch_status"], "adapter_unavailable");
        assert_eq!(json["executable"], false);
        assert_eq!(json["adapter_capability"]["live_supported"], true);
        assert_eq!(json["adapter_capability"]["adapter_bound"], false);
        assert!(json["blockers"].as_array().unwrap().iter().any(|value| {
            value
                .as_str()
                .unwrap_or_default()
                .starts_with("adapter:feishu:send_text:not_bound")
        }));
        assert!(json["execution_graph"].is_null());

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/policy/simulate")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        cross_plane_intent_from_action(&json["action"]).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let first_body = to_bytes(first.into_body(), usize::MAX).await.unwrap();
        let first_json: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
        assert_eq!(first_json["decision"]["decision"], "allow");
    }

    #[tokio::test]
    async fn cross_plane_adapter_registry_reports_supported_and_unsupported_live_operations() {
        let app = api_router(test_state_with_config(serde_json::json!({
            "gateway": {
                "platforms": [
                    {
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    },
                    {
                        "platformType": "wecom",
                        "enabled": true,
                        "corp_id": "corp",
                        "corp_secret": "secret",
                        "agent_id": "agent"
                    }
                ]
            }
        })));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-plane/action/adapters")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["kind"], "cross_plane_action_adapters");
        let capabilities = json["capabilities"].as_array().unwrap();
        assert!(capabilities.iter().any(|item| {
            item["platform"] == "feishu"
                && item["operation"] == "send_text"
                && item["live_supported"] == true
                && item["adapter_bound"] == false
        }));
        assert!(!capabilities
            .iter()
            .any(|item| item["platform"] == "wecom" && item["operation"] == "callback"));
    }

    #[tokio::test]
    async fn cross_plane_preflight_builds_surface_dispatch_target_plan() {
        let app = api_router(test_state_with_config_and_runtime(
            serde_json::json!({
                "gateway": {
                    "platforms": [{
                        "platformType": "feishu",
                        "enabled": true,
                        "app_id": "app-id",
                        "app_secret": "app-secret"
                    }]
                }
            }),
            None,
        ));
        let suffix = uuid::Uuid::new_v4().to_string();
        let principal = gateway_test_actor();
        let capability = format!("message.feishu.send_text.{suffix}");
        let grant = serde_json::json!({
            "id": format!("grant-dispatch-target-{suffix}"),
            "principal_id": principal,
            "capability": capability,
            "account_id": null,
            "target_ref": null,
            "resource_ref": null,
            "source_channel": null,
            "grant_type": "persistent",
            "expires_at": null,
            "remaining_uses": null,
            "created_by": "test",
            "approval_id": null
        });
        let grant_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/grants")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(grant.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(grant_response.status(), StatusCode::OK);

        let action = serde_json::json!({
            "actor_identity_ref": null,
            "source_channel": "channel://wechat/chat/source",
            "session_id": "test-session",
            "requested_capability": capability,
            "provider_account": "mock-docs-main",
            "target_ref": "channel://feishu/user/open-id-1/thread/chat-id-1",
            "resource_ref": "text://hello from cross plane",
            "risk": "high",
            "data_classification": "internal",
            "identity_trust": "verified"
        });
        let preflight = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cross-plane/action/preflight")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(action.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(preflight.status(), StatusCode::OK);
        let body = to_bytes(preflight.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["dispatch_target"]["ready"], true);
        assert_eq!(json["dispatch_target"]["platform"], "feishu");
        assert_eq!(json["dispatch_target"]["operation"], "send_text");
        assert_eq!(
            json["dispatch_target"]["session_key"],
            "feishu:open-id-1:chat-id-1"
        );
        assert_eq!(
            json["dispatch_target"]["outbound_message"]["text"],
            "hello from cross plane"
        );
        assert_eq!(
            json["dispatch_target"]["outbound_message"]["metadata"]["requested_capability"],
            capability
        );
    }
