    #[test]
    fn request_execution_decision_precedes_process_shared_fallback() {
        let mut request = runtime::build_runtime_execution_decision("request-bound turn", None);
        request.turn_ref = Some("turn-request".to_string());
        let request_id = request.decision_id.clone();
        let mut shared =
            runtime::build_runtime_execution_decision("unrelated concurrent turn", None);
        shared.turn_ref = Some("turn-shared".to_string());

        let selected = effective_runtime_execution_decision(Some(&request), Some(shared))
            .expect("request decision");

        assert_eq!(selected.decision_id, request_id);
        assert_eq!(selected.turn_ref.as_deref(), Some("turn-request"));
    }

    #[test]
    fn root_agent_action_actor_inherits_frozen_workspace_evidence_scopes() {
        let mut decision = runtime::build_runtime_execution_decision("root collaboration", None);
        decision.execution_graph_ref = Some("graph-root".to_string());
        decision.session_ref = Some("session-root".to_string());
        decision.turn_ref = Some("turn-root".to_string());
        decision
            .strategy
            .understanding
            .required_workspace_evidence_scopes = vec![
            "read:cowd-dev/Cargo.toml".to_string(),
            "read:cowd-dev/Cargo.toml".to_string(),
        ];
        let admitted_scopes = ["session:session-root".to_string()];
        let binding = RuntimeToolExecutionBinding {
            action_id: Some("root-action"),
            session_id: Some("session-root"),
            authorized_scopes: &admitted_scopes,
            memory_context: None, reality_context: None,
            model_lease: Some("deepseek-v4-flash"),
            parent_execution: None,
            execution_decision: Some(&decision),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        };

        let actor = root_agent_action_actor(binding);
        assert_eq!(
            actor.resource_scopes,
            vec![
                "read:cowd-dev/Cargo.toml".to_string(),
                "session:session-root".to_string(),
            ]
        );
        assert_eq!(actor.root_execution_id.as_deref(), Some("graph-root"));
    }

    #[test]
    fn evidence_scope_allowed_requires_exact_membership() {
        assert!(evidence_scope_allowed(
            &["session:s1".to_string()],
            "session:s1"
        ));
        assert!(!evidence_scope_allowed(
            &["session:s1".to_string()],
            "session:s2"
        ));
        assert!(!evidence_scope_allowed(
            &["session:s1".to_string()],
            "public"
        ));
        assert!(!evidence_scope_allowed(&[], "session:s1"));
    }

    #[test]
    fn governed_web_search_receives_a_runtime_authorization_under_workspace_write() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
        let requests = [runtime::tool_dispatch::ToolRequest {
            tool_use_id: "web-search-1".to_string(),
            tool_name: "web_search".to_string(),
            input: r#"{"query":"rust stable"}"#.to_string(),
            depends_on: Vec::new(),
        }];
        let prepared = executor.prepare_governed_invocations(&requests);
        let invocation = prepared.first().expect("governed invocation");
        assert_eq!(
            invocation.effect.required_permission,
            harness_contract::tool::ToolPermissionMode::ReadOnly
        );
        let negotiator = runtime::AuthorizationNegotiator::new();
        let policy = runtime::PermissionPolicy::new(runtime::PermissionMode::WorkspaceWrite);
        let evaluated = negotiator.assess_effective(
            &policy,
            &runtime::AuthorizationRequest {
                principal_id: "test:web-search".to_string(),
                capability: invocation.effect.tool_id.clone(),
                input: invocation.intent.normalized_input.to_string(),
                idempotency_key: "web-search-request".to_string(),
                effect: invocation.effect.clone(),
                parent_ceiling: runtime::PermissionMode::WorkspaceWrite,
                parent_lease_id: None,
                policy_revision: 1,
                recovery_scope: "web-search-request".to_string(),
                context: runtime::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        );
        let assessment = evaluated.assessment;
        let decision = runtime::ToolPolicy
            .authorize(
                &evaluated.effective,
                &assessment,
                "web-search-request",
                assessment
                    .lease
                    .clone()
                    .expect("read-only web search lease"),
                60,
            )
            .expect("read-only web search must receive a Runtime authorization");
        assert_eq!(decision.authorization.tool_id, "web_search");
    }

    #[test]
    fn production_executor_rejects_invalid_model_input_before_governance() {
        let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());

        let error = executor
            .validate_tool_input("bash", "{}")
            .expect_err("missing bash command must be rejected");
        assert!(error
            .to_string()
            .contains("missing required field `command`"));
        executor
            .validate_tool_input("bash", r#"{"command":"pwd"}"#)
            .expect("valid bash input");
        executor
            .validate_tool_input("enter_plan_mode", "{}")
            .expect("valid no-argument tool");
    }

    #[test]
    fn production_agent_actions_compile_as_independent_internal_transactions() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(crate::runtime_bootstrap::runtime_capability_tool_definitions())
            .expect("Agent action tools");
        let executor = GatewayToolExecutor::new(None, false, registry);
        let requests = [
            runtime::tool_dispatch::ToolRequest {
                tool_use_id: "publish-a".to_string(),
                tool_name: "task_publish".to_string(),
                input: serde_json::json!({
                    "team_ref": "team:a",
                    "title": "A",
                    "objective": "A",
                    "acceptance": "A",
                    "required_capabilities": [],
                    "depends_on": []
                })
                .to_string(),
                depends_on: Vec::new(),
            },
            runtime::tool_dispatch::ToolRequest {
                tool_use_id: "publish-b".to_string(),
                tool_name: "task_publish".to_string(),
                input: serde_json::json!({
                    "team_ref": "team:b",
                    "title": "B",
                    "objective": "B",
                    "acceptance": "B",
                    "required_capabilities": [],
                    "depends_on": []
                })
                .to_string(),
                depends_on: Vec::new(),
            },
        ];
        let workspace = std::env::current_dir().expect("workspace");
        let plan = runtime::GovernedToolCompiler
            .compile(&workspace, &requests, |name, input| {
                executor
                    .registered_tool_effect(name, input)
                    .map(|effect| (effect, 1, "gateway-production-tools".to_string()))
            })
            .expect("production Agent actions compile");

        assert!(plan.tasks.iter().all(|task| {
            task.resource_scope.kind == "internal_transaction"
                && task.predecessors.is_empty()
                && task.conflicts.is_empty()
                && task.can_parallelize
        }));
    }

    #[tokio::test]
    async fn runtime_capabilities_executes_without_mcp_state() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "runtime_capabilities".to_string(),
                description: Some("capability guidance".to_string()),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "intent": { "type": "string" }
                    },
                    "required": ["intent"],
                    "additionalProperties": false
                }),
                required_permission: ToolPermissionMode::ReadOnly,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.readonly",
                ),
            }])
            .expect("runtime tool registry");
        let executor = GatewayToolExecutor::new(None, false, registry);

        let output = executor
            .execute(
                "runtime_capabilities",
                r#"{"intent":"检查 README 是否反映最新架构"}"#,
            )
            .await
            .expect("runtime capabilities should execute without MCP");

        assert!(output.contains("runtime_capabilities"));
        let response: serde_json::Value = serde_json::from_str(&output).expect("capability json");
        assert_eq!(response["architecture"], "agent_first");
        assert_eq!(response["available"], false);
        assert_eq!(response["actions"], json!([]));
        assert_eq!(response["context_retrieval"]["available"], false);
    }

    fn signed_test_authorization(
        executor: &GatewayToolExecutor,
        tool_name: &str,
        input: &serde_json::Value,
        policy_revision: u64,
    ) -> harness_contract::tool::ToolExecutionAuthorization {
        let effect = executor
            .registered_tool_effect(tool_name, input)
            .expect("registered test effect");
        let request_id = format!("security-test:{tool_name}:{policy_revision}");
        let request = runtime::AuthorizationRequest {
            principal_id: "security-test-principal".to_string(),
            capability: effect.tool_id.clone(),
            input: input.to_string(),
            idempotency_key: request_id.clone(),
            effect: effect.clone(),
            parent_ceiling: runtime::PermissionMode::DangerFullAccess,
            parent_lease_id: None,
            policy_revision,
            recovery_scope: request_id.clone(),
            context: runtime::PermissionContext::default(),
            safe_alternatives: Vec::new(),
        };
        let policy = runtime::PermissionPolicy::new(runtime::PermissionMode::DangerFullAccess);
        let negotiator = runtime::AuthorizationNegotiator::new();
        let evaluated = negotiator.assess_effective(&policy, &request);
        let assessment = evaluated.assessment.lease.clone().map_or_else(
            || {
                negotiator.approve_effective(
                    &policy,
                    &request,
                    &evaluated.effective,
                    &harness_contract::policy::ApprovalGrant {
                        grant_id: format!("grant:{request_id}"),
                        approval_id: format!("approval:{request_id}"),
                        scope: harness_contract::policy::ApprovalGrantScope::Once,
                        principal_id: request.principal_id.clone(),
                        profile_id: "security-test".to_string(),
                        workspace_key: "security-test".to_string(),
                        capability: request.capability.clone(),
                        session_id: Some("security-session".to_string()),
                        turn_id: None,
                        task_id: None,
                        invocation_id: Some(request_id.clone()),
                        resource_targets: Vec::new(),
                        effect_descriptor_hash: Some(effect.descriptor_hash.clone()),
                        risk_ceiling: harness_contract::core::TaskRisk::Critical,
                        policy_revision,
                        status: harness_contract::policy::ApprovalGrantStatus::Active,
                        issued_by: harness_contract::policy::ApprovalDecisionActor {
                            kind: harness_contract::policy::ApprovalDecisionActorKind::Human,
                            actor_id: "security-test-human".to_string(),
                        },
                        created_at_ms: 1,
                        expires_at_ms: None,
                        revoked_at_ms: None,
                        revoke_reason: None,
                    },
                )
            },
            |_| evaluated.assessment.clone(),
        );
        runtime::ToolPolicy
            .authorize(
                &evaluated.effective,
                &assessment,
                request_id,
                assessment
                    .lease
                    .clone()
                    .expect("signed authorization lease"),
                30,
            )
            .expect("authorized test effect")
            .authorization
    }

    async fn execute_signed_test_tool(
        executor: &GatewayToolExecutor,
        tool_name: &str,
        input: &str,
        policy_revision: u64,
    ) -> Result<String, ToolError> {
        let value = serde_json::from_str(input).expect("test tool input JSON");
        let authorization = signed_test_authorization(executor, tool_name, &value, policy_revision);
        executor
            .execute_authorized(&authorization, tool_name, input)
            .await
    }

    #[tokio::test]
    async fn external_writes_require_runtime_authorization() {
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![
                RuntimeToolDefinition {
                    name: "mcp_tool".to_string(),
                    description: Some("security test MCP write".to_string()),
                    input_schema: json!({"type":"object","additionalProperties":true}),
                    required_permission: ToolPermissionMode::DangerFullAccess,
                    effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                        "runtime.external_danger",
                    ),
                },
                RuntimeToolDefinition {
                    name: "lark_cli_write".to_string(),
                    description: Some("security test Lark write".to_string()),
                    input_schema: json!({"type":"object","additionalProperties":true}),
                    required_permission: ToolPermissionMode::DangerFullAccess,
                    effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                        "runtime.external_danger",
                    ),
                },
            ])
            .expect("security control registry");
        let executor = GatewayToolExecutor::new(None, false, registry);
        let services = runtime::RuntimeServices::in_memory().expect("runtime services");
        services.publish_session_execution_policy(
            "security-session",
            runtime::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Yolo,
                    7,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        executor
            .bind_runtime_services(Arc::clone(&services))
            .expect("bind security runtime services");

        let make_request = |tool_name: &str, input: serde_json::Value| {
            let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
                &runtime::tool_dispatch::ToolRequest {
                    tool_use_id: format!("security:{tool_name}"),
                    tool_name: tool_name.to_string(),
                    input: input.to_string(),
                    depends_on: Vec::new(),
                },
            );
            request.category = runtime::ToolSafetyCategory::WriteLocal;
            request.session_id = Some("security-session".to_string());
            request.policy_revision = 7;
            request.sandbox_posture = harness_contract::policy::SandboxPosture::HostFullAccess;
            request
        };
        for (tool_name, input) in [
            (
                "mcp_tool",
                json!({"qualifiedName":"server.write","arguments":{}}),
            ),
            ("lark_cli_write", json!({"args":["base","record","create"]})),
        ] {
            let direct = executor
                .execute(tool_name, &input.to_string())
                .await
                .expect_err("direct ToolExecutor write must not bypass Runtime authorization");
            assert!(direct
                .to_string()
                .contains("requires Runtime authorization"));
            let request = make_request(tool_name, input);
            let outcome =
                runtime::RuntimeExecutionHost::execute_runtime_tool(&executor, &request).await;
            assert_eq!(
                outcome.status,
                runtime::RuntimeToolExecutionStatus::BlockedPermission
            );
            assert!(outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("requires signed Runtime authorization")));
        }
    }
