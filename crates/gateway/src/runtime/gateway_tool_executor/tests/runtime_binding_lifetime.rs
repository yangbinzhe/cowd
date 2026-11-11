mod runtime_binding_lifetime {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct SettlementAdapter {
        calls: AtomicUsize,
        entered: tokio::sync::Notify,
        release: Mutex<std::sync::mpsc::Receiver<()>>,
        fail_after_effect: bool,
    }

    impl mcp::McpService for SettlementAdapter {
        fn list_servers(&self) -> Result<Vec<mcp::McpServerProjection>, mcp::McpServiceError> {
            Err(mcp::McpServiceError::Unavailable(
                "fixture only".to_string(),
            ))
        }

        fn server(&self, name: &str) -> Result<mcp::McpServerProjection, mcp::McpServiceError> {
            Err(mcp::McpServiceError::NotFound(name.to_string()))
        }

        fn health(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Err(mcp::McpServiceError::Unavailable(
                "fixture only".to_string(),
            ))
        }

        fn reload_config(&self) -> Result<serde_json::Value, mcp::McpServiceError> {
            Err(mcp::McpServiceError::Unavailable(
                "fixture only".to_string(),
            ))
        }

        fn list_tools(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpToolProjection>, mcp::McpServiceError> {
            Err(mcp::McpServiceError::Unavailable(
                "fixture only".to_string(),
            ))
        }

        fn list_resources(
            &self,
            _server: Option<&str>,
        ) -> Result<Vec<mcp::McpResourceProjection>, mcp::McpServiceError> {
            Err(mcp::McpServiceError::Unavailable(
                "fixture only".to_string(),
            ))
        }

        fn read_resource(
            &self,
            _server: &str,
            uri: &str,
        ) -> Result<mcp::McpResourceProjection, mcp::McpServiceError> {
            Err(mcp::McpServiceError::NotFound(uri.to_string()))
        }

        fn call_tool(
            &self,
            request: mcp::McpToolCallRequest,
        ) -> Result<mcp::McpToolCallReceipt, mcp::McpServiceError> {
            assert_eq!(request.server, "lifetime");
            assert_eq!(request.tool, "effect");
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            self.release
                .lock()
                .expect("adapter gate")
                .recv_timeout(Duration::from_secs(10))
                .map_err(|error| mcp::McpServiceError::Request(error.to_string()))?;
            if self.fail_after_effect {
                return Err(mcp::McpServiceError::Request(
                    "effect applied but response lost".to_string(),
                ));
            }
            Ok(mcp::McpToolCallReceipt {
                server: request.server,
                tool: request.tool,
                ok: true,
                output: json!({"applied": true}),
            })
        }
    }

    struct RuntimeBindingFixture {
        root: tempfile::TempDir,
        executor: Arc<GatewayToolExecutor>,
        services: Arc<runtime::RuntimeServices>,
        adapter: Arc<SettlementAdapter>,
        release: std::sync::mpsc::SyncSender<()>,
    }

    fn runtime_binding_fixture(fail_after_effect: bool) -> RuntimeBindingFixture {
        let root = tempfile::tempdir().expect("isolated lifetime fixture");
        let (release, receiver) = std::sync::mpsc::sync_channel(1);
        let adapter = Arc::new(SettlementAdapter {
            calls: AtomicUsize::new(0),
            entered: tokio::sync::Notify::new(),
            release: Mutex::new(receiver),
            fail_after_effect,
        });
        let registry = GatewayToolRegistry::builtin()
            .with_runtime_tools(vec![RuntimeToolDefinition {
                name: "mcp_tool".to_string(),
                description: Some("controlled lifetime effect".to_string()),
                input_schema: json!({"type":"object","additionalProperties":true}),
                required_permission: ToolPermissionMode::DangerFullAccess,
                effect_resolver: crate::runtime_bootstrap::runtime_effect_resolver(
                    "runtime.external_danger",
                ),
            }])
            .expect("lifetime tool catalog");
        let tool_host = Arc::new(
            ToolHost::new(
                "lifetime",
                root.path(),
                ToolHostSnapshot::new(
                    Arc::new(registry),
                    Arc::new(tools::lsp_client::LspRegistry::new()),
                    Some(adapter.clone()),
                ),
            )
            .with_authorization_lease_verifier(Arc::new(
                runtime::AuthorizationNegotiator::verify_lease_signature,
            )),
        );
        let executor = Arc::new(GatewayToolExecutor::from_tool_host(None, false, tool_host));
        let services = runtime::RuntimeServices::builder(root.path().join("home"), root.path())
            .non_durable_backends_for_testing(root.path())
            .tool_execution_host(executor.clone())
            .build()
            .expect("Runtime with real Gateway host ownership");
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
        RuntimeBindingFixture {
            root,
            executor,
            services,
            adapter,
            release,
        }
    }

    async fn drain_runtime_binding_fixture(services: &Arc<runtime::RuntimeServices>) {
        let report = services.shutdown_execution().await;
        assert_eq!(report.forced_aborts, 0, "{report:?}");
        assert_eq!(report.remaining_keys, 0, "{report:?}");
        assert!(report.errors.is_empty(), "{report:?}");
        services.shutdown_maintenance().await;
    }

    fn running_lifetime_fence(
        services: &runtime::RuntimeServices,
    ) -> harness_contract::managed_agent::ManagedAgentInvocationFence {
        use harness_contract::agent::{
            AgentCapability, AgentDefinitionId, DefinitionScope, RevisionSelector,
        };
        use harness_contract::managed_agent::{
            ManagedAgentDefinition, ManagedAgentHealthPolicy, ManagedAgentOverlapPolicy,
            ManagedAgentRetryPolicy, ManagedAgentTarget, ManagedAgentTrigger,
        };
        let dispatcher = services.managed_agents();
        let managed_agent_id = "workspace/cowd/lifetime";
        dispatcher
            .register_definition(
                ManagedAgentDefinition {
                    managed_agent_id: managed_agent_id.to_string(),
                    revision: 1,
                    target: ManagedAgentTarget::Agent {
                        definition_id: AgentDefinitionId::new(
                            DefinitionScope::Workspace,
                            "cowd/lifetime",
                        )
                        .expect("definition identity"),
                        selector: RevisionSelector::LatestApprovedStable,
                    },
                    trigger: ManagedAgentTrigger::Manual,
                    session_id: "security-session".to_string(),
                    objective: "settle exactly one controlled effect".to_string(),
                    acceptance: vec!["one fenced receipt".to_string()],
                    permission_ceiling: harness_contract::policy::PermissionMode::DangerFullAccess,
                    model_lease: "fixture-no-provider".to_string(),
                    granted_capabilities: vec![AgentCapability::Read, AgentCapability::Write],
                    allowed_tool_contract_refs: vec!["tool/mcp_tool".to_string()],
                    allowed_skill_refs: Vec::new(),
                    resource_scopes: vec!["workspace".to_string()],
                    overlap_policy: ManagedAgentOverlapPolicy::Forbid,
                    retry_policy: ManagedAgentRetryPolicy::default(),
                    health_policy: ManagedAgentHealthPolicy::default(),
                    enabled: true,
                },
                1,
            )
            .expect("managed definition");
        let invocation = dispatcher
            .trigger_manual(managed_agent_id, "lifetime-manual", 2)
            .expect("manual invocation");
        let claim = dispatcher
            .claim_ready("lifetime-dispatcher", 3, 30_000, 1)
            .expect("claim invocation")
            .pop()
            .expect("one claim");
        let claim_token = claim.claim_token.as_deref().expect("claim token");
        dispatcher
            .begin_graph_registration(
                &invocation.invocation_id,
                "lifetime-dispatcher",
                claim.fence_generation,
                claim_token,
                "lifetime-run".to_string(),
            )
            .expect("graph intent");
        dispatcher
            .materialize_invocation(
                &invocation.invocation_id,
                "lifetime-dispatcher",
                claim.fence_generation,
                claim_token,
                "lifetime-run".to_string(),
                "graph-receipt:lifetime-run".to_string(),
            )
            .expect("graph receipt");
        dispatcher
            .start_invocation(
                &invocation.invocation_id,
                "lifetime-dispatcher",
                claim.fence_generation,
                claim_token,
                "lifetime-run".to_string(),
                4,
            )
            .expect("running invocation");
        harness_contract::managed_agent::ManagedAgentInvocationFence {
            managed_agent_id: managed_agent_id.to_string(),
            definition_revision: 1,
            invocation_id: invocation.invocation_id,
            attempt_no: claim.attempt_no,
            fence_generation: claim.fence_generation,
            dispatcher_id: "lifetime-dispatcher".to_string(),
        }
    }

    fn lifetime_tool_request(
        executor: &GatewayToolExecutor,
        tool_name: &str,
        input: serde_json::Value,
    ) -> runtime::RuntimeToolExecutionRequest {
        let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(
            &runtime::tool_dispatch::ToolRequest {
                tool_use_id: format!("lifetime-{tool_name}"),
                tool_name: tool_name.to_string(),
                input: input.to_string(),
                depends_on: Vec::new(),
            },
        );
        request.session_id = Some("security-session".to_string());
        request.policy_revision = 7;
        request.sandbox_posture = harness_contract::policy::SandboxPosture::HostFullAccess;
        request.category = runtime::ToolSafetyCategory::WriteLocal;
        request.authorization = Some(signed_test_authorization(executor, tool_name, &input, 7));
        attach_gateway_step(executor, &mut request);
        request
    }

    fn lifetime_effect_request(
        executor: &GatewayToolExecutor,
        fence: harness_contract::managed_agent::ManagedAgentInvocationFence,
    ) -> runtime::RuntimeToolExecutionRequest {
        let mut request = lifetime_tool_request(
            executor,
            "mcp_tool",
            json!({"qualifiedName":"mcp__lifetime__effect","arguments":{}}),
        );
        request.managed_invocation = Some(fence);
        request
    }

    #[tokio::test]
    async fn r1_runtime_binding_is_weak_and_single_assignment() {
        let fixture = runtime_binding_fixture(false);
        drain_runtime_binding_fixture(&fixture.services).await;
        let strong_count = Arc::strong_count(&fixture.services);
        assert!(fixture.executor.upgrade_runtime_services().is_none());
        fixture
            .executor
            .bind_runtime_services(&fixture.services)
            .expect("first binding");
        assert_eq!(Arc::strong_count(&fixture.services), strong_count);
        let executor_clone = Arc::clone(&fixture.executor);
        assert!(Arc::ptr_eq(
            &executor_clone.runtime_services,
            &fixture.executor.runtime_services,
        ));
        let upgraded = executor_clone
            .upgrade_runtime_services()
            .expect("live binding");
        assert!(Arc::ptr_eq(&upgraded, &fixture.services));
        drop(upgraded);
        assert_eq!(
            executor_clone.bind_runtime_services(&fixture.services),
            Err("runtime services already bound to gateway tool executor".to_string()),
        );
        let replacement = runtime::RuntimeServices::in_memory().expect("replacement services");
        drain_runtime_binding_fixture(&replacement).await;
        assert!(executor_clone.bind_runtime_services(&replacement).is_err());
        drop(fixture.services);
        assert!(fixture.executor.upgrade_runtime_services().is_none());
        assert!(executor_clone.upgrade_runtime_services().is_none());
        assert!(executor_clone.bind_runtime_services(&replacement).is_err());
    }

    #[tokio::test]
    async fn r1_runtime_binding_expiry_refuses_new_effect() {
        let fixture = runtime_binding_fixture(false);
        fixture
            .executor
            .bind_runtime_services(&fixture.services)
            .expect("bind services");
        let live_write = lifetime_tool_request(
            &fixture.executor,
            "edit",
            json!({"action":"write","path":"live-effect.txt","content":"settled live write"}),
        );
        assert!(live_write.managed_invocation.is_none());
        let live_outcome = runtime::RuntimeExecutionHost::execute_runtime_tool(
            fixture.executor.as_ref(),
            &live_write,
        )
        .await;
        assert_eq!(
            live_outcome.status,
            runtime::RuntimeToolExecutionStatus::Executed,
            "{live_outcome:?}"
        );
        let live_path = fixture.root.path().join("live-effect.txt");
        assert_eq!(
            std::fs::read_to_string(&live_path).expect("actual live effect"),
            "settled live write"
        );
        let expired_writes = ["expired-effect.txt", "live-effect.txt"].map(|path| {
            lifetime_tool_request(
                &fixture.executor,
                "edit",
                json!({"action":"write","path":path,"content":"must never apply"}),
            )
        });
        let fence = running_lifetime_fence(&fixture.services);
        let request = lifetime_effect_request(&fixture.executor, fence);
        let dispatcher = Arc::clone(fixture.services.managed_agents());
        let weak_services = Arc::downgrade(&fixture.services);
        drain_runtime_binding_fixture(&fixture.services).await;
        drop(fixture.services);
        assert!(weak_services.upgrade().is_none());
        assert!(fixture.executor.upgrade_runtime_services().is_none());
        for expired_request in expired_writes.into_iter().chain([request]) {
            let outcome = runtime::RuntimeExecutionHost::execute_runtime_tool(
                fixture.executor.as_ref(),
                &expired_request,
            )
            .await;
            assert_eq!(
                outcome.status,
                runtime::RuntimeToolExecutionStatus::BlockedPermission,
                "{outcome:?}"
            );
            assert!(outcome
                .error
                .as_deref()
                .is_some_and(|error| error.contains("no active Session policy")));
            assert!(outcome.output.is_none());
            assert!(outcome.observed_evidence.is_empty());
        }
        assert!(!fixture.root.path().join("expired-effect.txt").exists());
        assert_eq!(
            std::fs::read_to_string(live_path).expect("original live effect"),
            "settled live write"
        );
        assert_eq!(fixture.adapter.calls.load(Ordering::SeqCst), 0);
        assert!(dispatcher
            .outbox()
            .expect("no new effect intent")
            .is_empty());
    }

    #[tokio::test]
    async fn r1_runtime_binding_inflight_owner_survives_to_settlement() {
        for (managed, fail_after_effect) in
            [(true, false), (true, true), (false, false), (false, true)]
        {
            let fixture = runtime_binding_fixture(fail_after_effect);
            fixture
                .executor
                .bind_runtime_services(&fixture.services)
                .expect("bind services");
            let fence = running_lifetime_fence(&fixture.services);
            let mut request = lifetime_effect_request(&fixture.executor, fence.clone());
            if !managed {
                request.managed_invocation = None;
            }
            let dispatcher = Arc::clone(fixture.services.managed_agents());
            let weak_services = Arc::downgrade(&fixture.services);
            drain_runtime_binding_fixture(&fixture.services).await;
            let executing_host = Arc::clone(&fixture.executor);
            let executing_request = request.clone();
            let operation = tokio::spawn(async move {
                runtime::RuntimeExecutionHost::execute_runtime_tool(
                    executing_host.as_ref(),
                    &executing_request,
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(5), fixture.adapter.entered.notified())
                .await
                .expect("real executor reached controlled effect");
            let claimed = dispatcher.outbox().expect("claimed effect");
            if managed {
                assert_eq!(claimed.len(), 1);
                assert_eq!(claimed[0].status, runtime::FencedEffectStatus::Claimed);
            } else {
                assert!(claimed.is_empty());
            }
            drop(fixture.services);
            assert!(weak_services.strong_count() > 0);
            for invalid_binding in ["policy", "authorization", "fence"] {
                let mut denied_request = request.clone();
                match invalid_binding {
                    "policy" => denied_request.policy_revision += 1,
                    "authorization" => denied_request.authorization = None,
                    _ => {
                        denied_request.managed_invocation = Some(
                            harness_contract::managed_agent::ManagedAgentInvocationFence {
                                fence_generation: fence.fence_generation + 1,
                                ..fence.clone()
                            },
                        )
                    }
                }
                let denied = runtime::RuntimeExecutionHost::execute_runtime_tool(
                    fixture.executor.as_ref(),
                    &denied_request,
                )
                .await;
                assert_eq!(
                    denied.status,
                    runtime::RuntimeToolExecutionStatus::BlockedPermission,
                    "{invalid_binding}: {denied:?}"
                );
                assert!(denied.output.is_none());
            }
            assert_eq!(fixture.adapter.calls.load(Ordering::SeqCst), 1);
            assert_eq!(dispatcher.outbox().expect("unchanged claim"), claimed);
            fixture.release.send(()).expect("release effect result");
            let outcome = tokio::time::timeout(Duration::from_secs(5), operation)
                .await
                .expect("settlement bounded")
                .expect("executor task");
            let settled = dispatcher.outbox().expect("settled effect");
            let expected_status = if fail_after_effect {
                runtime::RuntimeToolExecutionStatus::Failed
            } else {
                runtime::RuntimeToolExecutionStatus::Executed
            };
            assert_eq!(outcome.status, expected_status, "{outcome:?}");
            if managed {
                assert_eq!(settled.len(), 1);
                if fail_after_effect {
                    assert_eq!(
                        settled[0].status,
                        runtime::FencedEffectStatus::ReconciliationRequired
                    );
                    assert!(settled[0].receipt_ref.is_none());
                    assert!(settled[0]
                        .error
                        .as_deref()
                        .is_some_and(|error| error.contains("effect applied but response lost")));
                } else {
                    assert_eq!(settled[0].status, runtime::FencedEffectStatus::Completed);
                    assert_eq!(
                        settled[0].receipt_ref.as_deref(),
                        Some(outcome.evidence_ref.as_str())
                    );
                }
            } else {
                assert!(settled.is_empty());
            }
            assert_eq!(fixture.adapter.calls.load(Ordering::SeqCst), 1);
            assert!(weak_services.upgrade().is_none());
            let refused = runtime::RuntimeExecutionHost::execute_runtime_tool(
                fixture.executor.as_ref(),
                &request,
            )
            .await;
            assert_eq!(
                refused.status,
                runtime::RuntimeToolExecutionStatus::BlockedPermission
            );
            assert_eq!(fixture.adapter.calls.load(Ordering::SeqCst), 1);
            assert_eq!(dispatcher.outbox().expect("no replayed effect"), settled);
        }
    }
}
