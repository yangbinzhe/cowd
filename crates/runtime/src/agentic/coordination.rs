//! Pure queries over the original Program journal; no second queue or writer.
use super::program::*;
use harness_contract::agent_action::{AgentAttemptMode, TaskIntentKind};
use std::collections::BTreeSet;

fn request_key(topic: &str, entry: &AgenticTopicEntryProjection) -> String {
    let mut refs = entry.refs.clone();
    refs.sort();
    refs.dedup();
    let mut recipients = entry.recipients.clone();
    recipients.sort();
    recipients.dedup();
    let mut intent = entry.intent.clone();
    if let Some(intent) = intent.as_mut() {
        intent.requested_capability_refs.sort();
        intent.requested_capability_refs.dedup();
    }
    serde_json::json!([
        topic,
        entry.actor_id,
        entry.intent_generation,
        entry.summary.as_deref().map(str::trim),
        entry.content_ref,
        refs,
        recipients,
        intent
    ])
    .to_string()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use harness_contract::agent::{AgentTaskPacket, AgenticExecutionFocus};
    use harness_contract::agent_action::*;
    use std::sync::Arc;

    pub(crate) struct Fixture {
        pub services: Arc<crate::RuntimeServices>,
        pub request: AgentActionEnvelope,
        pub packet: AgentTaskPacket,
        pub task_ref: String,
        pub wake_ref: String,
        pub workspace: tempfile::TempDir,
        pub primary_execution: String,
    }

    impl Fixture {
        /// Unique per fixture instance.
        ///
        /// The repetition ledger is keyed on the turn, so fixtures that share one
        /// turn share one breaker: one fixture's failures refused another fixture's
        /// call, and a test that asserts the framework does not extend the model
        /// loop was extended by a different test's failures.
        pub fn turn_id(&self) -> String {
            format!("coord-turn-{}", self.workspace.path().display())
        }
    }

    #[derive(Default)]
    struct ActionHost(
        std::sync::OnceLock<std::sync::Weak<crate::RuntimeServices>>,
        Option<tools::ToolHost>,
    );
    // The test controls the decision/Process exchange explicitly. Keep the
    // real Runner's backend pending so it cannot race that exchange by trying
    // the unconfigured test model (or a fixture script not yet written).
    pub(crate) struct ControlledAgenticWorker;
    impl crate::execution_core::graph::executors::AgentTaskBackendResolver for ControlledAgenticWorker {
        fn resolve(
            &self,
            packet: &AgentTaskPacket,
        ) -> Option<Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>> {
            packet.agentic_binding.as_ref().map(|_| {
                Arc::new(Self) as Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>
            })
        }
    }
    #[async_trait::async_trait]
    impl crate::execution_core::graph::executors::AgentTaskBackend for ControlledAgenticWorker {
        async fn execute(
            &self,
            _: AgentTaskPacket,
        ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
            std::future::pending().await
        }
        async fn cancel(&self, _: &AgentTaskPacket) -> Result<(), String> {
            Ok(())
        }
        fn cancellation_finalized(&self, _: &AgentTaskPacket) {}
    }
    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for ActionHost {
        fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
            self.1.as_ref().map(tools::ToolHost::pin_snapshot)
        }
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            assert!(request.authorization.is_some());
            let services = self.0.get().unwrap().upgrade().unwrap();
            // Real filesystem effects for the process-bridge gate. The child only
            // reaches paths inside the disposable workspace it was leased.
            let tool_input: serde_json::Value =
                serde_json::from_str(&request.input).unwrap_or(serde_json::Value::Null);
            match request.tool_name.as_str() {
                "edit" => {
                    let path = tool_input
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("proof.txt");
                    let content = tool_input
                        .get("content")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("");
                    let target = services.workspace_root().join(path);
                    if let Some(parent) = target.parent() {
                        std::fs::create_dir_all(parent).unwrap();
                    }
                    std::fs::write(&target, content).unwrap();
                    return crate::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Executed,
                        category: request.category,
                        output: Some(
                            serde_json::json!({"path": path, "bytes": content.len()}).to_string(),
                        ),
                        error: None,
                        evidence_ref: format!("process-effect:{}", request.tool_use_id),
                        observed_evidence: vec![],
                        pending_agent_batch: None,
                    };
                }
                "read_file" => {
                    let path = tool_input
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("proof.txt");
                    let target = services.workspace_root().join(path);
                    let content = std::fs::read_to_string(&target).unwrap_or_default();
                    return crate::RuntimeToolExecutionOutcome {
                        tool_use_id: request.tool_use_id.clone(),
                        tool_name: request.tool_name.clone(),
                        status: crate::RuntimeToolExecutionStatus::Executed,
                        category: request.category,
                        output: Some(
                            serde_json::json!({"path": path, "content": content}).to_string(),
                        ),
                        error: None,
                        evidence_ref: format!("process-effect:{}", request.tool_use_id),
                        observed_evidence: vec![],
                        pending_agent_batch: None,
                    };
                }
                _ => {}
            }
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("original action invocation capability required".into()),
                evidence_ref: String::new(),
                observed_evidence: vec![],
                pending_agent_batch: None,
            }
        }

        async fn execute_runtime_tool_with_action_invocation(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
            capability: &crate::agentic::action_invocation::VerifiedActionInvocation,
        ) -> crate::RuntimeToolExecutionOutcome {
            let result: Result<Result<AgentActionObservation, crate::AgentActionServiceError>, String> = async {
                capability.validate_request(request).map_err(str::to_string)?;
                let services = self.0.get().and_then(std::sync::Weak::upgrade)
                    .ok_or("original Runtime unavailable")?;
                let parent = request.parent_execution.as_ref().ok_or("original parent missing")?;
                let attempt = request.parent_execution_attempt.ok_or("original attempt missing")?;
                let actor = services.resolve_agent_action_actor_for_attempt(parent, attempt, None).await?;
                let original_input = harness_contract::step::parse_strict_json(&request.input)
                    .map_err(|error| error.to_string())?;
                let mut action_input = original_input.clone();
                let object = action_input.as_object_mut().ok_or("action input must be an object")?;
                let expected_revision = match object.remove("expected_revision") {
                    None | Some(serde_json::Value::Null) => None,
                    Some(serde_json::Value::Number(value)) => Some(value.as_u64().ok_or("invalid expected revision")?),
                    Some(_) => return Err("invalid expected revision".into()),
                };
                if request.tool_name == "state_inspect" && (object.contains_key("entry_ref") || object.contains_key("scope_ref")) {
                    object.remove("page_cursor");
                    object.remove("query");
                }
                let action: AgentAction = serde_json::from_value(serde_json::json!({
                    "kind":request.tool_name, "input":action_input,
                })).map_err(|error| error.to_string())?;
                let envelope = AgentActionEnvelope {
                    action_id: request.idempotency_key.clone(), actor, expected_revision, action,
                };
                Ok(services.submit_delegated_tool_action(capability, &envelope, &request.tool_name, &original_input).await)
            }.await;
            let (status, output, error) = match result {
                Ok(Err(crate::AgentActionServiceError::MemberUnknown(error))) =>
                    (crate::RuntimeToolExecutionStatus::Unknown, None, Some(error)),
                Ok(Err(error)) => (crate::RuntimeToolExecutionStatus::BlockedPermission, None, Some(error.to_string())),
                Ok(Ok(observation)) if matches!(observation.status, AgentActionStatus::Rejected | AgentActionStatus::Blocked) => {
                    let reason = observation.error.as_ref().map_or_else(
                        || "original action rejected".to_string(),
                        |error| format!("{}: {}", error.code, error.message),
                    );
                    (crate::RuntimeToolExecutionStatus::BlockedPermission, None, Some(reason))
                }
                Ok(Ok(observation)) => (crate::RuntimeToolExecutionStatus::Executed,
                    Some(serde_json::to_string(&observation).expect("action observation serializes")), None),
                Err(error) => (crate::RuntimeToolExecutionStatus::BlockedPermission, None, Some(error)),
            };
            crate::RuntimeToolExecutionOutcome {
                pending_agent_batch: matches!(&status, crate::RuntimeToolExecutionStatus::Unknown).then(||
                    crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::RecordedSink(request.tool_progress.clone()))),
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status,
                category: request.category,
                output,
                error,
                evidence_ref: format!("coordination-action:{}", request.tool_use_id),
                observed_evidence: vec![],
            }
        }
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            _: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            use harness_contract::{policy::*, tool::*};
            let (effect_kind, operation, permission) = match name {
                "edit" => (
                    ToolEffectKind::Write,
                    PermissionOperation::Write,
                    ToolPermissionMode::WorkspaceWrite,
                ),
                "read_file" => (
                    ToolEffectKind::Read,
                    PermissionOperation::Read,
                    ToolPermissionMode::ReadOnly,
                ),
                other if AGENT_ACTION_TOOL_IDS.contains(&other) => (
                    ToolEffectKind::Read,
                    PermissionOperation::Read,
                    ToolPermissionMode::ReadOnly,
                ),
                _ => return None,
            };
            Some(ToolEffectDescriptor {
                tool_id: name.into(),
                descriptor_hash: format!("coord-test:{name}"),
                effect_kind,
                idempotency: ToolIdempotency::Idempotent,
                scopes: vec![PermissionScope::new(PermissionResource::Tool, operation)],
                required_permission: permission,
                approval_class: ToolApprovalClass::None,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: EffectAssessment::default(),
            })
        }
    }

    pub(crate) async fn fixture(missing_graph: bool) -> Fixture {
        fixture_with_executor(missing_graph, None).await
    }

    pub(crate) async fn fixture_with_executor(
        missing_graph: bool,
        process: Option<crate::ProcessJsonlSpec>,
    ) -> Fixture {
        fixture_with_options(missing_graph, process, None, false, false, None, false).await
    }

    /// A TaskExecute-scoped process fixture. An ordinary write needs an active
    /// Task execution claim, which a Coordination focus does not provide.
    pub(crate) async fn fixture_with_process_task(process: crate::ProcessJsonlSpec) -> Fixture {
        fixture_with_options(false, Some(process), None, true, true, None, false).await
    }

    pub(crate) async fn fixture_with_provider(provider: Arc<crate::ProviderRegistry>) -> Fixture {
        fixture_with_options(false, None, Some(provider), false, false, None, false).await
    }

    pub(crate) async fn fixture_with_original_session() -> Fixture {
        fixture_with_options(false, None, None, false, false, None, true).await
    }

    async fn fixture_with_options(
        missing_graph: bool,
        process: Option<crate::ProcessJsonlSpec>,
        provider: Option<Arc<crate::ProviderRegistry>>,
        write: bool,
        task_execute: bool,
        runtime_backend: Option<Arc<dyn crate::AgentRuntimeBackend>>,
        original_session: bool,
    ) -> Fixture {
        let workspace = tempfile::tempdir().unwrap();
        let (permission_ceiling, resource_scopes, required_capabilities) = if write {
            (
                harness_contract::policy::PermissionMode::WorkspaceWrite,
                vec!["read:.".to_string(), "write:.".to_string()],
                vec!["read".to_string(), "write".to_string()],
            )
        } else {
            (
                harness_contract::policy::PermissionMode::ReadOnly,
                vec!["read:.".to_string()],
                vec!["read".to_string()],
            )
        };
        let process_catalog = Some({
            let actions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
                .iter()
                .map(|name| tools::RuntimeToolDefinition {
                    name: name.to_string(),
                    description: Some("Isolated coordination test adapter".into()),
                    input_schema: serde_json::json!({"type":"object"}),
                    required_permission: harness_contract::policy::PermissionMode::ReadOnly,
                    effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                        resolver_id: "runtime.agent_action".into(),
                        resolver_version: 2,
                    },
                })
                .collect();
            let catalog = tools::ToolCatalog::builtin()
                .with_runtime_tools(actions)
                .unwrap();
            tools::ToolHost::new(
                "process-coordination-fixture",
                workspace.path(),
                tools::ToolHostSnapshot::new(
                    Arc::new(catalog),
                    Arc::new(tools::lsp_client::LspRegistry::default()),
                    None,
                ),
            )
        });
        let host = Arc::new(ActionHost(Default::default(), process_catalog));
        let mut builder =
            crate::RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
                .tool_execution_host(host.clone());
        if let Some(provider) = provider {
            builder = builder.provider_registry(provider);
        }
        let services = builder.build().unwrap();
        host.0.set(Arc::downgrade(&services)).unwrap();
        if let Some(backend) = runtime_backend {
            // This case must retain the production resolver installed by assemble:
            // only AgentRuntime owns the authenticated admission/exit publication.
            services.agent_runtime().register_backend(backend);
        } else {
            services
                .agent_task_executor()
                .install_resolver(Arc::new(ControlledAgenticWorker));
        }
        let process_definition = process.map(|spec| {
            use harness_contract::agent::*;
            services
                .agent_runtime()
                .register_process_jsonl_command(spec.clone())
                .unwrap();
            let registry = services.definition_registry();
            let base = registry
                .resolve_agent(
                    &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
                    RevisionSelector::LatestApprovedStable,
                )
                .unwrap();
            let mut manifest = base.revision.manifest.clone();
            manifest.definition_id =
                AgentDefinitionId::new(DefinitionScope::Workspace, "coord/process").unwrap();
            manifest.executor = AgentExecutorPolicy::ProcessJsonl {
                command_ref: spec.command_ref,
                command_digest: spec.command_digest,
            };
            manifest.model_policy.allowed_models = vec!["test".into()];
            manifest.model_policy.fallback_allowed = false;
            let stored = registry
                .agents()
                .store_revision(manifest, &base.agent_markdown)
                .unwrap();
            registry
                .agents()
                .record_release_assignment(&ReleaseAssignment {
                    scope: DefinitionScope::Workspace,
                    revision_ref: stored.revision.revision_ref.clone(),
                    channel: ReleaseChannel::Stable,
                    status: ReleaseAssignmentStatus::Active,
                    authorization: ReleaseAuthorization::HumanApproval {
                        approval_ref: "test-fixture:coordination-command".into(),
                    },
                    content_digest: stored.revision.content_digest,
                })
                .unwrap();
            services.refresh_definition_catalog().unwrap();
            stored
                .revision
                .revision_ref
                .definition_id
                .as_str()
                .to_string()
        });
        services.publish_session_execution_policy(
            "coord-session",
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Autonomous,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
                ),
            ),
        );
        let turn_id = format!("coord-turn-{}", workspace.path().display());
        let (root_execution_id, _session_bus) = if original_session {
            let store = Arc::new(crate::test_support::session_store());
            let timestamp = chrono::Utc::now().to_rfc3339();
            store.create_session(&session::SessionRecord {
                session_id: "coord-session".into(), platform: "test".into(), chat_id: "coord".into(),
                user_id: None, model: None, created_at: timestamp.clone(), last_activity: timestamp,
                message_count: 0, reset_policy: "manual".into(), metadata_json: None,
                input_tokens: 0, output_tokens: 0, status: "active".into(),
            }).await.unwrap();
            let router = services.install_test_session_store(Arc::clone(&store)).unwrap();
            let admission = store.get_session_input_admission("coord-session").await.unwrap().unwrap();
            let now = chrono::Utc::now().timestamp_millis() as u64;
            let request = crate::RuntimeSessionIngressCommand {
                input_id: "coord-input".into(), request_id: "coord-request".into(), turn_id: turn_id.clone(),
                message_id: "coord-message".into(), session_generation: admission.generation,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None, classification_json: None, task_route_hint: None,
                created_at_ms: now, runtime_options_json: None,
            };
            router.persist_input("coord-session", "coordinate evidence", &request).await.unwrap();
            let claimed = store.claim_session_runtime_outbox("coord-worker", now, 120_000, 1, &[])
                .await.unwrap().pop().expect("original Session claim");
            let running = store.mark_session_runtime_outbox_running(&claimed.request_id, "coord-worker",
                claimed.session_generation, claimed.claim_token.as_deref().unwrap(), claimed.revision, now)
                .await.unwrap();
            let ingress = crate::TurnIngressRef {
                request_id: running.request_id.clone(), turn_id: running.turn_id.clone(),
                message_id: running.message_id.clone(), session_id: running.session_id.clone(),
                primary_task_id: "coord-task".into(), root_task_id: "coord-task".into(),
                session_generation: running.session_generation, input_sequence: running.sequence as u64,
                claim_owner: running.claim_owner.clone().unwrap(), claim_token: running.claim_token.clone().unwrap(),
                claim_revision: running.claim_fence_epoch.unwrap(), handoff_id: None,
            };
            let execution_id = crate::session_ingress_graph_id(&ingress.session_id, &ingress.request_id, &ingress.turn_id);
            let control = services.create_turn_cancellation_control(&execution_id, &ingress).unwrap();
            services.record_live_execution(&ingress.session_id, execution_id.clone(), ingress.turn_id.clone());
            let lease = services.bind_active_execution_bus(execution_id.clone(), crate::CowdEventBus::new()).unwrap();
            let fence = crate::SessionExecutionFence::from_claim(services.session_query_port().unwrap(),
                ingress.request_id.clone(), ingress.session_id.clone(), ingress.session_generation,
                ingress.input_sequence as usize, ingress.claim_owner.clone(), ingress.claim_token.clone()).unwrap();
            let input = crate::SessionInputStream::new(ingress.session_id.clone());
            input.set_active_turn(Some(harness_contract::turn::TurnId::from_string(&ingress.turn_id)));
            lease.install_session_fence(&ingress, fence, control, input,
                Arc::new(crate::conversation::OriginalTurnInputControl::default()),
                crate::permissions::PermissionPolicy::new(crate::permissions::PermissionMode::ReadOnly)).unwrap();
            (Some(execution_id), Some(lease))
        } else { (None, None) };
        let actions = services.agent_action_service();
        let root = AgentActorBinding {
            original_execution: None,
            objective_id: "coord-objective".into(),
            program_id: "coord-program".into(),
            session_id: "coord-session".into(),
            turn_id,
            root_execution_id,
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "coordinate evidence".into(),
            model_lease: "test".into(),
            permission_ceiling: Some(permission_ceiling),
            resource_scopes: resource_scopes.clone(),
            actor_id: "root:coord-session".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let apply = |id: &str, actor: &AgentActorBinding, action| {
            let out = actions
                .apply(&AgentActionEnvelope {
                    action_id: id.into(),
                    actor: actor.clone(),
                    expected_revision: None,
                    action,
                })
                .unwrap();
            assert_eq!(out.status, AgentActionStatus::Applied, "{out:?}");
            out
        };
        let team = apply(
            "team",
            &root,
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Evidence".into(),
                mission: "check sources".into(),
                objective: None,
            }),
        )
        .changed_refs[0]
            .clone();
        let invite = |id: &str| {
            apply(id, &root, AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":team,"role":"Reader","mission":"check evidence","required_capabilities":required_capabilities,
            "definition_ref":if id=="helper" {process_definition.as_deref()} else {None}
        })).unwrap())).changed_refs[0].clone()
        };
        let owner = invite("owner");
        let helper = invite("helper");
        let task_ref = apply("task", &root, AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
            "team_ref":team,"title":"Evidence","objective":"verify source","acceptance":"source checked","required_capabilities":required_capabilities
        })).unwrap())).changed_refs[0].clone();
        if task_execute {
            // G29 open: the synthetic fixture cannot yet produce a claim whose
            // status/generation/active-attempt matches effect_authority. Port the
            // agentic/execution harness claim+dispatch sequence here.
            let receipts = services
                .dispatch_ready_agentic_work("coord-program")
                .await
                .unwrap();
            assert_eq!(receipts.len(), 1, "{receipts:?}");
            let graph = services
                .graph_state_store()
                .load(&receipts[0].graph_id)
                .unwrap();
            let packet: AgentTaskPacket =
                serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
            assert!(matches!(
                packet.agentic_binding.as_ref().unwrap().focus,
                AgenticExecutionFocus::TaskExecute { .. }
            ));
            return Fixture {
                services,
                request: AgentActionEnvelope {
                    action_id: "process-task".into(),
                    actor: root,
                    expected_revision: None,
                    action: AgentAction::StateInspect(
                        serde_json::from_value(serde_json::json!({})).unwrap(),
                    ),
                },
                packet,
                task_ref,
                wake_ref: String::new(),
                workspace,
                primary_execution: receipts[0].graph_id.clone(),
            };
        }
        // The primary is a real production dispatch: the physical-identity gate
        // authenticates the running graph, its binding and start ticket, so the
        // author is resolved from the dispatched attempt, not a bare synthetic claim.
        let primary_receipts = services
            .dispatch_ready_agentic_work("coord-program")
            .await
            .unwrap();
        assert_eq!(primary_receipts.len(), 1, "{primary_receipts:?}");
        assert_eq!(primary_receipts[0].mode, "execute");
        let primary_execution = primary_receipts[0].graph_id.clone();
        let primary_graph = services
            .graph_state_store()
            .load(&primary_execution)
            .unwrap();
        let primary_node = primary_graph.nodes[0].id.clone();
        let primary_attempt = primary_graph.recovery_cursor.node_attempts[&primary_node];
        let author = services
            .resolve_agent_action_actor_for_attempt(
                &harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: primary_execution.clone(),
                    node_id: primary_node,
                },
                primary_attempt,
                Some(root.clone()),
            )
            .await
            .unwrap();
        if actions
            .project("coord-program")
            .unwrap()
            .tasks[&task_ref]
            .claim_execution_id
            .is_none()
        {
            apply(
                "primary-claim",
                &author,
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task_ref.clone(),
                    reason: Some("own the original work".into()),
                }),
            );
        }
        let request = AgentActionEnvelope {action_id:"help-request".into(),actor:author,expected_revision:None,
            action:AgentAction::MessagePublish(serde_json::from_value(serde_json::json!({
                "topic_ref":format!("topic:{team}"),"summary":"Check the boundary condition in source A","recipients":[helper],
                "intent":{"kind":"request_help","task_ref":task_ref}
            })).unwrap())};
        let result = actions.apply(&request).unwrap();
        assert_eq!(result.status, AgentActionStatus::Applied, "{result:?}");
        let projection = actions.project("coord-program").unwrap();
        let (_, wake) = projection.coordination_requests()[0];
        let wake_ref = wake.entry_id.clone();
        let expected_graph = if missing_graph {
            use sha2::Digest;
            let graph_id = format!(
                "agentic-graph:{:x}",
                sha2::Sha256::digest(
                    format!(
                        "coord-program|{task_ref}|{helper}|coordination|{}",
                        wake.revision
                    )
                    .as_bytes()
                )
            );
            let mut supervisor = root.clone();
            supervisor.actor_id = "runtime.program-supervisor".into();
            supervisor.kind = AgentActorKind::Supervisor;
            supervisor.execution_id = Some(graph_id.clone());
            let registered = actions
                .apply_runtime_supervisor(&AgentActionEnvelope {
                    action_id: "registered-before-crash".into(),
                    actor: supervisor,
                    expected_revision: None,
                    action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                        task_ref: task_ref.clone(),
                        execution_id: graph_id.clone(),
                        agent_ref: helper.clone(),
                        membership_id: AgenticProgramProjection::membership_id(&helper, &team),
                        mode: AgentAttemptMode::Coordination,
                        generation: wake.revision,
                    }),
                })
                .unwrap();
            assert_eq!(registered.status, AgentActionStatus::Applied, "{registered:?}");
            Some(graph_id)
        } else {
            None
        };
        let receipts = if missing_graph {
            services
                .recover_agentic_programs_on_startup()
                .await
                .unwrap()
        } else {
            services
                .dispatch_agentic_followups(
                    &request,
                    crate::AgenticDispatchContext {
                        session_id: root.session_id,
                        turn_id: root.turn_id,
                        model_lease: root.model_lease,
                        permission_ceiling,
                        resource_scopes: root.resource_scopes,
                    },
                )
                .await
                .unwrap()
        };
        assert_eq!(receipts.len(), 1, "{receipts:?}");
        assert_eq!(receipts[0].mode, "coordination");
        if let Some(expected) = expected_graph {
            assert_eq!(receipts[0].graph_id, expected);
        }
        let graph = services
            .graph_state_store()
            .load(&receipts[0].graph_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
        assert!(
            matches!(&packet.agentic_binding.as_ref().unwrap().focus,AgenticExecutionFocus::Coordination{wake_ref:ref id} if id==&wake_ref)
        );
        Fixture {
            services,
            request,
            packet,
            task_ref,
            wake_ref,
            workspace,
            primary_execution,
        }
    }

    pub(crate) async fn actor(fixture: &Fixture) -> AgentActorBinding {
        let graph = fixture
            .services
            .graph_state_store()
            .load(fixture.packet.graph_id())
            .unwrap();
        let node_id = fixture.packet.node_id();
        let attempt = graph.recovery_cursor.node_attempts[node_id];
        fixture
            .services
            .resolve_agent_action_actor_for_attempt(
                &harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: fixture.packet.graph_id().into(),
                    node_id: fixture.packet.node_id().into(),
                },
                attempt,
                None,
            )
            .await
            .unwrap()
    }

    fn context(envelope: &AgentActionEnvelope) -> crate::AgenticDispatchContext {
        crate::AgenticDispatchContext {
            session_id: envelope.actor.session_id.clone(),
            turn_id: envelope.actor.turn_id.clone(),
            model_lease: envelope.actor.model_lease.clone(),
            permission_ceiling: envelope.actor.permission_ceiling.unwrap(),
            resource_scopes: envelope.actor.resource_scopes.clone(),
        }
    }

    // Migration (034 judgment-chain closure): the four original coordination
    // real-PG topologies were moved out of this runtime module into explicit
    // ignored Gateway real-PG tests (same frozen base 2dcd87f1):
    //   crates/gateway/src/runtime/tests/coordination_original_pg.rs
    //   runtime_service::tests::coordination_original_pg::{
    //     coordination_visibility_respects_active_teams_recipients_and_program_topic,
    //     coordination_failure_and_epoch_change_do_not_punish_primary_work,
    //     coordination_missing_graph_recovers_same_registered_attempt,
    //     coordination_request_coalesces_and_preserves_scope}
    // The three default controls below exercise only the Program reducer and
    // visibility queries; they do not prove real Session, dispatch, or actor
    // admission. missing-graph startup recovery is an integration contract with
    // no pure equivalent. Mapping: plan/2026-11/1113-cowd-judgment-chain-closure/
    // coordination-fixture-pg-migration.md.
    // These tests exercise only the Program reducer and visibility queries;
    // Session/physical admission is covered by the separate PG tests below.
    fn business_case() -> (AgenticProgramProjection, AgentActionEnvelope, String, String, String, String) {
        let actions = crate::AgentActionService::new(Arc::new(crate::RuntimeEventStore::for_test()));
        let root = AgentActorBinding {
            original_execution: None, objective_id: "business-objective".into(), program_id: "business-program".into(),
            session_id: "business-session".into(), turn_id: "business-turn".into(), root_execution_id: None,
            required_team_count: 0, exact_team_count: None, objective_summary: "coordinate".into(),
            model_lease: "test".into(), permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec!["read:.".into()], actor_id: "root:business-session".into(), kind: AgentActorKind::Root,
            execution_id: None, team_id: None, agent_id: None,
        };
        let apply_root = |id: &str, action| actions.apply(&AgentActionEnvelope {
            action_id: id.into(), actor: root.clone(), expected_revision: None, action,
        }).unwrap().changed_refs[0].clone();
        let team = apply_root("business-team", AgentAction::TeamCreate(TeamCreateInput {
            name: "Evidence".into(), mission: "check".into(), objective: None,
        }));
        let foreign_team = apply_root("business-foreign-team", AgentAction::TeamCreate(TeamCreateInput {
            name: "Other".into(), mission: "check".into(), objective: None,
        }));
        let invite = |id: &str, target: &str| apply_root(id, AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":target,"role":"Reader","mission":"check","required_capabilities":["read"]
        })).unwrap()));
        let owner = invite("business-owner", &team);
        let helper = invite("business-helper", &team);
        let foreign = invite("business-foreign", &foreign_team);
        let task = apply_root("business-task", AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
            "team_ref":team,"title":"Evidence","objective":"verify","acceptance":"checked","required_capabilities":["read"]
        })).unwrap()));
        let mut projection = actions.project("business-program").unwrap();
        let mut supervisor = root.clone();
        supervisor.kind = AgentActorKind::Supervisor;
        supervisor.actor_id = "runtime.program-supervisor".into();
        supervisor.execution_id = Some("business-primary".into());
        let dispatch = AgentActionEnvelope { action_id: "business-dispatch".into(), actor: supervisor,
            expected_revision: None, action: AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: task.clone(), execution_id: "business-primary".into(), agent_ref: owner.clone(),
                membership_id: AgenticProgramProjection::membership_id(&owner, &team),
                mode: AgentAttemptMode::Execute, generation: 0,
            }) };
        let next = projection.revision + 1;
        projection.apply(&dispatch, None, next, 1);
        let mut author = root;
        author.kind = AgentActorKind::Agent;
        author.actor_id = owner.clone(); author.agent_id = Some(owner);
        author.team_id = Some(team.clone()); author.execution_id = Some("business-primary".into());
        let claim = AgentActionEnvelope {
            action_id: "business-primary-claim".into(), actor: author.clone(), expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput { task_ref: task.clone(), reason: None }),
        };
        let next = projection.revision + 1;
        projection.apply(&claim, None, next, 2);
        assert_eq!(projection.tasks[&task].claim_execution_id.as_deref(), Some("business-primary"));
        let request = AgentActionEnvelope { action_id: "business-help".into(), actor: author, expected_revision: None,
            action: AgentAction::MessagePublish(serde_json::from_value(serde_json::json!({
                "topic_ref":format!("topic:{team}"),"summary":"Check source A", "recipients":[helper],
                "intent":{"kind":"request_help","task_ref":task}
            })).unwrap()) };
        let next = projection.revision + 1;
        projection.apply(&request, Some("message:business-help"), next, 2);
        (projection, request, team, task, foreign_team, foreign)
    }

    #[test]
    fn coordination_business_visibility_uses_team_and_program_scope() {
        let (mut projection, mut request, _team, _task, foreign_team, foreign) = business_case();
        let revision = projection.coordination_requests()[0].1.revision;
        assert!(projection.agent_is_active_in(&foreign, &foreign_team));
        assert!(projection.coordination_team_id_for(&foreign, revision).is_none());
        if let AgentAction::MessagePublish(input) = &mut request.action { input.recipients = vec![foreign.clone()]; }
        request.action_id = "business-private-recipient".into();
        let next = projection.revision + 1;
        projection.apply(&request, Some("message:business-private"), next, 3);
        assert!(projection.coordination_team_id_for(&foreign, next).is_none());
        if let AgentAction::MessagePublish(input) = &mut request.action {
            input.topic_ref = format!("topic:{}", projection.program_id);
            input.recipients = vec![foreign.clone()];
        }
        request.action_id = "business-public".into();
        let next = projection.revision + 1;
        projection.apply(&request, Some("message:business-public"), next, 3);
        assert_eq!(projection.coordination_team_id_for(&foreign, next), Some(foreign_team.as_str()));
        assert!(projection.coordination_team_id_for(&request.actor.actor_id, next).is_none());
    }

    #[test]
    fn coordination_business_failure_and_epoch_keep_primary_work() {
        let (mut projection, request, team, task, _, _) = business_case();
        let wake = projection.coordination_requests()[0].1.revision;
        let helper = projection.active_team_ids_for(&request.actor.actor_id);
        assert!(helper.contains(&team.as_str()));
        let primary = projection.tasks[&task].claim_execution_id.clone();
        assert_eq!(primary.as_deref(), Some("business-primary"));
        let mut supervisor = request.clone();
        supervisor.actor.kind = AgentActorKind::Supervisor;
        let helper_id = match &request.action { AgentAction::MessagePublish(input) => input.recipients[0].clone(), _ => unreachable!() };
        supervisor.action = AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
            task_ref: task.clone(), execution_id: "business-coordination".into(), agent_ref: helper_id.clone(),
            membership_id: AgenticProgramProjection::membership_id(&helper_id, &team),
            mode: AgentAttemptMode::Coordination, generation: wake,
        });
        let next = projection.revision + 1;
        projection.apply(&supervisor, None, next, 3);
        supervisor.action = AgentAction::TaskAttemptFail(TaskAttemptFailInput {
            task_ref: task.clone(), execution_id: "business-coordination".into(), mode: AgentAttemptMode::Coordination,
            reason: "failed before replying".into(), retryable: false,
        });
        let next = projection.revision + 1;
        projection.apply(&supervisor, None, next, 3);
        assert_eq!(projection.tasks[&task].claim_execution_id, primary);
        assert_eq!(projection.tasks[&task].failed_attempts, 0);
        assert!(projection.coordination_wake(wake).unwrap().1.coordination.as_ref().unwrap().reason.as_deref().unwrap().contains("failed before replying"));
        let mut release = request.clone();
        release.action = AgentAction::TaskRelease(TaskReleaseInput { task_ref: task.clone(), reason: "replan".into(),
            expected_claim_generation: Some(projection.tasks[&task].claim_generation) });
        let next = projection.revision + 1;
        projection.apply(&release, None, next, 4);
        assert_eq!(projection.tasks[&task].claim_execution_id, None);
        assert!(!projection.tasks[&task].active_attempts.contains_key("business-primary"));
        let next_execution = "business-next-execution";
        let next_generation = projection.tasks[&task].claim_generation;
        supervisor.action = AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
            task_ref: task.clone(), execution_id: next_execution.into(), agent_ref: request.actor.actor_id.clone(),
            membership_id: AgenticProgramProjection::membership_id(&request.actor.actor_id, &team),
            mode: AgentAttemptMode::Execute, generation: next_generation,
        });
        let next = projection.revision + 1;
        projection.apply(&supervisor, None, next, 5);
        assert!(projection.tasks[&task].active_attempts.contains_key(next_execution));
        assert_ne!(Some(next_execution), primary.as_deref());
        let mut claim = request;
        claim.actor.execution_id = Some(next_execution.into());
        claim.action = AgentAction::TaskClaim(TaskClaimInput { task_ref: task.clone(), reason: None });
        let next = projection.revision + 1;
        projection.apply(&claim, None, next, 6);
        assert_eq!(projection.tasks[&task].claim_execution_id.as_deref(), Some(next_execution));
        assert!(!projection.coordination_request_current(projection.coordination_wake(wake).unwrap().1));
    }

    #[test]
    fn coordination_business_coalesces_identical_requests() {
        let (mut projection, mut request, _, _, _, foreign) = business_case();
        for index in 0..128 {
            request.action_id = format!("business-repeat-{index}");
            let next = projection.revision + 1;
            projection.apply(&request, Some(&format!("message:repeat-{index}")), next, next);
        }
        let mut canonical = std::collections::BTreeSet::new();
        for (_, entry) in projection.coordination_requests() {
            assert!(canonical.insert(entry.entry_id.clone()));
        }
        assert_eq!(canonical, std::collections::BTreeSet::from(["message:business-help".to_string()]));
        let revision = projection.coordination_requests()[0].1.revision;
        assert!(projection.coordination_team_id_for(&request.actor.actor_id, revision).is_none());
        assert!(projection.coordination_team_id_for(&foreign, revision).is_none());
        if let AgentAction::MessagePublish(input) = &mut request.action { input.summary = Some("New source B".into()); }
        let next = projection.revision + 1;
        projection.apply(&request, Some("message:business-changed"), next, next);
        let mut expected = canonical;
        assert!(expected.insert("message:business-changed".to_string()));
        let mut actual = std::collections::BTreeSet::new();
        for (_, entry) in projection.coordination_requests() {
            assert!(actual.insert(entry.entry_id.clone()));
        }
        assert_eq!(actual, expected);
    }

}

impl AgenticProgramProjection {
    /// Earliest identical request is its immutable wake identity. A different
    /// reference/content or Task epoch is new information, not a retry count.
    pub(crate) fn coordination_requests(&self) -> Vec<(&str, &AgenticTopicEntryProjection)> {
        let mut entries = self
            .topics
            .iter()
            .flat_map(|(topic, entries)| {
                entries
                    .iter()
                    .filter(|entry| {
                        entry
                            .intent
                            .as_ref()
                            .is_some_and(|intent| intent.kind == TaskIntentKind::RequestHelp)
                    })
                    .map(move |entry| (topic.as_str(), entry))
            })
            .collect::<Vec<_>>();
        entries.sort_by_key(|(_, entry)| entry.revision);
        let mut seen = BTreeSet::new();
        entries.retain(|(topic, entry)| seen.insert(request_key(topic, entry)));
        entries
    }

    pub(crate) fn coordination_wake(
        &self,
        revision: u64,
    ) -> Option<(&str, &AgenticTopicEntryProjection)> {
        self.coordination_requests()
            .into_iter()
            .find(|(_, entry)| entry.revision == revision)
    }

    pub(crate) fn coordination_wake_ref(
        &self,
        wake_ref: &str,
    ) -> Option<(&str, &AgenticTopicEntryProjection)> {
        self.coordination_requests()
            .into_iter()
            .find(|(_, entry)| entry.entry_id == wake_ref)
    }

    pub(crate) fn coordination_request_current(&self, entry: &AgenticTopicEntryProjection) -> bool {
        entry
            .intent
            .as_ref()
            .and_then(|intent| self.tasks.get(&intent.task_ref))
            .is_some_and(|task| {
                entry.intent_generation == Some(task.claim_generation)
                    && !matches!(
                        task.status,
                        AgenticTaskStatus::Accepted
                            | AgenticTaskStatus::Withdrawn
                            | AgenticTaskStatus::Superseded
                            | AgenticTaskStatus::CancelRequested
                    )
            })
    }

    pub(crate) fn coordination_team_id_for(&self, agent_id: &str, revision: u64) -> Option<&str> {
        let (topic, entry) = self.coordination_wake(revision)?;
        if entry.actor_id == agent_id
            || !super::action_service::topic_entry_visible(entry, agent_id)
        {
            return None;
        }
        let mut teams = self.active_team_ids_for(agent_id);
        teams.sort_unstable();
        teams.into_iter().find(|team| {
            super::action_service::readable_topic_refs(self, agent_id, team).contains(topic)
        })
    }

    pub(crate) fn coordination_replied(
        &self,
        wake_ref: &str,
        execution_id: &str,
        agent_id: &str,
    ) -> bool {
        let Some((topic, source)) = self.coordination_wake_ref(wake_ref) else {
            return false;
        };
        self.topics.get(topic).is_some_and(|entries| {
            entries.iter().any(|entry| {
                entry.revision > source.revision
                    && entry.actor_id == agent_id
                    && entry.source_execution_id.as_deref() == Some(execution_id)
                    && entry.refs.iter().any(|reference| reference == wake_ref)
                    && (entry
                        .summary
                        .as_deref()
                        .is_some_and(|text| !text.trim().is_empty())
                        || entry.content_ref.is_some())
                    && super::action_service::topic_entry_visible(entry, &source.actor_id)
            })
        })
    }

    pub(crate) fn settle_coordination(&mut self, execution_id: &str, reason: Option<String>) {
        for entry in self.topics.values_mut().flatten() {
            if let Some(consumption) = entry
                .coordination
                .as_mut()
                .filter(|c| c.execution_id == execution_id)
            {
                consumption.settled = true;
                consumption.reason = reason.clone();
            }
        }
    }

    pub(crate) fn execution_is_coordinator(&self, execution_id: Option<&str>) -> bool {
        execution_id.is_some_and(|id| {
            self.topics.values().flatten().any(|entry| {
                entry
                    .coordination
                    .as_ref()
                    .is_some_and(|consumption| consumption.execution_id == id)
            })
        })
    }

    pub(crate) fn coordination_attempt(
        &self,
        execution_id: &str,
    ) -> Option<&AgenticTaskAttemptProjection> {
        self.tasks
            .values()
            .find_map(|task| task.active_attempts.get(execution_id))
            .filter(|attempt| attempt.mode == AgentAttemptMode::Coordination)
    }
}

/// Narrow read-only observation surface for the Gateway real-PG coordination
/// tests. These expose the canonical Program projection queries (no rebuilt
/// projection) under the test-fixtures feature only; production visibility is
/// unchanged.
#[cfg(feature = "test-fixtures")]
impl AgenticProgramProjection {
    pub fn coordination_requests_for_test(&self) -> Vec<(String, AgenticTopicEntryProjection)> {
        self.coordination_requests()
            .into_iter()
            .map(|(topic, entry)| (topic.to_string(), entry.clone()))
            .collect()
    }

    pub fn coordination_team_id_for_test(&self, agent_id: &str, revision: u64) -> Option<String> {
        self.coordination_team_id_for(agent_id, revision)
            .map(|team| team.to_string())
    }

    pub fn coordination_request_current_for_test(&self, entry: &AgenticTopicEntryProjection) -> bool {
        self.coordination_request_current(entry)
    }
}
