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
        fixture_with_options(missing_graph, process, None, false, false, None).await
    }

    /// A TaskExecute-scoped process fixture. An ordinary write needs an active
    /// Task execution claim, which a Coordination focus does not provide.
    pub(crate) async fn fixture_with_process_task(process: crate::ProcessJsonlSpec) -> Fixture {
        fixture_with_options(false, Some(process), None, true, true, None).await
    }

    pub(crate) async fn fixture_with_provider(provider: Arc<crate::ProviderRegistry>) -> Fixture {
        fixture_with_options(false, None, Some(provider), false, false, None).await
    }

    async fn fixture_with_options(
        missing_graph: bool,
        process: Option<crate::ProcessJsonlSpec>,
        provider: Option<Arc<crate::ProviderRegistry>>,
        write: bool,
        task_execute: bool,
        runtime_backend: Option<Arc<dyn crate::AgentRuntimeBackend>>,
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
        let actions = services.agent_action_service();
        let root = AgentActorBinding {
            original_execution: None,
            objective_id: "coord-objective".into(),
            program_id: "coord-program".into(),
            session_id: "coord-session".into(),
            turn_id: format!("coord-turn-{}", workspace.path().display()),
            root_execution_id: None,
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
            };
        }
        let mut author = root.clone();
        author.actor_id = owner.clone();
        author.agent_id = Some(owner);
        author.team_id = Some(team.clone());
        author.kind = AgentActorKind::Agent;
        author.execution_id = Some("primary-execution".into());
        let mut primary =
            harness_contract::execution_graph::ExecutionGraph::new("active primary fixture");
        primary.id = "primary-execution".into();
        primary.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: root.session_id.clone(),
            turn_id: root.turn_id.clone(),
            root_task_id: task_ref.clone(),
            task_id: task_ref.clone(),
            generation: 1,
        });
        let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            harness_contract::execution_graph::ExecutionNodeKind::InlineModel,
            "inline_model",
            "held primary",
        );
        node.id = "primary-node".into();
        primary.node_statuses.insert(
            node.id.clone(),
            harness_contract::execution_graph::ExecutionNodeStatus::Planned,
        );
        primary.nodes.push(node);
        services.commit_service().register_graph(primary).unwrap();
        apply(
            "primary-claim",
            &author,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: Some("own the original work".into()),
            }),
        );
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
            apply(
                "registered-before-crash",
                &supervisor,
                AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                    task_ref: task_ref.clone(),
                    execution_id: graph_id.clone(),
                    agent_ref: helper.clone(),
                    membership_id: AgenticProgramProjection::membership_id(&helper, &team),
                    mode: AgentAttemptMode::Coordination,
                    generation: wake.revision,
                }),
            );
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
        }
    }

    pub(crate) async fn actor(fixture: &Fixture) -> AgentActorBinding {
        fixture
            .services
            .resolve_agent_action_actor(
                &harness_contract::execution_graph::ExecutionParentBinding {
                    execution_id: fixture.packet.graph_id().into(),
                    node_id: fixture.packet.node_id().into(),
                },
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

    #[tokio::test]
    async fn coordination_visibility_respects_active_teams_recipients_and_program_topic() {
        let fixture = fixture(false).await;
        let actions = fixture.services.agent_action_service();
        let mut root = fixture.request.actor.clone();
        root.kind = AgentActorKind::Root;
        root.actor_id = "root:coord-session".into();
        root.agent_id = None;
        root.team_id = None;
        root.execution_id = None;
        let create = |id: &str, action| {
            let out = actions
                .apply(&AgentActionEnvelope {
                    action_id: id.into(),
                    actor: root.clone(),
                    expected_revision: None,
                    action,
                })
                .unwrap();
            assert_eq!(out.status, AgentActionStatus::Applied, "{out:?}");
            out.changed_refs[0].clone()
        };
        let foreign_team = create(
            "foreign-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Other".into(),
                mission: "other evidence".into(),
                objective: None,
            }),
        );
        let foreign_member = create("foreign-member", AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":foreign_team,"role":"Reader","mission":"read public evidence","required_capabilities":["read"]
        })).unwrap()));
        let private = actions.project("coord-program").unwrap();
        let revision = private.coordination_requests()[0].1.revision;
        assert!(private.agent_is_active_in(&foreign_member, &foreign_team));
        assert!(private
            .coordination_team_id_for(&foreign_member, revision)
            .is_none());
        // Even an explicit recipient cannot read a different private Team's Topic.
        let mut request = fixture.request.clone();
        request.action_id = "private-cross-team-recipient".into();
        if let AgentAction::MessagePublish(input) = &mut request.action {
            input.recipients = vec![foreign_member.clone()];
        }
        let observation = actions.apply(&request).unwrap();
        if observation.status == AgentActionStatus::Applied {
            let projection = actions.project("coord-program").unwrap();
            assert!(projection
                .coordination_team_id_for(&foreign_member, projection.revision)
                .is_none());
        }
        let program_topic = format!("topic:{}", private.program_id);
        request.action_id = "public-cross-team-request".into();
        if let AgentAction::MessagePublish(input) = &mut request.action {
            input.topic_ref = program_topic;
        }
        assert_eq!(
            actions.apply(&request).unwrap().status,
            AgentActionStatus::Applied
        );
        let projection = actions.project("coord-program").unwrap();
        assert_eq!(
            projection.coordination_team_id_for(&foreign_member, projection.revision),
            Some(foreign_team.as_str())
        );
        assert!(projection
            .coordination_team_id_for(
                &fixture.packet.agentic_binding.as_ref().unwrap().agent_id,
                projection.revision
            )
            .is_none());
    }

    #[tokio::test]
    async fn coordination_failure_and_epoch_change_do_not_punish_primary_work() {
        let fixture = fixture(false).await;
        fixture
            .services
            .settle_abandoned_agentic_attempt(
                &fixture.packet,
                "fixture backend failed before replying",
            )
            .await
            .unwrap();
        let actions = fixture.services.agent_action_service();
        let failed = actions.project("coord-program").unwrap();
        assert_eq!(
            failed.tasks[&fixture.task_ref]
                .claim_execution_id
                .as_deref(),
            Some("primary-execution")
        );
        assert_eq!(failed.tasks[&fixture.task_ref].failed_attempts, 0);
        assert!(failed.coordination_requests()[0]
            .1
            .coordination
            .as_ref()
            .unwrap()
            .reason
            .as_deref()
            .unwrap()
            .contains("failed before replying"));
        assert!(fixture
            .services
            .dispatch_ready_agentic_work("coord-program")
            .await
            .unwrap()
            .is_empty());
        // A fresh Task epoch invalidates an otherwise unhandled old request.
        let mut release = fixture.request.clone();
        release.action_id = "primary-release".into();
        release.action = AgentAction::TaskRelease(TaskReleaseInput {
            task_ref: fixture.task_ref.clone(),
            reason: "replan the source".into(),
            expected_claim_generation: Some(actions.project("coord-program").unwrap().tasks[&fixture.task_ref].claim_generation),
        });
        assert_eq!(
            actions.apply(&release).unwrap().status,
            AgentActionStatus::Applied
        );
        let mut claim = fixture.request.clone();
        claim.action_id = "primary-next-epoch".into();
        claim.action = AgentAction::TaskClaim(TaskClaimInput {
            task_ref: fixture.task_ref.clone(),
            reason: None,
        });
        assert_eq!(
            actions.apply(&claim).unwrap().status,
            AgentActionStatus::Applied
        );
        let after = actions.project("coord-program").unwrap();
        assert!(!after.coordination_request_current(after.coordination_requests()[0].1));
    }

    #[tokio::test]
    async fn coordination_missing_graph_recovers_same_registered_attempt() {
        let fixture = fixture(true).await;
        assert!(fixture
            .services
            .recover_agentic_programs_on_startup()
            .await
            .unwrap()
            .is_empty());
        let projection = fixture
            .services
            .agent_action_service()
            .project("coord-program")
            .unwrap();
        assert_eq!(projection.tasks[&fixture.task_ref].failed_attempts, 0);
        assert_eq!(projection.tasks[&fixture.task_ref].active_attempts.len(), 1);
        actor(&fixture).await;
    }

    #[tokio::test]
    async fn coordination_request_coalesces_and_preserves_scope() {
        let fixture = fixture(false).await;
        let actions = fixture.services.agent_action_service();
        for index in 0..128 {
            let mut duplicate = fixture.request.clone();
            duplicate.action_id = format!("duplicate-{index}");
            assert_eq!(
                actions.apply(&duplicate).unwrap().status,
                AgentActionStatus::Applied
            );
        }
        let projection = actions.project("coord-program").unwrap();
        assert_eq!(projection.coordination_requests().len(), 1);
        assert!(fixture
            .services
            .dispatch_ready_agentic_work("coord-program")
            .await
            .unwrap()
            .is_empty());
        assert!(projection
            .coordination_team_id_for(
                &fixture.request.actor.actor_id,
                projection.coordination_requests()[0].1.revision
            )
            .is_none());
        assert!(projection
            .coordination_team_id_for(
                "foreign-agent",
                projection.coordination_requests()[0].1.revision
            )
            .is_none());
        let mut changed = fixture.request.clone();
        changed.action_id = "new-evidence".into();
        if let AgentAction::MessagePublish(input) = &mut changed.action {
            input.summary = Some("New source B contradicts A".into());
        }
        assert_eq!(
            actions.apply(&changed).unwrap().status,
            AgentActionStatus::Applied
        );
        assert_eq!(
            actions
                .project("coord-program")
                .unwrap()
                .coordination_requests()
                .len(),
            2
        );
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
