use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};

use crate::{
    ContextProfile, PermissionMode, PermissionPolicy, RuntimeExecutionHost, RuntimeServices,
    RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session, SharedPrompter,
    StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::execution_core::graph::{
    ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
};

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
    /// digest-bound TEAM.md public fragments. The key is the exact
    /// `agent_binding_digest:team_binding_digest` pair so a changed revision
    /// never reuses a stale prefix.
    team_prompt_cache: Mutex<BTreeMap<String, Vec<String>>>,
    team_prompt_cache_hits: AtomicU64,
    team_prompt_cache_builds: AtomicU64,
    team_prompt_cache_tokens: AtomicU64,
}

const COMPLETED_RUN_TOMBSTONE_LIMIT: usize = 1_024;

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: crate::SessionInputStream,
    completion: Arc<tokio::sync::Notify>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}

struct ActiveRunCleanup<'a> {
    worker: &'a InProcessAgentWorker,
    run_id: String,
    completion: Arc<tokio::sync::Notify>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}

struct PendingCancellationOwner<'a> {
    pending: &'a Mutex<BTreeSet<String>>,
    run_id: String,
}

impl Drop for PendingCancellationOwner<'_> {
    fn drop(&mut self) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
    }
}

impl Drop for ActiveRunCleanup<'_> {
    fn drop(&mut self) {
        // The tombstone is visible before the active handle disappears; the
        // completion flag/notification are published only after all maps are
        // clean. This also runs when the execute future is aborted.
        self.worker.record_completed_run(&self.run_id);
        self.worker
            .active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
        self.worker
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.run_id);
        self.completed.store(true, Ordering::SeqCst);
        self.completion.notify_waiters();
    }
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
            pending_cancellations: Mutex::new(BTreeSet::new()),
            completed_runs: Mutex::new(VecDeque::new()),
            team_prompt_cache: Mutex::new(BTreeMap::new()),
            team_prompt_cache_hits: AtomicU64::new(0),
            team_prompt_cache_builds: AtomicU64::new(0),
            team_prompt_cache_tokens: AtomicU64::new(0),
        }
    }

    fn cached_team_markdown_fragment(
        &self,
        binding_digest: &str,
        team_binding_digest: &str,
        team_instructions: &str,
    ) -> Vec<String> {
        let key = format!("{binding_digest}:{team_binding_digest}");
        let mut cache = self
            .team_prompt_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(segments) = cache.get(&key) {
            self.team_prompt_cache_hits.fetch_add(1, Ordering::Relaxed);
            return segments.clone();
        }
        let segment = format!(
            "Team protocol fragment (binding digest {}):\n{}",
            team_binding_digest,
            team_instructions.trim()
        );
        let segments = vec![segment];
        self.team_prompt_cache_tokens.fetch_add(
            crate::context_ledger::estimate_text_tokens(&segments.join("\n\n")),
            Ordering::Relaxed,
        );
        self.team_prompt_cache_builds
            .fetch_add(1, Ordering::Relaxed);
        cache.insert(key, segments.clone());
        segments
    }

    #[cfg(test)]
    fn team_prompt_cache_stats(&self) -> (u64, u64, u64) {
        (
            self.team_prompt_cache_hits.load(Ordering::Relaxed),
            self.team_prompt_cache_builds.load(Ordering::Relaxed),
            self.team_prompt_cache_tokens.load(Ordering::Relaxed),
        )
    }

    fn record_completed_run(&self, run_id: &str) {
        let mut completed = self
            .completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !completed.iter().any(|candidate| candidate == run_id) {
            completed.push_back(run_id.to_string());
        }
        while completed.len() > COMPLETED_RUN_TOMBSTONE_LIMIT {
            completed.pop_front();
        }
    }

    fn run_completed(&self, run_id: &str) -> bool {
        self.completed_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .any(|candidate| candidate == run_id)
    }
}

#[async_trait]
impl AgentRuntimeBackend for InProcessAgentWorker {
    fn kind(&self) -> AgentBackendKind {
        AgentBackendKind::InProcess
    }

    fn capabilities(&self) -> AgentBackendCapabilities {
        AgentBackendCapabilities {
            backend: AgentBackendKind::InProcess,
            supports_input: true,
            supports_interrupt: true,
            supports_pause: false,
            supports_resume: false,
            supports_cancel: true,
            supports_shutdown: true,
        }
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "in-process Agent execution requires a Runtime-compiled Binding".to_string()
        })?;
        binding.validate().map_err(|error| error.to_string())?;
        if packet
            .allowed_tools
            .iter()
            .any(|tool| !binding.tool_contract_refs.contains(tool))
        {
            return Err("AgentTaskPacket tool allow-list exceeds its Binding contract".to_string());
        }
        if packet
            .allowed_skills
            .iter()
            .any(|skill| !binding.skill_refs.contains(skill))
        {
            return Err(
                "AgentTaskPacket Skill allow-list exceeds its Binding contract".to_string(),
            );
        }
        let services = self
            .services
            .upgrade()
            .ok_or_else(|| "AgentRuntime is not bound to RuntimeServices".to_string())?;
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the in-process agent".to_string()
        })?;
        let execution_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .map_err(|error| {
                format!(
                    "in-process Agent graph `{}` is unavailable: {error}",
                    packet.graph_id()
                )
            })?;
        let parent_lineage = execution_graph.lineage.as_ref().ok_or_else(|| {
            format!(
                "in-process Agent graph `{}` has no canonical Session/Turn/Task lineage",
                packet.graph_id()
            )
        })?;
        parent_lineage.validate().map_err(str::to_string)?;
        if parent_lineage.session_id != packet.session_id()
            || parent_lineage.root_task_id != packet.assignment.root_task_id
        {
            return Err(format!(
                "AgentTaskPacket lineage does not match parent graph `{}`",
                packet.graph_id()
            ));
        }
        let execution_lineage = harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: parent_lineage.session_id.clone(),
            turn_id: parent_lineage.turn_id.clone(),
            root_task_id: parent_lineage.root_task_id.clone(),
            task_id: packet.task_id().to_string(),
            generation: parent_lineage.generation,
        };
        let parent_execution_id = execution_graph
            .parent_execution
            .as_ref()
            .map(|parent| parent.execution_id.clone());
        let packet_allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let requested_tool_names = packet_allowed_tools.iter().cloned().collect::<Vec<_>>();
        let tool_definitions = host.delegated_tool_definitions(&requested_tool_names);
        let allowed_tools = tool_definitions
            .iter()
            .map(|definition| definition.name.clone())
            .filter(|tool| packet_allowed_tools.contains(tool))
            .filter(|tool| {
                packet.team_id().is_none()
                    || delegated_tool_supports_bounded_scope(host.as_ref(), tool)
            })
            .collect::<BTreeSet<_>>();
        let tool_names = allowed_tools.iter().cloned().collect::<Vec<_>>();
        let memory_context = memory::MemoryTurnContext::new(
            packet.session_id(),
            binding.instance.instance_id.clone(),
        )
        .with_definition_lineage_id(Some(
            binding.definition_ref.definition_id.as_str().to_string(),
        ))
        .with_project_id(Some(crate::memory_project_id_for_workspace(
            services.workspace_root(),
        )))
        .with_task_id(Some(binding.data_lease.task_id.clone()))
        .with_team_id(binding.data_lease.team_id.clone())
        .with_cognitive_read_scopes(binding.data_lease.read_scopes.clone());
        let live_policy_control = services.session_execution_policy_control(packet.session_id());
        let live_session_policy = live_policy_control
            .as_ref()
            .map(crate::permissions::SessionExecutionPolicyControl::snapshot)
            .ok_or_else(|| {
                format!(
                    "agent_session_policy_missing: session `{}` has no executable policy snapshot",
                    packet.session_id()
                )
            })?;
        let commit_service = crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
            services.event_store(),
        ));
        let durable_agent_receipts = commit_service
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map_err(|error| {
                format!(
                    "delegated Agent tool receipt recovery is invalid for {}:{}:{}: {error}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )
            })?;
        let recovered_tool_receipt_count = durable_agent_receipts.len();
        let recovered_tool_receipt_prompt =
            recovered_agent_tool_receipt_prompt(&durable_agent_receipts);
        let durable_receipts = durable_agent_receipts
            .into_iter()
            .map(scoped_receipt_from_durable)
            .collect::<Vec<_>>();
        let recovered_sequence = durable_receipts
            .iter()
            .map(|receipt| receipt.sequence)
            .max()
            .unwrap_or(0);
        let tool_executor = Arc::new(ScopedRuntimeToolExecutor {
            host,
            allowed_tools: allowed_tools.clone(),
            session_id: packet.session_id().to_string(),
            sandbox_posture: live_session_policy.sandbox_posture,
            policy_revision: live_session_policy.revision,
            memory_context,
            model_lease: selection.model.clone(),
            execution_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            workspace_root: services.workspace_root().to_path_buf(),
            path_identity_resolver: Arc::clone(services.path_identity_resolver()),
            scope_locks: Arc::clone(services.scope_locks()),
            commit_service: Some(commit_service),
            resource_scopes: packet.team_id().map(|_| packet.resource_scopes.clone()),
            managed_invocation: packet.managed_invocation.clone(),
            next_receipt_sequence: AtomicU64::new(recovered_sequence),
            receipts: Mutex::new(durable_receipts),
        });
        if packet.policy_revision != 0 && packet.policy_revision != live_session_policy.revision {
            return Err(format!(
                "agent_policy_revision_stale: packet rev {} current rev {}; replan before provider/tool execution",
                packet.policy_revision, live_session_policy.revision
            ));
        }
        let bound_policy_control = Some(
            crate::permissions::SessionExecutionPolicyControl::from_policy(live_session_policy),
        );
        let policy = permission_policy(
            bound_policy_control,
            packet.permission_ceiling,
            &allowed_tools,
        );
        let cancellation = crate::CancellationToken::new();
        let (provider_event_sender, mut provider_event_receiver) = tokio::sync::mpsc::channel(64);
        let progress_runtime = Arc::clone(services.agent_runtime());
        let progress_agent_id = packet.agent_id().to_string();
        let progress_run_id = packet.run_id().to_string();
        let progress_reporter = tokio::spawn(async move {
            let mut saw_model_output = false;
            while let Some(event) = provider_event_receiver.recv().await {
                if matches!(event, crate::CowdEvent::TextDelta { .. }) && !saw_model_output {
                    saw_model_output = true;
                    let _ = progress_runtime.record_progress(
                        &progress_agent_id,
                        "agent.provider.first_output",
                        &format!("provider produced the first output for run {progress_run_id}"),
                    );
                }
            }
        });
        let child_session = delegated_child_session(
            packet.session_id(),
            &selection.model,
            services.workspace_root(),
        );
        // An in-process role is a child execution of the parent session, not
        // an unrelated surface session. Keep the canonical session/model
        // binding available to tool and orchestration contracts.
        let child_session_id = child_session.session_id.clone();
        // RuntimeServices owns the inspected Skill snapshot. The Binding's
        // refs below remain the capability ceiling; this worker never scans
        // package directories or falls back to an empty production profile.
        let skill_catalog = services.skill_catalog();
        let external_context_items = if binding.data_lease.team_working_state_visible {
            services
                .team_runtime()
                .read_working_state(crate::TeamWorkingStateReadRequest {
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    after_revision: None,
                    exact_revision: None,
                })
                .ok()
                .filter(|state| !state.entries.is_empty())
                .map(|state| {
                    let team_id = state.team_id.clone();
                    let board_revision = state.board_revision;
                    let summary = serde_json::to_string(&serde_json::json!({
                        "board_revision": board_revision,
                        "entries": state.entries.into_iter().rev().take(32).collect::<Vec<_>>(),
                        "instruction": "Use committed semantic entries only. Call team_board read_after at safe checkpoints before synthesis; publish findings, conflicts, unresolved work, or artifacts without private reasoning."
                    }))
                    .unwrap_or_else(|_| "{}".to_string());
                    let mut item = crate::ContextItem::new(
                        format!("team-board:{team_id}"),
                        crate::ContextSourceKind::AgentPeer,
                        crate::ContextRole::Evidence,
                        summary,
                    );
                    item.authority = crate::ContextAuthority::Tool;
                    item.evidence = vec![format!(
                        "team-working-state:{}:{}",
                        team_id, board_revision
                    )];
                    item
                })
                .into_iter()
                .collect()
        } else {
            Vec::new()
        };
        let (team_instructions, team_binding_digest) =
            if let Some(team_id) = binding.data_lease.team_id.as_deref() {
                let graph_id = format!("team-graph:{team_id}");
                match crate::team_binding::load_binding(services.event_store(), &graph_id) {
                    Ok(Some(team_binding)) => (
                        Some(team_binding.team_instructions),
                        Some(team_binding.binding_digest),
                    ),
                    _ => (None, None),
                }
            } else {
                (None, None)
            };
        let team_markdown_fragment = match (&team_instructions, &team_binding_digest) {
            (Some(instructions), Some(team_digest)) => self.cached_team_markdown_fragment(
                &binding.binding_digest,
                team_digest,
                instructions,
            ),
            _ => Vec::new(),
        };
        let mut prompt_segments = system_prompt(&packet, services.workspace_root(), &tool_names);
        prompt_segments.extend(team_markdown_fragment);
        if let Some(receipt_prompt) = recovered_tool_receipt_prompt {
            prompt_segments.push(receipt_prompt);
        }
        let host = StandardRuntimeHost::new(StandardRuntimeHostConfig {
            runtime_services: Arc::clone(&services),
            session: child_session,
            provider_registry: Arc::clone(services.provider_registry()),
            model: selection.model.clone(),
            tool_definitions: tool_definitions.clone(),
            tool_executor: Arc::clone(&tool_executor),
            permission_policy: policy,
            system_prompt: prompt_segments,
            feature_config: crate::RuntimeFeatureConfig::default(),
            emit_output: false,
            stream_callback: Some(provider_event_sender),
            tool_callback: None,
            model_context_window: None,
            hook_progress_reporter: None,
            external_context_items,
            skill_profiles: skill_catalog.profiles(),
            agent_skill_profile: harness_contract::skill::AgentSkillProfile {
                baseline_skill_refs: Vec::new(),
                template_skill_refs: Vec::new(),
                team_skill_refs: Vec::new(),
                task_skill_refs: binding.skill_refs.clone(),
                explicit_grants: binding.skill_refs.clone(),
                hidden_skill_refs: Vec::new(),
                adapter_ceiling: Vec::new(),
            },
            skill_prompt_assets: skill_catalog.prompt_assets(),
            skill_instruction_source: skill_catalog.instruction_source(),
            memory_agent_id: binding.instance.instance_id.clone(),
            memory_definition_lineage_id: Some(
                binding.definition_ref.definition_id.as_str().to_string(),
            ),
            memory_team_id: binding.data_lease.team_id.clone(),
            memory_read_scopes: binding.data_lease.read_scopes.clone(),
            reality_binding: Some(binding.clone()),
            execution_identity: Some(packet.assignment.execution_identity.clone()),
            execution_lineage: Some(execution_lineage),
            execution_parent: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: packet.graph_id().to_string(),
                node_id: packet.node_id().to_string(),
            }),
            execution_role: crate::TurnExecutionRole::DelegatedLeaf,
            recovered_tool_receipt_count,
        });
        let mut runtime = match host {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(format!(
                    "failed to initialize in-process agent host: {error}"
                ));
            }
        };
        packet.budget_lease.validate().map_err(str::to_string)?;
        runtime
            .set_delegated_provider_budget(packet.budget_lease.clone())
            .map_err(|error| error.to_string())?;
        // A delegated role has a bounded evidence obligation. It retains the
        // parent session authority but must not inherit MainTurn's broad,
        // open-ended exploration profile.
        runtime.set_context_profile(ContextProfile::SubAgent);
        runtime.set_execution_service_class(if binding.evaluation.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Maintenance
        } else if packet.managed_invocation.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Background
        } else {
            harness_contract::execution_graph::ExecutionServiceClass::Foreground
        });
        runtime.set_delegated_focus_policy(
            packet_focus_novelty_target_bp(&packet),
            packet_focus_acceptance_scopes(&packet),
            packet_required_output_fields(&packet),
        );
        // Delegated Agents share the parent Session's evidence authority, but
        // only the parent Turn may publish conversation messages. The child
        // result returns through AgentReturnPacket and the Team reducer.
        let input_stream = runtime.session_input_stream();
        let completion = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                packet.run_id().to_string(),
                ActiveInProcessRun {
                    cancellation: cancellation.clone(),
                    session_id: child_session_id,
                    input_stream,
                    completion: Arc::clone(&completion),
                    completed: Arc::clone(&completed),
                },
            );
        let active_run_cleanup = ActiveRunCleanup {
            worker: self,
            run_id: packet.run_id().to_string(),
            completion: Arc::clone(&completion),
            completed: Arc::clone(&completed),
        };
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(packet.run_id())
        {
            cancellation.cancel();
        }
        runtime.install_turn_control(cancellation, crate::HookAbortSignal::default());
        let child_bus = runtime.cowd_bus().cloned().ok_or_else(|| {
            "in-process Agent Runtime is missing its causal event bus".to_string()
        })?;
        let resolved_parent = parent_execution_id
            .as_deref()
            .and_then(|execution_id| services.resolve_active_execution_bus(execution_id));
        if let Some((root_execution_id, parent_bus)) = resolved_parent.as_ref() {
            child_bus.forward_to(
                parent_bus,
                crate::CowdExecutionLineage {
                    parent_execution_id: root_execution_id.clone(),
                    graph_id: packet.graph_id().to_string(),
                    node_id: packet.node_id().to_string(),
                    team_id: packet.team_id().map(str::to_owned),
                    agent_id: Some(packet.agent_id().to_string()),
                },
            );
        } else if let Some(parent_execution_id) = parent_execution_id.as_deref() {
            tracing::debug!(
                parent_execution_id,
                graph_id = packet.graph_id(),
                agent_id = packet.agent_id(),
                "root Session event bus is no longer active; child evidence remains durable"
            );
        }
        let root_execution_id = resolved_parent
            .as_ref()
            .map(|(root_execution_id, _)| root_execution_id.clone())
            .unwrap_or_else(|| packet.graph_id().to_string());
        let activity_generation = parent_lineage.generation;
        let activity_id = format!(
            "activity:execution:{}:node:{}",
            packet.graph_id(),
            packet.node_id()
        );
        let child_execution_scope = child_bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: packet.run_id().to_string(),
                session_id: packet.session_id().to_string(),
                turn_id: packet
                    .assignment
                    .execution_identity
                    .turn_id()
                    .unwrap_or(packet.run_id())
                    .to_string(),
            },
            Some(harness_contract::projection::RuntimeActivityBinding {
                root_execution_id,
                session_id: packet.session_id().to_string(),
                turn_id: packet
                    .assignment
                    .execution_identity
                    .turn_id()
                    .unwrap_or(packet.run_id())
                    .to_string(),
                root_task_id: packet.assignment.root_task_id.clone(),
                task_id: packet.task_id().to_string(),
                activity_id: activity_id.clone(),
                node_id: Some(packet.node_id().to_string()),
                parent_activity_id: Some(format!("activity:execution:{}", packet.graph_id())),
                initiator_activity_id: Some(activity_id),
                team_run_id: packet.team_id().map(str::to_owned),
                agent_instance_id: Some(packet.agent_id().to_string()),
                agent_run_id: Some(packet.run_id().to_string()),
                skill_id: None,
                skill_revision: None,
                skill_activation_id: None,
                tool_contract_id: None,
                tool_call_id: None,
                approval_id: None,
                parallel_group_id: None,
                revision: u64::from(packet.attempt.max(1)),
                fence: packet.expected_graph_revision.max(1),
                generation: activity_generation,
            }),
        );
        let _ = services.agent_runtime().record_progress(
            packet.agent_id(),
            "agent.execution.started",
            "provider-backed child execution admitted",
        );
        let result = runtime
            .submit_turn(&packet.objective, &SharedPrompter::none())
            .await;
        let mut summary = match result {
            Ok(summary) => summary,
            Err(error) => {
                let error = format!("in-process agent turn failed: {error}");
                services.fail_live_execution(packet.run_id(), error.clone());
                drop(runtime);
                drop(child_execution_scope);
                drop(active_run_cleanup);
                return Err(error);
            }
        };
        normalize_verified_narrative_terminal(&packet, &tool_executor, &mut summary);
        let evidence_refs = agent_evidence_refs(
            &packet,
            &summary.context_turn_report.audit_projections,
            &tool_executor
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            &packet,
            &summary,
            &evidence_refs,
            &tool_executor,
        );
        // `submit_turn` is the delegated child terminal boundary. From this
        // point onward Runtime performs only deterministic presentation
        // normalization and receipt evaluation: it never asks a Provider to
        // rewrite or repair the terminal answer. Receipt-backed technical
        // prose may be carried into a presentation field, while missing
        // risk/unresolved declarations remain missing so the canonical Agent
        // validator/Team reducer can degrade or reject without hidden model
        // work.
        // Dropping the host drops the provider callback sender. The bounded
        // reporter owns no runtime state beyond the lifecycle projection, so
        // it can be joined before the terminal Agent result is committed.
        drop(runtime);
        let _ = progress_reporter.await;
        let mut runtime_write_attempt_paths = summary.write_attempt_paths.clone();
        runtime_write_attempt_paths.extend(
            tool_executor
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .filter(|receipt| {
                    receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write
                })
                .flat_map(|receipt| receipt.paths.iter().cloned()),
        );
        runtime_write_attempt_paths.sort();
        runtime_write_attempt_paths.dedup();
        let observed_evidence = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .flat_map(|receipt| receipt.observed_evidence.iter().cloned())
            .collect::<Vec<_>>();
        let required_acceptance =
            crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
                &packet.required_acceptance,
                &packet.acceptance,
            );
        let terminal_structured_fields = structured_agent_output(&summary.final_answer)
            .map(|object| object.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        tracing::debug!(
            run_id = packet.run_id(),
            final_answer_bytes = summary.final_answer.len(),
            structured_fields = ?terminal_structured_fields,
            terminal_completion = ?summary.terminal_completion,
            "delegated Agent terminal carrier prepared"
        );
        let receipt_snapshot =
            crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                required_acceptance,
                acceptance.clone(),
                observed_evidence,
            );
        let (observed_acceptance, acceptance_evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(receipt_snapshot);
        let changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect::<Vec<_>>();
        let receipt_summary = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|receipt| {
                let digest_changed =
                    receipt
                        .paths
                        .iter()
                        .any(|path| match receipt.prior_states.get(path) {
                            Some(harness_contract::context::WorkspacePriorState::Existing {
                                sha256,
                            }) => {
                                receipt
                                    .after_digests
                                    .get(path)
                                    .and_then(|digest| digest.as_deref())
                                    != Some(sha256.as_str())
                            }
                            Some(harness_contract::context::WorkspacePriorState::Absent) => {
                                receipt.after_digests.get(path).is_some_and(Option::is_some)
                            }
                            None => false,
                        });
                format!(
                    "{}:{:?}:{:?}:changed={digest_changed}",
                    receipt.sequence, receipt.effect_kind, receipt.paths
                )
            })
            .collect::<Vec<_>>();
        let contract_criteria = packet_acceptance_contract(&packet)
            .into_iter()
            .map(|requirement| requirement.criterion)
            .collect::<Vec<_>>();
        let pending_evidence_scopes = packet_focus_acceptance_scopes(&packet);
        let _ = services.agent_runtime().record_progress(
            packet.agent_id(),
            "agent.acceptance.evaluated",
            &format!(
                "accepted={acceptance:?}; changes={changes:?}; receipts={receipt_summary:?}; contract={contract_criteria:?}; pending_evidence_scopes={pending_evidence_scopes:?}; observed_acceptance={observed_acceptance:?}"
            ),
        );
        let (status, failure) =
            agent_terminal_outcome(summary.terminal_completion, &summary.final_answer);
        let terminal_ref = format!("agent-terminal:{}", packet.run_id());
        match status {
            AgentTerminalStatus::Completed => services.complete_live_execution(
                packet.run_id(),
                &summary.context_turn_report,
                &runtime_write_attempt_paths,
                terminal_ref,
            ),
            AgentTerminalStatus::Blocked => services.block_live_execution(
                packet.run_id(),
                &summary.context_turn_report,
                &runtime_write_attempt_paths,
                terminal_ref,
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent was blocked".to_string()),
            ),
            AgentTerminalStatus::Cancelled => services.cancel_live_execution(
                packet.run_id(),
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent was cancelled".to_string()),
            ),
            AgentTerminalStatus::Failed => services.fail_live_execution(
                packet.run_id(),
                failure
                    .clone()
                    .unwrap_or_else(|| "delegated Agent failed".to_string()),
            ),
        }
        drop(child_execution_scope);
        drop(active_run_cleanup);
        Ok(AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(str::to_owned),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status,
            outcome: summary.final_answer,
            answer_candidate: None,
            observed_acceptance,
            acceptance_evaluation: Some(acceptance_evaluation),
            acceptance,
            evidence_refs,
            changes,
            runtime_change_receipts,
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: u64::from(summary.usage.input_tokens),
            output_tokens: u64::from(summary.usage.output_tokens),
            cached_tokens: summary
                .model_telemetry
                .cache_create_tokens
                .saturating_add(summary.model_telemetry.cache_read_tokens),
            // Keep the model that actually completed the child turn. The
            // selector value remains the requested lease and may differ after
            // a configured provider fallback.
            model: summary
                .model_telemetry
                .model
                .clone()
                .unwrap_or(selection.model),
            provider: selection.provider,
            tool_calls: summary.tool_results.len() as u64,
            duplicate_tool_calls: summary.duplicate_tool_calls,
            max_tool_concurrency_observed: u64::try_from(summary.max_tool_concurrency_observed)
                .unwrap_or(u64::MAX),
            parallel_tool_batches: u64::try_from(summary.parallel_tool_batches).unwrap_or(u64::MAX),
            runtime_write_attempt_paths,
            runtime_observed_resource_scopes: Vec::new(),
            failure,
        })
    }

    async fn command(
        &self,
        handle: &AgentRunHandle,
        request: &AgentCommandRequest,
    ) -> Result<(), harness_contract::agent::AgentCommandRejectReason> {
        match request.command {
            harness_contract::agent::AgentCommand::Interrupt
            | harness_contract::agent::AgentCommand::Cancel
            | harness_contract::agent::AgentCommand::Shutdown => {
                self.pending_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(handle.run_id.clone());
                let _pending_owner = PendingCancellationOwner {
                    pending: &self.pending_cancellations,
                    run_id: handle.run_id.clone(),
                };
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
                loop {
                    let active = self
                        .active_runs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&handle.run_id)
                        .map(|active| {
                            (
                                active.cancellation.clone(),
                                Arc::clone(&active.completion),
                                Arc::clone(&active.completed),
                            )
                        });
                    if let Some((token, completion, completed)) = active {
                        token.cancel();
                        tokio::time::timeout_at(deadline, async {
                            loop {
                                let notified = completion.notified();
                                if completed.load(Ordering::SeqCst) {
                                    break;
                                }
                                notified.await;
                            }
                        })
                        .await
                        .map_err(|_| {
                            harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend
                        })?;
                        if self
                            .active_runs
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .contains_key(&handle.run_id)
                        {
                            return Err(
                                harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend,
                            );
                        }
                        self.pending_cancellations
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&handle.run_id);
                        break;
                    }
                    if self.run_completed(&handle.run_id) {
                        self.pending_cancellations
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&handle.run_id);
                        break;
                    }
                    let pending = self
                        .pending_cancellations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .contains(&handle.run_id);
                    if !pending
                        && !self
                            .active_runs
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .contains_key(&handle.run_id)
                    {
                        // The worker consumed the pending cancellation and
                        // removed its active handle after cleanup.
                        break;
                    }
                    if tokio::time::Instant::now() >= deadline {
                        self.pending_cancellations
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&handle.run_id);
                        return Err(
                            harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend,
                        );
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
                Ok(())
            }
            harness_contract::agent::AgentCommand::SendInput => {
                let input = request
                    .input
                    .as_ref()
                    .ok_or(harness_contract::agent::AgentCommandRejectReason::InvalidInput)?;
                let active = self
                    .active_runs
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&handle.run_id)
                    .cloned()
                    .ok_or(
                        harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend,
                    )?;
                let envelope = SessionInputEnvelope::text(
                    active.session_id,
                    InputSourceKind::Agent,
                    agent_input_text(input),
                )
                .with_source_ref(format!("agent:{}", handle.agent_id))
                .with_source_message_id(request.command_id.clone());
                active
                    .input_stream
                    .admit(envelope, active.input_stream.runtime_state());
                Ok(())
            }
            harness_contract::agent::AgentCommand::Pause
            | harness_contract::agent::AgentCommand::Resume => {
                Err(harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend)
            }
        }
    }
}

fn delegated_child_session(
    session_id: &str,
    model: &str,
    workspace_root: &std::path::Path,
) -> Session {
    let mut session = Session::new().with_workspace_root(workspace_root);
    session.session_id = session_id.to_string();
    session.model = Some(model.to_string());
    session
}

fn packet_focus_novelty_target_bp(packet: &AgentTaskPacket) -> u16 {
    packet
        .team_role_assignment()
        .map(|assignment| assignment.identity.novelty_target_bp)
        .unwrap_or(0)
        .min(10_000)
}

fn packet_focus_acceptance_scopes(packet: &AgentTaskPacket) -> Vec<String> {
    let mut scopes = packet
        .required_acceptance
        .evidence_obligations
        .iter()
        .map(crate::path_identity::obligation_scope_key)
        .collect::<Vec<_>>();
    scopes.sort();
    scopes.dedup();
    scopes
}

fn packet_required_output_fields(packet: &AgentTaskPacket) -> Vec<String> {
    let mut fields = packet_acceptance_contract(packet)
        .into_iter()
        .filter_map(|requirement| match requirement.check {
            harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
            | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
                Some(field.as_str().to_string())
            }
            harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => Some(
                harness_contract::team::TeamStructuredOutputField::SourceVerification
                    .as_str()
                    .to_string(),
            ),
            harness_contract::team::TeamAcceptanceCheck::UpstreamReview => Some(
                harness_contract::team::TeamStructuredOutputField::Review
                    .as_str()
                    .to_string(),
            ),
            harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
            | harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
            harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound { .. } => {
                Some("legacy_acceptance".to_string())
            }
        })
        .collect::<Vec<_>>();
    fields.sort();
    fields.dedup();
    fields
}

fn agent_terminal_outcome(
    completion: harness_contract::goal::GoalCompletion,
    terminal_answer: &str,
) -> (AgentTerminalStatus, Option<String>) {
    match completion {
        harness_contract::goal::GoalCompletion::Satisfied => (AgentTerminalStatus::Completed, None),
        harness_contract::goal::GoalCompletion::Partial => (
            AgentTerminalStatus::Blocked,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::WaitingExternalDecision => (
            AgentTerminalStatus::Blocked,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::Cancelled => (
            AgentTerminalStatus::Cancelled,
            Some(terminal_answer.to_string()),
        ),
        harness_contract::goal::GoalCompletion::Open => (
            AgentTerminalStatus::Failed,
            Some("child turn returned an open goal as a terminal result".to_string()),
        ),
    }
}

fn agent_input_text(input: &AgentInput) -> String {
    match input {
        AgentInput::UserSupplement(text) => text.clone(),
        AgentInput::PeerMessage {
            from_agent_id,
            message,
        } => format!("Peer message from {from_agent_id}: {message}"),
        AgentInput::ControlContext(value) => format!("Control context: {value}"),
        AgentInput::ApprovalResult {
            approval_id,
            approved,
        } => format!(
            "Approval {approval_id}: {}",
            if *approved { "approved" } else { "denied" }
        ),
    }
}

struct ScopedRuntimeToolExecutor {
    host: Arc<dyn RuntimeExecutionHost>,
    allowed_tools: BTreeSet<String>,
    session_id: String,
    sandbox_posture: harness_contract::policy::SandboxPosture,
    policy_revision: u64,
    memory_context: memory::MemoryTurnContext,
    model_lease: String,
    execution_id: String,
    node_id: String,
    attempt: u32,
    workspace_root: std::path::PathBuf,
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    scope_locks: Arc<ScopeLockManager>,
    /// Production instances persist every canonical ToolHost receipt before
    /// exposing it to the child terminal evaluator. Unit fixtures may omit
    /// this ledger because they do not model a durable RuntimeServices host.
    commit_service: Option<crate::execution_core::graph::ExecutionCommitService>,
    /// `Some` marks a Team child and is always enforced. An empty list means
    /// no workspace authority; it never expands to the whole repository.
    resource_scopes: Option<Vec<String>>,
    managed_invocation: Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    next_receipt_sequence: AtomicU64,
    receipts: Mutex<Vec<ScopedToolExecutionReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedToolExecutionReceipt {
    sequence: u64,
    effect_kind: harness_contract::tool::ToolEffectKind,
    resource_scopes: Vec<String>,
    paths: Vec<String>,
    prior_states: BTreeMap<String, harness_contract::context::WorkspacePriorState>,
    after_digests: BTreeMap<String, Option<String>>,
    observed_evidence: Vec<harness_contract::context::ObservedEvidence>,
}

fn scoped_receipt_from_durable(
    receipt: crate::execution_core::graph::DurableAgentToolReceipt,
) -> ScopedToolExecutionReceipt {
    let mut paths = Vec::new();
    let mut prior_states = BTreeMap::new();
    let mut after_digests = BTreeMap::new();
    for evidence in &receipt.outcome.observed_evidence {
        let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
            &evidence.target
        else {
            continue;
        };
        let path = scope.path.workspace_relative_path.clone();
        if !paths.contains(&path) {
            paths.push(path.clone());
        }
        if let Some(state) = evidence.workspace_prior_state.clone() {
            prior_states.insert(path.clone(), state);
        }
        after_digests.insert(path, scope.path.observed_revision_or_digest.clone());
    }
    paths.sort();
    ScopedToolExecutionReceipt {
        sequence: receipt.sequence,
        effect_kind: receipt.effect_kind,
        resource_scopes: receipt.authorized_scopes,
        paths,
        prior_states,
        after_digests,
        observed_evidence: receipt.outcome.observed_evidence,
    }
}

/// Bound, Runtime-attested recovery context for a delegated attempt that
/// crashed after ToolHost committed effects. It deliberately contains only
/// the canonical receipt outputs and evidence the Agent already holds under
/// its role lease; it never reads the live workspace or asks the model to
/// reconstruct a side effect.
fn recovered_agent_tool_receipt_prompt(
    receipts: &[crate::execution_core::graph::DurableAgentToolReceipt],
) -> Option<String> {
    if receipts.is_empty() {
        return None;
    }
    let evidence = receipts
        .iter()
        .map(|receipt| {
            serde_json::json!({
                "sequence": receipt.sequence,
                "effect_kind": receipt.effect_kind,
                "authorized_scopes": receipt.authorized_scopes,
                "outcome": receipt.outcome,
            })
        })
        .collect::<Vec<_>>();
    let serialized = serde_json::to_string(&evidence).ok()?;
    let bounded = serialized.chars().take(48_000).collect::<String>();
    Some(format!(
        "# Durable tool-receipt recovery\nA previous process already committed the following Runtime ToolHost receipts for this exact Agent attempt. They are authoritative. Do not call tools, retry an action, or infer new workspace state. Produce one concise terminal response grounded only in these retained receipts; state any unresolved requirement plainly.\n\n{bounded}"
    ))
}

#[async_trait::async_trait]
impl ToolExecutor for ScopedRuntimeToolExecutor {
    async fn execute_output(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if tool_name == "tool_search" {
            let query = serde_json::from_str::<serde_json::Value>(input)
                .ok()
                .and_then(|value| {
                    value
                        .get("query")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_default();
            let mut receipt = self.tool_discovery_receipt();
            receipt.query = query;
            return serde_json::to_string(&receipt)
                .map(harness_contract::context::ToolOutputDraft::bounded_inline)
                .map_err(|error| {
                    ToolError::new(format!("serialize agent tool discovery: {error}"))
                });
        }
        if tool_name == "checkpoint_create" {
            return Err(ToolError::new(
                "checkpoint_create is a Runtime-internal mutation guard and cannot be invoked by the delegated model",
            ));
        }
        if !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is outside the AgentTaskPacket allow-list"
            )));
        }
        let normalized_input = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        )?;
        self.enforce_resource_ceiling(tool_name, &normalized_input)?;
        self.execute_scoped(tool_name, &normalized_input, None)
            .await
            .map(harness_contract::context::ToolOutputDraft::bounded_inline)
    }

    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        use harness_contract::tool::{
            ToolDescriptorHealth, ToolDescriptorRef, ToolDiscoveryReceipt, ToolPermissionMode,
        };

        let mut descriptors = Vec::with_capacity(self.allowed_tools.len().saturating_add(1));
        descriptors.push(ToolDescriptorRef {
            canonical_id: "tool_search".to_string(),
            display_name: "tool_search".to_string(),
            source: "delegated-agent".to_string(),
            schema_hash: "delegated-agent:tool-search:v1".to_string(),
            required_permission: ToolPermissionMode::ReadOnly,
            permission_source: "runtime bootstrap".to_string(),
            health: ToolDescriptorHealth::Healthy,
        });
        descriptors.extend(self.allowed_tools.iter().filter_map(|tool_name| {
            let descriptor = self
                .host
                .delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))?;
            Some(ToolDescriptorRef {
                canonical_id: tool_name.clone(),
                display_name: tool_name.clone(),
                source: "delegated-agent".to_string(),
                schema_hash: descriptor.descriptor_hash,
                required_permission: descriptor.required_permission,
                permission_source: "runtime binding plus host snapshot".to_string(),
                health: ToolDescriptorHealth::Healthy,
            })
        }));
        ToolDiscoveryReceipt {
            query: "delegated-agent".to_string(),
            catalog_revision: 0,
            descriptors,
            activation_candidates: self.allowed_tools.iter().cloned().collect(),
        }
    }

    fn registered_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        if tool_name == "checkpoint_create" {
            return self
                .internal_checkpoint_input(input.clone())
                .ok()
                .and_then(|input| {
                    self.host
                        .delegated_tool_effect_descriptor(tool_name, &input)
                });
        }
        self.allowed_tools.contains(tool_name).then(|| {
            let normalized = normalize_delegated_resource_value(
                tool_name,
                input.clone(),
                &self.workspace_root,
                &self.path_identity_resolver,
                self.resource_scopes.as_deref(),
            );
            self.host
                .delegated_tool_effect_descriptor(tool_name, &normalized)
        })?
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if authorization.tool_id != tool_name {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        if tool_name == "checkpoint_create" {
            return self
                .execute_internal_checkpoint(input, authorization.clone())
                .await
                .map(harness_contract::context::ToolOutputDraft::bounded_inline);
        }
        // Runtime-owned collaborative tools must be delegated back to the
        // Gateway RuntimeExecutionHost. They are not pure ToolHost adapters;
        // letting them fall through would fail every required Team node with
        // "has no ToolHost implementation adapter".
        if matches!(tool_name, "team_board" | "evidence_retrieve") {
            if !self.allowed_tools.contains(tool_name) {
                return Err(ToolError::new(
                    "agent tool authorization does not match the allowed tool request",
                ));
            }
            return self
                .execute_delegated_runtime_tool(tool_name, input, authorization.clone())
                .await
                .map(harness_contract::context::ToolOutputDraft::bounded_inline);
        }
        if !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        let normalized_input = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        )?;
        self.enforce_resource_ceiling(tool_name, &normalized_input)?;
        self.execute_scoped(tool_name, &normalized_input, Some(authorization.clone()))
            .await
            .map(harness_contract::context::ToolOutputDraft::bounded_inline)
    }

    fn available_tool_names(&self) -> Vec<String> {
        std::iter::once("tool_search".to_string())
            .chain(self.allowed_tools.iter().cloned())
            .collect()
    }

    fn observed_evidence_snapshot(&self) -> Vec<harness_contract::context::ObservedEvidence> {
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .flat_map(|receipt| receipt.observed_evidence.iter().cloned())
            .collect()
    }

    fn has_tool(&self, tool_name: &str) -> bool {
        tool_name == "tool_search"
            || self.allowed_tools.contains(tool_name)
            || (tool_name == "checkpoint_create"
                && self
                    .host
                    .delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))
                    .is_some())
    }

    fn classify_tool_safety(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Option<crate::ToolSafetyCategory> {
        if !self.allowed_tools.contains(tool_name) {
            return None;
        }
        let input = serde_json::from_str::<serde_json::Value>(input).ok()?;
        self.host
            .delegated_tool_effect_descriptor(tool_name, &input)
            .map(|effect| crate::ToolSafetyCategory::from_effect(&effect))
    }
}

impl ScopedRuntimeToolExecutor {
    fn internal_checkpoint_input(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let mut object = input
            .as_object()
            .cloned()
            .ok_or_else(|| ToolError::new("Runtime checkpoint input must be a JSON object"))?;
        if let Some(scopes) = self.resource_scopes.as_deref() {
            let mut paths = scopes
                .iter()
                .filter_map(|scope| scope.strip_prefix("write:"))
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            paths.sort();
            paths.dedup();
            if paths.is_empty() {
                return Err(ToolError::new(
                    "Runtime checkpoint requires a bounded Team write scope",
                ));
            }
            object.insert("paths".to_string(), serde_json::json!(paths));
        }
        Ok(serde_json::Value::Object(object))
    }

    async fn execute_internal_checkpoint(
        &self,
        input: &str,
        authorization: harness_contract::tool::ToolExecutionAuthorization,
    ) -> Result<String, ToolError> {
        let input = serde_json::from_str::<serde_json::Value>(input).map_err(|error| {
            ToolError::new(format!("invalid Runtime checkpoint input: {error}"))
        })?;
        let input = self.internal_checkpoint_input(input)?;
        if self
            .host
            .delegated_tool_effect_descriptor("checkpoint_create", &input)
            .is_none()
        {
            return Err(ToolError::new(
                "Runtime ToolHost has no checkpoint_create capability",
            ));
        }
        let request = RuntimeToolExecutionRequest {
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: 1,
            observation_wave_sequence: 0,
            idempotency_key: authorization
                .idempotency_key
                .clone()
                .unwrap_or_else(|| format!("agent-checkpoint:{}", uuid::Uuid::new_v4())),
            tool_use_id: format!("agent-checkpoint:{}", uuid::Uuid::new_v4()),
            tool_name: "checkpoint_create".to_string(),
            input: serde_json::to_string(&input).map_err(|error| {
                ToolError::new(format!("serialize Runtime checkpoint input: {error}"))
            })?,
            category: crate::ToolSafetyCategory::WriteLocal,
            authorization: Some(authorization),
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: Vec::new(),
            memory_context: Some(self.memory_context.clone()),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            // An Agent evaluation Binding is candidate provenance, not the
            // tool-free Judge surface. The exact Team resource ceiling above
            // remains the business-effect sandbox. This checkpoint is a
            // Runtime-owned guard and is deliberately not an Agent effect.
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        };
        let outcome = self.host.execute_runtime_tool(&request).await;
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => Ok(outcome.output.unwrap_or_default()),
            RuntimeToolExecutionStatus::BlockedPermission => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "checkpoint blocked by policy".into()),
            )),
            RuntimeToolExecutionStatus::Failed => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "checkpoint creation failed".into()),
            )),
        }
    }

    async fn execute_delegated_runtime_tool(
        &self,
        tool_name: &str,
        input: &str,
        authorization: harness_contract::tool::ToolExecutionAuthorization,
    ) -> Result<String, ToolError> {
        let input = serde_json::from_str::<serde_json::Value>(input).map_err(|error| {
            ToolError::new(format!("invalid Runtime delegated tool input: {error}"))
        })?;
        let operation = input
            .get("operation")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let category = if tool_name == "team_board" && operation == "publish" {
            crate::ToolSafetyCategory::WriteLocal
        } else {
            crate::ToolSafetyCategory::ReadOnly
        };
        let request = RuntimeToolExecutionRequest {
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: 1,
            observation_wave_sequence: 0,
            idempotency_key: authorization
                .idempotency_key
                .clone()
                .unwrap_or_else(|| format!("agent-runtime-tool:{}", uuid::Uuid::new_v4())),
            tool_use_id: format!("agent-runtime-tool:{}:{}", tool_name, uuid::Uuid::new_v4()),
            tool_name: tool_name.to_string(),
            input: serde_json::to_string(&input).map_err(|error| {
                ToolError::new(format!("serialize Runtime delegated tool input: {error}"))
            })?,
            category,
            authorization: Some(authorization),
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: self.authorized_scopes_for_tool(),
            memory_context: Some(self.memory_context.clone()),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            evaluation_isolated: false,
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        };
        let outcome = self.host.execute_runtime_tool(&request).await;
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => Ok(outcome.output.unwrap_or_default()),
            RuntimeToolExecutionStatus::BlockedPermission => {
                Err(ToolError::new(outcome.error.unwrap_or_else(|| {
                    format!("{tool_name} blocked by policy").into()
                })))
            }
            RuntimeToolExecutionStatus::Failed => {
                Err(ToolError::new(outcome.error.unwrap_or_else(|| {
                    format!("{tool_name} execution failed").into()
                })))
            }
        }
    }

    fn authorized_scopes_for_tool(&self) -> Vec<String> {
        let mut scopes = self.resource_scopes.clone().unwrap_or_default();
        let session_scope = format!("session:{}", self.session_id);
        if !scopes.iter().any(|scope| scope == &session_scope) {
            scopes.push(session_scope);
        }
        scopes
    }

    fn enforce_resource_ceiling(&self, tool_name: &str, input: &str) -> Result<(), ToolError> {
        let Some(allowed_scopes) = self.resource_scopes.as_deref() else {
            return Ok(());
        };
        // Context retrieval is bounded by the Runtime-issued MemoryTurnContext
        // and durable Session actor/workspace checks, not by a filesystem path.
        // Treating its read-only runtime scope as an unbounded path would make
        // Team Agents lose the context continuity that the primary Agent has.
        if tool_name == "context_retrieve" {
            return Ok(());
        }
        let input = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
        let descriptor = self
            .host
            .delegated_tool_effect_descriptor(tool_name, &input)
            .ok_or_else(|| ToolError::new("tool has no enforceable Runtime effect descriptor"))?;
        if descriptor.spawns_process
            || matches!(
                descriptor.effect_kind,
                harness_contract::tool::ToolEffectKind::Process
                    | harness_contract::tool::ToolEffectKind::Package
                    | harness_contract::tool::ToolEffectKind::System
                    | harness_contract::tool::ToolEffectKind::Destructive
                    | harness_contract::tool::ToolEffectKind::Unknown
            )
        {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` cannot prove a bounded Team resource scope"
            )));
        }
        let requested = crate::governed_tool_plan::resource_scope_from_effect(&descriptor);
        if requested.network {
            return allowed_scopes
                .iter()
                .any(|scope| scope == "network:*")
                .then_some(())
                .ok_or_else(|| {
                    ToolError::new(format!(
                        "tool `{tool_name}` is outside the Team network resource lease"
                    ))
                });
        }
        if requested.unknown || requested.kind == "runtime" || requested.paths.is_empty() {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` did not declare a bounded workspace path"
            )));
        }
        let write = matches!(
            descriptor.effect_kind,
            harness_contract::tool::ToolEffectKind::Write
        );
        for path in &requested.paths {
            if !resource_path_is_authorized(
                &self.path_identity_resolver,
                path,
                allowed_scopes,
                write,
            ) {
                return Err(ToolError::new(format!(
                    "tool `{tool_name}` path `{path}` is outside the Agent focus/resource lease"
                )));
            }
        }
        Ok(())
    }

    async fn execute_scoped(
        &self,
        tool_name: &str,
        input: &str,
        authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
    ) -> Result<String, ToolError> {
        let parsed_input = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
        let descriptor = self
            .host
            .delegated_tool_effect_descriptor(tool_name, &parsed_input)
            .ok_or_else(|| ToolError::new("tool has no enforceable Runtime effect descriptor"))?;
        let requested = crate::governed_tool_plan::resource_scope_from_effect(&descriptor);
        let resource_scopes = if requested.network {
            vec!["network:*".to_string()]
        } else {
            let mode = if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Write {
                "write"
            } else {
                "read"
            };
            requested
                .paths
                .iter()
                .map(|path| format!("{mode}:{path}"))
                .collect()
        };
        let sequence = self
            .next_receipt_sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        // AgentTask deliberately does not retain its broad resource locks while
        // awaiting the delegated child.  The concrete leaf effect therefore
        // acquires the same canonical locks used by graph ToolBatch nodes.  The
        // lease spans pre-image capture, execution and receipt materialization,
        // so neither the evidence snapshot nor the side effect can race another
        // in-process or persistent scoped executor.
        let lock_mode = if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Write {
            ScopeLockMode::Write
        } else {
            ScopeLockMode::Read
        };
        let lock_requests = requested
            .paths
            .iter()
            .map(|path| {
                self.path_identity_resolver
                    .resolve_planned_file(path)
                    .map(|identity| ScopeLockRequest {
                        scope: ScopedResource::workspace_object(identity),
                        mode: lock_mode,
                    })
                    .map_err(|error| {
                        ToolError::new(format!(
                            "tool `{tool_name}` has an invalid scoped lock target `{path}`: {error}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let _scope_lock = if lock_requests.is_empty() {
            None
        } else {
            Some(
                self.scope_locks
                    .acquire(lock_requests, None)
                    .await
                    .map_err(|error| {
                        ToolError::new(format!(
                            "tool `{tool_name}` could not acquire its scoped resource lease: {error}"
                        ))
                    })?,
            )
        };
        let idempotency_key = authorization
            .as_ref()
            .and_then(|value| value.idempotency_key.clone())
            .unwrap_or_else(|| {
                deterministic_scoped_tool_idempotency_key(
                    &self.execution_id,
                    &self.node_id,
                    self.attempt,
                    sequence,
                    tool_name,
                    input,
                )
            });
        let request = RuntimeToolExecutionRequest {
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: sequence,
            observation_wave_sequence: sequence,
            idempotency_key,
            tool_use_id: format!(
                "agent-tool:{}:{}:{}:{tool_name}",
                self.node_id, self.attempt, sequence
            ),
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            category: crate::ToolSafetyCategory::from_effect(&descriptor),
            authorization,
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: self.authorized_scopes_for_tool(),
            memory_context: Some(self.memory_context.clone()),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            // Candidate-evaluation provenance does not make the child a
            // Judge. ScopedRuntimeToolExecutor already enforces the exact
            // Runtime-compiled resource ceiling for every business effect.
            evaluation_isolated: false,
            managed_invocation: self.managed_invocation.clone(),
            tool_progress: crate::ToolProgressSink::default(),
        };
        let effect_state = self
            .commit_service
            .as_ref()
            .map(|service| service.begin_tool_effect(&request, &descriptor))
            .transpose()
            .map_err(|error| {
                ToolError::new(format!(
                    "tool `{tool_name}` durable effect admission failed: {error}"
                ))
            })?
            .unwrap_or(crate::execution_core::graph::ToolEffectState::Fresh);
        let (outcome, fresh_execution) = match effect_state {
            crate::execution_core::graph::ToolEffectState::Completed(mut outcome) => {
                outcome.tool_use_id.clone_from(&request.tool_use_id);
                outcome.tool_name.clone_from(&request.tool_name);
                outcome.category = request.category;
                for evidence in &mut outcome.observed_evidence {
                    evidence.provenance =
                        harness_contract::context::ObservedEvidenceProvenance::RetainedReplay;
                }
                (outcome, false)
            }
            crate::execution_core::graph::ToolEffectState::Uncertain => {
                return Err(ToolError::new(
                    "tool effect is uncertain; non-idempotent execution was not replayed",
                ));
            }
            crate::execution_core::graph::ToolEffectState::Fresh
            | crate::execution_core::graph::ToolEffectState::NotRequired => {
                (self.host.execute_runtime_tool(&request).await, true)
            }
        };
        if fresh_execution {
            if let Some(commit_service) = &self.commit_service {
                let committed =
                    if descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read {
                        commit_service
                            .commit_readonly_tool_receipts(&[(request.clone(), outcome.clone())])
                    } else {
                        commit_service.commit_tool_effect(&request, &descriptor, &outcome)
                    };
                if let Err(error) = committed {
                    return Err(ToolError::new(format!(
                        "tool `{tool_name}` completed but durable receipt commit failed: {error}"
                    )));
                }
            }
        }
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => {
                let observed_evidence = outcome.observed_evidence.clone();
                let prior_states = requested
                    .paths
                    .iter()
                    .filter_map(|path| {
                        let identity = self
                            .path_identity_resolver
                            .resolve_planned_file(path)
                            .ok()?;
                        observed_evidence
                            .iter()
                            .find_map(|evidence| match &evidence.target {
                                harness_contract::context::EvidenceTargetIdentity::Workspace {
                                    scope,
                                } if scope.path.workspace_id == identity.workspace_id
                                    && scope.path.repository_id == identity.repository_id
                                    && scope.path.workspace_relative_path
                                        == identity.workspace_relative_path =>
                                {
                                    evidence
                                        .workspace_prior_state
                                        .clone()
                                        .map(|state| (path.clone(), state))
                                }
                                _ => None,
                            })
                    })
                    .collect::<BTreeMap<_, _>>();
                let after_digests = requested
                    .paths
                    .iter()
                    .map(|path| {
                        let identity = self.path_identity_resolver.resolve_planned_file(path).ok();
                        let digest =
                            identity.as_ref().and_then(|identity| {
                                observed_evidence.iter().find_map(|evidence| {
                                    match &evidence.target {
                                harness_contract::context::EvidenceTargetIdentity::Workspace {
                                    scope,
                                } if scope.path.workspace_id == identity.workspace_id
                                    && scope.path.repository_id == identity.repository_id
                                    && scope.path.workspace_relative_path
                                        == identity.workspace_relative_path =>
                                {
                                    scope.path.observed_revision_or_digest.clone()
                                }
                                _ => None,
                            }
                                })
                            });
                        (path.clone(), digest)
                    })
                    .collect::<BTreeMap<_, _>>();
                self.receipts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(ScopedToolExecutionReceipt {
                        sequence,
                        effect_kind: descriptor.effect_kind,
                        resource_scopes,
                        paths: requested.paths,
                        prior_states,
                        after_digests,
                        observed_evidence,
                    });
                Ok(outcome.output.unwrap_or_default())
            }
            RuntimeToolExecutionStatus::BlockedPermission => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "tool blocked by policy".into()),
            )),
            RuntimeToolExecutionStatus::Failed => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "tool execution failed".into()),
            )),
        }
    }
}

fn deterministic_scoped_tool_idempotency_key(
    execution_id: &str,
    node_id: &str,
    attempt: u32,
    sequence: u64,
    tool_name: &str,
    input: &str,
) -> String {
    let input_sha256 = format!("{:x}", Sha256::digest(input.as_bytes()));
    format!("agent-tool:{execution_id}:{node_id}:{attempt}:{sequence}:{tool_name}:{input_sha256}")
}

fn normalize_delegated_resource_paths(
    tool_name: &str,
    input: &str,
    workspace_root: &std::path::Path,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> Result<String, ToolError> {
    let parsed = serde_json::from_str::<serde_json::Value>(input)
        .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
    let parsed = normalize_delegated_resource_value(
        tool_name,
        parsed,
        workspace_root,
        path_identity_resolver,
        resource_scopes,
    );
    serde_json::to_string(&parsed)
        .map_err(|error| ToolError::new(format!("serialize normalized scoped tool input: {error}")))
}

fn normalize_delegated_resource_value(
    tool_name: &str,
    parsed: serde_json::Value,
    workspace_root: &std::path::Path,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> serde_json::Value {
    let parsed = normalize_workspace_internal_resource_value(tool_name, parsed, workspace_root);
    let mut parsed = normalize_single_scope_relative_read_value(
        tool_name,
        parsed,
        path_identity_resolver,
        resource_scopes,
    );
    if tool_name != "glob_search" {
        return parsed;
    }
    let Some(scopes) = resource_scopes else {
        return parsed;
    };
    let Some(object) = parsed.as_object_mut() else {
        return parsed;
    };
    let Some(pattern) = object
        .get("pattern")
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().replace('\\', "/"))
    else {
        return parsed;
    };
    let requested_root = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|path| workspace_root_request(path, workspace_root));
    if !requested_root {
        return parsed;
    }

    let mut allowed = scopes
        .iter()
        .filter_map(|scope| {
            let (mode, path) = scope.split_once(':')?;
            matches!(mode, "read" | "write").then_some(path)
        })
        .filter_map(|path| {
            let parts = normalized_relative_parts(path)?;
            Some(if parts.is_empty() {
                ".".to_string()
            } else {
                parts.join("/")
            })
        })
        .collect::<Vec<_>>();
    allowed.sort();
    allowed.dedup();
    let matched = allowed
        .iter()
        .filter(|scope| {
            scope.as_str() == "."
                || pattern == scope.as_str()
                || pattern
                    .strip_prefix(scope.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut allowed = if matched.is_empty()
        && allowed.len() == 1
        && glob_pattern_has_no_explicit_root(&pattern)
    {
        allowed
    } else {
        matched
    };
    allowed.sort_by_key(|scope| std::cmp::Reverse(scope.len()));
    let Some(scope) = allowed.first() else {
        return parsed;
    };
    if scope == "." {
        object.insert(
            "path".to_string(),
            serde_json::Value::String(".".to_string()),
        );
        return parsed;
    }
    let suffix = pattern
        .strip_prefix(scope)
        .unwrap_or_default()
        .trim_start_matches('/');
    let Ok(scoped_identity) = path_identity_resolver.resolve_existing(scope) else {
        return parsed;
    };
    if scoped_identity.object_kind == harness_contract::context::WorkspaceObjectKind::File {
        let Some(file_name) = std::path::Path::new(&scoped_identity.workspace_relative_path)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            return parsed;
        };
        let parent = std::path::Path::new(scope)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(
                || ".".to_string(),
                |parent| parent.to_string_lossy().into_owned(),
            );
        object.insert("path".to_string(), serde_json::Value::String(parent));
        object.insert(
            "pattern".to_string(),
            serde_json::Value::String(file_name.to_string()),
        );
        return parsed;
    }
    if scoped_identity.object_kind != harness_contract::context::WorkspaceObjectKind::Directory {
        return parsed;
    }
    object.insert("path".to_string(), serde_json::Value::String(scope.clone()));
    if !suffix.is_empty() {
        object.insert(
            "pattern".to_string(),
            serde_json::Value::String(suffix.to_string()),
        );
    }
    parsed
}

fn glob_pattern_has_no_explicit_root(pattern: &str) -> bool {
    pattern
        .split('/')
        .next()
        .is_some_and(|segment| segment.contains(['*', '?', '[', '{']))
}

fn workspace_root_request(path: &str, workspace_root: &std::path::Path) -> bool {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "./" {
        return true;
    }
    let requested = std::path::Path::new(trimmed);
    requested.is_absolute()
        && requested
            .canonicalize()
            .ok()
            .zip(workspace_root.canonicalize().ok())
            .is_some_and(|(requested, root)| requested == root)
}

/// Native file tools take workspace-root-relative paths, while a delegated
/// objective may phrase a file relative to its sole authorized directory.
/// Normalize only that unambiguous read case: never search sibling scopes,
/// never rewrite an already-resolvable path, and require the exact scoped
/// candidate to satisfy the typed authorization boundary.
fn normalize_single_scope_relative_read_value(
    tool_name: &str,
    mut parsed: serde_json::Value,
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> serde_json::Value {
    if tool_name != "read_file" {
        return parsed;
    }
    let Some(scopes) = resource_scopes else {
        return parsed;
    };
    let mut directories = scopes
        .iter()
        .filter_map(|scope| {
            let (mode, path) = scope.split_once(':')?;
            matches!(mode, "read" | "write").then_some(path.trim().replace('\\', "/"))
        })
        .filter(|path| !matches!(path.as_str(), "" | "." | "./"))
        .filter(|path| {
            resolver.resolve_existing(path).is_ok_and(|identity| {
                identity.object_kind == harness_contract::context::WorkspaceObjectKind::Directory
            })
        })
        .collect::<Vec<_>>();
    directories.sort();
    directories.dedup();
    let [scope] = directories.as_slice() else {
        return parsed;
    };

    let replacements = resource_paths_from_input(&parsed)
        .into_iter()
        .filter_map(|requested| {
            let parts = normalized_relative_parts(&requested)?;
            let requested = parts.join("/");
            if requested.is_empty() || path_within_scope(&requested, scope) {
                return None;
            }
            if let Ok(identity) = resolver.resolve_existing(&requested) {
                let canonical = identity.workspace_relative_path;
                return (canonical != requested
                    && resource_path_is_authorized(resolver, &canonical, scopes, false))
                .then_some((requested, canonical));
            }
            let candidate = format!("{scope}/{requested}");
            resource_path_is_authorized(resolver, &candidate, scopes, false)
                .then_some((requested, candidate))
        })
        .collect::<BTreeMap<_, _>>();
    rewrite_resource_path_fields(&mut parsed, &replacements);
    parsed
}

fn rewrite_resource_path_fields(
    value: &mut serde_json::Value,
    replacements: &BTreeMap<String, String>,
) {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "path" | "file" | "file_path") {
                    if let Some(path) = value.as_str() {
                        let normalized = path.trim().replace('\\', "/");
                        if let Some(replacement) = replacements.get(&normalized) {
                            *value = serde_json::Value::String(replacement.clone());
                        }
                    }
                } else {
                    rewrite_resource_path_fields(value, replacements);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_resource_path_fields(value, replacements);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

/// Normalize workspace-internal absolute resource paths before both effect
/// description and execution. Keeping those two inputs byte-equivalent is a
/// security invariant: an input-sensitive ToolHost must be able to verify the
/// authorization descriptor without treating the safe relative rewrite as a
/// stale or escalated effect.
fn normalize_workspace_internal_resource_value(
    _tool_name: &str,
    mut parsed: serde_json::Value,
    workspace_root: &std::path::Path,
) -> serde_json::Value {
    let replacements = resource_paths_from_input(&parsed)
        .iter()
        .filter_map(|path| {
            let absolute = std::path::Path::new(path);
            if !absolute.is_absolute() {
                return None;
            }
            let relative = absolute.strip_prefix(workspace_root).ok()?;
            let parts = normalized_relative_parts(&relative.to_string_lossy())?;
            Some((
                path.clone(),
                if parts.is_empty() {
                    ".".to_string()
                } else {
                    parts.join("/")
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();
    if replacements.is_empty() {
        return parsed;
    }

    rewrite_resource_path_fields(&mut parsed, &replacements);
    parsed
}

fn resource_paths_from_input(input: &serde_json::Value) -> Vec<String> {
    fn collect(value: &serde_json::Value, paths: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    if matches!(key.as_str(), "path" | "file" | "file_path") {
                        if let Some(path) = value.as_str() {
                            paths.push(path.trim().replace('\\', "/"));
                        }
                    } else {
                        collect(value, paths);
                    }
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, paths);
                }
            }
            _ => {}
        }
    }
    let mut paths = Vec::new();
    collect(input, &mut paths);
    paths.sort();
    paths.dedup();
    paths
}

fn delegated_tool_supports_bounded_scope(host: &dyn RuntimeExecutionHost, tool_name: &str) -> bool {
    host.delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))
        .is_some_and(|descriptor| {
            !descriptor.spawns_process
                && !matches!(
                    descriptor.effect_kind,
                    harness_contract::tool::ToolEffectKind::Process
                        | harness_contract::tool::ToolEffectKind::Package
                        | harness_contract::tool::ToolEffectKind::System
                        | harness_contract::tool::ToolEffectKind::Destructive
                        | harness_contract::tool::ToolEffectKind::Unknown
                )
        })
}

fn resource_path_is_authorized(
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    requested: &str,
    allowed_scopes: &[String],
    write: bool,
) -> bool {
    let requested = if write {
        resolver.resolve_planned_file(requested)
    } else {
        resolver.resolve_existing(requested)
    };
    let Ok(requested) = requested else {
        return false;
    };
    allowed_scopes.iter().any(|scope| {
        let (mode, allowed) = scope.split_once(':').unwrap_or(("", ""));
        if (write && mode != "write") || (!write && mode != "read" && mode != "write") {
            return false;
        }
        // `read:.` / `write:.` are whole-workspace leases issued only to
        // full-trust (YOLO / danger-full-access) Teams. The Team
        // instantiation contract gates them behind
        // `allow_whole_workspace_scope`, so reaching this point with a `.`
        // scope already proves the Runtime authorized the entire workspace.
        // The workspace identity check below still bounds them to this
        // workspace and never to absolute or traversing paths.
        let allowed = if mode == "write" {
            resolver.resolve_planned_file(allowed)
        } else {
            resolver.resolve_existing(allowed)
        };
        let Ok(allowed) = allowed else {
            return false;
        };
        if requested.workspace_id != allowed.workspace_id
            || requested.repository_id != allowed.repository_id
        {
            return false;
        }
        if allowed.object_kind == harness_contract::context::WorkspaceObjectKind::Directory {
            path_within_scope(
                &requested.repository_relative_path,
                &allowed.repository_relative_path,
            )
        } else {
            requested.repository_relative_path == allowed.repository_relative_path
        }
    })
}

fn normalized_relative_parts(value: &str) -> Option<Vec<String>> {
    let normalized = value.trim().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for component in std::path::Path::new(&normalized).components() {
        match component {
            std::path::Component::Normal(part) => {
                parts.push(part.to_string_lossy().into_owned());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => return None,
        }
    }
    Some(parts)
}

fn permission_policy(
    live_control: Option<crate::permissions::SessionExecutionPolicyControl>,
    mode: PermissionMode,
    tools: &BTreeSet<String>,
) -> PermissionPolicy {
    let policy = live_control
        .map_or_else(
            || PermissionPolicy::new(mode),
            PermissionPolicy::with_execution_policy_control,
        )
        .with_immutable_ceiling(mode);
    tools.iter().fold(policy, |policy, tool| {
        policy.with_tool_requirement(tool, crate::agent_capability::agent_tool_permission(tool))
    })
}

fn system_prompt(
    packet: &AgentTaskPacket,
    workspace_root: &std::path::Path,
    tool_names: &[String],
) -> Vec<String> {
    let mut prompt = vec![
        "You are a delegated Cowd agent. Return an evidence-backed result for the assigned objective.".into(),
        "You are a leaf role inside an already-running protocol. Do not create a nested team or session; return findings and evidence to the protocol reducer.".into(),
        "Use only native tool calls exposed by this runtime. Never write simulated tool syntax such as <tool_call>, <function=...>, <parameter=...>, or JSON-shaped pseudo-calls in final text. If no native tool is authorized, answer directly from the supplied objective and upstream evidence.".into(),
        format!("Objective: {}", packet.objective),
        format!("Workspace root: {}", workspace_root.display()),
        format!(
            "Authorized resource scopes: {}. Every native file-tool path must be relative to the displayed Workspace root and retain the complete authorized scope prefix. For example, scope read:project means project/Cargo.toml, never bare Cargo.toml. A missing path never means the whole workspace.",
            if packet.resource_scopes.is_empty() {
                "(none)".to_string()
            } else {
                packet.resource_scopes.join(", ")
            }
        ),
    ];
    if let Some(binding) = &packet.binding {
        prompt.push(format!(
            "Agent Definition: {}@{} (binding {}).",
            binding.definition_ref.definition_id.as_str(),
            binding.definition_ref.revision,
            binding.binding_id,
        ));
        prompt.push(format!(
            "Definition instructions:\n{}",
            binding.instructions.trim()
        ));
        prompt.push(format!(
            "Cognitive data lease: read={:?}; write={:?}; team_working_state_visible={}",
            binding.data_lease.read_scopes,
            binding.data_lease.write_mode,
            binding.data_lease.team_working_state_visible,
        ));
    }
    if !packet.constraints.is_empty() {
        prompt.push(format!("Constraints: {}", packet.constraints.join("; ")));
    }
    if !packet.acceptance.is_empty() {
        prompt.push(format!("Acceptance: {}", packet.acceptance.join("; ")));
    }
    let mut required_write_scopes = Vec::new();
    let contract = packet_acceptance_contract(packet);
    if !contract.is_empty() {
        required_write_scopes.extend(contract.iter().flat_map(
            |requirement| match &requirement.check {
                harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { scopes, .. } => {
                    scopes.clone()
                }
                _ => Vec::new(),
            },
        ));
        required_write_scopes.sort();
        required_write_scopes.dedup();
        let mut fields = contract
            .iter()
            .filter_map(|requirement| match &requirement.check {
                harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
                | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
                    Some(field.as_str())
                }
                harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => {
                    Some("source_verification")
                }
                harness_contract::team::TeamAcceptanceCheck::UpstreamReview => Some("review"),
                harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
                harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound { .. } => {
                    Some("legacy_acceptance")
                }
                harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. } => None,
            })
            .collect::<Vec<_>>();
        fields.sort();
        fields.dedup();
        prompt.push(format!(
            "Give a concise terminal answer. When practical, label these presentation fields: {}. Native structured output, a JSON object, Markdown headings, and `Field: value` labels are all understood. Runtime derives acceptance from committed tool receipts, change paths, and upstream evidence bindings; prose never substitutes for those facts.",
            fields.join(", ")
        ));
    }
    if !tool_names.is_empty() {
        if required_write_scopes.is_empty() {
            prompt.push(format!(
                "Authorized tool contracts are available natively: {}. When the objective asks for source, workspace, file, or current-state evidence, use an authorized read-only tool and cite the resulting paths/receipts; do not substitute prior model knowledge.",
                tool_names.join(", ")
            ));
        } else {
            prompt.push(format!(
                "Authorized tool contracts are available natively: {}. This role has a Runtime-verified workspace-change obligation for: {}. Read each target at most once before mutation, invoke an authorized write tool for the required change, then perform a separate read-only verification. Repeated reads, prose claims, or simulated tool markup cannot replace the committed write receipt.",
                tool_names.join(", "),
                required_write_scopes.join(", ")
            ));
        }
    }
    prompt
}

fn agent_evidence_refs(
    packet: &AgentTaskPacket,
    audits: &[harness_contract::context::EvidenceAuditProjection],
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::context::EvidenceAccessRef> {
    let mut refs = packet.evidence_refs.clone();
    refs.extend(audits.iter().filter_map(|audit| audit.access.clone()));
    refs.extend(
        receipts
            .iter()
            .flat_map(|receipt| receipt.observed_evidence.iter())
            .filter_map(|evidence| evidence.evidence_ref.clone()),
    );
    refs.sort_by(|left, right| {
        left.evidence_ref
            .ref_type
            .cmp(&right.evidence_ref.ref_type)
            .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
    });
    refs.dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
    refs
}

/// Derive structured-field criteria from the terminal answer and canonical
/// ToolHost receipts.  Obligation matching itself remains exclusively owned
/// by `AcceptanceEvaluator::evaluate_required` at the terminal boundary.
fn derive_receipt_backed_satisfied_criteria(
    packet: &AgentTaskPacket,
    summary: &crate::TurnSummary,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    tool_executor: &ScopedRuntimeToolExecutor,
) -> (
    Vec<String>,
    Vec<harness_contract::agent::AgentChangeReceipt>,
) {
    let mut receipts = tool_executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    receipts.sort_by_key(|receipt| receipt.sequence);
    // A fresh, successfully committed scoped tool receipt is independent
    // evidence even when its content-addressed EvidenceRef equals an upstream
    // read of the same unchanged file. Comparing only EvidenceRef identity
    // incorrectly erased reviewer verification from the acceptance result.
    let produced_evidence = produced_runtime_evidence(packet, evidence_refs, &receipts);
    let changes = materialized_change_receipts(&receipts);
    let observed_evidence = receipts
        .iter()
        .flat_map(|receipt| receipt.observed_evidence.iter())
        .collect::<Vec<_>>();
    let owned_observed_evidence = observed_evidence
        .iter()
        .map(|observed| (*observed).clone())
        .collect::<Vec<_>>();
    let scope_observed = |scope: &str| {
        let raw = if scope.contains(':') {
            scope.to_string()
        } else {
            format!("read:{scope}")
        };
        // The whole-workspace read alias is only minted under a full-trust
        // lease. Compile it with root-alias tolerance so any Runtime-attested
        // descendant exact read satisfies it; the strict compiler would keep
        // the obligation unsatisfiable and fail every Team role terminal.
        let required = if matches!(raw.trim(), "read:." | "read:./") {
            tool_executor
                .path_identity_resolver
                .compile_obligation_with_root_alias(&raw, true)
                .unwrap_or_else(|_| {
                    tool_executor
                        .path_identity_resolver
                        .compile_obligation_or_unresolved(&raw)
                })
        } else {
            tool_executor
                .path_identity_resolver
                .compile_obligation_or_unresolved(&raw)
        };
        crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
            &required,
            &owned_observed_evidence,
        )
    };
    let output = structured_agent_output(&summary.final_answer);
    let field_present = |field: harness_contract::team::TeamStructuredOutputField| {
        let value = output
            .as_ref()
            .and_then(|object| object.get(field.as_str()));
        structured_field_materialized(field, value)
    };
    let changes_in_scopes = |scopes: &[String]| {
        !changes.is_empty()
            && changes.iter().all(|change| {
                scopes
                    .iter()
                    .any(|scope| path_within_scope(&change.path, scope))
            })
    };
    let upstream_changes = packet_upstream_change_receipts(packet);
    let upstream_evidence = packet
        .evidence_refs
        .iter()
        .any(crate::agent_result_validator::is_materialized_durable_evidence);
    let acceptance = packet_acceptance_contract(packet)
        .into_iter()
        .filter(|requirement| match &requirement.check {
            harness_contract::team::TeamAcceptanceCheck::StructuredField { field } => {
                // A pure reducer is grounded by the immutable predecessor
                // evidence carried in its packet. Requiring it to reacquire
                // the same source solely to populate a structured synthesis
                // field would defeat the upstream-only acceptance contract.
                (produced_evidence || upstream_evidence) && field_present(*field)
            }
            harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { scopes } => {
                produced_evidence
                    && !scopes.is_empty()
                    && scopes.iter().all(|scope| scope_observed(scope))
            }
            harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, scopes } => {
                produced_evidence && field_present(*field) && changes_in_scopes(&scopes)
            }
            harness_contract::team::TeamAcceptanceCheck::SourceVerification { scopes } => {
                produced_evidence
                    && field_present(
                        harness_contract::team::TeamStructuredOutputField::SourceVerification,
                    )
                    && changes_in_scopes(&scopes)
                    && changes.iter().all(|change| {
                        has_matching_pre_write_evidence(change, &receipts)
                            && has_matching_read_receipt(change, &receipts, true)
                    })
            }
            harness_contract::team::TeamAcceptanceCheck::UpstreamReview => {
                produced_evidence
                    && field_present(harness_contract::team::TeamStructuredOutputField::Review)
                    && upstream_evidence
                    && !upstream_changes.is_empty()
                    && upstream_changes
                        .iter()
                        .all(|change| has_matching_read_receipt(change, &receipts, false))
            }
            harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => upstream_evidence,
            harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound { scopes } => {
                produced_evidence
                    && !scopes.is_empty()
                    && scopes.iter().all(|scope| scope_observed(scope))
                    && output
                        .as_ref()
                        .and_then(|object| object.get("legacy_acceptance"))
                        .and_then(serde_json::Value::as_object)
                        .and_then(|legacy| legacy.get(&requirement.criterion))
                        .is_some_and(materialized_json_value)
            }
        })
        .map(|requirement| requirement.criterion)
        .collect::<Vec<_>>();
    (acceptance, changes)
}

fn packet_upstream_change_receipts(
    packet: &AgentTaskPacket,
) -> Vec<harness_contract::agent::AgentChangeReceipt> {
    let mut changes = packet
        .evidence_refs
        .iter()
        .filter_map(|evidence| {
            (crate::agent_result_validator::is_materialized_durable_evidence(evidence)
                && evidence.evidence_ref.ref_type == "runtime_change")
                .then(|| {
                    serde_json::from_str::<harness_contract::agent::AgentChangeReceipt>(
                        &evidence.evidence_ref.id,
                    )
                    .ok()
                })
                .flatten()
        })
        .collect::<Vec<_>>();
    changes.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.write_sequence.cmp(&right.write_sequence))
            .then_with(|| left.after_sha256.cmp(&right.after_sha256))
    });
    changes.dedup();
    changes
}

fn produced_runtime_evidence(
    packet: &AgentTaskPacket,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    receipts: &[ScopedToolExecutionReceipt],
) -> bool {
    !receipts.is_empty()
        || evidence_refs.iter().any(|evidence| {
            crate::agent_result_validator::is_materialized_durable_evidence(evidence)
                && !packet
                    .evidence_refs
                    .iter()
                    .any(|input| input.evidence_ref == evidence.evidence_ref)
        })
}

fn structured_field_materialized(
    field: harness_contract::team::TeamStructuredOutputField,
    value: Option<&serde_json::Value>,
) -> bool {
    if field == harness_contract::team::TeamStructuredOutputField::Risks {
        // An explicit empty list is a meaningful reviewed conclusion: no
        // risks were identified. The key must still be present; omission,
        // null, or an empty prose string remains non-materialized.
        value.is_some_and(|value| {
            matches!(value, serde_json::Value::Array(_)) || materialized_json_value(value)
        })
    } else {
        value.is_some_and(materialized_json_value)
    }
}

fn materialized_change_receipts(
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::agent::AgentChangeReceipt> {
    receipts
        .iter()
        .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
        .flat_map(|receipt| {
            receipt.paths.iter().filter_map(|path| {
                let prior = receipt.prior_states.get(path)?;
                let before = match prior {
                    harness_contract::context::WorkspacePriorState::Existing { sha256 } => {
                        Some(sha256.clone())
                    }
                    harness_contract::context::WorkspacePriorState::Absent => None,
                };
                let after = receipt.after_digests.get(path).cloned().flatten()?;
                (before.as_deref() != Some(after.as_str())).then(|| {
                    harness_contract::agent::AgentChangeReceipt {
                        path: path.clone(),
                        before_sha256: before,
                        after_sha256: after,
                        write_sequence: receipt.sequence,
                    }
                })
            })
        })
        .collect()
}

fn has_matching_read_receipt(
    change: &harness_contract::agent::AgentChangeReceipt,
    receipts: &[ScopedToolExecutionReceipt],
    require_later_sequence: bool,
) -> bool {
    receipts.iter().any(|receipt| {
        if (require_later_sequence && receipt.sequence <= change.write_sequence)
            || receipt.effect_kind != harness_contract::tool::ToolEffectKind::Read
        {
            return false;
        }
        // Tool effect planning may retain an absolute or `./`-prefixed key
        // while the public receipt path is workspace-relative. Resolve the
        // digest through the receipt's own key after scope normalization;
        // looking it up with the upstream spelling made a valid independent
        // review fail intermittently even though both paths named the same
        // workspace file.
        receipt.paths.iter().any(|receipt_path| {
            path_within_scope(receipt_path, &change.path)
                && path_within_scope(&change.path, receipt_path)
                && receipt
                    .after_digests
                    .get(receipt_path)
                    .and_then(|digest| digest.as_deref())
                    == Some(change.after_sha256.as_str())
        })
    })
}

fn has_matching_pre_write_evidence(
    change: &harness_contract::agent::AgentChangeReceipt,
    receipts: &[ScopedToolExecutionReceipt],
) -> bool {
    let Some(before_sha256) = change.before_sha256.as_deref() else {
        // For a new file, the write receipt itself is the Runtime-owned
        // absence proof: the tool host captured `None` before committing the
        // exact write whose sequence and after digest produced this change.
        // A later matching read is still required by SourceVerification.
        return receipts.iter().any(|receipt| {
            receipt.sequence == change.write_sequence
                && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write
                && receipt.paths.iter().any(|receipt_path| {
                    path_within_scope(receipt_path, &change.path)
                        && path_within_scope(&change.path, receipt_path)
                        && receipt.prior_states.get(receipt_path).is_some_and(|state| {
                            matches!(
                                state,
                                harness_contract::context::WorkspacePriorState::Absent
                            )
                        })
                        && receipt
                            .after_digests
                            .get(receipt_path)
                            .and_then(|digest| digest.as_deref())
                            == Some(change.after_sha256.as_str())
                })
        });
    };
    receipts.iter().any(|receipt| {
        receipt.sequence < change.write_sequence
            && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && receipt.paths.iter().any(|receipt_path| {
                path_within_scope(receipt_path, &change.path)
                    && path_within_scope(&change.path, receipt_path)
                    && receipt
                        .after_digests
                        .get(receipt_path)
                        .and_then(|digest| digest.as_deref())
                        == Some(before_sha256)
            })
    })
}

fn packet_acceptance_contract(
    packet: &AgentTaskPacket,
) -> Vec<harness_contract::team::TeamAcceptanceRequirement> {
    packet.output_acceptance.clone()
}

fn materialized_json_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::String(value) => !value.trim().is_empty(),
        serde_json::Value::Array(values) => !values.is_empty(),
        serde_json::Value::Object(values) => !values.is_empty(),
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
    }
}

/// Repair only the common syntactic drift where a provider leaves a trailing
/// comma before `}` or `]`. This deliberately does not invent keys, values, or
/// quote unquoted prose, so acceptance semantics remain model-authored.
fn without_json_trailing_commas(text: &str) -> String {
    let characters = text.chars().collect::<Vec<_>>();
    let mut repaired = String::with_capacity(text.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < characters.len() {
        let character = characters[index];
        if in_string {
            repaired.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            repaired.push('"');
            index += 1;
            continue;
        }
        if character == ',' {
            let mut lookahead = index + 1;
            while lookahead < characters.len() && characters[lookahead].is_ascii_whitespace() {
                lookahead += 1;
            }
            if lookahead < characters.len() && matches!(characters[lookahead], '}' | ']') {
                index += 1;
                continue;
            }
        }
        repaired.push(character);
        index += 1;
    }
    repaired
}

fn parse_first_contract_json(text: &str) -> Option<serde_json::Value> {
    let text = text.trim_start_matches('\u{feff}').trim();
    serde_json::Deserializer::from_str(text)
        .into_iter::<serde_json::Value>()
        .next()
        .and_then(Result::ok)
        .or_else(|| {
            let repaired = without_json_trailing_commas(text);
            (repaired != text).then(|| {
                serde_json::Deserializer::from_str(&repaired)
                    .into_iter::<serde_json::Value>()
                    .next()
                    .and_then(Result::ok)
            })?
        })
}

pub(crate) fn structured_agent_output(
    text: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    const CONTRACT_FIELDS: [&str; 14] = [
        "summary",
        "findings",
        "evidence",
        "plan",
        "implementation",
        "source_verification",
        "review",
        "risks",
        "unresolved",
        "proposal",
        "critique",
        "mitigation",
        "checkpoint",
        "legacy_acceptance",
    ];
    let has_contract_field = |object: &serde_json::Map<String, serde_json::Value>| {
        CONTRACT_FIELDS
            .iter()
            .any(|field| object.contains_key(*field))
    };
    let canonicalize = |mut object: serde_json::Map<String, serde_json::Value>| {
        const ALIASES: [(&str, &str); 13] = [
            ("conclusion", "summary"),
            ("result", "summary"),
            ("摘要", "summary"),
            ("总结", "summary"),
            ("finding", "findings"),
            ("observations", "findings"),
            ("发现", "findings"),
            ("proof", "evidence"),
            ("证据", "evidence"),
            ("open_questions", "unresolved"),
            ("gaps", "unresolved"),
            ("未解决", "unresolved"),
            ("risk", "risks"),
        ];
        for (alias, canonical) in ALIASES {
            if object.contains_key(canonical) {
                continue;
            }
            if let Some(key) = object
                .keys()
                .find(|key| key.eq_ignore_ascii_case(alias))
                .cloned()
            {
                if let Some(value) = object.remove(&key) {
                    object.insert(canonical.to_string(), value);
                }
            }
        }
        object
    };
    let contract_object = |object: serde_json::Map<String, serde_json::Value>| {
        let object = canonicalize(object);
        has_contract_field(&object).then_some(object)
    };
    if let Some(serde_json::Value::Object(object)) = parse_first_contract_json(text) {
        if let Some(object) = contract_object(object.clone()) {
            return Some(object);
        }
        // Providers commonly wrap an otherwise valid response in a single
        // `output`/`data`/`response`/`answer` envelope. Unwrap only a typed
        // object (or a string containing one) with a known contract field;
        // arbitrary prose and unrelated JSON remain untrusted.
        for wrapper in ["output", "data", "response", "answer"] {
            let Some(value) = object.get(wrapper) else {
                continue;
            };
            let nested = match value {
                serde_json::Value::Object(nested) => Some(nested.clone()),
                serde_json::Value::String(encoded) => parse_first_contract_json(encoded)
                    .and_then(|value: serde_json::Value| value.as_object().cloned()),
                _ => None,
            };
            if let Some(object) = nested.and_then(&contract_object) {
                return Some(object);
            }
        }
    }
    if let Some(object) = text
        .char_indices()
        .filter(|(_, character)| *character == '{')
        .filter_map(|(start, _)| parse_first_contract_json(&text[start..]))
        .filter_map(|value| value.as_object().cloned())
        // An agent may quote an upstream JSON result before returning its own
        // terminal object. The terminal contract is the last matching object,
        // while exact whole-response JSON was already handled above.
        .filter_map(&contract_object)
        // Nested rows inside the terminal object can individually match a
        // contract field (for example an `unresolved_or_risks` item shaped as
        // `{"id","title","mitigation"}`). Prefer the outermost terminal
        // object: it carries the primary `summary` field and the largest
        // field set, so a quoted or nested fragment never wins.
        .max_by_key(|object| (object.contains_key("summary"), object.len()))
    {
        return Some(object);
    }

    // Some providers occasionally honor the requested field names but return
    // exact level-two Markdown sections instead of JSON. Normalize only those
    // explicit contract headings; arbitrary prose remains non-structured.
    // Runtime acceptance still requires independent tool/change receipts, so
    // this cannot turn a self-reported review into verified evidence.
    let mut object = serde_json::Map::new();
    let mut active_field: Option<&str> = None;
    let mut active_lines = Vec::new();
    let flush = |object: &mut serde_json::Map<String, serde_json::Value>,
                 field: Option<&str>,
                 lines: &mut Vec<&str>| {
        if let Some(field) = field {
            let value = lines.join("\n").trim().to_string();
            if !value.is_empty() {
                object.insert(field.to_string(), serde_json::Value::String(value));
            }
        }
        lines.clear();
    };
    for line in text.lines() {
        let trimmed = line.trim();
        let heading = trimmed
            .strip_prefix('#')
            .map(|value| value.trim_start_matches('#').trim())
            .or_else(|| {
                trimmed
                    .strip_prefix("**")
                    .and_then(|value| value.strip_suffix("**"))
                    .map(|value| value.trim_end_matches(':').trim())
            });
        if let Some(heading) = heading {
            flush(&mut object, active_field, &mut active_lines);
            let normalized = heading.trim().to_ascii_lowercase().replace([' ', '-'], "_");
            active_field = CONTRACT_FIELDS
                .iter()
                .copied()
                .find(|field| *field == normalized)
                .or_else(|| match normalized.as_str() {
                    "conclusion" | "result" | "摘要" | "总结" => Some("summary"),
                    "finding" | "observations" | "发现" => Some("findings"),
                    "proof" | "证据" => Some("evidence"),
                    "open_questions" | "gaps" | "未解决" => Some("unresolved"),
                    "risk" | "风险" => Some("risks"),
                    _ => None,
                });
        } else if let Some((label, value)) = trimmed.split_once(':') {
            let normalized = label
                .trim()
                .trim_start_matches(['-', '*'])
                .trim()
                .to_ascii_lowercase()
                .replace([' ', '-'], "_");
            let field = CONTRACT_FIELDS
                .iter()
                .copied()
                .find(|field| *field == normalized)
                .or_else(|| match normalized.as_str() {
                    "conclusion" | "result" | "摘要" | "总结" => Some("summary"),
                    "finding" | "observations" | "发现" => Some("findings"),
                    "proof" | "证据" => Some("evidence"),
                    "open_questions" | "gaps" | "未解决" => Some("unresolved"),
                    "risk" | "风险" => Some("risks"),
                    _ => None,
                });
            if let Some(field) = field {
                flush(&mut object, active_field, &mut active_lines);
                active_field = Some(field);
                if !value.trim().is_empty() {
                    active_lines.push(value.trim());
                }
            } else if active_field.is_some() {
                active_lines.push(line);
            }
        } else if active_field.is_some() {
            active_lines.push(line);
        }
    }
    flush(&mut object, active_field, &mut active_lines);
    (!object.is_empty()).then_some(object)
}

fn normalize_verified_narrative_terminal(
    packet: &AgentTaskPacket,
    tool_executor: &ScopedRuntimeToolExecutor,
    summary: &mut crate::TurnSummary,
) {
    if summary.terminal_completion != harness_contract::goal::GoalCompletion::Satisfied {
        return;
    }
    let has_typed_receipt = tool_executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .any(|receipt| !receipt.observed_evidence.is_empty());
    let has_upstream_evidence = packet
        .evidence_refs
        .iter()
        .any(crate::agent_result_validator::is_materialized_durable_evidence);
    if !has_typed_receipt && !has_upstream_evidence {
        return;
    }
    let mut required_fields = packet_acceptance_contract(packet)
        .iter()
        .filter_map(narrative_field_for_requirement)
        .collect::<Vec<_>>();
    required_fields.sort_by_key(|field| field.as_str());
    required_fields.dedup();
    if required_fields.is_empty()
        || !required_fields
            .iter()
            .all(|field| narrative_field_can_be_normalized(*field))
    {
        return;
    }

    let Some(normalized) =
        normalized_narrative_terminal_body(&summary.final_answer, &required_fields)
    else {
        return;
    };
    summary.final_answer = normalized;
}

/// Maps only presentation-bearing checks to the field which accompanies the
/// Runtime-owned fact.  The fact itself is still checked independently below:
/// write/change checks require receipts, source verification requires the
/// pre/post read chain, and reviews require durable upstream evidence.
fn narrative_field_for_requirement(
    requirement: &harness_contract::team::TeamAcceptanceRequirement,
) -> Option<harness_contract::team::TeamStructuredOutputField> {
    match &requirement.check {
        harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
        | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
            Some(*field)
        }
        harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => {
            Some(harness_contract::team::TeamStructuredOutputField::SourceVerification)
        }
        harness_contract::team::TeamAcceptanceCheck::UpstreamReview => {
            Some(harness_contract::team::TeamStructuredOutputField::Review)
        }
        harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
        | harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence
        | harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound { .. } => None,
    }
}

/// A terminal answer may carry these presentation fields as natural language
/// after Runtime has independently verified the corresponding facts.  The
/// remaining fields represent a deliberate risk/unknown/legacy declaration;
/// silently manufacturing one from generic prose would hide information and
/// remains forbidden.
const fn narrative_field_can_be_normalized(
    field: harness_contract::team::TeamStructuredOutputField,
) -> bool {
    matches!(
        field,
        harness_contract::team::TeamStructuredOutputField::Findings
            | harness_contract::team::TeamStructuredOutputField::Summary
            | harness_contract::team::TeamStructuredOutputField::Plan
            | harness_contract::team::TeamStructuredOutputField::Implementation
            | harness_contract::team::TeamStructuredOutputField::SourceVerification
            | harness_contract::team::TeamStructuredOutputField::Review
            | harness_contract::team::TeamStructuredOutputField::Proposal
            | harness_contract::team::TeamStructuredOutputField::Critique
            | harness_contract::team::TeamStructuredOutputField::Mitigation
            | harness_contract::team::TeamStructuredOutputField::Checkpoint
    )
}

fn normalized_narrative_terminal_body(
    candidate: &str,
    fields: &[harness_contract::team::TeamStructuredOutputField],
) -> Option<String> {
    if fields.is_empty()
        || !fields
            .iter()
            .all(|field| narrative_field_can_be_normalized(*field))
    {
        return None;
    }
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut output = structured_agent_output(body).unwrap_or_default();
    for field in fields {
        if structured_field_materialized(*field, output.get(field.as_str())) {
            continue;
        }
        let value = match field {
            harness_contract::team::TeamStructuredOutputField::Findings => output
                .get("summary")
                .filter(|value| materialized_json_value(value))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            harness_contract::team::TeamStructuredOutputField::Summary => output
                .get("findings")
                .filter(|value| materialized_json_value(value))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            // These are presentation carriers, never independently trusted
            // acceptance facts.  Copying the Agent's own terminal wording is
            // safe only because callers have already established the
            // corresponding receipt/upstream evidence chain.
            harness_contract::team::TeamStructuredOutputField::Plan
            | harness_contract::team::TeamStructuredOutputField::Implementation
            | harness_contract::team::TeamStructuredOutputField::SourceVerification
            | harness_contract::team::TeamStructuredOutputField::Review
            | harness_contract::team::TeamStructuredOutputField::Proposal
            | harness_contract::team::TeamStructuredOutputField::Critique
            | harness_contract::team::TeamStructuredOutputField::Mitigation
            | harness_contract::team::TeamStructuredOutputField::Checkpoint => {
                serde_json::Value::String(body.to_string())
            }
            harness_contract::team::TeamStructuredOutputField::Risks
            | harness_contract::team::TeamStructuredOutputField::Unresolved
            | harness_contract::team::TeamStructuredOutputField::KeyDecisions
            | harness_contract::team::TeamStructuredOutputField::UnresolvedOrRisks => {
                return None;
            }
        };
        output.insert(field.as_str().to_string(), value);
    }
    serde_json::to_string(&output).ok()
}

#[cfg(test)]
mod structured_output_probe {
    use super::*;

    #[test]
    fn arbiter_terminal_text_extracts_key_decisions() {
        let text = "Write and read-back verification complete: `cross-team-decision-report.html` confirmed on disk (215 lines, sha256 d6340e87…), covering summary / evidence / key_decisions (K1-K8) / unresolved_or_risks (U1-U7, R1-R10) with all six roles' evidence citations and arbitration reasons. Terminal synthesis follows.\n\n{\"summary\":\"convergence_arbiter 终态收敛完成\",\"evidence\":[\"tool://tool-raw-call_00_GPhgxF1uJefA7wiTBDTR0830-2b7d0e1f4574cf50（write_file 成功）\"],\"key_decisions\":[{\"id\":\"K1\",\"decision\":\"保持自研确定性 Rust 内核\"}],\"unresolved_or_risks\":[{\"id\":\"U1\",\"item\":\"无真实数据集\"}]}";
        let parsed = structured_agent_output(text);
        assert!(
            parsed.is_some(),
            "contract JSON must be extracted from prose+JSON terminal"
        );
        let object = parsed.expect("parsed");
        assert!(object.contains_key("summary"));
        assert!(object.contains_key("evidence"));
        assert!(object.contains_key("key_decisions"));
        assert!(object.contains_key("unresolved_or_risks"));
        assert!(materialized_json_value(
            object.get("key_decisions").expect("kd")
        ));
        assert!(materialized_json_value(
            object.get("unresolved_or_risks").expect("ur")
        ));
    }

    #[test]
    fn real_arbiter_terminal_extracts_all_contract_fields() {
        let Ok(text) = std::fs::read_to_string("/tmp/arbiter_final.txt") else {
            return;
        };
        let parsed = structured_agent_output(&text);
        assert!(
            parsed.is_some(),
            "real arbiter terminal must yield a contract object"
        );
        let object = parsed.expect("parsed");
        for field in [
            "summary",
            "evidence",
            "key_decisions",
            "unresolved_or_risks",
        ] {
            assert!(
                object.contains_key(field),
                "missing {field}; keys={:?}",
                object.keys().collect::<Vec<_>>()
            );
        }
    }
}

fn normalized_scope(value: &str) -> &str {
    let value = value.trim();
    let value = ["read:", "write:", "workspace:"]
        .into_iter()
        .find_map(|prefix| value.strip_prefix(prefix))
        .unwrap_or(value);
    value.trim_start_matches("./").trim_end_matches('/')
}

fn path_within_scope(path: &str, scope: &str) -> bool {
    let path = normalized_scope(path);
    let scope = normalized_scope(scope);
    !path.is_empty()
        && !scope.is_empty()
        && (scope == "."
            || path == scope
            || path
                .strip_prefix(scope)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use harness_contract::agent::AgentCommand;
    use harness_contract::turn::TurnId;
    use sha2::{Digest, Sha256};

    fn test_authorization_lease(
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        ceiling: PermissionMode,
        idempotency_key: &str,
    ) -> harness_contract::policy::AuthorizationLease {
        harness_contract::policy::AuthorizationLease {
            lease_id: format!("test-lease:{idempotency_key}"),
            principal_id: "test-agent".to_string(),
            parent_lease_id: None,
            capability: descriptor.tool_id.clone(),
            scopes: descriptor.scopes.clone(),
            ceiling,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: idempotency_key.to_string(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "test-signature".to_string(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        }
    }

    fn test_capability_assessment(
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        required_mode: PermissionMode,
    ) -> harness_contract::policy::CapabilityAssessment {
        let effective =
            crate::AuthorizationNegotiator::compile_effective_descriptor(descriptor, "{}");
        harness_contract::policy::CapabilityAssessment {
            assessment_id: "test-assessment".to_string(),
            capability: descriptor.tool_id.clone(),
            effect: effective.descriptor.assessment,
            requested_scopes: effective.descriptor.scopes,
            required_mode,
            active_ceiling: PermissionMode::DangerFullAccess,
            parent_ceiling: PermissionMode::DangerFullAccess,
            risk: harness_contract::policy::RiskLevel::Low,
            path: harness_contract::policy::AuthorizationPath::PolicyAutoGrant,
            lease: None,
            gap: None,
            evidence_refs: Vec::new(),
            assessed_at_ms: 0,
        }
    }

    fn scoped_receipt(
        sequence: u64,
        effect_kind: harness_contract::tool::ToolEffectKind,
        path: &str,
        before: Option<&str>,
        after: Option<&str>,
    ) -> ScopedToolExecutionReceipt {
        ScopedToolExecutionReceipt {
            sequence,
            effect_kind,
            resource_scopes: vec![format!(
                "{}:{path}",
                if effect_kind == harness_contract::tool::ToolEffectKind::Write {
                    "write"
                } else {
                    "read"
                }
            )],
            paths: vec![path.to_string()],
            prior_states: before
                .map(|sha256| {
                    BTreeMap::from([(
                        path.to_string(),
                        harness_contract::context::WorkspacePriorState::Existing {
                            sha256: sha256.to_string(),
                        },
                    )])
                })
                .unwrap_or_else(|| {
                    BTreeMap::from([(
                        path.to_string(),
                        harness_contract::context::WorkspacePriorState::Absent,
                    )])
                }),
            after_digests: BTreeMap::from([(path.to_string(), after.map(str::to_string))]),
            observed_evidence: Vec::new(),
        }
    }

    fn test_agent_packet(
        evidence_refs: Vec<harness_contract::context::EvidenceAccessRef>,
    ) -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent",
                "run",
                "task",
                "session",
                "mission",
                Some("team"),
                "graph",
                "node",
            ),
            attempt: 1,
            expected_graph_revision: 0,
            policy_revision: 1,
            objective: "review".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            team_role_identity: None,
            team_role: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs,
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "agent",
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
        }
    }

    #[test]
    fn change_and_source_verification_require_digest_delta_and_post_write_read() {
        let unchanged = vec![scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Write,
            "src/lib.rs",
            Some("same"),
            Some("same"),
        )];
        assert!(materialized_change_receipts(&unchanged).is_empty());

        let read_before_write = vec![
            scoped_receipt(
                1,
                harness_contract::tool::ToolEffectKind::Read,
                "src/lib.rs",
                Some("before"),
                Some("before"),
            ),
            scoped_receipt(
                2,
                harness_contract::tool::ToolEffectKind::Write,
                "src/lib.rs",
                Some("before"),
                Some("after"),
            ),
        ];
        let change = materialized_change_receipts(&read_before_write)
            .pop()
            .expect("real digest change");
        assert!(has_matching_pre_write_evidence(&change, &read_before_write));
        assert!(!has_matching_read_receipt(
            &change,
            &read_before_write,
            true
        ));

        let write_then_read = vec![
            scoped_receipt(
                1,
                harness_contract::tool::ToolEffectKind::Write,
                "src/lib.rs",
                Some("before"),
                Some("after"),
            ),
            scoped_receipt(
                2,
                harness_contract::tool::ToolEffectKind::Read,
                "src/lib.rs",
                Some("after"),
                Some("after"),
            ),
        ];
        let ungrounded = materialized_change_receipts(&write_then_read)
            .pop()
            .expect("digest changed");
        assert!(!has_matching_pre_write_evidence(
            &ungrounded,
            &write_then_read
        ));
        assert!(has_matching_read_receipt(
            &ungrounded,
            &write_then_read,
            true
        ));

        let mut verified = read_before_write;
        verified.push(scoped_receipt(
            3,
            harness_contract::tool::ToolEffectKind::Read,
            "src/lib.rs",
            Some("after"),
            Some("after"),
        ));
        assert!(has_matching_pre_write_evidence(&change, &verified));
        assert!(has_matching_read_receipt(&change, &verified, true));
    }

    #[test]
    fn new_file_source_verification_uses_runtime_absence_proof_and_post_write_read() {
        let write_then_read = vec![
            scoped_receipt(
                1,
                harness_contract::tool::ToolEffectKind::Write,
                "evidence/report.html",
                None,
                Some("created"),
            ),
            scoped_receipt(
                2,
                harness_contract::tool::ToolEffectKind::Read,
                "evidence/report.html",
                Some("created"),
                Some("created"),
            ),
        ];
        let change = materialized_change_receipts(&write_then_read)
            .pop()
            .expect("new file is a materialized change");
        assert!(has_matching_pre_write_evidence(&change, &write_then_read));
        assert!(has_matching_read_receipt(&change, &write_then_read, true));

        let mut missing_absence_proof = write_then_read.clone();
        missing_absence_proof[0]
            .prior_states
            .remove("evidence/report.html");
        assert!(!has_matching_pre_write_evidence(
            &change,
            &missing_absence_proof
        ));

        assert!(!has_matching_read_receipt(
            &change,
            &write_then_read[..1],
            true
        ));
    }

    #[test]
    fn upstream_review_matches_normalized_receipt_path_and_its_digest_key() {
        let change = harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/auto-strategy-write/target.txt".to_string(),
            before_sha256: Some("before".to_string()),
            after_sha256: "after".to_string(),
            write_sequence: 3,
        };
        let receipt = ScopedToolExecutionReceipt {
            sequence: 1,
            effect_kind: harness_contract::tool::ToolEffectKind::Read,
            resource_scopes: vec!["read:./fixtures/auto-strategy-write/target.txt".to_string()],
            paths: vec!["./fixtures/auto-strategy-write/target.txt".to_string()],
            prior_states: BTreeMap::from([(
                "./fixtures/auto-strategy-write/target.txt".to_string(),
                harness_contract::context::WorkspacePriorState::Existing {
                    sha256: "after".to_string(),
                },
            )]),
            after_digests: BTreeMap::from([(
                "./fixtures/auto-strategy-write/target.txt".to_string(),
                Some("after".to_string()),
            )]),
            observed_evidence: Vec::new(),
        };

        assert!(has_matching_read_receipt(&change, &[receipt], false));
    }

    #[test]
    fn fresh_tool_receipt_is_evidence_even_when_content_ref_matches_upstream() {
        let upstream = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("tool", "same-content"),
            "sha256:same",
            1,
            "text/plain",
            "artifact://art_worker_upstream",
            "session:session",
        );
        let packet = test_agent_packet(vec![upstream.clone()]);
        assert!(!produced_runtime_evidence(
            &packet,
            &[upstream.clone()],
            &[]
        ));
        assert!(produced_runtime_evidence(
            &packet,
            &[upstream],
            &[scoped_receipt(
                1,
                harness_contract::tool::ToolEffectKind::Read,
                "fixtures/target.txt",
                Some("same"),
                Some("same"),
            )],
        ));
    }

    #[test]
    fn network_receipts_satisfy_only_the_network_evidence_lease() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let required = resolver.compile_obligation_or_unresolved("network:*");
        let observed = resolver
            .observe_tool_scope("web_search", "network:*", None, 1)
            .expect("network receipt");
        assert!(crate::path_identity::observed_evidence_satisfies(
            &required, &observed
        ));
    }

    #[test]
    fn unqualified_team_scope_matches_typed_runtime_receipts() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("scope");
        std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked").expect("file");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("resolver");
        let required = resolver.compile_obligation_or_unresolved("read:crates/runtime");
        let observed = resolver
            .observe_tool_scope(
                "read_file",
                "read:crates/runtime/src/lib.rs",
                Some("sha256:checked"),
                1,
            )
            .expect("receipt");
        assert!(crate::path_identity::observed_evidence_satisfies(
            &required, &observed
        ));
    }

    #[test]
    fn upstream_change_receipt_is_recovered_from_durable_evidence_binding() {
        let change = harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/target.txt".to_string(),
            before_sha256: Some("before".to_string()),
            after_sha256: "after".to_string(),
            write_sequence: 3,
        };
        let encoded = serde_json::to_string(&change).expect("change receipt JSON");
        let evidence = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
            "sha256:change",
            1,
            "application/json",
            "artifact://art_worker_change",
            "session:session",
        );
        let packet = test_agent_packet(vec![evidence]);

        assert_eq!(packet_upstream_change_receipts(&packet), vec![change]);
    }

    #[test]
    fn explicit_empty_risk_list_is_a_materialized_review_result() {
        use harness_contract::team::TeamStructuredOutputField;

        assert!(structured_field_materialized(
            TeamStructuredOutputField::Risks,
            Some(&serde_json::json!([])),
        ));
        assert!(!structured_field_materialized(
            TeamStructuredOutputField::Risks,
            None,
        ));
        assert!(!structured_field_materialized(
            TeamStructuredOutputField::Review,
            Some(&serde_json::json!([])),
        ));
    }

    struct NoopRuntimeExecutionHost;

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for NoopRuntimeExecutionHost {
        async fn execute_runtime_tool(
            &self,
            _request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            panic!("the capability advertisement test must not execute a tool")
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor_for_input(tool_name, input)
        }
    }

    struct EchoRuntimeExecutionHost;

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for EchoRuntimeExecutionHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            if request.authorization.is_none() {
                return crate::RuntimeToolExecutionOutcome {
                    tool_use_id: request.tool_use_id.clone(),
                    tool_name: request.tool_name.clone(),
                    status: crate::RuntimeToolExecutionStatus::BlockedPermission,
                    category: request.category,
                    output: None,
                    error: Some("missing propagated authorization".to_string()),
                    evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                    observed_evidence: Vec::new(),
                };
            }
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(format!("authorized:{}", request.tool_name)),
                error: None,
                evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                observed_evidence: Vec::new(),
            }
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor_for_input(tool_name, input)
        }
    }

    struct ConcurrencyTrackingRuntimeExecutionHost {
        active: std::sync::atomic::AtomicUsize,
        max_active: std::sync::atomic::AtomicUsize,
    }

    impl ConcurrencyTrackingRuntimeExecutionHost {
        fn new() -> Self {
            Self {
                active: std::sync::atomic::AtomicUsize::new(0),
                max_active: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn reset(&self) {
            self.active.store(0, Ordering::SeqCst);
            self.max_active.store(0, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for ConcurrencyTrackingRuntimeExecutionHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some("{}".to_string()),
                error: None,
                evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                observed_evidence: Vec::new(),
            }
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor_for_input(tool_name, input)
        }
    }

    fn concurrency_test_executor(
        root: &std::path::Path,
        host: Arc<ConcurrencyTrackingRuntimeExecutionHost>,
        scope_locks: Arc<ScopeLockManager>,
    ) -> ScopedRuntimeToolExecutor {
        ScopedRuntimeToolExecutor {
            host,
            allowed_tools: BTreeSet::from(["write_file".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: root.to_path_buf(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root)
                    .expect("path identities"),
            ),
            scope_locks,
            commit_service: None,
            resource_scopes: None,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        }
    }

    #[tokio::test]
    async fn delegated_leaf_effects_serialize_conflicts_and_parallelize_unrelated_paths() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("fixtures/sub")).expect("fixture directories");
        let host = Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new());
        let locks = Arc::new(ScopeLockManager::new());
        let first = Arc::new(concurrency_test_executor(
            root.path(),
            Arc::clone(&host),
            Arc::clone(&locks),
        ));
        let second = Arc::new(concurrency_test_executor(
            root.path(),
            Arc::clone(&host),
            Arc::clone(&locks),
        ));

        let same = tokio::join!(
            first.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/sub/target.txt","content":"one"}"#,
                None,
            ),
            second.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/sub/target.txt","content":"two"}"#,
                None,
            )
        );
        same.0.expect("first same-path effect");
        same.1.expect("second same-path effect");
        assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

        host.reset();
        let parent_child = tokio::join!(
            first.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/sub","content":"parent"}"#,
                None,
            ),
            second.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/sub/target.txt","content":"child"}"#,
                None,
            )
        );
        parent_child.0.expect("parent-path effect");
        parent_child.1.expect("child-path effect");
        assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

        host.reset();
        let unrelated = tokio::join!(
            first.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/left.txt","content":"left"}"#,
                None,
            ),
            second.execute_scoped(
                "write_file",
                r#"{"path":"fixtures/right.txt","content":"right"}"#,
                None,
            )
        );
        unrelated.0.expect("left effect");
        unrelated.1.expect("right effect");
        assert_eq!(host.max_active.load(Ordering::SeqCst), 2);
    }

    struct InputSensitiveRuntimeExecutionHost;

    impl InputSensitiveRuntimeExecutionHost {
        fn descriptor(
            tool_name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            let mut descriptor = test_tool_descriptor_for_input(tool_name, input)?;
            let encoded = serde_json::to_vec(input).ok()?;
            descriptor.descriptor_hash = format!("input:{:x}", Sha256::digest(encoded));
            Some(descriptor)
        }
    }

    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for InputSensitiveRuntimeExecutionHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            let parsed = serde_json::from_str::<serde_json::Value>(&request.input).ok();
            let current_hash = parsed.as_ref().and_then(|input| {
                Self::descriptor(&request.tool_name, input)
                    .map(|descriptor| descriptor.descriptor_hash)
            });
            let authorized_hash = request
                .authorization
                .as_ref()
                .map(|authorization| authorization.descriptor_hash.as_str());
            let authorized = current_hash.as_deref() == authorized_hash;
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: if authorized {
                    crate::RuntimeToolExecutionStatus::Executed
                } else {
                    crate::RuntimeToolExecutionStatus::BlockedPermission
                },
                category: request.category,
                output: authorized.then(|| format!("authorized:{}", request.tool_name)),
                error: (!authorized).then(|| "tool authorization is stale".to_string()),
                evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                observed_evidence: Vec::new(),
            }
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Self::descriptor(tool_name, input)
        }
    }

    fn test_tool_descriptor_for_input(
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        let (effect_kind, operation, required_permission) = match tool_name {
            "read_file" | "grep_search" | "glob_search" => (
                ToolEffectKind::Read,
                PermissionOperation::Read,
                ToolPermissionMode::ReadOnly,
            ),
            "checkpoint_create" | "write_file" => (
                ToolEffectKind::Write,
                PermissionOperation::Write,
                ToolPermissionMode::WorkspaceWrite,
            ),
            _ => return None,
        };
        Some(ToolEffectDescriptor {
            tool_id: tool_name.to_string(),
            descriptor_hash: format!("test-host:{tool_name}"),
            effect_kind,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope {
                resource: PermissionResource::File,
                operation,
                target: input
                    .get("path")
                    .or_else(|| input.get("file_path"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string),
            }],
            required_permission,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: harness_contract::policy::EffectAssessment::default(),
        })
    }

    #[test]
    fn read_only_ceiling_never_escalates_for_a_write_tool() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy(None, PermissionMode::ReadOnly, &tools);
        assert_eq!(policy.active_mode(), PermissionMode::ReadOnly);
        assert_eq!(
            policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
    }

    #[test]
    fn workspace_change_contract_retains_the_required_write_scope() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("fixtures")).expect("fixture directory");
        std::fs::write(root.path().join("fixtures/target.txt"), "target").expect("fixture file");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let mut packet = test_agent_packet(Vec::new());
        packet.required_acceptance = resolver.compile_required_acceptance(
            &[
                "implementation".to_string(),
                "source_verification".to_string(),
            ],
            &[
                "write:fixtures/target.txt".to_string(),
                "verify_after_write:fixtures/target.txt".to_string(),
            ],
        );

        assert_eq!(
            packet_focus_acceptance_scopes(&packet),
            [
                "verify_after_write:fixtures/target.txt",
                "write:fixtures/target.txt"
            ]
        );
        packet.required_acceptance = resolver.compile_required_acceptance(
            &["review".to_string()],
            &["verify_upstream_change:fixtures/target.txt".to_string()],
        );
        assert_eq!(
            packet_focus_acceptance_scopes(&packet),
            ["verify_upstream_change:fixtures/target.txt"]
        );
    }

    #[test]
    fn evidence_scope_contract_projects_a_typed_read_resource_scope() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("fixture directory");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let mut packet = test_agent_packet(Vec::new());
        packet.required_acceptance = resolver.compile_required_acceptance(
            &["evidence_scope:crates/runtime".to_string()],
            &["read:crates/runtime".to_string()],
        );

        assert_eq!(
            packet_focus_acceptance_scopes(&packet),
            ["read:crates/runtime"]
        );
    }

    #[test]
    fn network_evidence_scope_preserves_its_resource_kind() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let mut packet = test_agent_packet(Vec::new());
        packet.required_acceptance = resolver.compile_required_acceptance(
            &["evidence_scope:network:*".to_string()],
            &["network:*".to_string()],
        );

        assert_eq!(packet_focus_acceptance_scopes(&packet), ["network:*"]);
    }

    #[test]
    fn acceptance_contract_projects_materialized_output_fields_to_the_host() {
        let mut packet = test_agent_packet(Vec::new());
        packet.output_acceptance = vec![
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "evidence".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec!["read:fixtures/target.txt".to_string()],
                },
            },
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "review".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::UpstreamReview,
            },
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "risks".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::StructuredField {
                    field: harness_contract::team::TeamStructuredOutputField::Risks,
                },
            },
            harness_contract::team::TeamAcceptanceRequirement {
                criterion: "legacy".to_string(),
                check: harness_contract::team::TeamAcceptanceCheck::LegacyEvidenceBound {
                    scopes: vec!["read:fixtures/target.txt".to_string()],
                },
            },
        ];

        assert_eq!(
            packet_required_output_fields(&packet),
            [
                "legacy_acceptance".to_string(),
                "review".to_string(),
                "risks".to_string()
            ]
        );
    }

    #[test]
    fn workspace_internal_absolute_resource_path_is_normalized_once() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let target = root.path().join("fixtures/target.txt");
        let input = serde_json::json!({
            "path": target,
            "content": format!("do not rewrite {}", root.path().display()),
        })
        .to_string();

        let normalized =
            normalize_delegated_resource_paths("write_file", &input, root.path(), &resolver, None)
                .expect("normalize internal absolute path");
        let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
        assert_eq!(normalized["path"], "fixtures/target.txt");
        assert!(normalized["content"]
            .as_str()
            .is_some_and(|content| content.contains(&root.path().display().to_string())));
    }

    #[test]
    fn sole_directory_scope_normalizes_a_bare_delegated_read_path() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("external-app")).expect("project directory");
        std::fs::write(
            root.path().join("external-app/Cargo.toml"),
            "[package]\nname='external-app'\n",
        )
        .expect("fixture");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let input = serde_json::json!({"path": "Cargo.toml"}).to_string();
        assert_eq!(
            resolver
                .resolve_existing("external-app")
                .expect("scope directory")
                .object_kind,
            harness_contract::context::WorkspaceObjectKind::Directory
        );
        let candidate = resolver
            .resolve_existing("external-app/Cargo.toml")
            .expect("candidate");
        let scope = resolver.resolve_existing("external-app").expect("scope");
        assert!(
            resource_path_is_authorized(
                &resolver,
                "external-app/Cargo.toml",
                &["read:external-app".to_string()],
                false,
            ),
            "candidate={candidate:?}; scope={scope:?}"
        );
        let direct = normalize_single_scope_relative_read_value(
            "read_file",
            serde_json::json!({"path": "Cargo.toml"}),
            &resolver,
            Some(&["read:external-app".to_string()]),
        );
        assert_eq!(direct["path"], "external-app/Cargo.toml");

        let normalized = normalize_delegated_resource_paths(
            "read_file",
            &input,
            root.path(),
            &resolver,
            Some(&["read:external-app".to_string()]),
        )
        .expect("normalize sole scoped read");
        let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
        assert_eq!(normalized["path"], "external-app/Cargo.toml");
    }

    #[test]
    fn ambiguous_or_existing_bare_read_path_is_never_retargeted() {
        let root = tempfile::tempdir().expect("workspace");
        for project in ["one", "two"] {
            std::fs::create_dir_all(root.path().join(project)).expect("project directory");
            std::fs::write(root.path().join(project).join("Cargo.toml"), project).expect("fixture");
        }
        std::fs::write(root.path().join("Cargo.toml"), "root").expect("root fixture");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let input = serde_json::json!({"path": "Cargo.toml"}).to_string();

        let normalized = normalize_delegated_resource_paths(
            "read_file",
            &input,
            root.path(),
            &resolver,
            Some(&["read:one".to_string(), "read:two".to_string()]),
        )
        .expect("preserve ambiguous path");
        let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
        assert_eq!(normalized["path"], "Cargo.toml");
    }

    #[test]
    fn whole_workspace_lease_bounds_to_workspace_but_never_escapes() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
        let root_input = serde_json::json!({
            "pattern": "**/*.rs",
            "path": root.path(),
        })
        .to_string();
        let normalized = normalize_delegated_resource_paths(
            "glob_search",
            &root_input,
            root.path(),
            &resolver,
            Some(&["write:.".to_string()]),
        )
        .expect("normalize workspace root");
        let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
        assert_eq!(normalized["path"], ".");
        // `write:.` is a whole-workspace lease issued only to full-trust
        // Teams; it authorizes any path inside the workspace.
        assert!(resource_path_is_authorized(
            &resolver,
            "evidence/new-report.html",
            &["write:.".to_string()],
            true,
        ));
        // Traversal outside the workspace is never authorized, even under a
        // whole-workspace lease.
        assert!(!resource_path_is_authorized(
            &resolver,
            "../outside.html",
            &["write:.".to_string()],
            true,
        ));
    }

    #[test]
    fn exact_new_artifact_scope_remains_narrow_and_writable() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
        assert!(resource_path_is_authorized(
            &resolver,
            "evidence/report.html",
            &["write:evidence/report.html".to_string()],
            true,
        ));
        assert!(!resource_path_is_authorized(
            &resolver,
            "evidence/other.html",
            &["write:evidence/report.html".to_string()],
            true,
        ));
    }

    #[test]
    fn absolute_escape_and_parent_traversal_remain_unauthorized() {
        let root = tempfile::tempdir().expect("workspace");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities");
        let allowed = root.path().join("fixtures/target.txt");
        std::fs::create_dir_all(allowed.parent().expect("parent")).expect("scope directory");
        std::fs::write(&allowed, "before").expect("scope file");
        let outside = root.path().parent().expect("parent").join("outside.txt");
        let outside_input = serde_json::json!({"path": outside}).to_string();
        assert_eq!(
            normalize_delegated_resource_paths(
                "read_file",
                &outside_input,
                root.path(),
                &resolver,
                None,
            )
            .expect("unchanged outside input"),
            outside_input
        );
        assert!(!resource_path_is_authorized(
            &resolver,
            outside.to_string_lossy().as_ref(),
            &["read:fixtures/target.txt".into()],
            false,
        ));
        assert!(!resource_path_is_authorized(
            &resolver,
            "fixtures/../outside.txt",
            &["read:fixtures/target.txt".into()],
            false,
        ));
    }

    #[test]
    fn permission_policy_uses_the_explicit_packet_ceiling() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy(None, PermissionMode::WorkspaceWrite, &tools);
        assert_eq!(policy.active_mode(), PermissionMode::WorkspaceWrite);
        assert_eq!(
            policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
    }

    #[tokio::test]
    async fn team_tool_boundary_enforces_the_exact_focus_scope() {
        let root = tempfile::tempdir().expect("scoped workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("runtime scope");
        std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked")
            .expect("runtime file");
        std::fs::create_dir_all(root.path().join("crates/gateway")).expect("gateway scope");
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from([
                "read_file".to_string(),
                "grep_search".to_string(),
                "glob_search".to_string(),
                "context_retrieve".to_string(),
            ]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: root.path().to_path_buf(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                    .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: Some(vec!["read:crates/runtime".to_string()]),
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };

        executor
            .enforce_resource_ceiling("read_file", r#"{"path":"crates/runtime/src/lib.rs"}"#)
            .expect("in-scope read");
        assert!(executor
            .enforce_resource_ceiling("read_file", r#"{"path":"crates/gateway/src/lib.rs"}"#,)
            .is_err());
        assert!(executor
            .enforce_resource_ceiling("read_file", r#"{"path":"../secret"}"#)
            .is_err());
        assert!(executor
            .enforce_resource_ceiling("grep_search", r#"{"pattern":"unsafe"}"#)
            .is_err());
        executor
            .enforce_resource_ceiling(
                "context_retrieve",
                r#"{"source":"session_history","scope":"current"}"#,
            )
            .expect("Runtime-bound context retrieval is not a filesystem escape");

        let normalized = normalize_delegated_resource_paths(
            "glob_search",
            r#"{"pattern":"crates/runtime/**/*.rs","path":"."}"#,
            root.path(),
            &executor.path_identity_resolver,
            executor.resource_scopes.as_deref(),
        )
        .expect("bounded glob normalization");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&normalized).expect("normalized input"),
            serde_json::json!({"pattern":"**/*.rs","path":"crates/runtime"})
        );
        executor
            .enforce_resource_ceiling("glob_search", &normalized)
            .expect("the narrowed glob remains inside the exact focus scope");

        let outside_glob = normalize_delegated_resource_paths(
            "glob_search",
            r#"{"pattern":"crates/gateway/**/*.rs","path":"."}"#,
            root.path(),
            &executor.path_identity_resolver,
            executor.resource_scopes.as_deref(),
        )
        .expect("outside glob stays representable");
        assert!(executor
            .enforce_resource_ceiling("glob_search", &outside_glob)
            .is_err());

        let descriptor = test_tool_descriptor_for_input(
            "read_file",
            &serde_json::json!({"path": "crates/runtime/src/lib.rs"}),
        )
        .expect("read descriptor");
        let authorization = harness_contract::tool::ToolExecutionAuthorization {
            request_id: "absolute-read".into(),
            tool_id: "read_file".into(),
            descriptor_hash: descriptor.descriptor_hash.clone(),
            policy_revision: 1,
            scope: descriptor.scopes[0].clone(),
            authorization_lease: harness_contract::policy::AuthorizationLease {
                lease_id: "permission:read_only".into(),
                principal_id: "test-agent".into(),
                parent_lease_id: None,
                capability: "read_file".into(),
                scopes: descriptor.scopes.clone(),
                ceiling: harness_contract::policy::PermissionMode::ReadOnly,
                issued_at_ms: 0,
                expires_at_ms: u64::MAX,
                max_uses: 1,
                remaining_uses: 1,
                idempotency_key: "absolute-read".into(),
                policy_revision: 1,
                effect_descriptor_hash: descriptor.descriptor_hash.clone(),
                signature: "test-signature".into(),
                status: harness_contract::policy::AuthorizationLeaseStatus::Active,
            },
            timeout_lease: "timeout:30".into(),
            idempotency_key: None,
        };
        let absolute_input = serde_json::json!({
            "path": root.path().join("crates/runtime/src/lib.rs"),
        })
        .to_string();
        executor
            .execute_authorized_output(&authorization, "read_file", &absolute_input)
            .await
            .expect("workspace-internal absolute read is normalized and executed");
        let receipts = executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].paths, ["crates/runtime/src/lib.rs"]);
    }

    #[tokio::test]
    async fn absolute_path_authorization_and_execution_share_the_normalized_descriptor() {
        let root = tempfile::tempdir().expect("scoped workspace");
        let target = root.path().join("fixtures/target.txt");
        std::fs::create_dir_all(target.parent().expect("target parent")).expect("scope directory");
        std::fs::write(&target, "checked").expect("scope file");
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(InputSensitiveRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: root.path().to_path_buf(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                    .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: Some(vec!["read:fixtures/target.txt".to_string()]),
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };
        let absolute_value = serde_json::json!({"path": target});
        let descriptor = executor
            .registered_tool_effect("read_file", &absolute_value)
            .expect("normalized effect descriptor");
        let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &descriptor,
            &absolute_value.to_string(),
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &effective,
                &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
                "absolute-agent-read",
                test_authorization_lease(
                    &descriptor,
                    PermissionMode::ReadOnly,
                    "absolute-agent-read",
                ),
                30,
            )
            .expect("normalized read authorization")
            .authorization;

        assert_eq!(
            executor
                .execute_authorized_output(&authorization, "read_file", &absolute_value.to_string(),)
                .await
                .expect("same normalized descriptor must remain current"),
            harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
        );
        let receipts = executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(receipts[0].paths, ["fixtures/target.txt"]);
    }

    #[cfg(unix)]
    #[test]
    fn team_tool_boundary_rejects_symlink_escape_for_existing_and_new_targets() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().expect("scoped workspace");
        let outside = tempfile::tempdir().expect("outside workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
        std::fs::write(outside.path().join("secret.txt"), "secret").expect("outside fixture");
        symlink(outside.path(), root.path().join("crates/runtime/escape"))
            .expect("workspace symlink");
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string(), "write_file".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: root.path().to_path_buf(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                    .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: Some(vec![
                "read:crates/runtime".to_string(),
                "write:crates/runtime".to_string(),
            ]),
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };

        assert!(executor
            .enforce_resource_ceiling(
                "read_file",
                r#"{"path":"crates/runtime/escape/secret.txt"}"#,
            )
            .is_err());
        assert!(executor
            .enforce_resource_ceiling(
                "write_file",
                r#"{"path":"crates/runtime/escape/new.txt","content":"denied"}"#,
            )
            .is_err());
    }

    #[tokio::test]
    async fn scoped_executor_advertises_only_packet_authorized_tools() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(NoopRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string(), "grep_search".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: std::path::PathBuf::from("/workspace"),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(
                    &std::env::current_dir().expect("current directory"),
                )
                .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: None,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };

        assert!(executor.has_registered_tools());
        assert_eq!(
            executor.available_tool_names(),
            vec![
                "tool_search".to_string(),
                "grep_search".to_string(),
                "read_file".to_string(),
            ]
        );
        assert!(executor.classify_tool_safety("read_file", "{}").is_some());
        assert!(executor.classify_tool_safety("write_file", "{}").is_none());
        let discovery: harness_contract::tool::ToolDiscoveryReceipt = serde_json::from_str(
            &executor
                .execute_output("tool_search", r#"{"query":"read"}"#)
                .await
                .expect("bootstrap search should return the canonical receipt")
                .model_text(),
        )
        .expect("canonical discovery receipt");
        assert_eq!(discovery.query, "read");
        assert_eq!(
            discovery.activation_candidates,
            vec!["grep_search", "read_file"]
        );
        assert!(executor.has_tool("checkpoint_create"));
        assert!(!executor
            .available_tool_names()
            .contains(&"checkpoint_create".to_string()));
        assert!(executor
            .execute_output("checkpoint_create", r#"{"label":"model"}"#)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn scoped_executor_routes_hidden_checkpoint_for_runtime_guard_only() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: std::path::PathBuf::from("/workspace"),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(
                    &std::env::current_dir().expect("current directory"),
                )
                .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: Some(vec![
                "read:README.md".to_string(),
                "write:fixtures/target.txt".to_string(),
            ]),
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };
        let descriptor = executor
            .registered_tool_effect("checkpoint_create", &serde_json::json!({"label": "guard"}))
            .expect("Runtime guard must see the hidden checkpoint descriptor");
        let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &descriptor,
            r#"{"label":"guard"}"#,
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &effective,
                &test_capability_assessment(&descriptor, PermissionMode::WorkspaceWrite),
                "agent-checkpoint-test",
                test_authorization_lease(
                    &descriptor,
                    PermissionMode::WorkspaceWrite,
                    "agent-checkpoint-test",
                ),
                30,
            )
            .expect("Runtime should authorize its internal checkpoint")
            .authorization;

        assert_eq!(
            executor
                .execute_authorized_output(
                    &authorization,
                    "checkpoint_create",
                    r#"{"label":"guard"}"#,
                )
                .await
                .expect("hidden checkpoint should reach the pinned Runtime host"),
            harness_contract::context::ToolOutputDraft::bounded_inline(
                "authorized:checkpoint_create"
            )
        );
        assert!(executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
        assert_eq!(
            executor
                .internal_checkpoint_input(serde_json::json!({
                    "label": "guard",
                    "paths": ["model-must-not-control-this"]
                }))
                .expect("bounded checkpoint input")["paths"],
            serde_json::json!(["fixtures/target.txt"])
        );
    }

    #[test]
    fn structured_agent_output_accepts_fenced_json_without_trusting_prose() {
        let exact = r#"{"implementation":"done","source_verification":"receipt"}"#;
        let fenced = format!("```json\n{exact}\n```\n\nHuman-readable evidence summary.");

        assert_eq!(
            structured_agent_output(exact),
            structured_agent_output(&fenced)
        );
        assert_eq!(
            structured_agent_output(&fenced)
                .and_then(|object| object.get("implementation").cloned()),
            Some(serde_json::Value::String("done".to_string()))
        );
        assert!(structured_agent_output("implementation completed in prose").is_none());
        assert!(structured_agent_output(r#"prefix {"unrelated":"claim"} suffix"#).is_none());
        assert!(structured_agent_output(r#"{"unrelated":"claim"}"#).is_none());
    }

    #[test]
    fn structured_agent_output_tolerates_safe_provider_shape_drift() {
        let wrapped = r#"{"output":{"Conclusion":"done","证据":["tool://read"]}}"#;
        let output = structured_agent_output(wrapped).expect("known wrapped contract");
        assert_eq!(output["summary"], "done");
        assert_eq!(output["evidence"][0], "tool://read");

        let encoded = r#"{"data":"{\"finding\":\"one finding\",\"gaps\":[]}"}"#;
        let output = structured_agent_output(encoded).expect("encoded contract object");
        assert_eq!(output["findings"], "one finding");
        assert_eq!(output["unresolved"], serde_json::json!([]));

        let localized = "### 总结\n完成读取。\n\n**风险**\n无。";
        let output = structured_agent_output(localized).expect("localized headings");
        assert_eq!(output["summary"], "完成读取。");
        assert_eq!(output["risks"], "无。");

        let trailing = "{\"findings\":\"verified\",\"risks\":[],}";
        let output = structured_agent_output(trailing).expect("trailing comma repair");
        assert_eq!(output["findings"], "verified");
        assert_eq!(output["risks"], serde_json::json!([]));

        let labeled = "Summary: verified result\nRisks: none identified";
        let output = structured_agent_output(labeled).expect("exact labeled contract");
        assert_eq!(output["summary"], "verified result");
        assert_eq!(output["risks"], "none identified");

        assert!(structured_agent_output(r#"{"output":{"unrelated":"claim"}}"#).is_none());
    }

    #[test]
    fn structured_agent_output_normalizes_only_exact_contract_headings() {
        let markdown =
            "intro\n\n## Review\nverified from fresh receipts\n\n## Risks\nNone identified.\n";
        let output = structured_agent_output(markdown).expect("heading contract");
        assert_eq!(output["review"], "verified from fresh receipts");
        assert_eq!(output["risks"], "None identified.");
        assert!(structured_agent_output("Review complete; no risks.").is_none());

        let quoted_upstream_then_terminal = concat!(
            "upstream: {\"implementation\":\"done\",\"source_verification\":\"old\"}\n",
            "terminal: {\"review\":\"verified\",\"risks\":\"none\"}"
        );
        let output = structured_agent_output(quoted_upstream_then_terminal)
            .expect("last embedded contract object");
        assert!(output.get("implementation").is_none());
        assert_eq!(output["review"], "verified");
    }

    #[test]
    fn verified_narrative_terminal_accepts_prose_without_accepting_tool_markup() {
        use harness_contract::team::TeamStructuredOutputField;

        let prose = normalized_narrative_terminal_body(
            "Cargo.toml declares the workspace package metadata.",
            &[TeamStructuredOutputField::Findings],
        )
        .expect("verified bounded prose should be a findings carrier");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&prose).expect("normalized JSON")["findings"],
            "Cargo.toml declares the workspace package metadata."
        );
        assert!(
            normalized_narrative_terminal_body(
                "Both researchers confirmed the workspace metadata.",
                &[
                    TeamStructuredOutputField::Summary,
                    TeamStructuredOutputField::Unresolved,
                ],
            )
            .is_none(),
            "Runtime must not invent an unresolved conclusion that the Agent omitted"
        );
        assert!(normalized_narrative_terminal_body(
            "<synthesized_terminal evidence_committed=1 />",
            &[TeamStructuredOutputField::Findings],
        )
        .is_none());
        assert!(normalized_narrative_terminal_body(
            "<tool_call>read_file</tool_call>",
            &[TeamStructuredOutputField::Findings],
        )
        .is_none());
        assert!(
            normalized_narrative_terminal_body("reviewed", &[TeamStructuredOutputField::Review],)
                .is_some(),
            "a verified review may use ordinary terminal prose"
        );
    }

    #[test]
    fn verified_narrative_terminal_accepts_technical_prose_but_not_risk_declarations() {
        use harness_contract::team::TeamStructuredOutputField;

        let prose = "Updated the parser and verified the changed file with a fresh read.";
        let normalized = normalized_narrative_terminal_body(
            prose,
            &[
                TeamStructuredOutputField::Implementation,
                TeamStructuredOutputField::SourceVerification,
                TeamStructuredOutputField::Review,
            ],
        )
        .expect("receipt-verified technical prose should not require JSON syntax");
        let output = serde_json::from_str::<serde_json::Value>(&normalized).expect("JSON carrier");
        assert_eq!(output["implementation"], prose);
        assert_eq!(output["source_verification"], prose);
        assert_eq!(output["review"], prose);

        assert!(
            normalized_narrative_terminal_body(prose, &[TeamStructuredOutputField::Risks],)
                .is_none(),
            "Runtime must not infer that risks were considered"
        );
        assert!(
            normalized_narrative_terminal_body(
                prose,
                &[TeamStructuredOutputField::UnresolvedOrRisks],
            )
            .is_none(),
            "Runtime must not infer unresolved work from generic prose"
        );
    }

    #[test]
    fn terminal_structured_acceptance_is_single_pass_and_never_invents_missing_fields() {
        let terminal = r#"{"implementation":"done"}"#;
        let first = structured_agent_output(terminal).expect("deterministic terminal JSON");
        let second = structured_agent_output(terminal).expect("repeat deterministic parse");

        assert_eq!(first, second);
        assert_eq!(first["implementation"], "done");
        assert!(first.get("source_verification").is_none());
        assert!(first.get("review").is_none());
        assert!(structured_agent_output("implementation completed in prose").is_none());
    }

    #[tokio::test]
    async fn scoped_executor_propagates_runtime_authorization_for_normal_agent_tools() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string()]),
            session_id: "session".to_string(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            attempt: 1,
            workspace_root: std::path::PathBuf::from("/workspace"),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(
                    &std::env::current_dir().expect("current directory"),
                )
                .expect("path identities"),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: None,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };
        let descriptor = executor
            .registered_tool_effect("read_file", &serde_json::json!({"path": "README.md"}))
            .expect("allow-listed delegated tool must describe its effect");
        let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
            &descriptor,
            r#"{"path":"README.md"}"#,
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &effective,
                &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
                "agent-test",
                test_authorization_lease(&descriptor, PermissionMode::ReadOnly, "agent-test"),
                30,
            )
            .expect("read tool should be authorized")
            .authorization;
        assert_eq!(
            executor
                .execute_authorized_output(&authorization, "read_file", r#"{"path":"README.md"}"#)
                .await
                .expect("authorized tool should execute"),
            harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
        );
        assert!(executor
            .execute_output("read_file", r#"{"path":"README.md"}"#)
            .await
            .is_err());
        assert!(executor
            .execute_authorized_output(&authorization, "write_file", r#"{"path":"README.md"}"#)
            .await
            .is_err());
    }

    #[test]
    fn durable_audits_are_promoted_to_agent_evidence_refs() {
        let packet = AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None, "agent", "run", "task", "session", "mission", None, "graph", "node",
            ),
            attempt: 1,
            expected_graph_revision: 0,
            policy_revision: 1,
            objective: "inspect".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            team_role_identity: None,
            team_role: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: vec![harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed("upstream", "frame"),
                "sha256:frame",
                1,
                "text/plain",
                "artifact://art_worker_fixture_1",
                "session:session",
            )],
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "agent",
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
        };
        let tool_access = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("tool", "tool-1"),
            "sha256:tool",
            1,
            "text/plain",
            "artifact://art_worker_fixture_2",
            "session:session",
        );
        let audits = vec![harness_contract::context::EvidenceAuditProjection {
            evidence_ref: tool_access.evidence_ref.clone(),
            content_kind: harness_contract::context::EvidenceContentKind::Text,
            raw_tokens: 1,
            receipt_tokens: 1,
            omitted_tokens: 0,
            raw_available: true,
            access: Some(tool_access),
        }];

        assert_eq!(
            agent_evidence_refs(&packet, &audits, &[])
                .into_iter()
                .map(|reference| reference.evidence_ref.id)
                .collect::<Vec<_>>(),
            vec!["tool-1".to_string(), "frame".to_string()]
        );
    }

    #[test]
    fn delegated_child_session_inherits_the_runtime_services_workspace() {
        let workspace = std::path::Path::new("/workspace/project");
        let session = delegated_child_session("parent-session", "model", workspace);

        assert_eq!(session.session_id, "parent-session");
        assert_eq!(session.model.as_deref(), Some("model"));
        assert_eq!(session.workspace_root(), Some(workspace));
    }

    #[tokio::test]
    async fn send_input_enters_the_live_child_turn_inbox() {
        let worker = InProcessAgentWorker::new(Weak::new());
        let stream = crate::SessionInputStream::new("child-session");
        stream.set_active_turn(Some(TurnId::from_string("child-turn")));
        worker.active_runs.lock().unwrap().insert(
            "run-1".into(),
            ActiveInProcessRun {
                cancellation: crate::CancellationToken::new(),
                session_id: "child-session".into(),
                input_stream: stream.clone(),
                completion: Arc::new(tokio::sync::Notify::new()),
                completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        );
        worker
            .command(
                &AgentRunHandle {
                    run_id: "run-1".into(),
                    agent_id: "agent-1".into(),
                    backend: AgentBackendKind::InProcess,
                    revision: 1,
                    status: harness_contract::agent::AgentStatus::Running,
                },
                &AgentCommandRequest {
                    command_id: "input-1".into(),
                    agent_id: "agent-1".into(),
                    expected_revision: 1,
                    command: AgentCommand::SendInput,
                    input: Some(AgentInput::UserSupplement("use the new requirement".into())),
                },
            )
            .await
            .expect("input accepted");
        let inbox = stream.inbox_snapshot(Some(TurnId::from_string("child-turn")));
        assert_eq!(inbox.items.len(), 1);
        assert_eq!(inbox.items[0].content_preview, "use the new requirement");
        assert!(worker.capabilities().supports_input);
        assert!(!worker.capabilities().supports_pause);
    }

    #[tokio::test]
    async fn cancel_waits_for_cleanup_and_completed_tombstone_is_race_safe() {
        let worker = Arc::new(InProcessAgentWorker::new(Weak::new()));
        let cancellation = crate::CancellationToken::new();
        let completion = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        worker.active_runs.lock().unwrap().insert(
            "run-cancel".into(),
            ActiveInProcessRun {
                cancellation: cancellation.clone(),
                session_id: "child-session".into(),
                input_stream: crate::SessionInputStream::new("child-session"),
                completion: Arc::clone(&completion),
                completed: Arc::clone(&completed),
            },
        );
        let cleanup = ActiveRunCleanup {
            worker: worker.as_ref(),
            run_id: "run-cancel".into(),
            completion,
            completed,
        };
        let handle = AgentRunHandle {
            run_id: "run-cancel".into(),
            agent_id: "agent-cancel".into(),
            backend: AgentBackendKind::InProcess,
            revision: 1,
            status: harness_contract::agent::AgentStatus::Running,
        };
        let request = AgentCommandRequest {
            command_id: "cancel-1".into(),
            agent_id: "agent-cancel".into(),
            expected_revision: 1,
            command: AgentCommand::Cancel,
            input: None,
        };
        let cancel = {
            let worker = Arc::clone(&worker);
            let handle = handle.clone();
            let request = request.clone();
            tokio::spawn(async move { worker.command(&handle, &request).await })
        };
        cancellation.cancelled().await;
        drop(cleanup);
        tokio::time::timeout(std::time::Duration::from_secs(1), cancel)
            .await
            .expect("cancel returns after cleanup")
            .expect("cancel task joins")
            .expect("cancel is accepted");
        assert!(worker.active_runs.lock().unwrap().is_empty());
        assert!(worker.pending_cancellations.lock().unwrap().is_empty());
        assert!(worker.run_completed("run-cancel"));

        // A command arriving in the just-completed/no-active window must use
        // the bounded tombstone instead of waiting ten seconds as if the run
        // had not registered yet.
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            worker.command(&handle, &request),
        )
        .await
        .expect("completed tombstone resolves cancellation")
        .expect("completed cancellation is idempotent");
        assert!(worker.pending_cancellations.lock().unwrap().is_empty());

        // Dropping a command future while it is waiting for an active run (or
        // immediately after observing a completion tombstone) must also
        // release its pending entry; no worker cleanup may still be available
        // to do that on its behalf.
        let aborted_token = crate::CancellationToken::new();
        let aborted_completion = Arc::new(tokio::sync::Notify::new());
        let aborted_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        worker.active_runs.lock().unwrap().insert(
            "run-aborted-command".into(),
            ActiveInProcessRun {
                cancellation: aborted_token.clone(),
                session_id: "child-session".into(),
                input_stream: crate::SessionInputStream::new("child-session"),
                completion: Arc::clone(&aborted_completion),
                completed: Arc::clone(&aborted_completed),
            },
        );
        let aborted_cleanup = ActiveRunCleanup {
            worker: worker.as_ref(),
            run_id: "run-aborted-command".into(),
            completion: aborted_completion,
            completed: aborted_completed,
        };
        let aborted_handle = AgentRunHandle {
            run_id: "run-aborted-command".into(),
            agent_id: "agent-aborted-command".into(),
            ..handle.clone()
        };
        let aborted = {
            let worker = Arc::clone(&worker);
            let request = request.clone();
            tokio::spawn(async move { worker.command(&aborted_handle, &request).await })
        };
        aborted_token.cancelled().await;
        aborted.abort();
        let _ = aborted.await;
        assert!(
            worker.pending_cancellations.lock().unwrap().is_empty(),
            "PendingCancellationOwner must clean a dropped command future"
        );
        drop(aborted_cleanup);

        worker.record_completed_run("run-completed-abort-window");
        worker
            .pending_cancellations
            .lock()
            .unwrap()
            .insert("run-completed-abort-window".into());
        let completed_window_owner = PendingCancellationOwner {
            pending: &worker.pending_cancellations,
            run_id: "run-completed-abort-window".into(),
        };
        drop(completed_window_owner);
        assert!(
            worker.pending_cancellations.lock().unwrap().is_empty(),
            "an abort between pending insertion and tombstone inspection must be leak-free"
        );
    }

    #[test]
    fn blocked_child_turn_is_not_relabelled_as_completed_agent_work() {
        let (status, failure) = agent_terminal_outcome(
            harness_contract::goal::GoalCompletion::Partial,
            "provider path exhausted",
        );
        assert_eq!(status, AgentTerminalStatus::Blocked);
        assert_eq!(failure.as_deref(), Some("provider path exhausted"));
    }

    #[test]
    fn delegated_prompt_rejects_simulated_tool_markup() {
        let mut packet = AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "agent",
                "run",
                "task",
                "session",
                "mission",
                Some("team"),
                "graph",
                "node",
            ),
            attempt: 1,
            expected_graph_revision: 0,
            policy_revision: 1,
            objective: "inspect source".into(),
            required_acceptance: Default::default(),
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            team_role_identity: None,
            team_role: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
                "budget",
                "agent",
                "agent",
                1,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
        };
        let prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
        assert!(prompt.contains("Never write simulated tool syntax"));
        assert!(prompt.contains("If no native tool is authorized, answer directly"));
        assert!(!prompt.contains("## Runtime clock"));

        packet.resource_scopes = vec!["read:external-app".to_string()];
        let scoped_prompt =
            system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
        assert!(scoped_prompt.contains("scope read:project means project/Cargo.toml"));
        assert!(scoped_prompt.contains("never bare Cargo.toml"));

        packet.objective = "update fixtures/target.txt".into();
        packet.output_acceptance = vec![harness_contract::team::TeamAcceptanceRequirement {
            criterion: "implementation".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::WorkspaceChange {
                field: harness_contract::team::TeamStructuredOutputField::Implementation,
                scopes: vec!["write:fixtures/target.txt".to_string()],
            },
        }];
        let mutation_prompt = system_prompt(
            &packet,
            std::path::Path::new("/workspace"),
            &["read_file".into(), "write_file".into()],
        )
        .join("\n");
        assert!(mutation_prompt.contains("Read each target at most once before mutation"));
        assert!(mutation_prompt.contains("write:fixtures/target.txt"));
        assert!(mutation_prompt.contains("Repeated reads"));
        assert!(mutation_prompt.contains("Native structured output"));
        assert!(!mutation_prompt.contains("Return exactly one JSON object"));
    }

    #[test]
    fn team_markdown_fragment_cache_is_digest_bound_and_counts_metrics() {
        let worker = InProcessAgentWorker::new(std::sync::Weak::new());
        let first =
            worker.cached_team_markdown_fragment("binding-a", "team-a", "# Team\n\nReview.");
        assert!(first[0].contains("binding digest team-a"));
        assert_eq!(worker.team_prompt_cache_stats().0, 0);
        assert_eq!(worker.team_prompt_cache_stats().1, 1);
        assert!(
            worker.team_prompt_cache_stats().2 > 0,
            "token increment is recorded"
        );

        let second =
            worker.cached_team_markdown_fragment("binding-a", "team-a", "# Team\n\nReview.");
        assert_eq!(first, second);
        assert_eq!(
            worker.team_prompt_cache_stats().0,
            1,
            "same digest pair is a cache hit"
        );
        assert_eq!(worker.team_prompt_cache_stats().1, 1);

        worker.cached_team_markdown_fragment("binding-a", "team-b", "# Team\n\nReview.");
        worker.cached_team_markdown_fragment("binding-b", "team-a", "# Team\n\nReview.");
        assert_eq!(
            worker.team_prompt_cache_stats().1,
            3,
            "any digest change rebuilds the prefix; no stale prefix is reused"
        );
    }

    #[test]
    fn scoped_tool_effect_key_is_stable_across_worker_recovery() {
        let first = deterministic_scoped_tool_idempotency_key(
            "graph-1",
            "node-1",
            2,
            3,
            "write_file",
            r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
        );
        let recovered = deterministic_scoped_tool_idempotency_key(
            "graph-1",
            "node-1",
            2,
            3,
            "write_file",
            r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
        );
        let next_effect = deterministic_scoped_tool_idempotency_key(
            "graph-1",
            "node-1",
            2,
            4,
            "write_file",
            r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
        );

        assert_eq!(first, recovered);
        assert_ne!(first, next_effect);
        assert!(first.contains("agent-tool:graph-1:node-1:2:3:write_file:"));
    }

    #[test]
    fn recovered_receipt_context_is_bounded_and_explicitly_fences_tools() {
        let receipt = crate::execution_core::graph::DurableAgentToolReceipt {
            sequence: 7,
            effect_kind: harness_contract::tool::ToolEffectKind::Write,
            authorized_scopes: vec!["write:src/lib.rs".to_string()],
            outcome: crate::RuntimeToolExecutionOutcome {
                tool_use_id: "tool-7".to_string(),
                tool_name: "write_file".to_string(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: crate::ToolSafetyCategory::WriteLocal,
                output: Some("committed output".to_string()),
                error: None,
                evidence_ref: "tool://receipt-7".to_string(),
                observed_evidence: Vec::new(),
            },
        };

        let prompt = recovered_agent_tool_receipt_prompt(&[receipt]).expect("recovery prompt");
        assert!(prompt.contains("committed output"));
        assert!(prompt.contains("Do not call tools"));
        assert!(prompt.contains("ToolHost receipts"));
    }
}
