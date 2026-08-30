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

#[path = "in_process/stages.rs"]
mod stages;
use stages::*;
#[path = "in_process/terminal.rs"]
mod terminal;
use terminal::*;

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

#[cfg(test)]
#[path = "in_process/tests.rs"]
mod tests;

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
        let provider_model_obligations = packet
            .required_acceptance
            .evidence_obligations
            .iter()
            .filter(|obligation| {
                obligation.observation_requirement
                    == harness_contract::context::EvidenceObservationRequirement::ProviderModel
            })
            .cloned()
            .collect();
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
            provider_model_obligations,
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
        let (has_successful_escalation, has_source_evidence) = {
            let receipts = tool_executor
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                receipts
                    .iter()
                    .any(|receipt| receipt.tool_name == "request_collaboration_escalation"),
                receipts
                    .iter()
                    .any(|receipt| !receipt.observed_evidence.is_empty()),
            )
        };
        if needs_managed_escalation_recovery(
            packet.requires_managed_collaboration_escalation,
            has_successful_escalation,
            has_source_evidence,
        ) {
            // This is a bounded Agent-bound Runtime recovery, not an answer
            // rewrite. The provider turn has already produced real source
            // evidence, while the terminal outcome is not committed yet. A
            // second `submit_turn` would create a new child graph and detach
            // the resulting receipt from this Agent attempt, so execute the
            // same native tool through its original scoped executor instead.
            // Gateway still derives the Program revision/fences from the
            // immutable Agent parent binding; this code supplies only the
            // semantic delta permitted to the Agent.
            let _ = services.agent_runtime().record_progress(
                packet.agent_id(),
                "agent.escalation.recovery_requested",
                "source evidence is durable but the required native collaboration escalation is missing; executing one bounded Agent-bound native recovery",
            );
            let recovery_input = managed_escalation_recovery_input(&packet);
            if let Err(error) = tool_executor
                .execute_managed_escalation_recovery(&recovery_input)
                .await
            {
                let _ = services.agent_runtime().record_progress(
                    packet.agent_id(),
                    "agent.escalation.recovery_failed",
                    &format!("bounded native escalation recovery failed: {error}"),
                );
            }
        }
        normalize_verified_narrative_terminal(&packet, &tool_executor, &mut summary);
        let scoped_receipts = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let observed_evidence = model_observed_evidence(
            &packet.required_acceptance,
            &summary.model_observations,
            &scoped_receipts,
        );
        let evidence_refs = agent_evidence_refs(
            &packet,
            &summary.context_turn_report.audit_projections,
            &scoped_receipts,
        );
        let (acceptance, runtime_change_receipts) = derive_receipt_backed_satisfied_criteria(
            &packet,
            &summary,
            &evidence_refs,
            &tool_executor,
            &observed_evidence,
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
        // A bounded recovery turn can commit its native escalation through a
        // graph-owned ToolBatch after this worker's initial in-memory receipt
        // snapshot was assembled.  The durable receipt log is authoritative
        // at the Agent terminal boundary; checking only the startup snapshot
        // incorrectly fails an Agent whose Runtime-attested escalation has
        // already appended the follow-up Team.
        let escalation_satisfied = !packet.requires_managed_collaboration_escalation
            || tool_executor
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .any(|receipt| receipt.tool_name == "request_collaboration_escalation")
            || crate::execution_core::graph::ExecutionCommitService::new(Arc::clone(
                services.event_store(),
            ))
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .map(|receipts| {
                receipts
                    .iter()
                    .any(|receipt| receipt.outcome.tool_name == "request_collaboration_escalation")
            })
            .unwrap_or(false);
        let (mut status, mut failure) =
            agent_terminal_outcome(summary.terminal_completion, &summary.final_answer);
        if !escalation_satisfied {
            status = AgentTerminalStatus::Failed;
            failure = Some(
                "required managed collaboration escalation has no successful Runtime tool receipt"
                    .to_string(),
            );
        }
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
    /// Frozen semantic observation policy compiled into the Agent packet.
    /// This describes required delivery; it never claims delivery occurred.
    provider_model_obligations: Vec<harness_contract::context::EvidenceObligation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedToolExecutionReceipt {
    sequence: u64,
    /// Provider ToolUse identity for this concrete delivery attempt. Durable
    /// replay receipts deliberately leave it absent until actual redelivery.
    provider_invocation_id: Option<String>,
    tool_name: String,
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
        provider_invocation_id: None,
        tool_name: receipt.outcome.tool_name.clone(),
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
        self.execute_scoped(tool_name, &normalized_input, None, None)
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
        if matches!(
            tool_name,
            "team_board" | "evidence_retrieve" | "request_collaboration_escalation"
        ) {
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
        self.execute_scoped(
            tool_name,
            &normalized_input,
            Some(authorization.clone()),
            None,
        )
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

    fn model_delivery_requirement(
        &self,
        tool_name: &str,
        input: &str,
    ) -> crate::ToolModelDeliveryRequirement {
        crate::ToolModelDeliveryRequirement::exact(
            self.provider_model_obligation_ids(tool_name, input),
        )
    }

    async fn execute_authorized_invocation_output(
        &self,
        provider_invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if tool_name == "checkpoint_create"
            || matches!(
                tool_name,
                "team_board" | "evidence_retrieve" | "request_collaboration_escalation"
            )
        {
            return self
                .execute_authorized_output(authorization, tool_name, input)
                .await;
        }
        if authorization.tool_id != tool_name || !self.allowed_tools.contains(tool_name) {
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
        self.execute_scoped(
            tool_name,
            &normalized_input,
            Some(authorization.clone()),
            Some(provider_invocation_id),
        )
        .await
        .map(harness_contract::context::ToolOutputDraft::bounded_inline)
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

fn observed_evidence_matches_requested_path(
    observed: &harness_contract::context::ObservedEvidence,
    requested_paths: &[String],
) -> bool {
    matches!(
        &observed.target,
        harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
            if requested_paths.contains(&scope.path.workspace_relative_path)
    )
}

impl ScopedRuntimeToolExecutor {
    fn provider_model_obligation_ids(&self, tool_name: &str, input: &str) -> Vec<String> {
        if self.provider_model_obligations.is_empty()
            || !matches!(tool_name, "read_file" | "read_many")
        {
            return Vec::new();
        }
        let Ok(normalized) = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        ) else {
            return Vec::new();
        };
        let Some(descriptor) = serde_json::from_str::<serde_json::Value>(&normalized)
            .ok()
            .and_then(|input| {
                self.host
                    .delegated_tool_effect_descriptor(tool_name, &input)
            })
        else {
            return Vec::new();
        };
        let requested = crate::governed_tool_plan::resource_scope_from_effect(&descriptor);
        let requested_identities = requested
            .paths
            .iter()
            .filter_map(|path| self.path_identity_resolver.resolve_planned_file(path).ok())
            .collect::<Vec<_>>();
        let mut ids = self
            .provider_model_obligations
            .iter()
            .filter_map(|obligation| {
                let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
                    &obligation.target
                else {
                    return None;
                };
                requested_identities
                    .iter()
                    .any(|requested| {
                        let same_workspace = requested.workspace_id == scope.path.workspace_id
                            && requested.repository_id == scope.path.repository_id;
                        let exact_path = requested.workspace_relative_path
                            == scope.path.workspace_relative_path;
                        let descendant_verification = obligation.kind
                            == harness_contract::context::EvidenceObligationKind::VerifyAfterWrite
                            && (scope.path.workspace_relative_path.is_empty()
                                || exact_path
                                || requested
                                    .workspace_relative_path
                                    .strip_prefix(&scope.path.workspace_relative_path)
                                    .is_some_and(|suffix| suffix.starts_with('/')));
                        same_workspace
                            && (exact_path || descendant_verification)
                            && (scope.coverage
                                == harness_contract::context::EvidenceCoverageKind::ExactContent
                                || obligation.kind
                                    == harness_contract::context::EvidenceObligationKind::VerifyAfterWrite)
                    })
                .then(|| obligation.obligation_id.clone())
            })
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

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
        if tool_name == "request_collaboration_escalation" {
            let receipts = self
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if receipts
                .iter()
                .any(|receipt| receipt.tool_name == "request_collaboration_escalation")
            {
                return Err(ToolError::new(
                    "a managed Agent may request collaboration escalation at most once per attempt",
                ));
            }
            if !receipts
                .iter()
                .any(|receipt| !receipt.observed_evidence.is_empty())
            {
                return Err(ToolError::new(
                    "collaboration escalation requires a prior source-evidence receipt at a safe checkpoint",
                ));
            }
        }
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

    /// Execute the one Runtime-owned remediation for a managed-Agent
    /// escalation obligation that the provider omitted after producing source
    /// evidence. This is intentionally not exposed through [`ToolExecutor`]:
    /// model-originated calls must carry a normal invocation authorization.
    ///
    /// The recovery is already constrained by the immutable packet flag, the
    /// prior source-evidence receipt and the Gateway's parent-Team/Program
    /// attestation. `request_collaboration_escalation` is a read-only Runtime
    /// control-plane request with no workspace path. Sending it through the
    /// generic scoped executor incorrectly rejects it as an unbounded file
    /// effect before Gateway can validate that binding.
    async fn execute_managed_escalation_recovery(
        &self,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        const TOOL: &str = "request_collaboration_escalation";
        if !self.allowed_tools.contains(TOOL) {
            return Err(ToolError::new(
                "managed escalation recovery is outside the AgentTaskPacket allow-list",
            ));
        }
        {
            let receipts = self
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if receipts.iter().any(|receipt| receipt.tool_name == TOOL) {
                return Err(ToolError::new(
                    "a managed Agent may request collaboration escalation at most once per attempt",
                ));
            }
            if !receipts
                .iter()
                .any(|receipt| !receipt.observed_evidence.is_empty())
            {
                return Err(ToolError::new(
                    "collaboration escalation requires a prior source-evidence receipt at a safe checkpoint",
                ));
            }
        }
        let value = serde_json::from_str::<serde_json::Value>(input).map_err(|error| {
            ToolError::new(format!(
                "invalid managed escalation recovery input: {error}"
            ))
        })?;
        let descriptor = self
            .host
            .delegated_tool_effect_descriptor(TOOL, &value)
            .ok_or_else(|| ToolError::new("managed escalation has no Runtime effect descriptor"))?;
        if descriptor.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || descriptor.required_permission != harness_contract::policy::PermissionMode::ReadOnly
        {
            return Err(ToolError::new(
                "managed escalation recovery requires a read-only Runtime control descriptor",
            ));
        }
        let sequence = self
            .next_receipt_sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let request = RuntimeToolExecutionRequest {
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: sequence,
            observation_wave_sequence: sequence,
            idempotency_key: deterministic_scoped_tool_idempotency_key(
                &self.execution_id,
                &self.node_id,
                self.attempt,
                sequence,
                TOOL,
                input,
            ),
            tool_use_id: format!(
                "agent-runtime-recovery:{}:{}:{}:{TOOL}",
                self.node_id, self.attempt, sequence
            ),
            tool_name: TOOL.to_string(),
            input: input.to_string(),
            category: crate::ToolSafetyCategory::ReadOnly,
            // This call is not model-originated. Gateway accepts an absent
            // authorization only for a read-only control tool, then attests
            // the exact Agent/Team/Program binding before it can mutate the
            // Program through its fenced escalation transaction.
            authorization: None,
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
            managed_invocation: self.managed_invocation.clone(),
            tool_progress: crate::ToolProgressSink::default(),
        };
        let outcome = self.host.execute_runtime_tool(&request).await;
        if outcome.status != RuntimeToolExecutionStatus::Executed {
            return Err(ToolError::new(outcome.error.unwrap_or_else(|| {
                "managed escalation recovery was not executed".to_string()
            })));
        }
        if let Some(commit_service) = &self.commit_service {
            commit_service
                .commit_readonly_tool_receipts(&[(request, outcome.clone())])
                .map_err(|error| {
                    ToolError::new(format!(
                        "managed escalation recovery completed but durable receipt commit failed: {error}"
                    ))
                })?;
        }
        self.receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ScopedToolExecutionReceipt {
                sequence,
                provider_invocation_id: None,
                tool_name: TOOL.to_string(),
                effect_kind: descriptor.effect_kind,
                resource_scopes: vec!["runtime:collaboration_escalation".to_string()],
                paths: Vec::new(),
                prior_states: BTreeMap::new(),
                after_digests: BTreeMap::new(),
                observed_evidence: outcome.observed_evidence.clone(),
            });
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            outcome.output.unwrap_or_default(),
        ))
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
        provider_invocation_id: Option<&str>,
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
        let (mut outcome, fresh_execution) = match effect_state {
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
        // Delegated ToolHost adapters must return typed observations together
        // with a successful receipt. Some compatibility adapters return only
        // raw structured output. In that case Runtime may mint exact-content
        // evidence solely when the output itself proves start=1, EOF coverage,
        // no truncation and a valid full-file digest. Requested paths and a
        // successful status alone are never evidence. Discovery tools must
        // provide their own typed observations; writes and failures are never
        // inferred here.
        if outcome.status == RuntimeToolExecutionStatus::Executed
            && outcome.observed_evidence.is_empty()
            && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
        {
            let parsed = outcome
                .output
                .as_deref()
                .and_then(|output| serde_json::from_str::<serde_json::Value>(output).ok());
            if tool_name == "read_file" {
                if let Some(observed) = parsed.as_ref().and_then(|output| {
                    self.path_identity_resolver
                        .observe_complete_read_tool_output(tool_name, output, sequence)
                        .ok()
                        .filter(|observed| {
                            observed_evidence_matches_requested_path(observed, &requested.paths)
                        })
                }) {
                    outcome.observed_evidence.push(observed);
                }
            } else if tool_name == "read_many" {
                outcome.observed_evidence.extend(
                    parsed
                        .as_ref()
                        .and_then(|output| output.get("results"))
                        .and_then(serde_json::Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter(|item| {
                            item.get("status").and_then(serde_json::Value::as_str)
                                == Some("success")
                        })
                        .filter_map(|item| item.get("output"))
                        .filter_map(|output| {
                            self.path_identity_resolver
                                .observe_complete_read_tool_output("read_file", output, sequence)
                                .ok()
                                .filter(|observed| {
                                    observed_evidence_matches_requested_path(
                                        observed,
                                        &requested.paths,
                                    )
                                })
                        }),
                );
            }
        }
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
        // The delegated read receipt is now committed (or was recovered from
        // that committed receipt). Bind any typed observation to that durable
        // Runtime event before the Agent terminal consumes it. Gateway may
        // initially expose the observation with an unavailable raw-artifact
        // selector because transcript compaction happens later; the effect
        // receipt itself is already durable and is the correct provenance
        // carrier for acceptance.
        if outcome.status == RuntimeToolExecutionStatus::Executed
            && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && self.commit_service.is_some()
            && !outcome.observed_evidence.is_empty()
        {
            let output = outcome.output.as_deref().unwrap_or_default();
            let access = harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed(
                    "delegated_agent_read_receipt",
                    format!("{}:read-receipt", request.idempotency_key),
                ),
                format!("sha256:{:x}", Sha256::digest(output.as_bytes())),
                u64::try_from(output.len().max(1)).unwrap_or(u64::MAX),
                "application/vnd.cowd.tool-receipt+json",
                format!(
                    "event://execution-effect/{}/{}:read-receipt",
                    request.idempotency_key, request.idempotency_key
                ),
                format!("session:{}", self.session_id),
            );
            for observed in &mut outcome.observed_evidence {
                if observed
                    .evidence_ref
                    .as_ref()
                    .is_none_or(|current| !current.is_durable())
                {
                    observed.evidence_ref = Some(access.clone());
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
                        provider_invocation_id: provider_invocation_id.map(str::to_string),
                        tool_name: tool_name.to_string(),
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
                    Some(field.as_str().to_string())
                }
                harness_contract::team::TeamAcceptanceCheck::StructuredArtifact { name } => {
                    Some(name.clone())
                }
                harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => {
                    Some("source_verification".to_string())
                }
                harness_contract::team::TeamAcceptanceCheck::UpstreamReview => {
                    Some("review".to_string())
                }
                harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
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

/// Exact acquisition and exact model observation are separate facts. The
/// ToolHost receipt proves the former; a non-omitting model receipt proves
/// the latter. Only model-observed exact evidence may satisfy an Agent's
/// semantic acceptance contract.
fn model_observed_evidence(
    required: &harness_contract::context::RequiredAcceptance,
    model_observations: &[harness_contract::context::ProviderModelObservationAttestation],
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::context::ObservedEvidence> {
    receipts
        .iter()
        .flat_map(|receipt| {
            receipt.observed_evidence.iter().cloned().map(|mut observed| {
                let Some(provider_invocation_id) = receipt.provider_invocation_id.as_deref()
                else {
                    return observed;
                };
                let matching_attestation = model_observations.iter().find(|attestation| {
                    attestation.provider_invocation_id == provider_invocation_id
                        && required.evidence_obligations.iter().any(|obligation| {
                            obligation.observation_requirement
                                == harness_contract::context::EvidenceObservationRequirement::ProviderModel
                                && attestation
                                    .obligation_ids
                                    .contains(&obligation.obligation_id)
                                && crate::path_identity::observed_evidence_satisfies(
                                    obligation,
                                    &harness_contract::context::ObservedEvidence {
                                        model_observation: Some((*attestation).clone()),
                                        ..observed.clone()
                                    },
                                )
                        })
                });
                if let Some(attestation) = matching_attestation {
                    observed.model_observation = Some(attestation.clone());
                }
                observed
            })
        })
        .collect()
}

/// Derive structured-field criteria from the terminal answer and canonical
/// ToolHost receipts.  Obligation matching itself remains exclusively owned
/// by `AcceptanceEvaluator::evaluate_required` at the terminal boundary.
fn derive_receipt_backed_satisfied_criteria(
    packet: &AgentTaskPacket,
    summary: &crate::TurnSummary,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    tool_executor: &ScopedRuntimeToolExecutor,
    model_observed_evidence: &[harness_contract::context::ObservedEvidence],
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
            model_observed_evidence,
        )
    };
    let required_fields = packet_acceptance_contract(packet)
        .iter()
        .filter_map(|requirement| match &requirement.check {
            harness_contract::team::TeamAcceptanceCheck::StructuredField { field }
            | harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, .. } => {
                Some(field.as_str().to_string())
            }
            harness_contract::team::TeamAcceptanceCheck::StructuredArtifact { name } => {
                Some(name.clone())
            }
            harness_contract::team::TeamAcceptanceCheck::SourceVerification { .. } => {
                Some("source_verification".to_string())
            }
            harness_contract::team::TeamAcceptanceCheck::UpstreamReview => {
                Some("review".to_string())
            }
            harness_contract::team::TeamAcceptanceCheck::ScopedEvidence { .. }
            | harness_contract::team::TeamAcceptanceCheck::UpstreamEvidence => None,
        })
        .collect::<Vec<_>>();
    let output = structured_agent_output_for_fields(&summary.final_answer, &required_fields);
    let field_present = |field: harness_contract::team::TeamStructuredOutputField| {
        let value = output
            .as_ref()
            .and_then(|object| object.get(field.as_str()));
        structured_field_materialized(field, value)
    };
    let artifact_present = |name: &str| {
        output
            .as_ref()
            .and_then(|object| object.get(name))
            .is_some_and(materialized_json_value)
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
            harness_contract::team::TeamAcceptanceCheck::StructuredArtifact { name } => {
                artifact_present(name)
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
    structured_contract_field_materialized(field.as_str(), value)
}

/// Canonical materialization semantics for fixed Team presentation fields.
///
/// Disclosure fields distinguish an explicit empty list (reviewed, with no
/// items found) from an omitted or null field. Host presentation recovery and
/// delegated Agent acceptance must share this exact rule so a valid terminal
/// cannot be accepted by one boundary and rejected by the other.
pub(crate) fn structured_contract_field_materialized(
    field: &str,
    value: Option<&serde_json::Value>,
) -> bool {
    if matches!(field, "risks" | "unresolved" | "unresolved_or_risks") {
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
    // Keep this list aligned with `TeamStructuredOutputField`. The parser
    // remains allow-listed: an arbitrary prose label cannot become an
    // acceptance field merely because it contains a colon. Runtime evidence,
    // rather than a model-authored compatibility field, is the sole proof for
    // custom acceptance criteria.
    const CONTRACT_FIELDS: [&str; 15] = [
        "summary",
        "findings",
        "evidence",
        "plan",
        "implementation",
        "source_verification",
        "review",
        "risks",
        "unresolved",
        "key_decisions",
        "unresolved_or_risks",
        "proposal",
        "critique",
        "mitigation",
        "checkpoint",
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
    let embedded_contract = text
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
        .max_by_key(|object| (object.contains_key("summary"), object.len()));

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
            // Providers commonly render a requested label as a bold heading
            // (`**Field: summary**`) rather than a bare heading.  `Field:`
            // is presentation syntax, not part of the allow-listed field.
            let heading = heading.trim();
            let heading = heading
                .strip_prefix("Field:")
                .or_else(|| heading.strip_prefix("field:"))
                .unwrap_or(heading)
                .trim();
            let normalized = heading.to_ascii_lowercase().replace([' ', '-'], "_");
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
    // An explicit outer presentation contract is more authoritative than a
    // JSON example embedded in its findings. This matters especially when an
    // Agent is reviewing parsers, protocol fixtures, or data files whose
    // source text legitimately contains allow-listed contract keys. Exact
    // whole-response JSON and supported envelopes were already handled above;
    // embedded JSON remains the final compatibility fallback.
    (!object.is_empty()).then_some(object).or(embedded_contract)
}

/// Parse the fixed Team presentation contract plus any exact, Runtime-declared
/// artifact names for this role. Custom names remain closed by default: an
/// arbitrary JSON key or prose label is accepted only when it appears in the
/// immutable role contract passed by Runtime.
pub(crate) fn structured_agent_output_for_fields(
    text: &str,
    required: &[String],
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut output = structured_agent_output(text).unwrap_or_default();
    if required.is_empty() {
        return (!output.is_empty()).then_some(output);
    }

    let required_name = |candidate: &str| {
        let normalized = candidate
            .trim()
            .trim_end_matches(':')
            .trim()
            .to_ascii_lowercase()
            .replace([' ', '-'], "_");
        required
            .iter()
            .find(|field| field.to_ascii_lowercase() == normalized)
            .cloned()
    };
    let mut merge_required = |object: &serde_json::Map<String, serde_json::Value>| {
        for required_field in required {
            if output.contains_key(required_field) {
                continue;
            }
            if let Some((_, value)) = object
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(required_field))
            {
                output.insert(required_field.clone(), value.clone());
            }
        }
    };

    // Exact JSON and the four supported provider envelopes may carry a custom
    // artifact. Do not recursively trust arbitrary nested objects.
    if let Some(serde_json::Value::Object(object)) = parse_first_contract_json(text) {
        merge_required(&object);
        for wrapper in ["output", "data", "response", "answer"] {
            let Some(value) = object.get(wrapper) else {
                continue;
            };
            match value {
                serde_json::Value::Object(nested) => merge_required(nested),
                serde_json::Value::String(encoded) => {
                    if let Some(serde_json::Value::Object(nested)) =
                        parse_first_contract_json(encoded)
                    {
                        merge_required(&nested);
                    }
                }
                _ => {}
            }
        }
    }

    // Providers also commonly use exact Markdown sections. Unlike the fixed
    // parser, this scanner treats non-contract subheadings as content so a
    // rich custom report can contain its own hierarchy and tables.
    let mut custom = serde_json::Map::new();
    let mut active_field: Option<String> = None;
    let mut active_lines = Vec::new();
    let flush = |object: &mut serde_json::Map<String, serde_json::Value>,
                 field: &mut Option<String>,
                 lines: &mut Vec<&str>| {
        if let Some(field) = field.take() {
            let value = lines.join("\n").trim().to_string();
            if !value.is_empty() {
                object.insert(field, serde_json::Value::String(value));
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
                    .map(str::trim)
            });
        let labeled = heading.map(|label| (label, None)).or_else(|| {
            trimmed
                .split_once(':')
                .map(|(label, value)| (label, Some(value)))
        });
        if let Some((label, inline_value)) = labeled {
            let label = label.trim().trim_start_matches(['-', '*']).trim();
            if let Some(field) = required_name(label) {
                flush(&mut custom, &mut active_field, &mut active_lines);
                active_field = Some(field);
                if let Some(value) = inline_value.filter(|value| !value.trim().is_empty()) {
                    active_lines.push(value.trim());
                }
                continue;
            }
        }
        if active_field.is_some() {
            active_lines.push(line);
        }
    }
    flush(&mut custom, &mut active_field, &mut active_lines);
    merge_required(&custom);

    (!output.is_empty()).then_some(output)
}
