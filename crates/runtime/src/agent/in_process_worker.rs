use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
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

/// Executes a delegated task through the same RuntimeServices/Runner/provider
/// path as a primary turn. It never calls `ConversationRuntime` directly.
pub struct InProcessAgentWorker {
    services: Weak<RuntimeServices>,
    active_runs: Mutex<BTreeMap<String, ActiveInProcessRun>>,
    pending_cancellations: Mutex<BTreeSet<String>>,
    completed_runs: Mutex<VecDeque<String>>,
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
        }
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
                packet.team_id.is_none()
                    || delegated_tool_supports_bounded_scope(host.as_ref(), tool)
            })
            .collect::<BTreeSet<_>>();
        let tool_names = allowed_tools.iter().cloned().collect::<Vec<_>>();
        let tool_executor = Arc::new(ScopedRuntimeToolExecutor {
            host,
            allowed_tools: allowed_tools.clone(),
            session_id: packet.session_id.clone(),
            model_lease: selection.model.clone(),
            execution_id: packet.graph_id.clone(),
            node_id: packet.node_id.clone(),
            workspace_root: services.workspace_root().to_path_buf(),
            resource_scopes: packet
                .team_id
                .as_ref()
                .map(|_| packet.resource_scopes.clone()),
            evaluation_isolated: binding.evaluation.is_some(),
            managed_invocation: packet.managed_invocation.clone(),
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        });
        let policy = permission_policy(&packet.permission_lease, &allowed_tools);
        let cancellation = crate::CancellationToken::new();
        let (provider_event_sender, provider_event_receiver) = mpsc::sync_channel(64);
        let progress_runtime = Arc::clone(services.agent_runtime());
        let progress_agent_id = packet.agent_id.clone();
        let progress_run_id = packet.run_id.clone();
        let progress_reporter = std::thread::spawn(move || {
            let mut saw_model_output = false;
            while let Ok(event) = provider_event_receiver.recv() {
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
        let mut child_session = Session::new();
        // An in-process role is a child execution of the parent session, not
        // an unrelated surface session. Keep the canonical session/model
        // binding available to tool and orchestration contracts.
        child_session.session_id = packet.session_id.clone();
        child_session.model = Some(selection.model.clone());
        let child_session_id = child_session.session_id.clone();
        // RuntimeServices owns the inspected Skill snapshot. The Binding's
        // refs below remain the capability ceiling; this worker never scans
        // package directories or falls back to an empty production profile.
        let skill_catalog = services.skill_catalog();
        let host = StandardRuntimeHost::new(StandardRuntimeHostConfig {
            runtime_services: Arc::clone(&services),
            session: child_session,
            provider_registry: Arc::clone(services.provider_registry()),
            model: selection.model.clone(),
            tool_definitions: tool_definitions.clone(),
            tool_executor: Arc::clone(&tool_executor),
            permission_policy: policy,
            system_prompt: system_prompt(&packet, services.workspace_root(), &tool_names),
            feature_config: crate::RuntimeFeatureConfig::default(),
            emit_output: false,
            stream_callback: Some(provider_event_sender),
            tool_callback: None,
            model_context_window: None,
            // A child agent shares the parent Session authority for durable
            // tool evidence and context receipts. The session id is already
            // bound to the parent above, so this cannot create a parallel
            // store or leak raw tool output back inline as a fallback.
            session_store: services.session_store(),
            hook_progress_reporter: None,
            external_context_items: Vec::new(),
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
            memory_agent_id: binding.instance.instance_id.clone(),
            memory_definition_lineage_id: Some(
                binding.definition_ref.definition_id.as_str().to_string(),
            ),
            memory_team_id: binding.data_lease.team_id.clone(),
            memory_read_scopes: binding.data_lease.read_scopes.clone(),
            reality_binding: Some(binding.clone()),
            execution_parent: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: packet.graph_id.clone(),
                node_id: packet.node_id.clone(),
            }),
        });
        let mut runtime = match host {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(format!(
                    "failed to initialize in-process agent host: {error}"
                ));
            }
        };
        // A delegated role has a bounded evidence obligation. It retains the
        // parent session authority but must not inherit MainTurn's broad,
        // open-ended exploration profile.
        runtime.set_context_profile(ContextProfile::SubAgent);
        if let Some(limit) = agent_model_step_limit(packet.budget_lease.max_tokens) {
            runtime.set_model_step_limit_override(limit);
        }
        runtime.set_delegated_focus_policy(
            packet_focus_novelty_target_bp(&packet),
            packet_focus_acceptance_scopes(&packet),
        );
        // Delegated Agents share the parent Session's evidence authority, but
        // only the parent Turn may publish conversation messages. The child
        // result returns through AgentReturnPacket and the Team reducer.
        runtime.set_transcript_persistence(false);
        let input_stream = runtime.session_input_stream();
        let completion = Arc::new(tokio::sync::Notify::new());
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                packet.run_id.clone(),
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
            run_id: packet.run_id.clone(),
            completion: Arc::clone(&completion),
            completed: Arc::clone(&completed),
        };
        if self
            .pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&packet.run_id)
        {
            cancellation.cancel();
        }
        runtime.install_turn_control(cancellation, crate::HookAbortSignal::default());
        let _ = services.agent_runtime().record_progress(
            &packet.agent_id,
            "agent.execution.started",
            "provider-backed child execution admitted",
        );
        let result = runtime
            .submit_turn(&packet.objective, &SharedPrompter::none())
            .await;
        // Dropping the host drops the provider callback sender. The bounded
        // reporter owns no runtime state beyond the lifecycle projection, so
        // it can be joined before the terminal Agent result is committed.
        drop(runtime);
        let _ = progress_reporter.join();
        drop(active_run_cleanup);
        let summary = result.map_err(|error| format!("in-process agent turn failed: {error}"))?;
        let evidence_refs =
            agent_evidence_refs(&packet, &summary.context_turn_report.audit_projections);
        let (acceptance, runtime_change_receipts) =
            runtime_evaluated_acceptance(&packet, &summary, &evidence_refs, &tool_executor);
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
        let mut runtime_observed_resource_scopes = tool_executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .flat_map(|receipt| {
                let mode = if receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write {
                    "write"
                } else {
                    "read"
                };
                receipt
                    .paths
                    .iter()
                    .map(move |path| format!("{mode}:{path}"))
            })
            .collect::<Vec<_>>();
        runtime_observed_resource_scopes.sort();
        runtime_observed_resource_scopes.dedup();
        let changes = runtime_change_receipts
            .iter()
            .map(|receipt| receipt.path.clone())
            .collect();
        let (status, failure) =
            agent_terminal_outcome(summary.terminal_completion, &summary.final_answer);
        Ok(AgentReturnPacket {
            run_id: packet.run_id,
            agent_id: packet.agent_id,
            task_id: packet.task_id,
            session_id: packet.session_id,
            mission_id: packet.mission_id,
            team_id: packet.team_id,
            graph_id: packet.graph_id,
            node_id: packet.node_id,
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status,
            outcome: summary.final_answer,
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
            runtime_write_attempt_paths,
            runtime_observed_resource_scopes,
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

fn agent_model_step_limit(max_tokens: u64) -> Option<usize> {
    if max_tokens == 0 {
        return None;
    }
    // One Agent model step commonly repacks several thousand input tokens.
    // Convert the immutable token lease into a conservative absolute step
    // ceiling so cumulative request growth cannot turn a 24k role lease into
    // hundreds of thousands of provider tokens.
    Some(
        usize::try_from(max_tokens.saturating_add(3_999) / 4_000)
            .unwrap_or(6)
            .clamp(3, 6),
    )
}

fn packet_focus_novelty_target_bp(packet: &AgentTaskPacket) -> u16 {
    packet
        .constraints
        .iter()
        .find_map(|constraint| {
            constraint
                .strip_prefix("focus_novelty_target_bp:")
                .and_then(|value| value.parse::<u16>().ok())
        })
        .unwrap_or(0)
        .min(10_000)
}

fn packet_focus_acceptance_scopes(packet: &AgentTaskPacket) -> Vec<String> {
    focus_acceptance_scopes_from_constraints(&packet.constraints)
}

fn focus_acceptance_scopes_from_constraints(constraints: &[String]) -> Vec<String> {
    let explicit = constraints.iter().find_map(|constraint| {
        constraint
            .strip_prefix("focus_output_acceptance:")
            .filter(|value| !value.trim().is_empty())
    });
    let mut scopes = explicit
        .map(|value| value.split(',').map(str::trim).collect::<Vec<_>>())
        .filter(|criteria| {
            !criteria.is_empty()
                && criteria
                    .iter()
                    .all(|criterion| criterion.starts_with("evidence_scope:"))
        })
        .into_iter()
        .flatten()
        .filter_map(|criterion| criterion.strip_prefix("evidence_scope:"))
        .filter(|scope| !scope.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if scopes.is_empty() {
        // A mutation role may expose semantic output names such as
        // `implementation` instead of `evidence_scope:*`. Preserve the
        // Runtime-owned workspace-change acceptance contract so repeated
        // reads cannot be mistaken for completed mutation work.
        scopes.extend(
            constraints
                .iter()
                .find_map(|constraint| {
                    constraint
                        .strip_prefix("team_acceptance_contract:")
                        .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
                })
                .and_then(|value| value.as_array().cloned())
                .into_iter()
                .flatten()
                .filter_map(|criterion| criterion.get("check").cloned())
                .filter(|check| {
                    check.get("kind").and_then(serde_json::Value::as_str)
                        == Some("workspace_change")
                })
                .filter_map(|check| {
                    check
                        .get("scopes")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                })
                .flatten()
                .filter_map(|scope| scope.as_str().map(str::to_string)),
        );
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

fn agent_terminal_outcome(
    completion: harness_contract::goal::GoalCompletion,
    terminal_answer: &str,
) -> (AgentTerminalStatus, Option<String>) {
    match completion {
        harness_contract::goal::GoalCompletion::Satisfied => (AgentTerminalStatus::Completed, None),
        harness_contract::goal::GoalCompletion::Blocked => (
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
    model_lease: String,
    execution_id: String,
    node_id: String,
    workspace_root: std::path::PathBuf,
    /// `Some` marks a Team child and is always enforced. An empty list means
    /// no workspace authority; it never expands to the whole repository.
    resource_scopes: Option<Vec<String>>,
    evaluation_isolated: bool,
    managed_invocation: Option<harness_contract::managed_agent::ManagedAgentInvocationFence>,
    next_receipt_sequence: AtomicU64,
    receipts: Mutex<Vec<ScopedToolExecutionReceipt>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopedToolExecutionReceipt {
    sequence: u64,
    effect_kind: harness_contract::tool::ToolEffectKind,
    paths: Vec<String>,
    before_digests: BTreeMap<String, Option<String>>,
    after_digests: BTreeMap<String, Option<String>>,
}

impl ToolExecutor for ScopedRuntimeToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
        if tool_name == "ToolSearch" {
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
            return serde_json::to_string(&receipt).map_err(|error| {
                ToolError::new(format!("serialize agent tool discovery: {error}"))
            });
        }
        if !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is outside the AgentTaskPacket allow-list"
            )));
        }
        self.enforce_resource_ceiling(tool_name, input)?;
        self.execute_scoped(tool_name, input, None)
    }

    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        use harness_contract::tool::{
            ToolDescriptorHealth, ToolDescriptorRef, ToolDiscoveryReceipt, ToolPermissionMode,
        };

        let mut descriptors = Vec::with_capacity(self.allowed_tools.len().saturating_add(1));
        descriptors.push(ToolDescriptorRef {
            canonical_id: "ToolSearch".to_string(),
            display_name: "ToolSearch".to_string(),
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

    fn describe_tool_effect(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.allowed_tools
            .contains(tool_name)
            .then(|| self.host.delegated_tool_effect_descriptor(tool_name, input))
            .flatten()
    }

    fn execute_authorized(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
    ) -> Result<String, ToolError> {
        if authorization.tool_id != tool_name || !self.allowed_tools.contains(tool_name) {
            return Err(ToolError::new(
                "agent tool authorization does not match the allowed tool request",
            ));
        }
        self.enforce_resource_ceiling(tool_name, input)?;
        self.execute_scoped(tool_name, input, Some(authorization.clone()))
    }

    fn available_tool_names(&self) -> Vec<String> {
        std::iter::once("ToolSearch".to_string())
            .chain(self.allowed_tools.iter().cloned())
            .collect()
    }

    fn classify_tool_safety(
        &self,
        tool_name: &str,
        _input: &str,
    ) -> Option<crate::ToolSafetyCategory> {
        self.allowed_tools
            .contains(tool_name)
            .then(|| crate::ToolSafetyCategory::from_tool_name(tool_name))
    }
}

impl ScopedRuntimeToolExecutor {
    fn enforce_resource_ceiling(&self, tool_name: &str, input: &str) -> Result<(), ToolError> {
        let Some(allowed_scopes) = self.resource_scopes.as_deref() else {
            return Ok(());
        };
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
        let requested = crate::tool_execution_plan::resource_scope_for_tool_request(
            tool_name,
            &input,
            crate::ToolSafetyCategory::from_tool_name(tool_name),
        );
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
            if !resource_path_is_authorized(&self.workspace_root, path, allowed_scopes, write) {
                return Err(ToolError::new(format!(
                    "tool `{tool_name}` path `{path}` is outside the Agent focus/resource lease"
                )));
            }
        }
        Ok(())
    }

    fn execute_scoped(
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
        let requested = crate::tool_execution_plan::resource_scope_for_tool_request(
            tool_name,
            &parsed_input,
            crate::ToolSafetyCategory::from_tool_name(tool_name),
        );
        let sequence = self
            .next_receipt_sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let before_digests = requested
            .paths
            .iter()
            .map(|path| {
                (
                    path.clone(),
                    workspace_file_sha256(&self.workspace_root, path),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let idempotency_key = authorization
            .as_ref()
            .and_then(|value| value.idempotency_key.clone())
            .unwrap_or_else(|| {
                format!(
                    "agent-tool:{tool_name}:{}",
                    crate::tool_invocation::now_ms()
                )
            });
        let request = RuntimeToolExecutionRequest {
            idempotency_key,
            tool_use_id: format!("agent-tool:{}", uuid::Uuid::new_v4()),
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            category: crate::ToolSafetyCategory::from_tool_name(tool_name),
            authorization,
            session_id: Some(self.session_id.clone()),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            evaluation_isolated: self.evaluation_isolated,
            managed_invocation: self.managed_invocation.clone(),
        };
        let outcome = self.host.execute_runtime_tool(&request);
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => {
                let after_digests = requested
                    .paths
                    .iter()
                    .map(|path| {
                        (
                            path.clone(),
                            workspace_file_sha256(&self.workspace_root, path),
                        )
                    })
                    .collect::<BTreeMap<_, _>>();
                self.receipts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(ScopedToolExecutionReceipt {
                        sequence,
                        effect_kind: descriptor.effect_kind,
                        paths: requested.paths,
                        before_digests,
                        after_digests,
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

fn workspace_file_sha256(workspace_root: &std::path::Path, relative: &str) -> Option<String> {
    let parts = normalized_relative_parts(relative)?;
    let path = workspace_root.join(parts.iter().collect::<std::path::PathBuf>());
    let metadata = std::fs::symlink_metadata(&path).ok()?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return None;
    }
    let bytes = std::fs::read(path).ok()?;
    Some(format!("{:x}", Sha256::digest(bytes)))
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
    workspace_root: &std::path::Path,
    requested: &str,
    allowed_scopes: &[String],
    write: bool,
) -> bool {
    let Some(requested_parts) = normalized_relative_parts(requested) else {
        return false;
    };
    let Ok(canonical_root) = workspace_root.canonicalize() else {
        return false;
    };
    let requested_relative = requested_parts.iter().collect::<std::path::PathBuf>();
    let requested_path = workspace_root.join(&requested_relative);
    let Some(canonical_requested_ancestor) = canonical_existing_ancestor(&requested_path) else {
        return false;
    };
    if !canonical_requested_ancestor.starts_with(&canonical_root) {
        return false;
    }
    allowed_scopes.iter().any(|scope| {
        let (mode, allowed) = scope.split_once(':').unwrap_or(("", ""));
        if (write && mode != "write") || (!write && mode != "read" && mode != "write") {
            return false;
        }
        let Some(allowed_parts) = normalized_relative_parts(allowed) else {
            return false;
        };
        if allowed_parts.is_empty() {
            return false;
        }
        let allowed_relative = allowed_parts.iter().collect::<std::path::PathBuf>();
        let lexical_match = if workspace_root.join(&allowed_relative).is_dir() {
            requested_parts.starts_with(&allowed_parts)
        } else {
            requested_parts == allowed_parts
        };
        if !lexical_match {
            return false;
        }
        let Ok(canonical_allowed) = workspace_root.join(&allowed_relative).canonicalize() else {
            return false;
        };
        if canonical_allowed != canonical_root.join(&allowed_relative) {
            // A scope whose lexical identity resolves through a symlink can
            // alias another focus partition and defeat overlap accounting.
            return false;
        }
        canonical_allowed.starts_with(&canonical_root)
            && canonical_requested_ancestor.starts_with(&canonical_allowed)
    })
}

fn canonical_existing_ancestor(path: &std::path::Path) -> Option<std::path::PathBuf> {
    let mut candidate = path;
    loop {
        match candidate.canonicalize() {
            Ok(canonical) => return Some(canonical),
            Err(_) => candidate = candidate.parent()?,
        }
    }
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

fn permission_policy(lease: &str, tools: &BTreeSet<String>) -> PermissionPolicy {
    let mode = match lease {
        "danger-full-access" => PermissionMode::DangerFullAccess,
        "workspace-write" => PermissionMode::WorkspaceWrite,
        "prompt" => PermissionMode::Prompt,
        _ => PermissionMode::ReadOnly,
    };
    tools
        .iter()
        .fold(PermissionPolicy::new(mode), |policy, tool| {
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
            "Authorized resource scopes: {}. Use only relative paths inside these scopes; a missing path never means the whole workspace.",
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
    if let Some(contract) = packet_acceptance_contract(packet) {
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
            "Return exactly one JSON object without markdown fences. Populate every required structured field with a non-empty value: {}. Runtime derives acceptance from committed tool receipts, change paths, upstream evidence bindings, and this exact schema; repeating acceptance text does not satisfy it.",
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
) -> Vec<harness_contract::context::EvidenceAccessRef> {
    let mut refs = packet.evidence_refs.clone();
    refs.extend(audits.iter().filter_map(|audit| audit.access.clone()));
    refs.sort_by(|left, right| {
        left.evidence_ref
            .0
            .ref_type
            .cmp(&right.evidence_ref.0.ref_type)
            .then_with(|| left.evidence_ref.0.id.cmp(&right.evidence_ref.0.id))
    });
    refs.dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
    refs
}

fn runtime_evaluated_acceptance(
    packet: &AgentTaskPacket,
    summary: &crate::TurnSummary,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    tool_executor: &ScopedRuntimeToolExecutor,
) -> (
    Vec<String>,
    Vec<harness_contract::agent::AgentChangeReceipt>,
) {
    let produced_evidence = evidence_refs.iter().any(|evidence| {
        crate::agent_result_validator::is_materialized_durable_evidence(evidence)
            && !packet
                .evidence_refs
                .iter()
                .any(|input| input.evidence_ref == evidence.evidence_ref)
    });
    let mut receipts = tool_executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    receipts.sort_by_key(|receipt| receipt.sequence);
    let changes = materialized_change_receipts(&receipts);
    let observed_paths = receipts
        .iter()
        .flat_map(|receipt| receipt.paths.iter().cloned())
        .collect::<Vec<_>>();
    let output = serde_json::from_str::<serde_json::Value>(&summary.final_answer)
        .ok()
        .and_then(|value| value.as_object().cloned());
    let field_present = |field: harness_contract::team::TeamStructuredOutputField| {
        output
            .as_ref()
            .and_then(|object| object.get(field.as_str()))
            .is_some_and(materialized_json_value)
    };
    let scope_observed =
        |scope: &str, paths: &[String]| paths.iter().any(|path| path_within_scope(path, scope));
    let changes_in_scopes = |scopes: &[String]| {
        !changes.is_empty()
            && changes.iter().all(|change| {
                scopes
                    .iter()
                    .any(|scope| path_within_scope(&change.path, scope))
            })
    };
    let upstream_changes = packet
        .constraints
        .iter()
        .filter_map(|constraint| constraint.strip_prefix("upstream_change_scope:"))
        .filter_map(|value| {
            serde_json::from_str::<harness_contract::agent::AgentChangeReceipt>(value).ok()
        })
        .collect::<Vec<_>>();
    let upstream_evidence = packet
        .evidence_refs
        .iter()
        .any(crate::agent_result_validator::is_materialized_durable_evidence);
    let acceptance = packet_acceptance_contract(packet)
        .unwrap_or_default()
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
                    && scopes
                        .iter()
                        .all(|scope| scope_observed(scope, &observed_paths))
            }
            harness_contract::team::TeamAcceptanceCheck::WorkspaceChange { field, scopes } => {
                produced_evidence && field_present(*field) && changes_in_scopes(scopes)
            }
            harness_contract::team::TeamAcceptanceCheck::SourceVerification { scopes } => {
                produced_evidence
                    && field_present(
                        harness_contract::team::TeamStructuredOutputField::SourceVerification,
                    )
                    && changes_in_scopes(scopes)
                    && changes.iter().all(|change| {
                        has_matching_pre_write_read_receipt(change, &receipts)
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
                    && scopes
                        .iter()
                        .all(|scope| scope_observed(scope, &observed_paths))
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

fn materialized_change_receipts(
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::agent::AgentChangeReceipt> {
    receipts
        .iter()
        .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
        .flat_map(|receipt| {
            receipt.paths.iter().filter_map(|path| {
                let before = receipt.before_digests.get(path).cloned().flatten();
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
        (!require_later_sequence || receipt.sequence > change.write_sequence)
            && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && receipt.paths.contains(&change.path)
            && receipt
                .after_digests
                .get(&change.path)
                .and_then(|digest| digest.as_deref())
                == Some(change.after_sha256.as_str())
    })
}

fn has_matching_pre_write_read_receipt(
    change: &harness_contract::agent::AgentChangeReceipt,
    receipts: &[ScopedToolExecutionReceipt],
) -> bool {
    let Some(before_sha256) = change.before_sha256.as_deref() else {
        // A new file needs a separate typed absence proof, which this
        // contract does not yet expose. Do not call it source-verified.
        return false;
    };
    receipts.iter().any(|receipt| {
        receipt.sequence < change.write_sequence
            && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && receipt.paths.contains(&change.path)
            && receipt
                .after_digests
                .get(&change.path)
                .and_then(|digest| digest.as_deref())
                == Some(before_sha256)
    })
}

fn packet_acceptance_contract(
    packet: &AgentTaskPacket,
) -> Option<Vec<harness_contract::team::TeamAcceptanceRequirement>> {
    packet
        .constraints
        .iter()
        .find_map(|constraint| constraint.strip_prefix("team_acceptance_contract:"))
        .and_then(|value| serde_json::from_str(value).ok())
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
        && (path == scope
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
            paths: vec![path.to_string()],
            before_digests: BTreeMap::from([(path.to_string(), before.map(str::to_string))]),
            after_digests: BTreeMap::from([(path.to_string(), after.map(str::to_string))]),
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
        assert!(has_matching_pre_write_read_receipt(
            &change,
            &read_before_write
        ));
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
        assert!(!has_matching_pre_write_read_receipt(
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
        assert!(has_matching_pre_write_read_receipt(&change, &verified));
        assert!(has_matching_read_receipt(&change, &verified, true));
    }

    struct NoopRuntimeExecutionHost;

    impl crate::RuntimeExecutionHost for NoopRuntimeExecutionHost {
        fn execute_runtime_tool(
            &self,
            _request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            panic!("the capability advertisement test must not execute a tool")
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            _input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor(tool_name)
        }
    }

    struct EchoRuntimeExecutionHost;

    impl crate::RuntimeExecutionHost for EchoRuntimeExecutionHost {
        fn execute_runtime_tool(
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
            }
        }

        fn delegated_tool_effect_descriptor(
            &self,
            tool_name: &str,
            _input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor(tool_name)
        }
    }

    fn test_tool_descriptor(
        tool_name: &str,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        matches!(tool_name, "read_file" | "grep_search").then(|| ToolEffectDescriptor {
            tool_id: tool_name.to_string(),
            descriptor_hash: format!("test-host:{tool_name}"),
            effect_kind: ToolEffectKind::Read,
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![PermissionScope::new(
                PermissionResource::File,
                PermissionOperation::Read,
            )],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
        })
    }

    #[test]
    fn permission_policy_never_escalates_an_unspecified_lease() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy("unknown-lease", &tools);
        assert_eq!(policy.active_mode(), PermissionMode::ReadOnly);
        assert_eq!(
            policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
    }

    #[test]
    fn delegated_agent_step_limit_is_bounded_by_the_context_lease() {
        assert_eq!(agent_model_step_limit(0), None);
        assert_eq!(agent_model_step_limit(8_000), Some(3));
        assert_eq!(agent_model_step_limit(24_000), Some(6));
        assert_eq!(agent_model_step_limit(128_000), Some(6));
    }

    #[test]
    fn workspace_change_contract_retains_the_required_write_scope() {
        let constraints = vec![
            "focus_output_acceptance:implementation, source_verification".to_string(),
            "team_acceptance_contract:[{\"criterion\":\"implementation\",\"check\":{\"kind\":\"workspace_change\",\"scopes\":[\"write:fixtures/target.txt\"]}},{\"criterion\":\"source_verification\",\"check\":{\"kind\":\"source_verification\",\"scopes\":[\"write:fixtures/target.txt\"]}}]".to_string(),
        ];

        assert_eq!(
            focus_acceptance_scopes_from_constraints(&constraints),
            ["write:fixtures/target.txt"]
        );
        let review_constraints = vec![
            "focus_output_acceptance:evidence, review, risks".to_string(),
            "team_acceptance_contract:[{\"criterion\":\"evidence\",\"check\":{\"kind\":\"scoped_evidence\",\"scopes\":[\"read:fixtures/target.txt\",\"write:fixtures/target.txt\"]}}]".to_string(),
        ];
        assert!(focus_acceptance_scopes_from_constraints(&review_constraints).is_empty());
    }

    #[test]
    fn permission_policy_uses_the_explicit_packet_lease() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy("workspace-write", &tools);
        assert_eq!(policy.active_mode(), PermissionMode::WorkspaceWrite);
        assert_eq!(
            policy.required_mode_for("write_file"),
            PermissionMode::WorkspaceWrite
        );
    }

    #[test]
    fn team_tool_boundary_enforces_the_exact_focus_scope() {
        let root = tempfile::tempdir().expect("scoped workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
        std::fs::create_dir_all(root.path().join("crates/gateway")).expect("gateway scope");
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string(), "grep_search".to_string()]),
            session_id: "session".to_string(),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            workspace_root: root.path().to_path_buf(),
            resource_scopes: Some(vec!["read:crates/runtime".to_string()]),
            evaluation_isolated: false,
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
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            workspace_root: root.path().to_path_buf(),
            resource_scopes: Some(vec![
                "read:crates/runtime".to_string(),
                "write:crates/runtime".to_string(),
            ]),
            evaluation_isolated: false,
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

    #[test]
    fn scoped_executor_advertises_only_packet_authorized_tools() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(NoopRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string(), "grep_search".to_string()]),
            session_id: "session".to_string(),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            workspace_root: std::path::PathBuf::from("/workspace"),
            resource_scopes: None,
            evaluation_isolated: false,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };

        assert!(executor.has_registered_tools());
        assert_eq!(
            executor.available_tool_names(),
            vec![
                "ToolSearch".to_string(),
                "grep_search".to_string(),
                "read_file".to_string(),
            ]
        );
        assert!(executor.classify_tool_safety("read_file", "{}").is_some());
        assert!(executor.classify_tool_safety("write_file", "{}").is_none());
        let discovery: harness_contract::tool::ToolDiscoveryReceipt = serde_json::from_str(
            &executor
                .execute("ToolSearch", r#"{"query":"read"}"#)
                .expect("bootstrap search should return the canonical receipt"),
        )
        .expect("canonical discovery receipt");
        assert_eq!(discovery.query, "read");
        assert_eq!(
            discovery.activation_candidates,
            vec!["grep_search", "read_file"]
        );
    }

    #[test]
    fn scoped_executor_propagates_runtime_authorization_for_normal_agent_tools() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string()]),
            session_id: "session".to_string(),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
            workspace_root: std::path::PathBuf::from("/workspace"),
            resource_scopes: None,
            evaluation_isolated: false,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
        };
        let descriptor = executor
            .describe_tool_effect("read_file", &serde_json::json!({"path": "README.md"}))
            .expect("allow-listed delegated tool must describe its effect");
        let authorization = crate::ToolPolicy
            .authorize(&descriptor, "agent-test", PermissionMode::ReadOnly, 30)
            .expect("read tool should be authorized")
            .authorization;
        assert_eq!(
            executor
                .execute_authorized(&authorization, "read_file", r#"{"path":"README.md"}"#)
                .expect("authorized tool should execute"),
            "authorized:read_file"
        );
        assert!(executor
            .execute("read_file", r#"{"path":"README.md"}"#)
            .is_err());
        assert!(executor
            .execute_authorized(&authorization, "write_file", r#"{"path":"README.md"}"#)
            .is_err());
    }

    #[test]
    fn durable_audits_are_promoted_to_agent_evidence_refs() {
        let packet = AgentTaskPacket {
            run_id: "run".into(),
            agent_id: "agent".into(),
            task_id: "task".into(),
            session_id: "session".into(),
            mission_id: None,
            team_id: None,
            graph_id: "graph".into(),
            node_id: "node".into(),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "inspect".into(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: vec![harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::new("upstream", "frame"),
                "sha256:frame",
                1,
                "text/plain",
                "session-event://session/1",
                "session:session",
            )],
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ContextBudgetLeaseRef::new(
                "budget", "agent", "agent", 0, 1,
            ),
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
        };
        let tool_access = harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::new("tool", "tool-1"),
            "sha256:tool",
            1,
            "text/plain",
            "session-event://session/2",
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
            agent_evidence_refs(&packet, &audits)
                .into_iter()
                .map(|reference| reference.evidence_ref.0.id)
                .collect::<Vec<_>>(),
            vec!["tool-1".to_string(), "frame".to_string()]
        );
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
            harness_contract::goal::GoalCompletion::Blocked,
            "provider path exhausted",
        );
        assert_eq!(status, AgentTerminalStatus::Blocked);
        assert_eq!(failure.as_deref(), Some("provider path exhausted"));
    }

    #[test]
    fn delegated_prompt_rejects_simulated_tool_markup() {
        let mut packet = AgentTaskPacket {
            run_id: "run".into(),
            agent_id: "agent".into(),
            task_id: "task".into(),
            session_id: "session".into(),
            mission_id: None,
            team_id: Some("team".into()),
            graph_id: "graph".into(),
            node_id: "node".into(),
            attempt: 1,
            expected_graph_revision: 0,
            objective: "inspect source".into(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ContextBudgetLeaseRef::new(
                "budget", "agent", "agent", 0, 1,
            ),
            binding: None,
            managed_invocation: None,
            idempotency_key: "key".into(),
        };
        let prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
        assert!(prompt.contains("Never write simulated tool syntax"));
        assert!(prompt.contains("If no native tool is authorized, answer directly"));

        packet.objective = "update fixtures/target.txt".into();
        packet.constraints = vec![
            "team_acceptance_contract:[{\"criterion\":\"implementation\",\"check\":{\"kind\":\"workspace_change\",\"field\":\"implementation\",\"scopes\":[\"write:fixtures/target.txt\"]}}]".to_string(),
        ];
        let mutation_prompt = system_prompt(
            &packet,
            std::path::Path::new("/workspace"),
            &["read_file".into(), "write_file".into()],
        )
        .join("\n");
        assert!(mutation_prompt.contains("Read each target at most once before mutation"));
        assert!(mutation_prompt.contains("write:fixtures/target.txt"));
        assert!(mutation_prompt.contains("Repeated reads"));
    }
}
