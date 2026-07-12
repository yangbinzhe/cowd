use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};

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
}

#[derive(Clone)]
struct ActiveInProcessRun {
    cancellation: crate::CancellationToken,
    session_id: String,
    input_stream: crate::SessionInputStream,
}

impl InProcessAgentWorker {
    #[must_use]
    pub fn new(services: Weak<RuntimeServices>) -> Self {
        Self {
            services,
            active_runs: Mutex::new(BTreeMap::new()),
        }
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
        let services = self
            .services
            .upgrade()
            .ok_or_else(|| "AgentRuntime is not bound to RuntimeServices".to_string())?;
        let host = services.tool_execution_host().cloned().ok_or_else(|| {
            "RuntimeServices has no ToolHost for the in-process agent".to_string()
        })?;
        let allowed_tools = packet
            .allowed_tools
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        let tool_names = allowed_tools.iter().cloned().collect::<Vec<_>>();
        let tool_definitions = host.delegated_tool_definitions(&tool_names);
        let tool_executor = Arc::new(ScopedRuntimeToolExecutor {
            host,
            allowed_tools: allowed_tools.clone(),
            session_id: packet.session_id.clone(),
            model_lease: selection.model.clone(),
            execution_id: packet.graph_id.clone(),
            node_id: packet.node_id.clone(),
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
            skill_profiles: Vec::new(),
            agent_skill_profile: harness_contract::skill::AgentSkillProfile::default(),
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
        let input_stream = runtime.session_input_stream();
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                packet.run_id.clone(),
                ActiveInProcessRun {
                    cancellation: cancellation.clone(),
                    session_id: child_session_id,
                    input_stream,
                },
            );
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
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&packet.run_id);
        let summary = result.map_err(|error| format!("in-process agent turn failed: {error}"))?;
        let evidence_refs =
            agent_evidence_refs(&packet, &summary.context_turn_report.audit_projections);
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
            acceptance: packet.acceptance,
            evidence_refs,
            changes: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: u64::from(summary.usage.input_tokens),
            output_tokens: u64::from(summary.usage.output_tokens),
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
            | harness_contract::agent::AgentCommand::Shutdown => self
                .active_runs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&handle.run_id)
                .map(|active| active.cancellation.clone())
                .map(|token| token.cancel())
                .ok_or(harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend),
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
        let request = RuntimeToolExecutionRequest {
            idempotency_key: format!(
                "agent-tool:{tool_name}:{}",
                crate::tool_invocation::now_ms()
            ),
            tool_use_id: format!("agent-tool:{}", uuid::Uuid::new_v4()),
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            category: crate::ToolSafetyCategory::from_tool_name(tool_name),
            session_id: Some(self.session_id.clone()),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
        };
        let outcome = self.host.execute_runtime_tool(&request);
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => Ok(outcome.output.unwrap_or_default()),
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
        descriptors.extend(self.allowed_tools.iter().map(|tool_name| {
            let descriptor = scoped_tool_effect_descriptor(tool_name);
            ToolDescriptorRef {
                canonical_id: tool_name.clone(),
                display_name: tool_name.clone(),
                source: "delegated-agent".to_string(),
                schema_hash: descriptor.descriptor_hash,
                required_permission: descriptor.required_permission,
                permission_source: "agent task packet allow-list".to_string(),
                health: ToolDescriptorHealth::Healthy,
            }
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
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.allowed_tools
            .contains(tool_name)
            .then(|| scoped_tool_effect_descriptor(tool_name))
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
        self.execute(tool_name, input)
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

fn scoped_tool_effect_descriptor(tool_name: &str) -> harness_contract::tool::ToolEffectDescriptor {
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    let safety = crate::ToolSafetyCategory::from_tool_name(tool_name);
    let (effect_kind, idempotency, required_permission, scope, approval_class) = match safety {
        crate::ToolSafetyCategory::ReadOnly => (
            ToolEffectKind::Read,
            ToolIdempotency::Idempotent,
            ToolPermissionMode::ReadOnly,
            PermissionScope::new(PermissionResource::File, PermissionOperation::Read),
            ToolApprovalClass::None,
        ),
        crate::ToolSafetyCategory::WriteLocal => (
            ToolEffectKind::Write,
            ToolIdempotency::IdempotentWithKey,
            ToolPermissionMode::WorkspaceWrite,
            PermissionScope::new(PermissionResource::File, PermissionOperation::Write),
            ToolApprovalClass::Policy,
        ),
        crate::ToolSafetyCategory::Network => (
            ToolEffectKind::Network,
            ToolIdempotency::Unknown,
            ToolPermissionMode::DangerFullAccess,
            PermissionScope::new(PermissionResource::Network, PermissionOperation::Execute),
            ToolApprovalClass::Policy,
        ),
        crate::ToolSafetyCategory::Destructive => (
            ToolEffectKind::Destructive,
            ToolIdempotency::Unknown,
            ToolPermissionMode::DangerFullAccess,
            PermissionScope::new(PermissionResource::Tool, PermissionOperation::Execute),
            ToolApprovalClass::User,
        ),
    };
    ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: format!("delegated-agent:{tool_name}:{effect_kind:?}"),
        effect_kind,
        idempotency,
        scopes: vec![scope],
        required_permission,
        approval_class,
        uses_network: matches!(safety, crate::ToolSafetyCategory::Network),
        spawns_process: matches!(safety, crate::ToolSafetyCategory::Destructive),
        mutates_packages: false,
        mutates_system: matches!(safety, crate::ToolSafetyCategory::Destructive),
    }
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
            policy.with_tool_requirement(tool, mode)
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
    ];
    if !packet.constraints.is_empty() {
        prompt.push(format!("Constraints: {}", packet.constraints.join("; ")));
    }
    if !packet.acceptance.is_empty() {
        prompt.push(format!("Acceptance: {}", packet.acceptance.join("; ")));
    }
    if !tool_names.is_empty() {
        prompt.push(format!(
            "Authorized tool contracts are available natively: {}. When the objective asks for source, workspace, file, or current-state evidence, use an authorized read-only tool and cite the resulting paths/receipts; do not substitute prior model knowledge.",
            tool_names.join(", ")
        ));
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use harness_contract::agent::AgentCommand;
    use harness_contract::turn::TurnId;

    struct NoopRuntimeExecutionHost;

    impl crate::RuntimeExecutionHost for NoopRuntimeExecutionHost {
        fn execute_runtime_tool(
            &self,
            _request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            panic!("the capability advertisement test must not execute a tool")
        }
    }

    struct EchoRuntimeExecutionHost;

    impl crate::RuntimeExecutionHost for EchoRuntimeExecutionHost {
        fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
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
    }

    #[test]
    fn permission_policy_never_escalates_an_unspecified_lease() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy("unknown-lease", &tools);
        assert_eq!(policy.active_mode(), PermissionMode::ReadOnly);
    }

    #[test]
    fn permission_policy_uses_the_explicit_packet_lease() {
        let tools = BTreeSet::from(["write_file".to_string()]);
        let policy = permission_policy("workspace-write", &tools);
        assert_eq!(policy.active_mode(), PermissionMode::WorkspaceWrite);
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
    fn scoped_executor_requires_runtime_authorization_for_normal_agent_tools() {
        let executor = ScopedRuntimeToolExecutor {
            host: Arc::new(EchoRuntimeExecutionHost),
            allowed_tools: BTreeSet::from(["read_file".to_string()]),
            session_id: "session".to_string(),
            model_lease: "model".to_string(),
            execution_id: "graph".to_string(),
            node_id: "node".to_string(),
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
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ContextBudgetLeaseRef::new(
                "budget", "agent", "agent", 0, 1,
            ),
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
        let packet = AgentTaskPacket {
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
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
            model_lease: "model".into(),
            budget_lease: harness_contract::context::ContextBudgetLeaseRef::new(
                "budget", "agent", "agent", 0, 1,
            ),
            idempotency_key: "key".into(),
        };
        let prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
        assert!(prompt.contains("Never write simulated tool syntax"));
        assert!(prompt.contains("If no native tool is authorized, answer directly"));
    }
}
