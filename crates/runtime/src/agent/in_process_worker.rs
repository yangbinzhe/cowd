use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommandRequest, AgentInput, AgentReturnPacket, AgentTaskPacket, AgentTerminalStatus,
};
use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};

use crate::{
    PermissionMode, PermissionPolicy, ProviderToolDefinition, RuntimeExecutionHost,
    RuntimeServices, RuntimeToolExecutionRequest, RuntimeToolExecutionStatus, Session,
    SharedPrompter, StandardRuntimeHost, StandardRuntimeHostConfig, ToolError, ToolExecutor,
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
        let tool_executor = Arc::new(ScopedRuntimeToolExecutor {
            host,
            allowed_tools: allowed_tools.clone(),
        });
        let tool_definitions = allowed_tools
            .iter()
            .map(|name| ProviderToolDefinition {
                name: name.clone(),
                description: Some("Task-authorized runtime tool".into()),
                input_schema: serde_json::json!({"type":"object"}),
            })
            .collect::<Vec<_>>();
        let max_iterations = packet
            .constraints
            .iter()
            .find_map(|constraint| constraint.strip_prefix("max_iterations:"))
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(32);
        let policy = permission_policy(&packet.permission_lease, &allowed_tools);
        let cancellation = crate::CancellationToken::new();
        let child_session = Session::new();
        let child_session_id = child_session.session_id.clone();
        let host = StandardRuntimeHost::new(StandardRuntimeHostConfig {
            runtime_services: Arc::clone(&services),
            session: child_session,
            provider_registry: Arc::clone(services.provider_registry()),
            model: selection.model.clone(),
            tool_definitions: tool_definitions.clone(),
            tool_executor: Arc::clone(&tool_executor),
            permission_policy: policy,
            system_prompt: system_prompt(&packet),
            feature_config: crate::RuntimeFeatureConfig::default(),
            emit_output: false,
            stream_callback: None,
            tool_callback: None,
            model_context_window: None,
            session_store: None,
            hook_progress_reporter: None,
            external_context_items: Vec::new(),
            skill_profiles: Vec::new(),
            agent_skill_profile: harness_contract::skill::AgentSkillProfile::default(),
        });
        let mut runtime = match host {
            Ok(runtime) => runtime,
            Err(error) => {
                return Err(format!(
                    "failed to initialize in-process agent host: {error}"
                ));
            }
        };
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
        runtime.set_max_iterations(max_iterations);
        runtime.install_turn_control(cancellation, crate::HookAbortSignal::default());
        let result = runtime
            .submit_turn(&packet.objective, &SharedPrompter::none())
            .await;
        self.active_runs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&packet.run_id);
        let summary = result.map_err(|error| format!("in-process agent turn failed: {error}"))?;
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
            status: AgentTerminalStatus::Completed,
            outcome: summary.final_answer,
            acceptance: packet.acceptance,
            evidence_refs: packet.evidence_refs,
            changes: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: u64::from(summary.usage.input_tokens),
            output_tokens: u64::from(summary.usage.output_tokens),
            model: selection.model,
            provider: selection.provider,
            tool_calls: summary.tool_results.len() as u64,
            failure: None,
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
}

impl ToolExecutor for ScopedRuntimeToolExecutor {
    fn execute(&self, tool_name: &str, input: &str) -> Result<String, ToolError> {
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

fn system_prompt(packet: &AgentTaskPacket) -> Vec<String> {
    let mut prompt = vec![
        "You are a delegated Cowd agent. Return an evidence-backed result for the assigned objective.".into(),
        format!("Objective: {}", packet.objective),
    ];
    if !packet.constraints.is_empty() {
        prompt.push(format!("Constraints: {}", packet.constraints.join("; ")));
    }
    if !packet.acceptance.is_empty() {
        prompt.push(format!("Acceptance: {}", packet.acceptance.join("; ")));
    }
    prompt
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use harness_contract::agent::AgentCommand;
    use harness_contract::turn::TurnId;

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
}
