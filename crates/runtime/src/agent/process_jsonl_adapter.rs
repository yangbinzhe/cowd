use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Stdio};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommand, AgentCommandRejectReason, AgentCommandRequest, AgentReturnPacket, AgentTaskPacket,
};
use sandbox_launcher::{shell_command, SandboxLaunchSpec};
use serde::{Deserialize, Serialize};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessJsonlSpec {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ProcessEnvelope {
    protocol_version: u32,
    sequence: u64,
    run_id: String,
    agent_id: String,
    #[serde(default)]
    result: Option<AgentReturnPacket>,
    #[serde(default)]
    error: Option<String>,
}

struct ActiveProcess {
    child: Child,
    stdin: ChildStdin,
}

#[derive(Default)]
struct ProcessJsonlRegistry {
    specs: Mutex<BTreeMap<String, ProcessJsonlSpec>>,
    lifecycle: Mutex<ProcessJsonlLifecycle>,
}

#[derive(Default)]
struct ProcessJsonlLifecycle {
    starting: BTreeSet<String>,
    active: BTreeMap<String, Arc<Mutex<ActiveProcess>>>,
    pending_cancellation: BTreeSet<String>,
}

struct StartingRunGuard {
    registry: Arc<ProcessJsonlRegistry>,
    run_id: String,
}

impl Drop for StartingRunGuard {
    fn drop(&mut self) {
        let mut lifecycle = self
            .registry
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle.starting.remove(&self.run_id);
        lifecycle.pending_cancellation.remove(&self.run_id);
    }
}

/// A JSONL-only backend. The child only receives/returns protocol envelopes;
/// it is never allowed to write RuntimeEventStore. The adapter owns process
/// handles so commands have real process effects instead of only changing a
/// projection.
#[derive(Clone)]
pub struct ProcessJsonlAdapter {
    registry: Arc<ProcessJsonlRegistry>,
    workspace_root: Arc<PathBuf>,
}

impl ProcessJsonlAdapter {
    #[must_use]
    pub fn for_workspace(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            registry: Arc::new(ProcessJsonlRegistry::default()),
            workspace_root: Arc::new(workspace_root.into()),
        }
    }

    pub fn register(&self, agent_id: impl Into<String>, spec: ProcessJsonlSpec) {
        self.registry
            .specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(agent_id.into(), spec);
    }

    fn command_envelope(request: &AgentCommandRequest) -> Result<String, AgentCommandRejectReason> {
        serde_json::to_string(&serde_json::json!({
            "protocol_version": 1,
            "sequence": 0,
            "kind": "agent_command",
            "command_id": request.command_id,
            "command": request.command,
            "input": request.input,
        }))
        .map(|payload| format!("{payload}\n"))
        .map_err(|_| AgentCommandRejectReason::InvalidInput)
    }
}

#[async_trait]
impl AgentRuntimeBackend for ProcessJsonlAdapter {
    fn kind(&self) -> AgentBackendKind {
        AgentBackendKind::ProcessJsonl
    }

    fn capabilities(&self) -> AgentBackendCapabilities {
        AgentBackendCapabilities::process_jsonl()
    }

    async fn execute(
        &self,
        packet: AgentTaskPacket,
        _selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let spec = self
            .registry
            .specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(packet.agent_id())
            .cloned()
            .ok_or_else(|| "no ProcessJsonl spec is registered for this agent".to_string())?;
        let registry = Arc::clone(&self.registry);
        let workspace_root = Arc::clone(&self.workspace_root);
        let run_id = packet.run_id().to_string();
        {
            let mut lifecycle = registry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if lifecycle.starting.contains(&run_id) || lifecycle.active.contains_key(&run_id) {
                return Err("ProcessJsonl run is already active".to_string());
            }
            lifecycle.starting.insert(run_id.clone());
        }
        let starting = StartingRunGuard {
            registry: Arc::clone(&registry),
            run_id,
        };
        tokio::task::spawn_blocking(move || {
            let _starting = starting;
            execute_child(&registry, &workspace_root, &spec, &packet)
        })
        .await
        .map_err(|error| format!("process-jsonl worker join failed: {error}"))?
    }

    async fn command(
        &self,
        handle: &AgentRunHandle,
        request: &AgentCommandRequest,
    ) -> Result<(), AgentCommandRejectReason> {
        let active = {
            let mut lifecycle = self
                .registry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(active) = lifecycle.active.get(&handle.run_id).cloned() {
                active
            } else if lifecycle.starting.contains(&handle.run_id)
                && matches!(
                    request.command,
                    AgentCommand::Cancel | AgentCommand::Shutdown
                )
            {
                lifecycle.pending_cancellation.insert(handle.run_id.clone());
                return Ok(());
            } else {
                return Err(AgentCommandRejectReason::UnsupportedByBackend);
            }
        };
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match request.command {
            AgentCommand::Pause | AgentCommand::Resume => {
                Err(AgentCommandRejectReason::UnsupportedByBackend)
            }
            AgentCommand::Cancel | AgentCommand::Shutdown => terminate_process_tree(&mut active)
                .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend),
            AgentCommand::SendInput | AgentCommand::Interrupt => {
                let payload = Self::command_envelope(request)?;
                active
                    .stdin
                    .write_all(payload.as_bytes())
                    .and_then(|()| active.stdin.flush())
                    .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)
            }
        }
    }
}

fn execute_child(
    registry: &ProcessJsonlRegistry,
    workspace_root: &PathBuf,
    spec: &ProcessJsonlSpec,
    packet: &AgentTaskPacket,
) -> Result<AgentReturnPacket, String> {
    if spec.command.trim().is_empty() {
        return Err("ProcessJsonl command is empty".into());
    }
    let invocation = std::iter::once(spec.command.as_str())
        .chain(spec.args.iter().map(String::as_str))
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ");
    let mut launch_spec = SandboxLaunchSpec::workspace(workspace_root);
    launch_spec.working_directory = Some(workspace_root.clone());
    let prepared = shell_command(&format!("exec {invocation}"), &launch_spec)
        .map_err(|error| format!("prepare hardened ProcessJsonl sandbox failed: {error}"))?;
    let mut command = prepared.into_command();
    // The sandbox launcher may fork an inner namespace process. Give the
    // entire launch tree its own process group before spawn so cancellation
    // cannot leave a re-parented descendant holding JSONL pipes open.
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to start ProcessJsonl worker: {error}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| "ProcessJsonl worker stdin is unavailable".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "ProcessJsonl worker stdout is unavailable".to_string())?;
    let request = serde_json::json!({
        "protocol_version": 1,
        "sequence": 1,
        "kind": "agent_task",
        "packet": packet,
    });
    stdin
        .write_all(
            format!(
                "{}\n",
                serde_json::to_string(&request).map_err(|error| error.to_string())?
            )
            .as_bytes(),
        )
        .and_then(|()| stdin.flush())
        .map_err(|error| format!("failed to write ProcessJsonl request: {error}"))?;

    let active = Arc::new(Mutex::new(ActiveProcess { child, stdin }));
    let cancel_before_activation = {
        let mut lifecycle = registry
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        lifecycle
            .active
            .insert(packet.run_id().to_string(), Arc::clone(&active));
        lifecycle.pending_cancellation.remove(packet.run_id())
    };
    if cancel_before_activation {
        let mut process = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = terminate_process_tree(&mut process);
    }

    let result = read_process_result(stdout, packet);
    let exit_status = active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .child
        .wait()
        .map_err(|error| format!("failed to wait for ProcessJsonl worker: {error}"));
    registry
        .lifecycle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .active
        .remove(packet.run_id());

    let status = exit_status?;
    if !status.success() {
        return Err(format!("ProcessJsonl worker exited with {status}"));
    }
    result
}

#[cfg(unix)]
fn terminate_process_tree(active: &mut ActiveProcess) -> std::io::Result<()> {
    let process_group = active.child.id() as i32;
    // SAFETY: `execute_child` creates the child as leader of a fresh process
    // group. A negative PID therefore targets only this adapter-owned launch
    // tree; the live `Child` handle prevents confusing it with an arbitrary
    // unrelated process in normal operation.
    let result = unsafe { libc::kill(-process_group, libc::SIGKILL) };
    if result == 0 {
        Ok(())
    } else {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            active.child.kill().or(Ok(()))
        } else {
            Err(error)
        }
    }
}

#[cfg(not(unix))]
fn terminate_process_tree(active: &mut ActiveProcess) -> std::io::Result<()> {
    active.child.kill()
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn read_process_result(
    stdout: impl std::io::Read,
    packet: &AgentTaskPacket,
) -> Result<AgentReturnPacket, String> {
    let mut expected_sequence = 1;
    let mut terminal = None;
    for line in BufReader::new(stdout).lines() {
        let line = line.map_err(|error| format!("failed to read ProcessJsonl output: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let envelope: ProcessEnvelope = serde_json::from_str(&line)
            .map_err(|error| format!("malformed ProcessJsonl envelope: {error}"))?;
        if envelope.protocol_version != 1
            || envelope.sequence != expected_sequence
            || envelope.run_id != packet.run_id()
            || envelope.agent_id != packet.agent_id()
        {
            return Err("ProcessJsonl envelope binding or sequence is invalid".into());
        }
        expected_sequence = expected_sequence.saturating_add(1);
        if let Some(result) = envelope.result {
            if terminal.replace(result).is_some() {
                return Err("ProcessJsonl emitted duplicate terminal result".into());
            }
        }
        if let Some(error) = envelope.error {
            return Err(format!("ProcessJsonl worker reported error: {error}"));
        }
    }
    let mut terminal = terminal
        .ok_or_else(|| "ProcessJsonl worker exited without a terminal result".to_string())?;
    // A child process may report business output, but it cannot mint Runtime
    // observation truth. Until its tool effects cross the canonical ToolHost
    // receipt boundary, all typed evidence obligations remain unresolved.
    terminal.observed_acceptance = crate::path_identity::evaluate_observed_acceptance(
        &packet.required_acceptance,
        Vec::new(),
        Vec::new(),
    );
    terminal.runtime_observed_resource_scopes.clear();
    Ok(terminal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::context::ContextBudgetLeaseRef;

    fn task() -> AgentTaskPacket {
        AgentTaskPacket {
            assignment: crate::test_support::agent_assignment(
                None,
                "process-agent-1",
                "process-run-1",
                "process-task-1",
                "process-session-1",
                "process-mission-1",
                None,
                "process-graph-1",
                "process-node-1",
            ),
            attempt: 1,
            expected_graph_revision: 1,
            policy_revision: 1,
            objective: "wait for cancellation".into(),
            required_acceptance: Default::default(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "test".into(),
            budget_lease: ContextBudgetLeaseRef::new(
                "budget-process-1",
                "process-agent-1",
                "agent",
                1000,
                1,
            ),
            binding: None,
            managed_invocation: None,
            idempotency_key: "process-idempotency-1".into(),
        }
    }

    fn completed_return(packet: &AgentTaskPacket) -> AgentReturnPacket {
        AgentReturnPacket {
            run_id: packet.run_id().to_string(),
            agent_id: packet.agent_id().to_string(),
            task_id: packet.task_id().to_string(),
            session_id: packet.session_id().to_string(),
            mission_id: packet.mission_id().to_string(),
            team_id: packet.team_id().map(str::to_string),
            graph_id: packet.graph_id().to_string(),
            node_id: packet.node_id().to_string(),
            attempt: packet.attempt,
            expected_graph_revision: packet.expected_graph_revision,
            status: harness_contract::agent::AgentTerminalStatus::Completed,
            outcome: "child says complete".to_string(),
            observed_acceptance: harness_contract::context::ObservedAcceptance {
                satisfied_criteria: vec!["must-be-runtime-verified".to_string()],
                observed_evidence: vec![harness_contract::context::ObservedEvidence {
                    obligation_id: "forged".to_string(),
                    target: harness_contract::context::EvidenceTargetIdentity::Network {
                        endpoint: "*".to_string(),
                    },
                    observed_at_sequence: 99,
                    tool_name: "forged".to_string(),
                    provenance:
                        harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
                    evidence_ref: None,
                    workspace_prior_state: None,
                }],
                unresolved_obligation_ids: Vec::new(),
            },
            acceptance: vec!["must-be-runtime-verified".to_string()],
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            cached_tokens: 0,
            model: "child".to_string(),
            provider: "external".to_string(),
            tool_calls: 1,
            duplicate_tool_calls: 0,
            max_tool_concurrency_observed: 1,
            parallel_tool_batches: 0,
            runtime_write_attempt_paths: Vec::new(),
            runtime_observed_resource_scopes: vec!["network:*".to_string()],
            failure: None,
        }
    }

    #[test]
    fn process_child_cannot_self_assert_acceptance_or_observation_truth() {
        let mut packet = task();
        packet.required_acceptance = harness_contract::context::RequiredAcceptance {
            criteria: vec!["must-be-runtime-verified".to_string()],
            evidence_obligations: vec![harness_contract::context::EvidenceObligation {
                obligation_id: "network-required".to_string(),
                kind: harness_contract::context::EvidenceObligationKind::NetworkEvidence,
                target: harness_contract::context::EvidenceTargetIdentity::Network {
                    endpoint: "*".to_string(),
                },
            }],
        };
        let envelope = serde_json::json!({
            "protocol_version": 1,
            "sequence": 1,
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "result": completed_return(&packet),
        });
        let decoded = read_process_result(format!("{envelope}\n").as_bytes(), &packet)
            .expect("protocol envelope");

        assert!(decoded.observed_acceptance.satisfied_criteria.is_empty());
        assert!(decoded.observed_acceptance.observed_evidence.is_empty());
        assert_eq!(
            decoded.observed_acceptance.unresolved_obligation_ids,
            vec!["network-required".to_string()]
        );
        assert!(decoded.runtime_observed_resource_scopes.is_empty());
    }

    #[tokio::test]
    async fn cancel_kills_the_active_process_instead_of_only_acknowledging() {
        let adapter = ProcessJsonlAdapter::for_workspace(std::env::temp_dir());
        let packet = task();
        let mut command = std::process::Command::new("sh");
        command.args(["-c", "exec sleep 30"]);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("test process");
        let stdin = child.stdin.take().expect("test process stdin");
        let active = Arc::new(Mutex::new(ActiveProcess { child, stdin }));
        {
            adapter
                .registry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .active
                .insert(packet.run_id().to_string(), Arc::clone(&active));
        }
        let receipt = adapter
            .command(
                &AgentRunHandle {
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    backend: AgentBackendKind::ProcessJsonl,
                    revision: 1,
                    status: harness_contract::agent::AgentStatus::Running,
                },
                &AgentCommandRequest {
                    command_id: "cancel-process-1".into(),
                    agent_id: packet.agent_id().to_string(),
                    expected_revision: 1,
                    command: AgentCommand::Cancel,
                    input: None,
                },
            )
            .await;
        assert!(receipt.is_ok());
        let status = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .child
            .wait()
            .expect("cancelled process is reaped");
        assert!(!status.success());
        adapter
            .registry
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .remove(packet.run_id());
        assert!(adapter
            .registry
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active
            .is_empty());
    }

    #[tokio::test]
    async fn cancel_is_retained_while_process_jsonl_is_still_starting() {
        let adapter = ProcessJsonlAdapter::for_workspace(std::env::temp_dir());
        let packet = task();
        {
            adapter
                .registry
                .lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .starting
                .insert(packet.run_id().to_string());
        }
        let receipt = adapter
            .command(
                &AgentRunHandle {
                    run_id: packet.run_id().to_string(),
                    agent_id: packet.agent_id().to_string(),
                    backend: AgentBackendKind::ProcessJsonl,
                    revision: 1,
                    status: harness_contract::agent::AgentStatus::Running,
                },
                &AgentCommandRequest {
                    command_id: "cancel-starting-process-1".into(),
                    agent_id: packet.agent_id().to_string(),
                    expected_revision: 1,
                    command: AgentCommand::Cancel,
                    input: None,
                },
            )
            .await;
        assert!(receipt.is_ok());
        assert!(adapter
            .registry
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending_cancellation
            .contains(packet.run_id()));
    }
}
