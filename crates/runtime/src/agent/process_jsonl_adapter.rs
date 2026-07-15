use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Stdio};
use std::sync::{Arc, Mutex};

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
    active: Mutex<BTreeMap<String, Arc<Mutex<ActiveProcess>>>>,
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
    pub fn new() -> Self {
        Self::for_workspace(std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
    }

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
            .get(&packet.agent_id)
            .cloned()
            .ok_or_else(|| "no ProcessJsonl spec is registered for this agent".to_string())?;
        let registry = Arc::clone(&self.registry);
        let workspace_root = Arc::clone(&self.workspace_root);
        tokio::task::spawn_blocking(move || {
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
        let active = self
            .registry
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&handle.run_id)
            .cloned()
            .ok_or(AgentCommandRejectReason::UnsupportedByBackend)?;
        let mut active = active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match request.command {
            AgentCommand::Pause | AgentCommand::Resume => {
                Err(AgentCommandRejectReason::UnsupportedByBackend)
            }
            AgentCommand::Cancel | AgentCommand::Shutdown => active
                .child
                .kill()
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
    let mut child = prepared
        .into_command()
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
    {
        let mut processes = registry
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if processes.contains_key(&packet.run_id) {
            return Err("ProcessJsonl run is already active".into());
        }
        processes.insert(packet.run_id.clone(), Arc::clone(&active));
    }

    let result = read_process_result(stdout, packet);
    let exit_status = active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .child
        .wait()
        .map_err(|error| format!("failed to wait for ProcessJsonl worker: {error}"));
    registry
        .active
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&packet.run_id);

    let status = exit_status?;
    if !status.success() {
        return Err(format!("ProcessJsonl worker exited with {status}"));
    }
    result
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
            || envelope.run_id != packet.run_id
            || envelope.agent_id != packet.agent_id
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
    terminal.ok_or_else(|| "ProcessJsonl worker exited without a terminal result".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::context::ContextBudgetLeaseRef;

    fn task() -> AgentTaskPacket {
        AgentTaskPacket {
            run_id: "process-run-1".into(),
            agent_id: "process-agent-1".into(),
            task_id: "process-task-1".into(),
            session_id: "process-session-1".into(),
            mission_id: None,
            team_id: None,
            graph_id: "process-graph-1".into(),
            node_id: "process-node-1".into(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "wait for cancellation".into(),
            acceptance: Vec::new(),
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_lease: "read_only".into(),
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

    #[tokio::test]
    async fn cancel_kills_the_active_process_instead_of_only_acknowledging() {
        let adapter = ProcessJsonlAdapter::new();
        let packet = task();
        adapter.register(
            packet.agent_id.clone(),
            ProcessJsonlSpec {
                command: "sh".into(),
                args: vec!["-c".into(), "exec sleep 30".into()],
            },
        );
        let execution = {
            let adapter = adapter.clone();
            let packet = packet.clone();
            tokio::spawn(async move {
                adapter
                    .execute(
                        packet,
                        AgentModelSelection {
                            model: "test".into(),
                            provider: "test".into(),
                            registry_revision: 1,
                        },
                    )
                    .await
            })
        };
        for _ in 0..50 {
            if adapter
                .registry
                .active
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&packet.run_id)
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let receipt = adapter
            .command(
                &AgentRunHandle {
                    run_id: packet.run_id.clone(),
                    agent_id: packet.agent_id.clone(),
                    backend: AgentBackendKind::ProcessJsonl,
                    revision: 1,
                    status: harness_contract::agent::AgentStatus::Running,
                },
                &AgentCommandRequest {
                    command_id: "cancel-process-1".into(),
                    agent_id: packet.agent_id.clone(),
                    expected_revision: 1,
                    command: AgentCommand::Cancel,
                    input: None,
                },
            )
            .await;
        assert!(receipt.is_ok());
        let result = execution.await.expect("execution task");
        assert!(result.is_err());
        assert!(adapter
            .registry
            .active
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }
}
