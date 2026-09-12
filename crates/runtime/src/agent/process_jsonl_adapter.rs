use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, Weak};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use async_trait::async_trait;
use harness_contract::agent::{
    AgentCommand, AgentCommandRejectReason, AgentCommandRequest, AgentReturnPacket, AgentTaskPacket,
};
use harness_contract::context::ArtifactWriteDescriptor;
use sandbox_launcher::{program_command_with_args, SandboxLaunchSpec, SandboxWorkspaceAccess};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::agent_model_selector::AgentModelSelection;
use crate::agent_run_handle::{AgentBackendCapabilities, AgentBackendKind, AgentRunHandle};
use crate::agent_runtime::AgentRuntimeBackend;
use crate::config::AgentProcessTransportLimits;
use crate::{agent_in_process_worker::ProcessJsonlToolSession, RuntimeServices};

/// V2 permits arbitrarily large business content only through the content
/// plane. Control/action JSONL frames stay bounded so one broken worker cannot
/// consume unbounded Runtime memory while a pipe is being drained.
const PROCESS_JSONL_PROTOCOL_VERSION: u32 = 2;
#[cfg(test)]
const MAX_PROCESS_FRAME_BYTES: usize = 256 * 1024;
/// Bounds simultaneous staging files/handles, not total content or completed uploads.
#[cfg(test)]
const MAX_PROCESS_PENDING_CONTENT_UPLOADS: usize = 32;
#[cfg(test)]
const MAX_PROCESS_PENDING_COMMANDS: usize = 32;
#[cfg(test)]
const MAX_PROCESS_CACHED_TOOL_RESPONSES: usize = 32;
/// A worker that never proves the V2 handshake must not retain an execution
/// slot indefinitely. This protects only the transport admission boundary;
/// it is not a business-task or model-thinking budget.

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessJsonlSpec {
    /// Stable identifier from the Runtime-approved command manifest.
    pub command_ref: String,
    /// Digest of this exact command manifest. It is bound into the Agent
    /// Definition and prevents a mutable command label selecting new code for
    /// an already compiled packet.
    pub command_digest: String,
    /// A workspace-contained executable. Runtime never interpolates this into
    /// a model-controlled shell command; `args` remains an exact argv vector.
    pub executable: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_directory: Option<String>,
    /// `NAME -> env:HOST_VARIABLE` references. Values are resolved only in
    /// the launch path and never serialized, logged, or exposed to a model.
    #[serde(default)]
    pub environment_refs: BTreeMap<String, String>,
    #[serde(default)]
    pub sandbox_profile: crate::config::AgentExecutorSandboxProfile,
    #[serde(default)]
    pub transport_limits: AgentProcessTransportLimits,
}

impl ProcessJsonlSpec {
    #[must_use]
    pub fn new(
        command_ref: impl Into<String>,
        executable: impl Into<String>,
        args: Vec<String>,
    ) -> Self {
        let command_ref = command_ref.into();
        let executable = executable.into();
        let command_digest = Self::digest_for(
            &command_ref,
            &executable,
            &args,
            None,
            &BTreeMap::new(),
            crate::config::AgentExecutorSandboxProfile::default(),
            &AgentProcessTransportLimits::default(),
        );
        Self {
            command_ref,
            command_digest,
            executable,
            args,
            working_directory: None,
            environment_refs: BTreeMap::new(),
            sandbox_profile: crate::config::AgentExecutorSandboxProfile::default(),
            transport_limits: AgentProcessTransportLimits::default(),
        }
    }

    #[must_use]
    pub fn from_config(config: &crate::config::AgentExecutorCommandConfig) -> Self {
        Self {
            command_ref: config.command_ref.clone(),
            command_digest: config.manifest_digest.clone(),
            executable: config.executable.clone(),
            args: config.args.clone(),
            working_directory: config.working_directory.clone(),
            environment_refs: config.environment_refs.clone(),
            sandbox_profile: config.sandbox_profile,
            transport_limits: config.transport_limits,
        }
    }

    #[must_use]
    pub fn digest_for(
        command_ref: &str,
        executable: &str,
        args: &[String],
        working_directory: Option<&str>,
        environment_refs: &BTreeMap<String, String>,
        sandbox_profile: crate::config::AgentExecutorSandboxProfile,
        transport_limits: &AgentProcessTransportLimits,
    ) -> String {
        crate::config::process_executor_manifest_digest(
            command_ref,
            executable,
            args,
            working_directory,
            environment_refs,
            sandbox_profile,
            transport_limits,
        )
    }

    fn validate(&self) -> Result<(), String> {
        self.transport_limits.validate()?;
        if self.command_ref.trim().is_empty() || self.executable.trim().is_empty() {
            return Err(
                "ProcessJsonl command manifest has an empty reference or executable".to_string(),
            );
        }
        if self.executable.contains('\0') || self.args.iter().any(|arg| arg.contains('\0')) {
            return Err("ProcessJsonl command manifest contains a NUL argument".to_string());
        }
        if self
            .working_directory
            .as_deref()
            .is_some_and(|working_directory| working_directory.contains('\0'))
        {
            return Err(
                "ProcessJsonl command manifest contains a NUL working directory".to_string(),
            );
        }
        for (name, reference) in &self.environment_refs {
            if !valid_environment_name(name) || !valid_environment_reference(reference) {
                return Err(
                    "ProcessJsonl command manifest has an invalid environment reference"
                        .to_string(),
                );
            }
        }
        let expected = Self::digest_for(
            &self.command_ref,
            &self.executable,
            &self.args,
            self.working_directory.as_deref(),
            &self.environment_refs,
            self.sandbox_profile,
            &self.transport_limits,
        );
        if self.command_digest != expected {
            return Err(
                "ProcessJsonl command manifest digest does not match command content".to_string(),
            );
        }
        Ok(())
    }

    fn resolve_environment(&self) -> Result<Vec<(String, String)>, String> {
        self.environment_refs
            .iter()
            .map(|(name, reference)| {
                let variable = reference
                    .strip_prefix("env:")
                    .ok_or_else(|| "invalid ProcessJsonl environment reference".to_string())?;
                let value = std::env::var(variable).map_err(|_| {
                    format!(
                        "ProcessJsonl environment reference `{reference}` is unavailable for `{name}`"
                    )
                })?;
                if value.contains('\0') {
                    return Err(format!(
                        "ProcessJsonl environment reference `{reference}` contains a NUL byte"
                    ));
                }
                Ok((name.clone(), value))
            })
            .collect()
    }
}

fn valid_environment_name(name: &str) -> bool {
    !name.starts_with("COWD_")
        && name.chars().enumerate().all(|(index, character)| {
            matches!(
                (index, character),
                (0, 'A'..='Z' | 'a'..='z' | '_')
                    | (_, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_')
            )
        })
}

fn valid_environment_reference(reference: &str) -> bool {
    reference
        .strip_prefix("env:")
        .is_some_and(|name| !name.is_empty() && valid_environment_name(name))
}

#[derive(Debug, Deserialize)]
struct ProcessEnvelope {
    protocol_version: u32,
    sequence: u64,
    run_id: String,
    agent_id: String,
    #[serde(default)]
    manifest_digest: Option<String>,
    #[serde(default)]
    focus: Option<ProcessFocus>,
    #[serde(default)]
    capabilities: Option<ProcessCapabilities>,
    #[serde(default)]
    model_control: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    request_id: Option<String>,
    #[serde(default)]
    command_id: Option<String>,
    #[serde(default)]
    expected_run_revision: Option<u64>,
    #[serde(default)]
    accepted: Option<bool>,
    #[serde(default)]
    delivery_id: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
    #[serde(default)]
    input: Option<serde_json::Value>,
    #[serde(default)]
    upload_id: Option<String>,
    #[serde(default)]
    offset: Option<u64>,
    #[serde(default)]
    chunk: Option<String>,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    media_type: Option<String>,
    #[serde(default)]
    original_name: Option<String>,
    #[serde(default)]
    result: Option<AgentReturnPacket>,
    #[serde(default)]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ProcessFocus {
    graph_id: String,
    node_id: String,
    attempt: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ProcessCapabilities {
    #[serde(default)]
    commands: BTreeSet<String>,
    #[serde(default)]
    supports_recovery: bool,
    #[serde(default)]
    evidence_mode: Option<String>,
    #[serde(default)]
    model_control: Option<String>,
}

struct ActiveProcess {
    // Process lifecycle and stdin are independent locks. A blocked external
    // stdin write must never prevent a cancellation from killing the process
    // tree, nor may Runtime hold a process lifecycle lock while writing.
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    /// Runtime-to-child stream has its own sequence independent from the
    /// child-to-Runtime stream. Only protocol-ready runs may receive normal
    /// input; cancellation remains available before readiness.
    next_runtime_sequence: Mutex<u64>,
    protocol_ready: AtomicBool,
    terminal_received: AtomicBool,
    command_slots: Arc<tokio::sync::Semaphore>,
    command_writer: Arc<tokio::sync::Mutex<()>>,
    command_ack_supported: AtomicBool,
    pending_commands: Mutex<BTreeMap<String, PendingProcessCommand>>,
    limits: AgentProcessTransportLimits,
}

struct PendingProcessCommand {
    expected_run_revision: u64,
    completion: tokio::sync::oneshot::Sender<Result<(), AgentCommandRejectReason>>,
    // A dropped caller does not turn an unacknowledged command into free capacity.
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ActiveProcess {
    fn close_commands(&self) {
        self.protocol_ready.store(false, Ordering::Release);
        self.command_slots.close();
        self.pending_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn acknowledge_command(&self, envelope: &ProcessEnvelope) -> Result<(), String> {
        let id = envelope
            .command_id
            .as_deref()
            .ok_or("command_ack omitted command_id")?;
        let revision = envelope
            .expected_run_revision
            .ok_or("command_ack omitted expected_run_revision")?;
        let accepted = envelope.accepted.ok_or("command_ack omitted accepted")?;
        let mut pending = self
            .pending_commands
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let command = pending
            .get(id)
            .ok_or("command_ack has no pending command")?;
        if command.expected_run_revision != revision {
            return Err("command_ack revision does not match the pending command".into());
        }
        let command = pending
            .remove(id)
            .expect("pending command checked under the same lock");
        drop(pending);
        let _ = command.completion.send(if accepted {
            Ok(())
        } else {
            Err(AgentCommandRejectReason::UnsupportedByBackend)
        });
        Ok(())
    }
}

struct ProcessContentUpload {
    next_offset: u64,
    hasher: Sha256,
    writer: Box<dyn crate::ArtifactWriteSink>,
}

#[derive(Default)]
struct ProcessJsonlRegistry {
    specs: Mutex<BTreeMap<String, ProcessJsonlSpec>>,
    lifecycle: Mutex<ProcessJsonlLifecycle>,
}

#[derive(Default)]
struct ProcessJsonlLifecycle {
    starting: BTreeSet<String>,
    active: BTreeMap<String, Arc<ActiveProcess>>,
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
    services: Option<Weak<RuntimeServices>>,
}

impl ProcessJsonlAdapter {
    #[must_use]
    pub fn for_workspace(workspace_root: impl Into<PathBuf>) -> Self {
        Self {
            registry: Arc::new(ProcessJsonlRegistry::default()),
            workspace_root: Arc::new(workspace_root.into()),
            services: None,
        }
    }

    #[must_use]
    pub(crate) fn for_runtime(
        services: Weak<RuntimeServices>,
        workspace_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            registry: Arc::new(ProcessJsonlRegistry::default()),
            workspace_root: Arc::new(workspace_root.into()),
            services: Some(services),
        }
    }

    /// Register one immutable, Runtime-approved command manifest. Agent ids
    /// are intentionally absent: a packet selects only the exact
    /// Definition-bound `command_ref` and digest.
    pub fn register_command(&self, spec: ProcessJsonlSpec) -> Result<(), String> {
        spec.validate()?;
        let command_ref = spec.command_ref.clone();
        let mut specs = self
            .registry
            .specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = specs.get(&command_ref) {
            if existing.command_digest != spec.command_digest {
                return Err(format!(
                    "ProcessJsonl command manifest `{command_ref}` is already registered with a different digest"
                ));
            }
            return Ok(());
        }
        specs.insert(command_ref, spec);
        Ok(())
    }

    /// Check an immutable Binding against the startup-approved command
    /// registry before Runtime admits the run. This is deliberately separate
    /// from `execute`: a missing capability must not consume a provider/model
    /// turn merely to discover that no command can be launched.
    pub(crate) fn validate_packet_command(&self, packet: &AgentTaskPacket) -> Result<(), String> {
        self.resolve_packet_command(packet).map(|_| ())
    }

    fn resolve_packet_command(&self, packet: &AgentTaskPacket) -> Result<ProcessJsonlSpec, String> {
        let binding = packet.binding.as_ref().ok_or_else(|| {
            "ProcessJsonl execution requires a Runtime-compiled Agent Binding".to_string()
        })?;
        let (command_ref, command_digest) = match &binding.executor {
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                command_ref,
                command_digest,
            } => (command_ref, command_digest),
            _ => return Err("ProcessJsonl backend received a non-ProcessJsonl Binding".to_string()),
        };
        let spec = self
            .registry
            .specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(command_ref)
            .cloned()
            .ok_or_else(|| {
                format!(
                    "no Runtime-approved ProcessJsonl command manifest is registered for `{command_ref}`"
                )
            })?;
        if &spec.command_digest != command_digest {
            return Err(format!(
                "ProcessJsonl command manifest digest mismatch for `{command_ref}`"
            ));
        }
        Ok(spec)
    }

    fn command_envelope(
        handle: &AgentRunHandle,
        request: &AgentCommandRequest,
    ) -> serde_json::Value {
        serde_json::json!({
            "kind": "agent_command",
            "run_id": handle.run_id.as_str(),
            "agent_id": handle.agent_id.as_str(),
            "command_id": request.command_id,
            "expected_run_revision": request.expected_revision,
            "command": request.command,
            "input": request.input,
        })
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
        selection: AgentModelSelection,
    ) -> Result<AgentReturnPacket, String> {
        let spec = self.resolve_packet_command(&packet)?;
        let tool_session = match self.services.as_ref().and_then(Weak::upgrade) {
            Some(services) => Some(Arc::new(ProcessJsonlToolSession::prepare(
                &services, &packet, &selection,
            )?)),
            None if packet.allowed_tools.is_empty() => None,
            None => return Err(
                "ProcessJsonl task requests tools but this adapter is not Runtime ToolHost-bound"
                    .to_string(),
            ),
        };
        let runtime_handle = tokio::runtime::Handle::try_current().map_err(|error| {
            format!("ProcessJsonl bridge requires an active Tokio Runtime: {error}")
        })?;
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
            execute_child(
                &registry,
                &workspace_root,
                &spec,
                &packet,
                tool_session,
                runtime_handle,
            )
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
        match request.command {
            AgentCommand::Pause | AgentCommand::Resume => {
                Err(AgentCommandRejectReason::UnsupportedByBackend)
            }
            AgentCommand::Cancel | AgentCommand::Shutdown => {
                active.close_commands();
                let mut child = active
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                terminate_process_tree(&mut child)
                    .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)
            }
            AgentCommand::SendInput | AgentCommand::Interrupt => {
                let queued_at = std::time::Instant::now();
                if !active.protocol_ready.load(Ordering::Acquire)
                    || !active.command_ack_supported.load(Ordering::Acquire)
                {
                    return Err(AgentCommandRejectReason::UnsupportedByBackend);
                }
                let permit = Arc::clone(&active.command_slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)?;
                let writer = Arc::clone(&active.command_writer).lock_owned().await;
                let queue_wait = queued_at.elapsed();
                if !active.protocol_ready.load(Ordering::Acquire) {
                    return Err(AgentCommandRejectReason::UnsupportedByBackend);
                }
                let message = Self::command_envelope(handle, request);
                let (completion, acknowledged) = tokio::sync::oneshot::channel();
                tokio::task::spawn_blocking(move || {
                    let _writer = writer;
                    let mut pending = active
                        .pending_commands
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !active.protocol_ready.load(Ordering::Acquire)
                        || active.terminal_received.load(Ordering::Acquire)
                    {
                        return Err(AgentCommandRejectReason::UnsupportedByBackend);
                    }
                    let command_id = message["command_id"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    if command_id.is_empty() || pending.contains_key(&command_id) {
                        return Err(AgentCommandRejectReason::InvalidInput);
                    }
                    pending.insert(
                        command_id.clone(),
                        PendingProcessCommand {
                            expected_run_revision: message["expected_run_revision"]
                                .as_u64()
                                .expect("typed command revision"),
                            completion,
                            _permit: permit,
                        },
                    );
                    drop(pending);
                    let write_started = std::time::Instant::now();
                    let result = write_process_message(&active, &message);
                    if result.is_err() {
                        active.close_commands();
                        let _ = terminate_process_tree(
                            &mut active
                                .child
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner),
                        );
                    }
                    tracing::debug!(
                        run_id = message["run_id"].as_str().unwrap_or_default(),
                        queue_wait_us = queue_wait.as_micros() as u64,
                        write_us = write_started.elapsed().as_micros() as u64,
                        quota_source = "operator_process_manifest",
                        pending_command_limit = active.limits.pending_commands,
                        success = result.is_ok(),
                        "ProcessJsonl transport write observation"
                    );
                    result.map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)
                })
                .await
                .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)??;
                acknowledged
                    .await
                    .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)?
            }
        }
    }
}

fn execute_child(
    registry: &ProcessJsonlRegistry,
    workspace_root: &PathBuf,
    spec: &ProcessJsonlSpec,
    packet: &AgentTaskPacket,
    tool_session: Option<Arc<ProcessJsonlToolSession>>,
    runtime_handle: tokio::runtime::Handle,
) -> Result<AgentReturnPacket, String> {
    execute_child_with_handshake_timeout(
        registry,
        workspace_root,
        spec,
        packet,
        tool_session,
        runtime_handle,
        Duration::from_millis(spec.transport_limits.handshake_timeout_ms),
    )
}

fn execute_child_with_handshake_timeout(
    registry: &ProcessJsonlRegistry,
    workspace_root: &PathBuf,
    spec: &ProcessJsonlSpec,
    packet: &AgentTaskPacket,
    tool_session: Option<Arc<ProcessJsonlToolSession>>,
    runtime_handle: tokio::runtime::Handle,
    handshake_timeout: Duration,
) -> Result<AgentReturnPacket, String> {
    let mut launch_spec = SandboxLaunchSpec::workspace(workspace_root);
    launch_spec.working_directory = spec
        .working_directory
        .as_deref()
        .map(|directory| workspace_path(workspace_root, directory))
        .transpose()?;
    launch_spec.environment = spec.resolve_environment()?;
    match spec.sandbox_profile {
        crate::config::AgentExecutorSandboxProfile::WorkspaceReadWrite => {}
        crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly => {
            launch_spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
        }
        crate::config::AgentExecutorSandboxProfile::IsolatedReadWrite => {
            launch_spec.network_enabled = false;
        }
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly => {
            launch_spec.workspace_access = SandboxWorkspaceAccess::ReadOnly;
            launch_spec.network_enabled = false;
        }
    }
    let executable = workspace_path(workspace_root, &spec.executable)?;
    let prepared = program_command_with_args(&executable, &spec.args, &launch_spec)
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
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| "ProcessJsonl worker stdin is unavailable".to_string())?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "ProcessJsonl worker stdout is unavailable".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "ProcessJsonl worker stderr is unavailable".to_string())?;
    let active = Arc::new(ActiveProcess {
        child: Mutex::new(child),
        stdin: Mutex::new(stdin),
        next_runtime_sequence: Mutex::new(1),
        protocol_ready: AtomicBool::new(false),
        terminal_received: AtomicBool::new(false),
        command_slots: Arc::new(tokio::sync::Semaphore::new(
            spec.transport_limits.pending_commands,
        )),
        command_writer: Arc::new(tokio::sync::Mutex::new(())),
        command_ack_supported: AtomicBool::new(false),
        pending_commands: Mutex::new(BTreeMap::new()),
        limits: spec.transport_limits,
    });
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
        let mut child = active
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = terminate_process_tree(&mut child);
    }

    let stderr_limit = spec.transport_limits.stderr_tail_bytes;
    let stderr_tail = std::thread::spawn(move || read_stderr_tail(stderr, stderr_limit));
    let result = (|| {
        write_process_message(
            active.as_ref(),
            &serde_json::json!({
                "kind": "hello",
                "run_id": packet.run_id(),
                "agent_id": packet.agent_id(),
                "manifest_digest": spec.command_digest.as_str(),
                "focus": process_focus(packet),
                "capabilities": runtime_process_capabilities(),
                "transport_limits": spec.transport_limits,
                "model_control": "external_configured",
            }),
        )?;
        // A blocking pipe read cannot be cancelled by Tokio's timeout alone.
        // Keep the reader on its own thread, acknowledge its bounded V2
        // handshake through a channel, and only then permit it to consume the
        // long-lived exchange. On a timeout the outer lifecycle kills the
        // process tree and the reader is joined before this run is retired.
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let (exchange_tx, exchange_rx) = mpsc::sync_channel(1);
        let reader_active = Arc::clone(&active);
        let reader_packet = packet.clone();
        let reader_spec = spec.clone();
        let reader_tool_session = tool_session.clone();
        let reader_runtime_handle = runtime_handle.clone();
        let reader = std::thread::spawn(move || {
            let mut stdout = BufReader::new(stdout);
            let child_capabilities =
                match read_process_ready(&mut stdout, &reader_packet, &reader_spec) {
                    Ok(capabilities) => {
                        let _ = ready_tx.send(Ok(capabilities.clone()));
                        capabilities
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error.clone()));
                        return Err(error);
                    }
                };
            exchange_rx.recv().map_err(|_| {
                "Runtime abandoned ProcessJsonl before the V2 exchange could start".to_string()
            })?;
            read_process_exchange(
                &mut stdout,
                &reader_packet,
                Some(reader_active.as_ref()),
                reader_tool_session.as_deref(),
                Some(&reader_runtime_handle),
                Some(&child_capabilities),
            )
        });
        let handshake = match ready_rx.recv_timeout(handshake_timeout) {
            Ok(Ok(capabilities)) => {
                active.command_ack_supported.store(
                    capabilities.commands.contains("command_ack"),
                    Ordering::Release,
                );
                tool_session
                    .as_ref()
                    .map(|session| -> Result<_, String> {
                        Ok((
                            session.topic_delta()?,
                            session.working_context_delta(&runtime_handle)?,
                        ))
                    })
                    .transpose()
                    .and_then(|delta| {
                        write_process_message(
                            active.as_ref(),
                            &serde_json::json!({
                                "kind": "start",
                                "run_id": packet.run_id(),
                                "agent_id": packet.agent_id(),
                                "manifest_digest": spec.command_digest.as_str(),
                                "focus": process_focus(packet),
                                "model_control": "external_configured",
                                "packet": packet,
                                "context_delta": delta.as_ref().and_then(|pair|pair.0.as_ref()),
                                "working_context":delta.as_ref().and_then(|pair|pair.1.as_ref()),
                            }),
                        )
                    })
                    .and_then(|()| {
                        active.protocol_ready.store(true, Ordering::Release);
                        exchange_tx.send(()).map_err(|_| {
                            "ProcessJsonl reader stopped before the V2 exchange could start"
                                .to_string()
                        })
                    })
            }
            Ok(Err(error)) => Err(error),
            Err(mpsc::RecvTimeoutError::Timeout) => Err(format!(
                "ProcessJsonl worker did not complete the V2 ready handshake within {} seconds",
                handshake_timeout.as_secs_f64()
            )),
            Err(mpsc::RecvTimeoutError::Disconnected) => Err(
                "ProcessJsonl reader stopped before reporting its V2 ready handshake".to_string(),
            ),
        };
        // Do not leave the reader waiting on its gate when starting failed or
        // timed out. Its owner below joins it before the child lifecycle is
        // finalized; the outer error path physically terminates the process.
        drop(exchange_tx);
        match handshake {
            Ok(()) => reader.join().map_err(|_| {
                "ProcessJsonl reader thread panicked during the V2 exchange".to_string()
            })?,
            Err(error) => {
                // The reader may still be blocked in an OS pipe read (for
                // example after the handshake deadline). Close its producer
                // before joining it; dropping only the gate is insufficient
                // while no ready frame has arrived.
                let mut child = active
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let _ = terminate_process_tree(&mut child);
                drop(child);
                let _ = reader.join();
                Err(error)
            }
        }
    })();
    // A malformed frame or a rejected bridge request often leaves an
    // external worker waiting for another stdin message. Never wait for that
    // worker to decide to exit: cancellation is the only safe outcome once
    // the protocol is no longer trustworthy.
    let terminal_received = active.terminal_received.load(Ordering::Acquire);
    active.close_commands();
    if result.is_err() || terminal_received {
        let mut child = active
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = terminate_process_tree(&mut child);
    }
    let exit_status = active
        .child
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .wait()
        .map_err(|error| format!("failed to wait for ProcessJsonl worker: {error}"));
    registry
        .lifecycle
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .active
        .remove(packet.run_id());

    let stderr_tail = stderr_tail.join().unwrap_or_else(|_| ProcessStderrTail {
        tail: String::new(),
        dropped_bytes: None,
        read_error: Some("stderr collector panicked".into()),
    });
    tracing::debug!(
        run_id = packet.run_id(),
        dropped_bytes = stderr_tail.dropped_bytes,
        raw_tail_limit = spec.transport_limits.stderr_tail_bytes,
        quota_source = "operator_process_manifest",
        "ProcessJsonl stderr drain observation"
    );
    let status = exit_status?;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            let error = if spec.environment_refs.is_empty() {
                error
            } else {
                // Child-controlled frame fields and parser diagnostics may contain
                // transformed or truncated credentials. Exact-value replacement
                // cannot safely declassify these diagnostics.
                "worker protocol failure (credential-bearing diagnostic withheld)".into()
            };
            return Err(format!(
                "ProcessJsonl protocol failed: {error}; stderr tail: {stderr_tail}"
            ));
        }
    };
    if !status.success() && !terminal_received {
        return Err(format!(
            "ProcessJsonl worker exited with {status}; stderr tail: {stderr_tail}"
        ));
    }
    Ok(result)
}

#[cfg(unix)]
fn terminate_process_tree(child: &mut Child) -> std::io::Result<()> {
    let process_group = child.id() as i32;
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
            child.kill().or(Ok(()))
        } else {
            Err(error)
        }
    }
}

#[cfg(not(unix))]
fn terminate_process_tree(child: &mut Child) -> std::io::Result<()> {
    child.kill()
}

#[derive(Debug)]
struct ProcessStderrTail {
    tail: String,
    dropped_bytes: Option<u64>,
    read_error: Option<String>,
}

impl std::fmt::Display for ProcessStderrTail {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            output,
            "[worker stderr withheld; retained_bytes={}; dropped_bytes={:?}; read_failed={}]",
            self.tail.len(),
            self.dropped_bytes,
            self.read_error.is_some()
        )
    }
}

fn read_stderr_tail(mut stderr: impl Read, max_bytes: usize) -> ProcessStderrTail {
    let mut tail = std::collections::VecDeque::with_capacity(max_bytes);
    let mut buffer = [0_u8; 4096];
    let mut dropped_bytes = 0_u64;
    let mut read_error = None;
    loop {
        match stderr.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                if max_bytes == 0 {
                    dropped_bytes = dropped_bytes.saturating_add(read as u64);
                    continue;
                }
                for byte in &buffer[..read] {
                    if tail.len() == max_bytes {
                        tail.pop_front();
                        dropped_bytes = dropped_bytes.saturating_add(1);
                    }
                    tail.push_back(*byte);
                }
            }
            Err(error) => {
                read_error = Some(error.to_string());
                break;
            }
        }
    }
    ProcessStderrTail {
        tail: String::from_utf8_lossy(tail.make_contiguous()).into_owned(),
        dropped_bytes: Some(dropped_bytes),
        read_error,
    }
}

fn workspace_path(workspace_root: &Path, configured_path: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(configured_path);
    if path.is_absolute() {
        Ok(path)
    } else {
        Ok(workspace_root.join(path))
    }
}

#[cfg(test)]
fn read_process_result(
    stdout: impl std::io::Read,
    packet: &AgentTaskPacket,
) -> Result<AgentReturnPacket, String> {
    let mut stdout = BufReader::new(stdout);
    read_process_exchange(&mut stdout, packet, None, None, None, None)
}

fn process_focus(packet: &AgentTaskPacket) -> ProcessFocus {
    ProcessFocus {
        graph_id: packet.graph_id().to_string(),
        node_id: packet.node_id().to_string(),
        attempt: packet.attempt,
    }
}

fn runtime_process_capabilities() -> ProcessCapabilities {
    ProcessCapabilities {
        commands: BTreeSet::from([
            "start".to_string(),
            "tool_result".to_string(),
            "action_result".to_string(),
            "agent_command".to_string(),
            "content_ack".to_string(),
            "content_committed".to_string(),
            "context_acknowledged".to_string(),
        ]),
        supports_recovery: false,
        evidence_mode: Some("runtime_receipts".to_string()),
        model_control: Some("external_configured".to_string()),
    }
}

fn read_process_ready(
    stdout: &mut impl BufRead,
    packet: &AgentTaskPacket,
    spec: &ProcessJsonlSpec,
) -> Result<ProcessCapabilities, String> {
    let envelope =
        read_process_envelope(stdout, spec.transport_limits.frame_bytes)?.ok_or_else(|| {
            "ProcessJsonl worker reached EOF before the required V2 ready frame".to_string()
        })?;
    validate_process_envelope_identity(&envelope, packet, 1)?;
    if envelope.kind.as_deref() != Some("ready") {
        return Err("ProcessJsonl worker did not send a V2 ready frame".to_string());
    }
    if envelope.manifest_digest.as_deref() != Some(spec.command_digest.as_str())
        || envelope.focus.as_ref() != Some(&process_focus(packet))
    {
        return Err("ProcessJsonl ready frame does not match the bound manifest or focus".into());
    }
    let capabilities = envelope
        .capabilities
        .ok_or_else(|| "ProcessJsonl ready frame omitted required capabilities".to_string())?;
    if !capabilities.commands.contains("result")
        || !capabilities.commands.contains("error")
        || capabilities.supports_recovery
        || capabilities.evidence_mode.as_deref() != Some("runtime_receipts")
        || capabilities.model_control.as_deref() != Some("external_configured")
        || envelope.model_control.as_deref() != Some("external_configured")
    {
        return Err(
            "ProcessJsonl ready frame advertises unsupported or unverifiable capabilities"
                .to_string(),
        );
    }
    if packet.agentic_binding.is_some() && !capabilities.commands.contains("context_ack") {
        return Err("Agentic ProcessJsonl workers must support context_ack and consume context_delta on start/tool replies".into());
    }
    Ok(capabilities)
}

fn process_tool_request_fingerprint(
    kind: &str,
    tool_name: &str,
    input: &serde_json::Value,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(&(kind, tool_name, input)).map_err(|error| error.to_string())?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn read_process_exchange(
    stdout: &mut BufReader<impl std::io::Read>,
    packet: &AgentTaskPacket,
    active: Option<&ActiveProcess>,
    tool_session: Option<&ProcessJsonlToolSession>,
    runtime_handle: Option<&tokio::runtime::Handle>,
    child_capabilities: Option<&ProcessCapabilities>,
) -> Result<AgentReturnPacket, String> {
    let mut expected_sequence = 2;
    let mut terminal = None;
    let mut handled_tool_requests = VecDeque::<(String, String, serde_json::Value)>::new();
    let mut content_uploads = BTreeMap::<String, ProcessContentUpload>::new();
    let limits = active.map_or_else(AgentProcessTransportLimits::default, |active| active.limits);
    while let Some(envelope) = read_process_envelope(stdout, limits.frame_bytes)? {
        validate_process_envelope_identity(&envelope, packet, expected_sequence)?;
        expected_sequence = expected_sequence.saturating_add(1);
        let kind = envelope
            .kind
            .as_deref()
            .ok_or_else(|| "ProcessJsonl V2 frame omitted kind".to_string())?;
        if child_capabilities.is_some_and(|capabilities| !capabilities.commands.contains(kind)) {
            return Err(format!(
                "ProcessJsonl emitted `{kind}` without advertising that capability in ready"
            ));
        }
        if kind == "context_ack" {
            let session =
                tool_session.ok_or("Topic acknowledgement requires the Runtime bridge")?;
            let delivery_id = envelope
                .delivery_id
                .as_deref()
                .ok_or("Topic acknowledgement omitted delivery_id")?;
            session.acknowledge_topic_delta(delivery_id)?;
            write_process_message(
                active.ok_or("Topic acknowledgement requires an active worker")?,
                &serde_json::json!({"kind":"context_acknowledged","run_id":packet.run_id(),
                    "agent_id":packet.agent_id(),"delivery_id":delivery_id,"observation":"worker_transport"}),
            )?;
            continue;
        }
        if kind == "command_ack" {
            active
                .ok_or("command_ack requires an active worker")?
                .acknowledge_command(&envelope)?;
            continue;
        }
        if matches!(Some(kind), Some("tool_request") | Some("action_request")) {
            let response_kind = if kind == "action_request" {
                "action_result"
            } else {
                "tool_result"
            };
            let request_id = envelope.request_id.ok_or_else(|| {
                "ProcessJsonl action/tool request is missing request_id".to_string()
            })?;
            tool_session
                .ok_or("ProcessJsonl tool request requires the Runtime bridge")?
                .validate_current_policy()?;
            let tool_name = envelope.tool_name.ok_or_else(|| {
                "ProcessJsonl action/tool request is missing tool_name".to_string()
            })?;
            let input_value = envelope.input.unwrap_or_default();
            let fingerprint =
                process_tool_request_fingerprint(response_kind, &tool_name, &input_value)?;
            let response = if let Some((_, prior_fingerprint, response)) = handled_tool_requests
                .iter()
                .find(|(id, _, _)| id == &request_id)
            {
                if prior_fingerprint != &fingerprint {
                    return Err(
                        "ProcessJsonl request_id was reused for a different tool invocation".into(),
                    );
                }
                response.clone()
            } else {
                let session = tool_session.ok_or_else(|| {
                    "ProcessJsonl worker requested an action/tool without a Runtime ToolHost bridge"
                        .to_string()
                })?;
                let runtime_handle = runtime_handle.ok_or_else(|| {
                    "ProcessJsonl worker requested an action/tool without an active Runtime handle"
                        .to_string()
                })?;
                let input = serde_json::to_string(&input_value)
                    .map_err(|error| format!("encode ProcessJsonl tool input: {error}"))?;
                let response = if let Some(response) =
                    session.replay_or_begin_transport_request(&request_id, &fingerprint)?
                {
                    response
                } else {
                    let response = match runtime_handle.block_on(session.execute_tool(
                        &request_id,
                        &tool_name,
                        &input,
                    )) {
                        Ok(output) => serde_json::json!({
                            "kind": response_kind,
                            "run_id": packet.run_id(),
                            "agent_id": packet.agent_id(),
                            "request_id": request_id.clone(),
                            "output": output,
                        }),
                        Err(error) => serde_json::json!({
                            "kind": response_kind,
                            "run_id": packet.run_id(),
                            "agent_id": packet.agent_id(),
                            "request_id": request_id.clone(),
                            "error": error,
                        }),
                    };
                    session.complete_transport_request(&request_id, &fingerprint, &response)?;
                    response
                };
                if handled_tool_requests.len() == limits.cached_tool_responses {
                    handled_tool_requests.pop_front();
                }
                handled_tool_requests.push_back((request_id, fingerprint, response.clone()));
                response
            };
            let mut response = response;
            if let Some(session) = tool_session {
                response["context_delta"] =
                    session.topic_delta()?.unwrap_or(serde_json::Value::Null);
                response["working_context"] = session
                    .working_context_delta(
                        runtime_handle.ok_or("working context requires Runtime handle")?,
                    )?
                    .unwrap_or(serde_json::Value::Null);
            }
            write_process_message(
                active.ok_or_else(|| {
                    "ProcessJsonl worker requested a tool without an active child process"
                        .to_string()
                })?,
                &response,
            )?;
            continue;
        }
        if envelope.kind.as_deref() == Some("content_chunk") {
            let upload_id = valid_upload_id(envelope.upload_id.as_deref())?;
            let offset = envelope
                .offset
                .ok_or_else(|| "ProcessJsonl content_chunk is missing offset".to_string())?;
            let chunk = envelope
                .chunk
                .ok_or_else(|| "ProcessJsonl content_chunk is missing chunk".to_string())?;
            if chunk.len() > limits.content_chunk_bytes {
                return Err(format!(
                    "ProcessJsonl content chunk exceeds the {}-byte transport limit",
                    limits.content_chunk_bytes
                ));
            }
            if !content_uploads.contains_key(&upload_id) {
                if offset != 0 {
                    return Err("ProcessJsonl content upload must start at offset zero".to_string());
                }
                if content_uploads.len() >= limits.pending_uploads {
                    return Err(format!("ProcessJsonl exceeds the {} pending content uploads transport limit; commit an upload before opening another", limits.pending_uploads));
                }
                let session = tool_session.ok_or_else(|| {
                    "ProcessJsonl worker requested content storage without a Runtime artifact bridge"
                        .to_string()
                })?;
                let runtime_handle = runtime_handle.ok_or_else(|| {
                    "ProcessJsonl worker requested content storage without an active Runtime handle"
                        .to_string()
                })?;
                let media_type = bounded_metadata(
                    envelope
                        .media_type
                        .as_deref()
                        .unwrap_or("text/plain; charset=utf-8"),
                    "media_type",
                    128,
                )?;
                let original_name = envelope
                    .original_name
                    .as_deref()
                    .map(|value| bounded_metadata(value, "original_name", 256))
                    .transpose()?;
                let writer = runtime_handle
                    .block_on(session.artifact_store().begin(ArtifactWriteDescriptor {
                        media_type,
                        visibility_scope: format!("session:{}", packet.session_id()),
                        expected_bytes: None,
                        original_name,
                    }))
                    .map_err(|error| format!("begin ProcessJsonl content upload: {error}"))?;
                content_uploads.insert(
                    upload_id.clone(),
                    ProcessContentUpload {
                        next_offset: 0,
                        hasher: Sha256::new(),
                        writer,
                    },
                );
            }
            let upload = content_uploads
                .get_mut(&upload_id)
                .ok_or_else(|| "ProcessJsonl content upload disappeared".to_string())?;
            if upload.next_offset != offset {
                return Err(format!(
                    "ProcessJsonl content chunk offset {offset} does not match expected {}",
                    upload.next_offset
                ));
            }
            let runtime_handle = runtime_handle.ok_or_else(|| {
                "ProcessJsonl worker requested content storage without an active Runtime handle"
                    .to_string()
            })?;
            runtime_handle
                .block_on(upload.writer.write_chunk(chunk.as_bytes()))
                .map_err(|error| format!("write ProcessJsonl content chunk: {error}"))?;
            upload.hasher.update(chunk.as_bytes());
            upload.next_offset = upload.next_offset.saturating_add(chunk.len() as u64);
            write_process_message(
                active.ok_or_else(|| {
                    "ProcessJsonl worker uploaded content without an active child process"
                        .to_string()
                })?,
                &serde_json::json!({
                    "kind": "content_ack",
                    "run_id": packet.run_id(),
                    "agent_id": packet.agent_id(),
                    "upload_id": upload_id,
                    "next_offset": upload.next_offset,
                }),
            )?;
            continue;
        }
        if envelope.kind.as_deref() == Some("content_commit") {
            let upload_id = valid_upload_id(envelope.upload_id.as_deref())?;
            let expected_digest = envelope.sha256.ok_or_else(|| {
                "ProcessJsonl content_commit is missing the content sha256".to_string()
            })?;
            let mut upload = content_uploads.remove(&upload_id).ok_or_else(|| {
                "ProcessJsonl content_commit references an unknown upload".to_string()
            })?;
            let actual_digest = format!("sha256:{:x}", upload.hasher.clone().finalize());
            if actual_digest != expected_digest {
                if let Some(runtime_handle) = runtime_handle {
                    let _ = runtime_handle.block_on(upload.writer.abort());
                }
                return Err(
                    "ProcessJsonl content_commit digest does not match uploaded bytes".into(),
                );
            }
            let runtime_handle = runtime_handle.ok_or_else(|| {
                "ProcessJsonl content_commit has no active Runtime handle".to_string()
            })?;
            let artifact = runtime_handle
                .block_on(upload.writer.finish())
                .map_err(|error| format!("commit ProcessJsonl content upload: {error}"))?;
            if artifact.sha256 != actual_digest {
                return Err("ProcessJsonl artifact store returned an inconsistent digest".into());
            }
            write_process_message(
                active.ok_or_else(|| {
                    "ProcessJsonl worker committed content without an active child process"
                        .to_string()
                })?,
                &serde_json::json!({
                    "kind": "content_committed",
                    "run_id": packet.run_id(),
                    "agent_id": packet.agent_id(),
                    "upload_id": upload_id,
                    "artifact_ref": artifact,
                }),
            )?;
            continue;
        }
        match envelope.kind.as_deref() {
            Some("result") => {
                let result = envelope.result.ok_or_else(|| {
                    "ProcessJsonl result frame omitted its terminal result".to_string()
                })?;
                if stdout
                    .buffer()
                    .iter()
                    .any(|byte| !byte.is_ascii_whitespace())
                {
                    return Err("ProcessJsonl emitted a duplicate terminal or trailing frame after its result".into());
                }
                terminal = Some(result);
                if let Some(active) = active {
                    active.terminal_received.store(true, Ordering::Release);
                }
                // A terminal result closes the protocol. Do not allow a
                // misbehaving worker to hold Runtime on a blocking stdout
                // read after it has declared completion; execute_child kills
                // the remaining process tree before its final wait.
                break;
            }
            Some("error") => {
                let _error = envelope
                    .error
                    .ok_or_else(|| "ProcessJsonl error frame omitted its diagnostic".to_string())?;
                return Err(
                    "ProcessJsonl worker reported error (untrusted diagnostic withheld)".into(),
                );
            }
            _ => return Err("ProcessJsonl emitted an unsupported V2 frame kind".to_string()),
        }
    }
    let mut terminal = terminal
        .ok_or_else(|| "ProcessJsonl worker exited without a terminal result".to_string())?;
    // A child process may report business output, but it cannot mint Runtime
    // observation truth. Until its tool effects cross the canonical ToolHost
    // receipt boundary, all typed evidence obligations remain unresolved.
    if let Some(session) = tool_session {
        session.apply_terminal(packet, &mut terminal);
    } else {
        let required = crate::acceptance_evaluator::AcceptanceEvaluator::effective_required(
            &packet.required_acceptance,
            &packet.acceptance,
        );
        let (observed, evaluation) =
            crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_snapshot(
                crate::acceptance_evaluator::AcceptanceReceiptSnapshot::from_terminal(
                    required,
                    Vec::new(),
                    Vec::new(),
                ),
            );
        terminal.observed_acceptance = observed;
        terminal.acceptance_evaluation = Some(evaluation);
        terminal.acceptance.clear();
        terminal.runtime_observed_resource_scopes.clear();
    }
    Ok(terminal)
}

fn validate_process_envelope_identity(
    envelope: &ProcessEnvelope,
    packet: &AgentTaskPacket,
    expected_sequence: u64,
) -> Result<(), String> {
    if envelope.protocol_version != PROCESS_JSONL_PROTOCOL_VERSION
        || envelope.sequence != expected_sequence
        || envelope.run_id != packet.run_id()
        || envelope.agent_id != packet.agent_id()
    {
        return Err("ProcessJsonl V2 envelope binding or sequence is invalid".into());
    }
    Ok(())
}

fn valid_upload_id(upload_id: Option<&str>) -> Result<String, String> {
    let upload_id = upload_id
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 128)
        .ok_or_else(|| "ProcessJsonl content frame has an invalid upload_id".to_string())?;
    if !upload_id.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':')
    }) {
        return Err("ProcessJsonl content frame has an invalid upload_id".to_string());
    }
    Ok(upload_id.to_string())
}

fn bounded_metadata(value: &str, field: &str, limit: usize) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() || value.len() > limit || value.contains('\0') || value.contains('\n') {
        return Err(format!("ProcessJsonl content {field} is invalid"));
    }
    Ok(value.to_string())
}

fn read_process_envelope(
    stdout: &mut impl BufRead,
    max_frame_bytes: usize,
) -> Result<Option<ProcessEnvelope>, String> {
    loop {
        let Some(line) = read_process_line(stdout, max_frame_bytes)? else {
            return Ok(None);
        };
        if line.trim().is_empty() {
            continue;
        }
        return serde_json::from_str(&line)
            .map(Some)
            .map_err(|error| format!("malformed ProcessJsonl V2 envelope: {error}"));
    }
}

fn read_process_line(
    stdout: &mut impl BufRead,
    max_frame_bytes: usize,
) -> Result<Option<String>, String> {
    let mut line = Vec::new();
    loop {
        let buffer = stdout
            .fill_buf()
            .map_err(|error| format!("failed to read ProcessJsonl output: {error}"))?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(None)
            } else {
                Err("ProcessJsonl output ended in a truncated frame".to_string())
            };
        }
        let consumed = buffer
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(buffer.len(), |index| index + 1);
        if line.len().saturating_add(consumed) > max_frame_bytes {
            return Err(format!(
                "ProcessJsonl V2 frame exceeds the {max_frame_bytes}-byte transport limit"
            ));
        }
        let terminated = consumed <= buffer.len() && buffer[consumed - 1] == b'\n';
        line.extend_from_slice(&buffer[..consumed]);
        stdout.consume(consumed);
        if terminated {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return String::from_utf8(line)
                .map(Some)
                .map_err(|_| "ProcessJsonl V2 frame is not UTF-8".to_string());
        }
    }
}

fn write_process_message(
    active: &ActiveProcess,
    message: &serde_json::Value,
) -> Result<(), String> {
    let mut stdin = active
        .stdin
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut message = message.clone();
    let object = message
        .as_object_mut()
        .ok_or_else(|| "ProcessJsonl Runtime message must be an object".to_string())?;
    let mut sequence = active
        .next_runtime_sequence
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    object.insert(
        "protocol_version".to_string(),
        serde_json::Value::from(PROCESS_JSONL_PROTOCOL_VERSION),
    );
    object.insert("sequence".to_string(), serde_json::Value::from(*sequence));
    let payload = serde_json::to_string(&message)
        .map_err(|error| format!("encode ProcessJsonl Runtime message: {error}"))?;
    if payload.len().saturating_add(1) > active.limits.frame_bytes {
        return Err(format!("ProcessJsonl Runtime control frame exceeds {} bytes; publish long content through the content plane", active.limits.frame_bytes));
    }
    *sequence = sequence.saturating_add(1);
    stdin
        .write_all(format!("{payload}\n").as_bytes())
        .and_then(|()| stdin.flush())
        .map_err(|error| format!("write ProcessJsonl Runtime message: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::context::ChildExecutionBudgetReservation;

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
            output_acceptance: Vec::new(),
            acceptance: Vec::new(),
            cohort_prompt_package: None,
            constraints: Vec::new(),
            context_refs: Vec::new(),
            evidence_refs: Vec::new(),
            resource_scopes: Vec::new(),
            allowed_tools: Vec::new(),
            allowed_skills: Vec::new(),
            permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            model_lease: "test".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                "budget-process-1",
                "process-agent-1",
                "agent",
                1000,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            binding: None,
            managed_invocation: None,
            idempotency_key: "process-idempotency-1".into(),
            agentic_binding: None,
        }
    }

    pub(super) fn completed_return(packet: &AgentTaskPacket) -> AgentReturnPacket {
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
            answer_candidate: None,
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
                    model_observation: None,
                    workspace_prior_state: None,
                }],
                unresolved_obligation_ids: Vec::new(),
            },
            acceptance_evaluation: None,
            acceptance: vec!["must-be-runtime-verified".to_string()],
            evidence_refs: Vec::new(),
            changes: Vec::new(),
            runtime_change_receipts: Vec::new(),
            conflicts: Vec::new(),
            unresolved: Vec::new(),
            input_tokens: 0,
            output_tokens: 0,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
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
                observation_requirement: Default::default(),
            }],
        };
        let envelope = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION,
            "sequence": 2,
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "kind": "result",
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

    #[test]
    fn agentic_process_requires_explicit_topic_transport_ack_capability() {
        let mut packet = task();
        packet.agentic_binding = Some(harness_contract::agent::AgenticExecutionBinding {
            program_id: "program:topic".into(),
            agent_id: "logical-recipient".into(),
            membership_id: "membership:recipient".into(),
            team_id: "team:topic".into(),
            task_team_id: "team:topic".into(),
            source_spec_revision: 1,
            focus: harness_contract::agent::AgenticExecutionFocus::TaskExecute {
                task_ref: "task:topic".into(),
            },
        });
        let spec = ProcessJsonlSpec::new("command:topic", "worker", vec![]);
        let mut ready = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION, "sequence": 1, "kind":"ready",
            "run_id":packet.run_id(), "agent_id":packet.agent_id(), "manifest_digest":spec.command_digest,
            "focus":process_focus(&packet), "model_control":"external_configured",
            "capabilities":{"commands":["result","error"],"supports_recovery":false,
                "evidence_mode":"runtime_receipts","model_control":"external_configured"}
        });
        let bytes = format!("{ready}\n");
        assert!(
            read_process_ready(&mut BufReader::new(bytes.as_bytes()), &packet, &spec)
                .unwrap_err()
                .contains("context_ack")
        );
        ready["capabilities"]["commands"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!("context_ack"));
        let bytes = format!("{ready}\n");
        assert!(read_process_ready(&mut BufReader::new(bytes.as_bytes()), &packet, &spec).is_ok());
        let mut forged = ready.clone();
        forged["agent_id"] = serde_json::json!("another-worker");
        let bytes = format!("{forged}\n");
        assert!(read_process_ready(&mut BufReader::new(bytes.as_bytes()), &packet, &spec).is_err());
    }

    #[test]
    fn v2_handshake_binds_manifest_focus_and_directional_sequences() {
        let packet = task();
        let spec = ProcessJsonlSpec::new(
            "command:unit-worker",
            ".cowd/workers/unit-worker",
            Vec::new(),
        );
        let ready = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION,
            "sequence": 1,
            "kind": "ready",
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "manifest_digest": spec.command_digest.as_str(),
            "focus": process_focus(&packet),
            "model_control": "external_configured",
            "capabilities": {
                "commands": ["result", "error", "tool_request"],
                "supports_recovery": false,
                "evidence_mode": "runtime_receipts",
                "model_control": "external_configured"
            }
        });
        let result = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION,
            "sequence": 2,
            "kind": "result",
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "result": completed_return(&packet),
        });
        let mut frames = BufReader::new(std::io::Cursor::new(format!("{ready}\n{result}\n")));

        let capabilities = read_process_ready(&mut frames, &packet, &spec).expect("valid V2 ready");
        let returned =
            read_process_exchange(&mut frames, &packet, None, None, None, Some(&capabilities))
                .expect("valid V2 result");

        assert_eq!(
            returned.status,
            harness_contract::agent::AgentTerminalStatus::Completed
        );
    }

    #[test]
    fn process_v2_rejects_identity_sequence_truncation_and_buffered_duplicate_terminal() {
        let packet = task();
        let valid = serde_json::json!({"protocol_version":2,"sequence":2,"kind":"result",
            "run_id":packet.run_id(),"agent_id":packet.agent_id(),"result":completed_return(&packet)});
        for (field, value) in [
            ("run_id", serde_json::json!("foreign-run")),
            ("agent_id", serde_json::json!("foreign-agent")),
            ("sequence", serde_json::json!(3)),
        ] {
            let mut wrong = valid.clone();
            wrong[field] = value;
            assert!(
                read_process_result(format!("{wrong}\n").as_bytes(), &packet).is_err(),
                "{field}"
            );
        }
        assert!(
            read_process_result(valid.to_string().as_bytes(), &packet).is_err(),
            "truncated frame"
        );
        assert!(read_process_result(b"{malformed\n".as_slice(), &packet).is_err());
        assert!(
            read_process_result(b"".as_slice(), &packet).is_err(),
            "EOF without result"
        );
        let mut second = valid.clone();
        second["sequence"] = serde_json::json!(3);
        let error =
            read_process_result(format!("{valid}\n{second}\n").as_bytes(), &packet).unwrap_err();
        assert!(error.contains("duplicate terminal"), "{error}");
    }

    #[test]
    fn v1_and_oversized_frames_are_rejected_without_a_fallback_decoder() {
        let packet = task();
        let v1 = serde_json::json!({
            "protocol_version": 1,
            "sequence": 1,
            "kind": "result",
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "result": completed_return(&packet),
        });
        assert!(read_process_result(format!("{v1}\n").as_bytes(), &packet)
            .expect_err("V1 is not accepted")
            .contains("V2"));
        let oversized = format!("{}\n", "x".repeat(MAX_PROCESS_FRAME_BYTES));
        assert!(read_process_result(oversized.as_bytes(), &packet)
            .expect_err("oversized frame is rejected")
            .contains("transport limit"));
    }

    #[test]
    fn command_manifest_is_bound_by_reference_and_exact_digest() {
        let adapter = ProcessJsonlAdapter::for_workspace(std::env::temp_dir());
        let spec = ProcessJsonlSpec::new(
            "command:unit-worker",
            "unit-worker",
            vec!["--jsonl".to_string()],
        );
        adapter
            .register_command(spec.clone())
            .expect("valid immutable command manifest");
        let registered = adapter
            .registry
            .specs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get("command:unit-worker")
            .cloned()
            .expect("manifest is indexed by command_ref");
        assert_eq!(registered.command_digest, spec.command_digest);

        let mut tampered = spec;
        tampered.executable = "different-worker".to_string();
        assert!(adapter.register_command(tampered).is_err());
    }

    #[test]
    fn stderr_collector_continues_draining_but_retains_only_the_bounded_tail() {
        let input = format!("{}tail-marker", "x".repeat(20 * 1024));
        let input_len = input.len();
        let tail = read_stderr_tail(std::io::Cursor::new(input), 128);
        assert!(tail.tail.ends_with("tail-marker"));
        assert_eq!(tail.tail.len(), 128);
        assert_eq!(tail.dropped_bytes, Some((input_len - 128) as u64));
        assert!(tail.read_error.is_none());
    }

    #[test]
    fn untrusted_stderr_and_error_frames_do_not_become_public_diagnostics() {
        let marker = "synthetic-secret-never-a-real-credential";
        for limit in [3, 128] {
            let tail = read_stderr_tail(std::io::Cursor::new(marker), limit);
            let public = tail.to_string();
            assert!(!public.contains(marker));
            assert!(
                !public.contains(&tail.tail),
                "even a truncated secret is withheld"
            );
            assert!(public.contains("dropped_bytes="));
        }
        let packet = task();
        let frame = serde_json::json!({"protocol_version":2,"sequence":2,"kind":"error",
            "run_id":packet.run_id(),"agent_id":packet.agent_id(),"error":marker});
        let error = read_process_result(format!("{frame}\n").as_bytes(), &packet).unwrap_err();
        assert!(error.contains("worker reported error"));
        assert!(!error.contains(marker));
    }

    #[test]
    fn stderr_read_failure_preserves_observed_tail_and_dropped_byte_count() {
        struct FailedRead;
        impl Read for FailedRead {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("injected stderr read failure"))
            }
        }
        let tail = read_stderr_tail(std::io::Cursor::new(b"abcdef").chain(FailedRead), 3);
        assert_eq!(tail.tail, "def");
        assert_eq!(tail.dropped_bytes, Some(3));
        assert!(tail.read_error.unwrap().contains("injected"));
        let disabled = read_stderr_tail(std::io::Cursor::new(b"abcd"), 0);
        assert!(disabled.tail.is_empty());
        assert_eq!(disabled.dropped_bytes, Some(4));
    }

    #[test]
    fn configured_frame_limits_are_enforced_and_manifest_changes_are_fenced() {
        let mut bytes = std::io::Cursor::new(b"12345\n");
        assert!(read_process_line(&mut bytes, 5).is_err());
        assert_eq!(
            read_process_line(&mut std::io::Cursor::new(b"12345\n"), 6)
                .unwrap()
                .as_deref(),
            Some("12345")
        );
        let mut spec = ProcessJsonlSpec::new("configured", "worker", vec![]);
        spec.validate().unwrap();
        let original = spec.clone();
        spec.transport_limits.pending_commands = 2;
        assert!(spec.validate().unwrap_err().contains("digest"));
        assert_eq!(original.transport_limits.pending_commands, 32);
        original.validate().unwrap();
    }

    #[test]
    #[cfg(unix)]
    fn full_stdin_command_queue_does_not_block_tokio_or_cancel_and_releases_all_permits() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(2)
            .build()
            .unwrap();
        runtime.block_on(async {
            let adapter = ProcessJsonlAdapter::for_workspace(std::env::temp_dir());
            let packet = task();
            let mut command = std::process::Command::new("sh");
            command.args(["-c", "exec sleep 30"]).process_group(0);
            let mut child = command
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap();
            let stdin = child.stdin.take().unwrap();
            let active = Arc::new(ActiveProcess {
                child: Mutex::new(child),
                stdin: Mutex::new(stdin),
                next_runtime_sequence: Mutex::new(1),
                protocol_ready: AtomicBool::new(true),
                terminal_received: AtomicBool::new(false),
                command_slots: Arc::new(tokio::sync::Semaphore::new(MAX_PROCESS_PENDING_COMMANDS)),
                command_writer: Arc::new(tokio::sync::Mutex::new(())),
                command_ack_supported: AtomicBool::new(true),
                pending_commands: Mutex::new(BTreeMap::new()),
                limits: AgentProcessTransportLimits::default(),
            });
            adapter
                .registry
                .lifecycle
                .lock()
                .unwrap()
                .active
                .insert(packet.run_id().into(), Arc::clone(&active));
            // An OS-thread watchdog makes the old synchronous-pipe regression fail
            // deterministically even when it blocks this single Tokio thread.
            let (finished, watchdog_rx) = std::sync::mpsc::channel();
            let watchdog_active = Arc::clone(&active);
            let watchdog_fired = Arc::new(AtomicBool::new(false));
            let fired = Arc::clone(&watchdog_fired);
            let watchdog = std::thread::spawn(move || {
                if watchdog_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                    fired.store(true, Ordering::Release);
                    let _ = terminate_process_tree(&mut watchdog_active.child.lock().unwrap());
                }
            });
            assert!(write_process_message(
                &active,
                &serde_json::json!({"kind":"agent_command",
            "input":"x".repeat(MAX_PROCESS_FRAME_BYTES + 1)})
            )
            .is_err());
            assert_eq!(
                *active.next_runtime_sequence.lock().unwrap(),
                1,
                "rejecting an unsent oversized frame cannot skip a wire sequence"
            );
            let handle = AgentRunHandle {
                run_id: packet.run_id().into(),
                agent_id: packet.agent_id().into(),
                backend: AgentBackendKind::ProcessJsonl,
                revision: 1,
                status: harness_contract::agent::AgentStatus::Running,
            };
            let mut writers = tokio::task::JoinSet::new();
            for index in 0..=MAX_PROCESS_PENDING_COMMANDS {
                let adapter = adapter.clone();
                let handle = handle.clone();
                writers.spawn(async move {
                    adapter
                        .command(
                            &handle,
                            &AgentCommandRequest {
                                command_id: format!("backpressure:{index}"),
                                agent_id: handle.agent_id.clone(),
                                expected_revision: 1,
                                command: AgentCommand::SendInput,
                                input: Some(harness_contract::agent::AgentInput::UserSupplement(
                                    "x".repeat(128 * 1024),
                                )),
                            },
                        )
                        .await
                });
            }
            tokio::time::timeout(Duration::from_secs(2), async {
                while active.command_slots.available_permits() != 0
                    || active.stdin.try_lock().is_ok()
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("ordinary writes must yield while the OS pipe is full");
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), tokio::task::spawn_blocking(|| 42))
                    .await
                    .expect("one Process command queue must not occupy every blocking worker")
                    .unwrap(),
                42
            );
            tokio::time::timeout(
                Duration::from_secs(2),
                adapter.command(
                    &handle,
                    &AgentCommandRequest {
                        command_id: "cancel-full-queue".into(),
                        agent_id: handle.agent_id.clone(),
                        expected_revision: 1,
                        command: AgentCommand::Cancel,
                        input: None,
                    },
                ),
            )
            .await
            .expect("cancel bypasses the ordinary queue")
            .unwrap();
            tokio::time::timeout(Duration::from_secs(2), async {
                while let Some(result) = writers.join_next().await {
                    assert!(
                        result.unwrap().is_err(),
                        "a non-reading child cannot acknowledge these writes"
                    );
                }
            })
            .await
            .expect("all blocked and queued writes must be released");
            assert!(!active.child.lock().unwrap().wait().unwrap().success());
            assert_eq!(
                active.command_slots.available_permits(),
                MAX_PROCESS_PENDING_COMMANDS
            );
            assert!(active.command_slots.is_closed());
            finished.send(()).unwrap();
            watchdog.join().unwrap();
            assert!(!watchdog_fired.load(Ordering::Acquire));
            adapter
                .registry
                .lifecycle
                .lock()
                .unwrap()
                .active
                .remove(packet.run_id());
        });
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
        let active = Arc::new(ActiveProcess {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            next_runtime_sequence: Mutex::new(1),
            protocol_ready: AtomicBool::new(false),
            terminal_received: AtomicBool::new(false),
            command_slots: Arc::new(tokio::sync::Semaphore::new(MAX_PROCESS_PENDING_COMMANDS)),
            command_writer: Arc::new(tokio::sync::Mutex::new(())),
            command_ack_supported: AtomicBool::new(false),
            pending_commands: Mutex::new(BTreeMap::new()),
            limits: AgentProcessTransportLimits::default(),
        });
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
            .child
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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

    #[cfg(unix)]
    #[tokio::test]
    async fn command_requires_bound_ack_and_close_reclaims_abandoned_pending_slots() {
        let adapter = ProcessJsonlAdapter::for_workspace(std::env::temp_dir());
        let packet = task();
        let mut child = std::process::Command::new("sh")
            .args(["-c", "exec sleep 30"])
            .process_group(0)
            .stdin(Stdio::piped())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let active = Arc::new(ActiveProcess {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            next_runtime_sequence: Mutex::new(1),
            protocol_ready: AtomicBool::new(true),
            terminal_received: AtomicBool::new(false),
            command_slots: Arc::new(tokio::sync::Semaphore::new(2)),
            command_writer: Arc::new(tokio::sync::Mutex::new(())),
            command_ack_supported: AtomicBool::new(true),
            pending_commands: Mutex::new(BTreeMap::new()),
            limits: AgentProcessTransportLimits::default(),
        });
        struct Reclaim(Arc<ActiveProcess>);
        impl Drop for Reclaim {
            fn drop(&mut self) {
                self.0.close_commands();
                let mut child = self.0.child.lock().unwrap();
                let _ = terminate_process_tree(&mut child);
                let _ = child.wait();
            }
        }
        let _reclaim = Reclaim(active.clone());
        adapter
            .registry
            .lifecycle
            .lock()
            .unwrap()
            .active
            .insert(packet.run_id().into(), active.clone());
        let handle = AgentRunHandle {
            run_id: packet.run_id().into(),
            agent_id: packet.agent_id().into(),
            backend: AgentBackendKind::ProcessJsonl,
            revision: 17,
            status: harness_contract::agent::AgentStatus::Running,
        };
        let request = AgentCommandRequest {
            command_id: "ack-test".into(),
            agent_id: handle.agent_id.clone(),
            expected_revision: 17,
            command: AgentCommand::SendInput,
            input: Some(harness_contract::agent::AgentInput::UserSupplement(
                "hello".into(),
            )),
        };
        let wire = ProcessJsonlAdapter::command_envelope(&handle, &request);
        assert_eq!(wire["expected_run_revision"], 17);
        active.command_ack_supported.store(false, Ordering::Release);
        assert_eq!(
            adapter.command(&handle, &request).await,
            Err(AgentCommandRejectReason::UnsupportedByBackend)
        );
        assert_eq!(active.command_slots.available_permits(), 2);
        active.command_ack_supported.store(true, Ordering::Release);
        let ack = |id: &str, revision: u64, accepted: bool| -> ProcessEnvelope {
            serde_json::from_value(serde_json::json!({
                "protocol_version": 2, "sequence": 2, "run_id": packet.run_id(),
                "agent_id": packet.agent_id(), "kind": "command_ack",
                "command_id": id, "expected_run_revision": revision, "accepted": accepted
            }))
            .unwrap()
        };
        for accepted in [true, false] {
            let mut command = Box::pin(adapter.command(&handle, &request));
            assert!(
                tokio::time::timeout(Duration::from_millis(50), &mut command)
                    .await
                    .is_err(),
                "stdin write is not acceptance"
            );
            assert_eq!(active.command_slots.available_permits(), 1);
            let mut missing = ack("ack-test", 17, true);
            missing.command_id = None;
            assert!(active.acknowledge_command(&missing).is_err());
            let mut missing = ack("ack-test", 17, true);
            missing.expected_run_revision = None;
            assert!(active.acknowledge_command(&missing).is_err());
            let mut missing = ack("ack-test", 17, true);
            missing.accepted = None;
            assert!(active.acknowledge_command(&missing).is_err());
            assert!(active
                .acknowledge_command(&ack("unknown", 17, true))
                .is_err());
            assert!(active
                .acknowledge_command(&ack("ack-test", 18, true))
                .is_err());
            assert_eq!(active.command_slots.available_permits(), 1);
            active
                .acknowledge_command(&ack("ack-test", 17, accepted))
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), command)
                    .await
                    .unwrap()
                    .is_ok(),
                accepted
            );
            assert!(
                active
                    .acknowledge_command(&ack("ack-test", 17, accepted))
                    .is_err(),
                "duplicate ack is rejected"
            );
            assert_eq!(active.command_slots.available_permits(), 2);
        }
        let mut command = Box::pin(adapter.command(&handle, &request));
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut command)
                .await
                .is_err()
        );
        drop(command);
        assert_eq!(
            active.command_slots.available_permits(),
            1,
            "dropping caller cannot erase an in-flight command"
        );
        assert_eq!(
            adapter.command(&handle, &request).await,
            Err(AgentCommandRejectReason::InvalidInput)
        );
        active.close_commands();
        assert_eq!(active.command_slots.available_permits(), 2);
        assert!(active.pending_commands.lock().unwrap().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_worker_command_ack_is_consumed_and_eof_never_accepts_a_command() {
        use std::os::unix::fs::PermissionsExt;
        sandbox_launcher::probe().expect("command ack gate requires sandbox launcher");
        for send_ack in [true, false] {
            let workspace = tempfile::tempdir().unwrap();
            let packet = task();
            let spec = ProcessJsonlSpec::new("command:ack-worker", "worker.sh", vec![]);
            let ready = serde_json::json!({
                "protocol_version": 2, "sequence": 1, "kind": "ready",
                "run_id": packet.run_id(), "agent_id": packet.agent_id(),
                "manifest_digest": spec.command_digest, "focus": process_focus(&packet),
                "model_control": "external_configured", "capabilities": {
                    "commands": ["result", "error", "command_ack"], "supports_recovery": false,
                    "evidence_mode": "runtime_receipts", "model_control": "external_configured"
                }
            });
            let ack = serde_json::json!({
                "protocol_version": 2, "sequence": 2, "kind": "command_ack",
                "run_id": packet.run_id(), "agent_id": packet.agent_id(),
                "command_id": "real-ack", "expected_run_revision": 17, "accepted": true
            });
            let result = serde_json::json!({
                "protocol_version": 2, "sequence": 3, "kind": "result",
                "run_id": packet.run_id(), "agent_id": packet.agent_id(), "result": completed_return(&packet)
            });
            let worker = workspace.path().join("worker.sh");
            std::fs::write(&worker, format!(
                "#!/bin/sh\nIFS= read -r hello\nprintf '%s\\n' {}\nIFS= read -r start\nIFS= read -r command\n{}\n",
                shell_literal(&ready.to_string()),
                if send_ack { format!("printf '%s\\n' {}\nprintf '%s\\n' {}\nsleep 30", shell_literal(&ack.to_string()), shell_literal(&result.to_string())) } else { "exit 0".into() }
            )).unwrap();
            std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
            let adapter = ProcessJsonlAdapter::for_workspace(workspace.path());
            let registry = adapter.registry.clone();
            let root = workspace.path().to_path_buf();
            let worker_packet = packet.clone();
            let runtime = tokio::runtime::Handle::current();
            let execution = tokio::task::spawn_blocking(move || {
                execute_child(&registry, &root, &spec, &worker_packet, None, runtime)
            });
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if adapter
                        .registry
                        .lifecycle
                        .lock()
                        .unwrap()
                        .active
                        .get(packet.run_id())
                        .is_some_and(|active| active.protocol_ready.load(Ordering::Acquire))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("worker ready");
            let handle = AgentRunHandle {
                run_id: packet.run_id().into(),
                agent_id: packet.agent_id().into(),
                backend: AgentBackendKind::ProcessJsonl,
                revision: 17,
                status: harness_contract::agent::AgentStatus::Running,
            };
            let command = adapter
                .command(
                    &handle,
                    &AgentCommandRequest {
                        command_id: "real-ack".into(),
                        agent_id: handle.agent_id.clone(),
                        expected_revision: 17,
                        command: AgentCommand::Interrupt,
                        input: None,
                    },
                )
                .await;
            let returned = tokio::time::timeout(Duration::from_secs(5), execution)
                .await
                .expect("worker reclaimed")
                .unwrap();
            assert_eq!(command.is_ok(), send_ack);
            assert_eq!(returned.is_ok(), send_ack);
            assert!(adapter.registry.lifecycle.lock().unwrap().active.is_empty());
        }
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

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_real_worker_with_full_stderr_is_killed_without_waiting_for_exit() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        // Kernel sandbox hardening is an environmental prerequisite, not a
        // protocol result. Other tests cover the parser deterministically;
        // this one proves the real child lifecycle only where that preflight
        // is available.
        sandbox_launcher::probe().expect("Process failure gate requires the sandbox launcher");
        let workspace = tempfile::tempdir().expect("workspace");
        let worker = workspace.path().join("bad-worker.sh");
        std::fs::write(
            &worker,
            "#!/bin/sh\ndd if=/dev/zero bs=1024 count=512 1>&2 2>/dev/null\nprintf '{not-json\\n'\nsleep 30\n",
        )
        .expect("worker script");
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755))
            .expect("worker mode");
        let packet = task();
        let spec = ProcessJsonlSpec::new("command:bad-worker", "bad-worker.sh", Vec::new());
        let registry = Arc::new(ProcessJsonlRegistry::default());
        let workspace_root = workspace.path().to_path_buf();
        let packet_for_worker = packet.clone();
        let spec_for_worker = spec.clone();
        let handle = tokio::runtime::Handle::current();

        let result = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                execute_child(
                    &registry,
                    &workspace_root,
                    &spec_for_worker,
                    &packet_for_worker,
                    None,
                    handle,
                )
            }),
        )
        .await
        .expect("malformed worker must be terminated instead of waiting for sleep")
        .expect("worker thread");

        assert!(result
            .expect_err("malformed worker cannot complete")
            .contains("malformed ProcessJsonl V2 envelope"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_worker_injected_environment_is_not_exposed_by_protocol_failure() {
        use std::os::unix::fs::PermissionsExt;
        sandbox_launcher::probe().expect("credential diagnostic gate requires sandbox launcher");
        struct SyntheticEnvironment(String);
        impl Drop for SyntheticEnvironment {
            fn drop(&mut self) {
                std::env::remove_var(&self.0);
            }
        }
        let variable = format!(
            "PROCESS_DIAGNOSTIC_FIXTURE_{}",
            uuid::Uuid::new_v4().simple()
        );
        let marker = "synthetic-credential-未使用真实密钥";
        std::env::set_var(&variable, marker);
        let _environment = SyntheticEnvironment(variable.clone());
        let workspace = tempfile::tempdir().unwrap();
        let worker = workspace.path().join("diagnostic.sh");
        std::fs::write(
            &worker,
            "#!/bin/sh\nprintf '%s' \"$FIXTURE_TOKEN\" >&2\nprintf '{not-json\\n'\nsleep 30\n",
        )
        .unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut spec = ProcessJsonlSpec::new("command:diagnostic", "diagnostic.sh", vec![]);
        spec.environment_refs
            .insert("FIXTURE_TOKEN".into(), format!("env:{variable}"));
        spec.command_digest = ProcessJsonlSpec::digest_for(
            &spec.command_ref,
            &spec.executable,
            &spec.args,
            spec.working_directory.as_deref(),
            &spec.environment_refs,
            spec.sandbox_profile,
            &spec.transport_limits,
        );
        spec.validate().unwrap();
        let root = workspace.path().to_path_buf();
        let runtime = tokio::runtime::Handle::current();
        let error = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                execute_child(
                    &ProcessJsonlRegistry::default(),
                    &root,
                    &spec,
                    &task(),
                    None,
                    runtime,
                )
            }),
        )
        .await
        .expect("failed worker is reclaimed")
        .unwrap()
        .unwrap_err();
        assert!(error.contains("credential-bearing diagnostic withheld"));
        assert!(
            error.contains(&format!("retained_bytes={}", marker.len())),
            "child actually printed its injected value"
        );
        assert!(!error.contains(marker));
        assert!(!error.contains("未使用真实密钥"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_worker_that_never_sends_ready_is_reaped_at_the_handshake_deadline() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        sandbox_launcher::probe().expect("Process handshake gate requires the sandbox launcher");
        let workspace = tempfile::tempdir().expect("workspace");
        let worker = workspace.path().join("never-ready.sh");
        std::fs::write(&worker, "#!/bin/sh\nIFS= read -r hello\nsleep 30\n")
            .expect("worker script");
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755))
            .expect("worker mode");
        let packet = task();
        let spec = ProcessJsonlSpec::new("command:never-ready", "never-ready.sh", Vec::new());
        let registry = Arc::new(ProcessJsonlRegistry::default());
        let workspace_root = workspace.path().to_path_buf();
        let packet_for_worker = packet.clone();
        let spec_for_worker = spec.clone();
        let handle = tokio::runtime::Handle::current();

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::spawn_blocking(move || {
                execute_child_with_handshake_timeout(
                    &registry,
                    &workspace_root,
                    &spec_for_worker,
                    &packet_for_worker,
                    None,
                    handle,
                    Duration::from_millis(100),
                )
            }),
        )
        .await
        .expect("handshake timeout must reap a silent worker")
        .expect("worker thread");

        assert!(result
            .expect_err("a worker without ready cannot run")
            .contains("ready handshake"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn terminal_result_from_a_real_worker_does_not_wait_for_a_stubborn_child_exit() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        sandbox_launcher::probe().expect("Process terminal gate requires the sandbox launcher");
        let workspace = tempfile::tempdir().expect("workspace");
        let packet = task();
        let spec = ProcessJsonlSpec::new("command:stubborn-worker", "worker.sh", Vec::new());
        let ready = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION,
            "sequence": 1,
            "kind": "ready",
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "manifest_digest": spec.command_digest.as_str(),
            "focus": process_focus(&packet),
            "model_control": "external_configured",
            "capabilities": {
                "commands": ["result", "error"],
                "supports_recovery": false,
                "evidence_mode": "runtime_receipts",
                "model_control": "external_configured"
            }
        });
        let result = serde_json::json!({
            "protocol_version": PROCESS_JSONL_PROTOCOL_VERSION,
            "sequence": 2,
            "kind": "result",
            "run_id": packet.run_id(),
            "agent_id": packet.agent_id(),
            "result": completed_return(&packet),
        });
        let worker = workspace.path().join("worker.sh");
        std::fs::write(
            &worker,
            format!(
                "#!/bin/sh\nIFS= read -r hello\nprintf '%s\\n' {}\nIFS= read -r start\nprintf '%s\\n' {}\nsleep 30\n",
                shell_literal(&ready.to_string()),
                shell_literal(&result.to_string()),
            ),
        )
        .expect("worker script");
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755))
            .expect("worker mode");
        let registry = Arc::new(ProcessJsonlRegistry::default());
        let workspace_root = workspace.path().to_path_buf();
        let packet_for_worker = packet.clone();
        let spec_for_worker = spec.clone();
        let handle = tokio::runtime::Handle::current();

        let returned = tokio::time::timeout(
            Duration::from_secs(10),
            tokio::task::spawn_blocking(move || {
                execute_child(
                    &registry,
                    &workspace_root,
                    &spec_for_worker,
                    &packet_for_worker,
                    None,
                    handle,
                )
            }),
        )
        .await
        .expect("terminal frame must release the Runtime promptly")
        .expect("worker thread")
        .expect("valid terminal result");
        assert_eq!(
            returned.status,
            harness_contract::agent::AgentTerminalStatus::Completed
        );
    }

    #[cfg(unix)]
    fn shell_literal(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
    #[test]
    fn process_tool_request_identity_binds_kind_tool_and_canonical_input() {
        let first = serde_json::from_str(r#"{"path":"a","offset":1}"#).unwrap();
        let reordered = serde_json::from_str(r#"{"offset":1,"path":"a"}"#).unwrap();
        let identity =
            process_tool_request_fingerprint("tool_result", "read_file", &first).unwrap();
        assert_eq!(
            identity,
            process_tool_request_fingerprint("tool_result", "read_file", &reordered).unwrap()
        );
        assert_ne!(
            identity,
            process_tool_request_fingerprint(
                "tool_result",
                "read_file",
                &serde_json::json!({"path":"b","offset":1})
            )
            .unwrap()
        );
        assert_ne!(
            identity,
            process_tool_request_fingerprint("tool_result", "write_file", &first).unwrap()
        );
        assert_ne!(
            identity,
            process_tool_request_fingerprint("action_result", "read_file", &first).unwrap()
        );
    }

    struct TopicBridgeReadHost {
        source: PathBuf,
        publication: Mutex<
            Option<(
                crate::AgentActionService,
                harness_contract::agent_action::AgentActionEnvelope,
            )>,
        >,
        reads: std::sync::atomic::AtomicUsize,
    }
    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for TopicBridgeReadHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            assert!(request.authorization.is_some());
            self.reads.fetch_add(1, Ordering::SeqCst);
            let content = std::fs::read_to_string(&self.source).unwrap();
            if let Some((actions, publication)) = self.publication.lock().unwrap().take() {
                assert_eq!(
                    actions.apply(&publication).unwrap().status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
            }
            crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::Executed,
                category: request.category,
                output: Some(content),
                error: None,
                evidence_ref: format!("fixture-read:{}", request.tool_use_id),
                observed_evidence: vec![],
            }
        }
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            use harness_contract::{policy::*, tool::*};
            if name != "read_file" {
                return None;
            }
            Some(ToolEffectDescriptor {
                tool_id: name.into(),
                descriptor_hash: "topic-read-contract".into(),
                effect_kind: ToolEffectKind::Read,
                idempotency: ToolIdempotency::Idempotent,
                scopes: vec![PermissionScope {
                    resource: PermissionResource::File,
                    operation: PermissionOperation::Read,
                    target: input
                        .get("path")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_string),
                }],
                required_permission: ToolPermissionMode::ReadOnly,
                approval_class: ToolApprovalClass::None,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: EffectAssessment::default(),
            })
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_topic_bridge_delivers_start_and_tool_deltas_without_replaying_cached_tool(
    ) {
        exercise_real_process_topic_bridge(false, None).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_topic_bridge_rejects_changed_request_identity_before_effect() {
        exercise_real_process_topic_bridge(true, None).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_commits_multiframe_unicode_with_verified_digest() {
        exercise_real_process_topic_bridge(false, Some("commit")).await;
    }

    #[tokio::test]
    async fn real_process_content_streaming_uses_nondefault_manifest_transport_limits() {
        exercise_real_process_topic_bridge_with_limits(
            false,
            Some("commit"),
            AgentProcessTransportLimits {
                frame_bytes: 128 * 1024,
                content_chunk_bytes: 48 * 1024,
                pending_uploads: 2,
                pending_commands: 3,
                cached_tool_responses: 2,
                stderr_tail_bytes: 1024,
                handshake_timeout_ms: 2000,
            },
        )
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_cleans_uncommitted_upload_on_eof() {
        exercise_real_process_topic_bridge(false, Some("eof")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_rejects_nonbyte_offset_and_cleans_upload() {
        exercise_real_process_topic_bridge(false, Some("offset")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_rejects_digest_and_cleans_upload() {
        exercise_real_process_topic_bridge(false, Some("digest")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_discards_upload_on_result_without_commit() {
        exercise_real_process_topic_bridge(false, Some("result")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_bounds_pending_uploads_and_reclaims_every_file() {
        exercise_real_process_topic_bridge(false, Some("capacity")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_content_streaming_reuses_slots_beyond_the_pending_upload_limit() {
        exercise_real_process_topic_bridge(false, Some("sequential")).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn real_process_pending_transport_request_requires_effect_reconciliation_without_reexecution(
    ) {
        exercise_real_process_topic_bridge(false, Some("pending-request")).await;
    }

    #[cfg(unix)]
    async fn exercise_real_process_topic_bridge(changed_retry: bool, content_case: Option<&str>) {
        exercise_real_process_topic_bridge_with_limits(
            changed_retry,
            content_case,
            AgentProcessTransportLimits::default(),
        )
        .await;
    }

    async fn exercise_real_process_topic_bridge_with_limits(
        changed_retry: bool,
        content_case: Option<&str>,
        limits: AgentProcessTransportLimits,
    ) {
        use harness_contract::{agent::*, agent_action::*, execution_graph::*};
        use std::os::unix::fs::PermissionsExt;
        sandbox_launcher::probe()
            .expect("real ProcessJsonl source gate requires the installed sandbox launcher");
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let source = workspace.join("source.txt");
        std::fs::write(&source, "actual-source-body").unwrap();
        let host = Arc::new(TopicBridgeReadHost {
            source,
            publication: Mutex::new(None),
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let services = crate::RuntimeServices::test_builder(temp.path().join("home"), &workspace)
            .tool_execution_host(host.clone())
            .build()
            .unwrap();
        let mut packet = task();
        packet.allowed_tools = vec!["read_file".into()];
        packet.resource_scopes = vec!["read:.".into()];
        let root = AgentActorBinding {
            objective_id: "topic-process-objective".into(),
            program_id: "topic-process-program".into(),
            session_id: packet.session_id().into(),
            turn_id: "topic-turn".into(),
            root_execution_id: None,
            required_team_count: 1,
            objective_summary: "Verify Topic transport".into(),
            model_lease: "test-model".into(),
            permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
            resource_scopes: vec!["read:.".into()],
            actor_id: "root:topic-process".into(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        };
        let actions = services.agent_action_service();
        let envelope = |id: &str, action| AgentActionEnvelope {
            action_id: id.into(),
            actor: root.clone(),
            expected_revision: None,
            action,
        };
        let team = actions
            .apply(&envelope(
                "team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Process Topic Team".into(),
                    mission: "consume peer changes".into(),
                    objective: None,
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone();
        let agent = actions
            .apply(&envelope(
                "member",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team.clone(),
                    role: "Reader".into(),
                    mission: "read source".into(),
                    required_capabilities: vec!["read".into()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .unwrap()
            .changed_refs[0]
            .clone();
        let publication = |id: &str| {
            envelope(
                id,
                AgentAction::MessagePublish(MessagePublishInput {
                    topic_ref: format!("topic:{}", root.program_id),
                    summary: Some(id.into()),
                    content_ref: None,
                    refs: vec![],
                    recipients: vec![agent.clone()],
                    intent: None,
                    issue_dispositions: vec![],
                }),
            )
        };
        assert_eq!(
            actions
                .apply(&publication("initial-peer-context"))
                .unwrap()
                .status,
            AgentActionStatus::Applied
        );
        *host.publication.lock().unwrap() =
            Some((actions.clone(), publication("changed-peer-context")));
        let mut spec = ProcessJsonlSpec::new("command:topic-worker", "topic-worker.py", vec![]);
        spec.transport_limits = limits;
        spec.command_digest = ProcessJsonlSpec::digest_for(
            &spec.command_ref,
            &spec.executable,
            &spec.args,
            spec.working_directory.as_deref(),
            &spec.environment_refs,
            spec.sandbox_profile,
            &spec.transport_limits,
        );
        let registry = services.definition_registry();
        let base = registry
            .resolve_agent(
                &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
                RevisionSelector::LatestApprovedStable,
            )
            .unwrap();
        let mut manifest = base.revision.manifest.clone();
        manifest.definition_id =
            AgentDefinitionId::new(DefinitionScope::Workspace, "topic/worker").unwrap();
        manifest.executor = AgentExecutorPolicy::ProcessJsonl {
            command_ref: spec.command_ref.clone(),
            command_digest: spec.command_digest.clone(),
        };
        manifest.model_policy.allowed_models = vec!["test-model".into()];
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
                    approval_ref: "test-fixture:topic-command".into(),
                },
                content_digest: stored.revision.content_digest,
            })
            .unwrap();
        let mut request = crate::AgentBindingRequest::new(
            stored.revision.revision_ref.definition_id,
            RevisionSelector::LatestApprovedStable,
            packet.agent_id(),
            packet.session_id(),
            packet.task_id(),
        );
        request.team_id = Some(team.clone());
        request.granted_capabilities = vec![AgentCapability::Read];
        request.allowed_tool_contract_refs = packet.allowed_tools.clone();
        let binding = crate::AgentBindingCompiler::new(Arc::clone(registry))
            .compile(request)
            .unwrap()
            .snapshot;
        packet.assignment = crate::test_support::agent_assignment(
            Some(binding.definition_ref.clone()),
            packet.agent_id(),
            packet.run_id(),
            packet.task_id(),
            packet.session_id(),
            packet.mission_id(),
            Some(&team),
            packet.graph_id(),
            packet.node_id(),
        );
        packet.binding = Some(binding);
        packet.agentic_binding = Some(AgenticExecutionBinding {
            program_id: root.program_id.clone(),
            agent_id: agent.clone(),
            membership_id: format!("membership:{agent}:{team}"),
            team_id: team.clone(),
            task_team_id: team,
            source_spec_revision: 1,
            focus: AgenticExecutionFocus::TaskExecute {
                task_ref: packet.task_id().into(),
            },
        });
        let mut graph = ExecutionGraph::new("Process Topic transport");
        graph.id = packet.graph_id().into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: packet.session_id().into(),
            turn_id: "topic-turn".into(),
            root_task_id: packet.assignment.root_task_id.clone(),
            task_id: packet.task_id().into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            serde_json::to_string(&packet).unwrap(),
        );
        node.id = packet.node_id().into();
        graph.nodes.push(node);
        services.commit_service().register_graph(graph).unwrap();
        services.publish_session_execution_policy(
            packet.session_id(),
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Supervised,
                    packet.policy_revision,
                    harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                ),
            ),
        );
        let selection = crate::agent_model_selector::AgentModelSelection {
            model: "test-model".into(),
            provider: "test".into(),
            registry_revision: 0,
        };
        let bridge =
            Arc::new(ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap());
        let read_fingerprint = process_tool_request_fingerprint(
            "tool_result",
            "read_file",
            &serde_json::json!({"path":"source.txt"}),
        )
        .unwrap();
        if content_case == Some("pending-request") {
            assert!(bridge
                .replay_or_begin_transport_request("pending-crash", &read_fingerprint)
                .unwrap()
                .is_none());
        }
        let binding = packet.binding.as_ref().unwrap();
        let context = memory::MemoryTurnContext::new(
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
        let material = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: format!("session:{}", packet.session_id()),
                    expected_bytes: None,
                    original_name: None,
                },
                b"process-working-context-source",
            )
            .await
            .unwrap();
        services
            .working_context_command(
                &context,
                "process-working-pin",
                crate::working_context::WorkingContextInput::Pin {
                    source: crate::working_context::WorkingSource::Artifact {
                        content_ref: material.selector,
                    },
                },
            )
            .await
            .unwrap();
        let mut result = completed_return(&packet);
        result.observed_acceptance = Default::default();
        result.outcome = "observed both peer pages".into();
        let result_json = serde_json::to_string(&result).unwrap();
        let script=r###"#!/usr/bin/env python3
import sys,json
hello=json.loads(sys.stdin.readline());seq=0
assert hello['transport_limits']==json.loads(LIMITS_JSON)
identity={'run_id':hello['run_id'],'agent_id':hello['agent_id'],'protocol_version':2}
def send(kind,**fields):
 global seq
 seq+=1;print(json.dumps(dict(identity,sequence=seq,kind=kind,**fields)),flush=True)
def receive():return json.loads(sys.stdin.readline())
send('ready',manifest_digest=hello['manifest_digest'],focus=hello['focus'],model_control='external_configured',capabilities={'commands':['result','error','tool_request','context_ack'],'supports_recovery':False,'evidence_mode':'runtime_receipts','model_control':'external_configured'})
start=receive();delta=start['context_delta'];assert 'initial-peer-context' in json.dumps(delta)
assert 'process-working-context-source' in json.dumps(start['working_context'])
for ident,tool,args in [('denied-tool','write_file',{'path':'source.txt','content':'forbidden'}),('internal','checkpoint_create',{}),('escape','read_file',{'path':'../outside.txt'})]:
 send('tool_request',request_id=ident,tool_name=tool,input=args)
 denied=receive();assert 'error' in denied,denied
send('context_ack',delivery_id=delta['delivery_id']);assert receive()['kind']=='context_acknowledged'
send('tool_request',request_id='read-1',tool_name='read_file',input={'path':'source.txt'})
reply=receive();assert 'error' not in reply,reply
assert 'actual-source-body' in reply['output'];delta=reply['context_delta'];assert 'changed-peer-context' in json.dumps(delta)
send('context_ack',delivery_id=delta['delivery_id']);assert receive()['kind']=='context_acknowledged'
for index in range(CACHE_SIZE+1):
 send('tool_request',request_id='cache-eviction-'+str(index),tool_name='write_file',input={'path':'source.txt','content':'forbidden'})
 assert 'error' in receive()
send('tool_request',request_id='read-1',tool_name='read_file',input={'path':'source.txt'})
reply=receive();assert reply.get('context_delta') is None,reply
assert 'process-working-context-source' in json.dumps(reply['working_context'])
send('result',result=json.loads(RESULT_JSON))
"###.replace("RESULT_JSON",&format!("{:?}",result_json))
            .replace("LIMITS_JSON", &format!("{:?}", serde_json::to_string(&limits).unwrap()))
            .replace("CACHE_SIZE", &MAX_PROCESS_CACHED_TOOL_RESPONSES.to_string());
        let script = if changed_retry {
            script.replace("reply=receive();assert reply.get('context_delta') is None,reply", "reply=receive();assert reply.get('context_delta') is None,reply\nsend('tool_request',request_id='read-1',tool_name='read_file',input={'path':'different.txt'})\nreceive()")
        } else {
            script
        };
        let script = if content_case == Some("pending-request") {
            script.replace(&format!("send('result',result=json.loads({result_json:?}))"),
                "send('tool_request',request_id='pending-crash',tool_name='read_file',input={'path':'source.txt'})\nreceive()")
        } else if let Some(case) = content_case {
            // Exercise the real Process transport after the existing Topic/tool round trip.
            // Each encoded frame fits, but the complete body is far larger than one frame.
            let content_script = r###"
import hashlib
mode=CONTENT_CASE
if mode=='sequential':
 artifacts=[]
 for index in range(MAX_UPLOADS+1):
  fragment='顺序正文'+str(index)
  send('content_chunk',upload_id='item-'+str(index),offset=0,chunk=fragment)
  ack=receive();assert ack['kind']=='content_ack' and ack['next_offset']==len(fragment.encode('utf-8')),ack
  send('content_commit',upload_id='item-'+str(index),sha256='sha256:'+hashlib.sha256(fragment.encode('utf-8')).hexdigest())
  committed=receive();assert committed['kind']=='content_committed',committed
  artifacts.append(committed['artifact_ref'])
 result=json.loads(RESULT_JSON);result['outcome']=json.dumps(artifacts)
 send('result',result=result);sys.exit(0)
chunk='正文🙂\n'*4096
body=chunk*24
offset=0
for index in range(24):
 send('content_chunk',upload_id='body',offset=offset,chunk=chunk,media_type='text/plain',original_name='body.txt')
 offset+=len(chunk.encode('utf-8'))
 ack=receive();assert ack['kind']=='content_ack' and ack['next_offset']==offset,ack
 if index==0:
  if mode=='eof': sys.exit(0)
  if mode=='capacity':
   for pending in range(MAX_UPLOADS-1):
    send('content_chunk',upload_id='pending-'+str(pending),offset=0,chunk='')
    assert receive()['kind']=='content_ack'
   send('content_chunk',upload_id='overflow',offset=0,chunk='')
   receive();raise AssertionError('unbounded upload handles accepted')
  if mode=='offset':
   send('content_chunk',upload_id='body',offset=len(chunk),chunk=chunk)
   receive();raise AssertionError('invalid offset accepted')
if mode=='digest':
 send('content_commit',upload_id='body',sha256='sha256:'+'0'*64)
 receive();raise AssertionError('invalid digest accepted')
if mode=='commit':
 send('content_commit',upload_id='body',sha256='sha256:'+hashlib.sha256(body.encode('utf-8')).hexdigest())
 committed=receive();assert committed['kind']=='content_committed',committed
 result=json.loads(RESULT_JSON);result['outcome']=json.dumps(committed['artifact_ref'])
 send('result',result=result)
else:
 send('result',result=json.loads(RESULT_JSON))
"###
                .replace("CONTENT_CASE", &format!("{case:?}"))
                .replace("MAX_UPLOADS", &MAX_PROCESS_PENDING_CONTENT_UPLOADS.to_string())
                .replace("RESULT_JSON", &format!("{result_json:?}"));
            script
                .replace(
                    "'tool_request','context_ack'",
                    "'tool_request','context_ack','content_chunk','content_commit'",
                )
                .replace(
                    &format!("send('result',result=json.loads({result_json:?}))"),
                    &content_script,
                )
        } else {
            script
        };
        let initial_artifacts = services.artifact_store().stats().unwrap().artifacts;
        let worker = workspace.join("topic-worker.py");
        std::fs::write(&worker, script).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let process_registry = Arc::new(ProcessJsonlRegistry::default());
        let runtime_handle = tokio::runtime::Handle::current();
        let process_packet = packet.clone();
        let process_workspace = workspace.clone();
        let process_bridge = Arc::clone(&bridge);
        let worker_task = tokio::task::spawn_blocking(move || {
            execute_child(
                &process_registry,
                &process_workspace,
                &spec,
                &process_packet,
                Some(process_bridge),
                runtime_handle,
            )
        });
        let received = tokio::time::timeout(Duration::from_secs(30), worker_task)
            .await
            .expect("bounded process fixture")
            .unwrap();
        if content_case == Some("commit") {
            let artifact: harness_contract::context::ArtifactRef =
                serde_json::from_str(&received.unwrap().outcome).unwrap();
            let expected = "正文🙂\n".repeat(4096 * 24).into_bytes();
            assert!(expected.len() > MAX_PROCESS_FRAME_BYTES);
            assert_eq!(artifact.bytes, expected.len() as u64);
            assert_eq!(
                artifact.sha256,
                format!("sha256:{:x}", Sha256::digest(&expected))
            );
            assert_eq!(
                services
                    .artifact_store()
                    .read(&artifact, &format!("session:{}", packet.session_id()), None)
                    .await
                    .unwrap(),
                expected
            );
            assert!(services
                .artifact_store()
                .read(&artifact, "session:unrelated", None)
                .await
                .is_err());
        } else if content_case == Some("sequential") {
            let artifacts: Vec<harness_contract::context::ArtifactRef> =
                serde_json::from_str(&received.unwrap().outcome).unwrap();
            assert_eq!(artifacts.len(), MAX_PROCESS_PENDING_CONTENT_UPLOADS + 1);
            for (index, artifact) in artifacts.iter().enumerate() {
                let expected = format!("顺序正文{index}").into_bytes();
                assert_eq!(
                    artifact.sha256,
                    format!("sha256:{:x}", Sha256::digest(&expected))
                );
                assert_eq!(
                    services
                        .artifact_store()
                        .read(artifact, &format!("session:{}", packet.session_id()), None)
                        .await
                        .unwrap(),
                    expected
                );
            }
        } else if content_case == Some("pending-request") {
            assert!(received
                .unwrap_err()
                .contains("unresolved transport completion"));
        } else if content_case == Some("offset") {
            assert!(received.unwrap_err().contains("offset"));
        } else if content_case == Some("digest") {
            assert!(received.unwrap_err().contains("digest does not match"));
        } else if content_case == Some("eof") {
            assert!(received.is_err(), "EOF cannot commit or complete an upload");
        } else if content_case == Some("capacity") {
            assert!(received
                .unwrap_err()
                .contains("pending content uploads transport limit"));
        } else if changed_retry {
            assert!(received.unwrap_err().contains("request_id was reused"));
        } else {
            assert_eq!(received.unwrap().outcome, "observed both peer pages");
        }
        if content_case.is_some() {
            assert_eq!(
                services.artifact_store().stats().unwrap().artifacts,
                initial_artifacts
                    + match content_case {
                        Some("commit") => 1,
                        Some("sequential") => (MAX_PROCESS_PENDING_CONTENT_UPLOADS + 1) as u64,
                        _ => 0,
                    }
            );
            assert_eq!(
                std::fs::read_dir(temp.path().join("home/test-artifacts/staging"))
                    .unwrap()
                    .count(),
                0,
                "all streaming staging files must be reclaimed on return"
            );
        }
        let rebuilt_bridge =
            ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        if content_case.is_none() && !changed_retry {
            let barrier = Arc::new(std::sync::Barrier::new(8));
            let contenders = (0..8)
                .map(|_| {
                    let barrier = Arc::clone(&barrier);
                    let bridge = Arc::clone(&bridge);
                    let fingerprint = read_fingerprint.clone();
                    std::thread::spawn(move || {
                        barrier.wait();
                        bridge
                            .replay_or_begin_transport_request("simultaneous-request", &fingerprint)
                    })
                })
                .collect::<Vec<_>>();
            let admitted = contenders
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .filter(|result| matches!(result, Ok(None)))
                .count();
            assert_eq!(
                admitted, 1,
                "a duplicate begin receipt is not a second execution admission"
            );
        }
        let replay = rebuilt_bridge
            .replay_or_begin_transport_request("read-1", &read_fingerprint)
            .unwrap()
            .expect("the real response survives cache eviction and bridge reconstruction");
        assert!(replay["output"]
            .as_str()
            .unwrap()
            .contains("actual-source-body"));
        assert!(rebuilt_bridge
            .replay_or_begin_transport_request("read-1", "different-fingerprint")
            .unwrap_err()
            .contains("request_id was reused"));
        assert!(rebuilt_bridge
            .replay_or_begin_transport_request("unresolved-read", &read_fingerprint)
            .unwrap()
            .is_none());
        let another_bridge =
            ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        assert!(another_bridge
            .replay_or_begin_transport_request("unresolved-read", &read_fingerprint)
            .unwrap_err()
            .contains("unresolved transport completion"));
        let mut another_run = packet.clone();
        another_run.assignment.run_id = "different-physical-run".into();
        let isolated =
            ProcessJsonlToolSession::prepare(&services, &another_run, &selection).unwrap();
        assert!(
            isolated
                .replay_or_begin_transport_request("read-1", &read_fingerprint)
                .unwrap()
                .is_none(),
            "a different physical run cannot consume this run's cached transport response"
        );
        let control = services
            .session_execution_policy_control(packet.session_id())
            .unwrap();
        let mut updated = control.snapshot();
        updated.revision += 1;
        control.replace(updated).unwrap();
        assert!(bridge
            .execute_tool("stale-policy", "read_file", r#"{"path":"source.txt"}"#)
            .await
            .unwrap_err()
            .contains("policy revision is stale"));
        assert_eq!(host.reads.load(Ordering::SeqCst), 1);
        assert!(actions
            .topic_observations(
                &root.program_id,
                &agent,
                &packet.agentic_binding.as_ref().unwrap().team_id,
                packet.graph_id(),
                16,
                48 * 1024
            )
            .unwrap()
            .is_none());
    }
}
#[cfg(all(test, unix))]
mod coordination_tests {
    use super::*;
    #[tokio::test]
    async fn process_coordination_consumes_bound_wake_and_publishes_reply() {
        use std::os::unix::fs::PermissionsExt;
        sandbox_launcher::probe().expect("coordination Process gate requires sandbox launcher");
        let spec = ProcessJsonlSpec::new("command:coordination", "coordination-worker.py", vec![]);
        let fixture =
            crate::agentic::coordination::tests::fixture_with_executor(false, Some(spec.clone()))
                .await;
        let mut returned = super::tests::completed_return(&fixture.packet);
        returned.observed_acceptance = Default::default();
        returned.runtime_observed_resource_scopes.clear();
        returned.outcome = "processed the bound request".into();
        let script=r###"#!/usr/bin/env python3
import sys,json
hello=json.loads(sys.stdin.readline());seq=0
identity={'run_id':hello['run_id'],'agent_id':hello['agent_id'],'protocol_version':2}
def send(kind,**fields):
 global seq
 seq+=1;print(json.dumps(dict(identity,sequence=seq,kind=kind,**fields)),flush=True)
def receive(): return json.loads(sys.stdin.readline())
send('ready',manifest_digest=hello['manifest_digest'],focus=hello['focus'],model_control='external_configured',capabilities={'commands':['result','error','action_request','context_ack'],'supports_recovery':False,'evidence_mode':'runtime_receipts','model_control':'external_configured'})
start=receive();packet=start['packet'];binding=packet['agentic_binding'];wake=binding['focus']['wake_ref']
assert 'Check the boundary condition in source A' in packet['objective']
delta=start.get('context_delta')
if delta:
 send('context_ack',delivery_id=delta['delivery_id']);assert receive()['kind']=='context_acknowledged'
send('action_request',request_id='coordination-answer',tool_name='message_publish',input={'topic_ref':'topic:'+binding['team_id'],'summary':'Source A needs an explicit empty-input boundary check','refs':[wake]})
answer=receive();assert 'error' not in answer,answer
assert json.loads(answer['output'])['status']=='applied',answer
send('result',result=json.loads(RESULT_JSON))
"###.replace("RESULT_JSON",&format!("{:?}",serde_json::to_string(&returned).unwrap()));
        let worker = fixture.workspace.path().join("coordination-worker.py");
        std::fs::write(&worker, script).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o755)).unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            fixture
                .services
                .agent_runtime()
                .execute_task(fixture.packet.clone()),
        )
        .await
        .expect("bounded test child")
        .unwrap();
        assert!(result.failure.is_none(), "{result:?}");
        let projection = fixture
            .services
            .agent_action_service()
            .project("coord-program")
            .unwrap();
        assert!(projection.coordination_replied(
            &fixture.wake_ref,
            fixture.packet.graph_id(),
            &fixture.packet.agentic_binding.as_ref().unwrap().agent_id
        ));
        // Normal AgentRuntime exit, not a manual test settlement, consumes
        // the wake. Re-entering the same terminal run returns its cached
        // result without a second child or reply.
        assert!(
            projection.coordination_requests()[0]
                .1
                .coordination
                .as_ref()
                .unwrap()
                .settled
        );
        let revision = projection.revision;
        assert!(fixture
            .services
            .agent_runtime()
            .execute_task(fixture.packet.clone())
            .await
            .unwrap()
            .failure
            .is_none());
        let projection = fixture
            .services
            .agent_action_service()
            .project("coord-program")
            .unwrap();
        assert_eq!(projection.revision, revision);
        assert_eq!(
            projection.tasks[&fixture.task_ref]
                .claim_execution_id
                .as_deref(),
            Some("primary-execution")
        );
        assert!(fixture
            .services
            .dispatch_ready_agentic_work("coord-program")
            .await
            .unwrap()
            .is_empty());
    }
}
