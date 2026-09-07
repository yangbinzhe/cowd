use std::collections::{BTreeMap, BTreeSet};
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
use crate::{agent_in_process_worker::ProcessJsonlToolSession, RuntimeServices};

/// V2 permits arbitrarily large business content only through the content
/// plane. Control/action JSONL frames stay bounded so one broken worker cannot
/// consume unbounded Runtime memory while a pipe is being drained.
const PROCESS_JSONL_PROTOCOL_VERSION: u32 = 2;
const MAX_PROCESS_FRAME_BYTES: usize = 256 * 1024;
const MAX_PROCESS_CONTENT_CHUNK_BYTES: usize = 64 * 1024;
const STDERR_TAIL_BYTES: usize = 256 * 1024;
/// A worker that never proves the V2 handshake must not retain an execution
/// slot indefinitely. This protects only the transport admission boundary;
/// it is not a business-task or model-thinking budget.
const PROCESS_JSONL_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

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
        );
        Self {
            command_ref,
            command_digest,
            executable,
            args,
            working_directory: None,
            environment_refs: BTreeMap::new(),
            sandbox_profile: crate::config::AgentExecutorSandboxProfile::default(),
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
    ) -> String {
        let canonical = serde_json::json!({
            "command_ref": command_ref,
            "executable": executable,
            "args": args,
            "working_directory": working_directory,
            "environment_refs": environment_refs,
            "sandbox_profile": sandbox_profile,
        });
        format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()))
    }

    fn validate(&self) -> Result<(), String> {
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
                let mut child = active
                    .child
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                terminate_process_tree(&mut child)
                    .map_err(|_| AgentCommandRejectReason::UnsupportedByBackend)
            }
            AgentCommand::SendInput | AgentCommand::Interrupt => {
                if !active.protocol_ready.load(Ordering::Acquire) {
                    return Err(AgentCommandRejectReason::UnsupportedByBackend);
                }
                write_process_message(&active, &Self::command_envelope(handle, request))
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
        PROCESS_JSONL_HANDSHAKE_TIMEOUT,
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

    let stderr_tail = std::thread::spawn(move || read_stderr_tail(stderr, STDERR_TAIL_BYTES));
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
            Ok(Ok(_)) => write_process_message(
                active.as_ref(),
                &serde_json::json!({
                    "kind": "start",
                    "run_id": packet.run_id(),
                    "agent_id": packet.agent_id(),
                    "manifest_digest": spec.command_digest.as_str(),
                    "focus": process_focus(packet),
                    "model_control": "external_configured",
                    "packet": packet,
                }),
            )
            .and_then(|()| {
                active.protocol_ready.store(true, Ordering::Release);
                exchange_tx.send(()).map_err(|_| {
                    "ProcessJsonl reader stopped before the V2 exchange could start".to_string()
                })
            }),
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

    let stderr_tail = stderr_tail
        .join()
        .unwrap_or_else(|_| "<stderr collector panicked>".to_string());
    let status = exit_status?;
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            return Err(format!(
                "ProcessJsonl protocol failed: {error}; stderr tail: {stderr_tail}"
            ))
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

fn read_stderr_tail(mut stderr: impl Read, max_bytes: usize) -> String {
    let mut tail = std::collections::VecDeque::with_capacity(max_bytes);
    let mut buffer = [0_u8; 4096];
    loop {
        match stderr.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                for byte in &buffer[..read] {
                    if tail.len() == max_bytes {
                        tail.pop_front();
                    }
                    tail.push_back(*byte);
                }
            }
            Err(error) => return format!("<stderr read failed: {error}>"),
        }
    }
    String::from_utf8_lossy(tail.make_contiguous()).into_owned()
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
    let envelope = read_process_envelope(stdout)?.ok_or_else(|| {
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
    Ok(capabilities)
}

fn read_process_exchange(
    stdout: &mut impl BufRead,
    packet: &AgentTaskPacket,
    active: Option<&ActiveProcess>,
    tool_session: Option<&ProcessJsonlToolSession>,
    runtime_handle: Option<&tokio::runtime::Handle>,
    child_capabilities: Option<&ProcessCapabilities>,
) -> Result<AgentReturnPacket, String> {
    let mut expected_sequence = 2;
    let mut terminal = None;
    let mut handled_tool_requests = BTreeMap::<String, serde_json::Value>::new();
    let mut content_uploads = BTreeMap::<String, ProcessContentUpload>::new();
    while let Some(envelope) = read_process_envelope(stdout)? {
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
        if matches!(Some(kind), Some("tool_request") | Some("action_request")) {
            let response_kind = if kind == "action_request" {
                "action_result"
            } else {
                "tool_result"
            };
            let request_id = envelope.request_id.ok_or_else(|| {
                "ProcessJsonl action/tool request is missing request_id".to_string()
            })?;
            let response = if let Some(response) = handled_tool_requests.get(&request_id) {
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
                let tool_name = envelope.tool_name.ok_or_else(|| {
                    "ProcessJsonl action/tool request is missing tool_name".to_string()
                })?;
                let input = serde_json::to_string(&envelope.input.unwrap_or_default())
                    .map_err(|error| format!("encode ProcessJsonl tool input: {error}"))?;
                let response =
                    match runtime_handle.block_on(session.execute_tool(&tool_name, &input)) {
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
                handled_tool_requests.insert(request_id, response.clone());
                response
            };
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
            if chunk.len() > MAX_PROCESS_CONTENT_CHUNK_BYTES {
                return Err(format!(
                    "ProcessJsonl content chunk exceeds the {MAX_PROCESS_CONTENT_CHUNK_BYTES}-byte transport limit"
                ));
            }
            if !content_uploads.contains_key(&upload_id) {
                if offset != 0 {
                    return Err("ProcessJsonl content upload must start at offset zero".to_string());
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
                if terminal.replace(result).is_some() {
                    return Err("ProcessJsonl emitted duplicate terminal result".into());
                }
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
                let error = envelope
                    .error
                    .ok_or_else(|| "ProcessJsonl error frame omitted its diagnostic".to_string())?;
                return Err(format!("ProcessJsonl worker reported error: {error}"));
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

fn read_process_envelope(stdout: &mut impl BufRead) -> Result<Option<ProcessEnvelope>, String> {
    loop {
        let Some(line) = read_process_line(stdout)? else {
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

fn read_process_line(stdout: &mut impl BufRead) -> Result<Option<String>, String> {
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
        if line.len().saturating_add(consumed) > MAX_PROCESS_FRAME_BYTES {
            return Err(format!(
                "ProcessJsonl V2 frame exceeds the {MAX_PROCESS_FRAME_BYTES}-byte transport limit"
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
    *sequence = sequence.saturating_add(1);
    let payload = serde_json::to_string(&message)
        .map_err(|error| format!("encode ProcessJsonl Runtime message: {error}"))?;
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
        let tail = read_stderr_tail(std::io::Cursor::new(input), 128);
        assert!(tail.ends_with("tail-marker"));
        assert!(tail.len() <= 128);
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
        if sandbox_launcher::probe().is_err() {
            return;
        }
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
    async fn real_worker_that_never_sends_ready_is_reaped_at_the_handshake_deadline() {
        use std::os::unix::fs::PermissionsExt;
        use std::time::Duration;

        if sandbox_launcher::probe().is_err() {
            return;
        }
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

        if sandbox_launcher::probe().is_err() {
            return;
        }
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
}
