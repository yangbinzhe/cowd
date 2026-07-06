use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{mpsc, Mutex, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{
    dedupe_superseded_commit_events, final_assistant_text, load_system_prompt,
    run_provider_subagent_turn, summary_compression::compress_summary_text,
    AgentExecutionBackendKind, AgentExecutionCommand, AgentExecutionCommandKind,
    AgentExecutionEventEnvelope, AgentProcessJsonlSpec, CancellationToken, LaneCommitProvenance,
    LaneEvent, LaneEventBlocker, LaneFailureClass, PermissionPolicy, ProviderSubAgentTurnConfig,
    ProviderToolDefinition, ToolExecutor,
};

pub const DEFAULT_AGENT_MODEL: &str = "claude-sonnet-4-6";
pub const DEFAULT_AGENT_SYSTEM_DATE: &str = "2026-03-31";
pub const DEFAULT_AGENT_MAX_ITERATIONS: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSnapshot {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    pub name: String,
    pub description: String,
    #[serde(rename = "subagentType")]
    pub subagent_type: Option<String>,
    pub model: Option<String>,
    pub status: String,
    #[serde(default)]
    pub backend: AgentExecutionBackendKind,
    #[serde(rename = "outputFile")]
    pub output_file: String,
    #[serde(rename = "manifestFile")]
    pub manifest_file: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "startedAt", skip_serializing_if = "Option::is_none")]
    pub started_at: Option<String>,
    #[serde(rename = "completedAt", skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<String>,
    #[serde(rename = "laneEvents", default, skip_serializing_if = "Vec::is_empty")]
    pub lane_events: Vec<LaneEvent>,
    #[serde(rename = "currentBlocker", skip_serializing_if = "Option::is_none")]
    pub current_blocker: Option<LaneEventBlocker>,
    #[serde(rename = "derivedState")]
    pub derived_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SpawnAgentRequest {
    pub description: String,
    pub prompt: String,
    pub subagent_type: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub system_prompt: Vec<String>,
    pub allowed_tools: BTreeSet<String>,
    pub tool_definitions: Vec<ProviderToolDefinition>,
    pub permission_policy: PermissionPolicy,
    pub max_iterations: usize,
    pub store_dir: Option<PathBuf>,
    pub backend: AgentExecutionBackendKind,
    pub process_jsonl: Option<AgentProcessJsonlSpec>,
}

#[derive(Debug, Clone)]
pub struct AgentJob {
    pub manifest: AgentSnapshot,
    pub prompt: String,
    pub system_prompt: Vec<String>,
    pub allowed_tools: BTreeSet<String>,
    pub tool_definitions: Vec<ProviderToolDefinition>,
    pub permission_policy: PermissionPolicy,
    pub max_iterations: usize,
    pub cancellation_token: CancellationToken,
    pub process_jsonl: Option<AgentProcessJsonlSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentLifecycleEvent {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    #[serde(rename = "eventType")]
    pub event_type: String,
    #[serde(rename = "emittedAt")]
    pub emitted_at: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCommandReceipt {
    #[serde(rename = "agentId")]
    pub agent_id: String,
    pub command: String,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone)]
struct AgentRuntimeRecord {
    snapshot: AgentSnapshot,
    events: Vec<AgentLifecycleEvent>,
    cancellation_token: CancellationToken,
    command_tx: Option<mpsc::Sender<AgentExecutionCommand>>,
}

#[derive(Debug, Default)]
pub struct AgentLifecycleService {
    agents: Mutex<BTreeMap<String, AgentRuntimeRecord>>,
}

impl AgentLifecycleService {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_started(&self, snapshot: AgentSnapshot, token: CancellationToken) {
        let event = AgentLifecycleEvent {
            agent_id: snapshot.agent_id.clone(),
            event_type: String::from("agent.started"),
            emitted_at: iso8601_now(),
            message: format!("Agent `{}` started", snapshot.name),
        };
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                snapshot.agent_id.clone(),
                AgentRuntimeRecord {
                    snapshot,
                    events: vec![event],
                    cancellation_token: token,
                    command_tx: None,
                },
            );
    }

    pub fn attach_command_channel(
        &self,
        agent_id: &str,
        command_tx: mpsc::Sender<AgentExecutionCommand>,
    ) -> Result<(), String> {
        let mut agents = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = agents
            .get_mut(agent_id)
            .ok_or_else(|| String::from("agent not found"))?;
        record.command_tx = Some(command_tx);
        Ok(())
    }

    pub fn update_snapshot(&self, snapshot: AgentSnapshot, event_type: &str, message: String) {
        let event = AgentLifecycleEvent {
            agent_id: snapshot.agent_id.clone(),
            event_type: event_type.to_string(),
            emitted_at: iso8601_now(),
            message,
        };
        if let Some(record) = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(&snapshot.agent_id)
        {
            record.snapshot = snapshot;
            record.events.push(event);
        }
    }

    pub fn record_event(
        &self,
        agent_id: &str,
        event_type: impl Into<String>,
        message: impl Into<String>,
    ) {
        if let Some(record) = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_mut(agent_id)
        {
            record.events.push(AgentLifecycleEvent {
                agent_id: agent_id.to_string(),
                event_type: event_type.into(),
                emitted_at: iso8601_now(),
                message: message.into(),
            });
        }
    }

    pub fn list(&self) -> Vec<AgentSnapshot> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|record| record.snapshot.clone())
            .collect()
    }

    pub fn get(&self, agent_id: &str) -> Option<AgentSnapshot> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .map(|record| record.snapshot.clone())
    }

    pub fn events(&self, agent_id: &str) -> Option<Vec<AgentLifecycleEvent>> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .map(|record| record.events.clone())
    }

    #[must_use]
    pub fn command_capability(&self, agent_id: &str) -> Option<crate::AgentBackendCapability> {
        self.agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .map(|record| {
                record
                    .snapshot
                    .backend
                    .capability(record.command_tx.is_some())
            })
    }

    pub fn cancel(&self, agent_id: &str) -> Result<AgentCommandReceipt, String> {
        let mut agents = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = agents
            .get_mut(agent_id)
            .ok_or_else(|| String::from("agent not found"))?;
        if is_terminal_or_cancel_requested_status(&record.snapshot.status) {
            return Ok(AgentCommandReceipt {
                agent_id: agent_id.to_string(),
                command: String::from("cancel"),
                status: String::from("noop"),
                message: format!("agent is already {}", record.snapshot.status),
            });
        }
        record.cancellation_token.cancel();
        if let Some(command_tx) = &record.command_tx {
            let _ = command_tx.send(AgentExecutionCommand {
                agent_id: agent_id.to_string(),
                command: AgentExecutionCommandKind::Cancel,
                payload: None,
            });
        }
        let snapshot = persist_agent_control_state(
            &record.snapshot,
            "cancel_requested",
            "Cancellation requested for agent",
        )?;
        record.snapshot = snapshot;
        record.events.push(AgentLifecycleEvent {
            agent_id: agent_id.to_string(),
            event_type: String::from("agent.cancel_requested"),
            emitted_at: iso8601_now(),
            message: String::from("Cancellation requested for agent"),
        });
        Ok(AgentCommandReceipt {
            agent_id: agent_id.to_string(),
            command: String::from("cancel"),
            status: String::from("accepted"),
            message: String::from("cancellation requested"),
        })
    }

    pub fn command(
        &self,
        agent_id: &str,
        command: AgentExecutionCommandKind,
        payload: Option<serde_json::Value>,
    ) -> Result<crate::AgentExecutionCommandReceipt, String> {
        let mut agents = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let record = agents
            .get_mut(agent_id)
            .ok_or_else(|| String::from("agent not found"))?;
        if is_terminal_or_cancel_requested_status(&record.snapshot.status) {
            return Ok(crate::AgentExecutionCommandReceipt {
                agent_id: agent_id.to_string(),
                backend: record.snapshot.backend,
                command,
                status: String::from("noop"),
                message: format!("agent is already {}", record.snapshot.status),
            });
        }
        let Some(command_tx) = &record.command_tx else {
            return Err(format!(
                "agent backend {} does not expose a command channel",
                serde_json::to_string(&record.snapshot.backend)
                    .unwrap_or_else(|_| "\"unknown\"".to_string())
                    .trim_matches('"')
            ));
        };
        if command == AgentExecutionCommandKind::Cancel {
            record.cancellation_token.cancel();
        }
        command_tx
            .send(AgentExecutionCommand {
                agent_id: agent_id.to_string(),
                command,
                payload,
            })
            .map_err(|error| format!("failed to deliver agent command: {error}"))?;
        record.events.push(AgentLifecycleEvent {
            agent_id: agent_id.to_string(),
            event_type: format!("agent.command.{command:?}").to_ascii_lowercase(),
            emitted_at: iso8601_now(),
            message: String::from("agent command delivered"),
        });
        Ok(crate::AgentExecutionCommandReceipt {
            agent_id: agent_id.to_string(),
            backend: record.snapshot.backend,
            command,
            status: String::from("accepted"),
            message: String::from("agent command delivered"),
        })
    }

    pub fn projection(&self) -> serde_json::Value {
        let records = self
            .agents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let agents = records
            .values()
            .map(|record| {
                let mut value = serde_json::to_value(&record.snapshot)
                    .unwrap_or_else(|_| serde_json::json!({}));
                if let Some(object) = value.as_object_mut() {
                    object.insert(
                        "backend_capability".to_string(),
                        serde_json::json!(record
                            .snapshot
                            .backend
                            .capability(record.command_tx.is_some())),
                    );
                }
                value
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "kind": "runtime.agents",
            "count": agents.len(),
            "agents": agents,
        })
    }
}

pub fn global_agent_lifecycle_service() -> &'static AgentLifecycleService {
    static SERVICE: OnceLock<AgentLifecycleService> = OnceLock::new();
    SERVICE.get_or_init(AgentLifecycleService::new)
}

pub fn spawn_provider_agent<T>(
    request: SpawnAgentRequest,
    tool_executor: T,
) -> Result<AgentSnapshot, String>
where
    T: ToolExecutor + Send + 'static,
{
    let job = prepare_agent_job(request)?;
    let manifest = job.manifest.clone();
    global_agent_lifecycle_service()
        .register_started(manifest.clone(), job.cancellation_token.clone());
    let spawn_result = match job.manifest.backend {
        AgentExecutionBackendKind::InProcess => spawn_agent_job(job, tool_executor),
        AgentExecutionBackendKind::ProcessJsonl => spawn_process_jsonl_agent_job(job),
    };
    if let Err(error) = spawn_result {
        let error = format!("failed to spawn sub-agent: {error}");
        let failed = persist_agent_terminal_state(&manifest, "failed", None, Some(error.clone()))?;
        global_agent_lifecycle_service().update_snapshot(
            failed,
            "agent.failed",
            String::from("Agent failed before background execution started"),
        );
        return Err(error);
    }
    Ok(manifest)
}

pub fn prepare_agent_job(request: SpawnAgentRequest) -> Result<AgentJob, String> {
    if request.description.trim().is_empty() {
        return Err(String::from("description must not be empty"));
    }
    if request.prompt.trim().is_empty() {
        return Err(String::from("prompt must not be empty"));
    }

    let agent_id = make_agent_id();
    let output_dir = request.store_dir.map_or_else(agent_store_dir, Ok)?;
    std::fs::create_dir_all(&output_dir).map_err(|error| error.to_string())?;
    let output_file = output_dir.join(format!("{agent_id}.md"));
    let manifest_file = output_dir.join(format!("{agent_id}.json"));
    let normalized_subagent_type = normalize_subagent_type(request.subagent_type.as_deref());
    let model = resolve_agent_model(request.model.as_deref());
    let agent_name = request
        .name
        .as_deref()
        .map(slugify_agent_name)
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| slugify_agent_name(&request.description));
    let created_at = iso8601_now();

    let output_contents = format!(
        "# Agent Task\n\n- id: {}\n- name: {}\n- description: {}\n- subagent_type: {}\n- created_at: {}\n\n## Prompt\n\n{}\n",
        agent_id,
        agent_name,
        request.description,
        normalized_subagent_type,
        created_at,
        request.prompt
    );
    std::fs::write(&output_file, output_contents).map_err(|error| error.to_string())?;

    let manifest = AgentSnapshot {
        agent_id,
        name: agent_name,
        description: request.description,
        subagent_type: Some(normalized_subagent_type),
        model: Some(model),
        status: String::from("running"),
        backend: request.backend,
        output_file: output_file.display().to_string(),
        manifest_file: manifest_file.display().to_string(),
        created_at: created_at.clone(),
        started_at: Some(created_at),
        completed_at: None,
        lane_events: vec![LaneEvent::started(iso8601_now())],
        current_blocker: None,
        derived_state: String::from("working"),
        error: None,
    };
    write_agent_manifest(&manifest)?;

    Ok(AgentJob {
        manifest,
        prompt: request.prompt,
        system_prompt: request.system_prompt,
        allowed_tools: request.allowed_tools,
        tool_definitions: request.tool_definitions,
        permission_policy: request.permission_policy,
        max_iterations: request.max_iterations,
        cancellation_token: CancellationToken::new(),
        process_jsonl: request.process_jsonl,
    })
}

fn spawn_process_jsonl_agent_job(job: AgentJob) -> Result<(), String> {
    let spec = job
        .process_jsonl
        .clone()
        .ok_or_else(|| String::from("process-jsonl backend requires process_jsonl spec"))?;
    let (command_tx, command_rx) = mpsc::channel::<AgentExecutionCommand>();
    global_agent_lifecycle_service()
        .attach_command_channel(&job.manifest.agent_id, command_tx)
        .map_err(|error| format!("failed to attach process-jsonl command channel: {error}"))?;
    let thread_name = format!("cowd-agent-jsonl-{}", job.manifest.agent_id);
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || run_process_jsonl_agent_job(job, spec, command_rx))
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn run_process_jsonl_agent_job(
    job: AgentJob,
    spec: AgentProcessJsonlSpec,
    command_rx: mpsc::Receiver<AgentExecutionCommand>,
) {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_process_jsonl_agent_job_inner(&job, &spec, command_rx)
    }));
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            let cancelled = job.cancellation_token.is_cancelled()
                || error.to_ascii_lowercase().contains("cancel");
            let status = if cancelled { "cancelled" } else { "failed" };
            let event_type = if cancelled {
                "agent.cancelled"
            } else {
                "agent.failed"
            };
            if let Ok(snapshot) =
                persist_agent_terminal_state(&job.manifest, status, None, Some(error))
            {
                global_agent_lifecycle_service().update_snapshot(
                    snapshot,
                    event_type,
                    format!("Process JSONL agent {status}"),
                );
            }
        }
        Err(_) => {
            if let Ok(snapshot) = persist_agent_terminal_state(
                &job.manifest,
                "failed",
                None,
                Some(String::from("process-jsonl agent thread panicked")),
            ) {
                global_agent_lifecycle_service().update_snapshot(
                    snapshot,
                    "agent.failed",
                    String::from("Process JSONL agent thread panicked"),
                );
            }
        }
    }
}

fn run_process_jsonl_agent_job_inner(
    job: &AgentJob,
    spec: &AgentProcessJsonlSpec,
    command_rx: mpsc::Receiver<AgentExecutionCommand>,
) -> Result<(), String> {
    let mut command = Command::new(&spec.command);
    command.args(&spec.args);
    if let Some(cwd) = &spec.cwd {
        command.current_dir(cwd);
    }
    command
        .env("COWD_AGENT_ID", &job.manifest.agent_id)
        .env("COWD_AGENT_NAME", &job.manifest.name)
        .env("COWD_AGENT_MANIFEST_FILE", &job.manifest.manifest_file)
        .env("COWD_AGENT_OUTPUT_FILE", &job.manifest.output_file)
        .env("COWD_AGENT_BACKEND", "process-jsonl");
    for (key, value) in &spec.env {
        command.env(key, value);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|error| format!("failed to start process-jsonl backend: {error}"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| String::from("process-jsonl backend stdin is unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| String::from("process-jsonl backend stdout is unavailable"))?;
    let stderr = child.stderr.take();

    write_process_jsonl_spawn_command(job, &mut stdin)?;

    let (line_tx, line_rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let reader = std::io::BufReader::new(stdout);
        for line in reader.lines().map_while(Result::ok) {
            let _ = line_tx.send(line);
        }
    });
    let (stderr_tx, stderr_rx) = mpsc::channel::<String>();
    if let Some(stderr) = stderr {
        std::thread::spawn(move || {
            let mut reader = std::io::BufReader::new(stderr);
            let mut buffer = String::new();
            let _ = std::io::Read::read_to_string(&mut reader, &mut buffer);
            let _ = stderr_tx.send(buffer);
        });
    }

    let mut terminal_seen = false;
    loop {
        while let Ok(command) = command_rx.try_recv() {
            write_agent_execution_command(&mut stdin, &command)?;
        }

        while let Ok(line) = line_rx.try_recv() {
            if handle_process_jsonl_event(job, &line)? {
                terminal_seen = true;
            }
        }

        if job.cancellation_token.is_cancelled() {
            let _ = child.kill();
            let _ = child.wait();
            return Err(String::from("process-jsonl agent cancelled"));
        }

        if let Some(status) = child
            .try_wait()
            .map_err(|error| format!("failed to poll process-jsonl backend: {error}"))?
        {
            while let Ok(line) = line_rx.try_recv() {
                if handle_process_jsonl_event(job, &line)? {
                    terminal_seen = true;
                }
            }
            if terminal_seen {
                return Ok(());
            }
            let stderr = stderr_rx.try_recv().unwrap_or_default();
            return Err(format!(
                "process-jsonl backend exited before terminal event: status={status}, stderr={}",
                stderr.trim()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn write_process_jsonl_spawn_command(job: &AgentJob, stdin: &mut impl Write) -> Result<(), String> {
    let command = AgentExecutionCommand {
        agent_id: job.manifest.agent_id.clone(),
        command: AgentExecutionCommandKind::Spawn,
        payload: Some(serde_json::json!({
            "manifest": job.manifest,
            "prompt": job.prompt,
            "systemPrompt": job.system_prompt,
            "allowedTools": job.allowed_tools,
            "permissionMode": job.permission_policy.active_mode(),
            "maxIterations": job.max_iterations,
        })),
    };
    write_agent_execution_command(stdin, &command)
}

fn write_agent_execution_command(
    stdin: &mut impl Write,
    command: &AgentExecutionCommand,
) -> Result<(), String> {
    let encoded = serde_json::to_string(&command).map_err(|error| error.to_string())?;
    writeln!(stdin, "{encoded}").map_err(|error| error.to_string())
}

fn handle_process_jsonl_event(job: &AgentJob, line: &str) -> Result<bool, String> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }
    let envelope: AgentExecutionEventEnvelope = serde_json::from_str(trimmed).map_err(|error| {
        format!(
            "invalid process-jsonl event for {}: {error}",
            job.manifest.agent_id
        )
    })?;
    if envelope.agent_id != job.manifest.agent_id {
        return Err(format!(
            "process-jsonl event agent mismatch: expected {}, got {}",
            job.manifest.agent_id, envelope.agent_id
        ));
    }
    let message = envelope
        .payload
        .as_ref()
        .and_then(|payload| payload.get("message").and_then(serde_json::Value::as_str))
        .unwrap_or(envelope.event_type.as_str())
        .to_string();

    match envelope.event_type.as_str() {
        "agent.completed" => {
            let result = envelope
                .payload
                .as_ref()
                .and_then(|payload| payload.get("result").and_then(serde_json::Value::as_str))
                .unwrap_or_default();
            let snapshot =
                persist_agent_terminal_state(&job.manifest, "completed", Some(result), None)?;
            global_agent_lifecycle_service().update_snapshot(
                snapshot,
                "agent.completed",
                String::from("Process JSONL agent completed"),
            );
            Ok(true)
        }
        "agent.failed" => {
            let error = envelope
                .payload
                .as_ref()
                .and_then(|payload| payload.get("error").and_then(serde_json::Value::as_str))
                .unwrap_or("process-jsonl backend failed")
                .to_string();
            let snapshot =
                persist_agent_terminal_state(&job.manifest, "failed", None, Some(error))?;
            global_agent_lifecycle_service().update_snapshot(
                snapshot,
                "agent.failed",
                String::from("Process JSONL agent failed"),
            );
            Ok(true)
        }
        "agent.cancelled" => {
            let snapshot = persist_agent_terminal_state(
                &job.manifest,
                "cancelled",
                None,
                Some(String::from("process-jsonl backend cancelled")),
            )?;
            global_agent_lifecycle_service().update_snapshot(
                snapshot,
                "agent.cancelled",
                String::from("Process JSONL agent cancelled"),
            );
            Ok(true)
        }
        _ => {
            global_agent_lifecycle_service().record_event(
                &envelope.agent_id,
                envelope.event_type.clone(),
                message,
            );
            Ok(false)
        }
    }
}

fn spawn_agent_job<T>(job: AgentJob, tool_executor: T) -> Result<(), String>
where
    T: ToolExecutor + Send + 'static,
{
    let thread_name = format!("cowd-agent-{}", job.manifest.agent_id);
    std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_agent_job(&job, tool_executor)
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    let cancelled = job.cancellation_token.is_cancelled()
                        || error.to_ascii_lowercase().contains("cancel");
                    let status = if cancelled { "cancelled" } else { "failed" };
                    let event_type = if cancelled {
                        "agent.cancelled"
                    } else {
                        "agent.failed"
                    };
                    let message = if cancelled {
                        String::from("Agent cancelled")
                    } else {
                        String::from("Agent execution failed")
                    };
                    if let Ok(failed) =
                        persist_agent_terminal_state(&job.manifest, status, None, Some(error))
                    {
                        global_agent_lifecycle_service()
                            .update_snapshot(failed, event_type, message);
                    }
                }
                Err(_) => {
                    if let Ok(failed) = persist_agent_terminal_state(
                        &job.manifest,
                        "failed",
                        None,
                        Some(String::from("sub-agent thread panicked")),
                    ) {
                        global_agent_lifecycle_service().update_snapshot(
                            failed,
                            "agent.failed",
                            String::from("Agent thread panicked"),
                        );
                    }
                }
            }
        })
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn run_agent_job<T>(job: &AgentJob, tool_executor: T) -> Result<(), String>
where
    T: ToolExecutor,
{
    let model = job
        .manifest
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_AGENT_MODEL.to_string());
    let summary = run_provider_subagent_turn(
        ProviderSubAgentTurnConfig {
            model,
            system_prompt: job.system_prompt.clone(),
            tool_definitions: job.tool_definitions.clone(),
            permission_policy: job.permission_policy.clone(),
            max_iterations: job.max_iterations,
            cancellation_token: Some(job.cancellation_token.clone()),
        },
        tool_executor,
        job.prompt.clone(),
    )?;
    let final_text = final_assistant_text(&summary);
    let completed =
        persist_agent_terminal_state(&job.manifest, "completed", Some(final_text.as_str()), None)?;
    global_agent_lifecycle_service().update_snapshot(
        completed,
        "agent.completed",
        String::from("Agent completed"),
    );
    Ok(())
}

pub fn build_agent_system_prompt(subagent_type: &str) -> Result<Vec<String>, String> {
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    let mut prompt = load_system_prompt(
        cwd,
        DEFAULT_AGENT_SYSTEM_DATE.to_string(),
        std::env::consts::OS,
        "unknown",
    )
    .map_err(|error| error.to_string())?;
    prompt.push(format!(
        "You are a background sub-agent of type `{subagent_type}`. Work only on the delegated task, use only the tools available to you, do not ask the user questions, and finish with a concise result."
    ));
    Ok(prompt)
}

pub fn resolve_agent_model(model: Option<&str>) -> String {
    model
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or(DEFAULT_AGENT_MODEL)
        .to_string()
}

pub fn normalize_subagent_type(subagent_type: Option<&str>) -> String {
    let trimmed = subagent_type.map(str::trim).unwrap_or_default();
    if trimmed.is_empty() {
        return String::from("general-purpose");
    }

    match canonical_tool_token(trimmed).as_str() {
        "general" | "generalpurpose" | "generalpurposeagent" => String::from("general-purpose"),
        "explore" | "explorer" | "exploreagent" => String::from("Explore"),
        "plan" | "planagent" => String::from("Plan"),
        "verification" | "verificationagent" | "verify" | "verifier" => {
            String::from("Verification")
        }
        "cowdguide" | "cowdguideagent" | "guide" => String::from("cowd-guide"),
        "statusline" | "statuslinesetup" => String::from("statusline-setup"),
        _ => trimmed.to_string(),
    }
}

pub fn slugify_agent_name(description: &str) -> String {
    let mut out = description
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while out.contains("--") {
        out = out.replace("--", "-");
    }
    out.trim_matches('-').chars().take(32).collect()
}

pub fn agent_store_dir() -> Result<PathBuf, String> {
    if let Ok(path) = std::env::var("COWD_AGENT_STORE") {
        return Ok(PathBuf::from(path));
    }
    let cwd = std::env::current_dir().map_err(|error| error.to_string())?;
    if let Some(workspace_root) = cwd.ancestors().nth(2) {
        return Ok(workspace_root.join(".cowd/agents"));
    }
    Ok(cwd.join(".cowd/agents"))
}

pub fn persist_agent_terminal_state(
    manifest: &AgentSnapshot,
    status: &str,
    result: Option<&str>,
    error: Option<String>,
) -> Result<AgentSnapshot, String> {
    let is_cancelled = status.eq_ignore_ascii_case("cancelled");
    let blocker = if is_cancelled {
        None
    } else {
        error.as_deref().map(classify_lane_blocker)
    };
    append_agent_output(
        &manifest.output_file,
        &format_agent_terminal_output(status, result, blocker.as_ref(), error.as_deref()),
    )?;
    let mut next_manifest = manifest.clone();
    next_manifest.status = status.to_string();
    next_manifest.completed_at = Some(iso8601_now());
    next_manifest.current_blocker.clone_from(&blocker);
    next_manifest.derived_state =
        derive_agent_state(status, result, error.as_deref(), blocker.as_ref()).to_string();
    next_manifest.error = error;
    if is_cancelled {
        next_manifest.current_blocker = None;
        next_manifest.derived_state = String::from("interrupted_transport");
        next_manifest.lane_events.push(
            LaneEvent::new(
                crate::LaneEventName::Closed,
                crate::LaneEventStatus::Closed,
                iso8601_now(),
            )
            .with_detail("Agent cancelled"),
        );
    } else if let Some(blocker) = blocker {
        next_manifest
            .lane_events
            .push(LaneEvent::blocked(iso8601_now(), &blocker));
        next_manifest
            .lane_events
            .push(LaneEvent::failed(iso8601_now(), &blocker));
    } else {
        next_manifest.current_blocker = None;
        let finished_summary = build_lane_finished_summary(&next_manifest, result);
        next_manifest.lane_events.push(
            LaneEvent::finished(iso8601_now(), finished_summary.detail).with_data(
                serde_json::to_value(&finished_summary.data)
                    .expect("lane summary metadata should serialize"),
            ),
        );
        if let Some(provenance) = maybe_commit_provenance(result) {
            next_manifest.lane_events.push(LaneEvent::commit_created(
                iso8601_now(),
                Some(format!("commit {}", provenance.commit)),
                provenance,
            ));
        }
    }
    write_agent_manifest(&next_manifest)?;
    Ok(next_manifest)
}

fn persist_agent_control_state(
    manifest: &AgentSnapshot,
    status: &str,
    detail: &str,
) -> Result<AgentSnapshot, String> {
    append_agent_output(
        &manifest.output_file,
        &format!("\n## Control\n\n- status: {status}\n- detail: {detail}\n"),
    )?;
    let mut next_manifest = manifest.clone();
    next_manifest.status = status.to_string();
    next_manifest.derived_state = String::from("interrupted_transport");
    next_manifest.lane_events.push(
        LaneEvent::new(
            crate::LaneEventName::Blocked,
            crate::LaneEventStatus::Blocked,
            iso8601_now(),
        )
        .with_detail(detail),
    );
    write_agent_manifest(&next_manifest)?;
    Ok(next_manifest)
}

fn is_terminal_or_cancel_requested_status(status: &str) -> bool {
    matches!(
        status.trim().to_ascii_lowercase().as_str(),
        "completed" | "failed" | "cancelled" | "cancel_requested"
    )
}

pub fn derive_agent_state(
    status: &str,
    result: Option<&str>,
    error: Option<&str>,
    blocker: Option<&LaneEventBlocker>,
) -> &'static str {
    let normalized_status = status.trim().to_ascii_lowercase();
    let normalized_error = error.unwrap_or_default().to_ascii_lowercase();

    if normalized_status == "running" {
        return "working";
    }
    if normalized_status == "completed" {
        return if result.is_some_and(|value| !value.trim().is_empty()) {
            "finished_cleanable"
        } else {
            "finished_pending_report"
        };
    }
    if normalized_status == "cancelled" || normalized_status == "cancel_requested" {
        return "interrupted_transport";
    }
    if normalized_error.contains("background") {
        return "blocked_background_job";
    }
    if normalized_error.contains("merge conflict") || normalized_error.contains("cherry-pick") {
        return "blocked_merge_conflict";
    }
    if normalized_error.contains("mcp") {
        return "degraded_mcp";
    }
    if normalized_error.contains("transport")
        || normalized_error.contains("broken pipe")
        || normalized_error.contains("connection")
        || normalized_error.contains("interrupted")
    {
        return "interrupted_transport";
    }
    if blocker.is_some() {
        return "truly_idle";
    }
    "truly_idle"
}

pub fn maybe_commit_provenance(result: Option<&str>) -> Option<LaneCommitProvenance> {
    let commit = extract_commit_sha(result?)?;
    let branch = current_git_branch().unwrap_or_else(|| "unknown".to_string());
    let worktree = std::env::current_dir()
        .ok()
        .map(|path| path.display().to_string());
    Some(LaneCommitProvenance {
        commit: commit.clone(),
        branch,
        worktree,
        canonical_commit: Some(commit.clone()),
        superseded_by: None,
        lineage: vec![commit],
    })
}

pub fn classify_lane_failure(error: &str) -> LaneFailureClass {
    let normalized = error.to_ascii_lowercase();

    if normalized.contains("prompt") && normalized.contains("deliver") {
        LaneFailureClass::PromptDelivery
    } else if normalized.contains("trust") {
        LaneFailureClass::TrustGate
    } else if normalized.contains("branch")
        && (normalized.contains("stale") || normalized.contains("diverg"))
    {
        LaneFailureClass::BranchDivergence
    } else if normalized.contains("gateway") || normalized.contains("routing") {
        LaneFailureClass::GatewayRouting
    } else if normalized.contains("compile")
        || normalized.contains("build failed")
        || normalized.contains("cargo check")
    {
        LaneFailureClass::Compile
    } else if normalized.contains("test") {
        LaneFailureClass::Test
    } else if normalized.contains("tool failed")
        || normalized.contains("runtime tool")
        || normalized.contains("tool runtime")
    {
        LaneFailureClass::ToolRuntime
    } else if normalized.contains("workspace") && normalized.contains("mismatch") {
        LaneFailureClass::WorkspaceMismatch
    } else if normalized.contains("plugin") {
        LaneFailureClass::PluginStartup
    } else if normalized.contains("mcp") && normalized.contains("handshake") {
        LaneFailureClass::McpHandshake
    } else if normalized.contains("mcp") {
        LaneFailureClass::McpStartup
    } else {
        LaneFailureClass::Infra
    }
}

pub fn iso8601_now() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

fn write_agent_manifest(manifest: &AgentSnapshot) -> Result<(), String> {
    let mut normalized = manifest.clone();
    normalized.lane_events = dedupe_superseded_commit_events(&normalized.lane_events);
    std::fs::write(
        &normalized.manifest_file,
        serde_json::to_string_pretty(&normalized).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())
}

fn classify_lane_blocker(error: &str) -> LaneEventBlocker {
    LaneEventBlocker {
        failure_class: classify_lane_failure(error),
        detail: error.trim().to_string(),
    }
}

fn append_agent_output(path: &str, suffix: &str) -> Result<(), String> {
    use std::io::Write as _;

    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|error| error.to_string())?;
    file.write_all(suffix.as_bytes())
        .map_err(|error| error.to_string())
}

fn format_agent_terminal_output(
    status: &str,
    result: Option<&str>,
    blocker: Option<&LaneEventBlocker>,
    error: Option<&str>,
) -> String {
    let mut sections = vec![format!("\n## Result\n\n- status: {status}\n")];
    if let Some(blocker) = blocker {
        sections.push(format!(
            "\n### Blocker\n\n- failure_class: {}\n- detail: {}\n",
            serde_json::to_string(&blocker.failure_class)
                .unwrap_or_else(|_| "\"infra\"".to_string())
                .trim_matches('"'),
            blocker.detail.trim()
        ));
    }
    if let Some(result) = result.filter(|value| !value.trim().is_empty()) {
        sections.push(format!("\n### Final response\n\n{}\n", result.trim()));
    }
    if let Some(error) = error.filter(|value| !value.trim().is_empty()) {
        sections.push(format!("\n### Error\n\n{}\n", error.trim()));
    }
    sections.join("")
}

#[derive(Debug, Clone, Serialize)]
struct LaneFinishedSummaryData {
    #[serde(rename = "qualityFloorApplied")]
    quality_floor_applied: bool,
    reasons: Vec<String>,
    #[serde(rename = "rawSummary", skip_serializing_if = "Option::is_none")]
    raw_summary: Option<String>,
    #[serde(rename = "wordCount")]
    word_count: usize,
}

#[derive(Debug, Clone)]
struct LaneFinishedSummary {
    detail: Option<String>,
    data: LaneFinishedSummaryData,
}

#[derive(Debug)]
struct LaneSummaryAssessment {
    apply_quality_floor: bool,
    reasons: Vec<String>,
    word_count: usize,
}

fn build_lane_finished_summary(
    manifest: &AgentSnapshot,
    result: Option<&str>,
) -> LaneFinishedSummary {
    let raw_summary = result.map(str::trim).filter(|value| !value.is_empty());
    let assessment = assess_lane_summary_quality(raw_summary.unwrap_or_default());
    let detail = match raw_summary {
        Some(summary) if !assessment.apply_quality_floor => Some(compress_summary_text(summary)),
        Some(summary) => Some(compose_lane_summary_fallback(manifest, Some(summary))),
        None => Some(compose_lane_summary_fallback(manifest, None)),
    };

    LaneFinishedSummary {
        detail,
        data: LaneFinishedSummaryData {
            quality_floor_applied: raw_summary.is_none() || assessment.apply_quality_floor,
            reasons: assessment.reasons,
            raw_summary: raw_summary.map(str::to_string),
            word_count: assessment.word_count,
        },
    }
}

fn assess_lane_summary_quality(summary: &str) -> LaneSummaryAssessment {
    let words = summary
        .split(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '-' || ch == '#'))
        .filter(|token| !token.is_empty())
        .map(str::to_ascii_lowercase)
        .collect::<Vec<_>>();

    let word_count = words.len();
    let mut reasons = Vec::new();
    if summary.trim().is_empty() {
        reasons.push(String::from("empty"));
    }

    let control_only = !words.is_empty()
        && words
            .iter()
            .all(|word| CONTROL_ONLY_SUMMARY_WORDS.contains(&word.as_str()));
    if control_only {
        reasons.push(String::from("control_only"));
    }

    let has_context_signal = summary.contains('`')
        || summary.contains('/')
        || summary.contains(':')
        || summary.contains('#')
        || words
            .iter()
            .any(|word| CONTEXTUAL_SUMMARY_WORDS.contains(&word.as_str()));
    if word_count < MIN_LANE_SUMMARY_WORDS && !has_context_signal {
        reasons.push(String::from("too_short_without_context"));
    }

    LaneSummaryAssessment {
        apply_quality_floor: !reasons.is_empty(),
        reasons,
        word_count,
    }
}

fn compose_lane_summary_fallback(manifest: &AgentSnapshot, raw_summary: Option<&str>) -> String {
    let target = manifest.description.trim();
    let base = format!(
        "Completed lane `{}` for target: {}. Status: completed.",
        manifest.name,
        if target.is_empty() {
            "unspecified task"
        } else {
            target
        }
    );
    match raw_summary {
        Some(summary) => format!(
            "{base} Original stop summary was too vague to keep as the lane result: \"{}\".",
            summary.trim()
        ),
        None => format!("{base} No usable stop summary was produced by the lane."),
    }
}

fn canonical_tool_token(value: &str) -> String {
    let mut canonical = value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect::<String>();
    if let Some(stripped) = canonical.strip_suffix("tool") {
        canonical = stripped.to_string();
    }
    canonical
}

fn make_agent_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("agent-{nanos}")
}

fn extract_commit_sha(result: &str) -> Option<String> {
    result
        .split(|c: char| !c.is_ascii_hexdigit())
        .find(|token| token.len() >= 7 && token.len() <= 40)
        .map(str::to_string)
}

fn current_git_branch() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

const MIN_LANE_SUMMARY_WORDS: usize = 7;
const CONTROL_ONLY_SUMMARY_WORDS: &[&str] = &[
    "ack",
    "commit",
    "continue",
    "everyting",
    "everything",
    "keep",
    "next",
    "push",
    "ralph",
    "resume",
    "retry",
    "run",
    "stop",
    "sweep",
    "sweeping",
    "team",
];
const CONTEXTUAL_SUMMARY_WORDS: &[&str] = &[
    "added",
    "audited",
    "documented",
    "failed",
    "finished",
    "fixed",
    "implemented",
    "investigated",
    "merged",
    "pushed",
    "refactored",
    "removed",
    "reviewed",
    "tested",
    "updated",
    "verified",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_builtin_subagent_aliases() {
        assert_eq!(normalize_subagent_type(None), "general-purpose");
        assert_eq!(normalize_subagent_type(Some("explorer")), "Explore");
        assert_eq!(normalize_subagent_type(Some("plan-agent")), "Plan");
        assert_eq!(normalize_subagent_type(Some("verifier")), "Verification");
    }

    #[test]
    fn agent_state_classification_covers_finished_and_specific_blockers() {
        assert_eq!(derive_agent_state("running", None, None, None), "working");
        assert_eq!(
            derive_agent_state("completed", Some("done"), None, None),
            "finished_cleanable"
        );
        assert_eq!(
            derive_agent_state("completed", None, None, None),
            "finished_pending_report"
        );
        assert_eq!(
            derive_agent_state("failed", None, Some("mcp handshake timed out"), None),
            "degraded_mcp"
        );
        assert_eq!(
            derive_agent_state(
                "failed",
                None,
                Some("background terminal still running"),
                None
            ),
            "blocked_background_job"
        );
        assert_eq!(
            derive_agent_state("failed", None, Some("merge conflict while rebasing"), None),
            "blocked_merge_conflict"
        );
        assert_eq!(
            derive_agent_state(
                "failed",
                None,
                Some("transport interrupted after partial progress"),
                None
            ),
            "interrupted_transport"
        );
    }

    #[test]
    fn lane_failure_taxonomy_normalizes_common_blockers() {
        let cases = [
            (
                "prompt delivery failed in tmux pane",
                LaneFailureClass::PromptDelivery,
            ),
            (
                "trust prompt is still blocking startup",
                LaneFailureClass::TrustGate,
            ),
            (
                "branch stale against main after divergence",
                LaneFailureClass::BranchDivergence,
            ),
            (
                "compile failed after cargo check",
                LaneFailureClass::Compile,
            ),
            ("targeted tests failed", LaneFailureClass::Test),
            ("plugin bootstrap failed", LaneFailureClass::PluginStartup),
            ("mcp handshake timed out", LaneFailureClass::McpHandshake),
            (
                "mcp startup failed before listing tools",
                LaneFailureClass::McpStartup,
            ),
            (
                "gateway routing rejected the request",
                LaneFailureClass::GatewayRouting,
            ),
            (
                "tool failed: denied tool execution from hook",
                LaneFailureClass::ToolRuntime,
            ),
            (
                "workspace mismatch while resuming the managed session",
                LaneFailureClass::WorkspaceMismatch,
            ),
            ("thread creation failed", LaneFailureClass::Infra),
        ];

        for (message, expected) in cases {
            assert_eq!(classify_lane_failure(message), expected, "{message}");
        }
    }

    #[test]
    fn lifecycle_service_projects_events_and_cancel_token() {
        let service = AgentLifecycleService::new();
        let token = CancellationToken::new();
        let dir = std::env::temp_dir().join(format!("cowd-agent-lifecycle-{}", make_agent_id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let output_file = dir.join("agent-test.md");
        let manifest_file = dir.join("agent-test.json");
        std::fs::write(&output_file, "# Agent Task\n").expect("output");
        let snapshot = AgentSnapshot {
            agent_id: String::from("agent-test"),
            name: String::from("test-agent"),
            description: String::from("test"),
            subagent_type: Some(String::from("Explore")),
            model: Some(String::from(DEFAULT_AGENT_MODEL)),
            status: String::from("running"),
            backend: AgentExecutionBackendKind::InProcess,
            output_file: output_file.display().to_string(),
            manifest_file: manifest_file.display().to_string(),
            created_at: String::from("1"),
            started_at: Some(String::from("1")),
            completed_at: None,
            lane_events: vec![],
            current_blocker: None,
            derived_state: String::from("working"),
            error: None,
        };

        service.register_started(snapshot.clone(), token.clone());
        assert_eq!(service.list().len(), 1);
        assert_eq!(
            service.events("agent-test").expect("events")[0].event_type,
            "agent.started"
        );

        let receipt = service.cancel("agent-test").expect("cancel receipt");
        assert_eq!(receipt.status, "accepted");
        assert!(token.is_cancelled());
        assert_eq!(
            service.get("agent-test").expect("snapshot").status,
            "cancel_requested"
        );
        assert_eq!(
            service.events("agent-test").expect("events")[1].event_type,
            "agent.cancel_requested"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn lifecycle_cancel_does_not_rewrite_terminal_agents() {
        let service = AgentLifecycleService::new();
        let token = CancellationToken::new();
        let dir = std::env::temp_dir().join(format!("cowd-agent-lifecycle-{}", make_agent_id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let output_file = dir.join("agent-terminal.md");
        let manifest_file = dir.join("agent-terminal.json");
        std::fs::write(&output_file, "# Agent Task\n").expect("output");
        let snapshot = AgentSnapshot {
            agent_id: String::from("agent-terminal"),
            name: String::from("terminal-agent"),
            description: String::from("test"),
            subagent_type: Some(String::from("Explore")),
            model: Some(String::from(DEFAULT_AGENT_MODEL)),
            status: String::from("completed"),
            backend: AgentExecutionBackendKind::InProcess,
            output_file: output_file.display().to_string(),
            manifest_file: manifest_file.display().to_string(),
            created_at: String::from("1"),
            started_at: Some(String::from("1")),
            completed_at: Some(String::from("2")),
            lane_events: vec![],
            current_blocker: None,
            derived_state: String::from("finished_cleanable"),
            error: None,
        };
        write_agent_manifest(&snapshot).expect("manifest");
        service.register_started(snapshot, token.clone());

        let receipt = service.cancel("agent-terminal").expect("cancel receipt");
        assert_eq!(receipt.status, "noop");
        assert!(!token.is_cancelled());
        assert_eq!(
            service.get("agent-terminal").expect("snapshot").status,
            "completed"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn process_jsonl_backend_runs_external_agent_and_projects_terminal_state() {
        let dir = std::env::temp_dir().join(format!("cowd-agent-jsonl-{}", make_agent_id()));
        let script = r#"read line
printf '{"agentId":"%s","backend":"process-jsonl","eventType":"agent.heartbeat","emittedAt":"1","payload":{"message":"alive"}}\n' "$COWD_AGENT_ID"
printf '{"agentId":"%s","backend":"process-jsonl","eventType":"agent.completed","emittedAt":"2","payload":{"result":"completed from jsonl backend"}}\n' "$COWD_AGENT_ID"
"#;
        let request = SpawnAgentRequest {
            description: "Run external JSONL agent".to_string(),
            prompt: "Finish through a process backend.".to_string(),
            subagent_type: Some("Explore".to_string()),
            name: Some("jsonl-agent".to_string()),
            model: Some(DEFAULT_AGENT_MODEL.to_string()),
            system_prompt: vec!["system".to_string()],
            allowed_tools: BTreeSet::new(),
            tool_definitions: Vec::new(),
            permission_policy: PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            max_iterations: 1,
            store_dir: Some(dir.clone()),
            backend: AgentExecutionBackendKind::ProcessJsonl,
            process_jsonl: Some(AgentProcessJsonlSpec {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script.to_string()],
                cwd: None,
                env: BTreeMap::new(),
            }),
        };

        let manifest =
            spawn_provider_agent(request, crate::StaticToolExecutor::default()).expect("spawn");
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let status = global_agent_lifecycle_service()
                .get(&manifest.agent_id)
                .map(|snapshot| snapshot.status)
                .unwrap_or_default();
            if status == "completed" {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "process-jsonl agent did not complete; last status={status}"
            );
            std::thread::sleep(Duration::from_millis(25));
        }

        let events = global_agent_lifecycle_service()
            .events(&manifest.agent_id)
            .expect("events");
        assert!(events
            .iter()
            .any(|event| event.event_type == "agent.heartbeat"));
        let output = std::fs::read_to_string(&manifest.output_file).expect("output");
        assert!(output.contains("completed from jsonl backend"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn process_jsonl_backend_records_crash_failure_reason() {
        let dir = std::env::temp_dir().join(format!("cowd-agent-jsonl-crash-{}", make_agent_id()));
        let request = SpawnAgentRequest {
            description: "Run crashing JSONL agent".to_string(),
            prompt: "Crash before a terminal event.".to_string(),
            subagent_type: Some("Explore".to_string()),
            name: Some("jsonl-crash-agent".to_string()),
            model: Some(DEFAULT_AGENT_MODEL.to_string()),
            system_prompt: Vec::new(),
            allowed_tools: BTreeSet::new(),
            tool_definitions: Vec::new(),
            permission_policy: PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            max_iterations: 1,
            store_dir: Some(dir.clone()),
            backend: AgentExecutionBackendKind::ProcessJsonl,
            process_jsonl: Some(AgentProcessJsonlSpec {
                command: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "read line; echo backend crashed >&2; exit 42".to_string(),
                ],
                cwd: None,
                env: BTreeMap::new(),
            }),
        };

        let manifest =
            spawn_provider_agent(request, crate::StaticToolExecutor::default()).expect("spawn");
        let snapshot = wait_for_agent_status(&manifest.agent_id, "failed");
        assert!(snapshot
            .error
            .as_deref()
            .unwrap_or_default()
            .contains("backend crashed"));
        let output = std::fs::read_to_string(&manifest.output_file).expect("output");
        assert!(output.contains("process-jsonl backend exited before terminal event"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn process_jsonl_backend_delivers_runtime_input_commands() {
        let dir = std::env::temp_dir().join(format!("cowd-agent-jsonl-input-{}", make_agent_id()));
        let script = r#"read spawn
read input
escaped=$(printf '%s' "$input" | sed 's/\\/\\\\/g; s/"/\\"/g')
printf '{"agentId":"%s","backend":"process-jsonl","eventType":"agent.completed","emittedAt":"2","payload":{"result":"received command: %s"}}\n' "$COWD_AGENT_ID" "$escaped"
"#;
        let request = SpawnAgentRequest {
            description: "Run input-aware JSONL agent".to_string(),
            prompt: "Wait for input.".to_string(),
            subagent_type: Some("Explore".to_string()),
            name: Some("jsonl-input-agent".to_string()),
            model: Some(DEFAULT_AGENT_MODEL.to_string()),
            system_prompt: Vec::new(),
            allowed_tools: BTreeSet::new(),
            tool_definitions: Vec::new(),
            permission_policy: PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            max_iterations: 1,
            store_dir: Some(dir.clone()),
            backend: AgentExecutionBackendKind::ProcessJsonl,
            process_jsonl: Some(AgentProcessJsonlSpec {
                command: "sh".to_string(),
                args: vec!["-c".to_string(), script.to_string()],
                cwd: None,
                env: BTreeMap::new(),
            }),
        };

        let manifest =
            spawn_provider_agent(request, crate::StaticToolExecutor::default()).expect("spawn");
        let receipt = global_agent_lifecycle_service()
            .command(
                &manifest.agent_id,
                AgentExecutionCommandKind::Input,
                Some(serde_json::json!({"text": "continue"})),
            )
            .expect("input command");
        assert_eq!(receipt.status, "accepted");
        let snapshot = wait_for_agent_status(&manifest.agent_id, "completed");
        assert_eq!(snapshot.status, "completed");
        let output = std::fs::read_to_string(&manifest.output_file).expect("output");
        assert!(output.contains("continue"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn process_jsonl_backend_cancel_kills_external_agent() {
        let dir = std::env::temp_dir().join(format!("cowd-agent-jsonl-cancel-{}", make_agent_id()));
        let request = SpawnAgentRequest {
            description: "Run cancellable JSONL agent".to_string(),
            prompt: "Wait until cancelled.".to_string(),
            subagent_type: Some("Explore".to_string()),
            name: Some("jsonl-cancel-agent".to_string()),
            model: Some(DEFAULT_AGENT_MODEL.to_string()),
            system_prompt: Vec::new(),
            allowed_tools: BTreeSet::new(),
            tool_definitions: Vec::new(),
            permission_policy: PermissionPolicy::new(crate::PermissionMode::ReadOnly),
            max_iterations: 1,
            store_dir: Some(dir.clone()),
            backend: AgentExecutionBackendKind::ProcessJsonl,
            process_jsonl: Some(AgentProcessJsonlSpec {
                command: "sh".to_string(),
                args: vec![
                    "-c".to_string(),
                    "read line; while true; do sleep 1; done".to_string(),
                ],
                cwd: None,
                env: BTreeMap::new(),
            }),
        };

        let manifest =
            spawn_provider_agent(request, crate::StaticToolExecutor::default()).expect("spawn");
        let receipt = global_agent_lifecycle_service()
            .cancel(&manifest.agent_id)
            .expect("cancel");
        assert_eq!(receipt.status, "accepted");
        let snapshot = wait_for_agent_status(&manifest.agent_id, "cancelled");
        assert_eq!(snapshot.derived_state, "interrupted_transport");
        let _ = std::fs::remove_dir_all(dir);
    }

    fn wait_for_agent_status(agent_id: &str, expected: &str) -> AgentSnapshot {
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        loop {
            let snapshot = global_agent_lifecycle_service()
                .get(agent_id)
                .expect("agent snapshot");
            if snapshot.status == expected {
                return snapshot;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "agent did not reach {expected}; last status={}",
                snapshot.status
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}
