use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use chrono::Utc;

use crate::gateway::ActiveSessions;
use crate::runtime_boundary::{
    RuntimeBoundaryClock, RuntimeBoundarySnapshot, RuntimeBoundaryStatus,
};
use crate::runtime_protocol::{RuntimeErrorKind, RuntimeRequest, RuntimeResponse};
use crate::session_kernel::SessionKernel;
use crate::session_lifecycle_kernel::SessionLifecycleKernel;
use harness_contract::{
    task::{TaskId, TaskTurnBinding},
    turn::{
        TurnEvent, TurnId, TurnInput, TurnJournalEnvelope, TurnJournalPhase, TurnReceipt,
        TurnStatus,
    },
};
use runtime::agent_collaboration::CollaborationContextResult;
use session::SessionLeaseRegistry;
use tokio::time::timeout;

use crate::services::{
    ActiveMessagesPage, SessionCompactResult, SessionMessageCounts, SessionStatsSnapshot,
    SessionTokenCounts,
};

#[derive(Debug)]
pub(crate) enum RuntimeTurnExecutionError {
    NotFound(String),
    Timeout { seconds: u64 },
    Runtime(String),
    Join(String),
}

impl RuntimeTurnExecutionError {
    pub(crate) fn message(&self) -> String {
        match self {
            Self::NotFound(message) | Self::Runtime(message) | Self::Join(message) => {
                message.clone()
            }
            Self::Timeout { seconds } => format!("turn timed out after {seconds}s"),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnExecution {
    pub(crate) summary: runtime::TurnSummary,
    pub(crate) receipt: TurnReceipt,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeTurnOptions {
    pub(crate) profile: runtime::ContextProfile,
    pub(crate) max_iterations: Option<usize>,
    pub(crate) pre_messages: Vec<runtime::ConversationMessage>,
}

impl Default for RuntimeTurnOptions {
    fn default() -> Self {
        Self {
            profile: runtime::ContextProfile::MainTurn,
            max_iterations: None,
            pre_messages: Vec::new(),
        }
    }
}

trait RuntimeTurnBudgetTarget {
    fn max_iterations(&self) -> usize;
    fn set_max_iterations(&mut self, max_iterations: usize);
}

impl RuntimeTurnBudgetTarget for crate::runtime_entry::GatewayRuntimeEntry {
    fn max_iterations(&self) -> usize {
        self.max_iterations()
    }

    fn set_max_iterations(&mut self, max_iterations: usize) {
        self.set_max_iterations(max_iterations);
    }
}

fn apply_scoped_max_iterations<T: RuntimeTurnBudgetTarget>(
    runtime: &mut T,
    max_iterations: Option<usize>,
) -> Option<usize> {
    let previous = max_iterations.map(|_| runtime.max_iterations());
    if let Some(max_iterations) = max_iterations {
        runtime.set_max_iterations(max_iterations);
    }
    previous
}

fn restore_scoped_max_iterations<T: RuntimeTurnBudgetTarget>(
    runtime: &mut T,
    previous: Option<usize>,
) {
    if let Some(previous) = previous {
        runtime.set_max_iterations(previous);
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeService {
    sessions: Arc<ActiveSessions>,
    lease_registry: Arc<SessionLeaseRegistry>,
    session_kernel: Arc<SessionKernel>,
    lifecycle_kernel: Arc<SessionLifecycleKernel>,
    started_at: Instant,
    turns: Arc<Mutex<BTreeMap<String, TurnReceipt>>>,
    turn_bindings: Arc<Mutex<BTreeMap<String, TaskTurnBinding>>>,
}

impl RuntimeService {
    #[must_use]
    pub(crate) fn new(
        sessions: Arc<ActiveSessions>,
        lease_registry: Arc<SessionLeaseRegistry>,
        session_kernel: Arc<SessionKernel>,
        lifecycle_kernel: Arc<SessionLifecycleKernel>,
        started_at: Instant,
    ) -> Self {
        Self {
            sessions,
            lease_registry,
            session_kernel,
            lifecycle_kernel,
            started_at,
            turns: Arc::new(Mutex::new(BTreeMap::new())),
            turn_bindings: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    #[must_use]
    pub(crate) fn status_value(&self) -> serde_json::Value {
        let status = self.status();
        serde_json::json!({
            "ok": true,
            "protocol_version": status.protocol_version,
            "runtime_host": status.runtime_host,
            "active_sessions": status.active_sessions,
            "uptime_secs": status.uptime_secs,
        })
    }

    #[must_use]
    pub(crate) fn session_kernel(&self) -> Arc<SessionKernel> {
        self.session_kernel.clone()
    }

    #[must_use]
    pub(crate) fn lifecycle_kernel(&self) -> Arc<SessionLifecycleKernel> {
        self.lifecycle_kernel.clone()
    }

    #[must_use]
    pub(crate) fn status(&self) -> RuntimeBoundaryStatus {
        RuntimeBoundaryStatus {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: self.sessions.list().len(),
            uptime_secs: self.clock().uptime_secs(),
        }
    }

    pub(crate) async fn snapshot_value(&self) -> serde_json::Value {
        let snapshot = self.snapshot().await;
        let leases = self.lease_registry.list().await;
        let turns = self.turns_snapshot();
        let turn_bindings = self.turn_bindings_snapshot();
        serde_json::json!({
            "ok": true,
            "kind": "gateway_runtime_snapshot",
            "protocol_version": snapshot.protocol_version,
            "runtime_host": snapshot.runtime_host,
            "active_sessions": snapshot.active_sessions,
            "uptime_secs": snapshot.uptime_secs,
            "sessions": snapshot.sessions,
            "leases": {
                "total": leases.len(),
                "items": leases,
            },
            "lifecycle": self.lifecycle_kernel.snapshots().await,
            "turns": turns,
            "turn_bindings": turn_bindings,
            "transport": {
                "control": "gateway_http",
                "projection": "http_optional",
            },
        })
    }

    pub(crate) async fn submit_turn_value(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> serde_json::Value {
        if prompt.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "prompt is required",
            });
        }

        let input = Self::turn_input_for(session_id, task_id, prompt);
        let receipt = self.record_turn_from_input(&input, TurnStatus::Pending);
        let turn_id = input.turn_id.to_string();
        let journal_sequence = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist submitted turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "dispatch": "runtime_service",
            "accepted": true,
            "durable_journal": journal_sequence.is_some(),
            "journal_sequence": journal_sequence,
            "turn": receipt,
        })
    }

    pub(crate) fn turn_value(&self, turn_id: &str) -> serde_json::Value {
        let turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match turns.get(turn_id) {
            Some(turn) => serde_json::json!({
                "ok": true,
                "turn": turn,
            }),
            None => serde_json::json!({
                "ok": false,
                "error": "turn not found",
            }),
        }
    }

    pub(crate) fn turns_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "turns": self.turns_snapshot(),
            "turn_bindings": self.turn_bindings_snapshot(),
        })
    }

    pub(crate) async fn cancel_turn_value(&self, turn_id: &str) -> serde_json::Value {
        let (turn, aborted_run_id) = {
            let mut turns = self
                .turns
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(turn) = turns.get_mut(turn_id) else {
                return serde_json::json!({
                    "ok": false,
                    "error": "turn not found",
                });
            };

            turn.status = TurnStatus::Cancelled;
            turn.events.push(TurnEvent::new(
                TurnId::from_string(turn_id.to_string()),
                TurnStatus::Cancelled,
            ));
            let aborted_run_id = turn
                .session_id
                .as_deref()
                .and_then(crate::api_routes::abort_active_turn);
            (turn.clone(), aborted_run_id)
        };
        let journal_sequence = self
            .persist_turn_receipt_journal(&turn, TurnJournalPhase::Cancelled, None)
            .await
            .transpose()
            .map_err(|error| {
                tracing::warn!(
                    turn_id = %turn_id,
                    error = %error,
                    "failed to persist cancelled turn journal"
                );
                error
            })
            .ok()
            .flatten();

        serde_json::json!({
            "ok": true,
            "cancelled": true,
            "aborted_run_id": aborted_run_id,
            "journal_sequence": journal_sequence,
            "turn": turn,
        })
    }

    async fn persist_turn_input_journal(
        &self,
        input: &TurnInput,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = input
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            input.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": phase.as_str(),
                "prompt": input.prompt.clone(),
                "prompt_preview": input.prompt.chars().take(240).collect::<String>(),
                "task_id": input.task_id.clone(),
                "message": message,
                "created_at": input.created_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    async fn persist_turn_receipt_journal(
        &self,
        receipt: &TurnReceipt,
        phase: TurnJournalPhase,
        message: Option<String>,
    ) -> Option<Result<Option<usize>, memory::MemoryError>> {
        let session_id = receipt
            .session_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())?;
        let envelope = TurnJournalEnvelope::new(
            session_id,
            receipt.turn_id.clone(),
            phase,
            "gateway.runtime_service",
            serde_json::json!({
                "status": receipt.status.as_str(),
                "task_id": receipt.task_id.clone(),
                "context_report_id": receipt.context_report_id.clone(),
                "message": message,
                "completed_at": receipt.completed_at,
            }),
        );
        Some(
            self.session_kernel
                .append_turn_journal_event(session_id, envelope)
                .await,
        )
    }

    fn turn_input_for(
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnInput {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        input
    }

    fn record_turn_from_input(&self, input: &TurnInput, status: TurnStatus) -> TurnReceipt {
        let mut receipt = TurnReceipt::from_input(input, status.clone());
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), status));
        self.record_turn_binding(input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn turns_snapshot(&self) -> Vec<TurnReceipt> {
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    fn turn_bindings_snapshot(&self) -> Vec<TaskTurnBinding> {
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn has_active_session(&self, session_id: &str) -> bool {
        self.sessions.get(session_id).is_some()
    }

    pub(crate) fn register_runtime(
        &self,
        session_id: String,
        runtime: crate::runtime_entry::GatewayRuntimeEntry,
    ) -> Result<Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>>, String>
    {
        self.sessions.register(session_id, runtime)
    }

    pub(crate) fn remove_active_runtime(
        &self,
        session_id: &str,
    ) -> Option<Arc<tokio::sync::Mutex<crate::runtime_entry::GatewayRuntimeEntry>>> {
        self.sessions.remove(session_id)
    }

    pub(crate) fn remove_active_runtime_if_present(&self, session_id: &str) -> bool {
        self.remove_active_runtime(session_id).is_some()
    }

    pub(crate) async fn cowd_event_receiver(
        &self,
        session_id: &str,
    ) -> Option<tokio::sync::broadcast::Receiver<runtime::CowdEvent>> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.cowd_bus().map(|bus| bus.subscribe())
    }

    pub(crate) async fn configure_turn_context(
        &self,
        session_id: &str,
        profile: runtime::ContextProfile,
        resume_context: Option<runtime::ResumeContextPacket>,
        reality_context_items: Vec<runtime::ContextItem>,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(profile);
        runtime_guard.replace_external_context_sources(
            &[
                runtime::ContextSourceKind::Fact,
                runtime::ContextSourceKind::Matrix,
            ],
            reality_context_items,
        );
        if let Some(packet) = resume_context {
            runtime_guard.inject_resume_context(packet);
        }
        Ok(())
    }

    pub(crate) async fn install_turn_control(
        &self,
        session_id: &str,
        cancellation_token: runtime::CancellationToken,
        hook_abort_signal: runtime::HookAbortSignal,
    ) -> Result<(), RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.install_turn_control(cancellation_token, hook_abort_signal);
        Ok(())
    }

    pub(crate) async fn run_turn_with_timeout(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
        turn_timeout: Duration,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        self.run_turn_with_options(
            session_id,
            task_id,
            content,
            turn_timeout,
            RuntimeTurnOptions::default(),
        )
        .await
    }

    pub(crate) async fn run_turn_with_options(
        &self,
        session_id: &str,
        task_id: Option<String>,
        content: String,
        turn_timeout: Duration,
        options: RuntimeTurnOptions,
    ) -> Result<RuntimeTurnExecution, RuntimeTurnExecutionError> {
        let runtime_entry = self.sessions.get(session_id).ok_or_else(|| {
            RuntimeTurnExecutionError::NotFound(format!("session {session_id} not found"))
        })?;
        let input = Self::turn_input_for(Some(session_id.to_string()), task_id, content.clone());
        self.record_turn_from_input(&input, TurnStatus::Pending);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Submitted, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist submitted turn journal: {error}"
            )));
        }
        let receipt = self.record_turn_from_input(&input, TurnStatus::Running);
        if let Some(Err(error)) = self
            .persist_turn_input_journal(&input, TurnJournalPhase::Running, None)
            .await
        {
            return Err(RuntimeTurnExecutionError::Runtime(format!(
                "failed to persist running turn journal: {error}"
            )));
        }
        let turn_id = receipt.turn_id.clone();
        let turn_result = tokio::task::spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                let mut runtime_guard = runtime_entry.lock().await;
                runtime_guard.set_context_profile(options.profile);
                let previous_max_iterations =
                    apply_scoped_max_iterations(&mut *runtime_guard, options.max_iterations);
                for message in options.pre_messages {
                    if let Err(error) = runtime_guard.append_external_message(message).await {
                        restore_scoped_max_iterations(&mut *runtime_guard, previous_max_iterations);
                        return Ok(Err(error));
                    }
                }
                let result = timeout(
                    turn_timeout,
                    runtime_guard
                        .run_turn_async(&content, &runtime::permissions::SharedPrompter::none()),
                )
                .await;
                restore_scoped_max_iterations(&mut *runtime_guard, previous_max_iterations);
                result
            })
        })
        .await
        .map_err(|error| RuntimeTurnExecutionError::Join(format!("task join error: {error}")))?;

        match turn_result {
            Ok(Ok(summary)) => {
                let mut receipt = self.finish_turn(&turn_id, TurnStatus::Completed, None);
                receipt.context_report_id = Some(summary.context_turn_report.turn_id.clone());
                self.turns
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(turn_id.to_string(), receipt.clone());
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(&receipt, TurnJournalPhase::Completed, None)
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist completed turn journal"
                    );
                }
                Ok(RuntimeTurnExecution { summary, receipt })
            }
            Ok(Err(error)) => {
                let message = error.to_string();
                let receipt = self.finish_turn(&turn_id, TurnStatus::Failed, Some(message.clone()));
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(
                        &receipt,
                        TurnJournalPhase::Failed,
                        Some(message.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist failed turn journal"
                    );
                }
                Err(RuntimeTurnExecutionError::Runtime(message))
            }
            Err(_) => {
                let message = format!("turn timed out after {}s", turn_timeout.as_secs());
                let receipt = self.finish_turn(&turn_id, TurnStatus::Failed, Some(message.clone()));
                if let Some(Err(error)) = self
                    .persist_turn_receipt_journal(
                        &receipt,
                        TurnJournalPhase::Failed,
                        Some(message.clone()),
                    )
                    .await
                {
                    tracing::warn!(
                        turn_id = %turn_id,
                        error = %error,
                        "failed to persist timeout turn journal"
                    );
                }
                Err(RuntimeTurnExecutionError::Timeout {
                    seconds: turn_timeout.as_secs(),
                })
            }
        }
    }

    fn start_running_turn(
        &self,
        session_id: Option<String>,
        task_id: Option<String>,
        prompt: String,
    ) -> TurnReceipt {
        let mut input = TurnInput::new(prompt);
        input.session_id = session_id;
        input.task_id = task_id;
        let mut receipt = TurnReceipt::from_input(&input, TurnStatus::Running);
        receipt
            .events
            .push(TurnEvent::new(input.turn_id.clone(), TurnStatus::Running));
        self.record_turn_binding(&input);
        self.turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), receipt.clone());
        receipt
    }

    fn record_turn_binding(&self, input: &TurnInput) {
        let Some(task_id) = input
            .task_id
            .as_ref()
            .filter(|value| !value.trim().is_empty())
        else {
            return;
        };
        let mut binding =
            TaskTurnBinding::new(TaskId::from_string(task_id.clone()), input.turn_id.clone());
        binding.session_id = input.session_id.clone();
        self.turn_bindings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(input.turn_id.to_string(), binding);
    }

    fn finish_turn(
        &self,
        turn_id: &TurnId,
        status: TurnStatus,
        message: Option<String>,
    ) -> TurnReceipt {
        let mut turns = self
            .turns
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let turn = turns
            .entry(turn_id.to_string())
            .or_insert_with(|| TurnReceipt {
                turn_id: turn_id.clone(),
                status: status.clone(),
                session_id: None,
                task_id: None,
                events: Vec::new(),
                context_report_id: None,
                completed_at: None,
            });

        if turn.status != TurnStatus::Cancelled {
            turn.status = status.clone();
        }
        let mut event = TurnEvent::new(turn_id.clone(), status);
        event.message = message;
        turn.events.push(event);
        turn.completed_at = Some(Utc::now());
        turn.clone()
    }

    pub(crate) async fn session_snapshot(&self, session_id: &str) -> Option<runtime::Session> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        Some(runtime_guard.session().clone())
    }

    pub(crate) async fn sync_session_snapshot(
        &self,
        session_id: &str,
        session: &runtime::Session,
    ) -> Result<(), memory::MemoryError> {
        self.session_kernel
            .sync_runtime_session_snapshot(session_id, session)
            .await
            .map(|_| ())
    }

    pub(crate) async fn compact_active_session(
        &self,
        session_id: &str,
    ) -> Result<Option<SessionCompactResult>, memory::MemoryError> {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return Ok(None);
        };

        let mut runtime_guard = runtime_entry.lock().await;
        let (result, session_snapshot) =
            runtime_guard.compact_active_session(runtime::CompactionConfig::default());
        drop(runtime_guard);

        self.sync_session_snapshot(session_id, &session_snapshot)
            .await?;

        Ok(Some(SessionCompactResult {
            session_id: session_id.to_string(),
            compacted: result.removed_message_count > 0,
            removed_message_count: result.removed_message_count,
            summary: result.formatted_summary,
        }))
    }

    pub(crate) async fn active_session_stats(
        &self,
        session_id: &str,
    ) -> Option<SessionStatsSnapshot> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        let session = runtime_guard.active_session_stats_session();
        let messages = &session.messages;

        let user_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::User)
            .count();
        let assistant_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Assistant)
            .count();
        let tool_count = messages
            .iter()
            .filter(|message| message.role == runtime::MessageRole::Tool)
            .count();

        let input: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.input_tokens)
            .sum();
        let output: u32 = messages
            .iter()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| usage.output_tokens)
            .sum();

        let mut tool_usage = HashMap::new();
        for message in messages {
            if message.role == runtime::MessageRole::Assistant {
                for block in &message.blocks {
                    if let runtime::ContentBlock::ToolUse { name, .. } = block {
                        *tool_usage.entry(name.clone()).or_insert(0) += 1;
                    }
                }
            }
        }

        Some(SessionStatsSnapshot {
            session_id: session_id.to_string(),
            message_count: messages.len(),
            message_counts: SessionMessageCounts {
                user: user_count,
                assistant: assistant_count,
                tool: tool_count,
            },
            tokens: SessionTokenCounts {
                input,
                output,
                total: input + output,
            },
            tool_usage,
            duration_ms: session.updated_at_ms.saturating_sub(session.created_at_ms),
        })
    }

    pub(crate) fn last_context_envelope_nonblocking(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let envelope = match runtime_entry.try_lock() {
            Ok(runtime) => runtime.last_context_envelope(),
            Err(_) => {
                tracing::debug!(
                    %session_id,
                    "runtime context envelope skipped because active runtime is busy"
                );
                None
            }
        };
        envelope
    }

    pub(crate) async fn active_messages_page(
        &self,
        session_id: &str,
        offset: usize,
        from_seq: Option<usize>,
        limit: usize,
    ) -> Option<ActiveMessagesPage> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        let session = runtime_guard.session();

        let all_messages: Vec<serde_json::Value> = session
            .messages
            .iter()
            .map(|msg| {
                let role = match msg.role {
                    runtime::MessageRole::System => "system",
                    runtime::MessageRole::User => "user",
                    runtime::MessageRole::Assistant => "assistant",
                    runtime::MessageRole::Tool => "tool",
                };
                let blocks: Vec<serde_json::Value> = msg
                    .blocks
                    .iter()
                    .map(|block| match block {
                        runtime::ContentBlock::Text { text } => {
                            serde_json::json!({"type": "text", "text": text})
                        }
                        runtime::ContentBlock::Image {
                            media_type,
                            data,
                            source_path,
                        } => {
                            serde_json::json!({
                                "type": "image",
                                "media_type": media_type,
                                "source_path": source_path,
                                "size_bytes": data.len() * 3 / 4,
                            })
                        }
                        runtime::ContentBlock::Thinking {
                            thinking,
                            signature,
                        } => {
                            let mut value =
                                serde_json::json!({"type": "thinking", "thinking": thinking});
                            if let Some(signature) = signature {
                                value["signature"] =
                                    serde_json::Value::String(signature.clone());
                            }
                            value
                        }
                        runtime::ContentBlock::ToolUse { id, name, input } => {
                            serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                        }
                        runtime::ContentBlock::ToolResult {
                            tool_use_id,
                            tool_name,
                            output,
                            is_error,
                        } => {
                            serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error})
                        }
                    })
                    .collect();

                let mut value = serde_json::json!({"role": role, "blocks": blocks});
                if let Some(usage) = &msg.usage {
                    value["usage"] = serde_json::json!({
                        "input_tokens": usage.input_tokens,
                        "output_tokens": usage.output_tokens,
                        "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                        "cache_read_input_tokens": usage.cache_read_input_tokens,
                    });
                }
                value
            })
            .collect();
        let total = all_messages.len();
        let start = from_seq.unwrap_or(offset);
        let messages: Vec<serde_json::Value> =
            all_messages.into_iter().skip(start).take(limit).collect();
        let next_seq = (!messages.is_empty()).then_some(start + messages.len());
        let has_more = next_seq.map(|seq| seq < total).unwrap_or(start < total);

        Some(ActiveMessagesPage {
            session_id: session_id.to_string(),
            messages,
            total,
            offset,
            from_seq,
            next_seq,
            limit,
            has_more,
        })
    }

    pub(crate) async fn update_active_session_model(
        &self,
        session_id: &str,
        model: Option<&str>,
    ) -> bool {
        let Some(runtime_entry) = self.sessions.get(session_id) else {
            return false;
        };
        let Some(model) = model else {
            return true;
        };
        let mut runtime_guard = runtime_entry.lock().await;
        runtime_guard.update_session_model(model).await;
        true
    }

    pub(crate) async fn last_context_envelope(
        &self,
        session_id: &str,
    ) -> Option<runtime::ContextEnvelope> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_envelope()
    }

    pub(crate) async fn last_context_turn_report(
        &self,
        session_id: &str,
    ) -> Option<harness_contract::context::ContextTurnReport> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.last_context_turn_report()
    }

    pub(crate) async fn take_collaboration_result(
        &self,
        session_id: &str,
    ) -> Option<CollaborationContextResult> {
        let runtime_entry = self.sessions.get(session_id)?;
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.take_collaboration_result()
    }

    pub(crate) async fn snapshot(&self) -> RuntimeBoundarySnapshot {
        let mut session_ids = self.sessions.list();
        session_ids.sort();
        RuntimeBoundarySnapshot {
            protocol_version: crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            runtime_host: "gateway-runtime-host",
            active_sessions: session_ids.len(),
            uptime_secs: self.clock().uptime_secs(),
            sessions: session_ids,
        }
    }

    #[must_use]
    pub(crate) fn list_sessions_value(&self) -> serde_json::Value {
        serde_json::json!({
            "ok": true,
            "sessions": self.sessions.list(),
        })
    }

    pub(crate) async fn acquire_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
        mode: &str,
    ) -> serde_json::Value {
        self.lease_registry.acquire(session_id, owner, mode).await
    }

    pub(crate) async fn release_session_lease_value(
        &self,
        session_id: &str,
        owner: &str,
    ) -> serde_json::Value {
        self.lease_registry.release(session_id, owner).await
    }

    #[must_use]
    pub(crate) fn unsupported_protocol_value(request: &RuntimeRequest) -> serde_json::Value {
        let response = RuntimeResponse::unsupported_protocol(request);
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| "unsupported runtime protocol version".to_string());
        serde_json::json!({
            "ok": false,
            "protocol_version": crate::runtime_protocol::RUNTIME_PROTOCOL_VERSION,
            "request_id": response.request_id,
            "error": message,
            "error_kind": RuntimeErrorKind::UnsupportedProtocol,
            "retryable": false,
        })
    }

    fn clock(&self) -> RuntimeBoundaryClock {
        RuntimeBoundaryClock::from_uptime(self.started_at.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_max_iterations_restores_previous_runtime_budget() {
        struct BudgetProbe {
            max_iterations: usize,
        }

        impl RuntimeTurnBudgetTarget for BudgetProbe {
            fn max_iterations(&self) -> usize {
                self.max_iterations
            }

            fn set_max_iterations(&mut self, max_iterations: usize) {
                self.max_iterations = max_iterations;
            }
        }

        let mut probe = BudgetProbe { max_iterations: 64 };

        let previous = apply_scoped_max_iterations(&mut probe, Some(8));
        assert_eq!(previous, Some(64));
        assert_eq!(probe.max_iterations(), 8);

        restore_scoped_max_iterations(&mut probe, previous);
        assert_eq!(probe.max_iterations(), 64);

        let previous = apply_scoped_max_iterations(&mut probe, None);
        assert_eq!(previous, None);
        assert_eq!(probe.max_iterations(), 64);
    }

    #[tokio::test]
    async fn runtime_service_status_does_not_initialize_model_provider() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let value = service.status_value();
        assert_eq!(value["ok"], true);
        assert_eq!(value["runtime_host"], "gateway-runtime-host");
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(value.get(&removed_legacy_key).is_none());
        assert_eq!(value["active_sessions"], 0);
    }

    #[tokio::test]
    async fn runtime_service_snapshot_reports_lease_projection() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let lease = service
            .acquire_session_lease_value("session-1", "tui:test", "collaborative")
            .await;
        assert_eq!(lease["ok"], true);

        let snapshot = service.snapshot_value().await;
        assert_eq!(snapshot["kind"], "gateway_runtime_snapshot");
        assert!(snapshot.get("legacy_kind").is_none());
        let removed_legacy_key = ["dae", "mon"].concat();
        assert!(snapshot.get(&removed_legacy_key).is_none());
        assert_eq!(snapshot["leases"]["total"], 1);
        assert_eq!(snapshot["transport"]["control"], "gateway_http");
    }

    #[tokio::test]
    async fn runtime_service_records_durable_turn_journal() {
        let store = Arc::new(memory::UnifiedSessionStore::open_in_memory().unwrap());
        let session_kernel = Arc::new(SessionKernel::new(
            Arc::new(ActiveSessions::default()),
            Some(store.clone()),
            crate::event_bus::SessionEventBus::new(),
        ));
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            session_kernel,
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let submitted = service
            .submit_turn_value(
                Some("journal-session".to_string()),
                Some("task-a".to_string()),
                "persist this turn".to_string(),
            )
            .await;

        assert_eq!(submitted["ok"], true);
        assert_eq!(submitted["durable_journal"], true);
        let events = store
            .get_events_by_type_limited("journal-session", "TurnJournal", 0, 10)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
        assert_eq!(payload["event_type"], "turn.submitted");
        assert_eq!(payload["phase"], "submitted");
        assert_eq!(payload["payload"]["prompt"], "persist this turn");
        assert_eq!(payload["payload"]["task_id"], "task-a");
    }

    #[test]
    fn runtime_service_rejects_unsupported_protocol_as_legacy_socket_error() {
        let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
            "protocol_version": 999,
            "request_id": "req-old",
            "cmd": "status",
        }))
        .expect("request parses");

        let value = RuntimeService::unsupported_protocol_value(&request);
        assert_eq!(value["ok"], false);
        assert_eq!(value["request_id"], "req-old");
        assert_eq!(value["error_kind"], "unsupported_protocol");
        assert_eq!(value["retryable"], false);
        assert!(value["error"]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported runtime protocol version"));
    }

    #[test]
    fn runtime_service_records_executing_turn_lifecycle() {
        let service = RuntimeService::new(
            Arc::new(ActiveSessions::default()),
            Arc::new(SessionLeaseRegistry::default()),
            Arc::new(SessionKernel::new(
                Arc::new(ActiveSessions::default()),
                None,
                crate::event_bus::SessionEventBus::new(),
            )),
            Arc::new(SessionLifecycleKernel::new()),
            Instant::now(),
        );

        let running = service.start_running_turn(
            Some("session-turn".to_string()),
            Some("task-turn".to_string()),
            "execute real turn".to_string(),
        );
        assert_eq!(running.status, TurnStatus::Running);
        assert_eq!(running.session_id.as_deref(), Some("session-turn"));
        assert_eq!(running.task_id.as_deref(), Some("task-turn"));

        let completed = service.finish_turn(&running.turn_id, TurnStatus::Completed, None);
        assert_eq!(completed.status, TurnStatus::Completed);
        assert!(completed.completed_at.is_some());
        assert_eq!(completed.events.len(), 2);
        assert_eq!(completed.events[0].status, TurnStatus::Running);
        assert_eq!(completed.events[1].status, TurnStatus::Completed);

        let snapshot = service.turns_value();
        assert_eq!(snapshot["turn_bindings"][0]["task_id"], "task-turn");
        assert_eq!(
            snapshot["turn_bindings"][0]["turn_id"],
            running.turn_id.to_string()
        );
        assert_eq!(snapshot["turn_bindings"][0]["session_id"], "session-turn");
    }
}
