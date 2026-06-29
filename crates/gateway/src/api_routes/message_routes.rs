use std::{
    convert::Infallible,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{Path, Query, State as AxumState},
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::get,
    Json, Router,
};
use futures::{stream::Stream, StreamExt};
use runtime::{ContextProfile, ResumeContextPacket, ResumeContextSource};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;

use crate::event_bus::SessionEventBus;
use crate::runtime_service::{
    RuntimeService, RuntimeTurnExecution, RuntimeTurnExecutionError, RuntimeTurnOptions,
};
use crate::services::SessionService;
use crate::task_kernel::TaskRecord;

use super::{
    clear_active_turn_control, discard_active_turn_partial, record_active_turn_text_delta,
    register_active_turn_control, register_active_turn_partial, take_active_turn_partial, AppState,
    ErrorResponse,
};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/sessions/:id/messages",
            get(get_session_messages).post(send_message),
        )
        .route("/api/sessions/:id/stream", get(sse_stream_handler))
}

#[derive(Deserialize)]
struct SendMessageRequest {
    content: String,
}

#[derive(Deserialize)]
struct GetMessagesParams {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    from_seq: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TurnTimeoutClass {
    Direct,
    Standard,
    Deep,
}

fn classify_turn_timeout_prompt(prompt: &str, profile: ContextProfile) -> TurnTimeoutClass {
    if matches!(profile, ContextProfile::YoloGoal) {
        return TurnTimeoutClass::Deep;
    }

    let lower = prompt.to_lowercase();
    let deep_markers = [
        "deep",
        "architecture",
        "refactor",
        "multi-agent",
        "what if",
        "scenario",
        "simulation",
        "matrix",
        "memory",
        "harness",
        "沉浸式",
        "深度",
        "架构",
        "重构",
        "全量",
        "全盘",
        "复杂",
        "多agent",
        "多 agent",
        "跨session",
        "跨 session",
        "记忆",
        "矩阵",
        "推演",
        "测试",
        "验证",
    ];
    if prompt.chars().count() > 500
        || prompt.lines().count() > 6
        || deep_markers.iter().any(|marker| lower.contains(marker))
    {
        return TurnTimeoutClass::Deep;
    }

    let direct_markers = [
        "what is",
        "怎么写",
        "解释",
        "列出",
        "总结",
        "简单",
        "快速",
        "status",
        "help",
    ];
    if prompt.chars().count() <= 160 && direct_markers.iter().any(|marker| lower.contains(marker)) {
        return TurnTimeoutClass::Direct;
    }

    TurnTimeoutClass::Standard
}

fn turn_timeout_for_prompt(prompt: &str, profile: ContextProfile) -> Duration {
    match classify_turn_timeout_prompt(prompt, profile) {
        TurnTimeoutClass::Direct => Duration::from_secs(240),
        TurnTimeoutClass::Standard => Duration::from_secs(480),
        TurnTimeoutClass::Deep => Duration::from_secs(900),
    }
}

fn turn_max_iterations_for_prompt(prompt: &str, profile: ContextProfile) -> usize {
    match classify_turn_timeout_prompt(prompt, profile) {
        TurnTimeoutClass::Direct => 12,
        TurnTimeoutClass::Standard => 32,
        TurnTimeoutClass::Deep => 64,
    }
}

async fn append_session_timeline_event(
    session_service: &SessionService,
    session_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) {
    if let Err(error) = session_service
        .append_timeline_event(session_id, event_type, payload)
        .await
    {
        tracing::warn!(%session_id, %event_type, error = %error, "failed to append session event");
    }
}

pub(super) fn task_resume_context_packet(
    session_id: &str,
    task: &TaskRecord,
) -> ResumeContextPacket {
    let current_phase = task.current_phase.as_ref().and_then(|phase_ref| {
        task.phases
            .iter()
            .find(|phase| &phase.id == phase_ref || &phase.name == phase_ref)
    });
    let phase_summary = current_phase.map(|phase| {
        format!(
            "phase={} status={} objective={} acceptance=[{}]",
            phase.name,
            phase.status.as_str(),
            phase.objective,
            phase.acceptance.join("; ")
        )
    });
    let active_task = Some(format!(
        "id={} status={} yolo={} objective={}{}",
        task.id,
        task.status.as_str(),
        task.yolo_mode,
        task.objective,
        phase_summary
            .as_ref()
            .map(|summary| format!(" current_{summary}"))
            .unwrap_or_default()
    ));
    let recent_decisions = task
        .audit
        .iter()
        .rev()
        .take(5)
        .map(|event| format!("{}: {}", event.event_type, event.message))
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if let Some(reason) = task
        .blocker_reason
        .as_ref()
        .filter(|reason| !reason.is_empty())
    {
        blockers.push(reason.clone());
    }
    if task.failure_count > 0 {
        blockers.push(format!("failure_count={}", task.failure_count));
    }

    ResumeContextPacket {
        session_id: session_id.to_string(),
        handoff_summary: None,
        active_task,
        recent_decisions,
        blockers,
        source: ResumeContextSource::TaskRegistry,
    }
}

pub(super) fn runtime_run_started_payload(
    session_id: &str,
    run_id: &str,
    profile: ContextProfile,
    intent: &str,
    started_at_ms: u64,
) -> serde_json::Value {
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "started",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "profile": profile,
        "status": "running",
        "summary": intent.chars().take(120).collect::<String>(),
        "intent_preview": intent.chars().take(240).collect::<String>(),
        "started_at_ms": started_at_ms,
        "refs": [],
    })
}

pub(super) fn runtime_run_completed_payload(
    session_id: &str,
    run_id: &str,
    turn_id: Option<&str>,
    profile: ContextProfile,
    status: &str,
    iterations: Option<usize>,
    context_envelope_id: Option<String>,
    error: Option<String>,
    started_at_ms: u64,
    completed_at_ms: u64,
) -> serde_json::Value {
    let mut refs = context_envelope_id
        .as_ref()
        .map(|id| vec![serde_json::json!({"type": "context_envelope", "id": id})])
        .unwrap_or_default();
    if let Some(turn_id) = turn_id {
        refs.push(serde_json::json!({"type": "turn", "id": turn_id}));
    }
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "completed",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
        "turn_id": turn_id,
        "profile": profile,
        "status": status,
        "summary": error
            .as_ref()
            .map(|value| value.chars().take(160).collect::<String>())
            .unwrap_or_else(|| format!("turn {status}")),
        "iterations": iterations,
        "context_envelope_id": context_envelope_id,
        "error": error,
        "started_at_ms": started_at_ms,
        "completed_at_ms": completed_at_ms,
        "duration_ms": completed_at_ms.saturating_sub(started_at_ms),
        "refs": refs,
    })
}

struct RuntimeTurnSink<'a> {
    state: &'a AppState,
    runtime_service: &'a RuntimeService,
    event_bus: &'a SessionEventBus,
}

impl<'a> RuntimeTurnSink<'a> {
    async fn complete(
        &self,
        session_id: &str,
        run_id: &str,
        profile: ContextProfile,
        execution: RuntimeTurnExecution,
        started_at_ms: u64,
    ) -> serde_json::Value {
        let summary = execution.summary;
        let turn_id = execution.receipt.turn_id.to_string();
        let context_turn_report = summary.context_turn_report.clone();
        let final_text = summary
            .assistant_messages
            .last()
            .map(|msg| {
                msg.blocks
                    .iter()
                    .filter_map(|block| match block {
                        runtime::ContentBlock::Text { text } => Some(text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("")
            })
            .unwrap_or_default();

        let session_snapshot = self.runtime_service.session_snapshot(session_id).await;
        let context_envelope_id = self
            .runtime_service
            .last_context_envelope(session_id)
            .await
            .map(|envelope| envelope.id);
        let collaboration_result = self
            .runtime_service
            .take_collaboration_result(session_id)
            .await;
        if let Some(session_snapshot) = session_snapshot {
            if let Err(e) = self
                .runtime_service
                .sync_session_snapshot(session_id, &session_snapshot)
                .await
            {
                tracing::warn!(%session_id, error = %e, "failed to sync API session to SQLite");
            }
        }
        if let Some(collaboration_result) = collaboration_result {
            let memory_manager = self.state.services.memory.manager();
            if let Err(e) = self
                .state
                .services
                .session
                .persist_workgraph_review(
                    &collaboration_result.work_graph,
                    &collaboration_result.review_packet,
                    memory_manager.as_ref(),
                )
                .await
            {
                tracing::warn!(
                    %session_id,
                    error = %e,
                    "failed to persist collaboration closed-loop runtime event"
                );
            }
        }

        let sse_data = serde_json::json!({
            "type": "TurnComplete",
            "session_id": session_id,
            "turn_id": &turn_id,
            "response": final_text,
            "iterations": summary.iterations,
            "model_telemetry": summary.model_telemetry.clone(),
            "context_turn_report": context_turn_report.clone(),
        });
        self.event_bus
            .broadcast(session_id, &sse_data.to_string())
            .await;
        append_session_timeline_event(
            &self.state.services.session,
            session_id,
            "ContextTurnReport",
            serde_json::json!({
                "type": "ContextTurnReport",
                "session_id": session_id,
                "run_id": run_id,
                "turn_id": &turn_id,
                "model_telemetry": summary.model_telemetry.clone(),
                "context_turn_report": context_turn_report.clone(),
            }),
        )
        .await;
        append_session_timeline_event(
            &self.state.services.session,
            session_id,
            "RuntimeRun",
            runtime_run_completed_payload(
                session_id,
                run_id,
                Some(&turn_id),
                profile,
                "completed",
                Some(summary.iterations),
                context_envelope_id,
                None,
                started_at_ms,
                current_time_ms(),
            ),
        )
        .await;
        discard_active_turn_partial(session_id, run_id);

        serde_json::json!({
            "session_id": session_id,
            "turn_id": &turn_id,
            "turn": execution.receipt,
            "status": "complete",
            "response": final_text,
            "iterations": summary.iterations,
            "model_telemetry": summary.model_telemetry,
            "context_turn_report": context_turn_report,
        })
    }

    async fn fail(
        &self,
        session_id: &str,
        run_id: &str,
        profile: ContextProfile,
        status: StatusCode,
        error: &RuntimeTurnExecutionError,
        started_at_ms: u64,
    ) -> String {
        let error_msg = error.message();
        if let Some(session_snapshot) = self.runtime_service.session_snapshot(session_id).await {
            if let Err(e) = self
                .runtime_service
                .sync_session_snapshot(session_id, &session_snapshot)
                .await
            {
                tracing::warn!(%session_id, error = %e, "failed to sync failed API session to SQLite");
            }
        }
        let context_envelope_id = self
            .runtime_service
            .last_context_envelope(session_id)
            .await
            .map(|envelope| envelope.id);
        let partial = take_active_turn_partial(session_id, run_id)
            .filter(|partial| !partial.text.trim().is_empty());

        let sse_data = serde_json::json!({
            "type": "TurnError",
            "session_id": session_id,
            "run_id": run_id,
            "error": error_msg,
        });
        self.event_bus
            .broadcast(session_id, &sse_data.to_string())
            .await;
        if let Some(partial) = partial {
            let partial_text = partial.text;
            let partial_char_count = partial_text.chars().count();
            let partial_json = serde_json::json!({
                "type": "PartialAnswer",
                "session_id": session_id,
                "run_id": run_id,
                "reason": error_msg,
                "content": partial_text,
                "char_count": partial_char_count,
                "updated_at_ms": partial.updated_at_ms,
            });
            self.event_bus
                .broadcast(session_id, &partial_json.to_string())
                .await;
            append_session_timeline_event(
                &self.state.services.session,
                session_id,
                "PartialAnswer",
                partial_json,
            )
            .await;
        }
        append_session_timeline_event(
            &self.state.services.session,
            session_id,
            "RuntimeRun",
            runtime_run_completed_payload(
                session_id,
                run_id,
                None,
                profile,
                if status == StatusCode::REQUEST_TIMEOUT {
                    "timeout"
                } else {
                    "failed"
                },
                None,
                context_envelope_id,
                Some(error_msg.clone()),
                started_at_ms,
                current_time_ms(),
            ),
        )
        .await;
        error_msg
    }
}

async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service unavailable".to_string(),
            }),
        )
    })?;
    if !runtime_service.has_active_session(&id) {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        ));
    }

    let runtime_service = runtime_service.clone();

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    let session_id = id.clone();
    let event_bus = state.event_bus();
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_started_at_ms = current_time_ms();
    let active_task = state.services.task.current().unwrap_or_default();
    let active_task_id = active_task.as_ref().map(|task| task.id.clone());
    let run_profile = if active_task.as_ref().is_some_and(|task| task.yolo_mode) {
        ContextProfile::YoloGoal
    } else {
        ContextProfile::MainTurn
    };
    append_session_timeline_event(
        &state.services.session,
        &session_id,
        "RuntimeRun",
        runtime_run_started_payload(
            &session_id,
            &run_id,
            run_profile,
            &body.content,
            run_started_at_ms,
        ),
    )
    .await;

    if let Some(mut rx) = runtime_service.cowd_event_receiver(&session_id).await {
        let eb = event_bus.clone();
        let sid = session_id.clone();
        let session_service = state.services.session.clone();
        let active_run_id = run_id.clone();
        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                match event {
                    runtime::CowdEvent::TextDelta { text } => {
                        record_active_turn_text_delta(&sid, &active_run_id, &text);
                        eb.text_delta(&sid, &text).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "TextDelta",
                            serde_json::json!({"type":"TextDelta","run_id":active_run_id.clone(),"content":text}),
                        )
                        .await;
                    }
                    runtime::CowdEvent::ThinkingDelta { thinking } => {
                        eb.thinking_delta(&sid, &thinking).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "ThinkingDelta",
                            serde_json::json!({"type":"ThinkingDelta","run_id":active_run_id.clone(),"content":thinking}),
                        )
                        .await;
                    }
                    runtime::CowdEvent::ToolStart { id, name, preview } => {
                        eb.tool_start(&sid, &id, &name).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "ToolStart",
                            serde_json::json!({"type":"ToolStart","run_id":active_run_id.clone(),"id":id,"name":name,"preview":preview}),
                        )
                        .await;
                    }
                    runtime::CowdEvent::ToolProgress { id, name, progress } => {
                        eb.tool_progress(&sid, &id, &name, &progress).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "ToolProgress",
                            serde_json::json!({"type":"ToolProgress","run_id":active_run_id.clone(),"id":id,"name":name,"progress":progress}),
                        )
                        .await;
                    }
                    runtime::CowdEvent::ToolComplete {
                        id,
                        name,
                        summary,
                        exit_code,
                    } => {
                        eb.tool_complete(&sid, &id, &name, &summary, exit_code)
                            .await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "ToolComplete",
                            serde_json::json!({"type":"ToolComplete","run_id":active_run_id.clone(),"id":id,"name":name,"summary":summary,"exit_code":exit_code}),
                        )
                        .await;
                    }
                    runtime::CowdEvent::TurnComplete {
                        assistant_text,
                        iterations,
                    } => {
                        let json = serde_json::json!({"type":"TurnComplete","run_id":active_run_id.clone(),"text":assistant_text,"iterations":iterations});
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(&session_service, &sid, "TurnComplete", json)
                            .await;
                    }
                    runtime::CowdEvent::TurnStarted => {
                        let json = serde_json::json!({"type":"TurnStarted","run_id":active_run_id.clone()});
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(&session_service, &sid, "TurnStarted", json)
                            .await;
                    }
                    runtime::CowdEvent::TurnError { error } => {
                        let json = serde_json::json!({"type":"TurnError","run_id":active_run_id.clone(),"error":error});
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(&session_service, &sid, "TurnError", json)
                            .await;
                    }
                    runtime::CowdEvent::ContextEnvelope { envelope } => {
                        let json = serde_json::json!({
                            "type": "ContextEnvelope",
                            "envelope_id": envelope.id.clone(),
                            "run_id": active_run_id.clone(),
                            "session_id": envelope.identity.session_id.clone(),
                            "agent_id": envelope.identity.agent_id.clone(),
                            "profile": envelope.profile,
                            "diagnostics": envelope.diagnostics.clone(),
                            "budget": envelope.budget.clone(),
                            "hashes": {
                                "stable_head": envelope.diagnostics.stable_head_hash,
                                "runtime_header": envelope.diagnostics.runtime_header_hash,
                                "dynamic_tail": envelope.diagnostics.dynamic_tail_hash,
                            },
                            "envelope": envelope,
                        });
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "ContextEnvelope",
                            json,
                        )
                        .await;
                    }
                    runtime::CowdEvent::TokenUsage {
                        input,
                        output,
                        cache_create,
                        cache_read,
                    } => {
                        let json = serde_json::json!({
                            "type": "TokenUsage",
                            "run_id": active_run_id.clone(),
                            "input": input,
                            "output": output,
                            "cache_create": cache_create,
                            "cache_read": cache_read,
                            "total": input + output + cache_create + cache_read,
                        });
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(&session_service, &sid, "TokenUsage", json)
                            .await;
                    }
                    runtime::CowdEvent::RunModelTelemetry { telemetry } => {
                        let json = serde_json::json!({
                            "type": "RunModelTelemetry",
                            "run_id": active_run_id.clone(),
                            "telemetry": telemetry,
                        });
                        eb.broadcast(&sid, &json.to_string()).await;
                        append_session_timeline_event(
                            &session_service,
                            &sid,
                            "RunModelTelemetry",
                            json,
                        )
                        .await;
                    }
                    runtime::CowdEvent::Warning { .. }
                    | runtime::CowdEvent::CompactionNotice { .. } => {}
                    _ => {}
                }
            }
        });
    }

    let resume_context = active_task
        .as_ref()
        .map(|task| task_resume_context_packet(&session_id, task));
    runtime_service
        .configure_turn_context(&session_id, run_profile, resume_context)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
            )
        })?;

    let content = body.content;
    let turn_timeout = turn_timeout_for_prompt(&content, run_profile);
    let turn_max_iterations = turn_max_iterations_for_prompt(&content, run_profile);
    let cancellation_token = runtime::CancellationToken::new();
    let hook_abort_signal = runtime::HookAbortSignal::new();
    runtime_service
        .install_turn_control(
            &session_id,
            cancellation_token.clone(),
            hook_abort_signal.clone(),
        )
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: error.message(),
                }),
            )
        })?;
    register_active_turn_control(
        session_id.clone(),
        run_id.clone(),
        cancellation_token,
        hook_abort_signal,
    );
    register_active_turn_partial(session_id.clone(), run_id.clone());
    let (completion_tx, completion_rx) = oneshot::channel();
    let worker_state = state.clone();
    let worker_runtime_service = runtime_service.clone();
    let worker_event_bus = event_bus.clone();
    let worker_session_id = session_id.clone();
    let worker_run_id = run_id.clone();
    tokio::spawn(async move {
        let turn_result = worker_runtime_service
            .run_turn_with_options(
                &worker_session_id,
                active_task_id,
                content,
                turn_timeout,
                RuntimeTurnOptions {
                    profile: run_profile,
                    max_iterations: Some(turn_max_iterations),
                },
            )
            .await;
        clear_active_turn_control(&worker_session_id, &worker_run_id);
        let turn_sink = RuntimeTurnSink {
            state: &worker_state,
            runtime_service: &worker_runtime_service,
            event_bus: &worker_event_bus,
        };

        let completion = match turn_result {
            Ok(execution) => {
                let response = turn_sink
                    .complete(
                        &worker_session_id,
                        &worker_run_id,
                        run_profile,
                        execution,
                        run_started_at_ms,
                    )
                    .await;
                Ok(response)
            }
            Err(error) => {
                let status = match error {
                    crate::runtime_service::RuntimeTurnExecutionError::Timeout { .. } => {
                        StatusCode::REQUEST_TIMEOUT
                    }
                    crate::runtime_service::RuntimeTurnExecutionError::NotFound(_) => {
                        StatusCode::NOT_FOUND
                    }
                    crate::runtime_service::RuntimeTurnExecutionError::Runtime(_)
                    | crate::runtime_service::RuntimeTurnExecutionError::Join(_) => {
                        StatusCode::INTERNAL_SERVER_ERROR
                    }
                };
                let error_msg = turn_sink
                    .fail(
                        &worker_session_id,
                        &worker_run_id,
                        run_profile,
                        status,
                        &error,
                        run_started_at_ms,
                    )
                    .await;
                Err((status, error_msg))
            }
        };
        let _ = completion_tx.send(completion);
    });

    match completion_rx.await {
        Ok(Ok(response)) => Ok(Json(response)),
        Ok(Err((status, error_msg))) => Err((status, Json(ErrorResponse { error: error_msg }))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "runtime turn worker stopped before completion".to_string(),
            }),
        )),
    }
}

async fn get_session_messages(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Query(params): Query<GetMessagesParams>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let offset = params.offset.unwrap_or(0);
    let from_seq = params.from_seq;
    let limit = params.limit.unwrap_or(50).min(500);

    if state.has_unified_store() {
        let total = state
            .services
            .session
            .stored_message_count(&id)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);
        let db_messages = if let Some(seq) = from_seq {
            state
                .services
                .session
                .stored_messages_from_sequence(&id, seq, limit)
                .await
                .unwrap_or(Some(Vec::new()))
                .unwrap_or_default()
        } else {
            state
                .services
                .session
                .stored_messages(&id, offset, limit)
                .await
                .unwrap_or(Some(Vec::new()))
                .unwrap_or_default()
        };
        let messages: Vec<serde_json::Value> = db_messages
            .iter()
            .map(|m| {
                let blocks: Vec<serde_json::Value> =
                    serde_json::from_str(&m.content_json).unwrap_or_default();
                let mut val = serde_json::json!({
                    "session_id": m.session_id,
                    "sequence": m.sequence,
                    "role": m.role,
                    "blocks": blocks,
                    "created_at_ms": m.created_at_ms,
                });
                if let Some(ref tu) = m.token_usage_json {
                    if let Ok(usage) = serde_json::from_str::<serde_json::Value>(tu) {
                        val["token_usage"] = usage;
                    }
                }
                if let Some(ref tid) = m.tool_use_id {
                    val["tool_use_id"] = serde_json::Value::String(tid.clone());
                }
                if let Some(ref tn) = m.tool_name {
                    val["tool_name"] = serde_json::Value::String(tn.clone());
                }
                val
            })
            .collect();
        let next_seq = db_messages.last().map(|m| m.sequence + 1);
        let has_more = next_seq
            .map(|seq| seq < total)
            .unwrap_or_else(|| from_seq.unwrap_or(offset) < total);
        return Ok(Json(serde_json::json!({
            "session_id": id,
            "messages": messages,
            "total": total,
            "offset": offset,
            "from_seq": from_seq,
            "next_seq": next_seq,
            "limit": limit,
            "has_more": has_more,
        })));
    }

    let runtime_service = state.services.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "runtime service not configured".to_string(),
            }),
        )
    })?;

    runtime_service
        .active_messages_page(&id, offset, from_seq, limit)
        .await
        .map(|page| Json(serde_json::to_value(page).unwrap_or_else(|_| serde_json::json!({}))))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!("session {id} not found"),
                }),
            )
        })
}

struct SseStream {
    rx: ReceiverStream<String>,
    session_id: String,
    event_bus: Arc<SessionEventBus>,
    tx: mpsc::Sender<String>,
}

impl Stream for SseStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.rx.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(data)) => {
                std::task::Poll::Ready(Some(Ok(Event::default().data(data))))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl Drop for SseStream {
    fn drop(&mut self) {
        let event_bus = self.event_bus.clone();
        let session_id = self.session_id.clone();
        let tx = self.tx.clone();
        tokio::spawn(async move {
            event_bus.unsubscribe(&session_id, &tx).await;
        });
    }
}

async fn sse_stream_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(session_id): Path<String>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = mpsc::channel(256);
    let bus_tx = tx.clone();
    let event_bus = state.event_bus();
    event_bus.subscribe(&session_id, bus_tx).await;
    let _ = tx
        .send(
            serde_json::json!({
                "type": "Connected",
                "session_id": session_id,
            })
            .to_string(),
        )
        .await;

    let stream = SseStream {
        rx: ReceiverStream::new(rx),
        session_id,
        event_bus,
        tx,
    };

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turn_timeout_policy_expands_for_deep_tasks() {
        let direct = turn_timeout_for_prompt("解释一下这个函数", ContextProfile::MainTurn);
        let standard =
            turn_timeout_for_prompt("帮我分析当前实现并给出建议", ContextProfile::MainTurn);
        let deep = turn_timeout_for_prompt(
            "请进行深度架构分析，模拟 what if 场景，验证 memory matrix harness 多Agent协同并输出完整报告",
            ContextProfile::MainTurn,
        );

        assert!(direct < standard);
        assert!(standard < deep);
        assert_eq!(deep, Duration::from_secs(900));
        assert_eq!(
            turn_max_iterations_for_prompt("解释一下这个函数", ContextProfile::MainTurn),
            12
        );
        assert_eq!(
            turn_max_iterations_for_prompt(
                "请进行深度架构分析，模拟 what if 场景，验证 memory matrix harness 多Agent协同并输出完整报告",
                ContextProfile::MainTurn,
            ),
            64
        );
    }

    #[test]
    fn yolo_profile_uses_deep_turn_timeout() {
        assert_eq!(
            turn_timeout_for_prompt("继续执行", ContextProfile::YoloGoal),
            Duration::from_secs(900)
        );
    }
}
