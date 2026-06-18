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
use tokio::time::{timeout, Duration};
use tokio_stream::wrappers::ReceiverStream;

use crate::event_bus::SessionEventBus;
use crate::services::SessionService;
use crate::task_kernel::TaskRecord;

use super::{AppState, ErrorResponse};

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
    profile: ContextProfile,
    status: &str,
    iterations: Option<usize>,
    context_envelope_id: Option<String>,
    error: Option<String>,
    started_at_ms: u64,
    completed_at_ms: u64,
) -> serde_json::Value {
    let refs = context_envelope_id
        .as_ref()
        .map(|id| vec![serde_json::json!({"type": "context_envelope", "id": id})])
        .unwrap_or_default();
    serde_json::json!({
        "type": "RuntimeRun",
        "phase": "completed",
        "run_id": run_id,
        "parent_run_id": null,
        "kind": "main_turn",
        "session_id": session_id,
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

async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    let session_id = id.clone();
    let event_bus = state.event_bus();
    let run_id = uuid::Uuid::new_v4().to_string();
    let run_started_at_ms = current_time_ms();
    let active_task = state.services.task.current().unwrap_or_default();
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

    {
        let runtime_guard = runtime_entry.lock().await;
        if let Some(cowd_bus) = runtime_guard.cowd_bus() {
            let mut rx = cowd_bus.subscribe();
            let eb = event_bus.clone();
            let sid = session_id.clone();
            let session_service = state.services.session.clone();
            let active_run_id = run_id.clone();
            tokio::spawn(async move {
                while let Ok(event) = rx.recv().await {
                    match event {
                        runtime::CowdEvent::TextDelta { text } => {
                            eb.text_delta(&sid, &text).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "TextDelta",
                                serde_json::json!({"type":"TextDelta","content":text}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ThinkingDelta { thinking } => {
                            eb.thinking_delta(&sid, &thinking).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "ThinkingDelta",
                                serde_json::json!({"type":"ThinkingDelta","content":thinking}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ToolStart { id, name, preview } => {
                            eb.tool_start(&sid, &id, &name).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "ToolStart",
                                serde_json::json!({"type":"ToolStart","id":id,"name":name,"preview":preview}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::ToolProgress { id, name, progress } => {
                            eb.tool_progress(&sid, &id, &name, &progress).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "ToolProgress",
                                serde_json::json!({"type":"ToolProgress","id":id,"name":name,"progress":progress}),
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
                                serde_json::json!({"type":"ToolComplete","id":id,"name":name,"summary":summary,"exit_code":exit_code}),
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnComplete {
                            assistant_text,
                            iterations,
                        } => {
                            let json = serde_json::json!({"type":"TurnComplete","text":assistant_text,"iterations":iterations});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "TurnComplete",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnStarted => {
                            let json = serde_json::json!({"type":"TurnStarted"});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "TurnStarted",
                                json,
                            )
                            .await;
                        }
                        runtime::CowdEvent::TurnError { error } => {
                            let json = serde_json::json!({"type":"TurnError","error":error});
                            eb.broadcast(&sid, &json.to_string()).await;
                            append_session_timeline_event(
                                &session_service,
                                &sid,
                                "TurnError",
                                json,
                            )
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
                            // Durable context history is written by the
                            // runtime core so CLI, TUI, and WebUI all share
                            // one source of truth. The API layer only fans
                            // the event out to live clients.
                        }
                        runtime::CowdEvent::TokenUsage { .. }
                        | runtime::CowdEvent::Warning { .. }
                        | runtime::CowdEvent::CompactionNotice { .. } => {}
                        _ => {}
                    }
                }
            });
        }
    }

    if let Some(task) = active_task {
        let packet = task_resume_context_packet(&session_id, &task);
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(run_profile);
        runtime_guard.inject_resume_context(packet);
    } else {
        let runtime_guard = runtime_entry.lock().await;
        runtime_guard.set_context_profile(run_profile);
    }

    const TURN_TIMEOUT: Duration = Duration::from_secs(300);

    let content = body.content;
    let rt_entry = runtime_entry.clone();
    let turn_result = tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        handle.block_on(async move {
            let mut runtime_guard = rt_entry.lock().await;
            timeout(
                TURN_TIMEOUT,
                runtime_guard
                    .run_turn_async(&content, &runtime::permissions::SharedPrompter::none()),
            )
            .await
        })
    })
    .await;

    match turn_result {
        Ok(Ok(Ok(summary))) => {
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

            let session_snapshot = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard.session().clone()
            };
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };
            let collaboration_result = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard.take_collaboration_result()
            };
            if let Err(e) = state
                .services
                .session
                .sync_runtime_session_snapshot(&session_id, &session_snapshot)
                .await
            {
                tracing::warn!(%session_id, error = %e, "failed to sync API session to SQLite");
            }
            if let Some(collaboration_result) = collaboration_result {
                let memory_manager = state.services.memory.manager();
                if let Err(e) = state
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

            let response = serde_json::json!({
                "session_id": &session_id,
                "status": "complete",
                "response": final_text,
                "iterations": summary.iterations,
            });

            let sse_data = serde_json::json!({
                "type": "TurnComplete",
                "session_id": &session_id,
                "response": final_text,
                "iterations": summary.iterations,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event(
                &state.services.session,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "completed",
                    Some(summary.iterations),
                    context_envelope_id,
                    None,
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Ok(Json(response))
        }
        Ok(Ok(Err(e))) => {
            let error_msg = e.to_string();
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event(
                &state.services.session,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "failed",
                    None,
                    context_envelope_id,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Ok(Err(_elapsed)) => {
            let error_msg = format!("turn timed out after {}s", TURN_TIMEOUT.as_secs());
            let context_envelope_id = {
                let runtime_guard = runtime_entry.lock().await;
                runtime_guard
                    .last_context_envelope()
                    .map(|envelope| envelope.id)
            };

            let sse_data = serde_json::json!({
                "type": "TurnError",
                "session_id": &session_id,
                "error": error_msg,
            });
            event_bus
                .broadcast(&session_id, &sse_data.to_string())
                .await;
            append_session_timeline_event(
                &state.services.session,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "timeout",
                    None,
                    context_envelope_id,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;

            Err((
                StatusCode::REQUEST_TIMEOUT,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
        Err(join_err) => {
            let error_msg = format!("task join error: {join_err}");
            append_session_timeline_event(
                &state.services.session,
                &session_id,
                "RuntimeRun",
                runtime_run_completed_payload(
                    &session_id,
                    &run_id,
                    run_profile,
                    "failed",
                    None,
                    None,
                    Some(error_msg.clone()),
                    run_started_at_ms,
                    current_time_ms(),
                ),
            )
            .await;
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse { error: error_msg }),
            ))
        }
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

    let runtime_entry = state.active_runtime(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("session {id} not found"),
            }),
        )
    })?;

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
                    runtime::ContentBlock::Thinking { thinking, signature } => {
                        let mut val = serde_json::json!({"type": "thinking", "thinking": thinking});
                        if let Some(sig) = signature {
                            val["signature"] = serde_json::Value::String(sig.clone());
                        }
                        val
                    }
                    runtime::ContentBlock::ToolUse { id, name, input } => {
                        serde_json::json!({"type": "tool_use", "id": id, "name": name, "input": input})
                    }
                    runtime::ContentBlock::ToolResult { tool_use_id, tool_name, output, is_error } => {
                        serde_json::json!({"type": "tool_result", "tool_use_id": tool_use_id, "tool_name": tool_name, "output": output, "is_error": is_error})
                    }
                })
                .collect();

            let mut val = serde_json::json!({"role": role, "blocks": blocks});
            if let Some(usage) = &msg.usage {
                val["usage"] = serde_json::json!({
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "cache_creation_input_tokens": usage.cache_creation_input_tokens,
                    "cache_read_input_tokens": usage.cache_read_input_tokens,
                });
            }
            val
        })
        .collect();

    let total = all_messages.len();
    let start = from_seq.unwrap_or(offset);
    let messages: Vec<serde_json::Value> =
        all_messages.into_iter().skip(start).take(limit).collect();
    let next_seq = (!messages.is_empty()).then_some(start + messages.len());
    let has_more = next_seq.map(|seq| seq < total).unwrap_or(start < total);

    Ok(Json(serde_json::json!({
        "session_id": id,
        "messages": messages,
        "total": total,
        "offset": offset,
        "from_seq": from_seq,
        "next_seq": next_seq,
        "limit": limit,
        "has_more": has_more,
    })))
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
