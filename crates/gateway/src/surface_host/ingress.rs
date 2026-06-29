use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use memory::SessionRecord;
use sha2::{Digest, Sha256};
use surface::{SurfaceActionRequest, SurfaceFrame, SurfaceSendRequest};
use tokio::sync::Mutex;

use crate::api_routes::AppState;
use crate::runtime_service::RuntimeTurnOptions;

const SURFACE_QUICK_WALL_CLOCK: Duration = Duration::from_secs(240);
const SURFACE_DEEP_WALL_CLOCK: Duration = Duration::from_secs(900);
const SURFACE_QUICK_MAX_ITERATIONS: usize = 12;
const SURFACE_DEEP_MAX_ITERATIONS: usize = 64;

#[derive(Debug, Clone, Copy)]
struct SurfaceTurnPolicy {
    profile: runtime::ContextProfile,
    timeout: Duration,
    max_iterations: usize,
}

pub(crate) fn spawn_surface_ingress_dispatcher(state: Arc<AppState>) {
    let mut rx = state.services.surface.subscribe_events();
    let session_locks = Arc::new(Mutex::new(HashMap::<String, Arc<Mutex<()>>>::new()));
    tokio::spawn(async move {
        loop {
            let frame = match rx.recv().await {
                Ok(frame) => frame,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "surface ingress dispatcher lagged");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            };
            let SurfaceFrame::Event {
                surface,
                event,
                payload,
            } = frame
            else {
                continue;
            };
            if event != "message.received" {
                continue;
            }
            let state = state.clone();
            let session_locks = session_locks.clone();
            tokio::spawn(async move {
                if let Err(error) =
                    handle_surface_message(state, session_locks, surface, payload).await
                {
                    tracing::warn!(error = %error, "surface ingress message handling failed");
                }
            });
        }
    });
}

async fn handle_surface_message(
    state: Arc<AppState>,
    session_locks: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    surface: String,
    payload: serde_json::Value,
) -> Result<(), String> {
    let message_id = payload_string(&payload, "message_id")
        .or_else(|| payload_string(&payload, "id"))
        .unwrap_or_else(|| payload_fingerprint_id(&surface, &payload));

    let content = payload_string(&payload, "text")
        .or_else(|| payload_string(&payload, "content"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "surface message has no text content".to_string())?;
    let session_id = surface_session_id(&surface, &payload);
    let session_lock = surface_session_lock(&session_locks, &session_id).await;
    let _session_guard = session_lock.lock().await;
    let user_id = payload_string(&payload, "user_id");
    let thread_id = payload_string(&payload, "thread_id");
    let metadata = payload
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let inbox = state.services.surface.record_inbox_received(
        &surface,
        &message_id,
        &payload,
        &session_id,
        thread_id.clone(),
        user_id.clone(),
    )?;
    if inbox.duplicate {
        tracing::info!(
            %surface,
            %message_id,
            status = %inbox.record.status,
            "surface message ignored as durable duplicate"
        );
        return Ok(());
    }
    state
        .services
        .surface
        .mark_inbox_processing(&inbox.record.idempotency_key)?;
    ensure_surface_runtime_session(&state, &surface, &session_id, user_id.as_deref(), &metadata)
        .await?;

    state
        .services
        .session
        .append_timeline_event(
            &session_id,
            "SurfaceMessageReceived",
            serde_json::json!({
                "type": "SurfaceMessageReceived",
                "surface": surface,
                "message_id": message_id,
                "thread_id": thread_id,
                "user_id": user_id,
                "content_preview": content.chars().take(160).collect::<String>(),
                "payload": payload,
            }),
        )
        .await
        .map_err(|error| error.to_string())?;

    let runtime_service = state
        .services
        .runtime
        .as_ref()
        .ok_or_else(|| "runtime service unavailable".to_string())?;
    let turn_policy = surface_turn_policy(&content);
    let execution = match runtime_service
        .run_turn_with_options(
            &session_id,
            None,
            content.clone(),
            turn_policy.timeout,
            RuntimeTurnOptions {
                profile: turn_policy.profile,
                max_iterations: Some(turn_policy.max_iterations),
            },
        )
        .await
    {
        Ok(execution) => execution,
        Err(error) => {
            let message = error.message();
            append_surface_timeline_event(
                &state,
                &session_id,
                "SurfaceMessageProcessingFailed",
                serde_json::json!({
                    "type": "SurfaceMessageProcessingFailed",
                    "surface": surface.clone(),
                    "message_id": message_id.clone(),
                    "error": message,
                }),
            )
            .await?;
            state
                .services
                .surface
                .mark_inbox_failed(&inbox.record.idempotency_key, message.clone())?;
            send_surface_failure_notice(
                &state,
                &surface,
                &payload,
                &session_id,
                &message_id,
                &message,
            )
            .await;
            notify_surface_processing_lifecycle(
                &state,
                &surface,
                "message.processing_failed",
                &message_id,
                Some(message.clone()),
            )
            .await;
            return Err(message);
        }
    };
    if let Some(snapshot) = runtime_service.session_snapshot(&session_id).await {
        runtime_service
            .sync_session_snapshot(&session_id, &snapshot)
            .await
            .map_err(|error| error.to_string())?;
    }
    let response_text = final_text(&execution.summary);
    state
        .services
        .session
        .append_timeline_event(
            &session_id,
            "SurfaceMessageProcessed",
            serde_json::json!({
                "type": "SurfaceMessageProcessed",
                "surface": surface.clone(),
                "message_id": message_id.clone(),
                "turn_id": execution.receipt.turn_id,
                "context_turn_report": execution.summary.context_turn_report,
                "response_preview": response_text.chars().take(240).collect::<String>(),
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    state.services.surface.mark_inbox_processed(
        &inbox.record.idempotency_key,
        Some(execution.receipt.turn_id.to_string()),
    )?;

    if response_text.trim().is_empty() {
        notify_surface_processing_lifecycle(
            &state,
            &surface,
            "message.processing_complete",
            &message_id,
            None,
        )
        .await;
        return Ok(());
    }
    let recipient = surface_reply_recipient(&payload)
        .or(thread_id.clone())
        .or(user_id.clone())
        .unwrap_or_else(|| session_id.clone());
    let outbound_request = SurfaceSendRequest {
        surface: surface.clone(),
        recipient,
        thread: thread_id,
        text: response_text,
        metadata: serde_json::json!({
            "reply_to": message_id,
            "source_session_id": session_id,
            "source": "surface_ingress_dispatcher",
        }),
    };
    let outbound = match state.services.surface.send(outbound_request).await {
        Ok(outbound) => outbound,
        Err(error) => {
            append_surface_timeline_event(
                &state,
                &session_id,
                "SurfaceMessageReplyFailed",
                serde_json::json!({
                    "type": "SurfaceMessageReplyFailed",
                    "surface": surface.clone(),
                    "message_id": message_id.clone(),
                    "error": error,
                }),
            )
            .await?;
            state
                .services
                .surface
                .mark_inbox_reply_failed(&inbox.record.idempotency_key, error.clone())?;
            notify_surface_processing_lifecycle(
                &state,
                &surface,
                "message.processing_failed",
                &message_id,
                Some(error.clone()),
            )
            .await;
            return Err(error);
        }
    };
    if let Some(error) = outbound.error.clone() {
        append_surface_timeline_event(
            &state,
            &session_id,
            "SurfaceMessageReplyFailed",
            serde_json::json!({
                "type": "SurfaceMessageReplyFailed",
                "surface": surface.clone(),
                "message_id": message_id.clone(),
                "error": error.message.clone(),
                "code": error.code.clone(),
                "outbound": outbound,
            }),
        )
        .await?;
        state
            .services
            .surface
            .mark_inbox_reply_failed(&inbox.record.idempotency_key, error.message.clone())?;
        notify_surface_processing_lifecycle(
            &state,
            &surface,
            "message.processing_failed",
            &message_id,
            Some(error.message.clone()),
        )
        .await;
        return Err(error.message.clone());
    }
    state
        .services
        .surface
        .mark_inbox_replied(&inbox.record.idempotency_key)?;
    notify_surface_processing_lifecycle(
        &state,
        &surface,
        "message.processing_complete",
        &message_id,
        None,
    )
    .await;
    state
        .services
        .session
        .append_timeline_event(
            &session_id,
            "SurfaceMessageReplied",
            serde_json::json!({
                "type": "SurfaceMessageReplied",
                "surface": surface.clone(),
                "message_id": message_id,
                "outbound": outbound,
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn notify_surface_processing_lifecycle(
    state: &AppState,
    surface: &str,
    action: &str,
    message_id: &str,
    error: Option<String>,
) {
    let result = state
        .services
        .surface
        .action(SurfaceActionRequest {
            surface: surface.to_string(),
            action: action.to_string(),
            payload: serde_json::json!({
                "message_id": message_id,
                "error": error,
                "source": "surface_ingress_dispatcher",
            }),
        })
        .await;
    if let Err(error) = result {
        tracing::debug!(
            %surface,
            %action,
            %message_id,
            error = %error,
            "surface processing lifecycle notification failed"
        );
    }
}

fn surface_context_profile(content: &str) -> runtime::ContextProfile {
    let normalized = content.to_ascii_lowercase();
    let deep_markers = [
        "深度",
        "分析",
        "调研",
        "重构",
        "修改",
        "测试",
        "执行",
        "代码",
        "检查",
        "核查",
        "确认",
        "更新",
        "文档",
        "debug",
        "readme",
        "review",
        "refactor",
        "test",
        "implement",
        "investigate",
    ];
    if deep_markers
        .iter()
        .any(|marker| normalized.contains(marker))
    {
        runtime::ContextProfile::DeepInvestigation
    } else {
        runtime::ContextProfile::SurfaceQuickReply
    }
}

fn surface_turn_policy(content: &str) -> SurfaceTurnPolicy {
    let profile = surface_context_profile(content);
    match profile {
        runtime::ContextProfile::DeepInvestigation => SurfaceTurnPolicy {
            profile,
            timeout: SURFACE_DEEP_WALL_CLOCK,
            max_iterations: SURFACE_DEEP_MAX_ITERATIONS,
        },
        _ => SurfaceTurnPolicy {
            profile,
            timeout: SURFACE_QUICK_WALL_CLOCK,
            max_iterations: SURFACE_QUICK_MAX_ITERATIONS,
        },
    }
}

async fn send_surface_failure_notice(
    state: &AppState,
    surface: &str,
    payload: &serde_json::Value,
    session_id: &str,
    message_id: &str,
    error: &str,
) {
    let recipient = surface_reply_recipient(payload)
        .or_else(|| payload_string(payload, "thread_id"))
        .or_else(|| payload_string(payload, "user_id"))
        .unwrap_or_else(|| session_id.to_string());
    let thread = payload_string(payload, "thread_id");
    let result = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: surface.to_string(),
            recipient,
            thread,
            text: surface_failure_notice_text(error),
            metadata: serde_json::json!({
                "reply_to": message_id,
                "source_session_id": session_id,
                "source": "surface_ingress_dispatcher",
                "failure_notice": true,
                "failure_reason": error,
            }),
        })
        .await;
    match result {
        Ok(outbound) if outbound.error.is_none() => {}
        Ok(outbound) => {
            tracing::warn!(
                %surface,
                %message_id,
                error = ?outbound.error,
                "surface failure notice returned operation error"
            );
        }
        Err(send_error) => {
            tracing::warn!(
                %surface,
                %message_id,
                error = %send_error,
                "surface failure notice delivery failed"
            );
        }
    }
}

fn surface_failure_notice_text(error: &str) -> String {
    format!(
        "这条消息已经进入 Cowd，但本次 AI 处理没有在渠道执行预算内完成，因此没有生成完整结果。\n\n状态：已记录失败并清理处理中标记。\n原因：{error}\n\n你可以缩小问题范围后重发，或在 WebUI/TUI 中查看该 surface inbox 并执行重放。"
    )
}

async fn surface_session_lock(
    session_locks: &Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    session_id: &str,
) -> Arc<Mutex<()>> {
    let mut locks = session_locks.lock().await;
    locks
        .entry(session_id.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

async fn append_surface_timeline_event(
    state: &AppState,
    session_id: &str,
    event_type: &'static str,
    payload: serde_json::Value,
) -> Result<(), String> {
    state
        .services
        .session
        .append_timeline_event(session_id, event_type, payload)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

async fn ensure_surface_runtime_session(
    state: &AppState,
    surface: &str,
    session_id: &str,
    user_id: Option<&str>,
    metadata: &serde_json::Value,
) -> Result<(), String> {
    let runtime_service = state
        .services
        .runtime
        .as_ref()
        .ok_or_else(|| "runtime service unavailable".to_string())?;
    if runtime_service.has_active_session(session_id) {
        return Ok(());
    }
    let model = default_surface_session_model(state);
    let mut session = runtime::Session::new();
    session.session_id = session_id.to_string();
    session.model = Some(model.clone());
    let runtime = if let Some(store) = state.services.session.unified_store() {
        crate::runtime_factory::create_runtime_entry_with_session_store(
            store,
            session,
            session_id,
            model.clone(),
            surface_system_prompt(surface),
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    } else {
        crate::runtime_factory::create_runtime_entry(
            session,
            session_id,
            model.clone(),
            surface_system_prompt(surface),
            true,
            true,
            None,
            runtime::PermissionMode::WorkspaceWrite,
            None,
            None,
        )
    }
    .map_err(|error| error.to_string())?;
    runtime_service
        .register_runtime(session_id.to_string(), runtime)
        .map_err(|error| error.to_string())?;
    if state.services.session.has_unified_store()
        && state
            .services
            .session
            .stored_session(session_id)
            .await
            .map_err(|error| error.to_string())?
            .is_none()
    {
        let record = surface_session_record(surface, session_id, user_id, Some(model), metadata);
        state
            .services
            .session
            .upsert_stored_session(&record)
            .await
            .map_err(|error| error.to_string())?;
    }
    state
        .services
        .session
        .append_timeline_event(
            session_id,
            "SurfaceSessionRuntimeActivated",
            serde_json::json!({
                "type": "SurfaceSessionRuntimeActivated",
                "surface": surface,
                "session_id": session_id,
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn surface_system_prompt(surface: &str) -> Vec<String> {
    vec![format!(
        "你正在通过 `{surface}` 外部 surface 回复用户。必须优先给出可见、简洁、可执行的阶段性结果。\
        如果任务需要读代码、检查 README、调研或测试，只检查足以支撑结论的关键证据；不要进行无边界穷举。\
        如果当前 turn 的信息或时间不足，直接说明已检查内容、当前判断、剩余风险和建议下一步，而不是持续调用工具直到超时。\
        外部 surface 的用户体验要求：宁可给出有证据的阶段性结论，也不能让用户长时间没有任何回复。"
    )]
}

fn surface_session_id(surface: &str, payload: &serde_json::Value) -> String {
    payload_string(payload, "session")
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            let metadata = payload.get("metadata").unwrap_or(&serde_json::Value::Null);
            let chat_id = payload_string(metadata, "chat_id")
                .or_else(|| payload_string(payload, "thread_id"))
                .unwrap_or_else(|| "default".to_string());
            let user_id =
                payload_string(payload, "user_id").unwrap_or_else(|| "unknown".to_string());
            format!("{surface}:{user_id}:{chat_id}")
        })
}

fn surface_reply_recipient(payload: &serde_json::Value) -> Option<String> {
    let metadata = payload.get("metadata").unwrap_or(&serde_json::Value::Null);
    payload_string(metadata, "chat_id")
        .or_else(|| payload_string(payload, "thread_id"))
        .or_else(|| payload_string(payload, "user_id"))
}

fn default_surface_session_model(state: &AppState) -> String {
    state
        .services
        .system
        .runtime_config(&state.workspace_root, &state.config_home)
        .ok()
        .and_then(|config| config.model().map(str::to_string))
        .filter(|model| !model.trim().is_empty())
        .unwrap_or_else(|| crate::DEFAULT_MODEL.to_string())
}

fn surface_session_record(
    surface: &str,
    session_id: &str,
    user_id: Option<&str>,
    model: Option<String>,
    metadata: &serde_json::Value,
) -> SessionRecord {
    let now = chrono::Utc::now().to_rfc3339();
    let chat_id = payload_string(metadata, "chat_id").unwrap_or_else(|| session_id.to_string());
    SessionRecord {
        session_id: session_id.to_string(),
        platform: surface.to_string(),
        chat_id,
        user_id: user_id.map(ToOwned::to_owned),
        model,
        created_at: now.clone(),
        last_activity: now,
        message_count: 0,
        reset_policy: "none".to_string(),
        metadata_json: Some(
            serde_json::json!({
                "title": format!("{} {}", surface, session_id.chars().take(8).collect::<String>()),
                "surface": surface,
                "source": "surface_ingress_dispatcher",
                "metadata": metadata,
            })
            .to_string(),
        ),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

fn payload_string(payload: &serde_json::Value, key: &str) -> Option<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn payload_fingerprint_id(surface: &str, payload: &serde_json::Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(surface.as_bytes());
    hasher.update(b":");
    hasher.update(
        serde_json::to_string(payload)
            .unwrap_or_default()
            .as_bytes(),
    );
    format!("generated:{:x}", hasher.finalize())
}

fn final_text(summary: &runtime::TurnSummary) -> String {
    summary
        .assistant_messages
        .last()
        .map(|message| {
            message
                .blocks
                .iter()
                .filter_map(|block| match block {
                    runtime::ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("")
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surface_session_id_prefers_explicit_session() {
        let payload = serde_json::json!({
            "session": "session-explicit",
            "user_id": "user-a",
            "metadata": { "chat_id": "chat-a" }
        });

        assert_eq!(surface_session_id("feishu", &payload), "session-explicit");
    }

    #[test]
    fn surface_session_id_uses_surface_user_and_chat() {
        let payload = serde_json::json!({
            "user_id": "user-a",
            "metadata": { "chat_id": "chat-a" }
        });

        assert_eq!(
            surface_session_id("feishu", &payload),
            "feishu:user-a:chat-a"
        );
    }

    #[test]
    fn surface_reply_recipient_prefers_chat_id() {
        let payload = serde_json::json!({
            "user_id": "user-a",
            "thread_id": "thread-a",
            "metadata": { "chat_id": "chat-a" }
        });

        assert_eq!(surface_reply_recipient(&payload).as_deref(), Some("chat-a"));
    }

    #[test]
    fn surface_ingress_durable_idempotency_key_normalizes_surface_aliases() {
        assert_eq!(
            crate::surface_host::message_store::inbound_idempotency_key("lark", "msg-1"),
            "feishu:msg-1"
        );
    }

    #[test]
    fn surface_ingress_fallback_message_id_is_stable_for_same_payload() {
        let payload = serde_json::json!({
            "text": "hello",
            "user_id": "user-a",
            "metadata": {"chat_id": "chat-a"}
        });
        assert_eq!(
            payload_fingerprint_id("feishu", &payload),
            payload_fingerprint_id("feishu", &payload)
        );
    }

    #[test]
    fn readme_followup_uses_deep_surface_budget() {
        let policy = surface_turn_policy("我已经更新，看是否最新的readme还有问题");

        assert_eq!(policy.profile, runtime::ContextProfile::DeepInvestigation);
        assert_eq!(policy.max_iterations, SURFACE_DEEP_MAX_ITERATIONS);
        assert_eq!(policy.timeout, SURFACE_DEEP_WALL_CLOCK);
    }

    #[test]
    fn short_surface_message_uses_quick_budget() {
        let policy = surface_turn_policy("你好");

        assert_eq!(policy.profile, runtime::ContextProfile::SurfaceQuickReply);
        assert_eq!(policy.max_iterations, SURFACE_QUICK_MAX_ITERATIONS);
        assert_eq!(policy.timeout, SURFACE_QUICK_WALL_CLOCK);
    }

    #[test]
    fn surface_failure_notice_text_is_visible_and_actionable() {
        let text = surface_failure_notice_text("turn timed out after 240s");

        assert!(text.contains("已经进入 Cowd"));
        assert!(text.contains("turn timed out after 240s"));
        assert!(text.contains("重放"));
    }
}
