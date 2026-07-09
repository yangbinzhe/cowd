use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use memory::SessionRecord;
use sha2::{Digest, Sha256};
use surface::{message::MessageActionKind, SurfaceActionRequest, SurfaceFrame, SurfaceSendRequest};
use tokio::sync::Mutex;

use crate::api_routes::AppState;
use crate::runtime_service::RuntimeTurnOptions;

const SURFACE_QUICK_WALL_CLOCK: Duration = Duration::from_secs(240);
const SURFACE_MEDIA_WALL_CLOCK: Duration = Duration::from_secs(900);
const SURFACE_DEEP_WALL_CLOCK: Duration = Duration::from_secs(900);
const SURFACE_QUICK_MAX_ITERATIONS: usize = 12;
const SURFACE_MEDIA_MAX_ITERATIONS: usize = 24;
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
        .or_else(|| {
            (!payload_media_attachments(&payload, &message_id).is_empty())
                .then(|| "[Attachment]".to_string())
        })
        .ok_or_else(|| "surface message has no text content".to_string())?;
    let session_id = surface_session_id(&surface, &payload);
    let session_lock = surface_session_lock(&session_locks, &session_id).await;
    let session_guard = session_lock.lock().await;
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
    let current_media = payload_media_attachments(&payload, &message_id);
    let recent_media = if current_media.is_empty() && content_references_surface_media(&content) {
        recent_surface_media(&state, &surface, &session_id, &message_id)
    } else {
        Vec::new()
    };
    let current_resources =
        register_surface_resources(&state, &surface, &session_id, &current_media);
    let recent_resources = register_surface_resources(&state, &surface, &session_id, &recent_media);
    append_surface_resource_evidence(
        &state,
        &surface,
        &session_id,
        &message_id,
        &current_resources,
        &recent_resources,
    )
    .await?;
    let pre_messages = surface_runtime_pre_messages(&content, &current_media, &recent_media);
    let runtime_content = surface_runtime_content(&content, &current_resources, &recent_resources);
    let turn_policy = surface_turn_policy(&runtime_content);
    if runtime_service
        .session_input_projection(&session_id)
        .await
        .map_err(|error| error.message())?
        .active_turn_id
        .is_some()
    {
        let receipt = runtime_service
            .admit_session_input(
                SessionInputEnvelope::text(
                    session_id.clone(),
                    InputSourceKind::Surface,
                    runtime_content,
                )
                .with_source_ref(format!("surface:{surface}"))
                .with_source_message_id(message_id.clone())
                .with_idempotency_key(inbox.record.idempotency_key.clone())
                .with_metadata(serde_json::json!({
                    "surface": surface.clone(),
                    "thread_id": thread_id.clone(),
                    "user_id": user_id.clone(),
                    "payload_metadata": metadata,
                })),
            )
            .await
            .map_err(|error| error.message())?;
        state.services.surface.mark_inbox_processed(
            &inbox.record.idempotency_key,
            receipt.active_turn_id.as_ref().map(ToString::to_string),
        )?;
        append_surface_timeline_event(
            &state,
            &session_id,
            "SurfaceMessageAttachedToActiveTurn",
            serde_json::json!({
                "type": "SurfaceMessageAttachedToActiveTurn",
                "surface": surface.clone(),
                "message_id": message_id.clone(),
                "input_receipt": receipt,
            }),
        )
        .await?;
        notify_surface_processing_lifecycle(
            &state,
            &surface,
            MessageActionKind::ProcessingComplete.as_str(),
            &message_id,
            None,
        )
        .await;
        return Ok(());
    }
    drop(session_guard);
    let accepted_turn = runtime_service
        .accept_turn_with_options(&session_id, None, runtime_content.clone())
        .await
        .map_err(|error| error.message())?;
    let execution = match runtime_service
        .run_accepted_turn_with_options(
            &session_id,
            accepted_turn.turn_id.clone(),
            runtime_content,
            turn_policy.timeout,
            RuntimeTurnOptions {
                profile: turn_policy.profile,
                max_iterations: Some(turn_policy.max_iterations),
                pre_messages,
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
                MessageActionKind::ProcessingFailed.as_str(),
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
            MessageActionKind::ProcessingComplete.as_str(),
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
    let platform_reply_to = surface_platform_reply_to(&payload, &message_id);
    let outbound_request = SurfaceSendRequest {
        surface: surface.clone(),
        recipient,
        thread: thread_id,
        text: response_text,
        metadata: serde_json::json!({
            "reply_to": platform_reply_to,
            "local_reply_to": message_id,
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
                MessageActionKind::ProcessingFailed.as_str(),
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
            MessageActionKind::ProcessingFailed.as_str(),
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
        MessageActionKind::ProcessingComplete.as_str(),
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
    let normalized = surface_intent_text(content).to_ascii_lowercase();
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

fn surface_intent_text(content: &str) -> &str {
    content
        .split_once("\n## Attached Resources")
        .or_else(|| content.split_once("\n## Resource registration failures"))
        .map(|(intent, _)| intent.trim())
        .unwrap_or_else(|| content.trim())
}

fn surface_turn_policy(content: &str) -> SurfaceTurnPolicy {
    let profile = surface_context_profile(content);
    if profile != runtime::ContextProfile::DeepInvestigation
        && surface_content_has_media_attachment(content)
    {
        return SurfaceTurnPolicy {
            profile,
            timeout: SURFACE_MEDIA_WALL_CLOCK,
            max_iterations: SURFACE_MEDIA_MAX_ITERATIONS,
        };
    }
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

fn surface_content_has_media_attachment(content: &str) -> bool {
    content.contains("## Attached Resources")
        || content.contains("Resource registration failures")
        || content.contains("resource://")
}

fn surface_platform_reply_to(payload: &serde_json::Value, message_id: &str) -> String {
    payload
        .get("metadata")
        .and_then(|metadata| payload_string(metadata, "replayed_from_message_id"))
        .unwrap_or_else(|| message_id.to_string())
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
    let platform_reply_to = surface_platform_reply_to(payload, message_id);
    let result = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: surface.to_string(),
            recipient,
            thread,
            text: surface_failure_notice_text(error),
            metadata: serde_json::json!({
                "reply_to": platform_reply_to,
                "local_reply_to": message_id,
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

async fn append_surface_resource_evidence(
    state: &AppState,
    surface: &str,
    session_id: &str,
    message_id: &str,
    current_resources: &[SurfaceResourceRegistration],
    recent_resources: &[SurfaceResourceRegistration],
) -> Result<(), String> {
    if current_resources.is_empty() && recent_resources.is_empty() {
        return Ok(());
    }
    append_surface_timeline_event(
        state,
        session_id,
        "SurfaceMessageResourcesRegistered",
        serde_json::json!({
            "type": "SurfaceMessageResourcesRegistered",
            "surface": surface,
            "message_id": message_id,
            "current": surface_resource_evidence_rows(current_resources),
            "recent": surface_resource_evidence_rows(recent_resources),
        }),
    )
    .await
}

fn surface_resource_evidence_rows(
    resources: &[SurfaceResourceRegistration],
) -> Vec<serde_json::Value> {
    resources
        .iter()
        .map(|registration| {
            let resource = registration.resource.as_ref().map(|(resource, hint)| {
                serde_json::json!({
                    "resource_id": resource.id,
                    "uri": resource.uri,
                    "kind": resource.kind,
                    "declared_mime": resource.declared_mime,
                    "detected_mime": resource.detected_mime,
                    "storage_path": resource.storage_path,
                    "hint": hint,
                })
            });
            serde_json::json!({
                "source_message_id": registration.attachment.source_message_id,
                "local_path": registration.attachment.local_path,
                "media_type": registration.attachment.media_type,
                "resource": resource,
                "status": if registration.resource.is_some() { "registered" } else { "failed" },
            })
        })
        .collect()
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SurfaceMediaAttachment {
    source_message_id: String,
    media_type: String,
    local_path: String,
}

#[derive(Debug, Clone)]
struct SurfaceResourceRegistration {
    attachment: SurfaceMediaAttachment,
    resource: Option<(runtime::ResourceEnvelope, runtime::ResourceHint)>,
    error: Option<String>,
}

fn surface_runtime_content(
    content: &str,
    current_resources: &[SurfaceResourceRegistration],
    recent_resources: &[SurfaceResourceRegistration],
) -> String {
    if current_resources.is_empty() && recent_resources.is_empty() {
        return content.to_string();
    }
    let mut rendered = content.to_string();

    let resource_pairs = current_resources
        .iter()
        .chain(recent_resources.iter())
        .filter_map(|registration| registration.resource.clone())
        .collect::<Vec<_>>();
    rendered.push_str(&runtime::render_resource_context_markdown(&resource_pairs));

    let failures = current_resources
        .iter()
        .chain(recent_resources.iter())
        .filter(|registration| registration.error.is_some())
        .collect::<Vec<_>>();
    if !failures.is_empty() {
        rendered.push_str("\n\n## Resource registration failures\n\n");
        for registration in failures {
            rendered.push_str(&format!(
                "- type: {}, source_message: {}, error: {}\n",
                registration.attachment.media_type,
                registration.attachment.source_message_id,
                registration.error.as_deref().unwrap_or("unknown")
            ));
        }
    }
    rendered
}

fn register_surface_resources(
    state: &AppState,
    surface: &str,
    session_id: &str,
    media: &[SurfaceMediaAttachment],
) -> Vec<SurfaceResourceRegistration> {
    media
        .iter()
        .map(|attachment| {
            match runtime::register_resource_from_path(
                &state.config_home,
                &attachment.local_path,
                format!("surface:{surface}"),
                Some(attachment.source_message_id.clone()),
                Some(session_id.to_string()),
                Some(attachment.media_type.clone()),
            ) {
                Ok(resource) => SurfaceResourceRegistration {
                    attachment: attachment.clone(),
                    resource: Some(resource),
                    error: None,
                },
                Err(error) => {
                    tracing::warn!(
                        surface,
                        session_id,
                        media_type = %attachment.media_type,
                        local_path = %attachment.local_path,
                        source_message_id = %attachment.source_message_id,
                        error = %error,
                        "failed to register surface media as runtime resource"
                    );
                    SurfaceResourceRegistration {
                        attachment: attachment.clone(),
                        resource: None,
                        error: Some(error),
                    }
                }
            }
        })
        .collect()
}

fn surface_runtime_pre_messages(
    content: &str,
    current_media: &[SurfaceMediaAttachment],
    recent_media: &[SurfaceMediaAttachment],
) -> Vec<runtime::ConversationMessage> {
    current_media
        .iter()
        .chain(recent_media.iter())
        .filter(|attachment| media_attachment_is_image(attachment))
        .filter_map(|attachment| {
            runtime::image_user_message_from_path(
                &attachment.local_path,
                &attachment.media_type,
                content,
            )
            .map_err(|error| {
                tracing::warn!(
                    media_type = %attachment.media_type,
                    local_path = %attachment.local_path,
                    source_message_id = %attachment.source_message_id,
                    error = %error,
                    "failed to prepare surface image attachment for runtime"
                );
            })
            .ok()
        })
        .collect()
}

fn media_attachment_is_image(attachment: &SurfaceMediaAttachment) -> bool {
    attachment.media_type.starts_with("image/")
        || Path::new(&attachment.local_path)
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| {
                matches!(
                    extension.to_ascii_lowercase().as_str(),
                    "png" | "jpg" | "jpeg" | "gif" | "webp"
                )
            })
            .unwrap_or(false)
}

fn content_references_surface_media(content: &str) -> bool {
    let lowered = content.to_ascii_lowercase();
    let media_words = [
        "image",
        "photo",
        "picture",
        "attachment",
        "file",
        "video",
        "audio",
        "media",
        "图片",
        "照片",
        "图像",
        "附件",
        "文件",
        "视频",
        "语音",
        "音频",
        "刚才",
        "上面",
        "前面",
        "上一条",
        "发的",
    ];
    if media_words.iter().any(|word| lowered.contains(word)) {
        return true;
    }
    let trimmed = content.trim();
    trimmed.chars().count() <= 16 && (trimmed.contains("这个") || trimmed.contains("这张"))
}

fn recent_surface_media(
    state: &AppState,
    surface: &str,
    session_id: &str,
    current_message_id: &str,
) -> Vec<SurfaceMediaAttachment> {
    let mut attachments = state
        .services
        .surface
        .inbox(surface)
        .into_iter()
        .filter(|record| record.runtime_session_id.as_deref() == Some(session_id))
        .filter(|record| record.message_id != current_message_id)
        .filter_map(|record| {
            let attachments = payload_media_attachments(&record.payload_json, &record.message_id);
            (!attachments.is_empty()).then_some((record.received_at_ms, attachments))
        })
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::new(), |mut acc, (received_at_ms, attachments)| {
            for attachment in attachments {
                acc.push((received_at_ms, attachment));
            }
            acc
        });
    attachments.sort_by_key(|(received_at_ms, _)| std::cmp::Reverse(*received_at_ms));
    attachments
        .into_iter()
        .map(|(_, attachment)| attachment)
        .take(3)
        .collect()
}

fn payload_media_attachments(
    payload: &serde_json::Value,
    source_message_id: &str,
) -> Vec<SurfaceMediaAttachment> {
    let media_urls = payload_string_array(payload, "media_urls");
    let media_types = payload_string_array(payload, "media_types");
    media_urls
        .into_iter()
        .enumerate()
        .map(|(idx, local_path)| {
            let media_type = media_types
                .get(idx)
                .map(String::as_str)
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("application/octet-stream")
                .to_string();
            SurfaceMediaAttachment {
                source_message_id: source_message_id.to_string(),
                media_type,
                local_path,
            }
        })
        .collect()
}

fn payload_string_array(payload: &serde_json::Value, key: &str) -> Vec<String> {
    payload
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect()
        })
        .unwrap_or_default()
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
    fn media_surface_message_uses_media_budget_without_deep_context() {
        let policy = surface_turn_policy(
            "[Image]\n\n## Attached Resources\n\n### resource://res_test\n- kind: image\n",
        );

        assert_eq!(policy.profile, runtime::ContextProfile::SurfaceQuickReply);
        assert_eq!(policy.max_iterations, SURFACE_MEDIA_MAX_ITERATIONS);
        assert_eq!(policy.timeout, SURFACE_MEDIA_WALL_CLOCK);
    }

    #[test]
    fn media_surface_message_uses_deep_budget_when_user_intent_is_deep() {
        let policy = surface_turn_policy(
            "请分析这张图片是否有问题\n\n## Attached Resources\n\n### resource://res_test\n- kind: image\n",
        );

        assert_eq!(policy.profile, runtime::ContextProfile::DeepInvestigation);
        assert_eq!(policy.max_iterations, SURFACE_DEEP_MAX_ITERATIONS);
        assert_eq!(policy.timeout, SURFACE_DEEP_WALL_CLOCK);
    }

    #[test]
    fn replay_uses_original_message_id_as_platform_reply_target() {
        let payload = serde_json::json!({
            "metadata": {
                "replayed_from_message_id": "om_original"
            }
        });

        assert_eq!(
            surface_platform_reply_to(&payload, "om_original:replay:synthetic"),
            "om_original"
        );
    }

    #[test]
    fn surface_runtime_content_includes_resource_hints() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image_path = temp.path().join("img_001.png");
        std::fs::write(&image_path, b"fake-png").expect("image writes");
        let store = runtime::ResourceStore::default_for_config_home(&temp.path().join("home"));
        let resource = store
            .register_resource_from_path(
                &image_path,
                "surface:feishu",
                Some("current message".to_string()),
                Some("session-1".to_string()),
                Some("image/png".to_string()),
            )
            .expect("resource registers");
        let registration = SurfaceResourceRegistration {
            attachment: SurfaceMediaAttachment {
                source_message_id: "current message".to_string(),
                media_type: "image/png".to_string(),
                local_path: image_path.display().to_string(),
            },
            resource: Some(resource),
            error: None,
        };

        let content = surface_runtime_content("[Image]", &[registration], &[]);

        assert!(content.contains("## Attached Resources"));
        assert!(content.contains("resource://res_"));
        assert!(content.contains("kind: image"));
        assert!(content.contains("vision_analyze"));
    }

    #[test]
    fn surface_runtime_content_includes_recent_resources_for_followup() {
        let temp = tempfile::tempdir().expect("tempdir");
        let image_path = temp.path().join("img_002.jpg");
        std::fs::write(&image_path, b"fake-jpg").expect("image writes");
        let store = runtime::ResourceStore::default_for_config_home(&temp.path().join("home"));
        let resource = store
            .register_resource_from_path(
                &image_path,
                "surface:feishu",
                Some("om_image".to_string()),
                Some("session-1".to_string()),
                Some("image/jpeg".to_string()),
            )
            .expect("resource registers");
        let recent = vec![SurfaceResourceRegistration {
            attachment: SurfaceMediaAttachment {
                source_message_id: "om_image".to_string(),
                media_type: "image/jpeg".to_string(),
                local_path: image_path.display().to_string(),
            },
            resource: Some(resource),
            error: None,
        }];

        let content = surface_runtime_content("这个图片里面有什么", &[], &recent);

        assert!(content.contains("## Attached Resources"));
        assert!(content.contains("resource://res_"));
        assert!(content.contains("kind: image"));
        assert!(content.contains("vision_analyze"));
    }

    #[test]
    fn surface_runtime_content_includes_audio_boundary_for_mp3() {
        let temp = tempfile::tempdir().expect("tempdir");
        let audio_path = temp.path().join("voice.mp3");
        std::fs::write(&audio_path, b"fake-mp3").expect("audio writes");
        let store = runtime::ResourceStore::default_for_config_home(&temp.path().join("home"));
        let resource = store
            .register_resource_from_path(
                &audio_path,
                "surface:feishu",
                Some("om_audio".to_string()),
                Some("session-1".to_string()),
                Some("application/octet-stream".to_string()),
            )
            .expect("resource registers");
        let current = vec![SurfaceResourceRegistration {
            attachment: SurfaceMediaAttachment {
                source_message_id: "om_audio".to_string(),
                media_type: "application/octet-stream".to_string(),
                local_path: audio_path.display().to_string(),
            },
            resource: Some(resource),
            error: None,
        }];

        let content = surface_runtime_content("[Attachment]", &current, &[]);

        assert!(content.contains("## Attached Resources"));
        assert!(content.contains("kind: audio"));
        assert!(content.contains("transcription skill/plugin"));
        assert!(content.contains("Do not claim audio content"));
    }

    #[test]
    fn surface_runtime_pre_messages_attach_current_image_block() {
        let path = std::env::temp_dir().join(format!(
            "cowd-edge-image-pre-message-{}.jpg",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, b"fake-jpeg-bytes").expect("test image should write");
        let media = vec![SurfaceMediaAttachment {
            source_message_id: "om_image".to_string(),
            media_type: "image/jpeg".to_string(),
            local_path: path.display().to_string(),
        }];

        let messages = surface_runtime_pre_messages("描述这张图片", &media, &[]);

        assert_eq!(messages.len(), 1);
        assert!(messages[0]
            .blocks
            .iter()
            .any(|block| matches!(block, runtime::ContentBlock::Image { media_type, source_path, .. }
                if media_type == "image/jpeg" && source_path.as_deref() == Some(path.to_string_lossy().as_ref()))));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn plain_surface_text_does_not_reference_recent_media() {
        assert!(!content_references_surface_media("好的"));
        assert!(!content_references_surface_media("谢谢"));
    }

    #[test]
    fn media_followup_references_recent_media() {
        assert!(content_references_surface_media("这个图片里面有什么"));
        assert!(content_references_surface_media("刚才发的附件看一下"));
    }

    #[test]
    fn payload_with_media_can_use_attachment_placeholder() {
        let payload = serde_json::json!({
            "media_urls": ["/tmp/report.pdf"],
            "media_types": ["application/pdf"]
        });

        let content = payload_string(&payload, "text")
            .or_else(|| payload_string(&payload, "content"))
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                (!payload_media_attachments(&payload, "msg-1").is_empty())
                    .then(|| "[Attachment]".to_string())
            });

        assert_eq!(content.as_deref(), Some("[Attachment]"));
    }

    #[test]
    fn surface_failure_notice_text_is_visible_and_actionable() {
        let text = surface_failure_notice_text("turn timed out after 240s");

        assert!(text.contains("已经进入 Cowd"));
        assert!(text.contains("turn timed out after 240s"));
        assert!(text.contains("重放"));
    }
}
