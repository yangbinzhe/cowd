use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use memory::SessionRecord;
use surface::{SurfaceFrame, SurfaceSendRequest};
use tokio::sync::Mutex;

use crate::api_routes::AppState;

const SURFACE_TURN_TIMEOUT: Duration = Duration::from_secs(300);

pub(crate) fn spawn_surface_ingress_dispatcher(state: Arc<AppState>) {
    let mut rx = state.services.surface.subscribe_events();
    let seen = Arc::new(Mutex::new(HashSet::<String>::new()));
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
            let seen = seen.clone();
            tokio::spawn(async move {
                if let Err(error) = handle_surface_message(state, seen, surface, payload).await {
                    tracing::warn!(error = %error, "surface ingress message handling failed");
                }
            });
        }
    });
}

async fn handle_surface_message(
    state: Arc<AppState>,
    seen: Arc<Mutex<HashSet<String>>>,
    surface: String,
    payload: serde_json::Value,
) -> Result<(), String> {
    let message_id = payload_string(&payload, "message_id")
        .or_else(|| payload_string(&payload, "id"))
        .unwrap_or_else(|| format!("surface-message-{}", uuid::Uuid::new_v4()));
    let dedupe_key = format!("{surface}:{message_id}");
    {
        let mut seen = seen.lock().await;
        if !seen.insert(dedupe_key.clone()) {
            tracing::info!(%surface, %message_id, "surface message ignored as duplicate");
            return Ok(());
        }
    }

    let content = payload_string(&payload, "text")
        .or_else(|| payload_string(&payload, "content"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "surface message has no text content".to_string())?;
    let session_id = surface_session_id(&surface, &payload);
    let user_id = payload_string(&payload, "user_id");
    let thread_id = payload_string(&payload, "thread_id");
    let metadata = payload
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
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
    let execution = runtime_service
        .run_turn_with_timeout(&session_id, None, content, SURFACE_TURN_TIMEOUT)
        .await
        .map_err(|error| error.message())?;
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
                "surface": surface,
                "message_id": message_id,
                "turn_id": execution.receipt.turn_id,
                "response_preview": response_text.chars().take(240).collect::<String>(),
            }),
        )
        .await
        .map_err(|error| error.to_string())?;

    if response_text.trim().is_empty() {
        return Ok(());
    }
    let recipient = surface_reply_recipient(&payload)
        .or(thread_id.clone())
        .or(user_id.clone())
        .unwrap_or_else(|| session_id.clone());
    let outbound = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: surface.clone(),
            recipient,
            thread: thread_id,
            text: response_text,
            metadata: serde_json::json!({
                "reply_to": message_id,
                "source_session_id": session_id,
                "source": "surface_ingress_dispatcher",
            }),
        })
        .await
        .map_err(|error| error.to_string())?;
    state
        .services
        .session
        .append_timeline_event(
            &session_id,
            "SurfaceMessageReplied",
            serde_json::json!({
                "type": "SurfaceMessageReplied",
                "surface": surface,
                "message_id": message_id,
                "outbound": outbound,
            }),
        )
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
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
            vec![],
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
            vec![],
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
}
