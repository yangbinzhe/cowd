// ── RuntimeHost socket transition handler ───────────────────────
// owner: 0.9.292 Gateway RuntimeHost
// delete_by: 0.9.293
// replacement: Gateway HTTP/SSE service API
// new_old_path_policy: do not add new socket business commands

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::runtime_host::prompter::SocketPrompter;
use crate::runtime_host::singletons;
use crate::event_bus::SessionEventBus;
use crate::gateway::ActiveSessions;

// ── Stub callbacks (Phase A2) — will be wired to socket in Phase A3 ──

struct RuntimeHostToolCallback;

impl runtime::ToolCallback for RuntimeHostToolCallback {
    fn on_tool_start(&self, _id: &str, _name: &str, _preview: &str) {}
    fn on_tool_progress(&self, _id: &str, _name: &str, _progress: &str) {}
    fn on_tool_complete(
        &self,
        _id: &str,
        _name: &str,
        _result_summary: &str,
        _exit_code: Option<i32>,
    ) {}
}

struct RuntimeHostHookReporter;

impl runtime::HookProgressReporter for RuntimeHostHookReporter {
    fn on_event(&mut self, _event: &runtime::HookProgressEvent) {}
}

/// Handle a single Unix socket client connection.
/// Reads newline-delimited JSON commands and writes JSON responses.
/// Supported commands:
///   {"cmd":"create_session","model":"..."}
///   {"cmd":"chat","session_id":"...","content":"..."}
///   {"cmd":"list_sessions"}
pub(crate) async fn handle_unix_client(
    stream: UnixStream,
    sessions: Arc<ActiveSessions>,
    event_bus: Arc<SessionEventBus>,
    prompter_ref: Option<Arc<SocketPrompter>>,
) {
    let (reader, mut writer) = stream.into_split();
    let mut buf_reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        match buf_reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF
                break;
            }
            Ok(_n) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let response = match serde_json::from_str::<serde_json::Value>(trimmed) {
                    Ok(cmd) => {
                        match cmd.get("cmd").and_then(|c| c.as_str()) {
                            Some("create_session") => {
                                let model = cmd
                                    .get("model")
                                    .and_then(|m| m.as_str())
                                    .unwrap_or("claude-sonnet-4-6");
                                let session_id = uuid::Uuid::new_v4().to_string();
                                let session = runtime::Session::new();
                                match crate::build_runtime(
                                    session,
                                    &session_id,
                                    model.to_string(),
                                    vec![],
                                    true,
                                    true,
                                    None,
                                    runtime::PermissionMode::WorkspaceWrite,
                                    None,
                                    None,
                                ) {
                                    Ok(mut runtime) => {
                                        // Phase A2: inject runtime host capabilities.
                                        let cowd_bus = runtime::CowdEventBus::new();
                                        runtime = runtime
                                            .with_cowd_event_bus(cowd_bus)
                                            .with_tool_callback(Arc::new(RuntimeHostToolCallback))
                                            .with_hook_progress_reporter(Box::new(
                                                RuntimeHostHookReporter,
                                            ))
                                            .with_memory_manager(
                                                singletons::GLOBAL_MEMORY
                                                    .get()
                                                    .expect("GLOBAL_MEMORY must be initialised before session creation")
                                                    .clone(),
                                            );

                                        let _ = sessions.register(session_id.clone(), runtime);
                                        serde_json::json!({
                                            "ok": true,
                                            "session_id": session_id,
                                        })
                                    }
                                    Err(e) => {
                                        serde_json::json!({
                                            "ok": false,
                                            "error": format!("failed to build runtime: {e}"),
                                        })
                                    }
                                }
                            }
                            Some("chat") => {
                                let session_id = cmd
                                    .get("session_id")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or_default();
                                let content = cmd
                                    .get("content")
                                    .and_then(|c| c.as_str())
                                    .unwrap_or_default();

                                if session_id.is_empty() || content.is_empty() {
                                    serde_json::json!({
                                        "ok": false,
                                        "error": "session_id and content are required",
                                    })
                                } else {
                                    match sessions.get(session_id) {
                                        Some(entry) => {
                                            let mut guard = entry.lock().await;
                                            match guard
                                                .run_turn_async(content, &runtime::permissions::SharedPrompter::none())
                                                .await
                                            {
                                                Ok(summary) => {
                                                    let final_text = summary
                                                        .assistant_messages
                                                        .last()
                                                        .map(|msg| {
                                                            msg.blocks
                                                                .iter()
                                                                .filter_map(|block| match block {
                                                                    runtime::ContentBlock::Text { text } => {
                                                                        Some(text.as_str())
                                                                    }
                                                                    _ => None,
                                                                })
                                                                .collect::<Vec<_>>()
                                                                .join("")
                                                        })
                                                        .unwrap_or_default();

                                                    let sse_data = serde_json::json!({
                                                        "type": "TurnComplete",
                                                        "session_id": session_id,
                                                        "response": final_text,
                                                        "iterations": summary.iterations,
                                                    });
                                                    event_bus
                                                        .broadcast(session_id, &sse_data.to_string())
                                                        .await;

                                                    serde_json::json!({
                                                        "ok": true,
                                                        "response": final_text,
                                                        "iterations": summary.iterations,
                                                    })
                                                }
                                                Err(e) => {
                                                    let err_msg = e.to_string();
                                                    let sse_data = serde_json::json!({
                                                        "type": "TurnError",
                                                        "session_id": session_id,
                                                        "error": err_msg,
                                                    });
                                                    event_bus
                                                        .broadcast(session_id, &sse_data.to_string())
                                                        .await;
                                                    serde_json::json!({
                                                        "ok": false,
                                                        "error": err_msg,
                                                    })
                                                }
                                            }
                                        }
                                        None => {
                                            serde_json::json!({
                                                "ok": false,
                                                "error": format!("session {session_id} not found"),
                                            })
                                        }
                                    }
                                }
                            }
                            Some("list_sessions") => {
                                let ids = sessions.list();
                                serde_json::json!({ "ok": true, "sessions": ids })
                            }
                            Some("tool_approve") => {
                                let tool_id = cmd
                                    .get("id")
                                    .and_then(|i| i.as_str())
                                    .unwrap_or_default();
                                if let Some(prompter) = &prompter_ref {
                                    prompter.handle_response(tool_id, true);
                                }
                                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                                continue;
                            }
                            Some("tool_deny") => {
                                let tool_id = cmd
                                    .get("id")
                                    .and_then(|i| i.as_str())
                                    .unwrap_or_default();
                                if let Some(prompter) = &prompter_ref {
                                    prompter.handle_response(tool_id, false);
                                }
                                let _ = writer.write_all(b"{\"ok\":true}\n").await;
                                continue;
                            }
                            Some("chat_stream") => {
                                let session_id = cmd
                                    .get("session_id")
                                    .and_then(|s| s.as_str())
                                    .unwrap_or_default();
                                let content = cmd
                                    .get("content")
                                    .and_then(|c| c.as_str())
                                    .unwrap_or_default();

                                if session_id.is_empty() || content.is_empty() {
                                    let _ = writer.write_all(b"{\"error\":\"session_id and content are required\"}\n").await;
                                    continue;
                                }

                                let Some(entry) = sessions.get(session_id) else {
                                    let _ = writer.write_all(b"{\"error\":\"session not found\"}\n").await;
                                    continue;
                                };

                                // Extract CowdEventBus subscription before holding MutexGuard
                                let cowd_rx = {
                                    let guard = entry.lock().await;
                                    guard.cowd_bus().map(|b| b.subscribe())
                                }; // guard dropped — avoids !Send issue

                                // Execute turn in spawn_blocking (matches existing daemon.rs pattern)
                                let entry_clone = entry.clone();
                                let content_owned = content.to_string();

                                tokio::task::spawn_blocking(move || {
                                    let handle = tokio::runtime::Handle::current();
                                    handle.block_on(async move {
                                        let mut guard = entry_clone.lock().await;
                                        let _ = guard.run_turn_async(
                                            &content_owned,
                                            &runtime::permissions::SharedPrompter::none(),
                                        ).await;
                                    });
                                });

                                // Forward CowdEventBus events to socket
                                if let Some(mut rx) = cowd_rx {
                                    while let Ok(event) = rx.recv().await {
                                        let is_terminal = matches!(
                                            event,
                                            runtime::CowdEvent::TurnComplete { .. }
                                                | runtime::CowdEvent::TurnError { .. }
                                        );
                                        if let Ok(json_str) = serde_json::to_string(&event) {
                                            if writer.write_all(json_str.as_bytes()).await.is_err() {
                                                break;
                                            }
                                            if writer.write_all(b"\n").await.is_err() {
                                                break;
                                            }
                                        }
                                        if is_terminal {
                                            break;
                                        }
                                    }
                                }
                                continue;
                            }
                            Some("poll_events") => {
                                // Non-blocking: return empty events array immediately
                                // Real implementation would buffer events per session
                                let _ = writer.write_all(b"{\"events\":[]}\n").await;
                                continue;
                            }
                            Some(other) => {
                                serde_json::json!({
                                    "ok": false,
                                    "error": format!("unknown command: {other}"),
                                })
                            }
                            None => {
                                serde_json::json!({
                                    "ok": false,
                                    "error": "missing 'cmd' field",
                                })
                            }
                        }
                    }
                    Err(e) => {
                        serde_json::json!({
                            "ok": false,
                            "error": format!("invalid JSON: {e}"),
                        })
                    }
                };

                let mut resp_bytes = serde_json::to_vec(&response).unwrap_or_default();
                resp_bytes.push(b'\n');
                if let Err(e) = writer.write_all(&resp_bytes).await {
                    tracing::warn!("unix socket write error: {e}");
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("unix socket read error: {e}");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_daemon_tool_callback_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RuntimeHostToolCallback>();
    }

    #[test]
    fn test_daemon_hook_reporter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RuntimeHostHookReporter>();
    }
}
