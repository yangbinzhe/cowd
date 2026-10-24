use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use chrono::Utc;
use sandbox_launcher::{program_command, SandboxLaunchSpec};
use surface::{
    message::MessageActionKind, normalize_surface_id, SurfaceActionRequest, SurfaceDescriptor,
    SurfaceError, SurfaceFailureKind, SurfaceFrame, SurfaceLifecycle, SurfaceOperationResult,
    SurfaceRoute, SurfaceRuntimeSnapshot, SurfaceRuntimeStatus, SurfaceSendRequest,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use super::{classify_surface_error, managed_actions, normalize_request_path, SurfaceHost};

impl SurfaceHost {
    pub(crate) async fn send(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let source_session_id = request
            .metadata
            .get("source_session_id")
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
        let reply_to_message_id = request
            .metadata
            .get("local_reply_to")
            .or_else(|| request.metadata.get("reply_to"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);
        let delivery = self
            .messages
            .queue_outbox(&request, source_session_id, reply_to_message_id)
            .map_err(|error| SurfaceError::Invocation {
                surface: request.surface.clone(),
                reason: error,
            })?;
        if delivery.status == "sent" {
            return Ok(SurfaceOperationResult::ok(
                &delivery.surface,
                serde_json::json!({
                    "status": "sent",
                    "delivery_id": delivery.delivery_id,
                    "idempotent": true,
                }),
            ));
        }
        self.deliver_outbox(&delivery.delivery_id).await
    }

    pub(crate) async fn retry_outbox_delivery(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let delivery = self
            .messages
            .mark_delivery_replayed(delivery_id)
            .map_err(|error| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason: error,
            })?;
        self.deliver_outbox(&delivery.delivery_id).await
    }

    pub(crate) fn dead_letter_outbox_delivery(
        &self,
        delivery_id: &str,
        reason: impl Into<String>,
    ) -> Result<crate::surface_host::SurfaceOutboxRecord, String> {
        self.messages
            .mark_delivery_dead_letter(delivery_id, &reason.into())
    }

    async fn deliver_outbox(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let delivery = self
            .messages
            .get_outbox_by_delivery(delivery_id)
            .map_err(|reason| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason,
            })?
            .ok_or_else(|| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason: format!("surface delivery `{delivery_id}` not found"),
            })?;
        let request = serde_json::from_value::<SurfaceSendRequest>(delivery.request_json.clone())
            .map_err(|error| SurfaceError::Invocation {
            surface: delivery.surface.clone(),
            reason: format!("invalid surface outbox request: {error}"),
        })?;
        self.messages
            .mark_delivery_sending(delivery_id)
            .map_err(|error| SurfaceError::Invocation {
                surface: delivery.surface.clone(),
                reason: error,
            })?;
        match self.send_direct(request).await {
            Ok(result) if result.error.is_none() => {
                self.messages
                    .mark_delivery_sent(delivery_id, &result)
                    .map_err(|error| SurfaceError::Invocation {
                        surface: result.surface.clone(),
                        reason: error,
                    })?;
                Ok(result)
            }
            Ok(result) => {
                let message = result
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "surface send returned error".to_string());
                let retryable = result
                    .error
                    .as_ref()
                    .map(|error| is_retryable_surface_error(&error.code))
                    .unwrap_or(true);
                self.messages
                    .mark_delivery_failed(delivery_id, &message, retryable)
                    .map_err(|error| SurfaceError::Invocation {
                        surface: result.surface.clone(),
                        reason: error,
                    })?;
                Ok(result)
            }
            Err(error) => {
                let surface = delivery.surface.clone();
                let message = error.to_string();
                let _ = self
                    .messages
                    .mark_delivery_failed(delivery_id, &message, true);
                Err(SurfaceError::Invocation {
                    surface,
                    reason: message,
                })
            }
        }
    }

    async fn send_direct(
        &self,
        request: SurfaceSendRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if !surface.is_executable() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let mut metadata = request.metadata;
        if let Some(idempotency_key) = request.idempotency_key {
            metadata["idempotency_key"] = serde_json::Value::String(idempotency_key);
        }
        let frame = SurfaceFrame::Send {
            id: SurfaceFrame::new_id(),
            surface: surface_id.clone(),
            recipient: request.recipient,
            thread: request.thread,
            text: request.text,
            metadata,
        };
        self.invoke(surface, frame).await
    }

    pub(crate) async fn action(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        };
        if !surface.is_executable() {
            return Ok(SurfaceOperationResult::unavailable(&request.surface));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Action {
            id: SurfaceFrame::new_id(),
            surface: surface_id,
            action: request.action,
            payload: request.payload,
        };
        self.invoke(surface, frame).await
    }

    pub(crate) async fn action_stream(
        &self,
        request: SurfaceActionRequest,
    ) -> Result<mpsc::Receiver<Result<SurfaceOperationResult, SurfaceError>>, SurfaceError> {
        let Some(surface) = self.get(&request.surface) else {
            return Err(SurfaceError::Unavailable(request.surface));
        };
        if !surface.is_executable() {
            return Err(SurfaceError::Unavailable(surface.id));
        }
        let surface_id = normalize_surface_id(&request.surface);
        let frame = SurfaceFrame::Action {
            id: SurfaceFrame::new_id(),
            surface: surface_id,
            action: request.action,
            payload: request.payload,
        };
        if surface.lifecycle != SurfaceLifecycle::Managed {
            let result = self.invoke(surface, frame).await;
            let (tx, rx) = mpsc::channel(1);
            let _ = tx.send(result).await;
            return Ok(rx);
        }
        let (request_id, handshake) = operation_reply_identity(&surface.id, &frame)?;
        let process = self.managed_process(surface.clone()).await?;
        let mut frames = tokio::time::timeout(
            Duration::from_secs(30),
            process.client.invoke_stream(&frame),
        )
        .await
        .map_err(|_| SurfaceError::Invocation {
            surface: surface.id.clone(),
            reason: "managed surface stream start timed out".to_string(),
        })??;
        let (tx, rx) = mpsc::channel(8);
        let surface_id = surface.id.clone();
        self.gateway_tasks()
            .spawn_owned(
                crate::runtime_host::task_set::GatewayTaskKind::SurfaceStream,
                crate::runtime_host::task_set::GatewayTaskOwner::Surface(surface_id.clone()),
                move |cancellation| async move {
                    loop {
                        let frame = tokio::select! {
                            _ = cancellation.cancelled() => break,
                            frame = frames.recv() => frame,
                        };
                        let Some(frame) = frame else {
                            break;
                        };
                        let result = frame.map(|frame| {
                            operation_result_from_frame(&surface.id, &request_id, handshake, frame)
                        });
                        if tx.send(result).await.is_err() {
                            break;
                        }
                    }
                },
            )
            .map_err(|error| SurfaceError::Invocation {
                surface: surface_id,
                reason: format!("managed Surface action stream admission failed: {error}"),
            })?;
        Ok(rx)
    }

    pub(crate) async fn callback(
        &self,
        surface: &str,
        path: &str,
        method: &str,
        payload: serde_json::Value,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(descriptor) = self.get(surface) else {
            return Ok(SurfaceOperationResult::unavailable(surface));
        };
        if !descriptor
            .routes
            .iter()
            .any(|route| route_matches(route, path, method))
        {
            return Ok(SurfaceOperationResult::error(
                surface,
                "surface_route_not_found",
                format!(
                    "surface `{}` has no route for {method} {path}",
                    descriptor.id
                ),
            ));
        }
        self.action(SurfaceActionRequest {
            surface: descriptor.id,
            action: MessageActionKind::CallbackDispatch.as_str().to_string(),
            payload: serde_json::json!({
                "path": normalize_request_path(path),
                "method": method.to_ascii_uppercase(),
                "payload": payload,
            }),
        })
        .await
    }

    pub(crate) async fn check_surface_health(
        &self,
        surface: &str,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let Some(descriptor) = self.get(surface) else {
            return Ok(SurfaceOperationResult::unavailable(surface));
        };
        if !descriptor.is_executable() {
            self.set_runtime(SurfaceRuntimeSnapshot::builtin(&descriptor.id))
                .await;
            return Ok(SurfaceOperationResult::ok(
                &descriptor.id,
                serde_json::json!({
                    "status": "ready",
                    "kind": "builtin",
                    "route_count": descriptor.routes.len(),
                    "resource_count": descriptor.resources.len(),
                }),
            ));
        }
        let frame = SurfaceFrame::Health {
            id: SurfaceFrame::new_id(),
            surface: Some(descriptor.id.clone()),
        };
        let started = Instant::now();
        let timeout = Duration::from_millis(descriptor.health.timeout_ms.max(1));
        let result = tokio::time::timeout(timeout, self.invoke(descriptor.clone(), frame)).await;
        match result {
            Ok(Ok(result)) if result.error.is_none() => {
                let latency_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
                let mut snapshot = self.runtime_snapshot(&descriptor.id).unwrap_or_else(|| {
                    SurfaceRuntimeSnapshot::discovered(&descriptor.id, descriptor.lifecycle)
                });
                snapshot.status = SurfaceRuntimeStatus::Ready;
                snapshot.active = true;
                snapshot.last_seen_at = Some(Utc::now());
                snapshot.last_health_at = Some(Utc::now());
                snapshot.latency_ms = Some(latency_ms);
                snapshot.consecutive_failures = 0;
                snapshot.circuit_open = false;
                snapshot.next_retry_at = None;
                snapshot.last_error = None;
                snapshot.available_actions = managed_actions(false);
                self.set_runtime(snapshot).await;
                Ok(result)
            }
            Ok(Ok(result)) => {
                let message = result
                    .error
                    .as_ref()
                    .map(|error| error.message.clone())
                    .unwrap_or_else(|| "surface health returned error".to_string());
                self.record_surface_failure(
                    descriptor.clone(),
                    SurfaceFailureKind::ProtocolError,
                    message.clone(),
                )
                .await;
                Ok(result)
            }
            Ok(Err(error)) => {
                self.record_surface_failure(
                    descriptor.clone(),
                    classify_surface_error(&error),
                    error.to_string(),
                )
                .await;
                Err(error)
            }
            Err(_) => {
                let snapshot = self
                    .record_surface_failure(
                        descriptor.clone(),
                        SurfaceFailureKind::HealthTimeout,
                        format!("surface health timed out after {}ms", timeout.as_millis()),
                    )
                    .await;
                Ok(SurfaceOperationResult::error(
                    &snapshot.surface,
                    "surface_health_timeout",
                    "surface health check timed out",
                ))
            }
        }
    }

    pub(super) async fn invoke(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        if surface.lifecycle == SurfaceLifecycle::Managed {
            let surface_id = surface.id.clone();
            let (request_id, handshake) = operation_reply_identity(&surface_id, &frame)?;
            let response = match self.invoke_h2(surface.clone(), frame).await {
                Ok(response) => response,
                Err(error) => {
                    self.record_surface_failure(
                        surface,
                        classify_surface_error(&error),
                        error.to_string(),
                    )
                    .await;
                    return Err(error);
                }
            };
            return Ok(operation_result_from_frame(
                &surface_id,
                &request_id,
                handshake,
                response,
            ));
        }
        invoke_sidecar(surface, frame).await
    }

    pub(super) async fn invoke_h2(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceFrame, SurfaceError> {
        let process = self.managed_process(surface.clone()).await?;
        tokio::time::timeout(Duration::from_secs(30), process.client.invoke(&frame))
            .await
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id.clone(),
                reason: "managed surface request timed out".to_string(),
            })?
    }
}

fn is_retryable_surface_error(code: &str) -> bool {
    !matches!(
        code,
        "surface_unavailable" | "surface_route_not_found" | "surface_unsupported"
    )
}

async fn invoke_sidecar(
    surface: SurfaceDescriptor,
    frame: SurfaceFrame,
) -> Result<SurfaceOperationResult, SurfaceError> {
    let surface_id = surface.id.clone();
    let (request_id, handshake) = operation_reply_identity(&surface_id, &frame)?;
    let entry = surface
        .entry
        .clone()
        .ok_or_else(|| SurfaceError::Unavailable(surface_id.clone()))?;
    let manifest_path = PathBuf::from(&surface.source);
    let working_dir = manifest_path.parent().map(Path::to_path_buf);
    let mut command_path = PathBuf::from(entry);
    if command_path.is_relative() {
        if let Some(root) = &working_dir {
            command_path = root.join(command_path);
        }
    }

    let workspace_root = working_dir
        .as_deref()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "one-shot sidecar manifest has no parent directory".to_string(),
        })?;
    let mut sandbox = SandboxLaunchSpec::workspace(workspace_root);
    sandbox.working_directory = Some(workspace_root.to_path_buf());
    let prepared =
        program_command(&command_path, &sandbox).map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("sidecar sandbox unavailable: {error}"),
        })?;
    let mut command = tokio::process::Command::from(prepared.into_command());
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.kill_on_drop(true);
    let mut child = command.spawn().map_err(|error| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: format!("failed to launch `{}`: {error}", command_path.display()),
    })?;

    let mut stdin = child.stdin.take().ok_or_else(|| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "sidecar stdin is not available".to_string(),
    })?;
    let encoded = frame.encode_jsonl()?;
    tokio::time::timeout(Duration::from_secs(30), async {
        stdin.write_all(encoded.as_bytes()).await?;
        stdin.shutdown().await
    })
    .await
    .map_err(|_| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "one-shot sidecar stdin timed out after 30s".to_string(),
    })?
    .map_err(|error| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: format!("failed to write jsonl request: {error}"),
    })?;
    drop(stdin);

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "sidecar stdout is not available".to_string(),
        })?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: "sidecar stderr is not available".to_string(),
        })?;
    let execution = tokio::time::timeout(Duration::from_secs(30), async {
        let (status, stdout, stderr) = tokio::join!(
            child.wait(),
            drain_limited(stdout, 2 * 1024 * 1024),
            drain_limited(stderr, 1024 * 1024),
        );
        Ok::<_, std::io::Error>((status?, stdout?, stderr?))
    })
    .await;
    let (status, (stdout, stdout_truncated), (stderr, stderr_truncated)) = match execution {
        Ok(Ok(execution)) => execution,
        Ok(Err(error)) => {
            return Err(SurfaceError::Invocation {
                surface: surface_id,
                reason: format!("one-shot sidecar I/O failed: {error}"),
            })
        }
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(SurfaceError::Invocation {
                surface: surface_id,
                reason: "one-shot sidecar timed out after 30s".to_string(),
            });
        }
    };
    if stdout_truncated {
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "one-shot sidecar stdout exceeded 2 MiB".to_string(),
        });
    }
    if stderr_truncated {
        tracing::warn!(surface = %surface_id, "one-shot sidecar stderr exceeded 1 MiB and was truncated");
    }
    if !status.success() {
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: format!(
                "one-shot sidecar exited with {status}: {}",
                String::from_utf8_lossy(&stderr)
            ),
        });
    }
    let line = String::from_utf8(stdout)
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("sidecar stdout is not UTF-8: {error}"),
        })?
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_default()
        .to_string();
    if line.trim().is_empty() {
        let stderr = String::from_utf8_lossy(&stderr);
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: format!("sidecar returned no jsonl response: {stderr}"),
        });
    }

    let response = SurfaceFrame::decode_jsonl(&line)?;
    Ok(operation_result_from_frame(
        &surface_id,
        &request_id,
        handshake,
        response,
    ))
}

async fn drain_limited<R: AsyncRead + Unpin>(
    mut reader: R,
    limit: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut kept = Vec::with_capacity(limit.min(64 * 1024));
    let mut buffer = [0_u8; 16 * 1024];
    let mut truncated = false;
    loop {
        let read = reader.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        let remaining = limit.saturating_sub(kept.len());
        if remaining > 0 {
            kept.extend_from_slice(&buffer[..read.min(remaining)]);
        }
        if read > remaining {
            truncated = true;
        }
    }
    Ok((kept, truncated))
}

pub(super) fn operation_reply_identity(
    surface: &str,
    request: &SurfaceFrame,
) -> Result<(String, bool), SurfaceError> {
    match request {
        SurfaceFrame::Handshake { id, .. } => Ok((id.clone(), true)),
        SurfaceFrame::Configure { id, .. }
        | SurfaceFrame::Connect { id, .. }
        | SurfaceFrame::Disconnect { id, .. }
        | SurfaceFrame::Send { id, .. }
        | SurfaceFrame::Action { id, .. }
        | SurfaceFrame::Health { id, .. } => Ok((id.clone(), false)),
        _ => Err(SurfaceError::Invocation {
            surface: surface.to_owned(),
            reason: "surface operation requires a request frame".into(),
        }),
    }
}

pub(super) fn operation_result_from_frame(
    surface: &str,
    request_id: &str,
    handshake: bool,
    frame: SurfaceFrame,
) -> SurfaceOperationResult {
    match frame {
        SurfaceFrame::Ok { id, payload } if !handshake && id == request_id => {
            SurfaceOperationResult::ok(surface, payload)
        }
        SurfaceFrame::Error { id, code, message }
            if id.as_deref().is_none_or(|id| id == request_id) =>
        {
            SurfaceOperationResult::error(surface, code, message)
        }
        SurfaceFrame::HandshakeOk {
            id, capabilities, ..
        } if handshake && id == request_id => SurfaceOperationResult::ok(
            surface,
            serde_json::json!({
                "status": "ok", "capabilities": capabilities,
            }),
        ),
        other => SurfaceOperationResult::error(
            surface,
            "surface_unexpected_frame",
            format!("unexpected surface response for request {request_id}: {other:?}"),
        ),
    }
}

fn route_matches(route: &SurfaceRoute, path: &str, method: &str) -> bool {
    let route_path = normalize_request_path(&route.path);
    let request_path = normalize_request_path(path);
    route_path == request_path && route.method.eq_ignore_ascii_case(method)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_replies_require_the_original_kind_and_identity() {
        let request = SurfaceFrame::Send {
            id: "request-1".into(),
            surface: "fixture".into(),
            recipient: "target".into(),
            thread: None,
            text: "payload".into(),
            metadata: serde_json::Value::Null,
        };
        assert_eq!(
            operation_reply_identity("fixture", &request).unwrap(),
            ("request-1".into(), false)
        );
        for (frame, handshake, error) in [
            (
                SurfaceFrame::Ok {
                    id: "request-1".into(),
                    payload: serde_json::json!({"status":"sent"}),
                },
                false,
                None,
            ),
            (
                SurfaceFrame::Ok {
                    id: "foreign".into(),
                    payload: serde_json::json!({"status":"sent"}),
                },
                false,
                Some("surface_unexpected_frame"),
            ),
            (
                SurfaceFrame::HandshakeOk {
                    id: "request-1".into(),
                    surface_id: "fixture".into(),
                    capabilities: vec![],
                },
                false,
                Some("surface_unexpected_frame"),
            ),
            (
                SurfaceFrame::HandshakeOk {
                    id: "request-1".into(),
                    surface_id: "fixture".into(),
                    capabilities: vec![],
                },
                true,
                None,
            ),
            (
                SurfaceFrame::Ok {
                    id: "request-1".into(),
                    payload: serde_json::json!({}),
                },
                true,
                Some("surface_unexpected_frame"),
            ),
            (
                SurfaceFrame::Error {
                    id: None,
                    code: "busy".into(),
                    message: "failed".into(),
                },
                false,
                Some("busy"),
            ),
            (
                SurfaceFrame::Error {
                    id: Some("foreign".into()),
                    code: "busy".into(),
                    message: "failed".into(),
                },
                false,
                Some("surface_unexpected_frame"),
            ),
        ] {
            let result = operation_result_from_frame("fixture", "request-1", handshake, frame);
            assert_eq!(
                result.error.as_ref().map(|error| error.code.as_str()),
                error
            );
        }
        // A streamed action can have several correlated chunks.
        for index in 0..2 {
            let result = operation_result_from_frame(
                "fixture",
                "request-1",
                false,
                SurfaceFrame::Ok {
                    id: "request-1".into(),
                    payload: serde_json::json!({"chunk":index}),
                },
            );
            assert!(result.error.is_none());
            assert_eq!(result.payload.unwrap()["chunk"], index);
        }
    }

    fn send_request(mode: &str) -> SurfaceSendRequest {
        SurfaceSendRequest {
            surface: "reply-fixture".into(),
            recipient: "target".into(),
            thread: Some("thread".into()),
            text: "payload".into(),
            idempotency_key: Some(mode.into()),
            metadata: serde_json::json!({"reply_mode":mode}),
        }
    }

    fn assert_delivery(host: &SurfaceHost, mode: &str, result: SurfaceOperationResult) {
        let expected_error = match mode {
            "ok" => None,
            "unbound_error" => Some("busy"),
            _ => Some("surface_unexpected_frame"),
        };
        assert_eq!(
            result.error.as_ref().map(|error| error.code.as_str()),
            expected_error,
            "{mode}"
        );
        let records = host.outbox("reply-fixture").unwrap();
        let record = records
            .iter()
            .find(|record| record.request_json["metadata"]["reply_mode"] == mode)
            .unwrap();
        assert_eq!(record.status == "sent", mode == "ok", "{mode}");
        if mode == "ok" {
            assert_eq!(result.message_id.as_deref(), Some("original-message"));
        } else {
            assert!(record.last_error.is_some(), "{mode}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn sidecar_send_outbox_rejects_handshake_and_foreign_replies() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let directory = root.path().join("fixture");
        std::fs::create_dir(&directory).unwrap();
        let script = directory.join("reply-fixture");
        std::fs::write(&script, r#"#!/usr/bin/env python3
import json,sys
from pathlib import Path
r=json.loads(sys.stdin.readline())
assert r['type']=='send' and r['recipient']=='target' and r['text']=='payload'
Path(__file__).with_name('received.json').write_text(json.dumps(r))
mode=r['metadata']['reply_mode']
if mode=='handshake':
    reply={'type':'handshake-ok','id':r['id'],'surface_id':'reply-fixture','capabilities':[]}
elif mode=='unbound_error':
    reply={'type':'error','id':None,'code':'busy','message':'failed'}
else:
    reply={'type':'ok','id':r['id'] if mode=='ok' else 'foreign','payload':{'status':'sent','message_id':'original-message'}}
print(json.dumps(reply))
"#).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(directory.join(surface::SURFACE_MANIFEST_FILE), serde_json::to_vec(&serde_json::json!({
            "schema":"cowd.surface.v1", "id":"reply-fixture", "name":"Reply Fixture", "version":"1.0.0",
            "kind":"external-integration", "runtime":{"kind":"one-shot","entry":"reply-fixture","transport":"stdio-jsonl"},
            "capabilities":["send_text"], "default_enabled":true
        })).unwrap()).unwrap();
        let host = SurfaceHost::new(vec![root.path().into()]);
        assert_eq!(host.discover().discovered, 1);
        for mode in ["ok", "handshake", "foreign", "unbound_error"] {
            let result = host.send(send_request(mode)).await.unwrap();
            assert_delivery(&host, mode, result);
            let received: serde_json::Value =
                serde_json::from_slice(&std::fs::read(directory.join("received.json")).unwrap())
                    .unwrap();
            assert_eq!(received["metadata"]["reply_mode"], mode);
            assert_eq!(received["thread"], "thread");
        }
        assert_eq!(host.outbox("reply-fixture").unwrap().len(), 4);
        let report = host.gateway_tasks().shutdown().await;
        assert_eq!(report.forced_aborts, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn managed_send_outbox_rejects_handshake_and_foreign_replies() {
        use bytes::Bytes;
        use http_body_util::{BodyExt, Full};
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use std::convert::Infallible;
        use std::sync::Arc;
        let root = tempfile::tempdir().unwrap();
        let socket = root.path().join("s");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let received = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = received.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let service = hyper::service::service_fn(
                move |request: hyper::Request<hyper::body::Incoming>| {
                    let captured = captured.clone();
                    async move {
                        let bytes = request.into_body().collect().await.unwrap().to_bytes();
                        let frame: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                        captured.lock().unwrap().push(frame.clone());
                        if frame["type"] == "action" {
                            assert_eq!(frame["action"], "fixture.stream");
                            let id = if frame["payload"]["foreign"] == true {
                                serde_json::json!("foreign")
                            } else {
                                frame["id"].clone()
                            };
                            let mut body = Vec::new();
                            for index in 0..2 {
                                body.extend(
                                    serde_json::to_vec(&serde_json::json!({
                                        "type":"ok", "id":id, "payload":{"chunk":index}
                                    }))
                                    .unwrap(),
                                );
                                body.push(b'\n');
                            }
                            return Ok::<_, Infallible>(hyper::Response::new(Full::new(
                                Bytes::from(body),
                            )));
                        }
                        assert_eq!(frame["type"], "send");
                        assert_eq!(frame["recipient"], "target");
                        assert_eq!(frame["text"], "payload");
                        let reply = match frame["metadata"]["reply_mode"].as_str().unwrap() {
                            "handshake" => {
                                serde_json::json!({"type":"handshake-ok","id":frame["id"],"surface_id":"reply-fixture","capabilities":[]})
                            }
                            "unbound_error" => {
                                serde_json::json!({"type":"error","id":null,"code":"busy","message":"failed"})
                            }
                            mode => {
                                serde_json::json!({"type":"ok","id":if mode=="ok" {frame["id"].clone()} else {serde_json::json!("foreign")},"payload":{"status":"sent","message_id":"original-message"}})
                            }
                        };
                        Ok::<_, Infallible>(hyper::Response::new(Full::new(Bytes::from(
                            serde_json::to_vec(&reply).unwrap(),
                        ))))
                    }
                },
            );
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await
                .unwrap();
        });
        let host = SurfaceHost::new(Vec::new());
        let manifest: surface::SurfaceManifest = serde_json::from_value(serde_json::json!({
            "schema":"cowd.surface.v1", "id":"reply-fixture", "name":"Reply Fixture", "version":"1.0.0",
            "kind":"external-integration", "runtime":{"kind":"managed","artifact":"cowd-edge-fixture","driver_profile":"fixture","transport":"uds-http2"},
            "capabilities":["send_text"], "default_enabled":true
        })).unwrap();
        manifest.validate().unwrap();
        let descriptor = SurfaceDescriptor::from_manifest(&manifest, "local-contract-fixture");
        host.registry
            .write()
            .unwrap()
            .insert(descriptor.id.clone(), descriptor);
        let client = super::super::edge_h2::EdgeH2Client::connect(
            &socket,
            "reply-fixture",
            "fixture-token",
            host.gateway_tasks(),
        )
        .await
        .unwrap();
        // This tests an already-cached managed transport, not worker bootstrap.
        // The original cache carries an owned live child and the real H2 client.
        let mut command = tokio::process::Command::new("/usr/bin/cat");
        let child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let process = Arc::new(super::super::types::ManagedSurfaceProcess {
            pid: child.id(),
            started_at: Utc::now(),
            client,
            child: Arc::new(tokio::sync::Mutex::new(child)),
            events: Arc::new(tokio::sync::Mutex::new(std::collections::VecDeque::new())),
            runtime_dir: root.path().into(),
        });
        assert!(process.child.lock().await.try_wait().unwrap().is_none());
        host.managed
            .lock()
            .await
            .insert("reply-fixture".into(), process.clone());
        for mode in ["ok", "handshake", "foreign", "unbound_error"] {
            assert_delivery(&host, mode, host.send(send_request(mode)).await.unwrap());
        }
        assert_eq!(received.lock().unwrap().len(), 4);
        for foreign in [false, true] {
            let mut chunks = host
                .action_stream(SurfaceActionRequest {
                    surface: "reply-fixture".into(),
                    action: "fixture.stream".into(),
                    payload: serde_json::json!({"foreign":foreign}),
                })
                .await
                .unwrap();
            for index in 0..2 {
                let result = tokio::time::timeout(Duration::from_secs(2), chunks.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if foreign {
                    assert_eq!(result.error.unwrap().code, "surface_unexpected_frame");
                } else {
                    assert!(result.error.is_none());
                    assert_eq!(result.payload.unwrap()["chunk"], index);
                }
            }
            assert!(tokio::time::timeout(Duration::from_secs(2), chunks.recv())
                .await
                .unwrap()
                .is_none());
        }
        assert_eq!(received.lock().unwrap().len(), 6);
        let mut child = process.child.lock().await;
        child.kill().await.unwrap();
        assert!(child.wait().await.unwrap().code().is_none());
        drop(child);
        host.managed.lock().await.remove("reply-fixture");
        drop(process);
        let report = host.gateway_tasks().shutdown().await;
        assert_eq!(report.forced_aborts, 0);
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn send_uses_local_reply_to_for_gateway_ledger_when_present() {
        let host = SurfaceHost::baseline().unwrap();
        let _ = host
            .send(SurfaceSendRequest {
                surface: "feishu".to_string(),
                recipient: "chat-a".to_string(),
                thread: Some("thread-a".to_string()),
                text: "hello".to_string(),
                idempotency_key: None,
                metadata: serde_json::json!({
                    "reply_to": "om_external",
                    "local_reply_to": "om_external:replay:local",
                }),
            })
            .await;

        let outbox = host.outbox("feishu").unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(
            outbox[0].reply_to_message_id.as_deref(),
            Some("om_external:replay:local")
        );
        assert_eq!(
            outbox[0].request_json["metadata"]["reply_to"],
            serde_json::json!("om_external")
        );
    }
}
