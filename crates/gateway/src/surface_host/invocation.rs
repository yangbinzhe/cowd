use std::io::{BufRead, Write};
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
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;

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
        self.messages.mark_delivery_dead_letter(delivery_id, reason)
    }

    async fn deliver_outbox(
        &self,
        delivery_id: &str,
    ) -> Result<SurfaceOperationResult, SurfaceError> {
        let delivery = self
            .messages
            .get_outbox_by_delivery(delivery_id)
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
                    .mark_delivery_failed(delivery_id, message, retryable)
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
                    .mark_delivery_failed(delivery_id, message.clone(), true);
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
        if surface.entry.is_none() {
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
        if surface.entry.is_none() {
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
        if descriptor.entry.is_none() {
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
            let response = match self.invoke_managed(surface.clone(), frame).await {
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
            return Ok(operation_result_from_frame(&surface_id, response));
        }
        tokio::task::spawn_blocking(move || invoke_sidecar(surface, frame))
            .await
            .map_err(|error| SurfaceError::Invocation {
                surface: "unknown".to_string(),
                reason: format!("surface task join failed: {error}"),
            })?
    }

    pub(super) async fn invoke_managed(
        &self,
        surface: SurfaceDescriptor,
        frame: SurfaceFrame,
    ) -> Result<SurfaceFrame, SurfaceError> {
        let request_id = frame_id(&frame).ok_or_else(|| SurfaceError::Invocation {
            surface: surface.id.clone(),
            reason: "managed surface request frame missing id".to_string(),
        })?;
        let process = self.managed_process(surface.clone()).await?;
        let (sender, receiver) = oneshot::channel();
        process
            .pending
            .lock()
            .await
            .insert(request_id.clone(), sender);
        let encoded = frame.encode_jsonl()?;
        let write_result: Result<(), std::io::Error> = {
            let mut stdin = process.stdin.lock().await;
            if let Err(error) = stdin.write_all(encoded.as_bytes()).await {
                Err(error)
            } else {
                stdin.flush().await
            }
        };
        if let Err(error) = write_result {
            process.pending.lock().await.remove(&request_id);
            return Err(SurfaceError::Invocation {
                surface: surface.id,
                reason: format!("failed to write managed jsonl request: {error}"),
            });
        }
        tokio::time::timeout(Duration::from_secs(30), receiver)
            .await
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id.clone(),
                reason: "managed surface request timed out".to_string(),
            })?
            .map_err(|_| SurfaceError::Invocation {
                surface: surface.id,
                reason: "managed surface response channel closed".to_string(),
            })
    }
}

fn is_retryable_surface_error(code: &str) -> bool {
    !matches!(
        code,
        "surface_unavailable" | "surface_route_not_found" | "surface_unsupported"
    )
}

fn invoke_sidecar(
    surface: SurfaceDescriptor,
    frame: SurfaceFrame,
) -> Result<SurfaceOperationResult, SurfaceError> {
    let surface_id = surface.id.clone();
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
    let mut child = prepared
        .into_command()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to launch `{}`: {error}", command_path.display()),
        })?;

    let mut stdin = child.stdin.take().ok_or_else(|| SurfaceError::Invocation {
        surface: surface_id.clone(),
        reason: "sidecar stdin is not available".to_string(),
    })?;
    let encoded = frame.encode_jsonl()?;
    stdin
        .write_all(encoded.as_bytes())
        .and_then(|_| stdin.flush())
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
    let mut reader = std::io::BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| SurfaceError::Invocation {
            surface: surface_id.clone(),
            reason: format!("failed to read jsonl response: {error}"),
        })?;
    if line.trim().is_empty() {
        return Err(SurfaceError::Invocation {
            surface: surface_id,
            reason: "sidecar returned no jsonl response".to_string(),
        });
    }

    let response = SurfaceFrame::decode_jsonl(&line)?;
    let _ = child.wait();
    Ok(operation_result_from_frame(&surface_id, response))
}

fn operation_result_from_frame(surface: &str, frame: SurfaceFrame) -> SurfaceOperationResult {
    match frame {
        SurfaceFrame::Ok { payload, .. } => SurfaceOperationResult::ok(surface, payload),
        SurfaceFrame::Error { code, message, .. } => {
            SurfaceOperationResult::error(surface, code, message)
        }
        SurfaceFrame::HandshakeOk { capabilities, .. } => SurfaceOperationResult::ok(
            surface,
            serde_json::json!({
                "status": "ok",
                "capabilities": capabilities,
            }),
        ),
        other => SurfaceOperationResult::error(
            surface,
            "surface_unexpected_frame",
            format!("unexpected surface response frame: {other:?}"),
        ),
    }
}

pub(super) fn frame_id(frame: &SurfaceFrame) -> Option<String> {
    match frame {
        SurfaceFrame::Handshake { id, .. }
        | SurfaceFrame::HandshakeOk { id, .. }
        | SurfaceFrame::Configure { id, .. }
        | SurfaceFrame::Connect { id, .. }
        | SurfaceFrame::Disconnect { id, .. }
        | SurfaceFrame::Send { id, .. }
        | SurfaceFrame::Action { id, .. }
        | SurfaceFrame::Health { id, .. }
        | SurfaceFrame::Ok { id, .. } => Some(id.clone()),
        SurfaceFrame::Error { id, .. } => id.clone(),
        SurfaceFrame::Event { .. } => None,
    }
}

fn route_matches(route: &SurfaceRoute, path: &str, method: &str) -> bool {
    let route_path = normalize_request_path(&route.path);
    let request_path = normalize_request_path(path);
    route_path == request_path && route.method.eq_ignore_ascii_case(method)
}

#[cfg(test)]
mod tests {
    use surface::SurfaceSendRequest;

    use super::SurfaceHost;

    #[tokio::test]
    async fn send_uses_local_reply_to_for_gateway_ledger_when_present() {
        let host = SurfaceHost::default();
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

        let outbox = host.outbox("feishu");
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
