use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use memory::{OutboxFailureClass, SessionMissionOutboxOperation, UnifiedSessionStore};
use tokio::{sync::watch, task::JoinHandle};

use crate::{event_bus::SessionEventBus, runtime_service::RuntimeService};

const WORKER_BATCH: usize = 32;
const LEASE_MS: u64 = 30_000;
const MAX_ATTEMPTS: u32 = 8;

#[async_trait]
impl runtime::SessionIngressExecutor for RuntimeService {
    async fn execute_ingress(
        &self,
        record: &memory::SessionRuntimeOutboxRecord,
        content: &str,
    ) -> Result<runtime::SessionIngressExecutionReceipt, String> {
        self.execute_ingress_record(record, content).await
    }
}

pub(crate) struct SessionRuntimeBridge {
    shutdown: watch::Sender<bool>,
    handles: Vec<JoinHandle<()>>,
}

impl SessionRuntimeBridge {
    pub(crate) fn start(
        runtime_service: Arc<RuntimeService>,
        store: Arc<UnifiedSessionStore>,
        event_bus: Arc<SessionEventBus>,
    ) -> Result<Self, String> {
        let router = runtime_service.session_input_router();
        let (shutdown, ingress_rx) = watch::channel(false);
        let delivery_rx = shutdown.subscribe();
        let mission_rx = shutdown.subscribe();
        let ingress_runtime = Arc::clone(&runtime_service);
        let ingress = tokio::spawn(async move {
            run_ingress_worker(router, ingress_runtime, ingress_rx).await;
        });
        let delivery_runtime = Arc::clone(&runtime_service);
        let delivery_store = delivery_runtime
            .runtime_services()
            .session_terminal_delivery();
        let delivery = tokio::spawn(async move {
            run_delivery_worker(delivery_store, store, event_bus, delivery_rx).await;
        });
        let mission_store = runtime_service
            .session_kernel()
            .unified_store()
            .ok_or_else(|| "mission bridge requires UnifiedSessionStore".to_string())?;
        let mission_runtime = Arc::clone(runtime_service.runtime_services().mission_runtime());
        let workspace_key = runtime_service
            .runtime_services()
            .workspace_key()
            .to_string();
        let mission = tokio::spawn(async move {
            run_mission_membership_worker(
                mission_store,
                mission_runtime,
                workspace_key,
                mission_rx,
            )
            .await;
        });
        Ok(Self {
            shutdown,
            handles: vec![ingress, delivery, mission],
        })
    }

    pub(crate) async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        for handle in self.handles {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }
    }
}

async fn run_mission_membership_worker(
    store: Arc<UnifiedSessionStore>,
    mission: Arc<runtime::MissionRuntime>,
    workspace_key: String,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("gateway-mission-membership:{}", uuid::Uuid::new_v4());
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claimed = store
            .claim_session_mission_outbox(
                &workspace_key,
                &worker_id,
                now_ms(),
                LEASE_MS,
                WORKER_BATCH,
            )
            .await;
        match claimed {
            Ok(records) => {
                for record in records {
                    materialize_mission_membership(&store, &mission, &worker_id, record).await;
                }
            }
            Err(error) => {
                tracing::error!(%error, workspace_key, "mission membership outbox claim failed")
            }
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn materialize_mission_membership(
    store: &UnifiedSessionStore,
    mission: &runtime::MissionRuntime,
    worker_id: &str,
    record: memory::SessionMissionOutboxRecord,
) {
    let outcome = match record.operation {
        SessionMissionOutboxOperation::Register => mission
            .register_session(runtime::StartMissionSessionRequest {
                title: record.title.clone(),
                session_id: Some(record.session_id.clone()),
            })
            .map(|_| ()),
        SessionMissionOutboxOperation::Start => mission
            .start_session(runtime::StartMissionSessionRequest {
                title: record.title.clone(),
                session_id: Some(record.session_id.clone()),
            })
            .map(|_| ()),
        SessionMissionOutboxOperation::Close => {
            if mission.get_session(&record.session_id).is_some() {
                mission.close_session(&record.session_id).map(|_| ())
            } else {
                // A close may race a never-materialized register. There is no
                // aggregate state to mutate, so the requested final state is
                // already satisfied and must not poison the outbox.
                Ok(())
            }
        }
    };
    match outcome {
        Ok(()) => {
            if let Err(error) = store
                .ack_session_mission_outbox(
                    &record.request_id,
                    worker_id,
                    record.revision,
                    now_ms(),
                )
                .await
            {
                tracing::error!(request_id = %record.request_id, %error, "mission lifecycle applied but outbox acknowledgement failed");
            }
        }
        Err(error) => {
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            if let Err(failure) = store
                .fail_session_mission_outbox(
                    &record.request_id,
                    worker_id,
                    record.revision,
                    OutboxFailureClass::Retryable,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
                .await
            {
                tracing::error!(request_id = %record.request_id, error = %failure, "mission lifecycle failure state could not be recorded");
            }
        }
    }
}

async fn run_ingress_worker(
    router: Arc<runtime::SessionInputRouter>,
    runtime_service: Arc<RuntimeService>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        if *shutdown.borrow() {
            break;
        }
        match router
            .route_pending_with(runtime_service.as_ref(), WORKER_BATCH)
            .await
        {
            Ok(report) if report.claimed > 0 => tracing::debug!(
                claimed = report.claimed,
                materialized = report.materialized,
                retries = report.retry_scheduled,
                blocked = report.blocked,
                "session ingress batch processed"
            ),
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "session ingress worker failed"),
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn run_delivery_worker(
    event_store: runtime::SessionTerminalDeliveryPort,
    store: Arc<UnifiedSessionStore>,
    event_bus: Arc<SessionEventBus>,
    mut shutdown: watch::Receiver<bool>,
) {
    let worker_id = format!("gateway-delivery:{}", uuid::Uuid::new_v4());
    loop {
        if *shutdown.borrow() {
            break;
        }
        let claim_store = event_store.clone();
        let claim_worker = worker_id.clone();
        let claimed = tokio::task::spawn_blocking(move || {
            claim_store.claim(&claim_worker, now_ms(), LEASE_MS, WORKER_BATCH)
        })
        .await;
        match claimed {
            Ok(Ok(records)) => {
                for record in records {
                    deliver_terminal(&event_store, &store, &event_bus, &worker_id, record).await;
                }
            }
            Ok(Err(error)) => tracing::error!(%error, "terminal outbox claim failed"),
            Err(error) => tracing::error!(%error, "terminal outbox worker join failed"),
        }
        tokio::select! {
            _ = shutdown.changed() => {},
            _ = tokio::time::sleep(Duration::from_millis(100)) => {},
        }
    }
}

async fn deliver_terminal(
    event_store: &runtime::SessionTerminalDeliveryPort,
    store: &UnifiedSessionStore,
    event_bus: &SessionEventBus,
    worker_id: &str,
    record: runtime::RuntimeSessionOutboxRecord,
) {
    let outcome = match decode_terminal_payload(&record.payload_ref) {
        Ok(payload) => {
            let mut transcript = payload.transcript.unwrap_or_else(|| {
                vec![DecodedTerminalTranscriptMessage {
                    role: "assistant".to_string(),
                    content_json: serde_json::json!([
                        { "type": "text", "text": payload.text.clone() }
                    ])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: payload.token_usage_json.clone(),
                }]
            });
            annotate_terminal_tool_instances(
                &mut transcript,
                record.execution_id.as_deref(),
                record.turn_id.as_deref(),
                payload.ingress_message_id.as_deref(),
            );
            let transcript_len = transcript.len();
            let messages = transcript
                .into_iter()
                .enumerate()
                .map(|(index, message)| memory::SessionMessage {
                    stable_message_id: if index + 1 == transcript_len {
                        record.message_id.clone()
                    } else {
                        format!("{}:transcript:{index}", record.message_id)
                    },
                    session_id: record.session_id.clone(),
                    sequence: index,
                    role: message.role,
                    content_json: message.content_json,
                    blocks_count: message.blocks_count,
                    tool_use_id: message.tool_use_id,
                    tool_name: message.tool_name,
                    token_usage_json: message.token_usage_json,
                    created_at_ms: 0,
                })
                .collect::<Vec<_>>();
            let write = if let Some(ingress_message_id) = payload.ingress_message_id.as_deref() {
                store
                    .append_terminal_transcript_idempotent(
                        &record.message_id,
                        ingress_message_id,
                        &record.session_id,
                        &messages,
                        now_ms(),
                    )
                    .await
            } else {
                let terminal = messages.last().expect("legacy transcript has one row");
                store
                    .append_terminal_message_idempotent(
                        &record.message_id,
                        &record.session_id,
                        &terminal.content_json,
                        terminal.token_usage_json.as_deref(),
                        now_ms(),
                    )
                    .await
                    .map(|(terminal, inserted)| (vec![terminal], inserted))
            };
            match write {
                Ok((messages, inserted)) => {
                    let terminal = messages.last().cloned().ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript committed no terminal row".to_string(),
                        )
                    });
                    terminal.map(|terminal| {
                        (payload.text, payload.token_usage_json, terminal, inserted)
                    })
                }
                Err(error) => Err((
                    runtime::RuntimeSessionOutboxFailureClass::Permanent,
                    error.to_string(),
                )),
            }
        }
        Err(error) => Err(error),
    };
    match outcome {
        Ok((text, token_usage_json, message, inserted)) => {
            // The message write is exactly-once; delivery notification is
            // intentionally at-least-once. A process can die after commit but
            // before broadcast, so suppressing a duplicate notification would
            // leave live Surfaces permanently waiting. Stable terminal/message
            // identities make replay harmless and let each Surface dedupe.
            let mut event = serde_json::json!({
                "type": "TerminalCommitted",
                "session_id": record.session_id,
                "terminal_id": record.terminal_id,
                "message_id": record.message_id,
                "part_id": "assistant_text",
                "sequence": message.sequence,
                "response": text,
                "runtime_commit_cursor": record.commit_cursor,
                "replayed": !inserted,
            });
            if let Some(object) = event.as_object_mut() {
                if let Some(usage) = token_usage_json
                    .as_deref()
                    .and_then(|usage| serde_json::from_str(usage).ok())
                {
                    object.insert("token_usage".to_string(), usage);
                }
                if let Some(execution_id) = &record.execution_id {
                    object.insert(
                        "execution_id".to_string(),
                        serde_json::Value::String(execution_id.clone()),
                    );
                }
                if let Some(turn_id) = &record.turn_id {
                    object.insert(
                        "turn_id".to_string(),
                        serde_json::Value::String(turn_id.clone()),
                    );
                }
            }
            event_bus
                .broadcast(&record.session_id, &event.to_string())
                .await;
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let acknowledgement = tokio::task::spawn_blocking(move || {
                event_store.acknowledge(&terminal_id, &worker, revision, now_ms())
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(error) = acknowledgement {
                // The durable message ID makes replay safe. Leaving the lease
                // unacked intentionally lets the next worker take it over.
                tracing::error!(terminal_id = %record.terminal_id, %error, "terminal append committed but ack failed");
            }
        }
        Err((class, error)) => {
            let event_store = event_store.clone();
            let terminal_id = record.terminal_id.clone();
            let worker = worker_id.to_string();
            let revision = record.revision;
            let retry_at = now_ms().saturating_add(retry_delay_ms(record.attempts));
            let failure_record = tokio::task::spawn_blocking(move || {
                event_store.fail(
                    &terminal_id,
                    &worker,
                    revision,
                    class,
                    &error,
                    retry_at,
                    MAX_ATTEMPTS,
                    now_ms(),
                )
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result.map_err(|error| error.to_string()));
            if let Err(failure) = failure_record {
                tracing::error!(terminal_id = %record.terminal_id, error = %failure, "terminal failure state could not be recorded");
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalPayload {
    pub(crate) text: String,
    pub(crate) token_usage_json: Option<String>,
    pub(crate) ingress_message_id: Option<String>,
    pub(crate) transcript: Option<Vec<DecodedTerminalTranscriptMessage>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodedTerminalTranscriptMessage {
    pub(crate) role: String,
    pub(crate) content_json: String,
    pub(crate) blocks_count: usize,
    pub(crate) tool_use_id: Option<String>,
    pub(crate) tool_name: Option<String>,
    pub(crate) token_usage_json: Option<String>,
}

fn annotate_terminal_tool_instances(
    transcript: &mut [DecodedTerminalTranscriptMessage],
    execution_id: Option<&str>,
    turn_id: Option<&str>,
    ingress_message_id: Option<&str>,
) {
    let mut ordinals = HashMap::<String, u64>::new();
    let mut pending = HashMap::<String, VecDeque<String>>::new();
    for message in transcript {
        let Ok(mut blocks) = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        else {
            continue;
        };
        for block in &mut blocks {
            let Some(object) = block.as_object_mut() else {
                continue;
            };
            if let Some(turn_id) = turn_id {
                object.insert(
                    "cowd_turn_id".to_string(),
                    serde_json::Value::String(turn_id.to_string()),
                );
            }
            if let Some(ingress_message_id) = ingress_message_id {
                object.insert(
                    "cowd_turn_ingress_message_id".to_string(),
                    serde_json::Value::String(ingress_message_id.to_string()),
                );
            }
            if let Some(execution_id) = execution_id {
                object.insert(
                    "cowd_execution_id".to_string(),
                    serde_json::Value::String(execution_id.to_string()),
                );
            }
            let (provider_id, is_use) = match object.get("type").and_then(serde_json::Value::as_str)
            {
                Some("tool_use") => (
                    object
                        .get("id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    true,
                ),
                Some("tool_result") => (
                    object
                        .get("tool_use_id")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                    false,
                ),
                _ => (None, false),
            };
            let Some(provider_id) = provider_id else {
                continue;
            };
            let instance_id = if is_use {
                let ordinal = ordinals.entry(provider_id.clone()).or_default();
                let instance_id = format!("{provider_id}#cowd-{ordinal}");
                *ordinal = ordinal.saturating_add(1);
                pending
                    .entry(provider_id)
                    .or_default()
                    .push_back(instance_id.clone());
                instance_id
            } else {
                pending
                    .entry(provider_id.clone())
                    .or_default()
                    .pop_front()
                    .unwrap_or_else(|| {
                        let ordinal = ordinals.entry(provider_id.clone()).or_default();
                        let instance_id = format!("{provider_id}#cowd-{ordinal}");
                        *ordinal = ordinal.saturating_add(1);
                        instance_id
                    })
            };
            object.insert(
                "cowd_tool_instance_id".to_string(),
                serde_json::Value::String(instance_id),
            );
        }
        if let Ok(content_json) = serde_json::to_string(&blocks) {
            message.content_json = content_json;
        }
    }
}

pub(crate) fn decode_terminal_payload(
    payload_ref: &str,
) -> Result<DecodedTerminalPayload, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    if let Some(encoded) = payload_ref
        .strip_prefix("assistant_terminal_v2:")
        .or_else(|| payload_ref.strip_prefix("assistant_terminal_v1:"))
    {
        let is_v2 = payload_ref.starts_with("assistant_terminal_v2:");
        let payload = serde_json::from_str::<serde_json::Value>(encoded).map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                error.to_string(),
            )
        })?;
        let text = payload
            .get("text")
            .and_then(serde_json::Value::as_str)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal payload has no visible text".to_string(),
                )
            })?
            .to_string();
        let token_usage_json = decode_terminal_usage(payload.get("token_usage"), is_v2)?;
        let ingress_message_id = if is_v2 {
            Some(
                payload
                    .get("ingress_message_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|message_id| !message_id.trim().is_empty())
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript requires ingress_message_id".to_string(),
                        )
                    })?
                    .to_string(),
            )
        } else {
            None
        };
        let transcript = if is_v2 {
            let messages = payload
                .get("transcript")
                .and_then(serde_json::Value::as_array)
                .filter(|messages| !messages.is_empty() && messages.len() <= 10_000)
                .ok_or_else(|| {
                    (
                        runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                        "terminal transcript must contain 1..=10000 messages".to_string(),
                    )
                })?;
            let mut decoded = Vec::with_capacity(messages.len());
            for message in messages {
                let object = message.as_object().ok_or_else(|| {
                    (
                        runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                        "terminal transcript message must be an object".to_string(),
                    )
                })?;
                let role = object
                    .get("role")
                    .and_then(serde_json::Value::as_str)
                    .filter(|role| matches!(*role, "system" | "user" | "assistant" | "tool"))
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript message has an invalid role".to_string(),
                        )
                    })?
                    .to_string();
                let blocks = object
                    .get("blocks")
                    .and_then(serde_json::Value::as_array)
                    .filter(|blocks| !blocks.is_empty())
                    .ok_or_else(|| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            "terminal transcript message must contain blocks".to_string(),
                        )
                    })?;
                let (tool_use_id, tool_name) = blocks
                    .iter()
                    .find_map(|block| {
                        let block = block.as_object()?;
                        match block.get("type").and_then(serde_json::Value::as_str)? {
                            "tool_use" => Some((
                                block
                                    .get("id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                                block
                                    .get("name")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                            )),
                            "tool_result" => Some((
                                block
                                    .get("tool_use_id")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                                block
                                    .get("tool_name")
                                    .and_then(serde_json::Value::as_str)
                                    .map(ToOwned::to_owned),
                            )),
                            _ => None,
                        }
                    })
                    .unwrap_or((None, None));
                decoded.push(DecodedTerminalTranscriptMessage {
                    role,
                    content_json: serde_json::to_string(blocks).map_err(|error| {
                        (
                            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                            error.to_string(),
                        )
                    })?,
                    blocks_count: blocks.len(),
                    tool_use_id,
                    tool_name,
                    token_usage_json: decode_terminal_usage(object.get("usage"), false)?,
                });
            }
            let terminal = decoded.last_mut().ok_or_else(|| {
                (
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript has no final message".to_string(),
                )
            })?;
            let terminal_has_text = terminal.role == "assistant"
                && serde_json::from_str::<serde_json::Value>(&terminal.content_json)
                    .ok()
                    .and_then(|value| value.as_array().cloned())
                    .is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block.get("type").and_then(serde_json::Value::as_str) == Some("text")
                                && block.get("text").and_then(serde_json::Value::as_str)
                                    == Some(text.as_str())
                        })
                    });
            if !terminal_has_text {
                return Err((
                    runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                    "terminal transcript final assistant row does not contain terminal text"
                        .to_string(),
                ));
            }
            if terminal.token_usage_json.is_none() {
                terminal.token_usage_json = token_usage_json.clone();
            }
            Some(decoded)
        } else {
            None
        };
        return Ok(DecodedTerminalPayload {
            text,
            token_usage_json,
            ingress_message_id,
            transcript,
        });
    }
    let encoded = payload_ref.strip_prefix("assistant_json:").ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal payload does not use a supported typed schema".to_string(),
        )
    })?;
    serde_json::from_str::<String>(encoded)
        .map(|text| DecodedTerminalPayload {
            text,
            token_usage_json: None,
            ingress_message_id: None,
            transcript: None,
        })
        .map_err(|error| {
            (
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                error.to_string(),
            )
        })
}

fn decode_terminal_usage(
    usage: Option<&serde_json::Value>,
    required_core_fields: bool,
) -> Result<Option<String>, (runtime::RuntimeSessionOutboxFailureClass, String)> {
    let Some(usage) = usage else {
        return if required_core_fields {
            Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "terminal token_usage is required".to_string(),
            ))
        } else {
            Ok(None)
        };
    };
    let usage = usage.as_object().ok_or_else(|| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            "terminal token_usage must be an object".to_string(),
        )
    })?;
    for field in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let value = usage.get(field);
        if required_core_fields
            && matches!(field, "input_tokens" | "output_tokens")
            && value.is_none()
        {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!("terminal token_usage.{field} is required"),
            ));
        }
        if value.is_some_and(|value| value.as_u64().is_none_or(|value| value > i64::MAX as u64)) {
            return Err((
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                format!(
                    "terminal token_usage.{field} must be a non-negative 64-bit database integer"
                ),
            ));
        }
    }
    serde_json::to_string(usage).map(Some).map_err(|error| {
        (
            runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
            error.to_string(),
        )
    })
}

fn retry_delay_ms(attempt: u32) -> u64 {
    250_u64.saturating_mul(1_u64 << attempt.min(8))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::{SessionMissionOutboxOperation, SessionMissionOutboxRequest, SessionRecord};
    use tokio::sync::mpsc;

    async fn delivery_fixture() -> (
        runtime::SessionTerminalDeliveryPort,
        Arc<UnifiedSessionStore>,
        Arc<SessionEventBus>,
        mpsc::Receiver<String>,
    ) {
        let event_store = runtime::RuntimeServices::in_memory()
            .unwrap()
            .session_terminal_delivery();
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        store
            .create_session(&SessionRecord {
                session_id: "s1".to_string(),
                platform: "test".to_string(),
                chat_id: "chat".to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now,
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                estimated_cost_usd: 0.0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        let event_bus = SessionEventBus::new();
        let (tx, rx) = mpsc::channel(8);
        event_bus.subscribe("s1", tx).await;
        (event_store, store, event_bus, rx)
    }

    #[test]
    fn terminal_payload_requires_typed_prefix() {
        assert_eq!(
            decode_terminal_payload("assistant_json:\"done\"")
                .unwrap()
                .text,
            "done"
        );
        let payload = decode_terminal_payload(
            r#"assistant_terminal_v1:{"text":"done","token_usage":{"input_tokens":12,"output_tokens":3}}"#,
        )
        .unwrap();
        assert_eq!(payload.text, "done");
        assert!(payload
            .token_usage_json
            .as_deref()
            .is_some_and(|usage| usage.contains("\"input_tokens\":12")));
        assert!(decode_terminal_payload(
            r#"assistant_terminal_v1:{"text":"done","token_usage":{"input_tokens":"12","output_tokens":3}}"#
        )
        .is_err());
        assert!(decode_terminal_payload("evidence:1").is_err());
    }

    #[test]
    fn terminal_annotation_preserves_causality_on_non_tool_blocks() {
        let mut transcript = vec![DecodedTerminalTranscriptMessage {
            role: "assistant".to_string(),
            content_json: serde_json::json!([
                {"type": "thinking", "thinking": "reason"},
                {"type": "text", "text": "done"}
            ])
            .to_string(),
            blocks_count: 2,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: None,
        }];

        annotate_terminal_tool_instances(
            &mut transcript,
            Some("execution-1"),
            Some("turn-1"),
            Some("ingress-1"),
        );

        let blocks =
            serde_json::from_str::<Vec<serde_json::Value>>(transcript[0].content_json.as_str())
                .unwrap();
        assert_eq!(blocks.len(), 2);
        for block in blocks {
            assert_eq!(
                block
                    .get("cowd_execution_id")
                    .and_then(serde_json::Value::as_str),
                Some("execution-1")
            );
            assert_eq!(
                block
                    .get("cowd_turn_id")
                    .and_then(serde_json::Value::as_str),
                Some("turn-1")
            );
            assert_eq!(
                block
                    .get("cowd_turn_ingress_message_id")
                    .and_then(serde_json::Value::as_str),
                Some("ingress-1")
            );
        }
    }

    #[tokio::test]
    async fn mission_membership_bridge_replays_registration_once() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-session".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-register-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Mission session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let claimed = store
            .claim_session_mission_outbox("workspace-a", "worker", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let mission = Arc::new(
            runtime::MissionRuntime::event_sourced(
                Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap()),
                "workspace-a",
            )
            .unwrap(),
        );

        materialize_mission_membership(&store, &mission, "worker", claimed).await;

        assert!(mission.get_session("mission-session").is_some());
        assert_eq!(
            store
                .get_session_mission_outbox("mission-register-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            memory::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn mission_membership_replay_after_lost_ack_is_idempotent() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let now = chrono::Utc::now().to_rfc3339();
        let record = SessionRecord {
            session_id: "mission-replay".to_string(),
            platform: "test".to_string(),
            chat_id: "mission-replay".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let request = SessionMissionOutboxRequest {
            request_id: "mission-replay-1".to_string(),
            session_id: record.session_id.clone(),
            title: "Replay session".to_string(),
            workspace_key: "workspace-a".to_string(),
            operation: SessionMissionOutboxOperation::Register,
            created_at_ms: 100,
        };
        store
            .upsert_session_with_mission_outbox(&record, &request)
            .await
            .unwrap();
        let mission = Arc::new(
            runtime::MissionRuntime::event_sourced(
                Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap()),
                "workspace-a",
            )
            .unwrap(),
        );
        let first = store
            .claim_session_mission_outbox("workspace-a", "worker-a", 100, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();

        // Runtime applied the event, but the bridge process lost ownership
        // before the acknowledgement. A restarted worker must replay safely.
        materialize_mission_membership(&store, &mission, "wrong-worker", first).await;
        let replay = store
            .claim_session_mission_outbox("workspace-a", "worker-b", 150, 50, 10)
            .await
            .unwrap()
            .pop()
            .unwrap();
        materialize_mission_membership(&store, &mission, "worker-b", replay).await;

        assert_eq!(mission.list_sessions().len(), 1);
        assert_eq!(
            mission
                .events()
                .iter()
                .filter(|event| event.event_type == "mission.session.registered")
                .count(),
            1
        );
        assert_eq!(
            store
                .get_session_mission_outbox("mission-replay-1")
                .await
                .unwrap()
                .unwrap()
                .status,
            memory::OutboxStatus::Materialized
        );
    }

    #[tokio::test]
    async fn append_success_ack_failure_replays_notification_without_duplicate_message() {
        let (event_store, store, event_bus, mut rx) = delivery_fixture().await;
        event_store
            .enqueue("t1", "m1", "s1", 7, "assistant_json:\"done\"")
            .unwrap();
        let record = event_store
            .claim("owner-a", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(&event_store, &store, &event_bus, "wrong-owner", record).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        let terminal_event: serde_json::Value =
            serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(terminal_event["type"], "TerminalCommitted");
        assert_eq!(terminal_event["terminal_id"], "t1");
        assert_eq!(terminal_event["message_id"], "m1");
        assert_eq!(terminal_event["runtime_commit_cursor"], 7);
        assert_eq!(terminal_event["replayed"], false);
        assert_eq!(event_store.get("t1").unwrap().unwrap().status, "claimed");

        let reclaimed = event_store
            .claim("owner-b", 110, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "owner-b", reclaimed).await;
        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        let replayed: serde_json::Value =
            serde_json::from_str(&rx.try_recv().expect("retry must rebroadcast")).unwrap();
        assert_eq!(replayed["terminal_id"], "t1");
        assert_eq!(replayed["message_id"], "m1");
        assert_eq!(replayed["replayed"], true);
        assert!(rx.try_recv().is_err(), "one retry emits one notification");
        assert_eq!(
            event_store.get("t1").unwrap().unwrap().status,
            "materialized"
        );
    }

    #[tokio::test]
    async fn corrupt_terminal_is_poisoned_and_visible_to_operations() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        event_store
            .enqueue("poison", "m2", "s1", 8, "not-typed")
            .unwrap();
        let record = event_store
            .claim("worker", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        deliver_terminal(&event_store, &store, &event_bus, "worker", record).await;
        let poison = event_store.blocked(10).unwrap();
        assert_eq!(poison.len(), 1);
        assert_eq!(poison[0].terminal_id, "poison");
        assert_eq!(poison[0].failure_class.as_deref(), Some("corrupt_payload"));
    }

    #[tokio::test]
    async fn typed_terminal_atomically_materializes_usage_and_session_counters_before_ack() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        event_store
            .enqueue(
                "usage-terminal",
                "usage-message",
                "s1",
                8,
                r#"assistant_terminal_v1:{"text":"done","token_usage":{"input_tokens":12,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}"#,
            )
            .unwrap();
        let record = event_store
            .claim("worker", 100, 10, 1)
            .unwrap()
            .pop()
            .unwrap();

        deliver_terminal(&event_store, &store, &event_bus, "worker", record).await;

        let session = store.get_session("s1").await.unwrap().unwrap();
        let messages = store.get_messages("s1", 0, 10).await.unwrap();
        assert_eq!(session.message_count, 1);
        assert_eq!(session.input_tokens, 12);
        assert_eq!(session.output_tokens, 3);
        assert_eq!(
            messages[0]
                .token_usage_json
                .as_deref()
                .and_then(|usage| serde_json::from_str::<serde_json::Value>(usage).ok())
                .and_then(|usage| usage["output_tokens"].as_u64()),
            Some(3)
        );
    }

    #[tokio::test]
    async fn delivery_worker_starts_and_shuts_down_gracefully() {
        let (event_store, store, event_bus, _rx) = delivery_fixture().await;
        let (shutdown, receiver) = watch::channel(false);
        let handle = tokio::spawn(run_delivery_worker(event_store, store, event_bus, receiver));
        tokio::task::yield_now().await;
        shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("worker must observe graceful shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn delivery_worker_restart_materializes_terminal_exactly_once() {
        let (event_store, store, event_bus, mut rx) = delivery_fixture().await;
        event_store
            .enqueue(
                "restart-t1",
                "restart-m1",
                "s1",
                9,
                "assistant_json:\"done\"",
            )
            .unwrap();

        for _ in 0..2 {
            let (shutdown, receiver) = watch::channel(false);
            let handle = tokio::spawn(run_delivery_worker(
                event_store.clone(),
                Arc::clone(&store),
                Arc::clone(&event_bus),
                receiver,
            ));
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if event_store
                        .get("restart-t1")
                        .unwrap()
                        .is_some_and(|record| record.status == "materialized")
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("worker must materialize the durable terminal");
            shutdown.send(true).unwrap();
            handle.await.unwrap();
        }

        assert_eq!(store.get_messages("s1", 0, 10).await.unwrap().len(), 1);
        assert!(rx.try_recv().is_ok());
        assert!(
            rx.try_recv().is_err(),
            "restart must not rebroadcast terminal"
        );
    }
}
