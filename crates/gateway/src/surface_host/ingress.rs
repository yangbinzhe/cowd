use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness_contract::turn::{InputSourceKind, SessionInputEnvelope};
use sha2::{Digest, Sha256};
use surface::{message::MessageActionKind, SurfaceActionRequest, SurfaceFrame, SurfaceSendRequest};
use tokio::sync::Semaphore;

use crate::api_routes::AppState;
use crate::runtime_service::IngressRuntimeOptions;
use crate::services::session_service::{
    SurfaceRegisteredResourceEvidence, SurfaceResourceEvidence, SurfaceResourceRegistrationStatus,
    SurfaceSessionJournalEvent,
};
use crate::surface_host::SurfaceTurnCorrelation;

#[derive(Debug, Clone, Copy)]
struct SurfaceTurnPolicy {
    profile: runtime::ContextProfile,
}

pub(crate) fn spawn_surface_ingress_dispatcher(
    state: Arc<AppState>,
    tasks: Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
) -> Result<u64, crate::runtime_host::task_set::GatewayTaskSpawnError> {
    let mut rx = state.services.surface.subscribe_events();
    let concurrency = Arc::new(Semaphore::new(32));
    let claim_owner = format!("gateway-surface-ingress-{}", uuid::Uuid::new_v4());
    let task_owner = Arc::clone(&tasks);
    tasks.spawn(
        crate::runtime_host::task_set::GatewayTaskKind::SurfaceIngress,
        None,
        move |cancellation| async move {
            // This is only a process-local scan cache. The durable Surface
            // ledger and idempotent Session event remain the two authorities;
            // restart intentionally clears the cache and rechecks terminal
            // rows to repair a crash between ledger settlement and projection.
            let mut reconciled_terminal_inbox = BTreeSet::new();
            let mut retry_tick = tokio::time::interval(Duration::from_secs(5));
            retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut reconciliation_tick = tokio::time::interval(Duration::from_secs(30));
            reconciliation_tick
                .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    _ = retry_tick.tick() => {
                        retry_surface_trigger_events(&state).await;
                    }
                    _ = reconciliation_tick.tick() => {
                        // Process-local broadcasts provide immediate dispatch.
                        // This bounded scan repairs cross-process writes and
                        // notifications lost across a Gateway restart.
                        dispatch_pending_ingress(&state, &claim_owner, &concurrency, &task_owner).await;
                        reconcile_surface_terminal_deliveries(
                            &state,
                            &mut reconciled_terminal_inbox,
                        ).await;
                    }
                    received = rx.recv() => {
                        match received {
                            Ok(_) => {
                                // H2 client 已在 ACK 前持久化；broadcast 只负责低延迟唤醒。
                                dispatch_pending_ingress(&state, &claim_owner, &concurrency, &task_owner).await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                                tracing::warn!(skipped, "surface ingress dispatcher lagged");
                                // Lagged 后直接 repository scan，不依赖丢失的广播内容。
                                dispatch_pending_ingress(&state, &claim_owner, &concurrency, &task_owner).await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }
            }
        },
    )
}

async fn dispatch_pending_ingress(
    state: &Arc<AppState>,
    claim_owner: &str,
    concurrency: &Arc<Semaphore>,
    tasks: &Arc<crate::runtime_host::task_set::GatewayRuntimeTaskSet>,
) {
    let available = concurrency.available_permits();
    if available == 0 {
        return;
    }
    let claims =
        match state
            .services
            .surface
            .claim_ingress_frames(claim_owner, available.min(32), 300_000)
        {
            Ok(claims) => claims,
            Err(error) => {
                tracing::error!(error = %error, "surface durable ingress claim failed");
                return;
            }
        };
    for claim in claims {
        let Ok(permit) = concurrency.clone().try_acquire_owned() else {
            break;
        };
        let state = state.clone();
        let spawn_result = tasks.spawn(
            crate::runtime_host::task_set::GatewayTaskKind::SurfaceIngressWork,
            None,
            move |cancellation| async move {
                let record_key = claim.record_key;
                let result = tokio::select! {
                    _ = cancellation.cancelled() => {
                        drop(permit);
                        return;
                    }
                    result = process_durable_ingress_frame(state.clone(), claim.frame) => result,
                };
                match result {
                    Ok(()) => {
                        if let Err(error) = state.services.surface.complete_ingress_frame(&record_key) {
                            tracing::error!(%record_key, error = %error, "surface ingress completion persistence failed");
                        }
                    }
                    Err(error) => {
                        if let Err(persist_error) = state
                            .services
                            .surface
                            .fail_ingress_frame(&record_key, &error)
                        {
                            tracing::error!(%record_key, error = %persist_error, work_error = %error, "surface ingress failure persistence failed");
                        } else {
                            tracing::warn!(%record_key, error = %error, "surface durable ingress work failed");
                        }
                    }
                }
                drop(permit);
            },
        );
        if let Err(error) = spawn_result {
            tracing::debug!(%error, "surface ingress work admission closed during shutdown");
            break;
        }
    }
}

async fn process_durable_ingress_frame(
    state: Arc<AppState>,
    frame: SurfaceFrame,
) -> Result<(), String> {
    let SurfaceFrame::Event {
        surface,
        event,
        payload,
    } = frame
    else {
        return Err("durable surface ingress contains a non-event frame".to_string());
    };
    persist_and_dispatch_surface_trigger(state.clone(), &surface, &event, &payload).await?;
    if event == "message.received" {
        handle_surface_message(state, surface, payload).await?;
    }
    Ok(())
}

/// Terminal replies remain a Surface delivery concern.  The bridge recovers
/// correlations from the durable inbox plus Session ingress identity, then
/// sends through the unchanged Surface outbox with a stable idempotency key.
/// It never treats transient TextDelta as a channel reply.
async fn reconcile_surface_terminal_deliveries(
    state: &Arc<AppState>,
    reconciled_terminal_inbox: &mut BTreeSet<String>,
) {
    let runtime_service = state.services.runtime.as_ref();
    let inboxes = match state.services.surface.all_inbox() {
        Ok(inboxes) => inboxes,
        Err(error) => {
            tracing::warn!(error = %error, "surface terminal bridge inbox scan failed");
            return;
        }
    };
    for inbox in inboxes {
        if matches!(inbox.status.as_str(), "replied" | "failed")
            && reconciled_terminal_inbox.contains(&inbox.idempotency_key)
        {
            continue;
        }
        // The inbox row and durable ingress frame are the projection outbox.
        // Every scan reconstructs the phases implied by that durable state;
        // no process-local acknowledgement is required for correctness.
        if let Err(error) = repair_surface_baseline_projection(state, &inbox).await {
            tracing::warn!(
                surface = %inbox.surface,
                message_id = %inbox.message_id,
                error = %error,
                "surface timeline baseline repair failed"
            );
            continue;
        }
        if inbox.status != "received" {
            if let Err(error) = repair_surface_resource_projection(state, &inbox).await {
                tracing::warn!(
                    surface = %inbox.surface,
                    message_id = %inbox.message_id,
                    error = %error,
                    "surface resources projection repair failed"
                );
            }
        }
        if let Some(correlation) = inbox.correlation.as_ref() {
            if let Err(error) = append_surface_accepted_projection(
                state,
                &inbox,
                &correlation.turn_id,
                &correlation.execution_id,
            )
            .await
            {
                tracing::warn!(
                    surface = %inbox.surface,
                    message_id = %inbox.message_id,
                    error = %error,
                    "surface accepted projection repair failed"
                );
            }
        }
        if inbox.status == "replied" {
            let repair = repair_surface_replied_projection(state, &inbox).await;
            match repair {
                Ok(()) => {
                    reconciled_terminal_inbox.insert(inbox.idempotency_key.clone());
                }
                Err(error) => {
                    tracing::warn!(
                        surface = %inbox.surface,
                        message_id = %inbox.message_id,
                        error = %error,
                        "surface replied projection repair failed"
                    );
                }
            }
            continue;
        }
        if inbox.status == "failed" {
            reconciled_terminal_inbox.insert(inbox.idempotency_key.clone());
            continue;
        }
        if !matches!(
            inbox.status.as_str(),
            "processing" | "processed" | "reply_failed"
        ) {
            continue;
        }
        let Some(runtime_service) = runtime_service else {
            continue;
        };
        let correlation = inbox.correlation.as_ref();
        let (Some(session_id), Some(turn_id)) = (
            correlation
                .map(|item| item.session_id.as_str())
                .or(inbox.runtime_session_id.as_deref()),
            correlation
                .map(|item| item.turn_id.as_str())
                .or(inbox.runtime_turn_id.as_deref()),
        ) else {
            continue;
        };
        let records = match state.services.session.runtime_inputs(session_id, 100).await {
            Ok(records) => records,
            Err(error) => {
                tracing::warn!(
                    session_id,
                    turn_id,
                    error = %error,
                    "surface terminal bridge could not read ingress correlation"
                );
                continue;
            }
        };
        let Some(record) = records.into_iter().find(|record| record.turn_id == turn_id) else {
            continue;
        };
        let execution_id =
            runtime::session_ingress_graph_id(session_id, &record.request_id, &record.turn_id);
        if correlation.is_some_and(|item| item.execution_id != execution_id) {
            tracing::error!(
                surface = %inbox.surface,
                message_id = %inbox.message_id,
                correlated = ?correlation.map(|item| &item.execution_id),
                derived = %execution_id,
                "surface turn correlation does not match durable ingress identity"
            );
            continue;
        }
        if matches!(
            record.status,
            session::SessionRuntimeInputStatus::Failed
                | session::SessionRuntimeInputStatus::Blocked
                | session::SessionRuntimeInputStatus::Cancelled
                | session::SessionRuntimeInputStatus::Expired
        ) {
            let error = record
                .last_error
                .unwrap_or_else(|| "Runtime could not materialize the surface turn".to_string());
            if let Err(mark_error) = state
                .services
                .surface
                .mark_inbox_failed(&inbox.idempotency_key, error.clone())
            {
                tracing::warn!(
                    surface = %inbox.surface,
                    message_id = %inbox.message_id,
                    error = %mark_error,
                    "surface terminal bridge could not record Runtime failure"
                );
            }
            send_surface_failure_notice(
                state,
                &inbox.surface,
                &inbox.payload_json,
                session_id,
                &inbox.message_id,
                &error,
            )
            .await;
            continue;
        }
        if !matches!(
            record.status,
            session::SessionRuntimeInputStatus::Completed
                | session::SessionRuntimeInputStatus::Supplemented
        ) {
            continue;
        }
        let terminal_id = format!("turn-terminal:{}", record.request_id);
        let terminal = match runtime_service
            .runtime_services()
            .session_terminal_delivery()
            .get(&terminal_id)
        {
            Ok(Some(terminal)) => terminal,
            Ok(None) => continue,
            Err(error) => {
                tracing::warn!(%terminal_id, error = %error, "surface terminal bridge lookup failed");
                continue;
            }
        };
        let payload = match crate::session_runtime_bridge::load_terminal_payload(
            runtime_service.runtime_services().artifact_store(),
            &terminal,
        )
        .await
        {
            Ok(payload) => payload,
            Err((_, error)) => {
                let _ = state
                    .services
                    .surface
                    .mark_inbox_failed(&inbox.idempotency_key, error.clone());
                tracing::warn!(surface = %inbox.surface, message_id = %inbox.message_id, error, "surface terminal payload is invalid");
                continue;
            }
        };
        if let Err(error) =
            deliver_surface_terminal(state, &inbox, &payload.text, &terminal_id).await
        {
            tracing::warn!(
                surface = %inbox.surface,
                message_id = %inbox.message_id,
                %terminal_id,
                error = %error,
                "surface terminal bridge delivery did not settle"
            );
        }
    }
}

async fn deliver_surface_terminal(
    state: &AppState,
    inbox: &crate::surface_host::SurfaceInboxRecord,
    text: &str,
    terminal_id: &str,
) -> Result<(), String> {
    let correlation = inbox.correlation.as_ref();
    let session_id = correlation
        .map(|item| item.session_id.as_str())
        .or(inbox.runtime_session_id.as_deref())
        .unwrap_or_default();
    if correlation.is_some() {
        state
            .services
            .surface
            .record_inbox_terminal_delivery(&inbox.idempotency_key, terminal_id)?;
    }
    if text.trim().is_empty() {
        let replied_projection = SurfaceSessionJournalEvent::SurfaceMessageReplied {
            surface: inbox.surface.clone(),
            message_id: inbox.message_id.clone(),
            turn_id: correlation
                .map(|item| item.turn_id.clone())
                .unwrap_or_default(),
            execution_id: correlation
                .map(|item| item.execution_id.clone())
                .unwrap_or_default(),
            terminal_id: terminal_id.to_string(),
            status: "empty_terminal".to_string(),
            empty_terminal: true,
            outbox_ref: None,
            error_code: None,
        }
        .projection_draft(session_id.to_string())
        .map_err(|error| error.to_string())?;
        state.services.surface.mark_inbox_replied(
            &inbox.idempotency_key,
            std::slice::from_ref(&replied_projection),
        )?;
        notify_surface_processing_lifecycle(
            state,
            &inbox.surface,
            MessageActionKind::ProcessingComplete.as_str(),
            &inbox.message_id,
            None,
        )
        .await;
        flush_surface_projection(state, &inbox.idempotency_key, "replied").await?;
        return Ok(());
    }
    let recipient = surface_reply_recipient(&inbox.payload_json)
        .or_else(|| inbox.thread_id.clone())
        .or_else(|| inbox.sender_id.clone())
        .unwrap_or_else(|| session_id.to_string());
    let reply_to = surface_platform_reply_to(&inbox.payload_json, &inbox.message_id);
    let outbound = state
        .services
        .surface
        .send(SurfaceSendRequest {
            surface: inbox.surface.clone(),
            recipient,
            thread: inbox.thread_id.clone(),
            text: text.to_string(),
            idempotency_key: Some(correlation.map_or_else(
                || format!("surface-reply:{}:{}", inbox.surface, inbox.message_id),
                |item| item.reply_idempotency_key.clone(),
            )),
            metadata: serde_json::json!({
                "reply_to": reply_to,
                "local_reply_to": inbox.message_id,
                "source_session_id": session_id,
                "terminal_id": terminal_id,
                "source": "surface_terminal_bridge",
            }),
        })
        .await?;
    if let Some(error) = outbound.error.as_ref() {
        state
            .services
            .surface
            .mark_inbox_reply_failed(&inbox.idempotency_key, error.message.clone())?;
        notify_surface_processing_lifecycle(
            state,
            &inbox.surface,
            MessageActionKind::ProcessingFailed.as_str(),
            &inbox.message_id,
            Some(error.message.clone()),
        )
        .await;
        return Err(error.message.clone());
    }
    let replied_projection = SurfaceSessionJournalEvent::SurfaceMessageReplied {
        surface: inbox.surface.clone(),
        message_id: inbox.message_id.clone(),
        turn_id: correlation
            .map(|item| item.turn_id.clone())
            .unwrap_or_default(),
        execution_id: correlation
            .map(|item| item.execution_id.clone())
            .unwrap_or_default(),
        terminal_id: terminal_id.to_string(),
        status: "replied".to_string(),
        empty_terminal: false,
        outbox_ref: outbound
            .message_id
            .as_ref()
            .map(|message_id| format!("surface-outbox:{}:{message_id}", inbox.surface)),
        error_code: outbound.error.as_ref().map(|error| error.code.clone()),
    }
    .projection_draft(session_id.to_string())
    .map_err(|error| error.to_string())?;
    state.services.surface.mark_inbox_replied(
        &inbox.idempotency_key,
        std::slice::from_ref(&replied_projection),
    )?;
    notify_surface_processing_lifecycle(
        state,
        &inbox.surface,
        MessageActionKind::ProcessingComplete.as_str(),
        &inbox.message_id,
        None,
    )
    .await;
    flush_surface_projection(state, &inbox.idempotency_key, "replied").await?;
    Ok(())
}

/// Persist a Surface transport event before attempting Runtime delivery.  The
/// persisted record is the recoverable transport handoff; it is intentionally
/// not a second scheduler or source-matching implementation.
async fn persist_and_dispatch_surface_trigger(
    state: Arc<AppState>,
    surface: &str,
    event_type: &str,
    payload: &serde_json::Value,
) -> Result<(), String> {
    let event = normalize_surface_event(surface, event_type, payload);
    let receipt = state
        .services
        .surface
        .record_trigger_event_received(surface, event_type, &event, payload)
        .map_err(|error| format!("surface trigger event could not be persisted: {error}"))?;
    if receipt.duplicate {
        tracing::debug!(
            surface,
            event = event_type,
            status = %receipt.record.status,
            "surface trigger event resumed from durable duplicate"
        );
    }
    dispatch_surface_trigger_event(state, receipt.record.idempotency_key).await;
    Ok(())
}

async fn retry_surface_trigger_events(state: &Arc<AppState>) {
    let records = match state.services.surface.due_trigger_event_retries() {
        Ok(records) => records,
        Err(error) => {
            tracing::warn!(error = %error, "surface trigger retry scan failed");
            return;
        }
    };
    for record in records {
        dispatch_surface_trigger_event(state.clone(), record.idempotency_key).await;
    }
}

async fn dispatch_surface_trigger_event(state: Arc<AppState>, idempotency_key: String) {
    let record = match state
        .services
        .surface
        .mark_trigger_event_dispatching(&idempotency_key)
    {
        Ok(Some(record)) => record,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%idempotency_key, error = %error, "surface trigger event claim failed");
            return;
        }
    };
    let result = state
        .services
        .runtime
        .as_ref()
        .ok_or_else(|| "runtime service unavailable".to_string())
        .and_then(|runtime| {
            runtime
                .runtime_services()
                .accept_managed_agent_event(record.trigger.clone())
                .map(|_| ())
                .map_err(|error| error.to_string())
        });
    match result {
        Ok(()) => {
            if let Err(error) = state
                .services
                .surface
                .mark_trigger_event_accepted(&idempotency_key)
            {
                tracing::error!(%idempotency_key, error = %error, "Runtime accepted surface trigger but receipt persistence failed");
            }
        }
        Err(error) => {
            match state
                .services
                .surface
                .mark_trigger_event_failed(&idempotency_key, &error)
            {
                Ok(updated) => tracing::warn!(
                    surface = %updated.surface,
                    event = %updated.event_type,
                    attempts = updated.attempts,
                    max_attempts = updated.max_attempts,
                    status = %updated.status,
                    error = %error,
                    "surface trigger event Runtime handoff failed"
                ),
                Err(persistence_error) => tracing::error!(
                    %idempotency_key,
                    error = %persistence_error,
                    runtime_error = %error,
                    "surface trigger event failure could not be persisted"
                ),
            }
        }
    }
}

/// Normalize one Surface transport event into the Runtime-owned trigger
/// contract. This deliberately performs no matching, authorization or
/// scheduling: those decisions are durable Dispatcher responsibilities.
fn normalize_surface_event(
    surface: &str,
    event_type: &str,
    payload: &serde_json::Value,
) -> harness_contract::managed_agent::ManagedAgentTriggerEvent {
    let event_local_id = payload_string(payload, "event_id")
        .or_else(|| payload_string(payload, "message_id"))
        .or_else(|| payload_string(payload, "id"))
        .unwrap_or_else(|| payload_fingerprint_id(surface, payload));
    let session_id = surface_session_id(surface, payload);
    let thread_id = payload_string(payload, "thread_id");
    let user_id = payload_string(payload, "user_id");
    let mut attributes = BTreeMap::new();
    attributes.insert("surface".to_string(), surface.to_string());
    attributes.insert("session_id".to_string(), session_id.clone());
    if let Some(thread_id) = thread_id {
        attributes.insert("thread_id".to_string(), thread_id);
    }
    if let Some(user_id) = user_id {
        attributes.insert("user_id".to_string(), user_id);
    }
    let stable_key = format!("surface-event:{surface}:{event_type}:{event_local_id}");
    harness_contract::managed_agent::ManagedAgentTriggerEvent {
        event_id: stable_key.clone(),
        source_id: surface.to_string(),
        source_kind: "surface".to_string(),
        event_type: event_type.to_string(),
        subject: session_id,
        payload_ref: format!("surface-event:{stable_key}"),
        payload_digest: format!(
            "sha256:{:x}",
            Sha256::digest(payload.to_string().as_bytes())
        ),
        occurred_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
        source_sequence: None,
        idempotency_key: stable_key.clone(),
        source_capabilities: vec!["surface.event.receive".to_string()],
        attributes,
        trace_refs: vec![format!("surface:{surface}:event:{event_local_id}")],
    }
}

async fn handle_surface_message(
    state: Arc<AppState>,
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
    let user_id = payload_string(&payload, "user_id");
    let thread_id = payload_string(&payload, "thread_id");
    let metadata = payload
        .get("metadata")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let inbox_key = format!("{}:{message_id}", surface::normalize_surface_id(&surface));
    let content_preview = payload_string(&payload, "text")
        .or_else(|| payload_string(&payload, "content"))
        .unwrap_or_else(|| serde_json::to_string(&payload).unwrap_or_default())
        .chars()
        .take(160)
        .collect();
    let payload_sha256 = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_string(&payload)
                .unwrap_or_default()
                .as_bytes()
        )
    );
    let baseline_projections = vec![
        SurfaceSessionJournalEvent::SurfaceMessageReceived {
            surface: surface.clone(),
            message_id: message_id.clone(),
            thread_id: thread_id.clone(),
            user_id: user_id.clone(),
            content_preview,
            inbox_ref: format!("surface-inbox:{inbox_key}"),
            payload_sha256,
        }
        .projection_draft(session_id.clone())
        .map_err(|error| error.to_string())?,
        SurfaceSessionJournalEvent::SurfaceSessionRuntimeActivated {
            surface: surface.clone(),
            session_id: session_id.clone(),
            message_id: message_id.clone(),
        }
        .projection_draft(session_id.clone())
        .map_err(|error| error.to_string())?,
    ];
    let inbox = state.services.surface.record_inbox_received(
        &surface,
        &message_id,
        &payload,
        &session_id,
        thread_id.clone(),
        user_id.clone(),
        &baseline_projections,
    )?;
    if inbox.duplicate {
        tracing::info!(
            %surface,
            %message_id,
            status = %inbox.record.status,
            "surface message resumed from durable duplicate"
        );
    }
    ensure_surface_runtime_session(&state, &surface, &session_id, user_id.as_deref(), &metadata)
        .await?;
    repair_surface_baseline_projection(&state, &inbox.record).await?;

    let current_media = payload_media_attachments(&payload, &message_id);
    let recent_media = if current_media.is_empty() && content_references_surface_media(&content) {
        recent_surface_media(&state, &surface, &session_id, &message_id)?
    } else {
        Vec::new()
    };
    let current_resources =
        register_surface_resources(&state, &surface, &session_id, &current_media);
    let recent_resources = register_surface_resources(&state, &surface, &session_id, &recent_media);
    let resource_projection = SurfaceSessionJournalEvent::SurfaceMessageResourcesRegistered {
        surface: surface.clone(),
        message_id: message_id.clone(),
        current: surface_resource_evidence_rows(&current_resources),
        recent: surface_resource_evidence_rows(&recent_resources),
    }
    .projection_draft(session_id.clone())
    .map_err(|error| error.to_string())?;
    if !inbox
        .record
        .session_projections
        .iter()
        .any(|projection| projection.phase == "resources")
    {
        if inbox.record.status == "received" {
            state.services.surface.mark_inbox_processing(
                &inbox.record.idempotency_key,
                std::slice::from_ref(&resource_projection),
            )?;
        } else {
            state.services.surface.stage_inbox_projections(
                &inbox.record.idempotency_key,
                std::slice::from_ref(&resource_projection),
            )?;
        }
    }
    flush_surface_projection(&state, &inbox.record.idempotency_key, "resources").await?;
    let pre_messages = surface_runtime_pre_messages(&content, &current_media, &recent_media);
    let runtime_content = surface_runtime_content(&content, &current_resources, &recent_resources);
    let turn_policy = surface_turn_policy(&runtime_content);
    // Surface metadata enters the same durable SessionIngress as WebUI/TUI.
    // Runtime reads the opaque options only after it owns the per-session
    // executor, preserving image blocks and context policy without a direct
    // SurfaceHost turn path.
    let surface_turn_id = format!(
        "surface-turn:{}",
        stable_surface_turn_digest(&session_id, &inbox.record.idempotency_key)
    );
    let runtime_options = serde_json::to_value(IngressRuntimeOptions {
        profile: turn_policy.profile,
        pre_messages: pre_messages.into_iter().map(Into::into).collect(),
    })
    .map_err(|error| error.to_string())?;
    // Persist the deterministic ingress identity before the router admits the
    // input.  If the process stops after admission but before a response is
    // returned, the terminal-delivery bridge can still reconstruct exactly
    // one reply from this durable correlation.
    let execution_id = runtime::session_ingress_graph_id(
        &session_id,
        &inbox.record.idempotency_key,
        &surface_turn_id,
    );
    if inbox.duplicate {
        let durable_input_exists = if let Some(correlation) = inbox.record.correlation.as_ref() {
            let exists = state
                .services
                .session
                .runtime_inputs(&session_id, 500)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .any(|record| {
                    record.request_id == inbox.record.idempotency_key
                        && record.turn_id == correlation.turn_id
                });
            if exists {
                append_surface_accepted_projection(
                    &state,
                    &inbox.record,
                    &correlation.turn_id,
                    &correlation.execution_id,
                )
                .await?;
            }
            exists
        } else {
            false
        };
        if !surface_duplicate_requires_runtime_admission(
            &inbox.record.status,
            inbox.record.correlation.is_some(),
            durable_input_exists,
        ) {
            if inbox.record.status == "replied" {
                repair_surface_replied_projection(&state, &inbox.record).await?;
            }
            return Ok(());
        }
    }
    let correlation = SurfaceTurnCorrelation {
        surface: surface.clone(),
        message_id: message_id.clone(),
        inbox_idempotency_key: inbox.record.idempotency_key.clone(),
        session_id: session_id.clone(),
        turn_id: surface_turn_id.clone(),
        execution_id: execution_id.clone(),
        reply_to_message_id: surface_platform_reply_to(&payload, &message_id),
        reply_idempotency_key: format!("surface-reply:{surface}:{message_id}"),
        terminal_id: None,
        terminal_delivery_revision: 0,
    };
    let admission = state
        .services
        .session
        .admit_input(
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
                "turn_id": surface_turn_id.clone(),
                "runtime_options": runtime_options,
            })),
        )
        .await
        .map_err(|error| error)?;
    debug_assert_eq!(execution_id, admission.execution_graph_id);
    let accepted_projection = SurfaceSessionJournalEvent::SurfaceMessageAccepted {
        surface,
        message_id,
        turn_id: surface_turn_id,
        execution_id: admission.execution_graph_id,
    }
    .projection_draft(session_id)
    .map_err(|error| error.to_string())?;
    state.services.surface.mark_inbox_admitted(
        &inbox.record.idempotency_key,
        correlation,
        std::slice::from_ref(&accepted_projection),
    )?;
    flush_surface_projection(&state, &inbox.record.idempotency_key, "accepted").await?;
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
        return SurfaceTurnPolicy { profile };
    }
    match profile {
        runtime::ContextProfile::DeepInvestigation => SurfaceTurnPolicy { profile },
        _ => SurfaceTurnPolicy { profile },
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

fn stable_surface_turn_digest(session_id: &str, idempotency_key: &str) -> String {
    let digest = Sha256::digest(format!("{session_id}:{idempotency_key}").as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn surface_duplicate_requires_runtime_admission(
    status: &str,
    has_correlation: bool,
    durable_input_exists: bool,
) -> bool {
    if matches!(status, "replied" | "failed" | "dead_letter" | "cancelled") {
        return false;
    }
    !(has_correlation && durable_input_exists)
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
            idempotency_key: Some(format!("surface-failure:{surface}:{message_id}")),
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

async fn flush_surface_projection(
    state: &AppState,
    inbox_key: &str,
    phase: &str,
) -> Result<(), String> {
    let inbox = state
        .services
        .surface
        .inbox_by_key(inbox_key)?
        .ok_or_else(|| format!("surface inbox `{inbox_key}` not found during projection"))?;
    let projection = inbox
        .session_projections
        .iter()
        .find(|projection| projection.phase == phase)
        .cloned()
        .ok_or_else(|| {
            format!("surface inbox `{inbox_key}` has no durable `{phase}` projection")
        })?;
    if projection.projection_state == "applied" {
        return Ok(());
    }
    match state
        .services
        .session
        .append_surface_journal_projection(inbox_key, &projection)
        .await
    {
        Ok(_) => state.services.surface.mark_inbox_projection_applied(
            inbox_key,
            &projection.event_id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(i64::MAX as u128) as i64,
        ),
        Err(error) => {
            let error = error.to_string();
            let _ = state.services.surface.mark_inbox_projection_failed(
                inbox_key,
                &projection.event_id,
                &error,
            );
            Err(error)
        }
    }
}

async fn repair_surface_baseline_projection(
    state: &AppState,
    inbox: &crate::surface_host::SurfaceInboxRecord,
) -> Result<(), String> {
    let session_id = inbox
        .correlation
        .as_ref()
        .map(|item| item.session_id.as_str())
        .or(inbox.runtime_session_id.as_deref())
        .ok_or_else(|| {
            format!(
                "surface inbox `{}` has no Runtime Session identity",
                inbox.idempotency_key
            )
        })?;
    if !inbox
        .session_projections
        .iter()
        .any(|projection| projection.phase == "received")
    {
        let content_preview = payload_string(&inbox.payload_json, "text")
            .or_else(|| payload_string(&inbox.payload_json, "content"))
            .unwrap_or_else(|| inbox.payload_summary.clone())
            .chars()
            .take(160)
            .collect();
        let drafts = vec![
            SurfaceSessionJournalEvent::SurfaceMessageReceived {
                surface: inbox.surface.clone(),
                message_id: inbox.message_id.clone(),
                thread_id: inbox.thread_id.clone(),
                user_id: inbox.sender_id.clone(),
                content_preview,
                inbox_ref: format!("surface-inbox:{}", inbox.idempotency_key),
                payload_sha256: inbox.payload_hash.clone(),
            }
            .projection_draft(session_id.to_string())
            .map_err(|error| error.to_string())?,
            SurfaceSessionJournalEvent::SurfaceSessionRuntimeActivated {
                surface: inbox.surface.clone(),
                session_id: session_id.to_string(),
                message_id: inbox.message_id.clone(),
            }
            .projection_draft(session_id.to_string())
            .map_err(|error| error.to_string())?,
        ];
        state
            .services
            .surface
            .stage_inbox_projections(&inbox.idempotency_key, &drafts)?;
    }
    if !surface_projection_is_applied(inbox, "received") {
        flush_surface_projection(state, &inbox.idempotency_key, "received").await?;
    }
    if !surface_projection_is_applied(inbox, "activated") {
        flush_surface_projection(state, &inbox.idempotency_key, "activated").await?;
    }
    Ok(())
}

async fn append_surface_accepted_projection(
    state: &AppState,
    inbox: &crate::surface_host::SurfaceInboxRecord,
    turn_id: &str,
    execution_id: &str,
) -> Result<(), String> {
    let session_id = inbox
        .correlation
        .as_ref()
        .map(|item| item.session_id.as_str())
        .or(inbox.runtime_session_id.as_deref())
        .ok_or_else(|| {
            format!(
                "surface inbox `{}` has no Runtime Session identity",
                inbox.idempotency_key
            )
        })?;
    if !inbox
        .session_projections
        .iter()
        .any(|projection| projection.phase == "accepted")
    {
        let draft = SurfaceSessionJournalEvent::SurfaceMessageAccepted {
            surface: inbox.surface.clone(),
            message_id: inbox.message_id.clone(),
            turn_id: turn_id.to_string(),
            execution_id: execution_id.to_string(),
        }
        .projection_draft(session_id.to_string())
        .map_err(|error| error.to_string())?;
        state
            .services
            .surface
            .stage_inbox_projections(&inbox.idempotency_key, std::slice::from_ref(&draft))?;
    }
    if surface_projection_is_applied(inbox, "accepted") {
        Ok(())
    } else {
        flush_surface_projection(state, &inbox.idempotency_key, "accepted").await
    }
}

async fn repair_surface_resource_projection(
    state: &AppState,
    inbox: &crate::surface_host::SurfaceInboxRecord,
) -> Result<(), String> {
    if inbox
        .session_projections
        .iter()
        .any(|projection| projection.phase == "resources")
    {
        return if surface_projection_is_applied(inbox, "resources") {
            Ok(())
        } else {
            flush_surface_projection(state, &inbox.idempotency_key, "resources").await
        };
    }
    let session_id = inbox
        .correlation
        .as_ref()
        .map(|item| item.session_id.as_str())
        .or(inbox.runtime_session_id.as_deref())
        .ok_or_else(|| {
            format!(
                "surface inbox `{}` has no Runtime Session identity",
                inbox.idempotency_key
            )
        })?;
    let content = payload_string(&inbox.payload_json, "text")
        .or_else(|| payload_string(&inbox.payload_json, "content"))
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            (!payload_media_attachments(&inbox.payload_json, &inbox.message_id).is_empty())
                .then(|| "[Attachment]".to_string())
        })
        .unwrap_or_default();
    let current_media = payload_media_attachments(&inbox.payload_json, &inbox.message_id);
    let recent_media = if current_media.is_empty() && content_references_surface_media(&content) {
        recent_surface_media(state, &inbox.surface, session_id, &inbox.message_id)?
    } else {
        Vec::new()
    };
    let current_resources =
        register_surface_resources(state, &inbox.surface, session_id, &current_media);
    let recent_resources =
        register_surface_resources(state, &inbox.surface, session_id, &recent_media);
    let draft = SurfaceSessionJournalEvent::SurfaceMessageResourcesRegistered {
        surface: inbox.surface.clone(),
        message_id: inbox.message_id.clone(),
        current: surface_resource_evidence_rows(&current_resources),
        recent: surface_resource_evidence_rows(&recent_resources),
    }
    .projection_draft(session_id.to_string())
    .map_err(|error| error.to_string())?;
    state
        .services
        .surface
        .stage_inbox_projections(&inbox.idempotency_key, std::slice::from_ref(&draft))?;
    flush_surface_projection(state, &inbox.idempotency_key, "resources").await
}

async fn repair_surface_replied_projection(
    state: &AppState,
    inbox: &crate::surface_host::SurfaceInboxRecord,
) -> Result<(), String> {
    if inbox
        .session_projections
        .iter()
        .any(|projection| projection.phase == "replied")
    {
        return if surface_projection_is_applied(inbox, "replied") {
            Ok(())
        } else {
            flush_surface_projection(state, &inbox.idempotency_key, "replied").await
        };
    }
    let correlation = inbox.correlation.as_ref().ok_or_else(|| {
        format!(
            "replied surface inbox `{}` has no turn correlation",
            inbox.idempotency_key
        )
    })?;
    let terminal_id = correlation.terminal_id.as_deref().ok_or_else(|| {
        format!(
            "replied surface inbox `{}` has no terminal identity",
            inbox.idempotency_key
        )
    })?;
    let outbound = state
        .services
        .surface
        .outbox(&inbox.surface)?
        .into_iter()
        .find(|record| record.idempotency_key == correlation.reply_idempotency_key);
    let draft = SurfaceSessionJournalEvent::SurfaceMessageReplied {
        surface: inbox.surface.clone(),
        message_id: inbox.message_id.clone(),
        turn_id: correlation.turn_id.clone(),
        execution_id: correlation.execution_id.clone(),
        terminal_id: terminal_id.to_string(),
        status: if outbound.is_some() {
            "replied".to_string()
        } else {
            "empty_terminal".to_string()
        },
        empty_terminal: outbound.is_none(),
        outbox_ref: outbound
            .as_ref()
            .map(|record| format!("surface-outbox:{}", record.delivery_id)),
        error_code: None,
    }
    .projection_draft(correlation.session_id.clone())
    .map_err(|error| error.to_string())?;
    state
        .services
        .surface
        .stage_inbox_projections(&inbox.idempotency_key, std::slice::from_ref(&draft))?;
    flush_surface_projection(state, &inbox.idempotency_key, "replied").await
}

fn surface_projection_is_applied(
    inbox: &crate::surface_host::SurfaceInboxRecord,
    phase: &str,
) -> bool {
    inbox
        .session_projections
        .iter()
        .any(|projection| projection.phase == phase && projection.projection_state == "applied")
}

fn surface_resource_evidence_rows(
    resources: &[SurfaceResourceRegistration],
) -> Vec<SurfaceResourceEvidence> {
    resources
        .iter()
        .map(|registration| {
            let resource = registration.resource.as_ref().map(|(resource, _hint)| {
                SurfaceRegisteredResourceEvidence {
                    resource_id: resource.id.clone(),
                    uri: resource.uri.clone(),
                    kind: resource.kind.as_str().to_string(),
                    declared_mime: resource.declared_mime.clone(),
                    detected_mime: resource.detected_mime.clone(),
                    artifact_selector: resource.artifact.selector.clone(),
                    sha256: resource.artifact.sha256.clone(),
                    bytes: resource.artifact.bytes,
                }
            });
            SurfaceResourceEvidence {
                source_message_id: registration.attachment.source_message_id.clone(),
                media_type: registration.attachment.media_type.clone(),
                resource,
                status: if registration.resource.is_some() {
                    SurfaceResourceRegistrationStatus::Registered
                } else {
                    SurfaceResourceRegistrationStatus::Failed
                },
                error_code: registration
                    .error
                    .as_ref()
                    .map(|_| "resource_registration_failed".to_string()),
            }
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
    let session_service = &state.services.session;
    let mut request = crate::services::EnsureSessionRequest::new(
        session_id,
        None,
        crate::services::SessionSource::Surface(surface.to_string()),
    );
    request.user_id = user_id.map(ToOwned::to_owned);
    request.chat_id = payload_string(metadata, "chat_id");
    request.title = Some(format!(
        "{} {}",
        surface,
        session_id.chars().take(8).collect::<String>()
    ));
    request.metadata = serde_json::json!({
        "surface": surface,
        "source": "surface_ingress_dispatcher",
        "metadata": metadata,
    });
    session_service.ensure_surface_session(request).await?;
    Ok(())
}

pub(super) fn surface_session_id(surface: &str, payload: &serde_json::Value) -> String {
    payload_string(payload, "session")
        .or_else(|| payload_string(payload, "session_id"))
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
    resource: Option<(runtime::ResourceProjection, runtime::ResourceHint)>,
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

    let mut resource_ids = std::collections::BTreeSet::new();
    let resource_pairs = current_resources
        .iter()
        .chain(recent_resources.iter())
        .filter_map(|registration| registration.resource.as_ref())
        .filter(|(resource, _)| resource_ids.insert(resource.id.clone()))
        .map(|(resource, hint)| hint.prompt_hint(resource))
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
    let artifact_store = state.services.artifact_store();
    media
        .iter()
        .map(|attachment| {
            let Some(artifact_store) = artifact_store.as_ref() else {
                return SurfaceResourceRegistration {
                    attachment: attachment.clone(),
                    resource: None,
                    error: Some("runtime artifact store is unavailable".to_string()),
                };
            };
            let store = runtime::ResourceStore::from_artifact_store(
                &state.config_home,
                Arc::clone(artifact_store),
                state.services.resource_capability_index(),
            );
            let resource_key = format!(
                "surface:{surface}:session:{session_id}:message:{}:type:{}:path:{}",
                attachment.source_message_id, attachment.media_type, attachment.local_path
            );
            match store.register_resource_from_path_idempotent(
                &attachment.local_path,
                &resource_key,
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
) -> Result<Vec<SurfaceMediaAttachment>, String> {
    let mut attachments = state
        .services
        .surface
        .inbox(surface)?
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
    Ok(attachments
        .into_iter()
        .map(|(_, attachment)| attachment)
        .take(3)
        .collect())
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
    fn surface_session_id_accepts_canonical_session_id() {
        let payload = serde_json::json!({
            "session_id": "session-canonical",
            "user_id": "user-a",
            "thread_id": "chat-a"
        });

        assert_eq!(surface_session_id("feishu", &payload), "session-canonical");
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
    fn duplicate_after_correlation_crash_resumes_only_when_durable_input_is_missing() {
        assert!(surface_duplicate_requires_runtime_admission(
            "processing",
            false,
            false
        ));
        assert!(surface_duplicate_requires_runtime_admission(
            "processed",
            true,
            false
        ));
        assert!(!surface_duplicate_requires_runtime_admission(
            "processed",
            true,
            true
        ));
    }

    #[test]
    fn terminal_duplicate_repairs_projection_without_reopening_runtime_admission() {
        assert!(!surface_duplicate_requires_runtime_admission(
            "replied", true, true
        ));
        assert!(!surface_duplicate_requires_runtime_admission(
            "replied", false, false
        ));
        assert!(!surface_duplicate_requires_runtime_admission(
            "failed", true, false
        ));
    }

    #[test]
    fn readme_followup_uses_deep_surface_profile_without_business_budget() {
        let policy = surface_turn_policy("我已经更新，看是否最新的readme还有问题");

        assert_eq!(policy.profile, runtime::ContextProfile::DeepInvestigation);
    }

    #[test]
    fn short_surface_message_uses_quick_profile_without_business_budget() {
        let policy = surface_turn_policy("你好");

        assert_eq!(policy.profile, runtime::ContextProfile::SurfaceQuickReply);
    }

    #[test]
    fn media_surface_message_uses_media_budget_without_deep_context() {
        let policy = surface_turn_policy(
            "[Image]\n\n## Attached Resources\n\n### resource://res_test\n- kind: image\n",
        );

        assert_eq!(policy.profile, runtime::ContextProfile::SurfaceQuickReply);
    }

    #[test]
    fn media_surface_message_uses_deep_budget_when_user_intent_is_deep() {
        let policy = surface_turn_policy(
            "请分析这张图片是否有问题\n\n## Attached Resources\n\n### resource://res_test\n- kind: image\n",
        );

        assert_eq!(policy.profile, runtime::ContextProfile::DeepInvestigation);
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
    fn resources_projection_inputs_rebuild_from_durable_inbox_payload() {
        let payload = serde_json::json!({
            "message_id": "om-resource",
            "text": "inspect attachment",
            "media_urls": ["/tmp/cowd-resource-a.png", "/tmp/cowd-resource-b.pdf"],
            "media_types": ["image/png", "application/pdf"]
        });

        let first = payload_media_attachments(&payload, "om-resource");
        let replayed_payload: serde_json::Value =
            serde_json::from_str(&payload.to_string()).expect("durable payload roundtrips");
        let replay = payload_media_attachments(&replayed_payload, "om-resource");

        assert_eq!(first, replay);
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].source_message_id, "om-resource");
        assert_eq!(replay[0].media_type, "image/png");
        assert_eq!(replay[1].local_path, "/tmp/cowd-resource-b.pdf");
    }

    #[test]
    fn resources_projection_replay_uses_content_addressed_registration() {
        let temp = tempfile::tempdir().expect("tempdir");
        let resource_path = temp.path().join("replay-resource.txt");
        std::fs::write(&resource_path, b"stable resource payload").expect("resource writes");
        let store = runtime::ResourceStore::default_for_config_home(&temp.path().join("home"));

        let first = store
            .register_resource_from_path_idempotent(
                &resource_path,
                "surface:feishu:session:session-resource:message:om-resource:type:text/plain:path:replay-resource.txt",
                "surface:feishu",
                Some("om-resource".to_string()),
                Some("session-resource".to_string()),
                Some("text/plain".to_string()),
            )
            .expect("initial registration");
        let replay = store
            .register_resource_from_path_idempotent(
                &resource_path,
                "surface:feishu:session:session-resource:message:om-resource:type:text/plain:path:replay-resource.txt",
                "surface:feishu",
                Some("om-resource".to_string()),
                Some("session-resource".to_string()),
                Some("text/plain".to_string()),
            )
            .expect("replayed registration");

        assert_eq!(first.0.id, replay.0.id);
        assert_eq!(first.0.artifact.sha256, replay.0.artifact.sha256);
    }

    #[test]
    fn surface_failure_notice_text_is_visible_and_actionable() {
        let text = surface_failure_notice_text("turn timed out after 240s");

        assert!(text.contains("已经进入 Cowd"));
        assert!(text.contains("turn timed out after 240s"));
        assert!(text.contains("重放"));
    }

    #[test]
    fn surface_events_normalize_to_stable_runtime_trigger_contracts() {
        let payload = serde_json::json!({
            "message_id": "om_123",
            "thread_id": "chat_456",
            "user_id": "user_789",
            "session_id": "session_ops",
            "text": "inspect the incident"
        });

        let event = normalize_surface_event("feishu", "message.received", &payload);

        assert_eq!(
            event.event_id,
            "surface-event:feishu:message.received:om_123"
        );
        assert_eq!(event.idempotency_key, event.event_id);
        assert_eq!(event.source_id, "feishu");
        assert_eq!(event.source_kind, "surface");
        assert_eq!(event.subject, "session_ops");
        assert_eq!(event.source_capabilities, vec!["surface.event.receive"]);
        assert_eq!(
            event.attributes.get("thread_id"),
            Some(&"chat_456".to_string())
        );
        assert_eq!(
            event.attributes.get("user_id"),
            Some(&"user_789".to_string())
        );
        assert!(event.payload_digest.starts_with("sha256:"));
        assert_eq!(event.trace_refs, vec!["surface:feishu:event:om_123"]);
    }
}
